use std::{
    collections::{BTreeMap, BTreeSet},
    convert::Infallible,
    net::Ipv4Addr,
    path::Path,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    body::Body,
    extract::{Path as AxumPath, Query, Request, State},
    http::{HeaderMap, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use clap::Args;
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::{Value, json};
use swarmlite_client::ControllerClientError;
use swarmlite_core::model::{
    DataSessionCreateResponse, DataSessionOperation, DeploymentListResponse, ServiceListResponse,
    StackDeploymentListResponse, StatusResponse, TaskListResponse,
};
use swarmlite_protocol::data_plane::{DataFrame, DataFrameKind};
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::Message;

use crate::{
    ConnectionArgs,
    connection::{self, ControllerConnection},
};

mod commands;
mod details;
mod inspect;
mod resources;

include!(concat!(env!("OUT_DIR"), "/ui_assets.rs"));

#[derive(Debug, Args)]
pub(super) struct UiArgs {
    #[command(flatten)]
    connection: ConnectionArgs,
    /// Local dashboard port (0 selects an available port).
    #[arg(long, default_value_t = 0)]
    port: u16,
    /// Print the URL without opening a browser.
    #[arg(long)]
    no_open: bool,
}

struct UiState {
    connection: ControllerConnection,
    data_dir: std::path::PathBuf,
    command_controller: Option<String>,
    command_token: Option<String>,
    runs: commands::Runs,
    authority: String,
    session: String,
    shutdown: watch::Receiver<bool>,
}

pub(super) async fn run(data_dir: &Path, options: UiArgs) -> Result<()> {
    let command_controller = options.connection.controller.clone();
    let command_token = options.connection.token.clone();
    let connection = connection::resolve(
        data_dir,
        options.connection.controller,
        options.connection.token,
    )
    .await?;
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, options.port))
        .await
        .context("failed to bind local UI port")?;
    let address = listener.local_addr()?;
    let session = uuid::Uuid::new_v4().simple().to_string();
    let url = format!("http://{address}/#session={session}");
    let (shutdown, receiver) = watch::channel(false);
    let state = Arc::new(UiState {
        connection,
        data_dir: data_dir.to_owned(),
        command_controller,
        command_token,
        runs: commands::Runs::default(),
        authority: address.to_string(),
        session,
        shutdown: receiver,
    });
    println!(
        "Swarmlite UI: {url}\nManagement session. Keep this process running; press Ctrl-C to close it."
    );
    if !options.no_open {
        tokio::spawn(open_browser(url));
    }
    let result = axum::serve(listener, router(state.clone()))
        .with_graceful_shutdown(async move {
            shutdown_signal().await;
            let _ = shutdown.send(true);
        })
        .await
        .context("local UI server failed");
    commands::finish(&state).await;
    result
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        if let Ok(mut terminate) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = terminate.recv() => {} }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}

async fn open_browser(url: String) {
    let program = if cfg!(target_os = "macos") {
        "open"
    } else {
        "xdg-open"
    };
    let result = tokio::process::Command::new(program)
        .arg(url)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .await;
    if !matches!(result, Ok(status) if status.success()) {
        eprintln!("Could not open a browser automatically. Open the printed URL manually.");
    }
}

fn router(state: Arc<UiState>) -> Router {
    let api = Router::new()
        .route("/api/overview", get(overview))
        .route("/api/services", get(services))
        .route("/api/deployments", get(deployments))
        .route("/api/stacks/{name}/deployments", get(stack_deployments))
        .route("/api/services/{target}/tasks", get(tasks))
        .route("/api/logs", get(logs))
        .merge(details::routes())
        .merge(inspect::routes())
        .merge(resources::routes())
        .merge(commands::routes())
        .route_layer(middleware::from_fn_with_state(state.clone(), authorize));
    api.fallback(asset)
        .layer(middleware::from_fn_with_state(state.clone(), local_request))
        .with_state(state)
}

async fn local_request(
    State(state): State<Arc<UiState>>,
    request: Request,
    next: Next,
) -> Response {
    if !valid_source(request.headers(), &state.authority) {
        return StatusCode::FORBIDDEN.into_response();
    }
    let mut response = next.run(request).await;
    let headers = response.headers_mut();
    headers.insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
    headers.insert(header::X_CONTENT_TYPE_OPTIONS, "nosniff".parse().unwrap());
    headers.insert(header::REFERRER_POLICY, "no-referrer".parse().unwrap());
    headers.insert(header::CONTENT_SECURITY_POLICY, "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'".parse().unwrap());
    response
}

fn valid_source(headers: &HeaderMap, authority: &str) -> bool {
    headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        == Some(authority)
        && headers
            .get(header::ORIGIN)
            .is_none_or(|value| value == format!("http://{authority}").as_str())
        && headers
            .get("sec-fetch-site")
            .is_none_or(|value| value != "cross-site")
}

async fn authorize(State(state): State<Arc<UiState>>, request: Request, next: Next) -> Response {
    let supplied = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .unwrap_or_default();
    if !equal_secret(supplied.as_bytes(), state.session.as_bytes()) {
        return (StatusCode::UNAUTHORIZED, Json(json!({"error": "Open the full URL printed by swarmlite ui to connect this browser."}))).into_response();
    }
    if request.method() != axum::http::Method::GET
        && request.method() != axum::http::Method::HEAD
        && !request
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| {
                v.split(';')
                    .next()
                    .is_some_and(|mime| mime.trim() == "application/json")
            })
    {
        return (
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            Json(json!({"error":"Operations require an application/json request."})),
        )
            .into_response();
    }
    next.run(request).await
}

fn equal_secret(left: &[u8], right: &[u8]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .fold(0u8, |diff, (a, b)| diff | (a ^ b))
            == 0
}

async fn asset(request: Request) -> Response {
    if request.method() != axum::http::Method::GET && request.method() != axum::http::Method::HEAD {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }
    let path = if request.uri().path() == "/" {
        "/index.html"
    } else {
        request.uri().path()
    };
    let Some((_, bytes)) = ASSETS.iter().find(|(url, _)| *url == path) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let mime = match path.rsplit('.').next().unwrap_or_default() {
        "html" => "text/html; charset=utf-8",
        "js" => "text/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "ico" => "image/x-icon",
        "woff2" => "font/woff2",
        _ => "application/octet-stream",
    };
    ([(header::CONTENT_TYPE, mime)], *bytes).into_response()
}

type ApiResult<T> = Result<Json<T>, ApiError>;
#[derive(Debug)]
struct ApiError(StatusCode, String);
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({"error": self.1}))).into_response()
    }
}
impl From<ControllerClientError> for ApiError {
    fn from(error: ControllerClientError) -> Self {
        match error {
            ControllerClientError::Http {status, message, ..} => Self(status, message),
            _ => Self(StatusCode::BAD_GATEWAY, "Controller is unreachable or returned an invalid response. Check the CLI connection.".into()),
        }
    }
}
async fn bounded<T>(
    future: impl std::future::Future<Output = Result<T, ControllerClientError>>,
) -> Result<T, ApiError> {
    tokio::time::timeout(Duration::from_secs(15), future)
        .await
        .map_err(|_| {
            ApiError(
                StatusCode::GATEWAY_TIMEOUT,
                "Controller request timed out.".into(),
            )
        })?
        .map_err(Into::into)
}

async fn overview(State(state): State<Arc<UiState>>) -> ApiResult<Value> {
    let status: StatusResponse = bounded(state.connection.get_json("/v1/status")).await?;
    // Deliberately project only dashboard fields, never credentials or workload environment values.
    let nodes: Vec<_> = status.state.members.values().map(|member| json!({
        "id": member.id, "address": member.address, "gateway_enabled": member.gateway_enabled,
        "version": status.state.nodes.get(&member.id).and_then(|node| node.swarmlite_version.as_ref()),
    })).collect();
    Ok(Json(
        json!({"cluster_id": status.cluster_id, "controller_id": status.controller_id,
        "generation": status.generation, "gateway": status.gateway, "recovery": status.recovery, "nodes": nodes}),
    ))
}
async fn services(State(state): State<Arc<UiState>>) -> ApiResult<ServiceListResponse> {
    Ok(Json(
        bounded(state.connection.get_json("/v1/services")).await?,
    ))
}
async fn deployments(State(state): State<Arc<UiState>>) -> ApiResult<DeploymentListResponse> {
    Ok(Json(
        bounded(state.connection.get_json("/v1/deployments")).await?,
    ))
}
fn encode(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}
async fn tasks(
    State(state): State<Arc<UiState>>,
    AxumPath(target): AxumPath<String>,
) -> ApiResult<TaskListResponse> {
    Ok(Json(
        bounded(
            state
                .connection
                .get_json(&format!("/v1/services/{}/tasks", encode(&target))),
        )
        .await?,
    ))
}
async fn stack_deployments(
    State(state): State<Arc<UiState>>,
    AxumPath(name): AxumPath<String>,
) -> ApiResult<StackDeploymentListResponse> {
    Ok(Json(
        bounded(
            state
                .connection
                .get_json(&format!("/v1/stacks/{}/deployments", encode(&name))),
        )
        .await?,
    ))
}

#[derive(Deserialize)]
struct LogQuery {
    target: String,
    #[serde(default = "default_tail")]
    tail: u32,
    #[serde(default)]
    follow: bool,
}
fn default_tail() -> u32 {
    200
}
async fn logs(
    State(state): State<Arc<UiState>>,
    Query(query): Query<LogQuery>,
) -> Result<Response, ApiError> {
    if query.target.is_empty() || query.target.len() > 512 || query.tail > 10_000 {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "A target and tail between 0 and 10000 are required.".into(),
        ));
    }
    let session: DataSessionCreateResponse = bounded(state.connection.send_json(
        reqwest::Method::POST,
        "/v1/data-sessions",
        Some(&DataSessionOperation::Logs {
            target: query.target,
            tail: query.tail,
            follow: query.follow,
        }),
    ))
    .await?;
    let mut socket = tokio::time::timeout(
        Duration::from_secs(15),
        state.connection.connect_data_websocket(
            &format!("/v1/data-sessions/{}/client", encode(&session.session_id)),
            &session.attach_token,
        ),
    )
    .await
    .map_err(|_| {
        ApiError(
            StatusCode::GATEWAY_TIMEOUT,
            "Log connection timed out.".into(),
        )
    })?
    .map_err(|_| {
        ApiError(
            StatusCode::BAD_GATEWAY,
            "Could not connect to the Controller log stream.".into(),
        )
    })?;
    let mut shutdown = state.shutdown.clone();
    let stream = async_stream::stream! {
        yield Ok::<_, Infallible>(format!("{}\n", json!({"type": "streams", "streams": session.streams})));
        let expected: BTreeSet<_> = session.streams.iter().map(|stream| stream.stream_id).collect();
        let mut ended = BTreeSet::new();
        let mut sequences = BTreeMap::new();
        while ended.len() < expected.len() {
            let message = tokio::select! { _ = shutdown.changed() => break, message = socket.next() => message };
            let error = match message {
                Some(Ok(Message::Binary(bytes))) => match DataFrame::decode(&bytes) {
                    Ok(frame) if expected.contains(&frame.stream_id) && !ended.contains(&frame.stream_id)
                        && frame.sequence == *sequences.get(&frame.stream_id).unwrap_or(&0) => {
                        sequences.insert(frame.stream_id, frame.sequence.saturating_add(1));
                        let kind = match frame.kind {
                            DataFrameKind::Data => "data", DataFrameKind::Error => "error",
                            DataFrameKind::End => { ended.insert(frame.stream_id); "end" },
                            _ => { yield Ok(format!("{}\n", json!({"type":"failure", "message":"Unexpected log frame."}))); break; }
                        };
                        yield Ok(format!("{}\n", json!({"type":kind, "stream_id":frame.stream_id, "channel":frame.channel as u8, "payload":BASE64.encode(&frame.payload)})));
                        continue;
                    }
                    _ => "Invalid or out-of-order log frame.",
                },
                Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
                _ => "Log connection closed before all tasks finished streaming.",
            };
            yield Ok(format!("{}\n", json!({"type":"failure", "message":error})));
            break;
        }
        let _ = socket.close(None).await;
    };
    Ok((
        [
            (header::CONTENT_TYPE, "application/x-ndjson"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        ],
        Body::from_stream(stream),
    )
        .into_response())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{extract::WebSocketUpgrade, routing::post};
    use swarmlite_protocol::data_plane::DataChannel;

    async fn serve(app: Router) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (url, task)
    }

    async fn dashboard(controller: String) -> (String, tokio::task::JoinHandle<()>) {
        let connection = connection::resolve(
            Path::new("/unused"),
            Some(controller),
            Some("cluster-secret-for-tests".into()),
        )
        .await
        .unwrap();
        let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let (keepalive, shutdown) = watch::channel(false);
        let state = Arc::new(UiState {
            connection,
            data_dir: std::path::PathBuf::from("/unused"),
            command_controller: None,
            command_token: None,
            runs: commands::Runs::default(),
            authority: address.to_string(),
            session: "browser-session".into(),
            shutdown,
        });
        let task = tokio::spawn(async move {
            let _keepalive = keepalive;
            axum::serve(listener, router(state)).await.unwrap()
        });
        (format!("http://{address}"), task)
    }

    #[tokio::test]
    async fn local_dashboard_enforces_boundary_and_serves_embedded_assets() {
        let controller = Router::new().route(
            "/v1/services",
            get(|headers: HeaderMap| async move {
                assert_eq!(
                    headers[header::AUTHORIZATION],
                    "Bearer cluster-secret-for-tests"
                );
                Json(json!({"services": []}))
            }),
        );
        let (controller, upstream) = serve(controller).await;
        let (ui, server) = dashboard(controller).await;
        let http = reqwest::Client::new();
        let index = http.get(&ui).send().await.unwrap();
        assert_eq!(index.status(), StatusCode::OK);
        assert_eq!(index.headers()[header::CACHE_CONTROL], "no-store");
        assert!(
            index.headers()[header::CONTENT_SECURITY_POLICY]
                .to_str()
                .unwrap()
                .contains("frame-ancestors 'none'")
        );
        let html = index.text().await.unwrap();
        assert!(html.contains("Swarmlite"));
        for (asset, _) in ASSETS
            .iter()
            .filter(|(path, _)| path.starts_with("/assets/"))
        {
            assert_eq!(
                http.get(format!("{ui}{asset}"))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                StatusCode::OK
            );
        }
        assert!(!html.contains("browser-session"));
        assert_eq!(
            http.get(format!("{ui}/api/services"))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        let authorized = http
            .get(format!("{ui}/api/services"))
            .bearer_auth("browser-session")
            .send()
            .await
            .unwrap();
        assert_eq!(authorized.status(), StatusCode::OK);
        assert_eq!(
            authorized.json::<Value>().await.unwrap(),
            json!({"services": []})
        );
        for (header, value) in [
            ("origin", "https://evil.example"),
            ("host", "evil.example"),
            ("sec-fetch-site", "cross-site"),
        ] {
            assert_eq!(
                http.get(format!("{ui}/api/services"))
                    .header(header, value)
                    .bearer_auth("browser-session")
                    .send()
                    .await
                    .unwrap()
                    .status(),
                StatusCode::FORBIDDEN
            );
        }
        assert_eq!(
            http.post(format!("{ui}/api/services"))
                .bearer_auth("browser-session")
                .json(&json!({}))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::METHOD_NOT_ALLOWED
        );
        assert_eq!(
            http.get(format!("{ui}/api/cluster"))
                .bearer_auth("browser-session")
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND
        );
        server.abort();
        upstream.abort();
    }

    fn sample_deployment() -> Value {
        json!({"stack":"demo", "generation":42, "revision":1, "status":"reconciling", "started_at_unix_ms":1, "last_progress_at_unix_ms":1, "progress_deadline_seconds":300, "services":[]})
    }

    #[tokio::test]
    async fn ui_rejects_non_allowlisted_writes_without_forwarding() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();
        let controller = Router::new().fallback(move || {
            counter.fetch_add(1, Ordering::SeqCst);
            async { StatusCode::OK }
        });
        let (controller, upstream) = serve(controller).await;
        let (ui, server) = dashboard(controller).await;
        let http = reqwest::Client::new();
        for path in [
            "/services/demo.web/scale",
            "/services/demo.web/restart",
            "/stacks/demo/retry",
            "/stacks/demo/rollback",
            "/jobs/demo.backup/run",
            "/job-tasks/task/cancel",
            "/overview",
            "/jobs/demo.backup/history",
        ] {
            for method in [
                reqwest::Method::POST,
                reqwest::Method::PUT,
                reqwest::Method::PATCH,
                reqwest::Method::DELETE,
            ] {
                let response = http
                    .request(method, format!("{ui}/api{path}"))
                    .bearer_auth("browser-session")
                    .json(&json!({"replicas":0}))
                    .send()
                    .await
                    .unwrap();
                assert!(matches!(
                    response.status(),
                    StatusCode::METHOD_NOT_ALLOWED | StatusCode::NOT_FOUND
                ));
            }
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        server.abort();
        upstream.abort();
    }

    #[tokio::test]
    async fn deployment_detail_forwards_selected_generation() {
        let controller = Router::new().route(
            "/v1/stacks/demo/deployment",
            get(
                |Query(query): Query<std::collections::HashMap<String, String>>| async move {
                    assert_eq!(query.get("generation").map(String::as_str), Some("42"));
                    Json(sample_deployment())
                },
            ),
        );
        let (controller, upstream) = serve(controller).await;
        let (ui, server) = dashboard(controller).await;
        let response = reqwest::Client::new()
            .get(format!("{ui}/api/stacks/demo/deployment?generation=42"))
            .bearer_auth("browser-session")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.json::<Value>().await.unwrap()["generation"], 42);
        server.abort();
        upstream.abort();
    }

    #[tokio::test]
    async fn logs_translate_frames_without_exposing_controller_tokens() {
        let controller = Router::new()
            .route("/v1/data-sessions", post(|headers: HeaderMap, Json(body): Json<Value>| async move {
                assert_eq!(headers[header::AUTHORIZATION], "Bearer cluster-secret-for-tests");
                assert_eq!(body, json!({"operation":"logs", "target":"demo.web", "tail":200, "follow":false}));
                Json(json!({"session_id":"logs", "attach_token":"private-attach-token", "streams":[{"stream_id":1,"task_id":"task","node_id":"node","stack":"demo","service":"web","slot":0}]}))
            }))
            .route("/v1/data-sessions/logs/client", get(|headers: HeaderMap, ws: WebSocketUpgrade| async move {
                assert_eq!(headers[header::AUTHORIZATION], "Bearer private-attach-token");
                ws.on_upgrade(|mut socket| async move {
                    for frame in [DataFrame::data(1, 0, DataChannel::Stdout, "hello\n"), DataFrame::error(1, 1, "task failed"), DataFrame::end(1, 2)] {
                        socket.send(axum::extract::ws::Message::Binary(frame.encode().unwrap().into())).await.unwrap();
                    }
                })
            }));
        let (controller, upstream) = serve(controller).await;
        let (ui, server) = dashboard(controller).await;
        let response = reqwest::Client::new()
            .get(format!("{ui}/api/logs?target=demo.web"))
            .bearer_auth("browser-session")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let text = response.text().await.unwrap();
        assert!(!text.contains("private-attach-token"));
        assert!(!text.contains("cluster-secret"));
        let frames: Vec<Value> = text
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(frames.len(), 4);
        assert_eq!(frames[0]["type"], "streams");
        assert_eq!(frames[1]["type"], "data");
        assert_eq!(
            BASE64
                .decode(frames[1]["payload"].as_str().unwrap())
                .unwrap(),
            b"hello\n"
        );
        assert_eq!(frames[2]["type"], "error");
        assert_eq!(frames[3]["type"], "end");
        server.abort();
        upstream.abort();
    }
}
