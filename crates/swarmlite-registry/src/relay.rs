use std::{net::SocketAddr, sync::Arc};

use anyhow::{Context, Result};
use axum::{
    Router,
    body::Body,
    extract::{OriginalUri, State},
    http::{HeaderMap, Method, StatusCode, header},
    response::{IntoResponse, Response},
    routing::any,
};
use futures_util::TryStreamExt;
use tokio::{net::TcpListener, task::JoinHandle};
use tracing::warn;

#[derive(Clone)]
struct RelayState {
    controller: String,
    token: String,
    client: reqwest::Client,
}

pub struct RelayHandle {
    authority: String,
    task: Option<JoinHandle<()>>,
}

impl RelayHandle {
    pub fn authority(&self) -> &str {
        &self.authority
    }
}

impl Drop for RelayHandle {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort()
        }
    }
}

pub async fn spawn_relay(
    controller: impl Into<String>,
    token: impl Into<String>,
) -> Result<RelayHandle> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .context("failed to bind the local image relay")?;
    let address = listener.local_addr()?;
    let state = Arc::new(RelayState {
        controller: controller.into().trim_end_matches('/').to_owned(),
        token: token.into(),
        client: reqwest::Client::builder().no_proxy().build()?,
    });
    let app = Router::new()
        .route("/v2/", any(forward))
        .route("/v2/{*path}", any(forward))
        .with_state(state);
    let task = tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, app).await {
            warn!(%error, "local image relay stopped unexpectedly");
        }
    });
    Ok(RelayHandle {
        authority: authority(address),
        task: Some(task),
    })
}

fn authority(address: SocketAddr) -> String {
    match address {
        SocketAddr::V4(_) => address.to_string(),
        SocketAddr::V6(address) => format!("[{}]:{}", address.ip(), address.port()),
    }
}

async fn forward(
    State(state): State<Arc<RelayState>>,
    OriginalUri(uri): OriginalUri,
    method: Method,
    headers: HeaderMap,
) -> Response {
    if method != Method::GET && method != Method::HEAD {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }
    let url = format!(
        "{}{}",
        state.controller,
        uri.path_and_query()
            .map_or(uri.path(), |value| value.as_str())
    );
    let mut request = state.client.request(method, url).bearer_auth(&state.token);
    for (name, value) in &headers {
        if !hop_by_hop(name.as_str()) && name != header::HOST && name != header::AUTHORIZATION {
            request = request.header(name, value);
        }
    }
    let upstream = match request.send().await {
        Ok(response) => response,
        Err(error) => {
            return (
                StatusCode::BAD_GATEWAY,
                format!("local image relay could not reach Controller: {error}"),
            )
                .into_response();
        }
    };
    let status = upstream.status();
    let mut builder = Response::builder().status(status);
    for (name, value) in upstream.headers() {
        if !hop_by_hop(name.as_str()) {
            builder = builder.header(name, value);
        }
    }
    let stream = upstream.bytes_stream().map_err(std::io::Error::other);
    builder
        .body(Body::from_stream(stream))
        .unwrap_or_else(|error| {
            (
                StatusCode::BAD_GATEWAY,
                format!("invalid Controller response: {error}"),
            )
                .into_response()
        })
}

fn hop_by_hop(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{extract::State, http::HeaderMap, routing::get};
    use tokio::sync::oneshot;

    type ForwardedRequest = Arc<tokio::sync::Mutex<Option<oneshot::Sender<(String, String)>>>>;

    #[tokio::test]
    async fn forwards_path_and_injects_controller_token() {
        async fn upstream(
            State(tx): State<ForwardedRequest>,
            uri: OriginalUri,
            headers: HeaderMap,
        ) -> &'static str {
            let token = headers
                .get(header::AUTHORIZATION)
                .unwrap()
                .to_str()
                .unwrap()
                .to_owned();
            if let Some(tx) = tx.lock().await.take() {
                let _ = tx.send((uri.0.to_string(), token));
            }
            "ok"
        }
        let (tx, rx) = oneshot::channel();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new()
            .route("/{*path}", get(upstream))
            .with_state(Arc::new(tokio::sync::Mutex::new(Some(tx))));
        tokio::spawn(axum::serve(listener, app).into_future());
        let relay = spawn_relay(format!("http://{address}"), "cluster-secret")
            .await
            .unwrap();
        let body = reqwest::get(format!(
            "http://{}/v2/f/ghcr.io/acme/api/manifests/1.2",
            relay.authority()
        ))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
        assert_eq!(body, "ok");
        let (path, token) = rx.await.unwrap();
        assert_eq!(path, "/v2/f/ghcr.io/acme/api/manifests/1.2");
        assert_eq!(token, "Bearer cluster-secret");
    }
}
