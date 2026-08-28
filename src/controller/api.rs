use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, Query, State, WebSocketUpgrade},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use serde::Deserialize;
use serde_json::json;
use tracing::error;

use crate::model::{
    AgentCommandAck, AgentCommandPollResponse, AgentCommandResult, BootstrapResponse,
    ClusterConfigResponse, ClusterConfigUpdate, ConfigBlobCheckRequest, ConfigBlobCheckResponse,
    DataSessionCreateResponse, DataSessionOperation, HeartbeatResponse, JoinRequest, JoinResponse,
    KvDeleteRequest, KvListResponse, KvLockAcquireRequest, KvLockAcquireResponse,
    KvLockMutationRequest, KvObjectResponse, KvPutRequest, KvStatResponse, MAX_CONFIG_FILE_BYTES,
    MAX_STACK_CONFIG_BYTES, NodeGatewayResponse, NodeGatewayUpdate, NodeHeartbeat,
    NodeLabelRemoveRequest, NodeLabelSetRequest, NodeLabelsResponse, RegistryCredential,
    RegistryLoginRequest, RegistryLoginResponse, ServiceInspectResponse, ServiceListResponse,
    ServiceScaleRequest, StackApplyRequest, StackDeploymentResponse, StackListResponse,
    StackValidationResponse, StatusResponse, TaskListResponse,
};
use swarmlite_stack::{config_digest, parse_stack_document, resolve_config_digests};

use super::{Controller, ControllerError};

pub(super) fn router(controller: Arc<Controller>) -> Router {
    Router::new()
        .route("/healthz", get(health))
        .route("/v1/status", get(status))
        .route("/v1/cluster", get(bootstrap))
        .route(
            "/v1/config",
            get(get_cluster_config).patch(update_cluster_config),
        )
        .route("/v1/registry-credentials", put(set_registry_credential))
        .route("/v1/stacks", get(list_stacks))
        .route("/v1/stacks/{name}", put(apply_stack).delete(remove_stack))
        .route("/v1/stacks/{name}/validate", put(validate_stack))
        .route("/v1/stacks/{name}/tasks", get(stack_tasks))
        .route("/v1/stacks/{name}/deployment", get(stack_deployment))
        .route("/v1/configs/check", post(check_config_blobs))
        .route("/v1/configs/{digest}", get(get_config_blob))
        .route("/v1/services", get(list_services))
        .route("/v1/services/{target}", get(inspect_service))
        .route("/v1/services/{target}/tasks", get(service_tasks))
        .route("/v1/services/{target}/scale", post(scale_service))
        .route(
            "/v1/services/{target}/force-update",
            post(force_update_service),
        )
        .route("/v1/tasks", get(target_tasks))
        .route("/v1/data-sessions", post(create_data_session))
        .route(
            "/v1/data-sessions/{session_id}/client",
            get(attach_data_client),
        )
        .route(
            "/v1/data-sessions/{session_id}/nodes/{node_id}",
            get(attach_data_agent),
        )
        .route("/v1/nodes/{node_id}/join", put(join_node))
        .route("/v1/nodes/{node_id}/heartbeat", post(heartbeat))
        .route("/v1/nodes/{node_id}/commands", get(next_agent_command))
        .route(
            "/v1/nodes/{node_id}/commands/{command_id}/result",
            post(agent_command_result),
        )
        .route(
            "/v1/nodes/{node_id}/gateway",
            get(get_node_gateway).put(update_node_gateway),
        )
        .route(
            "/v1/nodes/{node_id}/labels",
            get(get_node_labels)
                .put(set_node_label)
                .delete(remove_node_label),
        )
        .route("/v1/kv", get(kv_object).put(put_kv).delete(delete_kv))
        .route("/v1/kv/keys", get(list_kv))
        .route("/v1/kv/stat", get(stat_kv))
        .route("/v1/kv/locks/acquire", post(acquire_kv_lock))
        .route("/v1/kv/locks/renew", post(renew_kv_lock))
        .route("/v1/kv/locks/release", post(release_kv_lock))
        .with_state(controller)
}

async fn health(State(controller): State<Arc<Controller>>) -> Json<serde_json::Value> {
    let status = controller.status().await;
    Json(json!({
        "ok": true,
        "controller_id": controller.config.cluster.controller_id,
        "generation": status.generation,
    }))
}

async fn status(
    State(controller): State<Arc<Controller>>,
    headers: HeaderMap,
) -> Result<Json<StatusResponse>, ControllerError> {
    require_auth(&controller, &headers)?;
    Ok(Json(controller.status().await))
}

async fn bootstrap(
    State(controller): State<Arc<Controller>>,
    headers: HeaderMap,
) -> Result<Json<BootstrapResponse>, ControllerError> {
    require_auth(&controller, &headers)?;
    controller.bootstrap().await.map(Json)
}

async fn get_cluster_config(
    State(controller): State<Arc<Controller>>,
    headers: HeaderMap,
) -> Result<Json<ClusterConfigResponse>, ControllerError> {
    require_auth(&controller, &headers)?;
    controller.get_cluster_config().await.map(Json)
}

async fn update_cluster_config(
    State(controller): State<Arc<Controller>>,
    headers: HeaderMap,
    Json(update): Json<ClusterConfigUpdate>,
) -> Result<Json<ClusterConfigResponse>, ControllerError> {
    require_auth(&controller, &headers)?;
    controller.update_cluster_config(update).await.map(Json)
}

async fn set_registry_credential(
    State(controller): State<Arc<Controller>>,
    headers: HeaderMap,
    Json(request): Json<RegistryLoginRequest>,
) -> Result<Json<RegistryLoginResponse>, ControllerError> {
    require_auth(&controller, &headers)?;
    controller.set_registry_credential(request).await.map(Json)
}

async fn join_node(
    State(controller): State<Arc<Controller>>,
    Path(node_id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<JoinRequest>,
) -> Result<Json<JoinResponse>, ControllerError> {
    require_auth(&controller, &headers)?;
    controller.join_node(&node_id, body).await.map(Json)
}

async fn get_node_gateway(
    State(controller): State<Arc<Controller>>,
    Path(node_id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<NodeGatewayResponse>, ControllerError> {
    require_auth(&controller, &headers)?;
    controller.node_gateway(&node_id).await.map(Json)
}

async fn update_node_gateway(
    State(controller): State<Arc<Controller>>,
    Path(node_id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<NodeGatewayUpdate>,
) -> Result<Json<NodeGatewayResponse>, ControllerError> {
    require_auth(&controller, &headers)?;
    controller
        .update_node_gateway(&node_id, body)
        .await
        .map(Json)
}

async fn get_node_labels(
    State(controller): State<Arc<Controller>>,
    Path(node_id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<NodeLabelsResponse>, ControllerError> {
    require_auth(&controller, &headers)?;
    controller.node_labels(&node_id).await.map(Json)
}

async fn set_node_label(
    State(controller): State<Arc<Controller>>,
    Path(node_id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<NodeLabelSetRequest>,
) -> Result<Json<NodeLabelsResponse>, ControllerError> {
    require_auth(&controller, &headers)?;
    controller.set_node_label(&node_id, body).await.map(Json)
}

async fn remove_node_label(
    State(controller): State<Arc<Controller>>,
    Path(node_id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<NodeLabelRemoveRequest>,
) -> Result<Json<NodeLabelsResponse>, ControllerError> {
    require_auth(&controller, &headers)?;
    controller.remove_node_label(&node_id, body).await.map(Json)
}

async fn apply_stack(
    State(controller): State<Arc<Controller>>,
    Path(name): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<StackDeploymentResponse>, ControllerError> {
    require_auth(&controller, &headers)?;
    let request = parse_stack_apply_body(&headers, &body)?;
    let PreparedStackApply {
        stack: parsed,
        blobs,
        registry_credentials,
    } = prepare_stack_apply(request, &controller.repository)?;
    let config_digests = parsed
        .services
        .values()
        .flat_map(|service| service.configs.iter().map(|config| config.digest.clone()))
        .collect::<BTreeSet<_>>();
    controller.repository.put_config_blobs(&blobs)?;
    controller.repository.pin_config_blobs(&config_digests)?;
    controller
        .apply_with_registry_credentials(&name, parsed, registry_credentials)
        .await
        .map(Json)
}

async fn validate_stack(
    State(controller): State<Arc<Controller>>,
    Path(name): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<StackValidationResponse>, ControllerError> {
    require_auth(&controller, &headers)?;
    let request = parse_stack_apply_body(&headers, &body)?;
    let PreparedStackApply { stack: parsed, .. } =
        prepare_stack_apply(request, &controller.repository)?;
    controller.validate_apply(&name, &parsed).await?;
    Ok(Json(StackValidationResponse {
        stack: name,
        valid: true,
    }))
}

async fn get_config_blob(
    State(controller): State<Arc<Controller>>,
    Path(digest): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ControllerError> {
    require_auth(&controller, &headers)?;
    let digest = normalize_config_digest(&digest)?;
    let contents = controller
        .repository
        .get_config_blob(&digest)?
        .ok_or_else(|| ControllerError::NotFound(format!("config {digest:?} not found")))?;
    let mut response = Response::new(axum::body::Body::from(contents));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("private, immutable"),
    );
    Ok(response)
}

async fn check_config_blobs(
    State(controller): State<Arc<Controller>>,
    headers: HeaderMap,
    Json(request): Json<ConfigBlobCheckRequest>,
) -> Result<Json<ConfigBlobCheckResponse>, ControllerError> {
    require_auth(&controller, &headers)?;
    let mut missing = std::collections::BTreeSet::new();
    for digest in request.digests {
        let digest = normalize_config_digest(&digest)?;
        if controller.repository.config_blob_size(&digest)?.is_none() {
            missing.insert(digest);
        }
    }
    Ok(Json(ConfigBlobCheckResponse { missing }))
}

fn normalize_config_digest(digest: &str) -> Result<String, ControllerError> {
    if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ControllerError::Invalid(
            "config digest must be a 64-character SHA-256 value".into(),
        ));
    }
    Ok(digest.to_ascii_lowercase())
}

fn parse_stack_apply_body(
    headers: &HeaderMap,
    body: &[u8],
) -> Result<StackApplyRequest, ControllerError> {
    let is_json = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("application/json"));
    if is_json {
        serde_json::from_slice(body).map_err(|error| ControllerError::Invalid(error.to_string()))
    } else {
        let yaml = std::str::from_utf8(body)
            .map_err(|error| ControllerError::Invalid(error.to_string()))?;
        Ok(StackApplyRequest {
            yaml: yaml.to_owned(),
            configs: Default::default(),
        })
    }
}

#[derive(Debug)]
struct PreparedStackApply {
    stack: swarmlite_stack::ParsedStack,
    blobs: BTreeMap<String, Vec<u8>>,
    registry_credentials: BTreeMap<String, RegistryCredential>,
}

fn prepare_stack_apply(
    request: StackApplyRequest,
    repository: &crate::storage::StateRepository,
) -> Result<PreparedStackApply, ControllerError> {
    let document = parse_stack_document(&request.yaml)
        .map_err(|error| ControllerError::Invalid(format!("{error:#}")))?;
    let registry_credentials = crate::registry::validate_stack_credentials(document.registries)
        .map_err(|error| ControllerError::Invalid(format!("{error:#}")))?;
    for name in request.configs.keys() {
        if !document.configs.contains_key(name) {
            return Err(ControllerError::Invalid(format!(
                "uploaded config {name:?} is not declared by the Stack"
            )));
        }
    }

    let mut blobs = BTreeMap::new();
    for (name, payload) in &request.configs {
        let digest = normalize_config_digest(&payload.digest)?;
        let Some(data_base64) = &payload.data_base64 else {
            continue;
        };
        let contents = BASE64_STANDARD.decode(data_base64).map_err(|_| {
            ControllerError::Invalid(format!("config {name:?} is not valid Base64"))
        })?;
        if config_digest(&contents) != digest {
            return Err(ControllerError::Invalid(format!(
                "config {name:?} contents do not match digest {digest}"
            )));
        }
        if contents.len() > MAX_CONFIG_FILE_BYTES {
            return Err(ControllerError::Invalid(format!(
                "config {name:?} contains {} bytes; each config may contain at most {MAX_CONFIG_FILE_BYTES} bytes",
                contents.len()
            )));
        }
        match blobs.entry(digest) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(contents);
            }
            std::collections::btree_map::Entry::Occupied(entry) if entry.get() != &contents => {
                return Err(ControllerError::Invalid(format!(
                    "multiple config payloads claim digest {:?} with different contents",
                    entry.key()
                )));
            }
            std::collections::btree_map::Entry::Occupied(_) => {}
        }
    }

    let mut total_bytes = 0_usize;
    let mut digests = BTreeMap::new();
    for name in document.configs.keys() {
        let payload = request.configs.get(name).ok_or_else(|| {
            ControllerError::Invalid(format!(
                "config {name:?} has no local digest; deploy with a current Swarmlite CLI"
            ))
        })?;
        let digest = normalize_config_digest(&payload.digest)?;
        let size = match blobs.get(&digest) {
            Some(contents) => contents.len(),
            None => repository.config_blob_size(&digest)?.ok_or_else(|| {
                ControllerError::Invalid(format!(
                    "config {name:?} digest {digest} is missing from the Controller; retry the deployment so the CLI uploads it"
                ))
            })?,
        };
        if size > MAX_CONFIG_FILE_BYTES {
            return Err(ControllerError::Invalid(format!(
                "config {name:?} contains {size} bytes; each config may contain at most {MAX_CONFIG_FILE_BYTES} bytes"
            )));
        }
        total_bytes = total_bytes
            .checked_add(size)
            .ok_or_else(|| ControllerError::Invalid("total config size overflow".into()))?;
        if total_bytes > MAX_STACK_CONFIG_BYTES {
            return Err(ControllerError::Invalid(format!(
                "Stack configs contain {total_bytes} bytes; at most {MAX_STACK_CONFIG_BYTES} bytes may be uploaded per deployment"
            )));
        }
        digests.insert(name.clone(), digest);
    }

    let mut stack = document.stack;
    resolve_config_digests(&mut stack, &digests)
        .map_err(|error| ControllerError::Invalid(format!("{error:#}")))?;
    Ok(PreparedStackApply {
        stack,
        blobs,
        registry_credentials,
    })
}

async fn list_stacks(
    State(controller): State<Arc<Controller>>,
    headers: HeaderMap,
) -> Result<Json<StackListResponse>, ControllerError> {
    require_auth(&controller, &headers)?;
    Ok(Json(controller.list_stacks().await))
}

async fn remove_stack(
    State(controller): State<Arc<Controller>>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> Result<Json<StackDeploymentResponse>, ControllerError> {
    require_auth(&controller, &headers)?;
    controller.remove_stack(&name).await.map(Json)
}

async fn stack_tasks(
    State(controller): State<Arc<Controller>>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> Result<Json<TaskListResponse>, ControllerError> {
    require_auth(&controller, &headers)?;
    controller.stack_tasks(&name).await.map(Json)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ServiceListQuery {
    #[serde(default)]
    stack: Option<String>,
}

async fn list_services(
    State(controller): State<Arc<Controller>>,
    Query(query): Query<ServiceListQuery>,
    headers: HeaderMap,
) -> Result<Json<ServiceListResponse>, ControllerError> {
    require_auth(&controller, &headers)?;
    controller
        .list_services(query.stack.as_deref())
        .await
        .map(Json)
}

async fn inspect_service(
    State(controller): State<Arc<Controller>>,
    Path(target): Path<String>,
    headers: HeaderMap,
) -> Result<Json<ServiceInspectResponse>, ControllerError> {
    require_auth(&controller, &headers)?;
    controller.inspect_service(&target).await.map(Json)
}

async fn service_tasks(
    State(controller): State<Arc<Controller>>,
    Path(target): Path<String>,
    headers: HeaderMap,
) -> Result<Json<TaskListResponse>, ControllerError> {
    require_auth(&controller, &headers)?;
    controller.service_tasks(&target).await.map(Json)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskTargetQuery {
    #[serde(default)]
    target: Option<String>,
}

async fn target_tasks(
    State(controller): State<Arc<Controller>>,
    Query(query): Query<TaskTargetQuery>,
    headers: HeaderMap,
) -> Result<Json<TaskListResponse>, ControllerError> {
    require_auth(&controller, &headers)?;
    match query.target {
        Some(target) => controller.target_tasks(&target).await.map(Json),
        None => Ok(Json(controller.list_tasks().await)),
    }
}

async fn scale_service(
    State(controller): State<Arc<Controller>>,
    Path(target): Path<String>,
    headers: HeaderMap,
    Json(body): Json<ServiceScaleRequest>,
) -> Result<Json<StackDeploymentResponse>, ControllerError> {
    require_auth(&controller, &headers)?;
    controller
        .scale_service(&target, body.replicas)
        .await
        .map(Json)
}

async fn force_update_service(
    State(controller): State<Arc<Controller>>,
    Path(target): Path<String>,
    headers: HeaderMap,
) -> Result<Json<StackDeploymentResponse>, ControllerError> {
    require_auth(&controller, &headers)?;
    controller.force_update_service(&target).await.map(Json)
}

async fn create_data_session(
    State(controller): State<Arc<Controller>>,
    headers: HeaderMap,
    Json(operation): Json<DataSessionOperation>,
) -> Result<Json<DataSessionCreateResponse>, ControllerError> {
    require_auth(&controller, &headers)?;
    controller.create_data_session(operation).await.map(Json)
}

async fn attach_data_client(
    State(controller): State<Arc<Controller>>,
    Path(session_id): Path<String>,
    headers: HeaderMap,
    websocket: WebSocketUpgrade,
) -> Result<Response, ControllerError> {
    let token = bearer_token(&headers).ok_or(ControllerError::Unauthorized)?;
    let attachment = controller.sessions.attach_client(&session_id, token)?;
    Ok(websocket
        .max_message_size(crate::data_plane::MAX_DATA_FRAME_BYTES)
        .on_upgrade(move |socket| attachment.serve(socket)))
}

async fn attach_data_agent(
    State(controller): State<Arc<Controller>>,
    Path((session_id, node_id)): Path<(String, String)>,
    headers: HeaderMap,
    websocket: WebSocketUpgrade,
) -> Result<Response, ControllerError> {
    let token = bearer_token(&headers).ok_or(ControllerError::Unauthorized)?;
    let attachment = controller
        .sessions
        .attach_agent(&session_id, &node_id, token)?;
    Ok(websocket
        .max_message_size(crate::data_plane::MAX_DATA_FRAME_BYTES)
        .on_upgrade(move |socket| attachment.serve(socket)))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AgentCommandQuery {
    #[serde(default = "default_command_wait_seconds")]
    wait_seconds: u64,
}

fn default_command_wait_seconds() -> u64 {
    20
}

async fn next_agent_command(
    State(controller): State<Arc<Controller>>,
    Path(node_id): Path<String>,
    Query(query): Query<AgentCommandQuery>,
    headers: HeaderMap,
) -> Result<Json<AgentCommandPollResponse>, ControllerError> {
    require_auth(&controller, &headers)?;
    let command = controller
        .commands
        .next(
            &node_id,
            std::time::Duration::from_secs(query.wait_seconds.min(25)),
        )
        .await;
    Ok(Json(AgentCommandPollResponse { command }))
}

async fn agent_command_result(
    State(controller): State<Arc<Controller>>,
    Path((node_id, command_id)): Path<(String, String)>,
    headers: HeaderMap,
    Json(body): Json<AgentCommandResult>,
) -> Result<Json<AgentCommandAck>, ControllerError> {
    require_auth(&controller, &headers)?;
    let accepted = controller.commands.complete(&node_id, &command_id, body);
    Ok(Json(AgentCommandAck { accepted }))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StackDeploymentQuery {
    generation: u64,
    #[serde(default)]
    after_revision: Option<u64>,
    #[serde(default = "default_deployment_wait_seconds")]
    wait_seconds: u64,
}

fn default_deployment_wait_seconds() -> u64 {
    25
}

async fn stack_deployment(
    State(controller): State<Arc<Controller>>,
    Path(name): Path<String>,
    Query(query): Query<StackDeploymentQuery>,
    headers: HeaderMap,
) -> Result<Json<StackDeploymentResponse>, ControllerError> {
    require_auth(&controller, &headers)?;
    controller
        .wait_for_deployment(
            &name,
            query.generation,
            query.after_revision,
            std::time::Duration::from_secs(query.wait_seconds.min(30)),
        )
        .await
        .map(Json)
}

async fn heartbeat(
    State(controller): State<Arc<Controller>>,
    Path(node_id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<NodeHeartbeat>,
) -> Result<Json<HeartbeatResponse>, ControllerError> {
    require_auth(&controller, &headers)?;
    controller.heartbeat(&node_id, body).await.map(Json)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct KvKeyQuery {
    key: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct KvListQuery {
    #[serde(default)]
    prefix: String,
    #[serde(default)]
    recursive: bool,
}

async fn put_kv(
    State(controller): State<Arc<Controller>>,
    headers: HeaderMap,
    Json(body): Json<KvPutRequest>,
) -> Result<StatusCode, ControllerError> {
    require_auth(&controller, &headers)?;
    controller.put_kv(body).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn delete_kv(
    State(controller): State<Arc<Controller>>,
    headers: HeaderMap,
    Json(body): Json<KvDeleteRequest>,
) -> Result<StatusCode, ControllerError> {
    require_auth(&controller, &headers)?;
    controller.delete_kv(body).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn kv_object(
    State(controller): State<Arc<Controller>>,
    headers: HeaderMap,
    Query(query): Query<KvKeyQuery>,
) -> Result<Json<KvObjectResponse>, ControllerError> {
    require_auth(&controller, &headers)?;
    controller.kv_object(&query.key).await.map(Json)
}

async fn list_kv(
    State(controller): State<Arc<Controller>>,
    headers: HeaderMap,
    Query(query): Query<KvListQuery>,
) -> Result<Json<KvListResponse>, ControllerError> {
    require_auth(&controller, &headers)?;
    controller
        .list_kv(&query.prefix, query.recursive)
        .await
        .map(Json)
}

async fn stat_kv(
    State(controller): State<Arc<Controller>>,
    headers: HeaderMap,
    Query(query): Query<KvKeyQuery>,
) -> Result<Json<KvStatResponse>, ControllerError> {
    require_auth(&controller, &headers)?;
    controller.stat_kv(&query.key).await.map(Json)
}

async fn acquire_kv_lock(
    State(controller): State<Arc<Controller>>,
    headers: HeaderMap,
    Json(body): Json<KvLockAcquireRequest>,
) -> Result<Json<KvLockAcquireResponse>, ControllerError> {
    require_auth(&controller, &headers)?;
    controller.acquire_kv_lock(body).await.map(Json)
}

async fn renew_kv_lock(
    State(controller): State<Arc<Controller>>,
    headers: HeaderMap,
    Json(body): Json<KvLockMutationRequest>,
) -> Result<StatusCode, ControllerError> {
    require_auth(&controller, &headers)?;
    controller.renew_kv_lock(body).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn release_kv_lock(
    State(controller): State<Arc<Controller>>,
    headers: HeaderMap,
    Json(body): Json<KvLockMutationRequest>,
) -> Result<StatusCode, ControllerError> {
    require_auth(&controller, &headers)?;
    controller.release_kv_lock(body).await?;
    Ok(StatusCode::NO_CONTENT)
}

fn require_auth(controller: &Controller, headers: &HeaderMap) -> Result<(), ControllerError> {
    if controller.authorized(headers) {
        Ok(())
    } else {
        Err(ControllerError::Unauthorized)
    }
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
}

impl IntoResponse for ControllerError {
    fn into_response(self) -> Response {
        match self {
            Self::Unauthorized => (
                StatusCode::UNAUTHORIZED,
                Json(json!({"error": "unauthorized"})),
            )
                .into_response(),
            Self::Invalid(message) => {
                (StatusCode::BAD_REQUEST, Json(json!({"error": message}))).into_response()
            }
            Self::NotFound(message) => {
                (StatusCode::NOT_FOUND, Json(json!({"error": message}))).into_response()
            }
            Self::Conflict(message) => {
                (StatusCode::CONFLICT, Json(json!({"error": message}))).into_response()
            }
            Self::Storage(error) => {
                error!(%error, "controller storage request failed");
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({"error": error.to_string()})),
                )
                    .into_response()
            }
        }
    }
}

impl Controller {
    pub(super) fn authorized(&self, headers: &HeaderMap) -> bool {
        headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .is_some_and(|token| constant_time_eq(token.as_bytes(), self.token.as_bytes()))
    }
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        model::{
            CLUSTER_SCHEMA_VERSION, ClusterGatewayConfig, ClusterSettings, StackConfigPayload,
        },
        storage::StateRepository,
    };

    const YAML: &str = r#"
services:
  web:
    image: nginx
    configs:
      - source: nginx-config
        target: /etc/nginx/conf.d/default.conf
configs:
  nginx-config:
    file: ./default.conf
"#;

    fn repository() -> (tempfile::TempDir, StateRepository) {
        let directory = tempfile::tempdir().unwrap();
        let repository = StateRepository::open(
            directory.path(),
            ClusterSettings {
                schema_version: CLUSTER_SCHEMA_VERSION,
                cluster_id: "config-api-test".into(),
                controller_id: "controller-test".into(),
                controller_port: crate::config::DEFAULT_CONTROLLER_PORT,
                gateway: ClusterGatewayConfig::default(),
            },
        )
        .unwrap();
        (directory, repository)
    }

    #[test]
    fn resolves_uploaded_config_content_into_service_digest() {
        let contents = b"server { listen 80; }\n";
        let digest = config_digest(contents);
        let (_directory, repository) = repository();
        let prepared = prepare_stack_apply(
            StackApplyRequest {
                yaml: YAML.into(),
                configs: BTreeMap::from([(
                    "nginx-config".into(),
                    StackConfigPayload {
                        digest: digest.clone(),
                        data_base64: Some(BASE64_STANDARD.encode(contents)),
                    },
                )]),
            },
            &repository,
        )
        .unwrap();
        assert_eq!(prepared.stack.services["web"].configs[0].digest, digest);
        assert_eq!(prepared.blobs[&digest], contents);
        assert!(prepared.registry_credentials.is_empty());
    }

    #[test]
    fn resolves_digest_only_config_when_controller_already_has_the_blob() {
        let contents = b"server { listen 80; }\n".to_vec();
        let digest = config_digest(&contents);
        let (_directory, repository) = repository();
        repository
            .put_config_blobs(&BTreeMap::from([(digest.clone(), contents)]))
            .unwrap();

        let prepared = prepare_stack_apply(
            StackApplyRequest {
                yaml: YAML.into(),
                configs: BTreeMap::from([(
                    "nginx-config".into(),
                    StackConfigPayload {
                        digest: digest.clone(),
                        data_base64: None,
                    },
                )]),
            },
            &repository,
        )
        .unwrap();

        assert_eq!(prepared.stack.services["web"].configs[0].digest, digest);
        assert!(prepared.blobs.is_empty());
        assert!(prepared.registry_credentials.is_empty());
    }

    #[test]
    fn validates_and_normalizes_inline_registry_credentials() {
        let (_directory, repository) = repository();
        let prepared = prepare_stack_apply(
            StackApplyRequest {
                yaml: r#"
services:
  web:
    image: ghcr.io/example/private:latest
x-swarmlite:
  registries:
    GHCR.IO:
      username: octocat
      password: private-token
"#
                .into(),
                configs: BTreeMap::new(),
            },
            &repository,
        )
        .unwrap();

        assert_eq!(
            prepared.stack.services["web"].image,
            "ghcr.io/example/private:latest"
        );
        assert!(prepared.blobs.is_empty());
        assert_eq!(prepared.registry_credentials["ghcr.io"].username, "octocat");
        assert_eq!(
            prepared.registry_credentials["ghcr.io"].password,
            "private-token"
        );
    }

    #[test]
    fn registry_validation_errors_do_not_echo_the_inline_password() {
        let (_directory, repository) = repository();
        let error = prepare_stack_apply(
            StackApplyRequest {
                yaml: r#"
services:
  web:
    image: nginx
x-swarmlite:
  registries:
    https://ghcr.io:
      username: octocat
      password: do-not-echo-this-token
"#
                .into(),
                configs: BTreeMap::new(),
            },
            &repository,
        )
        .unwrap_err();

        let message = format!("{error:?}");
        assert!(message.contains("registry must be a hostname"));
        assert!(!message.contains("do-not-echo-this-token"));
    }

    #[test]
    fn rejects_config_file_that_was_not_uploaded() {
        let (_directory, repository) = repository();
        assert!(matches!(
            prepare_stack_apply(
                StackApplyRequest {
                    yaml: YAML.into(),
                    configs: BTreeMap::new(),
                },
                &repository,
            ),
            Err(ControllerError::Invalid(message)) if message.contains("no local digest")
        ));
    }

    #[test]
    fn rejects_digest_only_config_missing_from_controller_storage() {
        let (_directory, repository) = repository();
        assert!(matches!(
            prepare_stack_apply(
                StackApplyRequest {
                    yaml: YAML.into(),
                    configs: BTreeMap::from([(
                        "nginx-config".into(),
                        StackConfigPayload {
                            digest: "a".repeat(64),
                            data_base64: None,
                        },
                    )]),
                },
                &repository,
            ),
            Err(ControllerError::Invalid(message)) if message.contains("missing from the Controller")
        ));
    }
}
