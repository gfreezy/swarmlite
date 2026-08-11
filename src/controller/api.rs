use std::sync::Arc;

use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, Query, State, WebSocketUpgrade},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use serde::Deserialize;
use serde_json::json;
use tracing::error;

use crate::model::{
    AgentCommandAck, AgentCommandPollResponse, AgentCommandResult, BootstrapResponse,
    ClusterConfigResponse, ClusterConfigUpdate, DataSessionCreateResponse, DataSessionOperation,
    HeartbeatResponse, JoinRequest, JoinResponse, KvDeleteRequest, KvListResponse,
    KvLockAcquireRequest, KvLockAcquireResponse, KvLockMutationRequest, KvObjectResponse,
    KvPutRequest, KvStatResponse, NodeGatewayResponse, NodeGatewayUpdate, NodeHeartbeat,
    NodeLabelRemoveRequest, NodeLabelSetRequest, NodeLabelsResponse, RegistryLoginRequest,
    RegistryLoginResponse, ServiceInspectResponse, ServiceListResponse, ServiceScaleRequest,
    StackDeploymentResponse, StackListResponse, StatusResponse, TaskListResponse,
};
use swarmlite_stack::parse_stack;

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
        .route("/v1/stacks/{name}/tasks", get(stack_tasks))
        .route("/v1/stacks/{name}/deployment", get(stack_deployment))
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
    let yaml =
        std::str::from_utf8(&body).map_err(|error| ControllerError::Invalid(error.to_string()))?;
    let parsed = parse_stack(yaml).map_err(|error| ControllerError::Invalid(error.to_string()))?;
    controller.apply(&name, parsed).await.map(Json)
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
    target: String,
}

async fn target_tasks(
    State(controller): State<Arc<Controller>>,
    Query(query): Query<TaskTargetQuery>,
    headers: HeaderMap,
) -> Result<Json<TaskListResponse>, ControllerError> {
    require_auth(&controller, &headers)?;
    controller.target_tasks(&query.target).await.map(Json)
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
