use super::{Controller, ControllerError};
use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
};
use serde::Deserialize;
use std::sync::Arc;
use swarmlite_core::service_operations::*;

#[derive(Clone)]
struct DeployState {
    controller: Arc<Controller>,
    allow_admin: bool,
}

fn operation_router(controller: Arc<Controller>, allow_admin: bool) -> Router {
    Router::new()
        .route("/v1/deploy/services/{id}/redeploy", post(redeploy))
        .route("/v1/deploy/operations/{id}", get(operation))
        .layer(DefaultBodyLimit::max(16 * 1024))
        .with_state(DeployState {
            controller,
            allow_admin,
        })
}

pub(super) fn public_router(controller: Arc<Controller>) -> Router {
    operation_router(controller, false)
}

pub(super) fn admin_router(controller: Arc<Controller>) -> Router {
    let admin = Router::new()
        .route("/v1/deploy-tokens", post(create_token).get(list_tokens))
        .route("/v1/deploy-tokens/{id}", delete(revoke_token))
        .route("/v1/services/{id}/restart", post(restart))
        .route("/v1/operations/{id}", get(admin_operation))
        .layer(DefaultBodyLimit::max(16 * 1024))
        .with_state(DeployState {
            controller: controller.clone(),
            allow_admin: true,
        });
    admin.merge(operation_router(controller, true))
}

fn bearer(headers: &HeaderMap) -> Result<&str, ControllerError> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .ok_or(ControllerError::Unauthorized)
}
fn admin(state: &DeployState, headers: &HeaderMap) -> Result<(), ControllerError> {
    if state.controller.authorized(headers) {
        Ok(())
    } else {
        Err(ControllerError::Unauthorized)
    }
}
fn key(headers: &HeaderMap) -> Result<&str, ControllerError> {
    headers
        .get("Idempotency-Key")
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| ControllerError::Invalid("Idempotency-Key is required".into()))
}
fn no_store(value: impl serde::Serialize) -> Response {
    ([(header::CACHE_CONTROL, "no-store")], Json(value)).into_response()
}
fn public_error(error: ControllerError) -> Response {
    match error {
        ControllerError::Storage(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error":"storage_unavailable"})),
        )
            .into_response(),
        error => error.into_response(),
    }
}

async fn create_token(
    State(state): State<DeployState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ControllerError> {
    admin(&state, &headers)?;
    let request = serde_json::from_slice::<DeployTokenCreate>(&body)
        .map_err(|_| ControllerError::Invalid("invalid token request".into()))?;
    Ok(no_store(
        state.controller.create_deploy_token(request).await?,
    ))
}
#[derive(Default, Deserialize)]
struct TokenQuery {
    #[serde(default)]
    show_secret: bool,
}
async fn list_tokens(
    State(state): State<DeployState>,
    headers: HeaderMap,
    Query(query): Query<TokenQuery>,
) -> Result<Response, ControllerError> {
    admin(&state, &headers)?;
    Ok(no_store(
        state.controller.list_deploy_tokens(query.show_secret)?,
    ))
}
async fn revoke_token(
    State(state): State<DeployState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<StatusCode, ControllerError> {
    admin(&state, &headers)?;
    state.controller.revoke_deploy_token(&id).await?;
    Ok(StatusCode::NO_CONTENT)
}
async fn submit(
    state: DeployState,
    headers: HeaderMap,
    id: String,
    body: Bytes,
    kind: ServiceOperationKind,
) -> Response {
    let result = async {
        let bearer = bearer(&headers)?;
        state
            .controller
            .deploy_principal(bearer, state.allow_admin)?;
        let request = serde_json::from_slice::<RedeployRequest>(&body).map_err(|_| {
            ControllerError::Invalid("expected an empty JSON object or source metadata".into())
        })?;
        state
            .controller
            .submit_service_operation(
                bearer,
                state.allow_admin,
                &id,
                kind,
                key(&headers)?,
                request,
            )
            .await
    }
    .await;
    match result {
        Ok(operation) => {
            let location = format!("/v1/deploy/operations/{}", operation.operation_id);
            (
                StatusCode::ACCEPTED,
                [
                    (header::LOCATION, location),
                    (header::CACHE_CONTROL, "no-store".into()),
                ],
                Json(operation),
            )
                .into_response()
        }
        Err(error) => public_error(error),
    }
}
async fn redeploy(
    State(state): State<DeployState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    body: Bytes,
) -> Response {
    submit(state, headers, id, body, ServiceOperationKind::PullRedeploy).await
}
async fn restart(
    State(state): State<DeployState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    body: Bytes,
) -> Response {
    if let Err(error) = admin(&state, &headers) {
        return error.into_response();
    }
    submit(state, headers, id, body, ServiceOperationKind::Restart).await
}
#[derive(Default, Deserialize)]
struct OperationQuery {
    after_revision: Option<u64>,
    #[serde(default)]
    wait_seconds: u64,
}
async fn operation(
    State(state): State<DeployState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<OperationQuery>,
) -> Response {
    let result = async {
        state
            .controller
            .get_service_operation(
                bearer(&headers)?,
                state.allow_admin,
                &id,
                query.after_revision,
                query.wait_seconds,
            )
            .await
    }
    .await;
    match result {
        Ok(operation) => no_store(operation),
        Err(error) => public_error(error),
    }
}
async fn admin_operation(
    State(state): State<DeployState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<OperationQuery>,
) -> Response {
    if let Err(error) = admin(&state, &headers) {
        return error.into_response();
    }
    operation(State(state), headers, Path(id), Query(query)).await
}
