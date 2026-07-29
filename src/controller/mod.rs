use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    future::Future,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use axum::extract::DefaultBodyLimit;
use futures_util::future::join_all;
use tokio::{
    net::TcpListener,
    sync::{Mutex, Notify},
};
use tower_http::trace::TraceLayer;
use tracing::{error, info, warn};

const CONTROLLER_TIMEOUT_MS: i64 = 60_000;
const MIN_KV_LOCK_LEASE_MS: u64 = 1_000;
const MAX_KV_LOCK_LEASE_MS: u64 = 300_000;
const MAX_HTTP_BODY_BYTES: usize = 6 * 1024 * 1024;

use crate::{
    config::ControllerConfig,
    gateway, kv,
    model::{
        BootstrapResponse, ClusterConfigResponse, ClusterConfigUpdate, ClusterMode,
        ClusterSettings, ClusterState, ControllerRecord, DesiredTaskState, GatewayStatus,
        HeartbeatResponse, JoinRequest, JoinResponse, KvDeleteRequest, KvListResponse, KvLock,
        KvLockAcquireRequest, KvLockAcquireResponse, KvLockMutationRequest, KvLockStatus,
        KvObjectResponse, KvPutRequest, KvPutResponse, KvStatResponse, KvState, LeaderRecord,
        NodeHeartbeat, NodeLabelRemoveRequest, NodeLabelSetRequest, NodeLabelsResponse, NodeMember,
        NodeRole, NodeRolesResponse, NodeRolesUpdate, ObservedTaskState, ServiceRecord,
        StackRecord, StatusResponse, TaskAssignment, UnclaimedTask, service_spec_hash,
        valid_gateway_image,
    },
    scheduler,
    storage::{StateRepository, StorageError},
};
use swarmlite_stack::ParsedStack;

mod api;
mod cluster;
mod deployment;
mod gateway_sync;
mod heartbeat;
mod kv_store;
mod leadership;
mod membership;
mod nodes;
mod recovery;
mod stacks;

use membership::*;
use recovery::*;
use stacks::*;

pub(crate) async fn run_with_repository_and_token_until<F>(
    config: ControllerConfig,
    token: String,
    repository: StateRepository,
    shutdown: F,
) -> Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let raft = repository.raft().clone();
    let raft_router = raft.rpc_router();
    let controller = Arc::new(
        Controller::new(config.clone(), token, repository)
            .await
            .map_err(anyhow::Error::msg)?,
    );
    let app = api::router(controller.clone())
        .nest("/internal/raft", raft_router)
        .layer(DefaultBodyLimit::max(MAX_HTTP_BODY_BYTES))
        .layer(TraceLayer::new_for_http());
    let listener = TcpListener::bind(config.listen)
        .await
        .with_context(|| format!("failed to listen on {}", config.listen))?;
    let background = controller.clone();
    let control_loop = tokio::spawn(async move { background.control_loop().await });
    let gateway_background = controller.clone();
    let gateway_loop = tokio::spawn(async move { gateway_background.gateway_sync_loop().await });
    info!(address = %config.listen, "controller API listening");
    let result = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await
        .map_err(anyhow::Error::from);
    control_loop.abort();
    gateway_loop.abort();
    let shutdown_result = raft.shutdown().await.map_err(anyhow::Error::msg);
    result.and(shutdown_result)
}

struct Inner {
    generation: u64,
    cluster: ClusterSettings,
    state: ClusterState,
    kv: KvState,
    is_leader: bool,
    live_nodes: HashMap<String, Instant>,
    controller_ack_candidates: HashMap<String, Instant>,
}

pub struct Controller {
    config: ControllerConfig,
    token: String,
    repository: StateRepository,
    inner: Mutex<Inner>,
    gateway_client: reqwest::Client,
    gateway_notify: Notify,
    gateway_sync: Mutex<GatewaySyncState>,
}

#[derive(Debug, Default)]
struct GatewaySyncState {
    applied_generation: Option<u64>,
    applied_controller_set_generations: BTreeMap<String, u64>,
    endpoint_errors: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RoleOperation {
    Set,
    Add,
    Remove,
}

#[derive(Debug)]
enum ControllerError {
    Unauthorized,
    Invalid(String),
    NotFound(String),
    Conflict(String),
    NotLeader(Option<String>),
    Storage(StorageError),
}

impl From<StorageError> for ControllerError {
    fn from(value: StorageError) -> Self {
        match value {
            StorageError::NotLeader(location) => Self::NotLeader(location),
            error => Self::Storage(error),
        }
    }
}

fn unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod tests;
