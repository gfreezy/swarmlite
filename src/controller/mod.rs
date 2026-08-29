use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    future::Future,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use axum::extract::DefaultBodyLimit;
use tokio::{
    net::TcpListener,
    sync::{Mutex, watch},
};
use tower_http::trace::TraceLayer;
use tracing::{debug, info, warn};

const MIN_KV_LOCK_LEASE_MS: u64 = 1_000;
const MAX_KV_LOCK_LEASE_MS: u64 = 300_000;
const MAX_HTTP_BODY_BYTES: usize = 16 * 1024 * 1024;

use crate::{
    config::ControllerConfig,
    gateway, kv,
    model::{
        BootstrapResponse, CONFIG_GC_GRACE_PERIOD_SECONDS, ClusterConfigResponse,
        ClusterConfigUpdate, ClusterSettings, ClusterState, DataSessionCreateResponse,
        DataSessionOperation, DataSessionStream, DeploymentImageResolutionNodeRecord,
        DeploymentImageResolutionRecord, DesiredTaskState, GatewayAssignment,
        GatewayRecoverySnapshot, GatewayReport, GatewayStatus, HeartbeatResponse,
        ImageResolutionAssignment, ImageResolutionProgress, ImageResolutionReport,
        ImageResolutionServiceAssignment, ImageResolutionStatus, JoinRequest, JoinResponse,
        KvDeleteRequest, KvListResponse, KvLockAcquireRequest, KvLockAcquireResponse,
        KvLockMutationRequest, KvObjectResponse, KvPutRequest, KvStatResponse, NodeGatewayResponse,
        NodeGatewayUpdate, NodeHeartbeat, NodeLabelRemoveRequest, NodeLabelSetRequest,
        NodeLabelsResponse, NodeMember, ObservedTaskState, RegistryCredential,
        RegistryLoginRequest, RegistryLoginResponse, ServiceInspectResponse, ServiceListResponse,
        ServiceRecord, ServiceSummary, StackDeploymentCondition, StackDeploymentConditionKind,
        StackDeploymentError, StackDeploymentGatewayProgress, StackDeploymentImageProgress,
        StackDeploymentListResponse, StackDeploymentRecord, StackDeploymentResponse,
        StackDeploymentServiceProgress, StackDeploymentSnapshot, StackDeploymentStatus,
        StackDeploymentSummary, StackDeploymentTaskPhaseProgress, StackListResponse, StackRecord,
        StackSummary, StatusResponse, TaskAssignment, TaskListResponse, TaskReconcileError,
        TaskReconcileProgress, TaskReconcileReport, TaskRecord, TaskRemovalAssignment, TaskSummary,
        UnclaimedTask, refresh_managed_gateway_image, service_spec_hash, valid_gateway_image,
    },
    scheduler,
    storage::{StateRepository, StorageError},
};
use swarmlite_stack::{ParsedStack, StackGatewaySpec};

mod api;
mod cluster;
mod commands;
mod deployment;
mod gateway_control;
mod heartbeat;
mod kv_store;
mod lifecycle;
mod membership;
mod nodes;
mod recovery;
mod registries;
mod resources;
mod sessions;
mod stacks;

use deployment::{
    apply_image_progress, apply_image_resolution_report, apply_task_result,
    deployment_replacement_ready, mark_deployment_progress,
};
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
    let controller = Arc::new(
        Controller::new(config.clone(), token, repository)
            .await
            .map_err(anyhow::Error::msg)?,
    );
    let app = api::router(controller.clone())
        .layer(DefaultBodyLimit::max(MAX_HTTP_BODY_BYTES))
        .layer(TraceLayer::new_for_http());
    let listener = TcpListener::bind(config.listen)
        .await
        .with_context(|| format!("failed to listen on {}", config.listen))?;
    let background = controller.clone();
    let control_loop = tokio::spawn(async move { background.control_loop().await });
    info!(address = %config.listen, "controller API listening");
    let result = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await
        .map_err(anyhow::Error::from);
    control_loop.abort();
    result
}

struct Inner {
    generation: u64,
    status_revision: u64,
    cluster: ClusterSettings,
    state: ClusterState,
    live_nodes: HashMap<String, Instant>,
    gateway_generation: u64,
    gateway_config: serde_json::Value,
    gateway_snapshot: GatewayRecoverySnapshot,
    gateway_reports: HashMap<String, GatewayReport>,
    task_progress: HashMap<(String, crate::model::TaskReconcilePhase), TaskReconcileProgress>,
}

pub struct Controller {
    config: ControllerConfig,
    token: String,
    repository: StateRepository,
    kv_repository: kv::KvRepository,
    commands: commands::AgentCommandBroker,
    sessions: sessions::DataSessionBroker,
    deploying_stacks: std::sync::Mutex<BTreeSet<String>>,
    status_changes: watch::Sender<u64>,
    inner: Mutex<Inner>,
}

#[derive(Debug)]
enum ControllerError {
    Unauthorized,
    Invalid(String),
    NotFound(String),
    Conflict(String),
    Storage(StorageError),
}

impl From<StorageError> for ControllerError {
    fn from(value: StorageError) -> Self {
        Self::Storage(value)
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
