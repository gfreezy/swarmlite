use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    future::Future,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use futures_util::future::join_all;
use serde::Deserialize;
use serde_json::json;
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
        NodeRole, NodeRoles, NodeRolesResponse, NodeRolesUpdate, ObservedTaskState, RecoveryStatus,
        ServiceRecord, StackGatewaySpec, StackRecord, StatusResponse, TaskAssignment, TaskRecord,
        UnclaimedTask, agent_roles, service_spec_hash, valid_gateway_image,
    },
    scheduler,
    stack::{ParsedStack, parse_stack},
    storage::{StateRepository, StorageError},
};

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
    let app = Router::new()
        .route("/healthz", get(health))
        .route("/v1/status", get(status))
        .route("/v1/cluster", get(bootstrap))
        .route(
            "/v1/config",
            get(get_cluster_config).patch(update_cluster_config),
        )
        .route("/v1/stacks/{name}", put(apply_stack))
        .route("/v1/nodes/{node_id}/join", put(join_node))
        .route("/v1/nodes/{node_id}/heartbeat", post(heartbeat))
        .route("/v1/gateway", get(gateway_config))
        .route(
            "/v1/nodes/{node_id}/roles",
            get(get_node_roles)
                .put(set_node_roles)
                .patch(add_node_roles)
                .delete(remove_node_roles),
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
        .with_state(controller.clone())
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

impl Controller {
    async fn new(
        config: ControllerConfig,
        token: String,
        repository: StateRepository,
    ) -> Result<Self, StorageError> {
        let versioned = repository.initialize_with_cluster(&config.cluster).await?;
        let gateway_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.gateway.request_timeout_seconds))
            .build()
            .map_err(|error| StorageError::Backend(error.to_string()))?;
        // Preserve existing assignments during the first node timeout after a takeover.
        let live_nodes = versioned
            .state
            .tasks
            .values()
            .map(|task| (task.node_id.clone(), Instant::now()))
            .collect();
        Ok(Self {
            config,
            token,
            repository,
            inner: Mutex::new(Inner {
                generation: versioned.generation,
                cluster: versioned.cluster,
                state: versioned.state,
                kv: versioned.kv,
                is_leader: false,
                live_nodes,
                controller_ack_candidates: HashMap::new(),
            }),
            gateway_client,
            gateway_notify: Notify::new(),
            gateway_sync: Mutex::new(GatewaySyncState::default()),
        })
    }

    async fn control_loop(self: Arc<Self>) {
        let mut ticker =
            tokio::time::interval(Duration::from_secs(self.config.reconcile_interval_seconds));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            if let Err(error) = self.tick().await {
                warn!(%error, "controller reconciliation tick failed");
            }
        }
    }

    async fn tick(&self) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().await;
        if !self.repository.is_leader() {
            inner.is_leader = false;
            self.refresh_locked(&mut inner).await?;
            return Ok(());
        }
        if !inner.is_leader {
            self.refresh_locked(&mut inner).await?;
            self.try_acquire_locked(&mut inner).await?;
            return Ok(());
        }

        if inner.is_leader {
            let timeout = Duration::from_secs(self.config.node_timeout_seconds);
            let now = Instant::now();
            let live: BTreeSet<String> = inner
                .live_nodes
                .iter()
                .filter(|(_, seen)| now.duration_since(**seen) <= timeout)
                .map(|(id, _)| id.clone())
                .collect();
            let previous = inner.state.clone();
            let now_unix_ms = unix_ms();
            let voters = self.repository.voter_ids();
            let mut changed = prune_controllers(&mut inner.state, now_unix_ms, &voters);
            changed |= scheduler::finish_drains(&mut inner.state, now_unix_ms);
            changed |= scheduler::reconcile(&mut inner.state, &live);
            if changed && let Err(error) = self.commit_locked(&mut inner).await {
                inner.state = previous;
                return Err(error);
            }
            return Ok(());
        }
        Ok(())
    }

    fn expire_local_lease(&self, inner: &mut Inner) {
        if inner.is_leader && !self.repository.is_leader() {
            warn!("Raft leadership changed; entering standby mode");
            inner.is_leader = false;
        }
    }

    async fn refresh_locked(&self, inner: &mut Inner) -> Result<(), StorageError> {
        let latest = if self.repository.is_leader() {
            self.repository.load_consistent().await?
        } else {
            self.repository.load_local().await?
        };
        if latest.generation != inner.generation {
            inner.generation = latest.generation;
            inner.cluster = latest.cluster;
            inner.state = latest.state;
            inner.kv = latest.kv;
        }
        Ok(())
    }

    async fn try_acquire_locked(&self, inner: &mut Inner) -> Result<(), StorageError> {
        self.repository
            .ensure_voter(
                self.repository.raft().node_id(),
                self.repository.raft().local_node().clone(),
            )
            .await?;
        let term = self.repository.current_term();
        info!(term, "acquired controller leadership");
        inner.is_leader = true;
        inner.live_nodes.clear();
        inner.controller_ack_candidates.clear();
        inner.state.nodes.clear();
        let takeover_time = Instant::now();
        for node_id in inner.state.members.keys() {
            inner
                .controller_ack_candidates
                .insert(node_id.clone(), takeover_time);
        }
        for node_id in inner.state.tasks.values().map(|task| task.node_id.clone()) {
            inner.live_nodes.insert(node_id, takeover_time);
        }
        let mut drains_reset = false;
        for task in inner.state.tasks.values_mut() {
            if task.desired == DesiredTaskState::Draining
                && task.drain_until_unix_ms.take().is_some()
            {
                drains_reset = true;
            }
        }
        let now = unix_ms();
        let self_record = ControllerRecord {
            node_id: self.config.controller_id.clone(),
            advertise_url: self.config.advertise_url.trim_end_matches('/').to_owned(),
            raft_id: self.repository.raft().node_id(),
            raft_url: self.repository.raft().local_node().raft_url.clone(),
            reserved_at_unix_ms: now,
        };
        let controller_changed = inner
            .state
            .controllers
            .get(&self.config.controller_id)
            .is_none_or(|record| {
                record.advertise_url != self_record.advertise_url
                    || record.raft_id != self_record.raft_id
                    || record.raft_url != self_record.raft_url
            });
        inner
            .state
            .controllers
            .insert(self.config.controller_id.clone(), self_record);
        let address = reqwest::Url::parse(&self.config.advertise_url)
            .ok()
            .and_then(|url| url.host_str().map(ToOwned::to_owned))
            .unwrap_or_default();
        let (roles, labels, automatic_roles, joined_at_unix_ms) = inner
            .state
            .members
            .get(&self.config.controller_id)
            .map(|member| {
                (
                    member.roles.clone(),
                    member.labels.clone(),
                    member.automatic_roles,
                    member.joined_at_unix_ms,
                )
            })
            .unwrap_or_else(|| {
                (
                    self.config.roles.clone(),
                    self.config.labels.clone(),
                    true,
                    now,
                )
            });
        let self_member = NodeMember {
            id: self.config.controller_id.clone(),
            address,
            roles,
            labels,
            automatic_roles,
            controller_url: self.config.advertise_url.trim_end_matches('/').to_owned(),
            raft_id: self.repository.raft().node_id(),
            raft_url: self.repository.raft().local_node().raft_url.clone(),
            joined_at_unix_ms,
        };
        let member_changed = inner
            .state
            .members
            .get(&self.config.controller_id)
            .is_none_or(|member| member != &self_member);
        if member_changed {
            inner
                .state
                .members
                .insert(self.config.controller_id.clone(), self_member);
        }
        if drains_reset || controller_changed || member_changed {
            self.commit_locked(inner).await?;
        } else {
            self.gateway_notify.notify_one();
        }
        Ok(())
    }

    async fn commit_locked(&self, inner: &mut Inner) -> Result<(), StorageError> {
        self.expire_local_lease(inner);
        if !inner.is_leader {
            return Err(StorageError::Conflict);
        }
        match self
            .repository
            .replace(inner.generation, &inner.cluster, &inner.state, &inner.kv)
            .await
        {
            Ok(generation) => {
                inner.generation = generation;
                self.gateway_notify.notify_one();
                Ok(())
            }
            Err(error) => {
                inner.is_leader = false;
                Err(error)
            }
        }
    }

    async fn commit_kv_locked(&self, inner: &mut Inner) -> Result<(), StorageError> {
        self.expire_local_lease(inner);
        if !inner.is_leader {
            return Err(StorageError::Conflict);
        }
        match self
            .repository
            .replace(inner.generation, &inner.cluster, &inner.state, &inner.kv)
            .await
        {
            Ok(generation) => {
                inner.generation = generation;
                Ok(())
            }
            Err(error) => {
                inner.is_leader = false;
                Err(error)
            }
        }
    }

    fn leader_redirect(&self, path: &str) -> ControllerError {
        let location = self
            .repository
            .leader_url()
            .map(|leader| format!("{}{}", leader.trim_end_matches('/'), path));
        ControllerError::NotLeader(location)
    }

    fn leader_redirect_with_query(&self, path: &str, query: &[(&str, String)]) -> ControllerError {
        let location = self.repository.leader_url().and_then(|leader| {
            let target = format!("{}{}", leader.trim_end_matches('/'), path);
            let mut target = reqwest::Url::parse(&target).ok()?;
            target.query_pairs_mut().extend_pairs(query.iter().cloned());
            Some(target.into())
        });
        ControllerError::NotLeader(location)
    }

    fn cluster_settings(&self, inner: &Inner) -> Result<ClusterSettings, StorageError> {
        Ok(inner.cluster.clone())
    }

    async fn get_cluster_config(&self) -> Result<ClusterConfigResponse, ControllerError> {
        let mut inner = self.inner.lock().await;
        self.expire_local_lease(&mut inner);
        if !inner.is_leader {
            return Err(self.leader_redirect("/v1/config"));
        }
        Ok(ClusterConfigResponse {
            generation: inner.generation,
            config: self.cluster_settings(&inner)?,
        })
    }

    async fn update_cluster_config(
        &self,
        update: ClusterConfigUpdate,
    ) -> Result<ClusterConfigResponse, ControllerError> {
        let mut inner = self.inner.lock().await;
        self.expire_local_lease(&mut inner);
        if !inner.is_leader {
            return Err(self.leader_redirect("/v1/config"));
        }

        let ClusterConfigUpdate {
            mode,
            gateway_image,
        } = update;
        if mode.is_none() && gateway_image.is_none() {
            return Err(ControllerError::Invalid(
                "cluster configuration update must contain a key".to_owned(),
            ));
        }
        if gateway_image
            .as_deref()
            .is_some_and(|image| !valid_gateway_image(image))
        {
            return Err(ControllerError::Invalid(
                "gateway-image must be a non-empty OCI image reference without whitespace"
                    .to_owned(),
            ));
        }

        let mut cluster = self.cluster_settings(&inner)?;
        let previous_cluster = inner.cluster.clone();
        let previous_state = inner.state.clone();
        let mut changed = false;

        if let Some(mode) = mode {
            if cluster.mode == ClusterMode::Ha && mode == ClusterMode::Standalone {
                return Err(ControllerError::Conflict(
                    "switching an HA cluster back to standalone is not supported".to_owned(),
                ));
            }
            if cluster.mode != mode {
                cluster.mode = mode;
                if mode == ClusterMode::Ha {
                    fill_automatic_ha_controllers(&mut inner.state);
                }
                changed = true;
            }
        }

        if let Some(image) = gateway_image
            && cluster.gateway.image != image
        {
            cluster.gateway.image = image;
            changed = true;
        }

        if changed {
            inner.cluster = cluster.clone();
            if let Err(error) = self.commit_locked(&mut inner).await {
                inner.cluster = previous_cluster;
                inner.state = previous_state;
                return Err(error.into());
            }
            info!(mode = ?cluster.mode, gateway_image = %cluster.gateway.image, "updated cluster configuration");
        }

        Ok(ClusterConfigResponse {
            generation: inner.generation,
            config: cluster,
        })
    }

    async fn bootstrap(&self) -> Result<BootstrapResponse, ControllerError> {
        let inner = self.inner.lock().await;
        let cluster = self.cluster_settings(&inner)?;
        let (controller_set_generation, voters) = self.repository.controller_set();
        Ok(BootstrapResponse {
            cluster,
            controllers: controller_urls(&inner.state, Some(&self.config.advertise_url), &voters),
            controller_set_generation,
        })
    }

    async fn join_node(
        &self,
        node_id: &str,
        request: JoinRequest,
    ) -> Result<JoinResponse, ControllerError> {
        for (key, value) in &request.labels {
            validate_node_label(key, value)?;
        }
        if node_id != request.node_id {
            return Err(ControllerError::Invalid(
                "node ID in path and request body differ".to_owned(),
            ));
        }
        if request.node_id.trim().is_empty() || request.address.trim().is_empty() {
            return Err(ControllerError::Invalid(
                "node ID and address must not be empty".to_owned(),
            ));
        }
        let mut inner = self.inner.lock().await;
        self.expire_local_lease(&mut inner);
        if !inner.is_leader {
            return Err(self.leader_redirect(&format!("/v1/nodes/{node_id}/join")));
        }
        let cluster = self.cluster_settings(&inner)?;

        let previous = inner.state.clone();
        let now = unix_ms();
        let voters = self.repository.voter_ids();
        let mut changed = prune_controllers(&mut inner.state, now, &voters);
        let roles = if let Some(existing) = inner.state.members.get(node_id).cloned() {
            if existing.raft_id != request.raft_id {
                return Err(ControllerError::Invalid(
                    "a node cannot change its persisted raft_id".to_owned(),
                ));
            }
            if let Some(requested) = &request.requested_roles {
                let requested = normalized_roles(requested.clone());
                if requested != existing.roles {
                    return Err(ControllerError::Conflict(
                        "this node is already joined with different roles; use `swarmlite role set`"
                            .to_owned(),
                    ));
                }
            }
            if !request.labels.is_empty() && request.labels != existing.labels {
                return Err(ControllerError::Conflict(
                    "this node is already joined with different labels; use `swarmlite node label set` or `remove`"
                        .to_owned(),
                ));
            }
            let member = inner.state.members.get_mut(node_id).expect("member exists");
            if member.address != request.address
                || member.controller_url != request.controller_url
                || member.raft_url != request.raft_url
            {
                member.address.clone_from(&request.address);
                member.controller_url.clone_from(&request.controller_url);
                member.raft_url.clone_from(&request.raft_url);
                changed = true;
            }
            existing.roles
        } else {
            let (roles, automatic_roles) = match request.requested_roles.clone() {
                Some(roles) => (normalized_roles(roles), false),
                None => {
                    let mut roles = automatic_join_roles(&inner.state, cluster.mode);
                    roles.extend(request.recovered_roles);
                    (normalized_roles(roles), true)
                }
            };
            validate_role_limits(&inner.state, node_id, &roles, cluster.mode)?;
            inner.state.members.insert(
                node_id.to_owned(),
                NodeMember {
                    id: node_id.to_owned(),
                    address: request.address.clone(),
                    roles: roles.clone(),
                    labels: request.labels.clone(),
                    automatic_roles,
                    controller_url: request.controller_url.clone(),
                    raft_id: request.raft_id,
                    raft_url: request.raft_url.clone(),
                    joined_at_unix_ms: now,
                },
            );
            changed = true;
            roles
        };
        if roles.contains(&NodeRole::Controller) {
            changed |= ensure_controller_record(&mut inner.state, node_id, now)?;
        }
        if changed && let Err(error) = self.commit_locked(&mut inner).await {
            inner.state = previous;
            return Err(error.into());
        }
        inner
            .controller_ack_candidates
            .insert(node_id.to_owned(), Instant::now());
        let (controller_set_generation, voters) = self.repository.controller_set();
        let labels = inner.state.members[node_id].labels.clone();
        Ok(JoinResponse {
            cluster,
            roles,
            labels,
            controllers: controller_urls(&inner.state, Some(&self.config.advertise_url), &voters),
            controller_set_generation,
        })
    }

    async fn node_roles(&self, node_id: &str) -> Result<NodeRolesResponse, ControllerError> {
        let mut inner = self.inner.lock().await;
        self.expire_local_lease(&mut inner);
        if !inner.is_leader {
            return Err(self.leader_redirect(&format!("/v1/nodes/{node_id}/roles")));
        }
        let member =
            inner.state.members.get(node_id).ok_or_else(|| {
                ControllerError::NotFound(format!("node {node_id} is not joined"))
            })?;
        Ok(NodeRolesResponse {
            node_id: node_id.to_owned(),
            roles: member.roles.clone(),
        })
    }

    async fn update_node_roles(
        &self,
        node_id: &str,
        update: NodeRolesUpdate,
        operation: RoleOperation,
    ) -> Result<NodeRolesResponse, ControllerError> {
        if update.roles.is_empty() && operation != RoleOperation::Set {
            return Err(ControllerError::Invalid(
                "at least one role must be supplied".to_owned(),
            ));
        }
        if operation == RoleOperation::Remove && update.roles.contains(&NodeRole::Agent) {
            return Err(ControllerError::Conflict(
                "the mandatory agent role cannot be removed".to_owned(),
            ));
        }

        let mut inner = self.inner.lock().await;
        self.expire_local_lease(&mut inner);
        if !inner.is_leader {
            return Err(self.leader_redirect(&format!("/v1/nodes/{node_id}/roles")));
        }
        let current = inner
            .state
            .members
            .get(node_id)
            .ok_or_else(|| ControllerError::NotFound(format!("node {node_id} is not joined")))?
            .roles
            .clone();
        let mut roles = match operation {
            RoleOperation::Set => update.roles,
            RoleOperation::Add => current.union(&update.roles).copied().collect(),
            RoleOperation::Remove => current.difference(&update.roles).copied().collect(),
        };
        roles.insert(NodeRole::Agent);
        if roles == current {
            return Ok(NodeRolesResponse {
                node_id: node_id.to_owned(),
                roles,
            });
        }

        validate_role_limits(&inner.state, node_id, &roles, inner.cluster.mode)?;
        if current.contains(&NodeRole::Gateway) && !roles.contains(&NodeRole::Gateway) {
            let gateway_count = inner
                .state
                .members
                .values()
                .filter(|member| member.roles.contains(&NodeRole::Gateway))
                .count();
            if gateway_count <= 1 {
                return Err(ControllerError::Conflict(
                    "cannot remove the cluster's last gateway role".to_owned(),
                ));
            }
        }
        if current.contains(&NodeRole::Controller) && !roles.contains(&NodeRole::Controller) {
            let controller_count = inner
                .state
                .members
                .values()
                .filter(|member| member.roles.contains(&NodeRole::Controller))
                .count();
            if controller_count <= 1 {
                return Err(ControllerError::Conflict(
                    "cannot remove the cluster's last controller role".to_owned(),
                ));
            }
        }

        let (controller_set_generation, voters) = self.repository.controller_set();
        let removed_voter = (current.contains(&NodeRole::Controller)
            && !roles.contains(&NodeRole::Controller))
        .then(|| {
            inner
                .state
                .controllers
                .get(node_id)
                .map(|record| record.raft_id)
        })
        .flatten()
        .filter(|raft_id| voters.contains(raft_id));
        if removed_voter.is_some() {
            if voters.len() <= 1 {
                return Err(ControllerError::Conflict(
                    "cannot remove the last active controller voter; wait for another controller to be promoted"
                        .to_owned(),
                ));
            }
            let pending = pending_controller_set_acknowledgements(
                &inner,
                self.config.node_timeout_seconds,
                controller_set_generation,
            );
            if !pending.is_empty() {
                return Err(ControllerError::Conflict(format!(
                    "cannot remove controller until active agents apply controller set generation {controller_set_generation}; waiting for {}",
                    pending.into_iter().collect::<Vec<_>>().join(", ")
                )));
            }
            let pending_gateways = {
                let sync = self.gateway_sync.lock().await;
                pending_gateway_controller_set_acknowledgements(
                    &inner,
                    &sync,
                    self.config.node_timeout_seconds,
                    self.config.gateway.admin_port,
                    controller_set_generation,
                )
            };
            if !pending_gateways.is_empty() {
                return Err(ControllerError::Conflict(format!(
                    "cannot remove controller until active Caddy gateways apply controller set generation {controller_set_generation}; waiting for {}",
                    pending_gateways.into_iter().collect::<Vec<_>>().join(", ")
                )));
            }
        }

        let previous = inner.state.clone();
        let member = inner
            .state
            .members
            .get_mut(node_id)
            .expect("member was checked above");
        member.roles.clone_from(&roles);
        member.automatic_roles = false;
        if roles.contains(&NodeRole::Controller) {
            ensure_controller_record(&mut inner.state, node_id, unix_ms())?;
        } else {
            inner.state.controllers.remove(node_id);
        }
        if let Err(error) = self.commit_locked(&mut inner).await {
            inner.state = previous;
            return Err(error.into());
        }
        if let Some(raft_id) = removed_voter
            && let Err(error) = self.repository.remove_voter(raft_id).await
        {
            inner.state = previous;
            if let Err(rollback_error) = self.commit_locked(&mut inner).await {
                error!(
                    %rollback_error,
                    node_id,
                    "failed to roll back node roles after voter removal failed"
                );
            }
            return Err(error.into());
        }
        info!(node_id, roles = ?roles, "updated node roles");
        Ok(NodeRolesResponse {
            node_id: node_id.to_owned(),
            roles,
        })
    }

    async fn node_labels(&self, node_id: &str) -> Result<NodeLabelsResponse, ControllerError> {
        let mut inner = self.inner.lock().await;
        self.expire_local_lease(&mut inner);
        if !inner.is_leader {
            return Err(self.leader_redirect(&format!("/v1/nodes/{node_id}/labels")));
        }
        let member =
            inner.state.members.get(node_id).ok_or_else(|| {
                ControllerError::NotFound(format!("node {node_id} is not joined"))
            })?;
        Ok(NodeLabelsResponse {
            node_id: node_id.to_owned(),
            labels: member.labels.clone(),
        })
    }

    async fn set_node_label(
        &self,
        node_id: &str,
        request: NodeLabelSetRequest,
    ) -> Result<NodeLabelsResponse, ControllerError> {
        validate_node_label(&request.key, &request.value)?;
        self.update_node_label(node_id, request.key, Some(request.value))
            .await
    }

    async fn remove_node_label(
        &self,
        node_id: &str,
        request: NodeLabelRemoveRequest,
    ) -> Result<NodeLabelsResponse, ControllerError> {
        validate_node_label_key(&request.key)?;
        self.update_node_label(node_id, request.key, None).await
    }

    async fn update_node_label(
        &self,
        node_id: &str,
        key: String,
        value: Option<String>,
    ) -> Result<NodeLabelsResponse, ControllerError> {
        let mut inner = self.inner.lock().await;
        self.expire_local_lease(&mut inner);
        if !inner.is_leader {
            return Err(self.leader_redirect(&format!("/v1/nodes/{node_id}/labels")));
        }
        let current = inner
            .state
            .members
            .get(node_id)
            .ok_or_else(|| ControllerError::NotFound(format!("node {node_id} is not joined")))?
            .labels
            .clone();
        let mut labels = current.clone();
        let removed = value.is_none();
        match value {
            Some(value) => {
                labels.insert(key.clone(), value);
            }
            None => {
                labels.remove(&key);
            }
        }
        if labels == current {
            return Ok(NodeLabelsResponse {
                node_id: node_id.to_owned(),
                labels,
            });
        }

        let previous = inner.state.clone();
        inner
            .state
            .members
            .get_mut(node_id)
            .expect("member was checked above")
            .labels
            .clone_from(&labels);
        if let Some(node) = inner.state.nodes.get_mut(node_id) {
            node.labels.clone_from(&labels);
        }
        let live = current_live_nodes(&inner, self.config.node_timeout_seconds);
        scheduler::reconcile(&mut inner.state, &live);
        if let Err(error) = self.commit_locked(&mut inner).await {
            inner.state = previous;
            return Err(error.into());
        }
        info!(node_id, label = %key, removed, "updated node label");
        Ok(NodeLabelsResponse {
            node_id: node_id.to_owned(),
            labels,
        })
    }

    async fn apply(&self, stack_name: &str, parsed: ParsedStack) -> Result<u64, ControllerError> {
        validate_stack_name(stack_name)?;
        let ParsedStack {
            services,
            gateway: stack_gateway,
        } = parsed;
        let has_gateway = {
            let inner = self.inner.lock().await;
            inner
                .state
                .members
                .values()
                .any(|member| member.roles.contains(&NodeRole::Gateway))
        };
        if !has_gateway && !stack_gateway.http_routes.is_empty() {
            return Err(ControllerError::Invalid(
                "gateway routing is enabled but no node has the gateway role".to_owned(),
            ));
        }
        let mut inner = self.inner.lock().await;
        self.expire_local_lease(&mut inner);
        if !inner.is_leader {
            return Err(self.leader_redirect(&format!("/v1/stacks/{stack_name}")));
        }
        validate_gateway_hostname_ownership(&inner.state, stack_name, &stack_gateway)?;
        let previous = inner.state.clone();
        let previous_gateway = inner
            .state
            .stacks
            .get(stack_name)
            .map(|stack| stack.gateway.clone())
            .unwrap_or_default();
        let desired_ids: BTreeSet<String> = services
            .keys()
            .map(|name| service_id(stack_name, name))
            .collect();
        for service in inner
            .state
            .services
            .values_mut()
            .filter(|service| service.stack == stack_name)
        {
            service.deleted = !desired_ids.contains(&service.id);
        }
        for (name, spec) in services {
            let id = service_id(stack_name, &name);
            let routing_ports_changed = gateway::routed_service_ports(&previous_gateway, &name)
                != gateway::routed_service_ports(&stack_gateway, &name);
            match inner.state.services.get_mut(&id) {
                Some(existing)
                    if existing.spec == spec && !existing.deleted && !routing_ports_changed => {}
                Some(existing) => {
                    existing.revision += 1;
                    existing.spec = spec;
                    existing.deleted = false;
                }
                None => {
                    inner.state.services.insert(
                        id.clone(),
                        ServiceRecord {
                            id,
                            stack: stack_name.to_owned(),
                            name,
                            revision: 1,
                            spec,
                            deleted: false,
                        },
                    );
                }
            }
        }
        inner.state.stacks.insert(
            stack_name.to_owned(),
            StackRecord {
                name: stack_name.to_owned(),
                applied_at_unix_ms: unix_ms(),
                services: desired_ids.into_iter().collect(),
                gateway: stack_gateway,
            },
        );
        adopt_unclaimed_tasks(&mut inner.state, stack_name);
        let live = current_live_nodes(&inner, self.config.node_timeout_seconds);
        scheduler::reconcile(&mut inner.state, &live);
        if let Err(error) = self.commit_locked(&mut inner).await {
            inner.state = previous;
            return Err(error.into());
        }
        Ok(inner.generation)
    }

    async fn heartbeat(
        &self,
        node_id: &str,
        heartbeat: NodeHeartbeat,
    ) -> Result<HeartbeatResponse, ControllerError> {
        let NodeHeartbeat { mut node, tasks } = heartbeat;
        if node_id != node.id {
            return Err(ControllerError::Invalid(
                "node ID in path and request body differ".to_owned(),
            ));
        }
        let mut inner = self.inner.lock().await;
        self.expire_local_lease(&mut inner);
        if !inner.is_leader {
            return Err(self.leader_redirect(&format!("/v1/nodes/{node_id}/heartbeat")));
        }

        let previous = inner.state.clone();
        let now = unix_ms();
        let voters = self.repository.voter_ids();
        let mut changed = prune_controllers(&mut inner.state, now, &voters);
        let mut soft_changed = false;
        let (desired_roles, desired_labels) = {
            let member = inner.state.members.get_mut(node_id).ok_or_else(|| {
                ControllerError::Invalid("node must join before sending heartbeats".to_owned())
            })?;
            if member.raft_id != node.raft_id {
                return Err(ControllerError::Invalid(
                    "heartbeat raft_id differs from the joined node identity".to_owned(),
                ));
            }
            if member.address != node.address {
                member.address.clone_from(&node.address);
                changed = true;
            }
            (member.roles.clone(), member.labels.clone())
        };
        if desired_roles.contains(&NodeRole::Controller) {
            changed |= ensure_controller_record(&mut inner.state, node_id, now)?;
            if node.roles.contains(&NodeRole::Controller) {
                let record = inner.state.controllers[node_id].clone();
                if !self.repository.is_voter(record.raft_id) {
                    self.repository
                        .ensure_voter(
                            record.raft_id,
                            swarmlite_raft::ControllerNode {
                                raft_url: record.raft_url,
                                api_url: record.advertise_url,
                            },
                        )
                        .await?;
                    self.gateway_notify.notify_one();
                }
            }
        }
        node.labels.clone_from(&desired_labels);
        soft_changed |= inner.state.nodes.get(node_id).is_none_or(|existing| {
            serde_json::to_value(existing).ok() != serde_json::to_value(&node).ok()
        });
        inner.live_nodes.insert(node_id.to_owned(), Instant::now());
        inner.state.nodes.insert(node_id.to_owned(), node);

        let reports: HashMap<_, _> = tasks
            .into_iter()
            .map(|report| (report.id.clone(), report))
            .collect();
        let reported_ids = reports.keys().cloned().collect::<BTreeSet<_>>();
        let before_unclaimed = inner.state.unclaimed_tasks.len();
        inner
            .state
            .unclaimed_tasks
            .retain(|task_id, task| task.node_id != node_id || reported_ids.contains(task_id));
        soft_changed |= inner.state.unclaimed_tasks.len() != before_unclaimed;
        for report in reports.values() {
            if inner.state.tasks.contains_key(&report.id) {
                soft_changed |= inner.state.unclaimed_tasks.remove(&report.id).is_some();
                continue;
            }
            let unclaimed = report
                .cluster_id
                .as_deref()
                .filter(|cluster_id| *cluster_id == self.config.cluster.cluster_id)
                .and_then(|_| {
                    Some(UnclaimedTask {
                        id: report.id.clone(),
                        stack: report.stack.clone()?,
                        service: report.service.clone()?,
                        slot: report.slot?,
                        revision: report.revision.unwrap_or(1),
                        spec_hash: report.spec_hash.clone()?,
                        node_id: node_id.to_owned(),
                        observed: report.observed.clone(),
                        ports: report.ports.clone(),
                        container_id: report.container_id.clone(),
                    })
                });
            if let Some(unclaimed) = unclaimed
                && inner.state.unclaimed_tasks.get(&report.id) != Some(&unclaimed)
            {
                inner
                    .state
                    .unclaimed_tasks
                    .insert(report.id.clone(), unclaimed);
                soft_changed = true;
            }
        }
        let assigned_ids: Vec<String> = inner
            .state
            .tasks
            .values()
            .filter(|task| task.node_id == node_id)
            .map(|task| task.id.clone())
            .collect();
        let mut remove = Vec::new();
        for id in assigned_ids {
            let task = inner.state.tasks.get_mut(&id).unwrap();
            match reports.get(&id) {
                Some(report) => {
                    if task.observed != report.observed || task.container_id != report.container_id
                    {
                        task.observed = report.observed.clone();
                        task.container_id = report.container_id.clone();
                        soft_changed = true;
                    }
                }
                None if task.desired == DesiredTaskState::Stopped => {
                    remove.push(id);
                }
                None if matches!(
                    task.observed,
                    ObservedTaskState::Starting
                        | ObservedTaskState::Running
                        | ObservedTaskState::Healthy
                ) =>
                {
                    task.observed = ObservedTaskState::Failed;
                    soft_changed = true;
                }
                None => {}
            }
        }
        for id in remove {
            inner.state.tasks.remove(&id);
            changed = true;
        }

        let live = current_live_nodes(&inner, self.config.node_timeout_seconds);
        changed |= scheduler::reconcile(&mut inner.state, &live);
        if changed && let Err(error) = self.commit_locked(&mut inner).await {
            inner.state = previous;
            return Err(error.into());
        }
        if soft_changed && !changed {
            self.gateway_notify.notify_one();
        }

        let term = self.repository.current_term();
        let generation = inner.generation;
        let assignments = inner
            .state
            .tasks
            .values()
            .filter(|task| {
                task.node_id == node_id
                    && matches!(
                        task.desired,
                        DesiredTaskState::Running | DesiredTaskState::Draining
                    )
            })
            .filter_map(|task| {
                let service = inner.state.services.get(&task.service_id)?;
                Some(TaskAssignment {
                    id: task.id.clone(),
                    cluster_id: self.config.cluster.cluster_id.clone(),
                    stack: service.stack.clone(),
                    service: service.name.clone(),
                    service_id: task.service_id.clone(),
                    revision: task.revision,
                    slot: task.slot,
                    spec: service.spec.clone(),
                    ports: task.ports.clone(),
                    leader_term: term,
                    generation,
                    spec_hash: service_spec_hash(&service.spec),
                })
            })
            .collect();
        let remove_tasks = inner
            .state
            .tasks
            .values()
            .filter(|task| task.node_id == node_id && task.desired == DesiredTaskState::Stopped)
            .map(|task| task.id.clone())
            .collect();
        let (controller_set_generation, voters) = self.repository.controller_set();
        Ok(HeartbeatResponse {
            leader_term: term,
            generation,
            controller_set_generation,
            cluster: inner.cluster.clone(),
            assignments,
            roles: desired_roles,
            labels: desired_labels,
            controllers: controller_urls(&inner.state, Some(&self.config.advertise_url), &voters),
            remove_tasks,
        })
    }

    async fn status(&self) -> StatusResponse {
        let mut inner = self.inner.lock().await;
        self.expire_local_lease(&mut inner);
        let generation = inner.generation;
        let (controller_set_generation, _) = self.repository.controller_set();
        let leader = self.repository.raft().leader().map(|(raft_id, node)| {
            let id = inner
                .state
                .controllers
                .values()
                .find(|record| record.raft_id == raft_id)
                .map_or_else(|| raft_id.to_string(), |record| record.node_id.clone());
            LeaderRecord {
                id,
                term: self.repository.current_term(),
                advertise_url: node.api_url,
            }
        });
        let is_leader = inner.is_leader;
        let state = inner.state.clone();
        let recovery = recovery_status(&state);
        drop(inner);
        let gateway_sync = self.gateway_sync.lock().await;
        StatusResponse {
            cluster_id: self.config.cluster.cluster_id.clone(),
            generation,
            controller_set_generation,
            leader,
            is_leader,
            gateway: GatewayStatus {
                enabled: state
                    .members
                    .values()
                    .any(|member| member.roles.contains(&NodeRole::Gateway)),
                desired_generation: generation,
                applied_generation: gateway_sync.applied_generation,
                desired_controller_set_generation: controller_set_generation,
                applied_controller_set_generations: gateway_sync
                    .applied_controller_set_generations
                    .clone(),
                endpoint_errors: gateway_sync.endpoint_errors.clone(),
            },
            recovery,
            state,
        }
    }

    async fn leader_status(&self) -> Result<StatusResponse, ControllerError> {
        let status = self.status().await;
        if status.is_leader {
            Ok(status)
        } else if self.repository.is_leader() {
            Err(StorageError::Backend("Raft leader is still initializing".to_owned()).into())
        } else {
            Err(self.leader_redirect("/v1/status"))
        }
    }

    async fn gateway(&self) -> Result<gateway::HttpServer, ControllerError> {
        let mut inner = self.inner.lock().await;
        self.expire_local_lease(&mut inner);
        if !inner.is_leader {
            return Err(self.leader_redirect("/v1/gateway"));
        }
        Ok(gateway::generate(&inner.state, &self.config.gateway.listen))
    }

    async fn gateway_sync_loop(self: Arc<Self>) {
        let mut ticker = tokio::time::interval(Duration::from_secs(
            self.config.gateway.resync_interval_seconds,
        ));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                _ = self.gateway_notify.notified() => {}
            }
            loop {
                match self.sync_gateway_once().await {
                    Ok(()) => break,
                    Err(error) => {
                        warn!(%error, "gateway configuration sync failed");
                        tokio::select! {
                            _ = tokio::time::sleep(Duration::from_secs(
                                self.config.gateway.retry_interval_seconds,
                            )) => {}
                            _ = self.gateway_notify.notified() => {}
                        }
                    }
                }
            }
        }
    }

    async fn sync_gateway_once(&self) -> Result<(), String> {
        let (generation, controller_set_generation, server, storage, endpoints) = {
            let mut inner = self.inner.lock().await;
            self.expire_local_lease(&mut inner);
            if !inner.is_leader {
                return Ok(());
            }
            let (controller_set_generation, voters) = self.repository.controller_set();
            (
                inner.generation,
                controller_set_generation,
                gateway::generate(&inner.state, &self.config.gateway.listen),
                gateway::storage(
                    controller_urls(&inner.state, Some(&self.config.advertise_url), &voters),
                    controller_set_generation,
                ),
                gateway_endpoints(&inner.state, self.config.gateway.admin_port),
            )
        };

        if endpoints.is_empty() {
            let mut sync = self.gateway_sync.lock().await;
            sync.applied_generation = None;
            sync.applied_controller_set_generations.clear();
            sync.endpoint_errors.clear();
            return Ok(());
        }

        let results = join_all(endpoints.iter().map(|endpoint| async {
            match self.push_gateway_storage(endpoint, &storage).await {
                Ok(()) => (true, self.push_gateway_server(endpoint, &server).await),
                Err(error) => (false, Err(error)),
            }
        }))
        .await;
        let mut endpoint_errors = BTreeMap::new();
        let mut storage_applied = Vec::new();
        for (endpoint, (storage_succeeded, result)) in endpoints.iter().cloned().zip(results) {
            if storage_succeeded {
                storage_applied.push(endpoint.clone());
            }
            if let Err(error) = result {
                endpoint_errors.insert(endpoint, error);
            }
        }
        {
            let mut sync = self.gateway_sync.lock().await;
            sync.endpoint_errors = endpoint_errors.clone();
            sync.applied_controller_set_generations
                .retain(|endpoint, _| endpoints.contains(endpoint));
            for endpoint in storage_applied {
                sync.applied_controller_set_generations
                    .insert(endpoint, controller_set_generation);
            }
            if endpoint_errors.is_empty() {
                sync.applied_generation = Some(generation);
            }
        }
        if !endpoint_errors.is_empty() {
            return Err(endpoint_errors
                .into_iter()
                .map(|(endpoint, error)| format!("{endpoint}: {error}"))
                .collect::<Vec<_>>()
                .join("; "));
        }

        info!(
            generation,
            controller_set_generation, "gateway configuration applied"
        );
        let mut inner = self.inner.lock().await;
        self.expire_local_lease(&mut inner);
        if !inner.is_leader || inner.generation != generation {
            self.gateway_notify.notify_one();
            return Ok(());
        }
        let deadline = unix_ms() + self.config.gateway.drain_timeout_seconds as i64 * 1000;
        let previous = inner.state.clone();
        let mut changed = false;
        for task in inner.state.tasks.values_mut() {
            if task.desired == DesiredTaskState::Draining && task.drain_until_unix_ms.is_none() {
                task.drain_until_unix_ms = Some(deadline);
                changed = true;
            }
        }
        if changed && let Err(error) = self.commit_locked(&mut inner).await {
            inner.state = previous;
            return Err(error.to_string());
        }
        Ok(())
    }

    async fn push_gateway_server(
        &self,
        endpoint: &str,
        server: &gateway::HttpServer,
    ) -> Result<(), String> {
        let url = format!(
            "{}/config/apps/http/servers/{}",
            endpoint.trim_end_matches('/'),
            self.config.gateway.server_name
        );
        let response = self
            .gateway_client
            .post(&url)
            .json(server)
            .send()
            .await
            .map_err(|error| error.to_string())?;
        if response.status().is_success() {
            return Ok(());
        }
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        let body = body.chars().take(512).collect::<String>();
        Err(format!("{status} {body}"))
    }

    async fn push_gateway_storage(
        &self,
        endpoint: &str,
        storage: &gateway::StorageConfig,
    ) -> Result<(), String> {
        let url = format!("{}/config/storage", endpoint.trim_end_matches('/'));
        let response = self
            .gateway_client
            .post(&url)
            .json(storage)
            .send()
            .await
            .map_err(|error| error.to_string())?;
        if response.status().is_success() {
            return Ok(());
        }
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        let body = body.chars().take(512).collect::<String>();
        Err(format!("{status} {body}"))
    }

    async fn put_kv(&self, request: KvPutRequest) -> Result<KvPutResponse, ControllerError> {
        let mut inner = self.inner.lock().await;
        self.expire_local_lease(&mut inner);
        if !inner.is_leader {
            return Err(self.leader_redirect("/v1/kv"));
        }
        let previous = inner.kv.clone();
        let response = kv::apply_put(&mut inner.kv, request).map_err(ControllerError::Invalid)?;
        if response.applied
            && let Err(error) = self.commit_kv_locked(&mut inner).await
        {
            inner.kv = previous;
            return Err(error.into());
        }
        Ok(response)
    }

    async fn delete_kv(&self, request: KvDeleteRequest) -> Result<KvPutResponse, ControllerError> {
        let mut inner = self.inner.lock().await;
        self.expire_local_lease(&mut inner);
        if !inner.is_leader {
            return Err(self.leader_redirect("/v1/kv"));
        }
        let previous = inner.kv.clone();
        let response =
            kv::apply_delete(&mut inner.kv, request).map_err(ControllerError::Invalid)?;
        if response.applied
            && let Err(error) = self.commit_kv_locked(&mut inner).await
        {
            inner.kv = previous;
            return Err(error.into());
        }
        Ok(response)
    }

    async fn kv_object(&self, key: &str) -> Result<KvObjectResponse, ControllerError> {
        let mut inner = self.inner.lock().await;
        self.expire_local_lease(&mut inner);
        if !inner.is_leader {
            return Err(self.leader_redirect_with_query("/v1/kv", &[("key", key.to_owned())]));
        }
        kv::get(&inner.kv, key)
            .map_err(ControllerError::Invalid)?
            .ok_or_else(|| ControllerError::NotFound(format!("KV key {key} was not found")))
    }

    async fn list_kv(
        &self,
        path: &str,
        recursive: bool,
    ) -> Result<KvListResponse, ControllerError> {
        let mut inner = self.inner.lock().await;
        self.expire_local_lease(&mut inner);
        if !inner.is_leader {
            return Err(self.leader_redirect_with_query(
                "/v1/kv/keys",
                &[
                    ("prefix", path.to_owned()),
                    ("recursive", recursive.to_string()),
                ],
            ));
        }
        kv::list(&inner.kv, path, recursive)
            .map_err(ControllerError::Invalid)?
            .ok_or_else(|| ControllerError::NotFound(format!("KV path {path} was not found")))
    }

    async fn stat_kv(&self, key: &str) -> Result<KvStatResponse, ControllerError> {
        let mut inner = self.inner.lock().await;
        self.expire_local_lease(&mut inner);
        if !inner.is_leader {
            return Err(self.leader_redirect_with_query("/v1/kv/stat", &[("key", key.to_owned())]));
        }
        kv::stat(&inner.kv, key)
            .map_err(ControllerError::Invalid)?
            .ok_or_else(|| ControllerError::NotFound(format!("KV key {key} was not found")))
    }

    async fn acquire_kv_lock(
        &self,
        request: KvLockAcquireRequest,
    ) -> Result<KvLockAcquireResponse, ControllerError> {
        validate_kv_lock_identity(&request.name, &request.owner_id)?;
        validate_kv_lock_lease(request.lease_millis)?;
        let mut inner = self.inner.lock().await;
        self.expire_local_lease(&mut inner);
        if !inner.is_leader {
            return Err(self.leader_redirect("/v1/kv/locks/acquire"));
        }

        let now = unix_ms();
        if let Some(lock) = inner.kv.locks.get(&request.name)
            && lock.lease_until_unix_ms > now
            && lock.owner_id != request.owner_id
        {
            return Ok(KvLockAcquireResponse {
                status: KvLockStatus::Busy,
                fencing_token: None,
                lease_until_unix_ms: Some(lock.lease_until_unix_ms),
                retry_after_millis: Some(
                    u64::try_from(lock.lease_until_unix_ms - now)
                        .unwrap_or(1_000)
                        .clamp(100, 1_000),
                ),
            });
        }

        let previous = inner.kv.clone();
        let lease_until_unix_ms = lease_deadline(now, request.lease_millis)?;
        let fencing_token = if let Some(lock) = inner.kv.locks.get_mut(&request.name)
            && lock.lease_until_unix_ms > now
            && lock.owner_id == request.owner_id
        {
            lock.lease_until_unix_ms = lease_until_unix_ms;
            lock.fencing_token
        } else {
            inner.kv.next_fencing_token = inner
                .kv
                .next_fencing_token
                .checked_add(1)
                .ok_or_else(|| ControllerError::Invalid("KV lock token overflow".to_owned()))?;
            let token = inner.kv.next_fencing_token;
            inner.kv.locks.insert(
                request.name,
                KvLock {
                    owner_id: request.owner_id,
                    fencing_token: token,
                    lease_until_unix_ms,
                },
            );
            token
        };
        if let Err(error) = self.commit_kv_locked(&mut inner).await {
            inner.kv = previous;
            return Err(error.into());
        }
        Ok(KvLockAcquireResponse {
            status: KvLockStatus::Acquired,
            fencing_token: Some(fencing_token),
            lease_until_unix_ms: Some(lease_until_unix_ms),
            retry_after_millis: None,
        })
    }

    async fn renew_kv_lock(&self, request: KvLockMutationRequest) -> Result<(), ControllerError> {
        validate_kv_lock_identity(&request.name, &request.owner_id)?;
        let lease_millis = request
            .lease_millis
            .ok_or_else(|| ControllerError::Invalid("lease_millis is required".to_owned()))?;
        validate_kv_lock_lease(lease_millis)?;
        let mut inner = self.inner.lock().await;
        self.expire_local_lease(&mut inner);
        if !inner.is_leader {
            return Err(self.leader_redirect("/v1/kv/locks/renew"));
        }
        let now = unix_ms();
        let previous = inner.kv.clone();
        let lock = inner
            .kv
            .locks
            .get_mut(&request.name)
            .filter(|lock| {
                lock.owner_id == request.owner_id
                    && lock.fencing_token == request.fencing_token
                    && lock.lease_until_unix_ms > now
            })
            .ok_or_else(|| {
                ControllerError::Conflict("the KV lock is no longer owned".to_owned())
            })?;
        lock.lease_until_unix_ms = lease_deadline(now, lease_millis)?;
        if let Err(error) = self.commit_kv_locked(&mut inner).await {
            inner.kv = previous;
            return Err(error.into());
        }
        Ok(())
    }

    async fn release_kv_lock(&self, request: KvLockMutationRequest) -> Result<(), ControllerError> {
        validate_kv_lock_identity(&request.name, &request.owner_id)?;
        let mut inner = self.inner.lock().await;
        self.expire_local_lease(&mut inner);
        if !inner.is_leader {
            return Err(self.leader_redirect("/v1/kv/locks/release"));
        }
        let Some(lock) = inner.kv.locks.get(&request.name) else {
            return Ok(());
        };
        if lock.owner_id != request.owner_id || lock.fencing_token != request.fencing_token {
            return Err(ControllerError::Conflict(
                "the KV lock is owned by another writer".to_owned(),
            ));
        }
        let previous = inner.kv.clone();
        inner.kv.locks.remove(&request.name);
        if let Err(error) = self.commit_kv_locked(&mut inner).await {
            inner.kv = previous;
            return Err(error.into());
        }
        Ok(())
    }

    fn authorized(&self, headers: &HeaderMap) -> bool {
        headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .is_some_and(|token| constant_time_eq(token.as_bytes(), self.token.as_bytes()))
    }
}

async fn health(State(controller): State<Arc<Controller>>) -> Json<serde_json::Value> {
    let status = controller.status().await;
    Json(json!({
        "ok": true,
        "controller_id": controller.config.controller_id,
        "is_leader": status.is_leader,
        "leader": status.leader,
        "generation": status.generation,
        "controller_set_generation": status.controller_set_generation,
    }))
}

async fn status(
    State(controller): State<Arc<Controller>>,
    headers: HeaderMap,
) -> Result<Json<StatusResponse>, ControllerError> {
    require_auth(&controller, &headers)?;
    controller.leader_status().await.map(Json)
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

async fn join_node(
    State(controller): State<Arc<Controller>>,
    Path(node_id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<JoinRequest>,
) -> Result<Json<JoinResponse>, ControllerError> {
    require_auth(&controller, &headers)?;
    controller.join_node(&node_id, body).await.map(Json)
}

async fn get_node_roles(
    State(controller): State<Arc<Controller>>,
    Path(node_id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<NodeRolesResponse>, ControllerError> {
    require_auth(&controller, &headers)?;
    controller.node_roles(&node_id).await.map(Json)
}

async fn set_node_roles(
    State(controller): State<Arc<Controller>>,
    Path(node_id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<NodeRolesUpdate>,
) -> Result<Json<NodeRolesResponse>, ControllerError> {
    require_auth(&controller, &headers)?;
    controller
        .update_node_roles(&node_id, body, RoleOperation::Set)
        .await
        .map(Json)
}

async fn add_node_roles(
    State(controller): State<Arc<Controller>>,
    Path(node_id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<NodeRolesUpdate>,
) -> Result<Json<NodeRolesResponse>, ControllerError> {
    require_auth(&controller, &headers)?;
    controller
        .update_node_roles(&node_id, body, RoleOperation::Add)
        .await
        .map(Json)
}

async fn remove_node_roles(
    State(controller): State<Arc<Controller>>,
    Path(node_id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<NodeRolesUpdate>,
) -> Result<Json<NodeRolesResponse>, ControllerError> {
    require_auth(&controller, &headers)?;
    controller
        .update_node_roles(&node_id, body, RoleOperation::Remove)
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
) -> Result<Json<serde_json::Value>, ControllerError> {
    require_auth(&controller, &headers)?;
    let yaml =
        std::str::from_utf8(&body).map_err(|error| ControllerError::Invalid(error.to_string()))?;
    let parsed = parse_stack(yaml).map_err(|error| ControllerError::Invalid(error.to_string()))?;
    let generation = controller.apply(&name, parsed).await?;
    Ok(Json(json!({
        "stack": name,
        "generation": generation,
        "status": "accepted"
    })))
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

async fn gateway_config(
    State(controller): State<Arc<Controller>>,
    headers: HeaderMap,
) -> Result<Json<gateway::HttpServer>, ControllerError> {
    require_auth(&controller, &headers)?;
    controller.gateway().await.map(Json)
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
) -> Result<Json<KvPutResponse>, ControllerError> {
    require_auth(&controller, &headers)?;
    controller.put_kv(body).await.map(Json)
}

async fn delete_kv(
    State(controller): State<Arc<Controller>>,
    headers: HeaderMap,
    Json(body): Json<KvDeleteRequest>,
) -> Result<Json<KvPutResponse>, ControllerError> {
    require_auth(&controller, &headers)?;
    controller.delete_kv(body).await.map(Json)
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
            Self::NotLeader(Some(location)) => {
                let mut response = (
                    StatusCode::TEMPORARY_REDIRECT,
                    Json(json!({"error": "not leader", "leader": location})),
                )
                    .into_response();
                if let Ok(value) = location.parse() {
                    response.headers_mut().insert(header::LOCATION, value);
                }
                response
            }
            Self::NotLeader(None) => (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "no active leader"})),
            )
                .into_response(),
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

fn validate_kv_lock_identity(name: &str, owner_id: &str) -> Result<(), ControllerError> {
    if name.trim().is_empty() || name.len() > 1_024 {
        return Err(ControllerError::Invalid(
            "KV lock name must contain 1 to 1024 bytes".to_owned(),
        ));
    }
    if owner_id.trim().is_empty() || owner_id.len() > 512 {
        return Err(ControllerError::Invalid(
            "KV lock owner_id must contain 1 to 512 bytes".to_owned(),
        ));
    }
    Ok(())
}

fn validate_kv_lock_lease(lease_millis: u64) -> Result<(), ControllerError> {
    if !(MIN_KV_LOCK_LEASE_MS..=MAX_KV_LOCK_LEASE_MS).contains(&lease_millis) {
        return Err(ControllerError::Invalid(format!(
            "KV lock lease_millis must be between {MIN_KV_LOCK_LEASE_MS} and {MAX_KV_LOCK_LEASE_MS}"
        )));
    }
    Ok(())
}

fn lease_deadline(now: i64, lease_millis: u64) -> Result<i64, ControllerError> {
    let lease_millis = i64::try_from(lease_millis)
        .map_err(|_| ControllerError::Invalid("KV lock lease is too large".to_owned()))?;
    now.checked_add(lease_millis)
        .ok_or_else(|| ControllerError::Invalid("KV lock lease overflow".to_owned()))
}

fn current_live_nodes(inner: &Inner, timeout_seconds: u64) -> BTreeSet<String> {
    let now = Instant::now();
    let timeout = Duration::from_secs(timeout_seconds);
    inner
        .live_nodes
        .iter()
        .filter(|(_, seen)| now.duration_since(**seen) <= timeout)
        .map(|(id, _)| id.clone())
        .collect()
}

fn pending_controller_set_acknowledgements(
    inner: &Inner,
    timeout_seconds: u64,
    controller_set_generation: u64,
) -> BTreeSet<String> {
    let now = Instant::now();
    let timeout = Duration::from_secs(timeout_seconds);
    let mut candidates = current_live_nodes(inner, timeout_seconds);
    candidates.extend(
        inner
            .controller_ack_candidates
            .iter()
            .filter(|(_, seen)| now.duration_since(**seen) <= timeout)
            .map(|(id, _)| id.clone()),
    );
    candidates
        .into_iter()
        .filter(|node_id| {
            inner
                .state
                .nodes
                .get(node_id)
                .is_none_or(|node| node.controller_set_generation < controller_set_generation)
        })
        .collect()
}

fn pending_gateway_controller_set_acknowledgements(
    inner: &Inner,
    sync: &GatewaySyncState,
    timeout_seconds: u64,
    admin_port: u16,
    controller_set_generation: u64,
) -> BTreeSet<String> {
    current_live_nodes(inner, timeout_seconds)
        .into_iter()
        .filter_map(|node_id| inner.state.nodes.get(&node_id))
        .filter(|node| node.roles.contains(&NodeRole::Gateway))
        .map(|node| format!("http://{}:{admin_port}", format_host(&node.address)))
        .filter(|endpoint| {
            sync.applied_controller_set_generations
                .get(endpoint)
                .is_none_or(|generation| *generation < controller_set_generation)
        })
        .collect()
}

fn normalized_roles(mut roles: NodeRoles) -> NodeRoles {
    roles.insert(NodeRole::Agent);
    roles
}

fn validate_node_label_key(key: &str) -> Result<(), ControllerError> {
    if key.is_empty()
        || key.len() > 256
        || key.trim() != key
        || key.contains('=')
        || key.chars().any(char::is_control)
    {
        return Err(ControllerError::Invalid(
            "node label key must contain 1 to 256 bytes without control characters, '=' or surrounding whitespace"
                .to_owned(),
        ));
    }
    Ok(())
}

fn validate_node_label(key: &str, value: &str) -> Result<(), ControllerError> {
    validate_node_label_key(key)?;
    if value.len() > 4_096 || value.chars().any(char::is_control) {
        return Err(ControllerError::Invalid(
            "node label value must contain at most 4096 bytes without control characters"
                .to_owned(),
        ));
    }
    Ok(())
}

fn role_count(state: &ClusterState, role: NodeRole, except_node: Option<&str>) -> usize {
    state
        .members
        .values()
        .filter(|member| Some(member.id.as_str()) != except_node)
        .filter(|member| member.roles.contains(&role))
        .count()
}

fn validate_role_limits(
    state: &ClusterState,
    node_id: &str,
    roles: &NodeRoles,
    mode: ClusterMode,
) -> Result<(), ControllerError> {
    if !roles.contains(&NodeRole::Agent) {
        return Err(ControllerError::Invalid(
            "every node must have the agent role".to_owned(),
        ));
    }
    let controller_count = role_count(state, NodeRole::Controller, Some(node_id))
        + usize::from(roles.contains(&NodeRole::Controller));
    if controller_count > mode.controller_limit() {
        return Err(ControllerError::Conflict(format!(
            "{mode:?} allows at most {} controller role(s)",
            mode.controller_limit()
        )));
    }
    Ok(())
}

fn automatic_join_roles(state: &ClusterState, mode: ClusterMode) -> NodeRoles {
    let mut roles = agent_roles();
    if mode == ClusterMode::Ha
        && role_count(state, NodeRole::Controller, None) < mode.controller_limit()
    {
        roles.insert(NodeRole::Controller);
    }
    roles
}

fn fill_automatic_ha_controllers(state: &mut ClusterState) {
    let mut controller_count = role_count(state, NodeRole::Controller, None);
    let mut candidates = state
        .members
        .values()
        .filter(|member| member.automatic_roles && member.roles.contains(&NodeRole::Agent))
        .map(|member| (member.joined_at_unix_ms, member.id.clone()))
        .collect::<Vec<_>>();
    candidates.sort();
    for (_, node_id) in candidates {
        let member = state
            .members
            .get_mut(&node_id)
            .expect("role candidate must still exist");
        if controller_count < ClusterMode::Ha.controller_limit()
            && !member.roles.contains(&NodeRole::Controller)
        {
            member.roles.insert(NodeRole::Controller);
            controller_count += 1;
        }
        if controller_count == ClusterMode::Ha.controller_limit() {
            break;
        }
    }
}

fn ensure_controller_record(
    state: &mut ClusterState,
    node_id: &str,
    now_unix_ms: i64,
) -> Result<bool, ControllerError> {
    let member = state
        .members
        .get(node_id)
        .ok_or_else(|| ControllerError::NotFound(format!("node {node_id} is not joined")))?;
    if member.controller_url.trim().is_empty() || member.raft_id == 0 || member.raft_url.is_empty()
    {
        return Err(ControllerError::Invalid(format!(
            "node {node_id} has an invalid controller identity"
        )));
    }
    let changed = state.controllers.get(node_id).is_none_or(|record| {
        record.advertise_url != member.controller_url
            || record.raft_id != member.raft_id
            || record.raft_url != member.raft_url
    });
    if changed {
        state.controllers.insert(
            node_id.to_owned(),
            ControllerRecord {
                node_id: node_id.to_owned(),
                advertise_url: member.controller_url.trim_end_matches('/').to_owned(),
                raft_id: member.raft_id,
                raft_url: member.raft_url.clone(),
                reserved_at_unix_ms: now_unix_ms,
            },
        );
    }
    Ok(changed)
}

fn gateway_endpoints(state: &ClusterState, admin_port: u16) -> Vec<String> {
    state
        .nodes
        .values()
        .filter(|node| node.roles.contains(&NodeRole::Gateway))
        .map(|node| format!("http://{}:{admin_port}", format_host(&node.address)))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn format_host(host: &str) -> String {
    if host.contains(':') && !(host.starts_with('[') && host.ends_with(']')) {
        format!("[{host}]")
    } else {
        host.to_owned()
    }
}

fn prune_controllers(state: &mut ClusterState, now_unix_ms: i64, voters: &BTreeSet<u64>) -> bool {
    let previous_len = state.controllers.len();
    state.controllers.retain(|_, record| {
        voters.contains(&record.raft_id)
            || now_unix_ms.saturating_sub(record.reserved_at_unix_ms) <= CONTROLLER_TIMEOUT_MS
    });
    state.controllers.len() != previous_len
}

fn controller_urls(
    state: &ClusterState,
    fallback: Option<&str>,
    voters: &BTreeSet<u64>,
) -> Vec<String> {
    let mut urls = state
        .controllers
        .values()
        .filter(|record| voters.contains(&record.raft_id))
        .map(|record| record.advertise_url.trim_end_matches('/').to_owned())
        .collect::<BTreeSet<_>>();
    if urls.is_empty()
        && let Some(fallback) = fallback
    {
        urls.insert(fallback.trim_end_matches('/').to_owned());
    }
    urls.into_iter().collect()
}

fn adopt_unclaimed_tasks(state: &mut ClusterState, stack_name: &str) {
    let services = state
        .services
        .values()
        .filter(|service| service.stack == stack_name && !service.deleted)
        .cloned()
        .collect::<Vec<_>>();
    let mut adopted = 0_usize;
    for service in services {
        let spec_hash = service_spec_hash(&service.spec);
        let routed_ports = gateway::service_ports(state, &service);
        let mut occupied_slots = state
            .tasks
            .values()
            .filter(|task| {
                task.service_id == service.id && task.desired != DesiredTaskState::Stopped
            })
            .map(|task| task.slot)
            .collect::<BTreeSet<_>>();
        let mut candidates = state
            .unclaimed_tasks
            .values()
            .filter(|task| {
                task.stack == stack_name
                    && task.service == service.name
                    && task.spec_hash == spec_hash
                    && task.slot < service.spec.replicas
                    && routed_ports.iter().all(|target| {
                        task.ports
                            .iter()
                            .any(|port| port.target == *target && port.protocol == "tcp")
                    })
            })
            .cloned()
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| {
            left.slot
                .cmp(&right.slot)
                .then_with(|| {
                    recovery_task_priority(&right.observed)
                        .cmp(&recovery_task_priority(&left.observed))
                })
                .then_with(|| right.revision.cmp(&left.revision))
                .then_with(|| left.id.cmp(&right.id))
        });
        for candidate in candidates {
            if occupied_slots.contains(&candidate.slot) || state.tasks.contains_key(&candidate.id) {
                continue;
            }
            occupied_slots.insert(candidate.slot);
            state.tasks.insert(
                candidate.id.clone(),
                TaskRecord {
                    id: candidate.id.clone(),
                    service_id: service.id.clone(),
                    revision: service.revision,
                    slot: candidate.slot,
                    node_id: candidate.node_id,
                    desired: DesiredTaskState::Running,
                    observed: candidate.observed,
                    ports: candidate.ports,
                    container_id: candidate.container_id,
                    drain_until_unix_ms: None,
                },
            );
            state.unclaimed_tasks.remove(&candidate.id);
            adopted += 1;
        }
    }
    if adopted > 0 {
        info!(
            stack = stack_name,
            adopted, "adopted existing task containers"
        );
    }
}

fn recovery_status(state: &ClusterState) -> RecoveryStatus {
    let mut slots = BTreeMap::new();
    for task in state.unclaimed_tasks.values() {
        *slots
            .entry((task.stack.clone(), task.service.clone(), task.slot))
            .or_insert(0_usize) += 1;
    }
    RecoveryStatus {
        awaiting_adoption: state.unclaimed_tasks.len(),
        conflicting_slots: slots.values().filter(|count| **count > 1).count(),
    }
}

fn recovery_task_priority(state: &ObservedTaskState) -> u8 {
    match state {
        ObservedTaskState::Healthy => 5,
        ObservedTaskState::Running => 4,
        ObservedTaskState::Starting => 3,
        ObservedTaskState::Pending => 2,
        ObservedTaskState::Failed => 1,
        ObservedTaskState::Lost => 0,
    }
}

fn service_id(stack: &str, service: &str) -> String {
    format!("{stack}.{service}")
}

fn validate_gateway_hostname_ownership(
    state: &ClusterState,
    stack_name: &str,
    gateway: &StackGatewaySpec,
) -> Result<(), ControllerError> {
    let requested = gateway
        .http_routes
        .iter()
        .flat_map(|route| route.hostnames.iter())
        .collect::<BTreeSet<_>>();
    for stack in state
        .stacks
        .values()
        .filter(|stack| stack.name != stack_name)
    {
        if let Some(hostname) = stack
            .gateway
            .http_routes
            .iter()
            .flat_map(|route| route.hostnames.iter())
            .find(|hostname| requested.contains(hostname))
        {
            return Err(ControllerError::Conflict(format!(
                "gateway hostname {hostname:?} is already owned by stack {:?}",
                stack.name
            )));
        }
    }
    Ok(())
}

fn validate_stack_name(name: &str) -> Result<(), ControllerError> {
    if name.is_empty()
        || !name
            .chars()
            .all(|value| value.is_ascii_alphanumeric() || matches!(value, '-' | '_' | '.'))
    {
        Err(ControllerError::Invalid(
            "stack name may contain only letters, numbers, '.', '-' and '_'".to_owned(),
        ))
    } else {
        Ok(())
    }
}

fn unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
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
    use std::collections::BTreeMap;
    use std::time::Duration;

    use axum::routing::post;
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use swarmlite_raft::{ControllerNode, NodeConfig, RaftNode};

    use crate::{
        config::GatewayConfig,
        model::{
            CLUSTER_SCHEMA_VERSION, ClusterGatewayConfig, KvVersion, NodeRecord, PortBinding,
            ServicePort, ServiceSpec, StackGatewaySpec, TaskRecord, TaskReport, initial_roles,
        },
    };

    use super::*;

    fn test_controller_config(cluster: &ClusterSettings) -> ControllerConfig {
        ControllerConfig {
            controller_id: "controller-a".into(),
            roles: initial_roles(),
            labels: BTreeMap::new(),
            listen: "127.0.0.1:0".parse().unwrap(),
            advertise_url: "http://10.0.0.10:8080".into(),
            node_timeout_seconds: 20,
            reconcile_interval_seconds: 1,
            gateway: GatewayConfig::default(),
            cluster: cluster.clone(),
        }
    }

    #[test]
    fn rejects_a_gateway_hostname_owned_by_another_stack() {
        let gateway = parse_stack(
            r#"
services:
  web:
    image: nginx
x-swarmlite:
  http_routes:
    - hostnames: [EXAMPLE.com]
      rules:
        - backend: { service: web, port: 80 }
"#,
        )
        .unwrap()
        .gateway;
        let mut state = ClusterState::default();
        state.stacks.insert(
            "first".into(),
            StackRecord {
                name: "first".into(),
                applied_at_unix_ms: 1,
                services: vec!["first.web".into()],
                gateway: gateway.clone(),
            },
        );

        let error = validate_gateway_hostname_ownership(&state, "second", &gateway).unwrap_err();
        assert!(matches!(
            error,
            ControllerError::Conflict(message)
                if message.contains("example.com") && message.contains("first")
        ));
        validate_gateway_hostname_ownership(&state, "first", &gateway).unwrap();
    }

    #[tokio::test]
    async fn kv_is_lww_and_locks_are_fenced() {
        let cluster = ClusterSettings {
            schema_version: CLUSTER_SCHEMA_VERSION,
            cluster_id: "kv-test".into(),
            mode: ClusterMode::Standalone,
            controller_port: 8080,
            gateway: ClusterGatewayConfig::default(),
        };
        let (repository, _raft, _directory) = test_repository(&cluster).await;
        let controller = Controller::new(
            test_controller_config(&cluster),
            "secret".into(),
            repository.clone(),
        )
        .await
        .unwrap();
        controller.tick().await.unwrap();

        let new_version = KvVersion {
            physical_unix_ms: 20,
            logical: 0,
            replica_id: "writer-a".into(),
        };
        assert!(
            controller
                .put_kv(KvPutRequest {
                    key: "apps/demo/config".into(),
                    value_base64: STANDARD.encode("new"),
                    version: new_version.clone(),
                    modified_at_unix_ms: 20,
                })
                .await
                .unwrap()
                .applied
        );
        assert!(
            !controller
                .put_kv(KvPutRequest {
                    key: "apps/demo/config".into(),
                    value_base64: STANDARD.encode("old"),
                    version: KvVersion {
                        physical_unix_ms: 10,
                        logical: 0,
                        replica_id: "writer-b".into(),
                    },
                    modified_at_unix_ms: 10,
                })
                .await
                .unwrap()
                .applied
        );
        assert_eq!(
            controller
                .kv_object("apps/demo/config")
                .await
                .unwrap()
                .value_base64,
            STANDARD.encode("new")
        );
        assert_eq!(
            repository.load_consistent().await.unwrap().kv.objects["apps/demo/config"].version,
            new_version
        );

        let acquired = controller
            .acquire_kv_lock(KvLockAcquireRequest {
                name: "jobs/demo".into(),
                owner_id: "writer-a".into(),
                lease_millis: 30_000,
            })
            .await
            .unwrap();
        assert_eq!(acquired.status, KvLockStatus::Acquired);
        let token = acquired.fencing_token.unwrap();
        let busy = controller
            .acquire_kv_lock(KvLockAcquireRequest {
                name: "jobs/demo".into(),
                owner_id: "writer-b".into(),
                lease_millis: 30_000,
            })
            .await
            .unwrap();
        assert_eq!(busy.status, KvLockStatus::Busy);
        assert!(
            controller
                .release_kv_lock(KvLockMutationRequest {
                    name: "jobs/demo".into(),
                    owner_id: "writer-b".into(),
                    fencing_token: token,
                    lease_millis: None,
                })
                .await
                .is_err()
        );
        controller
            .release_kv_lock(KvLockMutationRequest {
                name: "jobs/demo".into(),
                owner_id: "writer-a".into(),
                fencing_token: token,
                lease_millis: None,
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn ha_automatically_fills_only_controller_roles() {
        let cluster = ClusterSettings {
            schema_version: CLUSTER_SCHEMA_VERSION,
            cluster_id: "controller-assignment-test".into(),
            mode: ClusterMode::Ha,
            controller_port: 8080,
            gateway: ClusterGatewayConfig::default(),
        };
        let (repository, _raft, _directory) = test_repository(&cluster).await;
        let config = test_controller_config(&cluster);
        let controller = Controller::new(config, "secret".into(), repository)
            .await
            .unwrap();
        controller.tick().await.unwrap();

        let first = controller
            .join_node("node-b", test_join_request("node-b", "10.0.0.22", 2))
            .await
            .unwrap();
        assert_eq!(
            first.roles,
            BTreeSet::from([NodeRole::Controller, NodeRole::Agent])
        );

        let second = controller
            .join_node("node-c", test_join_request("node-c", "10.0.0.23", 3))
            .await
            .unwrap();
        assert_eq!(
            second.roles,
            BTreeSet::from([NodeRole::Controller, NodeRole::Agent])
        );

        let third = controller
            .join_node("node-d", test_join_request("node-d", "10.0.0.24", 4))
            .await
            .unwrap();
        assert_eq!(third.roles, agent_roles());
        assert_eq!(controller.inner.lock().await.state.controllers.len(), 3);
        assert_eq!(
            role_count(
                &controller.inner.lock().await.state,
                NodeRole::Gateway,
                None
            ),
            1
        );

        let mut explicit = test_join_request("node-e", "10.0.0.25", 5);
        explicit.requested_roles = Some(BTreeSet::from([NodeRole::Gateway]));
        let joined = controller.join_node("node-e", explicit).await.unwrap();
        assert_eq!(
            joined.roles,
            BTreeSet::from([NodeRole::Agent, NodeRole::Gateway])
        );
        let error = controller
            .update_node_roles(
                "controller-a",
                NodeRolesUpdate {
                    roles: BTreeSet::from([NodeRole::Controller]),
                },
                RoleOperation::Remove,
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            ControllerError::Conflict(message) if message.contains("last active controller voter")
        ));
        controller
            .update_node_roles(
                "controller-a",
                NodeRolesUpdate {
                    roles: BTreeSet::from([NodeRole::Gateway]),
                },
                RoleOperation::Remove,
            )
            .await
            .unwrap();
        assert!(matches!(
            controller
                .update_node_roles(
                    "node-e",
                    NodeRolesUpdate {
                        roles: BTreeSet::from([NodeRole::Gateway]),
                    },
                    RoleOperation::Remove,
                )
                .await,
            Err(ControllerError::Conflict(_))
        ));

        let mut conflicting = test_join_request("node-f", "10.0.0.26", 6);
        conflicting.requested_roles = Some(BTreeSet::from([NodeRole::Controller]));
        assert!(matches!(
            controller.join_node("node-f", conflicting).await,
            Err(ControllerError::Conflict(_))
        ));
    }

    #[tokio::test]
    async fn standalone_keeps_one_controller_and_allows_unlimited_gateways() {
        let cluster = ClusterSettings {
            schema_version: CLUSTER_SCHEMA_VERSION,
            cluster_id: "standalone-role-test".into(),
            mode: ClusterMode::Standalone,
            controller_port: 8080,
            gateway: ClusterGatewayConfig::default(),
        };
        let (repository, _raft, _directory) = test_repository(&cluster).await;
        let controller = Controller::new(
            test_controller_config(&cluster),
            "secret".into(),
            repository,
        )
        .await
        .unwrap();
        controller.tick().await.unwrap();

        for (node_id, address, raft_id) in [
            ("gateway-b", "10.0.0.22", 2),
            ("gateway-c", "10.0.0.23", 3),
            ("gateway-d", "10.0.0.24", 4),
        ] {
            let mut request = test_join_request(node_id, address, raft_id);
            if node_id == "gateway-b" {
                request.recovered_roles = BTreeSet::from([NodeRole::Gateway]);
            } else {
                request.requested_roles = Some(BTreeSet::from([NodeRole::Gateway]));
            }
            let joined = controller.join_node(node_id, request).await.unwrap();
            assert_eq!(
                joined.roles,
                BTreeSet::from([NodeRole::Agent, NodeRole::Gateway])
            );
        }

        let joined = controller
            .join_node("agent-e", test_join_request("agent-e", "10.0.0.25", 5))
            .await
            .unwrap();
        assert_eq!(joined.roles, agent_roles());

        let mut request = test_join_request("controller-f", "10.0.0.26", 6);
        request.requested_roles = Some(BTreeSet::from([NodeRole::Controller]));
        assert!(matches!(
            controller.join_node("controller-f", request).await,
            Err(ControllerError::Conflict(_))
        ));
    }

    #[tokio::test]
    async fn switches_standalone_to_ha_but_not_back() {
        let cluster = ClusterSettings {
            schema_version: CLUSTER_SCHEMA_VERSION,
            cluster_id: "config-update-test".into(),
            mode: ClusterMode::Standalone,
            controller_port: 8080,
            gateway: ClusterGatewayConfig::default(),
        };
        let (repository, _raft, _directory) = test_repository(&cluster).await;
        let observer = repository.clone();
        let config = test_controller_config(&cluster);
        let controller = Controller::new(config, "secret".into(), repository)
            .await
            .unwrap();
        controller.tick().await.unwrap();

        let updated = controller
            .update_cluster_config(ClusterConfigUpdate {
                mode: Some(ClusterMode::Ha),
                gateway_image: None,
            })
            .await
            .unwrap();
        assert_eq!(updated.config.mode, ClusterMode::Ha);
        assert_eq!(
            observer.load_consistent().await.unwrap().cluster.mode,
            ClusterMode::Ha
        );
        assert_eq!(controller.get_cluster_config().await.unwrap(), updated);

        let joined = controller
            .join_node("node-b", test_join_request("node-b", "10.0.0.22", 2))
            .await
            .unwrap();
        assert!(joined.roles.contains(&NodeRole::Controller));

        let image = "ghcr.io/example/swarmlite-caddy:v2";
        let updated = controller
            .update_cluster_config(ClusterConfigUpdate {
                mode: None,
                gateway_image: Some(image.to_owned()),
            })
            .await
            .unwrap();
        assert_eq!(updated.config.gateway.image, image);
        assert_eq!(
            observer
                .load_consistent()
                .await
                .unwrap()
                .cluster
                .gateway
                .image,
            image
        );
        let error = controller
            .update_cluster_config(ClusterConfigUpdate {
                mode: None,
                gateway_image: Some("bad image".to_owned()),
            })
            .await
            .unwrap_err();
        assert!(matches!(error, ControllerError::Invalid(_)));
        assert_eq!(
            controller
                .get_cluster_config()
                .await
                .unwrap()
                .config
                .gateway
                .image,
            image
        );

        let error = controller
            .update_cluster_config(ClusterConfigUpdate {
                mode: Some(ClusterMode::Standalone),
                gateway_image: None,
            })
            .await
            .unwrap_err();
        assert!(matches!(error, ControllerError::Conflict(_)));
    }

    #[tokio::test]
    async fn node_labels_are_authoritative_and_persisted() {
        let cluster = ClusterSettings {
            schema_version: CLUSTER_SCHEMA_VERSION,
            cluster_id: "node-label-test".into(),
            mode: ClusterMode::Standalone,
            controller_port: 8080,
            gateway: ClusterGatewayConfig::default(),
        };
        let (repository, _raft, _directory) = test_repository(&cluster).await;
        let observer = repository.clone();
        let mut config = test_controller_config(&cluster);
        config.labels = BTreeMap::from([("region".into(), "cn-east".into())]);
        let controller = Controller::new(config, "secret".into(), repository)
            .await
            .unwrap();
        controller.tick().await.unwrap();
        assert_eq!(
            observer.load_consistent().await.unwrap().state.members["controller-a"].labels,
            BTreeMap::from([("region".into(), "cn-east".into())])
        );

        let mut request = test_join_request("node-a", "127.0.0.1", 2);
        request.labels = BTreeMap::from([("disk".into(), "ssd".into())]);
        let joined = controller.join_node("node-a", request).await.unwrap();
        assert_eq!(
            joined.labels,
            BTreeMap::from([("disk".into(), "ssd".into())])
        );

        let mut conflicting = test_join_request("node-a", "127.0.0.1", 2);
        conflicting.labels = BTreeMap::from([("disk".into(), "hdd".into())]);
        assert!(matches!(
            controller.join_node("node-a", conflicting).await,
            Err(ControllerError::Conflict(_))
        ));

        let mut reported = test_node();
        reported.labels = BTreeMap::from([
            ("disk".into(), "hdd".into()),
            ("untrusted".into(), "value".into()),
        ]);
        let response = controller
            .heartbeat(
                "node-a",
                NodeHeartbeat {
                    node: reported,
                    tasks: Vec::new(),
                },
            )
            .await
            .unwrap();
        assert_eq!(
            response.labels,
            BTreeMap::from([("disk".into(), "ssd".into())])
        );
        assert_eq!(
            controller.inner.lock().await.state.nodes["node-a"].labels,
            response.labels
        );

        let labels = controller
            .set_node_label(
                "node-a",
                NodeLabelSetRequest {
                    key: "region".into(),
                    value: "cn-north".into(),
                },
            )
            .await
            .unwrap();
        assert_eq!(labels.labels["region"], "cn-north");
        assert_eq!(
            observer.load_consistent().await.unwrap().state.members["node-a"].labels,
            labels.labels
        );

        let labels = controller
            .remove_node_label("node-a", NodeLabelRemoveRequest { key: "disk".into() })
            .await
            .unwrap();
        assert_eq!(
            labels.labels,
            BTreeMap::from([("region".into(), "cn-north".into())])
        );
        assert!(matches!(
            controller
                .set_node_label(
                    "node-a",
                    NodeLabelSetRequest {
                        key: " bad".into(),
                        value: "value".into(),
                    },
                )
                .await,
            Err(ControllerError::Invalid(_))
        ));
    }

    #[tokio::test]
    async fn promotes_reserved_controller_through_raft() {
        let caddy_received = Arc::new(Mutex::new(Vec::<serde_json::Value>::new()));
        let caddy_app = Router::new()
            .route("/config/storage", post(capture_gateway_config))
            .route(
                "/config/apps/http/servers/swarmlite",
                post(capture_gateway_config),
            )
            .with_state(caddy_received.clone());
        let caddy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let caddy_address = caddy_listener.local_addr().unwrap();
        let caddy_server =
            tokio::spawn(async move { axum::serve(caddy_listener, caddy_app).await.unwrap() });

        let cluster = ClusterSettings {
            schema_version: CLUSTER_SCHEMA_VERSION,
            cluster_id: "controller-promotion-test".into(),
            mode: ClusterMode::Ha,
            controller_port: 8080,
            gateway: ClusterGatewayConfig::default(),
        };
        let (repository, leader_raft, _leader_directory) = test_repository(&cluster).await;
        let mut config = test_controller_config(&cluster);
        config.advertise_url = "http://127.0.0.1:19090".into();
        config.gateway.admin_port = caddy_address.port();
        let controller = Controller::new(
            config,
            "0123456789abcdef0123456789abcdef".into(),
            repository,
        )
        .await
        .unwrap();
        controller.tick().await.unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let api_url = format!("http://{address}");
        let raft_url = format!("{api_url}/internal/raft");
        let follower_directory = tempfile::tempdir().unwrap();
        let follower_raft = RaftNode::open(NodeConfig::new(
            2,
            ControllerNode {
                raft_url: raft_url.clone(),
                api_url: api_url.clone(),
            },
            follower_directory.path(),
            cluster.cluster_id.clone(),
            "0123456789abcdef0123456789abcdef",
        ))
        .await
        .unwrap();
        let app = Router::new().nest("/internal/raft", follower_raft.rpc_router());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let joined = controller
            .join_node(
                "controller-b",
                JoinRequest {
                    node_id: "controller-b".into(),
                    address: address.ip().to_string(),
                    requested_roles: None,
                    recovered_roles: NodeRoles::new(),
                    controller_url: api_url.clone(),
                    raft_id: 2,
                    raft_url: raft_url.clone(),
                    labels: BTreeMap::new(),
                },
            )
            .await
            .unwrap();
        assert!(joined.roles.contains(&NodeRole::Controller));

        let joined_controller_set_generation = joined.controller_set_generation;
        let mut heartbeat_node = NodeRecord {
            id: "controller-b".into(),
            address: address.ip().to_string(),
            labels: BTreeMap::new(),
            cpu_millis: 1000,
            memory_bytes: 1024,
            port_range_start: 20_000,
            port_range_end: 29_999,
            roles: joined.roles,
            controller_url: api_url.clone(),
            raft_id: 2,
            raft_url,
            controller_set_generation: joined_controller_set_generation,
        };
        let response = controller
            .heartbeat(
                "controller-b",
                NodeHeartbeat {
                    node: heartbeat_node.clone(),
                    tasks: Vec::new(),
                },
            )
            .await
            .unwrap();

        assert!(response.roles.contains(&NodeRole::Controller));
        assert!(response.controllers.contains(&api_url));
        assert!(response.controller_set_generation > joined_controller_set_generation);
        assert!(leader_raft.voter_ids().contains(&2));
        heartbeat_node.controller_set_generation = response.controller_set_generation;
        controller
            .heartbeat(
                "controller-b",
                NodeHeartbeat {
                    node: heartbeat_node,
                    tasks: Vec::new(),
                },
            )
            .await
            .unwrap();
        assert!(
            controller
                .inner
                .lock()
                .await
                .state
                .controllers
                .contains_key("controller-b")
        );
        assert_eq!(
            controller.inner.lock().await.state.nodes["controller-b"].controller_set_generation,
            response.controller_set_generation
        );

        let current_controller_set_generation = response.controller_set_generation;
        let mut agent_request = test_join_request("node-c", "127.0.0.2", 3);
        agent_request.requested_roles = Some(BTreeSet::from([NodeRole::Agent, NodeRole::Gateway]));
        let joined_agent = controller.join_node("node-c", agent_request).await.unwrap();
        let mut agent_node = test_node();
        agent_node.id = "node-c".into();
        agent_node.address = caddy_address.ip().to_string();
        agent_node.roles = joined_agent.roles;
        agent_node.raft_id = 3;
        agent_node.controller_set_generation = current_controller_set_generation - 1;
        let agent_response = controller
            .heartbeat(
                "node-c",
                NodeHeartbeat {
                    node: agent_node.clone(),
                    tasks: Vec::new(),
                },
            )
            .await
            .unwrap();
        let error = controller
            .update_node_roles(
                "controller-b",
                NodeRolesUpdate {
                    roles: BTreeSet::from([NodeRole::Controller]),
                },
                RoleOperation::Remove,
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            ControllerError::Conflict(message)
                if message.contains("generation") && message.contains("node-c")
        ));

        agent_node.controller_set_generation = agent_response.controller_set_generation;
        controller
            .heartbeat(
                "node-c",
                NodeHeartbeat {
                    node: agent_node,
                    tasks: Vec::new(),
                },
            )
            .await
            .unwrap();
        let error = controller
            .update_node_roles(
                "controller-b",
                NodeRolesUpdate {
                    roles: BTreeSet::from([NodeRole::Controller]),
                },
                RoleOperation::Remove,
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            ControllerError::Conflict(message)
                if message.contains("Caddy gateways")
                    && message.contains(&caddy_address.to_string())
        ));

        controller.sync_gateway_once().await.unwrap();
        assert_eq!(
            controller
                .gateway_sync
                .lock()
                .await
                .applied_controller_set_generations[&format!("http://{caddy_address}")],
            current_controller_set_generation
        );
        controller
            .update_node_roles(
                "controller-b",
                NodeRolesUpdate {
                    roles: BTreeSet::from([NodeRole::Controller]),
                },
                RoleOperation::Remove,
            )
            .await
            .unwrap();
        assert!(!leader_raft.voter_ids().contains(&2));

        leader_raft.shutdown().await.unwrap();
        follower_raft.shutdown().await.unwrap();
        server.abort();
        caddy_server.abort();
    }

    #[tokio::test]
    async fn caddy_acknowledgement_starts_drain_deadline() {
        let received = Arc::new(Mutex::new(Vec::<serde_json::Value>::new()));
        let app = Router::new()
            .route("/config/storage", post(capture_gateway_config))
            .route(
                "/config/apps/http/servers/swarmlite",
                post(capture_gateway_config),
            )
            .with_state(received.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let cluster = ClusterSettings {
            schema_version: CLUSTER_SCHEMA_VERSION,
            cluster_id: "caddy-publisher-test".into(),
            mode: ClusterMode::Standalone,
            controller_port: 8080,
            gateway: ClusterGatewayConfig::default(),
        };
        let mut config = test_controller_config(&cluster);
        config.controller_id = "controller-test".into();
        config.gateway.admin_port = address.port();
        config.gateway.listen = vec![":18089".into()];
        config.gateway.drain_timeout_seconds = 3;
        let (repository, _raft, _directory) = test_repository(&cluster).await;
        let controller = Controller::new(config, "test-token".into(), repository)
            .await
            .unwrap();
        {
            let mut inner = controller.inner.lock().await;
            controller.try_acquire_locked(&mut inner).await.unwrap();
            let mut node = test_node();
            node.roles.insert(NodeRole::Gateway);
            inner.state.nodes.insert("node-a".into(), node);
            inner
                .state
                .services
                .insert("demo.web".into(), test_service());
            inner.state.stacks.insert(
                "demo".into(),
                StackRecord {
                    name: "demo".into(),
                    applied_at_unix_ms: unix_ms(),
                    services: vec!["demo.web".into()],
                    gateway: parse_stack(
                        r#"
services:
  web:
    image: nginx:1.29-alpine
x-swarmlite:
  http_routes:
    - hostnames: [example.com]
      rules:
        - backend: { service: web, port: 80 }
"#,
                    )
                    .unwrap()
                    .gateway,
                },
            );
            inner.state.tasks.insert("old-task".into(), draining_task());
            controller.commit_locked(&mut inner).await.unwrap();
        }

        let controller_set_generation = controller.repository.controller_set().0;
        controller.sync_gateway_once().await.unwrap();

        let inner = controller.inner.lock().await;
        let task = &inner.state.tasks["old-task"];
        assert_eq!(task.desired, DesiredTaskState::Draining);
        assert!(
            task.drain_until_unix_ms
                .is_some_and(|value| value > unix_ms())
        );
        drop(inner);
        let requests = received.lock().await;
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0]["module"], "swarmlite");
        assert_eq!(requests[0]["token_env"], "SWARMLITE_TOKEN");
        assert_eq!(
            requests[0]["controller_set_generation"],
            controller_set_generation
        );
        assert!(!requests[0]["controllers"].as_array().unwrap().is_empty());
        assert_eq!(requests[1]["listen"][0], ":18089");
        assert!(
            requests[1]["routes"]
                .as_array()
                .unwrap()
                .iter()
                .any(|route| {
                    route["handle"].as_array().is_some_and(|handlers| {
                        handlers.iter().any(|handler| handler["status_code"] == 503)
                    })
                })
        );
        server.abort();
    }

    async fn test_repository(
        cluster: &ClusterSettings,
    ) -> (StateRepository, Arc<RaftNode>, tempfile::TempDir) {
        let directory = tempfile::tempdir().unwrap();
        let raft = RaftNode::open(NodeConfig::new(
            1,
            ControllerNode {
                raft_url: "http://127.0.0.1:19090/internal/raft".into(),
                api_url: "http://127.0.0.1:19090".into(),
            },
            directory.path(),
            cluster.cluster_id.clone(),
            "0123456789abcdef0123456789abcdef",
        ))
        .await
        .unwrap();
        raft.initialize().await.unwrap();
        raft.raft()
            .wait(Some(Duration::from_secs(5)))
            .current_leader(1, "test controller becomes leader")
            .await
            .unwrap();
        (
            StateRepository::new(raft.clone(), cluster.clone()),
            raft,
            directory,
        )
    }

    fn test_join_request(node_id: &str, address: &str, raft_id: u64) -> JoinRequest {
        let controller_url = format!("http://{address}:8080");
        JoinRequest {
            node_id: node_id.to_owned(),
            address: address.to_owned(),
            requested_roles: None,
            recovered_roles: NodeRoles::new(),
            controller_url: controller_url.clone(),
            raft_id,
            raft_url: format!("{controller_url}/internal/raft"),
            labels: BTreeMap::new(),
        }
    }

    async fn capture_gateway_config(
        State(received): State<Arc<Mutex<Vec<serde_json::Value>>>>,
        Json(body): Json<serde_json::Value>,
    ) -> StatusCode {
        received.lock().await.push(body);
        StatusCode::OK
    }

    fn test_node() -> NodeRecord {
        NodeRecord {
            id: "node-a".into(),
            address: "127.0.0.1".into(),
            labels: BTreeMap::new(),
            cpu_millis: 1000,
            memory_bytes: 1024,
            port_range_start: 20_000,
            port_range_end: 29_999,
            roles: agent_roles(),
            controller_url: "http://127.0.0.1:8080".into(),
            raft_id: 2,
            raft_url: "http://127.0.0.1:8080/internal/raft".into(),
            controller_set_generation: 0,
        }
    }

    fn test_service() -> ServiceRecord {
        ServiceRecord {
            id: "demo.web".into(),
            stack: "demo".into(),
            name: "web".into(),
            revision: 2,
            spec: ServiceSpec {
                image: "nginx:1.29-alpine".into(),
                command: Vec::new(),
                entrypoint: Vec::new(),
                environment: Vec::new(),
                ports: vec![ServicePort {
                    target: 80,
                    published: None,
                    protocol: "tcp".into(),
                }],
                volumes: Vec::new(),
                container_labels: BTreeMap::new(),
                service_labels: BTreeMap::new(),
                healthcheck: None,
                replicas: 1,
                constraints: Vec::new(),
                max_surge: 1,
                stop_grace_period_seconds: 10,
            },
            deleted: false,
        }
    }

    #[tokio::test]
    async fn heartbeat_then_deploy_adopts_the_existing_container() {
        let cluster = ClusterSettings {
            schema_version: CLUSTER_SCHEMA_VERSION,
            cluster_id: "recovery-test".into(),
            mode: ClusterMode::Standalone,
            controller_port: 8080,
            gateway: ClusterGatewayConfig::default(),
        };
        let (repository, _raft, _directory) = test_repository(&cluster).await;
        let config = test_controller_config(&cluster);
        let controller = Controller::new(config, "secret".into(), repository)
            .await
            .unwrap();
        controller.tick().await.unwrap();
        controller
            .join_node("node-a", test_join_request("node-a", "127.0.0.1", 2))
            .await
            .unwrap();

        let mut service = test_service();
        service.spec.service_labels.clear();
        let spec_hash = service_spec_hash(&service.spec);
        controller
            .heartbeat(
                "node-a",
                NodeHeartbeat {
                    node: test_node(),
                    tasks: vec![TaskReport {
                        id: "existing-task".into(),
                        observed: ObservedTaskState::Healthy,
                        container_id: Some("container-existing".into()),
                        cluster_id: Some(cluster.cluster_id.clone()),
                        stack: Some("demo".into()),
                        service: Some("web".into()),
                        slot: Some(0),
                        revision: Some(7),
                        spec_hash: Some(spec_hash),
                        ports: vec![PortBinding {
                            target: 80,
                            published: 20_001,
                            protocol: "tcp".into(),
                        }],
                    }],
                },
            )
            .await
            .unwrap();
        assert!(
            controller
                .inner
                .lock()
                .await
                .state
                .unclaimed_tasks
                .contains_key("existing-task")
        );

        controller
            .apply(
                "demo",
                ParsedStack {
                    services: BTreeMap::from([("web".into(), service.spec)]),
                    gateway: StackGatewaySpec::default(),
                },
            )
            .await
            .unwrap();

        let inner = controller.inner.lock().await;
        assert_eq!(inner.state.tasks.len(), 1);
        let task = &inner.state.tasks["existing-task"];
        assert_eq!(task.container_id.as_deref(), Some("container-existing"));
        assert_eq!(task.ports[0].published, 20_001);
        assert!(inner.state.unclaimed_tasks.is_empty());
    }

    #[test]
    fn adopts_matching_unclaimed_container_by_stack_service_and_slot() {
        let service = test_service();
        let spec_hash = service_spec_hash(&service.spec);
        let mut state = ClusterState::default();
        state.services.insert(service.id.clone(), service);
        state.unclaimed_tasks.insert(
            "existing-task".into(),
            UnclaimedTask {
                id: "existing-task".into(),
                stack: "demo".into(),
                service: "web".into(),
                slot: 0,
                revision: 7,
                spec_hash,
                node_id: "node-a".into(),
                observed: ObservedTaskState::Healthy,
                ports: vec![PortBinding {
                    target: 80,
                    published: 20_001,
                    protocol: "tcp".into(),
                }],
                container_id: Some("container-existing".into()),
            },
        );

        adopt_unclaimed_tasks(&mut state, "demo");

        let task = &state.tasks["existing-task"];
        assert_eq!(task.service_id, "demo.web");
        assert_eq!(task.slot, 0);
        assert_eq!(task.container_id.as_deref(), Some("container-existing"));
        assert!(state.unclaimed_tasks.is_empty());
    }

    fn draining_task() -> TaskRecord {
        TaskRecord {
            id: "old-task".into(),
            service_id: "demo.web".into(),
            revision: 1,
            slot: 0,
            node_id: "node-a".into(),
            desired: DesiredTaskState::Draining,
            observed: ObservedTaskState::Healthy,
            ports: vec![PortBinding {
                target: 80,
                published: 20_001,
                protocol: "tcp".into(),
            }],
            container_id: Some("container-old".into()),
            drain_until_unix_ms: None,
        }
    }
}
