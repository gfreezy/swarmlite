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
    caddy,
    config::ControllerConfig,
    kv,
    model::{
        BootstrapResponse, CaddyStatus, ClusterConfigResponse, ClusterConfigUpdate,
        ClusterSettings, ClusterState, ControllerRecord, DesiredTaskState, HeartbeatResponse,
        JoinRequest, JoinResponse, KvDeleteRequest, KvListResponse, KvLock, KvLockAcquireRequest,
        KvLockAcquireResponse, KvLockMutationRequest, KvLockStatus, KvObjectResponse, KvPutRequest,
        KvPutResponse, KvStatResponse, KvState, LeaderRecord, NodeHeartbeat, NodeRole,
        ObservedTaskState, RecoveryStatus, ServiceRecord, StackRecord, StatusResponse,
        TaskAssignment, TaskRecord, UnclaimedTask, service_spec_hash, valid_controller_count,
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
        .route("/v1/caddy", get(caddy_config))
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
    let caddy_background = controller.clone();
    let caddy_loop = tokio::spawn(async move { caddy_background.caddy_sync_loop().await });
    info!(address = %config.listen, "controller API listening");
    let result = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await
        .map_err(anyhow::Error::from);
    control_loop.abort();
    caddy_loop.abort();
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
}

pub struct Controller {
    config: ControllerConfig,
    token: String,
    repository: StateRepository,
    inner: Mutex<Inner>,
    caddy_client: reqwest::Client,
    caddy_notify: Notify,
    caddy_sync: Mutex<CaddySyncState>,
}

#[derive(Debug, Default)]
struct CaddySyncState {
    applied_generation: Option<u64>,
    endpoint_errors: BTreeMap<String, String>,
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
        let caddy_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.caddy.request_timeout_seconds))
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
            }),
            caddy_client,
            caddy_notify: Notify::new(),
            caddy_sync: Mutex::new(CaddySyncState::default()),
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
            changed |= self.reconcile_controller_count_locked(&mut inner).await?;
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
        inner.state.nodes.clear();
        let takeover_time = Instant::now();
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
        let self_record = ControllerRecord {
            node_id: self.config.controller_id.clone(),
            advertise_url: self.config.advertise_url.trim_end_matches('/').to_owned(),
            raft_id: self.repository.raft().node_id(),
            raft_url: self.repository.raft().local_node().raft_url.clone(),
            reserved_at_unix_ms: unix_ms(),
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
        if drains_reset || controller_changed {
            self.commit_locked(inner).await?;
        } else {
            self.caddy_notify.notify_one();
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
                self.caddy_notify.notify_one();
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

    async fn reconcile_controller_count_locked(
        &self,
        inner: &mut Inner,
    ) -> Result<bool, StorageError> {
        let target = usize::from(self.cluster_settings(inner)?.controllers);
        if inner.state.controllers.len() <= target {
            return Ok(false);
        }

        let voters = self.repository.voter_ids();
        let candidate = inner
            .state
            .controllers
            .values()
            .filter(|record| record.node_id != self.config.controller_id)
            .min_by(|left, right| {
                voters
                    .contains(&left.raft_id)
                    .cmp(&voters.contains(&right.raft_id))
                    .then_with(|| right.node_id.cmp(&left.node_id))
            })
            .cloned()
            .ok_or_else(|| {
                StorageError::InvalidData(
                    "controller target cannot be reached without removing the active leader"
                        .to_owned(),
                )
            })?;

        if self.repository.is_voter(candidate.raft_id) {
            self.repository.remove_voter(candidate.raft_id).await?;
        }
        inner.state.controllers.remove(&candidate.node_id);
        info!(
            node_id = %candidate.node_id,
            controllers = target,
            "demoted excess controller after cluster config update"
        );
        Ok(true)
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
        if !valid_controller_count(update.controllers) {
            return Err(ControllerError::Invalid(
                "controllers must be 1 or an odd number greater than or equal to 3".to_owned(),
            ));
        }

        let mut inner = self.inner.lock().await;
        self.expire_local_lease(&mut inner);
        if !inner.is_leader {
            return Err(self.leader_redirect("/v1/config"));
        }

        let mut cluster = self.cluster_settings(&inner)?;
        if cluster.controllers != update.controllers {
            cluster.controllers = update.controllers;
            let previous = std::mem::replace(&mut inner.cluster, cluster.clone());
            if let Err(error) = self.commit_locked(&mut inner).await {
                inner.cluster = previous;
                return Err(error.into());
            }
            info!(controllers = update.controllers, "updated cluster config");
        }

        Ok(ClusterConfigResponse {
            generation: inner.generation,
            config: cluster,
        })
    }

    async fn bootstrap(&self) -> Result<BootstrapResponse, ControllerError> {
        let inner = self.inner.lock().await;
        let cluster = self.cluster_settings(&inner)?;
        let voters = self.repository.voter_ids();
        Ok(BootstrapResponse {
            cluster,
            controllers: controller_urls(&inner.state, Some(&self.config.advertise_url), &voters),
        })
    }

    async fn join_node(
        &self,
        node_id: &str,
        request: JoinRequest,
    ) -> Result<JoinResponse, ControllerError> {
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
        let already_controller = inner.state.controllers.contains_key(node_id);
        let has_capacity = inner.state.controllers.len() < usize::from(cluster.controllers);
        let should_control = already_controller
            || (request.controller_capable && cluster.controllers > 1 && has_capacity);
        if should_control {
            let url = request.controller_url.clone().ok_or_else(|| {
                ControllerError::Invalid(
                    "controller-capable nodes must provide controller_url".to_owned(),
                )
            })?;
            let raft_id = request.raft_id.ok_or_else(|| {
                ControllerError::Invalid("controller-capable nodes must provide raft_id".to_owned())
            })?;
            let raft_url = request.raft_url.clone().ok_or_else(|| {
                ControllerError::Invalid(
                    "controller-capable nodes must provide raft_url".to_owned(),
                )
            })?;
            if inner
                .state
                .controllers
                .get(node_id)
                .is_some_and(|record| record.raft_id != raft_id)
            {
                return Err(ControllerError::Invalid(
                    "a node cannot change its persisted raft_id".to_owned(),
                ));
            }
            let record = ControllerRecord {
                node_id: node_id.to_owned(),
                advertise_url: url,
                raft_id,
                raft_url,
                reserved_at_unix_ms: inner
                    .state
                    .controllers
                    .get(node_id)
                    .map_or(now, |record| record.reserved_at_unix_ms),
            };
            changed |= inner.state.controllers.get(node_id).is_none_or(|existing| {
                existing.advertise_url != record.advertise_url
                    || existing.raft_id != record.raft_id
                    || existing.raft_url != record.raft_url
            });
            inner.state.controllers.insert(node_id.to_owned(), record);
        }
        if changed && let Err(error) = self.commit_locked(&mut inner).await {
            inner.state = previous;
            return Err(error.into());
        }
        let role = if inner.state.controllers.contains_key(node_id) {
            NodeRole::Controller
        } else {
            NodeRole::Worker
        };
        Ok(JoinResponse {
            cluster,
            role,
            controllers: controller_urls(
                &inner.state,
                Some(&self.config.advertise_url),
                &self.repository.voter_ids(),
            ),
        })
    }

    async fn apply(&self, stack_name: &str, parsed: ParsedStack) -> Result<u64, ControllerError> {
        validate_stack_name(stack_name)?;
        if self.config.caddy.admin_endpoints.is_empty()
            && parsed.services.values().any(caddy::is_enabled)
        {
            return Err(ControllerError::Invalid(
                "ingress is enabled but caddy.admin_endpoints is empty".to_owned(),
            ));
        }
        let mut inner = self.inner.lock().await;
        self.expire_local_lease(&mut inner);
        if !inner.is_leader {
            return Err(self.leader_redirect(&format!("/v1/stacks/{stack_name}")));
        }
        let previous = inner.state.clone();
        let desired_ids: BTreeSet<String> = parsed
            .services
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
        for (name, spec) in parsed.services {
            let id = service_id(stack_name, &name);
            match inner.state.services.get_mut(&id) {
                Some(existing) if existing.spec == spec && !existing.deleted => {}
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
        if node_id != heartbeat.node.id {
            return Err(ControllerError::Invalid(
                "node ID in path and request body differ".to_owned(),
            ));
        }
        let mut inner = self.inner.lock().await;
        self.expire_local_lease(&mut inner);
        if !inner.is_leader {
            return Err(self.leader_redirect(&format!("/v1/nodes/{node_id}/heartbeat")));
        }

        let cluster = self.cluster_settings(&inner)?;
        let previous = inner.state.clone();
        let now = unix_ms();
        let voters = self.repository.voter_ids();
        let mut changed = prune_controllers(&mut inner.state, now, &voters);
        let mut soft_changed = false;
        let already_controller = inner.state.controllers.contains_key(node_id);
        let mut node_role = if already_controller {
            NodeRole::Controller
        } else {
            NodeRole::Worker
        };
        if heartbeat.node.controller_capable {
            let has_capacity = inner.state.controllers.len() < usize::from(cluster.controllers);
            if node_role == NodeRole::Controller || (cluster.controllers > 1 && has_capacity) {
                let controller_url = heartbeat.node.controller_url.clone().ok_or_else(|| {
                    ControllerError::Invalid(
                        "controller-capable nodes must provide controller_url".to_owned(),
                    )
                })?;
                let raft_id = heartbeat.node.raft_id.ok_or_else(|| {
                    ControllerError::Invalid(
                        "controller-capable nodes must provide raft_id".to_owned(),
                    )
                })?;
                let raft_url = heartbeat.node.raft_url.clone().ok_or_else(|| {
                    ControllerError::Invalid(
                        "controller-capable nodes must provide raft_url".to_owned(),
                    )
                })?;
                let existing = inner.state.controllers.get(node_id);
                if existing.is_some_and(|record| record.raft_id != raft_id) {
                    return Err(ControllerError::Invalid(
                        "a node cannot change its persisted raft_id".to_owned(),
                    ));
                }
                let membership_needs_update = existing.is_some_and(|record| {
                    !self.repository.is_voter(record.raft_id)
                        || record.raft_id != raft_id
                        || record.raft_url != raft_url
                        || record.advertise_url != controller_url
                });
                if membership_needs_update {
                    self.repository
                        .ensure_voter(
                            raft_id,
                            swarmlite_raft::ManagerNode {
                                raft_url: raft_url.clone(),
                                api_url: controller_url.clone(),
                            },
                        )
                        .await?;
                }
                let should_persist = existing.is_none_or(|record| {
                    record.advertise_url != controller_url
                        || record.raft_id != raft_id
                        || record.raft_url != raft_url
                });
                let reserved_at_unix_ms = existing.map_or(now, |record| record.reserved_at_unix_ms);
                inner.state.controllers.insert(
                    node_id.to_owned(),
                    ControllerRecord {
                        node_id: node_id.to_owned(),
                        advertise_url: controller_url,
                        raft_id,
                        raft_url,
                        reserved_at_unix_ms,
                    },
                );
                changed |= should_persist;
                node_role = NodeRole::Controller;
            }
        }
        soft_changed |= inner.state.nodes.get(node_id).is_none_or(|existing| {
            serde_json::to_value(existing).ok() != serde_json::to_value(&heartbeat.node).ok()
        });
        inner.live_nodes.insert(node_id.to_owned(), Instant::now());
        inner.state.nodes.insert(node_id.to_owned(), heartbeat.node);

        let reports: HashMap<_, _> = heartbeat
            .tasks
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
            self.caddy_notify.notify_one();
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
        Ok(HeartbeatResponse {
            leader_term: term,
            generation,
            assignments,
            node_role,
            controllers: controller_urls(
                &inner.state,
                Some(&self.config.advertise_url),
                &self.repository.voter_ids(),
            ),
            remove_tasks,
        })
    }

    async fn status(&self) -> StatusResponse {
        let mut inner = self.inner.lock().await;
        self.expire_local_lease(&mut inner);
        let generation = inner.generation;
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
        let caddy_sync = self.caddy_sync.lock().await;
        StatusResponse {
            cluster_id: self.config.cluster.cluster_id.clone(),
            generation,
            leader,
            is_leader,
            caddy: CaddyStatus {
                enabled: !self.config.caddy.admin_endpoints.is_empty(),
                desired_generation: generation,
                applied_generation: caddy_sync.applied_generation,
                endpoint_errors: caddy_sync.endpoint_errors.clone(),
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

    async fn caddy(&self) -> Result<caddy::HttpServer, ControllerError> {
        let mut inner = self.inner.lock().await;
        self.expire_local_lease(&mut inner);
        if !inner.is_leader {
            return Err(self.leader_redirect("/v1/caddy"));
        }
        Ok(caddy::generate(&inner.state, &self.config.caddy.listen))
    }

    async fn caddy_sync_loop(self: Arc<Self>) {
        if self.config.caddy.admin_endpoints.is_empty() {
            info!("Caddy publishing is disabled because no admin endpoints are configured");
            return;
        }
        let mut ticker = tokio::time::interval(Duration::from_secs(
            self.config.caddy.resync_interval_seconds,
        ));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                _ = self.caddy_notify.notified() => {}
            }
            loop {
                match self.sync_caddy_once().await {
                    Ok(()) => break,
                    Err(error) => {
                        warn!(%error, "Caddy configuration sync failed");
                        tokio::select! {
                            _ = tokio::time::sleep(Duration::from_secs(
                                self.config.caddy.retry_interval_seconds,
                            )) => {}
                            _ = self.caddy_notify.notified() => {}
                        }
                    }
                }
            }
        }
    }

    async fn sync_caddy_once(&self) -> Result<(), String> {
        let (generation, server) = {
            let mut inner = self.inner.lock().await;
            self.expire_local_lease(&mut inner);
            if !inner.is_leader {
                return Ok(());
            }
            (
                inner.generation,
                caddy::generate(&inner.state, &self.config.caddy.listen),
            )
        };

        let results = join_all(
            self.config
                .caddy
                .admin_endpoints
                .iter()
                .map(|endpoint| self.push_caddy_server(endpoint, &server)),
        )
        .await;
        let endpoint_errors = self
            .config
            .caddy
            .admin_endpoints
            .iter()
            .cloned()
            .zip(results)
            .filter_map(|(endpoint, result)| result.err().map(|error| (endpoint, error)))
            .collect::<BTreeMap<_, _>>();
        {
            let mut sync = self.caddy_sync.lock().await;
            sync.endpoint_errors = endpoint_errors.clone();
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

        info!(generation, "Caddy configuration applied");
        let mut inner = self.inner.lock().await;
        self.expire_local_lease(&mut inner);
        if !inner.is_leader || inner.generation != generation {
            self.caddy_notify.notify_one();
            return Ok(());
        }
        let deadline = unix_ms() + self.config.caddy.drain_timeout_seconds as i64 * 1000;
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

    async fn push_caddy_server(
        &self,
        endpoint: &str,
        server: &caddy::HttpServer,
    ) -> Result<(), String> {
        let url = format!(
            "{}/config/apps/http/servers/{}",
            endpoint.trim_end_matches('/'),
            self.config.caddy.server_name
        );
        let response = self
            .caddy_client
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

async fn caddy_config(
    State(controller): State<Arc<Controller>>,
    headers: HeaderMap,
) -> Result<Json<caddy::HttpServer>, ControllerError> {
    require_auth(&controller, &headers)?;
    controller.caddy().await.map(Json)
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
    use swarmlite_raft::{ManagerNode, NodeConfig, RaftNode};

    use crate::{
        caddy::{ENABLE_LABEL, HOST_LABEL, PORT_LABEL},
        config::CaddyConfig,
        model::{
            ClusterCaddyConfig, KvVersion, NodeRecord, PortBinding, ServicePort, ServiceSpec,
            TaskRecord, TaskReport,
        },
    };

    use super::*;

    fn test_controller_config(cluster: &ClusterSettings) -> ControllerConfig {
        ControllerConfig {
            controller_id: "controller-a".into(),
            listen: "127.0.0.1:0".parse().unwrap(),
            advertise_url: "http://10.0.0.10:8080".into(),
            node_timeout_seconds: 20,
            reconcile_interval_seconds: 1,
            caddy: CaddyConfig::default(),
            cluster: cluster.clone(),
        }
    }

    #[tokio::test]
    async fn kv_is_lww_and_locks_are_fenced() {
        let cluster = ClusterSettings {
            schema_version: 2,
            cluster_id: "kv-test".into(),
            controllers: 1,
            controller_port: 8080,
            caddy: ClusterCaddyConfig::default(),
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
    async fn automatically_assigns_controllers_without_exceeding_target() {
        let cluster = ClusterSettings {
            schema_version: 2,
            cluster_id: "controller-assignment-test".into(),
            controllers: 3,
            controller_port: 8080,
            caddy: ClusterCaddyConfig::default(),
        };
        let (repository, _raft, _directory) = test_repository(&cluster).await;
        let config = test_controller_config(&cluster);
        let controller = Controller::new(config, "secret".into(), repository)
            .await
            .unwrap();
        controller.tick().await.unwrap();

        let first = controller
            .join_node(
                "node-b",
                JoinRequest {
                    node_id: "node-b".into(),
                    address: "10.0.0.22".into(),
                    controller_capable: true,
                    controller_url: Some("http://10.0.0.22:8080".into()),
                    raft_id: Some(2),
                    raft_url: Some("http://10.0.0.22:8080/internal/raft".into()),
                    labels: BTreeMap::new(),
                },
            )
            .await
            .unwrap();
        assert_eq!(first.role, NodeRole::Controller);

        let second = controller
            .join_node(
                "node-c",
                JoinRequest {
                    node_id: "node-c".into(),
                    address: "10.0.0.23".into(),
                    controller_capable: true,
                    controller_url: Some("http://10.0.0.23:8080".into()),
                    raft_id: Some(3),
                    raft_url: Some("http://10.0.0.23:8080/internal/raft".into()),
                    labels: BTreeMap::new(),
                },
            )
            .await
            .unwrap();
        assert_eq!(second.role, NodeRole::Controller);

        let third = controller
            .join_node(
                "node-d",
                JoinRequest {
                    node_id: "node-d".into(),
                    address: "10.0.0.24".into(),
                    controller_capable: true,
                    controller_url: Some("http://10.0.0.24:8080".into()),
                    raft_id: Some(4),
                    raft_url: Some("http://10.0.0.24:8080/internal/raft".into()),
                    labels: BTreeMap::new(),
                },
            )
            .await
            .unwrap();
        assert_eq!(third.role, NodeRole::Worker);
        assert_eq!(controller.inner.lock().await.state.controllers.len(), 3);

        controller
            .inner
            .lock()
            .await
            .state
            .controllers
            .get_mut("node-b")
            .unwrap()
            .reserved_at_unix_ms = unix_ms() - CONTROLLER_TIMEOUT_MS - 1;
        let promoted = controller
            .heartbeat(
                "node-d",
                NodeHeartbeat {
                    node: NodeRecord {
                        id: "node-d".into(),
                        address: "10.0.0.24".into(),
                        labels: BTreeMap::new(),
                        cpu_millis: 1000,
                        memory_bytes: 1024,
                        port_range_start: 20_000,
                        port_range_end: 29_999,
                        controller_capable: true,
                        controller_url: Some("http://10.0.0.24:8080".into()),
                        raft_id: Some(4),
                        raft_url: Some("http://10.0.0.24:8080/internal/raft".into()),
                    },
                    tasks: Vec::new(),
                },
            )
            .await
            .unwrap();
        assert_eq!(promoted.node_role, NodeRole::Controller);
        assert!(!controller.repository.is_voter(4));
    }

    #[tokio::test]
    async fn updates_controller_target_in_raft_and_reconciles_roles() {
        let cluster = ClusterSettings {
            schema_version: 2,
            cluster_id: "config-update-test".into(),
            controllers: 1,
            controller_port: 8080,
            caddy: ClusterCaddyConfig::default(),
        };
        let (repository, _raft, _directory) = test_repository(&cluster).await;
        let observer = repository.clone();
        let config = test_controller_config(&cluster);
        let controller = Controller::new(config, "secret".into(), repository)
            .await
            .unwrap();
        controller.tick().await.unwrap();

        let error = controller
            .update_cluster_config(ClusterConfigUpdate { controllers: 2 })
            .await
            .unwrap_err();
        assert!(matches!(error, ControllerError::Invalid(_)));

        let updated = controller
            .update_cluster_config(ClusterConfigUpdate { controllers: 3 })
            .await
            .unwrap();
        assert_eq!(updated.config.controllers, 3);
        assert_eq!(
            observer
                .load_consistent()
                .await
                .unwrap()
                .cluster
                .controllers,
            3
        );
        assert_eq!(controller.get_cluster_config().await.unwrap(), updated);

        let joined = controller
            .join_node(
                "node-b",
                JoinRequest {
                    node_id: "node-b".into(),
                    address: "10.0.0.22".into(),
                    controller_capable: true,
                    controller_url: Some("http://10.0.0.22:8080".into()),
                    raft_id: Some(2),
                    raft_url: Some("http://10.0.0.22:8080/internal/raft".into()),
                    labels: BTreeMap::new(),
                },
            )
            .await
            .unwrap();
        assert_eq!(joined.role, NodeRole::Controller);

        controller
            .update_cluster_config(ClusterConfigUpdate { controllers: 1 })
            .await
            .unwrap();
        controller.tick().await.unwrap();
        assert_eq!(controller.inner.lock().await.state.controllers.len(), 1);
        assert_eq!(controller.bootstrap().await.unwrap().cluster.controllers, 1);
    }

    #[tokio::test]
    async fn promotes_reserved_manager_through_raft() {
        let cluster = ClusterSettings {
            schema_version: 2,
            cluster_id: "manager-promotion-test".into(),
            controllers: 3,
            controller_port: 8080,
            caddy: ClusterCaddyConfig::default(),
        };
        let (repository, leader_raft, _leader_directory) = test_repository(&cluster).await;
        let mut config = test_controller_config(&cluster);
        config.advertise_url = "http://127.0.0.1:19090".into();
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
            ManagerNode {
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
                    controller_capable: true,
                    controller_url: Some(api_url.clone()),
                    raft_id: Some(2),
                    raft_url: Some(raft_url.clone()),
                    labels: BTreeMap::new(),
                },
            )
            .await
            .unwrap();
        assert_eq!(joined.role, NodeRole::Controller);

        let response = controller
            .heartbeat(
                "controller-b",
                NodeHeartbeat {
                    node: NodeRecord {
                        id: "controller-b".into(),
                        address: address.ip().to_string(),
                        labels: BTreeMap::new(),
                        cpu_millis: 1000,
                        memory_bytes: 1024,
                        port_range_start: 20_000,
                        port_range_end: 29_999,
                        controller_capable: true,
                        controller_url: Some(api_url.clone()),
                        raft_id: Some(2),
                        raft_url: Some(raft_url),
                    },
                    tasks: Vec::new(),
                },
            )
            .await
            .unwrap();

        assert_eq!(response.node_role, NodeRole::Controller);
        assert!(response.controllers.contains(&api_url));
        assert!(leader_raft.voter_ids().contains(&2));
        assert!(
            controller
                .inner
                .lock()
                .await
                .state
                .controllers
                .contains_key("controller-b")
        );

        leader_raft.shutdown().await.unwrap();
        follower_raft.shutdown().await.unwrap();
        server.abort();
    }

    #[tokio::test]
    async fn caddy_acknowledgement_starts_drain_deadline() {
        let received = Arc::new(Mutex::new(Vec::<serde_json::Value>::new()));
        let app = Router::new()
            .route(
                "/config/apps/http/servers/swarmlite",
                post(capture_caddy_config),
            )
            .with_state(received.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let cluster = ClusterSettings {
            schema_version: 2,
            cluster_id: "caddy-publisher-test".into(),
            controllers: 1,
            controller_port: 8080,
            caddy: ClusterCaddyConfig::default(),
        };
        let mut config = test_controller_config(&cluster);
        config.controller_id = "controller-test".into();
        config.caddy.admin_endpoints = vec![format!("http://{address}")];
        config.caddy.listen = vec![":18089".into()];
        config.caddy.drain_timeout_seconds = 3;
        let (repository, _raft, _directory) = test_repository(&cluster).await;
        let controller = Controller::new(config, "test-token".into(), repository)
            .await
            .unwrap();
        {
            let mut inner = controller.inner.lock().await;
            controller.try_acquire_locked(&mut inner).await.unwrap();
            inner.state.nodes.insert("node-a".into(), test_node());
            inner
                .state
                .services
                .insert("demo.web".into(), test_service());
            inner.state.tasks.insert("old-task".into(), draining_task());
            controller.commit_locked(&mut inner).await.unwrap();
        }

        controller.sync_caddy_once().await.unwrap();

        let inner = controller.inner.lock().await;
        let task = &inner.state.tasks["old-task"];
        assert_eq!(task.desired, DesiredTaskState::Draining);
        assert!(
            task.drain_until_unix_ms
                .is_some_and(|value| value > unix_ms())
        );
        drop(inner);
        let requests = received.lock().await;
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0]["listen"][0], ":18089");
        assert_eq!(
            requests[0]["routes"][0]["handle"][0]["upstreams"],
            json!([])
        );
        server.abort();
    }

    async fn test_repository(
        cluster: &ClusterSettings,
    ) -> (StateRepository, Arc<RaftNode>, tempfile::TempDir) {
        let directory = tempfile::tempdir().unwrap();
        let raft = RaftNode::open(NodeConfig::new(
            1,
            ManagerNode {
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
            .current_leader(1, "test manager becomes leader")
            .await
            .unwrap();
        (
            StateRepository::new(raft.clone(), cluster.clone()),
            raft,
            directory,
        )
    }

    async fn capture_caddy_config(
        State(received): State<Arc<Mutex<Vec<serde_json::Value>>>>,
        Json(body): Json<serde_json::Value>,
    ) -> StatusCode {
        received.lock().await.push(body);
        StatusCode::OK
    }

    fn test_node() -> NodeRecord {
        NodeRecord {
            id: "node-a".into(),
            address: "10.0.0.21".into(),
            labels: BTreeMap::new(),
            cpu_millis: 1000,
            memory_bytes: 1024,
            port_range_start: 20_000,
            port_range_end: 29_999,
            controller_capable: false,
            controller_url: None,
            raft_id: None,
            raft_url: None,
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
                service_labels: BTreeMap::from([
                    (ENABLE_LABEL.into(), "true".into()),
                    (HOST_LABEL.into(), "example.com".into()),
                    (PORT_LABEL.into(), "80".into()),
                ]),
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
            schema_version: 2,
            cluster_id: "recovery-test".into(),
            controllers: 1,
            controller_port: 8080,
            caddy: ClusterCaddyConfig::default(),
        };
        let (repository, _raft, _directory) = test_repository(&cluster).await;
        let config = test_controller_config(&cluster);
        let controller = Controller::new(config, "secret".into(), repository)
            .await
            .unwrap();
        controller.tick().await.unwrap();

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
