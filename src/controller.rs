use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use serde_json::json;
use tokio::{net::TcpListener, sync::Mutex};
use tower_http::trace::TraceLayer;
use tracing::{error, info, warn};

use crate::{
    config::ControllerConfig,
    model::{
        ClusterState, DesiredTaskState, HeartbeatResponse, LeaderLease, NodeHeartbeat,
        ObservedTaskState, ServiceRecord, StackRecord, StatusResponse, TaskAssignment,
    },
    scheduler,
    stack::{ParsedStack, parse_stack},
    storage::{S3ObjectStore, StateRepository, StorageError, VersionedMeta},
    traefik,
};

pub async fn run(config: ControllerConfig) -> Result<()> {
    let token = config.token()?;
    let store = Arc::new(
        S3ObjectStore::new(&config.storage)
            .await
            .map_err(anyhow::Error::msg)?,
    );
    let repository = StateRepository::from_s3(store, &config.storage, config.cluster_id.clone());
    let controller = Arc::new(
        Controller::new(config.clone(), token, repository)
            .await
            .map_err(anyhow::Error::msg)?,
    );
    let background = controller.clone();
    tokio::spawn(async move { background.control_loop().await });

    let app = Router::new()
        .route("/healthz", get(health))
        .route("/v1/status", get(status))
        .route("/v1/stacks/{name}", put(apply_stack))
        .route("/v1/nodes/{node_id}/heartbeat", post(heartbeat))
        .route("/v1/traefik", get(traefik_config))
        .layer(TraceLayer::new_for_http())
        .with_state(controller);
    let listener = TcpListener::bind(config.listen)
        .await
        .with_context(|| format!("failed to listen on {}", config.listen))?;
    info!(address = %config.listen, "controller API listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

struct Inner {
    versioned_meta: VersionedMeta,
    state: ClusterState,
    is_leader: bool,
    last_renew: Instant,
    live_nodes: HashMap<String, Instant>,
}

pub struct Controller {
    config: ControllerConfig,
    token: String,
    repository: StateRepository,
    inner: Mutex<Inner>,
}

#[derive(Debug)]
enum ControllerError {
    Unauthorized,
    Invalid(String),
    NotLeader(Option<String>),
    Storage(StorageError),
}

impl From<StorageError> for ControllerError {
    fn from(value: StorageError) -> Self {
        Self::Storage(value)
    }
}

impl Controller {
    async fn new(
        config: ControllerConfig,
        token: String,
        repository: StateRepository,
    ) -> Result<Self, StorageError> {
        let versioned_meta = repository.initialize().await?;
        let state = repository.load_state(&versioned_meta.value).await?;
        // Preserve existing assignments during the first node timeout after a takeover.
        let live_nodes = state
            .nodes
            .keys()
            .map(|id| (id.clone(), Instant::now()))
            .collect();
        Ok(Self {
            config,
            token,
            repository,
            inner: Mutex::new(Inner {
                versioned_meta,
                state,
                is_leader: false,
                last_renew: Instant::now(),
                live_nodes,
            }),
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
        self.expire_local_lease(&mut inner);
        if inner.is_leader {
            if inner.last_renew.elapsed()
                >= Duration::from_secs(self.config.lease.renew_interval_seconds)
                && let Err(error) = self.renew_locked(&mut inner).await
            {
                inner.is_leader = false;
                return Err(error);
            }

            let timeout = Duration::from_secs(self.config.node_timeout_seconds);
            let now = Instant::now();
            let live: BTreeSet<String> = inner
                .live_nodes
                .iter()
                .filter(|(_, seen)| now.duration_since(**seen) <= timeout)
                .map(|(id, _)| id.clone())
                .collect();
            let previous = inner.state.clone();
            if scheduler::reconcile(&mut inner.state, &live)
                && let Err(error) = self.commit_locked(&mut inner).await
            {
                inner.state = previous;
                return Err(error);
            }
            return Ok(());
        }

        self.refresh_locked(&mut inner).await?;
        let now = unix_ms();
        let expired = inner
            .versioned_meta
            .value
            .leader
            .as_ref()
            .is_none_or(|leader| {
                leader.lease_until_unix_ms + self.config.lease.clock_skew_seconds as i64 * 1000
                    <= now
            });
        if expired {
            self.try_acquire_locked(&mut inner).await?;
        }
        Ok(())
    }

    fn expire_local_lease(&self, inner: &mut Inner) {
        if !inner.is_leader {
            return;
        }
        let deadline = inner
            .versioned_meta
            .value
            .leader
            .as_ref()
            .map(|leader| leader.lease_until_unix_ms)
            .unwrap_or_default()
            - self.config.lease.clock_skew_seconds as i64 * 1000;
        if unix_ms() >= deadline {
            warn!("local leader lease expired; entering fail-closed standby mode");
            inner.is_leader = false;
        }
    }

    async fn refresh_locked(&self, inner: &mut Inner) -> Result<(), StorageError> {
        let Some(latest) = self.repository.load_meta().await? else {
            return Err(StorageError::Backend("meta object disappeared".to_owned()));
        };
        if latest.value.generation != inner.versioned_meta.value.generation
            || latest.value.snapshot_key != inner.versioned_meta.value.snapshot_key
        {
            inner.state = self.repository.load_state(&latest.value).await?;
        }
        inner.versioned_meta = latest;
        Ok(())
    }

    async fn try_acquire_locked(&self, inner: &mut Inner) -> Result<(), StorageError> {
        let mut candidate = inner.versioned_meta.value.clone();
        let term = candidate
            .leader
            .as_ref()
            .map_or(1, |leader| leader.term + 1);
        candidate.leader = Some(LeaderLease {
            id: self.config.controller_id.clone(),
            term,
            advertise_url: self.config.advertise_url.trim_end_matches('/').to_owned(),
            lease_until_unix_ms: unix_ms() + self.config.lease.duration_seconds as i64 * 1000,
        });
        match self
            .repository
            .cas_meta(&candidate, &inner.versioned_meta.etag)
            .await
        {
            Ok(etag) => {
                candidate
                    .leader
                    .as_ref()
                    .inspect(|leader| info!(term = leader.term, "acquired controller leadership"));
                inner.versioned_meta = VersionedMeta {
                    value: candidate,
                    etag,
                };
                inner.is_leader = true;
                inner.last_renew = Instant::now();
                inner.live_nodes.clear();
                let takeover_time = Instant::now();
                for id in inner.state.nodes.keys() {
                    inner.live_nodes.insert(id.clone(), takeover_time);
                }
                Ok(())
            }
            Err(StorageError::Conflict) => {
                self.refresh_locked(inner).await?;
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    async fn renew_locked(&self, inner: &mut Inner) -> Result<(), StorageError> {
        let mut meta = inner.versioned_meta.value.clone();
        let Some(leader) = meta.leader.as_mut() else {
            inner.is_leader = false;
            return Err(StorageError::Conflict);
        };
        if leader.id != self.config.controller_id {
            inner.is_leader = false;
            return Err(StorageError::Conflict);
        }
        leader.lease_until_unix_ms = unix_ms() + self.config.lease.duration_seconds as i64 * 1000;
        let etag = self
            .repository
            .cas_meta(&meta, &inner.versioned_meta.etag)
            .await?;
        inner.versioned_meta = VersionedMeta { value: meta, etag };
        inner.last_renew = Instant::now();
        Ok(())
    }

    async fn commit_locked(&self, inner: &mut Inner) -> Result<(), StorageError> {
        self.expire_local_lease(inner);
        if !inner.is_leader {
            return Err(StorageError::Conflict);
        }
        let leader = inner
            .versioned_meta
            .value
            .leader
            .as_ref()
            .ok_or(StorageError::Conflict)?;
        if leader.id != self.config.controller_id {
            inner.is_leader = false;
            return Err(StorageError::Conflict);
        }

        let generation = inner.versioned_meta.value.generation + 1;
        let snapshot_key = self
            .repository
            .put_snapshot(generation, &inner.state)
            .await?;
        let mut meta = inner.versioned_meta.value.clone();
        meta.generation = generation;
        meta.snapshot_key = snapshot_key;
        meta.leader.as_mut().unwrap().lease_until_unix_ms =
            unix_ms() + self.config.lease.duration_seconds as i64 * 1000;
        match self
            .repository
            .cas_meta(&meta, &inner.versioned_meta.etag)
            .await
        {
            Ok(etag) => {
                inner.versioned_meta = VersionedMeta { value: meta, etag };
                inner.last_renew = Instant::now();
                Ok(())
            }
            Err(error) => {
                inner.is_leader = false;
                Err(error)
            }
        }
    }

    fn leader_redirect(&self, inner: &Inner, path: &str) -> ControllerError {
        let location = inner
            .versioned_meta
            .value
            .leader
            .as_ref()
            .map(|leader| format!("{}{}", leader.advertise_url.trim_end_matches('/'), path));
        ControllerError::NotLeader(location)
    }

    async fn apply(&self, stack_name: &str, parsed: ParsedStack) -> Result<u64, ControllerError> {
        validate_stack_name(stack_name)?;
        let mut inner = self.inner.lock().await;
        self.expire_local_lease(&mut inner);
        if !inner.is_leader {
            return Err(self.leader_redirect(&inner, &format!("/v1/stacks/{stack_name}")));
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
        let live = current_live_nodes(&inner, self.config.node_timeout_seconds);
        scheduler::reconcile(&mut inner.state, &live);
        if let Err(error) = self.commit_locked(&mut inner).await {
            inner.state = previous;
            return Err(error.into());
        }
        Ok(inner.versioned_meta.value.generation)
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
            return Err(self.leader_redirect(&inner, &format!("/v1/nodes/{node_id}/heartbeat")));
        }

        let previous = inner.state.clone();
        let mut changed = inner.state.nodes.get(node_id).is_none_or(|existing| {
            serde_json::to_value(existing).ok() != serde_json::to_value(&heartbeat.node).ok()
        });
        inner.live_nodes.insert(node_id.to_owned(), Instant::now());
        inner.state.nodes.insert(node_id.to_owned(), heartbeat.node);

        let reports: HashMap<_, _> = heartbeat
            .tasks
            .into_iter()
            .map(|report| (report.id.clone(), report))
            .collect();
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
                        changed = true;
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
                    changed = true;
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

        let term = inner
            .versioned_meta
            .value
            .leader
            .as_ref()
            .map_or(0, |leader| leader.term);
        let generation = inner.versioned_meta.value.generation;
        let assignments = inner
            .state
            .tasks
            .values()
            .filter(|task| task.node_id == node_id && task.desired == DesiredTaskState::Running)
            .filter_map(|task| {
                let service = inner.state.services.get(&task.service_id)?;
                Some(TaskAssignment {
                    id: task.id.clone(),
                    service_id: task.service_id.clone(),
                    revision: task.revision,
                    slot: task.slot,
                    spec: service.spec.clone(),
                    ports: task.ports.clone(),
                    leader_term: term,
                    generation,
                })
            })
            .collect();
        Ok(HeartbeatResponse {
            leader_term: term,
            generation,
            assignments,
        })
    }

    async fn status(&self) -> StatusResponse {
        let inner = self.inner.lock().await;
        StatusResponse {
            cluster_id: self.config.cluster_id.clone(),
            generation: inner.versioned_meta.value.generation,
            leader: inner.versioned_meta.value.leader.clone(),
            is_leader: inner.is_leader,
            state: inner.state.clone(),
        }
    }

    async fn traefik(&self) -> traefik::DynamicConfiguration {
        let inner = self.inner.lock().await;
        traefik::generate(&inner.state)
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
    Ok(Json(controller.status().await))
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

async fn traefik_config(
    State(controller): State<Arc<Controller>>,
    headers: HeaderMap,
) -> Result<Json<traefik::DynamicConfiguration>, ControllerError> {
    require_auth(&controller, &headers)?;
    Ok(Json(controller.traefik().await))
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

async fn shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        warn!(%error, "failed to install shutdown signal handler");
    }
}
