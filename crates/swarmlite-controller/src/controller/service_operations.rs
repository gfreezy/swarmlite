use super::*;
use sha2::{Digest, Sha256};
use swarmlite_core::service_operations::*;

#[derive(Clone)]
pub(super) enum Principal {
    Admin,
    Deploy {
        token_id: String,
        service_id: String,
    },
}

impl Principal {
    pub(super) fn id(&self) -> &str {
        match self {
            Self::Admin => "admin",
            Self::Deploy { token_id, .. } => token_id,
        }
    }
}

impl Controller {
    pub(super) fn deploy_principal(
        &self,
        bearer: &str,
        allow_admin: bool,
    ) -> Result<Principal, ControllerError> {
        if allow_admin && super::api::constant_time_eq(bearer.as_bytes(), self.token.as_bytes()) {
            return Ok(Principal::Admin);
        }
        let raw = bearer
            .strip_prefix("sldt_")
            .ok_or(ControllerError::Unauthorized)?;
        let (id, secret) = raw.split_once('.').ok_or(ControllerError::Unauthorized)?;
        let digest = Sha256::digest(secret.as_bytes());
        let stored = self
            .repository
            .deploy_tokens()?
            .into_iter()
            .find(|token| token.metadata.id == id)
            .ok_or(ControllerError::Unauthorized)?;
        let difference = stored
            .digest
            .iter()
            .zip(digest.iter())
            .fold(0_u8, |sum, (a, b)| sum | (a ^ b));
        if stored.digest.len() != digest.len()
            || difference != 0
            || !stored.metadata.active(unix_ms())
        {
            return Err(ControllerError::Unauthorized);
        }
        Ok(Principal::Deploy {
            token_id: id.into(),
            service_id: stored.metadata.service_id,
        })
    }

    pub(super) async fn create_deploy_token(
        &self,
        request: DeployTokenCreate,
    ) -> Result<DeployToken, ControllerError> {
        if request.name.trim().is_empty()
            || request.name.len() > 128
            || request.name.chars().any(char::is_control)
        {
            return Err(ControllerError::Invalid(
                "name must contain 1 to 128 bytes without control characters".into(),
            ));
        }
        let now = unix_ms();
        let expiry = expiry_from_duration(&request.expires_in, now)
            .map_err(|message| ControllerError::Invalid(message.into()))?;
        let inner = self.inner.lock().await;
        let service = resolve_service(&inner.state, &request.service, "deploy-token create")?;
        if service.spec.job.is_some() {
            return Err(ControllerError::Invalid(
                "deploy tokens cannot target jobs".into(),
            ));
        }
        let id = uuid::Uuid::new_v4().simple().to_string();
        // Three independent UUIDv4 values provide 366 bits of OS-backed randomness.
        let secret = format!(
            "{}{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        );
        let mut metadata = DeployToken {
            id: id.clone(),
            name: request.name,
            service_id: service.id,
            created_at_unix_ms: now,
            expires_at_unix_ms: expiry,
            revoked_at_unix_ms: None,
            token: None,
        };
        self.repository
            .create_deploy_token(&crate::storage::StoredDeployToken {
                metadata: metadata.clone(),
                digest: Sha256::digest(secret.as_bytes()).to_vec(),
                secret: Some(secret.clone()),
            })?;
        metadata.token = Some(format!("sldt_{id}.{secret}"));
        info!(token_id = %id, "administrator created deploy token");
        Ok(metadata)
    }

    pub(super) fn list_deploy_tokens(
        &self,
        show_secret: bool,
    ) -> Result<Vec<DeployToken>, ControllerError> {
        let now = unix_ms();
        self.repository
            .deploy_tokens()?
            .into_iter()
            .map(|stored| {
                let mut metadata = stored.metadata;
                if show_secret && metadata.active(now) {
                    metadata.token = stored
                        .secret
                        .map(|secret| format!("sldt_{}.{secret}", metadata.id));
                    info!(token_id = %metadata.id, "administrator viewed deploy token");
                }
                Ok(metadata)
            })
            .collect()
    }

    pub(super) async fn revoke_deploy_token(&self, id: &str) -> Result<(), ControllerError> {
        // Serialize revocation with acceptance's final credential validation.
        let _inner = self.inner.lock().await;
        if !self.repository.revoke_deploy_token(id, unix_ms())? {
            return Err(ControllerError::NotFound("token not found".into()));
        }
        info!(token_id = %id, "administrator revoked deploy token");
        Ok(())
    }

    pub(super) async fn submit_service_operation(
        &self,
        bearer: &str,
        allow_admin: bool,
        target: &str,
        kind: ServiceOperationKind,
        key: &str,
        request: RedeployRequest,
    ) -> Result<ServiceOperation, ControllerError> {
        if key.is_empty()
            || key.len() > 256
            || !key.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
        {
            return Err(ControllerError::Invalid(
                "a printable Idempotency-Key of 1 to 256 bytes is required".into(),
            ));
        }
        let mut inner = self.inner.lock().await;
        let principal = self.deploy_principal(bearer, allow_admin)?;
        if let Principal::Deploy { service_id, .. } = &principal
            && (service_id != target || kind != ServiceOperationKind::PullRedeploy)
        {
            return Err(ControllerError::Forbidden);
        }
        // An accepted request remains queryable by its key even if the service
        // was subsequently removed. Only new operations require a live target.
        let canonical_target = resolve_service(&inner.state, target, "restart")
            .map(|service| service.id)
            .unwrap_or_else(|_| target.to_owned());
        let hash = format!(
            "{:x}",
            Sha256::digest(
                serde_json::to_vec(&(kind, &canonical_target, &request.source))
                    .expect("serializable request")
            )
        );
        if let Some(existing) = inner
            .state
            .service_operations
            .values()
            .find(|op| op.principal_id == principal.id() && op.idempotency_key == key)
        {
            return if existing.request_hash == hash {
                Ok(existing.view.clone())
            } else {
                Err(ControllerError::Conflict("idempotency_key_conflict".into()))
            };
        }
        let service = resolve_service(&inner.state, target, "restart")?;
        if matches!(principal, Principal::Deploy { .. }) && service.id != target {
            return Err(ControllerError::Forbidden);
        }
        if service.spec.job.is_some() || service.spec.replicas == 0 {
            return Err(ControllerError::Invalid(
                "operation requires a nonzero, non-job service".into(),
            ));
        }
        if inner
            .state
            .service_operations
            .values()
            .any(|op| op.stack == service.stack && op.view.status.active())
            || inner
                .state
                .stacks
                .get(&service.stack)
                .and_then(|stack| stack.deployment.as_ref())
                .is_some_and(|d| d.status.is_active())
        {
            return Err(ControllerError::Conflict(
                "stack has an active operation or deployment".into(),
            ));
        }
        let now = unix_ms();
        if inner.state.service_operations.values().any(|op| {
            op.principal_id == principal.id() && now.saturating_sub(op.created_at_unix_ms) < 1_000
        }) {
            return Err(ControllerError::RateLimited);
        }
        let live = current_live_nodes(&inner, self.config.node_timeout_seconds);
        let tasks = inner
            .state
            .tasks
            .values()
            .filter(|task| {
                task.service_id == service.id && task.desired == DesiredTaskState::Running
            })
            .map(|task| {
                if task.revision != service.revision
                    || !live.contains(&task.node_id)
                    || !inner
                        .state
                        .nodes
                        .get(&task.node_id)
                        .is_some_and(|node| node.supports_service_operations)
                {
                    return Err(ControllerError::Conflict(
                        "agent is unavailable or does not support service operations".into(),
                    ));
                }
                Ok(RestartTarget {
                    task_id: task.id.clone(),
                    node_id: task.node_id.clone(),
                    container_id: task.container_id.clone().ok_or_else(|| {
                        ControllerError::Conflict("task has no observed container".into())
                    })?,
                    image_id: task.image_id.clone().ok_or_else(|| {
                        ControllerError::Conflict(
                            "task has no observed image ID; wait for a heartbeat".into(),
                        )
                    })?,
                    restarted: false,
                    completed: false,
                    deadline_unix_ms: None,
                })
            })
            .collect::<Result<Vec<_>, ControllerError>>()?;
        if tasks.len() != service.spec.replicas as usize {
            return Err(ControllerError::Conflict(
                "service does not have its expected executable tasks".into(),
            ));
        }
        let preparing_nodes = if kind == ServiceOperationKind::PullRedeploy {
            candidate_nodes(&inner.state, &service, &live)?
        } else {
            BTreeSet::new()
        };
        let target_revision = if kind == ServiceOperationKind::PullRedeploy {
            service
                .revision
                .checked_add(1)
                .ok_or_else(|| ControllerError::Invalid("service revision overflow".into()))?
        } else {
            service.revision
        };
        let id = format!("op_{}", uuid::Uuid::new_v4().simple());
        let view = ServiceOperation {
            operation_id: id.clone(),
            service: service.id.clone(),
            kind,
            status: ServiceOperationStatus::Queued,
            phase: if kind == ServiceOperationKind::Restart {
                "restarting"
            } else {
                "pulling"
            }
            .into(),
            revision: 1,
            deployment_generation: None,
            completed_tasks: 0,
            total_tasks: tasks.len(),
            stalled: false,
            blocked: false,
            error: None,
        };
        let operation = ServiceOperationRecord {
            view: view.clone(),
            principal_id: principal.id().into(),
            idempotency_key: key.into(),
            request_hash: hash,
            stack: service.stack,
            generation: inner.generation + 1,
            target_revision,
            image: service.spec.image,
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
            finished_at_unix_ms: None,
            source: request.source,
            tasks,
            preparing_nodes,
            prepared_images: BTreeMap::new(),
        };
        let previous = inner.state.clone();
        inner.state.service_operations.insert(id.clone(), operation);
        if let Err(error) = self.commit_locked(&mut inner).await {
            inner.state = previous;
            return Err(error.into());
        }
        info!(operation_id = %id, "accepted service operation");
        Ok(view)
    }

    pub(super) async fn get_service_operation(
        &self,
        bearer: &str,
        allow_admin: bool,
        id: &str,
        after: Option<u64>,
        wait: u64,
    ) -> Result<ServiceOperation, ControllerError> {
        let mut changes = self.status_changes.subscribe();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(wait.min(25));
        loop {
            let view = {
                let inner = self.inner.lock().await;
                let principal = self.deploy_principal(bearer, allow_admin)?;
                let operation = inner
                    .state
                    .service_operations
                    .get(id)
                    .filter(|operation| {
                        matches!(principal, Principal::Admin)
                            || operation.principal_id == principal.id()
                    })
                    .ok_or_else(|| ControllerError::NotFound("operation not found".into()))?;
                operation.view.clone()
            };
            if after != Some(view.revision)
                || !view.status.active()
                || wait == 0
                || tokio::time::Instant::now() >= deadline
            {
                return Ok(view);
            }
            let _ = tokio::time::timeout_at(deadline, changes.changed()).await;
        }
    }

    pub(super) fn advance_service_operations_locked(&self, inner: &mut Inner) -> bool {
        let before =
            serde_json::to_vec(&inner.state.service_operations).expect("serializable operations");
        let now = unix_ms();
        let live = current_live_nodes(inner, self.config.node_timeout_seconds);
        let ids = inner
            .state
            .service_operations
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for id in ids {
            let Some(mut operation) = inner.state.service_operations.remove(&id) else {
                continue;
            };
            let old = serde_json::to_vec(&operation).expect("serializable operation");
            if operation.view.kind == ServiceOperationKind::PullRedeploy
                && operation.pins_images(&inner.state)
                && let Some(service) = inner.state.services.get(&operation.view.service)
            {
                match candidate_nodes(&inner.state, service, &live) {
                    Ok(nodes) => operation.preparing_nodes.extend(nodes),
                    Err(_) if operation.view.deployment_generation.is_none() => {
                        fail(&mut operation, "unsupported_agent", now)
                    }
                    Err(_) => {
                        operation.view.blocked = true;
                    }
                }
            }
            if operation.view.status.active() {
                operation.view.status = ServiceOperationStatus::Running;
                if operation.view.kind == ServiceOperationKind::Restart {
                    if let Some(target) = operation.tasks.iter_mut().find(|task| !task.completed) {
                        let task = inner.state.tasks.get(&target.task_id);
                        if !live.contains(&target.node_id)
                            || task.is_none_or(|task| {
                                task.container_id
                                    .as_deref()
                                    .is_some_and(|id| id != target.container_id)
                                    || task.desired != DesiredTaskState::Running
                            })
                        {
                            fail(&mut operation, "restart_target_unavailable", now);
                        } else if target
                            .deadline_unix_ms
                            .is_some_and(|deadline| now >= deadline)
                        {
                            fail(&mut operation, "restart_timed_out", now);
                        } else {
                            if target.deadline_unix_ms.is_none() {
                                let grace = inner
                                    .state
                                    .services
                                    .get(&operation.view.service)
                                    .map_or(10, |service| service.spec.stop_grace_period_seconds);
                                target.deadline_unix_ms = Some(
                                    now.saturating_add(
                                        i64::try_from(
                                            grace.saturating_add(120).saturating_mul(1000),
                                        )
                                        .unwrap_or(i64::MAX),
                                    ),
                                );
                            }
                            if target.restarted
                                && task.is_some_and(|task| {
                                    task.container_id.as_deref() == Some(&target.container_id)
                                        && task.image_id.as_deref() == Some(&target.image_id)
                                        && task.observed == ObservedTaskState::Healthy
                                })
                            {
                                target.completed = true;
                            }
                            operation.view.phase = if target.restarted {
                                "waiting_health"
                            } else {
                                "restarting"
                            }
                            .into();
                        }
                    }
                    operation.view.completed_tasks =
                        operation.tasks.iter().filter(|task| task.completed).count();
                    if operation.tasks.iter().all(|task| task.completed) {
                        succeed(&mut operation, now);
                    }
                } else {
                    if let Some(generation) = operation.view.deployment_generation {
                        let deployment = inner
                            .state
                            .stacks
                            .get(&operation.stack)
                            .and_then(|stack| stack.deployment.as_ref())
                            .filter(|deployment| deployment.generation == generation);
                        match deployment {
                            Some(deployment)
                                if deployment.status == StackDeploymentStatus::Healthy =>
                            {
                                succeed(&mut operation, now)
                            }
                            Some(deployment)
                                if deployment.status == StackDeploymentStatus::Failed =>
                            {
                                fail(&mut operation, "deployment_failed", now)
                            }
                            Some(deployment) => {
                                operation.view.stalled =
                                    deployment.status == StackDeploymentStatus::Stalled;
                                operation.view.blocked =
                                    deployment.status == StackDeploymentStatus::Blocked;
                                operation.view.phase = "waiting_health".into();
                                operation.view.completed_tasks = inner
                                    .state
                                    .tasks
                                    .values()
                                    .filter(|task| {
                                        task.service_id == operation.view.service
                                            && task.revision == operation.target_revision
                                            && task.observed == ObservedTaskState::Healthy
                                    })
                                    .count();
                            }
                            None => fail(&mut operation, "superseded", now),
                        }
                    } else if now.saturating_sub(operation.created_at_unix_ms) > 15 * 60 * 1000 {
                        fail(&mut operation, "image_preparation_timed_out", now);
                    }
                }
            }
            // Revisions are operation-local and monotonic across controller restarts.
            if old != serde_json::to_vec(&operation).expect("serializable operation") {
                operation.view.revision = operation.view.revision.saturating_add(1);
                operation.updated_at_unix_ms = now;
            }
            inner.state.service_operations.insert(id, operation);
        }
        let removable = inner
            .state
            .service_operations
            .values()
            .filter(|op| {
                !op.view.status.active()
                    && !op.pins_images(&inner.state)
                    && op
                        .finished_at_unix_ms
                        .is_some_and(|finished| now.saturating_sub(finished) >= 90 * 86_400_000)
            })
            .map(|op| op.view.operation_id.clone())
            .collect::<Vec<_>>();
        for id in removable {
            inner.state.service_operations.remove(&id);
        }
        before
            != serde_json::to_vec(&inner.state.service_operations).expect("serializable operations")
    }

    pub(super) async fn start_prepared_service_operation(&self) {
        let ready = {
            let inner = self.inner.lock().await;
            inner
                .state
                .service_operations
                .values()
                .find(|op| {
                    op.view.status.active()
                        && op.view.kind == ServiceOperationKind::PullRedeploy
                        && op.view.deployment_generation.is_none()
                        && !op.preparing_nodes.is_empty()
                        && op
                            .preparing_nodes
                            .iter()
                            .all(|node| op.prepared_images.contains_key(node))
                })
                .map(|op| (op.view.operation_id.clone(), op.view.service.clone()))
        };
        if let Some((id, service)) = ready
            && let Err(error) = self.apply_prepared_service_operation(&id, &service).await
        {
            warn!(operation_id = %id, ?error, "could not start prepared service deployment");
            if matches!(
                error,
                ControllerError::Storage(_) | ControllerError::Conflict(_)
            ) {
                return;
            }
            let mut inner = self.inner.lock().await;
            let previous = inner.state.clone();
            if let Some(op) = inner.state.service_operations.get_mut(&id) {
                fail(op, "deployment_start_failed", unix_ms());
                op.view.revision += 1;
            }
            if self.commit_locked(&mut inner).await.is_err() {
                inner.state = previous;
            }
        }
    }
}

pub(super) fn candidate_nodes(
    state: &ClusterState,
    service: &ServiceRecord,
    live: &BTreeSet<String>,
) -> Result<BTreeSet<String>, ControllerError> {
    let candidates = state
        .nodes
        .values()
        .filter(|node| {
            live.contains(&node.id)
                && scheduler::matches_constraints(node, &service.spec.constraints)
        })
        .collect::<Vec<_>>();
    if candidates
        .iter()
        .any(|node| !node.supports_service_operations)
    {
        return Err(ControllerError::Conflict(
            "matching agents must support service operations".into(),
        ));
    }
    Ok(candidates.into_iter().map(|node| node.id.clone()).collect())
}

pub(super) fn fail(operation: &mut ServiceOperationRecord, error: &str, now: i64) {
    operation.view.status = ServiceOperationStatus::Failed;
    operation.view.phase = "failed".into();
    operation.view.error = Some(error.into());
    operation.finished_at_unix_ms = Some(now);
}
fn succeed(operation: &mut ServiceOperationRecord, now: i64) {
    operation.view.error = None;
    operation.view.blocked = false;
    operation.view.stalled = false;
    operation.view.status = ServiceOperationStatus::Succeeded;
    operation.view.phase = "complete".into();
    operation.view.completed_tasks = operation.view.total_tasks;
    operation.finished_at_unix_ms = Some(now);
}
