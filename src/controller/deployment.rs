use super::*;

#[derive(Clone, Copy)]
enum RevisionPolicy<'a> {
    DetectChanges,
    Force(&'a str),
    Preserve(&'a str),
}

impl RevisionPolicy<'_> {
    fn forces(self, service: &str) -> bool {
        matches!(self, Self::Force(target) if target == service)
    }

    fn preserves(self, service: &str) -> bool {
        matches!(self, Self::Preserve(target) if target == service)
    }
}

pub(super) struct StackDeployment<'a> {
    active: &'a std::sync::Mutex<BTreeSet<String>>,
    stack_name: String,
}

impl Drop for StackDeployment<'_> {
    fn drop(&mut self) {
        self.active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.stack_name);
    }
}

impl Controller {
    pub(super) async fn apply(
        &self,
        stack_name: &str,
        parsed: ParsedStack,
    ) -> Result<StackDeploymentResponse, ControllerError> {
        validate_stack_name(stack_name)?;
        let _deployment = self.begin_stack_deployment(stack_name)?;
        self.apply_guarded(stack_name, parsed, RevisionPolicy::DetectChanges)
            .await
    }

    pub(super) async fn scale_service(
        &self,
        target: &str,
        replicas: u32,
    ) -> Result<StackDeploymentResponse, ControllerError> {
        let service = {
            let inner = self.inner.lock().await;
            resolve_service(&inner.state, target)?
        };
        let stack_name = service.stack.clone();
        let _deployment = self.begin_stack_deployment(&stack_name)?;
        let mut parsed = {
            let inner = self.inner.lock().await;
            current_stack(&inner.state, &stack_name)?
        };
        parsed
            .services
            .get_mut(&service.name)
            .expect("resolved service must be present")
            .replicas = replicas;
        self.apply_guarded(&stack_name, parsed, RevisionPolicy::Preserve(&service.name))
            .await
    }

    pub(super) async fn force_update_service(
        &self,
        target: &str,
    ) -> Result<StackDeploymentResponse, ControllerError> {
        let service = {
            let inner = self.inner.lock().await;
            resolve_service(&inner.state, target)?
        };
        let _deployment = self.begin_stack_deployment(&service.stack)?;
        let parsed = {
            let inner = self.inner.lock().await;
            current_stack(&inner.state, &service.stack)?
        };
        self.apply_guarded(&service.stack, parsed, RevisionPolicy::Force(&service.name))
            .await
    }

    pub(super) async fn remove_stack(
        &self,
        stack_name: &str,
    ) -> Result<StackDeploymentResponse, ControllerError> {
        validate_stack_name(stack_name)?;
        {
            let inner = self.inner.lock().await;
            if !stack_is_active(&inner.state, stack_name) {
                return Err(ControllerError::NotFound(format!(
                    "stack {stack_name:?} not found"
                )));
            }
        }
        let _deployment = self.begin_stack_deployment(stack_name)?;
        self.apply_guarded(
            stack_name,
            ParsedStack {
                services: Default::default(),
                gateway: Default::default(),
            },
            RevisionPolicy::DetectChanges,
        )
        .await
    }

    async fn apply_guarded(
        &self,
        stack_name: &str,
        parsed: ParsedStack,
        revision_policy: RevisionPolicy<'_>,
    ) -> Result<StackDeploymentResponse, ControllerError> {
        let ParsedStack {
            services,
            gateway: stack_gateway,
        } = parsed;
        let mut inner = self.inner.lock().await;
        if inner
            .state
            .stacks
            .get(stack_name)
            .and_then(|stack| stack.deployment.as_ref())
            .is_some_and(|deployment| deployment.status == StackDeploymentStatus::Deploying)
        {
            return Err(ControllerError::Conflict(format!(
                "stack {stack_name:?} already has a deployment in progress"
            )));
        }
        let has_gateway = inner
            .state
            .members
            .values()
            .any(|member| member.gateway_enabled);
        if !has_gateway && !stack_gateway.http_routes.is_empty() {
            return Err(ControllerError::Invalid(
                "gateway routing is enabled but no node has its gateway enabled".to_owned(),
            ));
        }
        validate_gateway_hostname_ownership(&inner.state, stack_name, &stack_gateway)?;
        let previous = inner.state.clone();
        let deployment_generation = inner.generation.checked_add(1).ok_or_else(|| {
            ControllerError::Invalid("control-plane generation overflow".to_owned())
        })?;
        let started_at_unix_ms = unix_ms();
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
        let mut new_service_ids = BTreeSet::new();
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
                    if existing.spec == spec
                        && !existing.deleted
                        && !routing_ports_changed
                        && !revision_policy.forces(&name) => {}
                Some(existing) => {
                    if !revision_policy.preserves(&name) {
                        let Some(revision) = existing.revision.checked_add(1) else {
                            let service_id = existing.id.clone();
                            inner.state = previous;
                            return Err(ControllerError::Invalid(format!(
                                "service {service_id:?} revision overflow"
                            )));
                        };
                        existing.revision = revision;
                    }
                    existing.spec = spec;
                    existing.deleted = false;
                }
                None => {
                    new_service_ids.insert(id.clone());
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
                applied_at_unix_ms: started_at_unix_ms,
                services: desired_ids.into_iter().collect(),
                gateway: stack_gateway,
                deployment: Some(StackDeploymentRecord {
                    generation: deployment_generation,
                    status: StackDeploymentStatus::Deploying,
                    started_at_unix_ms,
                    finished_at_unix_ms: None,
                    errors: Vec::new(),
                }),
            },
        );
        restore_unclaimed_service_revisions(&mut inner.state, &new_service_ids);
        adopt_unclaimed_tasks(&mut inner.state, stack_name);
        let live = current_live_nodes(&inner, self.config.node_timeout_seconds);
        scheduler::reconcile(&mut inner.state, &live);
        refresh_stack_deployments(
            &mut inner.state,
            unix_ms(),
            self.config.deployment_timeout_seconds,
        );
        if let Err(error) = self.commit_locked(&mut inner).await {
            inner.state = previous;
            return Err(error.into());
        }
        deployment_response(&inner, stack_name, deployment_generation)
    }

    pub(super) fn begin_stack_deployment(
        &self,
        stack_name: &str,
    ) -> Result<StackDeployment<'_>, ControllerError> {
        let mut active = self
            .deploying_stacks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !active.insert(stack_name.to_owned()) {
            return Err(ControllerError::Conflict(format!(
                "stack {stack_name:?} already has a deployment in progress"
            )));
        }
        Ok(StackDeployment {
            active: &self.deploying_stacks,
            stack_name: stack_name.to_owned(),
        })
    }

    pub(super) async fn wait_for_deployment(
        &self,
        stack_name: &str,
        generation: u64,
        after_revision: Option<u64>,
        wait: Duration,
    ) -> Result<StackDeploymentResponse, ControllerError> {
        let mut changes = self.status_changes.subscribe();
        let snapshot = {
            let inner = self.inner.lock().await;
            deployment_response(&inner, stack_name, generation)?
        };
        if after_revision != Some(snapshot.revision) || snapshot.status.is_terminal() {
            return Ok(snapshot);
        }
        let _ = tokio::time::timeout(wait, changes.changed()).await;
        let inner = self.inner.lock().await;
        deployment_response(&inner, stack_name, generation)
    }
}

pub(super) fn refresh_stack_deployments(
    state: &mut ClusterState,
    now_unix_ms: i64,
    timeout_seconds: u64,
) -> bool {
    let stack_names = state.stacks.keys().cloned().collect::<Vec<_>>();
    let mut changed = false;
    for stack_name in stack_names {
        let Some(deployment) = state
            .stacks
            .get(&stack_name)
            .and_then(|stack| stack.deployment.clone())
        else {
            continue;
        };
        if deployment.status != StackDeploymentStatus::Deploying {
            continue;
        }
        let next_status = if deployment_is_healthy(state, &stack_name, deployment.generation) {
            Some(StackDeploymentStatus::Healthy)
        } else {
            let timeout_ms =
                i64::try_from(timeout_seconds.saturating_mul(1_000)).unwrap_or(i64::MAX);
            (now_unix_ms.saturating_sub(deployment.started_at_unix_ms) >= timeout_ms)
                .then_some(StackDeploymentStatus::TimedOut)
        };
        if let Some(status) = next_status {
            let deployment = state
                .stacks
                .get_mut(&stack_name)
                .and_then(|stack| stack.deployment.as_mut())
                .expect("deployment was present above");
            deployment.status = status;
            deployment.finished_at_unix_ms = Some(now_unix_ms);
            changed = true;
        }
    }
    changed
}

pub(super) fn apply_task_result(
    state: &mut ClusterState,
    node_id: &str,
    report: &TaskReconcileReport,
) -> (bool, bool) {
    let Some(task) = state.tasks.get(&report.task_id) else {
        return (false, false);
    };
    if task.node_id != node_id {
        return (false, false);
    }
    let task_revision = task.revision;
    let Some(service) = state.services.get(&task.service_id) else {
        return (false, false);
    };
    let service_revision = service.revision;
    let stack_name = service.stack.clone();
    let service_name = service.name.clone();
    let Some(deployment) = state
        .stacks
        .get(&stack_name)
        .and_then(|stack| stack.deployment.as_ref())
    else {
        return (false, false);
    };
    if report.desired_generation != deployment.generation
        || report
            .applied_generation
            .is_some_and(|generation| generation != report.desired_generation)
        || (report.error.is_none()) != report.applied_generation.is_some()
    {
        return (false, false);
    }

    let mut soft_changed = false;
    let task = state
        .tasks
        .get_mut(&report.task_id)
        .expect("task was present above");
    let next_error = report.error.as_ref().map(|message| TaskReconcileError {
        phase: report.phase,
        message: message.clone(),
    });
    if task.applied_generation != report.applied_generation || task.reconcile_error != next_error {
        task.applied_generation = report.applied_generation;
        task.reconcile_error = next_error;
        soft_changed = true;
    }
    if report.error.is_some() && task.observed != ObservedTaskState::Failed {
        task.observed = ObservedTaskState::Failed;
        soft_changed = true;
    }

    let Some(message) = report.error.as_ref() else {
        return (soft_changed, false);
    };
    if task_revision != service_revision && report.phase != crate::model::TaskReconcilePhase::Remove
    {
        return (soft_changed, false);
    }
    let deployment = state
        .stacks
        .get_mut(&stack_name)
        .and_then(|stack| stack.deployment.as_mut())
        .expect("deployment was present above");
    if !matches!(
        deployment.status,
        StackDeploymentStatus::Deploying | StackDeploymentStatus::Failed
    ) {
        return (soft_changed, false);
    }
    let error = StackDeploymentError {
        task_id: report.task_id.clone(),
        service: service_name,
        node_id: node_id.to_owned(),
        phase: report.phase,
        message: message.clone(),
    };
    let mut deployment_changed = false;
    if !deployment.errors.contains(&error) {
        deployment.errors.push(error);
        deployment_changed = true;
    }
    if deployment.status == StackDeploymentStatus::Deploying {
        deployment.status = StackDeploymentStatus::Failed;
        deployment.finished_at_unix_ms = Some(unix_ms());
        deployment_changed = true;
    }
    (true, deployment_changed)
}

fn deployment_is_healthy(state: &ClusterState, stack_name: &str, generation: u64) -> bool {
    let Some(stack) = state.stacks.get(stack_name) else {
        return false;
    };
    let replicas_healthy = stack.services.iter().all(|service_id| {
        let Some(service) = state
            .services
            .get(service_id)
            .filter(|service| !service.deleted)
        else {
            return false;
        };
        state
            .tasks
            .values()
            .filter(|task| {
                task.service_id == service.id
                    && task.revision == service.revision
                    && task.desired == DesiredTaskState::Running
                    && task.observed == ObservedTaskState::Healthy
                    && task.applied_generation == Some(generation)
            })
            .count()
            >= service.spec.replicas as usize
    });
    replicas_healthy
        && state.tasks.values().all(|task| {
            let Some(service) = state.services.get(&task.service_id) else {
                return true;
            };
            if service.stack != stack_name {
                return true;
            }
            let is_current = !service.deleted
                && task.revision == service.revision
                && task.desired == DesiredTaskState::Running;
            is_current || task.observed == ObservedTaskState::Lost
        })
}

fn deployment_response(
    inner: &Inner,
    stack_name: &str,
    generation: u64,
) -> Result<StackDeploymentResponse, ControllerError> {
    let stack = inner
        .state
        .stacks
        .get(stack_name)
        .ok_or_else(|| ControllerError::NotFound(format!("stack {stack_name:?} not found")))?;
    let deployment = stack
        .deployment
        .as_ref()
        .filter(|deployment| deployment.generation == generation)
        .ok_or_else(|| {
            ControllerError::NotFound(format!(
                "deployment generation {generation} for stack {stack_name:?} not found"
            ))
        })?;
    let services = stack
        .services
        .iter()
        .filter_map(|service_id| inner.state.services.get(service_id))
        .filter(|service| !service.deleted)
        .map(|service| {
            let tasks = inner.state.tasks.values().filter(|task| {
                task.service_id == service.id
                    && task.revision == service.revision
                    && task.desired == DesiredTaskState::Running
            });
            let mut applied = 0_u32;
            let mut healthy = 0_u32;
            for task in tasks {
                if task.applied_generation == Some(generation) {
                    applied += 1;
                    if task.observed == ObservedTaskState::Healthy {
                        healthy += 1;
                    }
                }
            }
            StackDeploymentServiceProgress {
                service: service.name.clone(),
                replicas: service.spec.replicas,
                applied,
                healthy,
            }
        })
        .collect();
    Ok(StackDeploymentResponse {
        stack: stack_name.to_owned(),
        generation,
        revision: inner.status_revision,
        status: deployment.status,
        started_at_unix_ms: deployment.started_at_unix_ms,
        finished_at_unix_ms: deployment.finished_at_unix_ms,
        services,
        errors: deployment.errors.clone(),
    })
}
