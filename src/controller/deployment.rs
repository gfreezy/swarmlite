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

    fn refreshes_images(self) -> bool {
        matches!(self, Self::DetectChanges)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ServiceUpdatePlan {
    VerifyOnly,
    ResolveImage,
    Reconcile { bump_revision: bool },
}

fn service_update_plan(
    previous: &ServiceRecord,
    next: &swarmlite_stack::ServiceSpec,
    routing_ports_changed: bool,
    revision_policy: RevisionPolicy<'_>,
) -> ServiceUpdatePlan {
    let service_changed = previous.deleted || previous.spec != *next || routing_ports_changed;
    if revision_policy.forces(&previous.name) {
        return ServiceUpdatePlan::Reconcile {
            bump_revision: true,
        };
    }
    if service_changed {
        return ServiceUpdatePlan::Reconcile {
            bump_revision: !revision_policy.preserves(&previous.name),
        };
    }
    if revision_policy.refreshes_images() && next.pull_policy.refreshes_cached_image(&next.image) {
        ServiceUpdatePlan::ResolveImage
    } else {
        ServiceUpdatePlan::VerifyOnly
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
    pub(super) async fn validate_apply(
        &self,
        stack_name: &str,
        parsed: &ParsedStack,
    ) -> Result<(), ControllerError> {
        validate_stack_name(stack_name)?;
        let _deployment = self.begin_stack_deployment(stack_name)?;
        let inner = self.inner.lock().await;
        validate_apply_locked(&inner, stack_name, &parsed.gateway)
    }

    #[cfg(test)]
    pub(super) async fn apply(
        &self,
        stack_name: &str,
        parsed: ParsedStack,
    ) -> Result<StackDeploymentResponse, ControllerError> {
        self.apply_with_registry_credentials(stack_name, parsed, BTreeMap::new())
            .await
    }

    pub(super) async fn apply_with_registry_credentials(
        &self,
        stack_name: &str,
        parsed: ParsedStack,
        registry_credentials: BTreeMap<String, RegistryCredential>,
    ) -> Result<StackDeploymentResponse, ControllerError> {
        validate_stack_name(stack_name)?;
        let _deployment = self.begin_stack_deployment(stack_name)?;
        self.apply_guarded(
            stack_name,
            parsed,
            RevisionPolicy::DetectChanges,
            registry_credentials,
        )
        .await
    }

    pub(super) async fn scale_service(
        &self,
        target: &str,
        replicas: u32,
    ) -> Result<StackDeploymentResponse, ControllerError> {
        let service = {
            let inner = self.inner.lock().await;
            resolve_service(&inner.state, target, "scale")?
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
        self.apply_guarded(
            &stack_name,
            parsed,
            RevisionPolicy::Preserve(&service.name),
            BTreeMap::new(),
        )
        .await
    }

    pub(super) async fn force_update_service(
        &self,
        target: &str,
    ) -> Result<StackDeploymentResponse, ControllerError> {
        let service = {
            let inner = self.inner.lock().await;
            resolve_service(&inner.state, target, "restart")?
        };
        let _deployment = self.begin_stack_deployment(&service.stack)?;
        let parsed = {
            let inner = self.inner.lock().await;
            current_stack(&inner.state, &service.stack)?
        };
        self.apply_guarded(
            &service.stack,
            parsed,
            RevisionPolicy::Force(&service.name),
            BTreeMap::new(),
        )
        .await
    }

    pub(super) async fn remove_stack(
        &self,
        stack_name: &str,
    ) -> Result<StackDeploymentResponse, ControllerError> {
        validate_stack_name(stack_name)?;
        {
            let inner = self.inner.lock().await;
            require_stack(&inner.state, stack_name, "rm")?;
        }
        let _deployment = self.begin_stack_deployment(stack_name)?;
        self.apply_guarded(
            stack_name,
            ParsedStack {
                services: Default::default(),
                gateway: Default::default(),
            },
            RevisionPolicy::DetectChanges,
            BTreeMap::new(),
        )
        .await
    }

    async fn apply_guarded(
        &self,
        stack_name: &str,
        parsed: ParsedStack,
        revision_policy: RevisionPolicy<'_>,
        registry_credentials: BTreeMap<String, RegistryCredential>,
    ) -> Result<StackDeploymentResponse, ControllerError> {
        let ParsedStack {
            services,
            gateway: stack_gateway,
        } = parsed;
        let mut inner = self.inner.lock().await;
        validate_apply_locked(&inner, stack_name, &stack_gateway)?;
        let previous = inner.state.clone();
        inner
            .state
            .registry_credentials
            .extend(registry_credentials);
        let deployment_generation = inner.generation + 1;
        let started_at_unix_ms = unix_ms();
        let previous_gateway = inner
            .state
            .stacks
            .get(stack_name)
            .map(|stack| stack.gateway.clone())
            .unwrap_or_default();
        let live = current_live_nodes(&inner, self.config.node_timeout_seconds);
        let wait_for_gateway =
            !previous_gateway.http_routes.is_empty() || !stack_gateway.http_routes.is_empty();
        let desired_ids: BTreeSet<String> = services
            .keys()
            .map(|name| service_id(stack_name, name))
            .collect();
        let mut new_service_ids = BTreeSet::new();
        let mut image_resolutions = BTreeMap::new();
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
            let update_plan = previous.services.get(&id).map(|existing| {
                service_update_plan(existing, &spec, routing_ports_changed, revision_policy)
            });
            match inner.state.services.get_mut(&id) {
                Some(existing) => {
                    match update_plan.expect("existing service has a previous record") {
                        ServiceUpdatePlan::VerifyOnly => {
                            if revision_policy.refreshes_images() {
                                image_resolutions.insert(
                                    id.clone(),
                                    DeploymentImageResolutionRecord {
                                        service_id: id,
                                        service: name,
                                        image: spec.image.clone(),
                                        baseline_revision: existing.revision,
                                        status: ImageResolutionStatus::Skipped,
                                        nodes: BTreeMap::new(),
                                    },
                                );
                            }
                        }
                        ServiceUpdatePlan::ResolveImage => {
                            if existing.revision == u64::MAX {
                                let service_id = existing.id.clone();
                                inner.state = previous;
                                return Err(ControllerError::Invalid(format!(
                                    "service {service_id:?} revision overflow"
                                )));
                            }
                            let mut targets =
                                BTreeMap::<String, DeploymentImageResolutionNodeRecord>::new();
                            for task in previous.tasks.values().filter(|task| {
                                task.service_id == existing.id
                                    && task.revision == existing.revision
                                    && task.desired == DesiredTaskState::Running
                                    && live.contains(&task.node_id)
                                    && task.container_id.is_some()
                                    && matches!(
                                        task.observed,
                                        ObservedTaskState::Starting
                                            | ObservedTaskState::Running
                                            | ObservedTaskState::Healthy
                                    )
                            }) {
                                targets
                                    .entry(task.node_id.clone())
                                    .or_insert_with(|| DeploymentImageResolutionNodeRecord {
                                        task_ids: Vec::new(),
                                        status: ImageResolutionStatus::Checking,
                                        old_image_ids: BTreeMap::new(),
                                        resolved_image_id: None,
                                        error: None,
                                    })
                                    .task_ids
                                    .push(task.id.clone());
                            }
                            let status = if targets.is_empty() {
                                ImageResolutionStatus::Unchanged
                            } else {
                                ImageResolutionStatus::Checking
                            };
                            image_resolutions.insert(
                                id.clone(),
                                DeploymentImageResolutionRecord {
                                    service_id: id,
                                    service: name,
                                    image: spec.image.clone(),
                                    baseline_revision: existing.revision,
                                    status,
                                    nodes: targets,
                                },
                            );
                        }
                        ServiceUpdatePlan::Reconcile { bump_revision } => {
                            if bump_revision {
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
                    }
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
                    wait_for_gateway,
                    finished_at_unix_ms: None,
                    errors: Vec::new(),
                    image_resolutions,
                }),
            },
        );
        restore_unclaimed_service_revisions(&mut inner.state, &new_service_ids);
        adopt_unclaimed_tasks(&mut inner.state, stack_name);
        scheduler::reconcile(&mut inner.state, &live);
        if let Err(error) = self.refresh_stack_deployments_locked(&mut inner, unix_ms()) {
            inner.state = previous;
            return Err(error.into());
        }
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

    pub(super) fn refresh_stack_deployments_locked(
        &self,
        inner: &mut Inner,
        now_unix_ms: i64,
    ) -> Result<bool, StorageError> {
        let (gateway_generation, _) = self.next_gateway_snapshot(inner)?;
        let enabled = inner
            .state
            .members
            .values()
            .filter(|member| member.gateway_enabled)
            .map(|member| member.id.as_str())
            .collect::<Vec<_>>();
        let gateway_ready = !enabled.is_empty()
            && enabled.iter().all(|node_id| {
                inner.gateway_reports.get(*node_id).is_some_and(|report| {
                    report.error.is_none() && report.applied_generation == Some(gateway_generation)
                })
            });
        Ok(refresh_stack_deployments(
            &mut inner.state,
            now_unix_ms,
            self.config.deployment_timeout_seconds,
            gateway_ready,
        ))
    }
}

fn validate_apply_locked(
    inner: &Inner,
    stack_name: &str,
    stack_gateway: &StackGatewaySpec,
) -> Result<(), ControllerError> {
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
    validate_gateway_hostname_ownership(&inner.state, stack_name, stack_gateway)?;
    inner
        .generation
        .checked_add(1)
        .ok_or_else(|| ControllerError::Invalid("control-plane generation overflow".to_owned()))?;
    Ok(())
}

pub(super) fn refresh_stack_deployments(
    state: &mut ClusterState,
    now_unix_ms: i64,
    timeout_seconds: u64,
    gateway_ready: bool,
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
        let next_status =
            if deployment_is_healthy(state, &stack_name, deployment.generation, gateway_ready) {
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

pub(super) fn apply_image_progress(
    state: &mut ClusterState,
    node_id: &str,
    progress: &ImageResolutionProgress,
) -> bool {
    if !matches!(
        progress.status,
        ImageResolutionStatus::Checking
            | ImageResolutionStatus::Pulling
            | ImageResolutionStatus::Comparing
    ) {
        return false;
    }
    let Some(deployment) = state
        .stacks
        .values_mut()
        .filter_map(|stack| stack.deployment.as_mut())
        .find(|deployment| {
            deployment.generation == progress.deployment_generation
                && deployment.status == StackDeploymentStatus::Deploying
        })
    else {
        return false;
    };
    let mut changed = false;
    for resolution in deployment
        .image_resolutions
        .values_mut()
        .filter(|resolution| resolution.image == progress.image && !resolution.status.is_complete())
    {
        let Some(node) = resolution.nodes.get_mut(node_id) else {
            continue;
        };
        if !node.status.is_complete() && node.status != progress.status {
            node.status = progress.status;
            changed = true;
        }
        let aggregate = resolution
            .nodes
            .values()
            .filter(|node| !node.status.is_complete())
            .map(|node| node.status)
            .max()
            .unwrap_or(resolution.status);
        if !resolution.status.is_complete() && resolution.status != aggregate {
            resolution.status = aggregate;
            changed = true;
        }
    }
    changed
}

pub(super) fn apply_image_resolution_report(
    state: &mut ClusterState,
    node_id: &str,
    report: &ImageResolutionReport,
) -> bool {
    let Some(stack_name) = state.stacks.iter().find_map(|(stack_name, stack)| {
        stack
            .deployment
            .as_ref()
            .filter(|deployment| {
                deployment.generation == report.deployment_generation
                    && deployment.status == StackDeploymentStatus::Deploying
            })
            .map(|_| stack_name.clone())
    }) else {
        return false;
    };

    let expected = state
        .stacks
        .get(&stack_name)
        .and_then(|stack| stack.deployment.as_ref())
        .into_iter()
        .flat_map(|deployment| deployment.image_resolutions.values())
        .filter(|resolution| {
            resolution.image == report.image
                && !resolution.status.is_complete()
                && resolution
                    .nodes
                    .get(node_id)
                    .is_some_and(|node| !node.status.is_complete())
        })
        .map(|resolution| (resolution.service_id.clone(), resolution.clone()))
        .collect::<BTreeMap<_, _>>();
    if expected.is_empty() {
        return false;
    }

    let validation_error = report.error.clone().or_else(|| {
        let Some(resolved_image_id) = report.resolved_image_id.as_deref() else {
            return Some("runtime did not report the resolved image ID".to_owned());
        };
        if resolved_image_id.is_empty() {
            return Some("runtime reported an empty image ID".to_owned());
        }
        let reported = report
            .services
            .iter()
            .map(|service| service.service_id.as_str())
            .collect::<BTreeSet<_>>();
        let expected_ids = expected.keys().map(String::as_str).collect::<BTreeSet<_>>();
        if reported != expected_ids {
            return Some("image resolution report did not cover the assigned services".to_owned());
        }
        for service in &report.services {
            let expected_tasks = expected[&service.service_id]
                .nodes
                .get(node_id)
                .expect("target node was filtered above")
                .task_ids
                .iter()
                .map(String::as_str)
                .collect::<BTreeSet<_>>();
            let reported_tasks = service
                .old_image_ids
                .keys()
                .map(String::as_str)
                .collect::<BTreeSet<_>>();
            if expected_tasks != reported_tasks {
                return Some(format!(
                    "image resolution report for service {:?} did not cover its running tasks",
                    service.service_id
                ));
            }
            let changed = service
                .old_image_ids
                .values()
                .any(|image_id| image_id != resolved_image_id);
            if service.changed != changed {
                return Some(format!(
                    "image resolution report for service {:?} contained an inconsistent comparison",
                    service.service_id
                ));
            }
        }
        None
    });
    if let Some(message) = validation_error {
        let deployment = state
            .stacks
            .get_mut(&stack_name)
            .and_then(|stack| stack.deployment.as_mut())
            .expect("deployment was found above");
        for resolution in deployment
            .image_resolutions
            .values_mut()
            .filter(|resolution| expected.contains_key(&resolution.service_id))
        {
            resolution.status = ImageResolutionStatus::Failed;
            if let Some(node) = resolution.nodes.get_mut(node_id) {
                node.status = ImageResolutionStatus::Failed;
                node.error = Some(message.clone());
            }
            let error = StackDeploymentError {
                task_id: format!("image:{}", report.image),
                service: resolution.service.clone(),
                node_id: node_id.to_owned(),
                phase: crate::model::TaskReconcilePhase::Pull,
                message: message.clone(),
            };
            if !deployment.errors.contains(&error) {
                deployment.errors.push(error);
            }
        }
        deployment.status = StackDeploymentStatus::Failed;
        deployment.finished_at_unix_ms = Some(unix_ms());
        return true;
    }

    let resolved_image_id = report
        .resolved_image_id
        .as_ref()
        .expect("validated successful report has an image ID")
        .clone();
    let reports = report
        .services
        .iter()
        .map(|service| (service.service_id.as_str(), service))
        .collect::<BTreeMap<_, _>>();
    let mut changed_services = Vec::new();
    {
        let deployment = state
            .stacks
            .get_mut(&stack_name)
            .and_then(|stack| stack.deployment.as_mut())
            .expect("deployment was found above");
        for resolution in deployment
            .image_resolutions
            .values_mut()
            .filter(|resolution| expected.contains_key(&resolution.service_id))
        {
            let service_report = reports[resolution.service_id.as_str()];
            let node = resolution
                .nodes
                .get_mut(node_id)
                .expect("target node was validated above");
            node.old_image_ids.clone_from(&service_report.old_image_ids);
            node.resolved_image_id = Some(resolved_image_id.clone());
            node.error = None;
            node.status = if service_report.changed {
                ImageResolutionStatus::Changed
            } else {
                ImageResolutionStatus::Unchanged
            };
            if resolution
                .nodes
                .values()
                .all(|node| node.status.is_complete())
            {
                resolution.status = if resolution
                    .nodes
                    .values()
                    .any(|node| node.status == ImageResolutionStatus::Changed)
                {
                    changed_services
                        .push((resolution.service_id.clone(), resolution.baseline_revision));
                    ImageResolutionStatus::Changed
                } else {
                    ImageResolutionStatus::Unchanged
                };
            }
        }
    }
    for (service_id, baseline_revision) in changed_services {
        let service = state
            .services
            .get_mut(&service_id)
            .expect("image resolution references an existing service");
        if service.revision == baseline_revision {
            service.revision = service
                .revision
                .checked_add(1)
                .expect("image resolution revision overflow was validated at apply time");
        }
    }
    true
}

fn deployment_is_healthy(
    state: &ClusterState,
    stack_name: &str,
    generation: u64,
    gateway_ready: bool,
) -> bool {
    let Some(stack) = state.stacks.get(stack_name) else {
        return false;
    };
    let Some(deployment) = stack
        .deployment
        .as_ref()
        .filter(|deployment| deployment.generation == generation)
    else {
        return false;
    };
    if !deployment
        .image_resolutions
        .values()
        .all(|resolution| resolution.status.is_complete())
    {
        return false;
    }
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
                    && task.ports.iter().all(|port| port.published.is_some())
            })
            .count()
            >= service.spec.replicas as usize
    });
    (!deployment.wait_for_gateway || gateway_ready)
        && replicas_healthy
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
    let mut task_phase_counts = BTreeMap::new();
    for progress in inner
        .task_progress
        .values()
        .filter(|progress| progress.desired_generation == generation)
    {
        let Some(task) = inner.state.tasks.get(&progress.task_id) else {
            continue;
        };
        let Some(service) = inner.state.services.get(&task.service_id) else {
            continue;
        };
        if service.stack == stack_name {
            *task_phase_counts.entry(progress.phase).or_insert(0_u32) += 1;
        }
    }
    let task_phases = task_phase_counts
        .into_iter()
        .map(|(phase, tasks)| StackDeploymentTaskPhaseProgress { phase, tasks })
        .collect();
    let image_resolutions = deployment
        .image_resolutions
        .values()
        .map(|resolution| StackDeploymentImageProgress {
            service: resolution.service.clone(),
            image: resolution.image.clone(),
            status: resolution.status,
            completed_nodes: u32::try_from(
                resolution
                    .nodes
                    .values()
                    .filter(|node| node.status.is_complete())
                    .count(),
            )
            .unwrap_or(u32::MAX),
            total_nodes: u32::try_from(resolution.nodes.len()).unwrap_or(u32::MAX),
        })
        .collect();
    let pending_removals = u32::try_from(
        inner
            .state
            .tasks
            .values()
            .filter(|task| {
                let Some(service) = inner.state.services.get(&task.service_id) else {
                    return false;
                };
                if service.stack != stack_name {
                    return false;
                }
                let is_current = !service.deleted
                    && task.revision == service.revision
                    && task.desired == DesiredTaskState::Running;
                !is_current && task.observed != ObservedTaskState::Lost
            })
            .count(),
    )
    .unwrap_or(u32::MAX);
    let gateway = deployment.wait_for_gateway.then(|| {
        let enabled = inner
            .state
            .members
            .values()
            .filter(|member| member.gateway_enabled)
            .map(|member| member.id.as_str())
            .collect::<Vec<_>>();
        let applied_nodes = enabled
            .iter()
            .filter(|node_id| {
                inner.gateway_reports.get(**node_id).is_some_and(|report| {
                    report.error.is_none()
                        && report.applied_generation == Some(inner.gateway_generation)
                })
            })
            .count();
        let errors = enabled
            .iter()
            .filter_map(|node_id| {
                inner
                    .gateway_reports
                    .get(*node_id)
                    .and_then(|report| report.error.clone())
                    .map(|error| ((*node_id).to_owned(), error))
            })
            .collect();
        StackDeploymentGatewayProgress {
            generation: inner.gateway_generation,
            applied_nodes: u32::try_from(applied_nodes).unwrap_or(u32::MAX),
            total_nodes: u32::try_from(enabled.len()).unwrap_or(u32::MAX),
            errors,
        }
    });
    Ok(StackDeploymentResponse {
        stack: stack_name.to_owned(),
        generation,
        revision: inner.status_revision,
        status: deployment.status,
        started_at_unix_ms: deployment.started_at_unix_ms,
        finished_at_unix_ms: deployment.finished_at_unix_ms,
        services,
        pending_removals,
        task_phases,
        image_resolutions,
        gateway,
        errors: deployment.errors.clone(),
    })
}
