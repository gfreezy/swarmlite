use super::*;

impl Controller {
    pub(super) async fn heartbeat(
        &self,
        node_id: &str,
        heartbeat: NodeHeartbeat,
    ) -> Result<HeartbeatResponse, ControllerError> {
        let NodeHeartbeat {
            mut node,
            tasks,
            task_inventory_error,
            task_results,
            task_progress,
            image_results,
            image_progress,
            gateway: gateway_report,
        } = heartbeat;
        if node_id != node.id {
            return Err(ControllerError::Invalid(
                "node ID in path and request body differ".to_owned(),
            ));
        }
        let mut inner = self.inner.lock().await;
        let previous = inner.state.clone();
        let mut changed = false;
        let mut soft_changed = false;
        let (gateway_enabled, desired_labels) = {
            let member = inner.state.members.get_mut(node_id).ok_or_else(|| {
                ControllerError::Invalid("node must join before sending heartbeats".to_owned())
            })?;
            if member.address != node.address {
                member.address.clone_from(&node.address);
                changed = true;
            }
            (member.gateway_enabled, member.labels.clone())
        };
        node.gateway_enabled = gateway_enabled;
        node.labels.clone_from(&desired_labels);
        if gateway_enabled {
            soft_changed |= inner.gateway_reports.get(node_id) != Some(&gateway_report);
            inner
                .gateway_reports
                .insert(node_id.to_owned(), gateway_report);
        } else {
            soft_changed |= inner.gateway_reports.remove(node_id).is_some();
        }
        inner.live_nodes.insert(node_id.to_owned(), Instant::now());
        inner.state.nodes.insert(node_id.to_owned(), node);

        let reports: HashMap<_, _> = tasks
            .into_iter()
            .map(|report| (report.id.clone(), report))
            .collect();
        let reported_ids = reports.keys().cloned().collect::<BTreeSet<_>>();
        if task_inventory_error.is_none() {
            inner
                .state
                .unclaimed_tasks
                .retain(|task_id, task| task.node_id != node_id || reported_ids.contains(task_id));
        }
        for report in reports.values() {
            if inner.state.tasks.contains_key(&report.id) {
                inner.state.unclaimed_tasks.remove(&report.id);
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
                        config_digests: report.config_digests.clone(),
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
        let mut observed_failures = Vec::new();
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
                    let ports_resolved = task.ports.len() == report.ports.len()
                        && task.ports.iter().all(|expected| {
                            report.ports.iter().any(|actual| {
                                actual.target == expected.target
                                    && actual.protocol == expected.protocol
                                    && actual.published.is_some()
                            })
                        });
                    if ports_resolved && task.ports != report.ports {
                        task.ports.clone_from(&report.ports);
                        changed = true;
                    }
                    if task.config_digests.is_empty() && !report.config_digests.is_empty() {
                        task.config_digests.clone_from(&report.config_digests);
                        changed = true;
                    }
                    if task.desired == DesiredTaskState::Running
                        && report.observed == ObservedTaskState::Failed
                    {
                        observed_failures.push((id.clone(), "container reported failed"));
                    }
                }
                None if task_inventory_error.is_some() => {}
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
                    observed_failures.push((id.clone(), "container disappeared from the runtime"));
                }
                None => {}
            }
        }
        for report in &task_results {
            let phase = if report.error.is_none() {
                crate::model::TaskReconcilePhase::Verify
            } else {
                report.phase
            };
            soft_changed |= apply_task_progress(
                &mut inner,
                node_id,
                &TaskReconcileProgress {
                    task_id: report.task_id.clone(),
                    desired_generation: report.desired_generation,
                    phase,
                },
            );
            let (task_changed, deployment_changed) =
                apply_task_result(&mut inner.state, node_id, report);
            soft_changed |= task_changed;
            changed |= deployment_changed;
        }
        for progress in &image_progress {
            changed |= apply_image_progress(&mut inner.state, node_id, progress);
        }
        for report in &image_results {
            changed |= apply_image_resolution_report(&mut inner.state, node_id, report);
        }
        for progress in &task_progress {
            soft_changed |= apply_task_progress(&mut inner, node_id, progress);
        }
        let reported_failures = task_results
            .iter()
            .filter(|report| report.error.is_some())
            .map(|report| report.task_id.as_str())
            .collect::<BTreeSet<_>>();
        for (task_id, message) in observed_failures {
            if reported_failures.contains(task_id.as_str()) {
                continue;
            }
            let Some(desired_generation) = task_deployment_generation(&inner.state, &task_id)
            else {
                continue;
            };
            let report = TaskReconcileReport {
                task_id,
                desired_generation,
                applied_generation: None,
                phase: crate::model::TaskReconcilePhase::Verify,
                error: Some(message.to_owned()),
            };
            let (task_changed, deployment_changed) =
                apply_task_result(&mut inner.state, node_id, &report);
            soft_changed |= task_changed;
            changed |= deployment_changed;
        }
        for id in remove {
            inner.state.tasks.remove(&id);
            inner.task_progress.retain(|(task_id, _), _| task_id != &id);
            changed = true;
        }

        let live = current_live_nodes(&inner, self.config.node_timeout_seconds);
        changed |= scheduler::reconcile(&mut inner.state, &live);
        changed |= self.refresh_stack_deployments_locked(&mut inner, unix_ms())?;
        if changed && let Err(error) = self.commit_locked(&mut inner).await {
            inner.state = previous;
            return Err(error.into());
        }
        if !changed {
            self.refresh_gateway_snapshot(&mut inner)?;
            if soft_changed {
                self.notify_status_locked(&mut inner);
            }
        }
        self.acknowledge_gateway_drains(&mut inner).await?;

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
                let deployment_generation = inner
                    .state
                    .stacks
                    .get(&service.stack)
                    .and_then(|stack| stack.deployment.as_ref())
                    .map_or(0, |deployment| deployment.generation);
                Some(TaskAssignment {
                    id: task.id.clone(),
                    cluster_id: self.config.cluster.cluster_id.clone(),
                    stack: service.stack.clone(),
                    service: service.name.clone(),
                    service_id: task.service_id.clone(),
                    revision: task.revision,
                    slot: task.slot,
                    desired: task.desired.clone(),
                    spec: service.spec.clone(),
                    ports: task.ports.clone(),
                    generation,
                    deployment_generation,
                    spec_hash: service_spec_hash(&service.spec),
                    image_resolved: image_was_resolved_on_node(&inner.state, service, task),
                })
            })
            .collect();
        let image_assignments = image_assignments_for_node(&inner.state, node_id);
        let remove_tasks = inner
            .state
            .tasks
            .values()
            .filter(|task| task.node_id == node_id && task.desired == DesiredTaskState::Stopped)
            .filter_map(|task| {
                let service = inner.state.services.get(&task.service_id)?;
                let deployment_generation = inner
                    .state
                    .stacks
                    .get(&service.stack)
                    .and_then(|stack| stack.deployment.as_ref())
                    .map_or(0, |deployment| deployment.generation);
                Some(TaskRemovalAssignment {
                    id: task.id.clone(),
                    deployment_generation,
                })
            })
            .collect();
        let gateway_config = self.gateway_assignment(&inner, gateway_enabled);
        let registry_credentials = inner.state.registry_credentials.clone();
        let registry_credentials_hash = crate::registry::credentials_hash(&registry_credentials);
        Ok(HeartbeatResponse {
            generation,
            cluster: inner.cluster.clone(),
            assignments,
            image_assignments,
            gateway_enabled,
            labels: desired_labels,
            remove_tasks,
            gateway_config,
            registry_credentials,
            registry_credentials_hash,
        })
    }

    pub(super) async fn status(&self) -> StatusResponse {
        let inner = self.inner.lock().await;
        let generation = inner.generation;
        let state = inner.state.clone();
        let recovery = recovery_status(&state);
        let enabled = state
            .members
            .values()
            .filter(|member| member.gateway_enabled)
            .map(|member| member.id.as_str())
            .collect::<Vec<_>>();
        let applied_generation = (!enabled.is_empty()
            && enabled.iter().all(|node_id| {
                inner.gateway_reports.get(*node_id).is_some_and(|report| {
                    report.error.is_none()
                        && report.applied_generation == Some(inner.gateway_generation)
                })
            }))
        .then_some(inner.gateway_generation);
        let endpoint_errors = enabled
            .iter()
            .filter_map(|node_id| {
                inner
                    .gateway_reports
                    .get(*node_id)
                    .and_then(|report| report.error.clone())
                    .map(|error| ((*node_id).to_owned(), error))
            })
            .collect();
        StatusResponse {
            cluster_id: self.config.cluster.cluster_id.clone(),
            generation,
            controller_id: self.config.cluster.controller_id.clone(),
            gateway: GatewayStatus {
                enabled: state.members.values().any(|member| member.gateway_enabled),
                desired_generation: inner.gateway_generation,
                applied_generation,
                endpoint_errors,
            },
            recovery,
            state,
        }
    }
}

fn image_assignments_for_node(
    state: &ClusterState,
    node_id: &str,
) -> Vec<ImageResolutionAssignment> {
    let mut grouped = BTreeMap::<(u64, String), BTreeMap<String, Vec<String>>>::new();
    for deployment in state
        .stacks
        .values()
        .filter_map(|stack| stack.deployment.as_ref())
        .filter(|deployment| deployment.status == StackDeploymentStatus::Deploying)
    {
        for resolution in deployment
            .image_resolutions
            .values()
            .filter(|resolution| !resolution.status.is_complete())
        {
            let Some(node) = resolution
                .nodes
                .get(node_id)
                .filter(|node| !node.status.is_complete())
            else {
                continue;
            };
            grouped
                .entry((deployment.generation, resolution.image.clone()))
                .or_default()
                .insert(resolution.service_id.clone(), node.task_ids.clone());
        }
    }
    grouped
        .into_iter()
        .map(
            |((deployment_generation, image), services)| ImageResolutionAssignment {
                deployment_generation,
                image,
                services: services
                    .into_iter()
                    .map(|(service_id, task_ids)| ImageResolutionServiceAssignment {
                        service_id,
                        task_ids,
                    })
                    .collect(),
            },
        )
        .collect()
}

fn image_was_resolved_on_node(
    state: &ClusterState,
    service: &ServiceRecord,
    task: &TaskRecord,
) -> bool {
    state
        .stacks
        .get(&service.stack)
        .and_then(|stack| stack.deployment.as_ref())
        .and_then(|deployment| deployment.image_resolutions.get(&service.id))
        .is_some_and(|resolution| {
            resolution.status == ImageResolutionStatus::Changed
                && resolution.image == service.spec.image
                && resolution.baseline_revision.checked_add(1) == Some(task.revision)
                && resolution.nodes.get(&task.node_id).is_some_and(|node| {
                    node.status.is_complete() && node.resolved_image_id.is_some()
                })
        })
}

fn apply_task_progress(inner: &mut Inner, node_id: &str, progress: &TaskReconcileProgress) -> bool {
    let Some(task) = inner
        .state
        .tasks
        .get(&progress.task_id)
        .filter(|task| task.node_id == node_id)
    else {
        return false;
    };
    let task_id = task.id.clone();
    let Some(deployment_generation) = task_deployment_generation(&inner.state, &task_id) else {
        return false;
    };
    if progress.desired_generation != deployment_generation {
        return false;
    }
    let key = (task_id, progress.phase);
    if inner.task_progress.get(&key) == Some(progress) {
        return false;
    }
    inner.task_progress.insert(key, progress.clone());
    true
}

fn task_deployment_generation(state: &ClusterState, task_id: &str) -> Option<u64> {
    let task = state.tasks.get(task_id)?;
    let service = state.services.get(&task.service_id)?;
    state
        .stacks
        .get(&service.stack)?
        .deployment
        .as_ref()
        .map(|deployment| deployment.generation)
}
