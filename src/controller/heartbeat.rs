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
                    if report.observed == ObservedTaskState::Failed {
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
            let (task_changed, deployment_changed) =
                apply_task_result(&mut inner.state, node_id, report);
            soft_changed |= task_changed;
            changed |= deployment_changed;
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
            changed = true;
        }

        let live = current_live_nodes(&inner, self.config.node_timeout_seconds);
        changed |= scheduler::reconcile(&mut inner.state, &live);
        changed |= refresh_stack_deployments(
            &mut inner.state,
            unix_ms(),
            self.config.deployment_timeout_seconds,
        );
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
                })
            })
            .collect();
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
