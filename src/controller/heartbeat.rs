use super::*;

impl Controller {
    pub(super) async fn heartbeat(
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

    pub(super) async fn status(&self) -> StatusResponse {
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

    pub(super) async fn leader_status(&self) -> Result<StatusResponse, ControllerError> {
        let status = self.status().await;
        if status.is_leader {
            Ok(status)
        } else if self.repository.is_leader() {
            Err(StorageError::Backend("Raft leader is still initializing".to_owned()).into())
        } else {
            Err(self.leader_redirect("/v1/status"))
        }
    }
}
