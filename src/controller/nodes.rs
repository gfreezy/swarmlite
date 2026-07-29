use super::*;

impl Controller {
    pub(super) async fn bootstrap(&self) -> Result<BootstrapResponse, ControllerError> {
        let inner = self.inner.lock().await;
        let cluster = self.cluster_settings(&inner)?;
        let (controller_set_generation, voters) = self.repository.controller_set();
        Ok(BootstrapResponse {
            cluster,
            controllers: controller_urls(&inner.state, Some(&self.config.advertise_url), &voters),
            controller_set_generation,
        })
    }

    pub(super) async fn join_node(
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

    pub(super) async fn node_roles(
        &self,
        node_id: &str,
    ) -> Result<NodeRolesResponse, ControllerError> {
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

    pub(super) async fn update_node_roles(
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

    pub(super) async fn node_labels(
        &self,
        node_id: &str,
    ) -> Result<NodeLabelsResponse, ControllerError> {
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

    pub(super) async fn set_node_label(
        &self,
        node_id: &str,
        request: NodeLabelSetRequest,
    ) -> Result<NodeLabelsResponse, ControllerError> {
        validate_node_label(&request.key, &request.value)?;
        self.update_node_label(node_id, request.key, Some(request.value))
            .await
    }

    pub(super) async fn remove_node_label(
        &self,
        node_id: &str,
        request: NodeLabelRemoveRequest,
    ) -> Result<NodeLabelsResponse, ControllerError> {
        validate_node_label_key(&request.key)?;
        self.update_node_label(node_id, request.key, None).await
    }

    pub(super) async fn update_node_label(
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
}
