use super::*;

impl Controller {
    pub(super) async fn bootstrap(&self) -> Result<BootstrapResponse, ControllerError> {
        let inner = self.inner.lock().await;
        Ok(BootstrapResponse {
            cluster: inner.cluster.clone(),
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
        if request.node_id == self.config.cluster.controller_id {
            return Err(ControllerError::Conflict(
                "the controller node is fixed and cannot join through the agent join API"
                    .to_owned(),
            ));
        }

        let mut inner = self.inner.lock().await;
        let cluster = inner.cluster.clone();
        let previous = inner.state.clone();
        let mut changed = false;
        let gateway_enabled = if let Some(existing) = inner.state.members.get(node_id).cloned() {
            if request.gateway_enabled != existing.gateway_enabled {
                return Err(ControllerError::Conflict(
                    "this node is already joined with a different gateway setting; use `swarmlite gateway enable` or `disable`"
                        .to_owned(),
                ));
            }
            if !request.labels.is_empty() && request.labels != existing.labels {
                return Err(ControllerError::Conflict(
                    "this node is already joined with different labels; use `swarmlite node label set` or `remove`"
                        .to_owned(),
                ));
            }
            if inner.state.members[node_id].address != request.address {
                inner.state.members.get_mut(node_id).unwrap().address = request.address.clone();
                changed = true;
            }
            existing.gateway_enabled
        } else {
            inner.state.members.insert(
                node_id.to_owned(),
                NodeMember {
                    id: node_id.to_owned(),
                    address: request.address.clone(),
                    gateway_enabled: request.gateway_enabled,
                    labels: request.labels.clone(),
                    joined_at_unix_ms: unix_ms(),
                },
            );
            changed = true;
            request.gateway_enabled
        };
        if changed && let Err(error) = self.commit_locked(&mut inner).await {
            inner.state = previous;
            return Err(error.into());
        }
        let labels = inner.state.members[node_id].labels.clone();
        Ok(JoinResponse {
            cluster,
            gateway_enabled,
            labels,
        })
    }

    pub(super) async fn node_gateway(
        &self,
        node_id: &str,
    ) -> Result<NodeGatewayResponse, ControllerError> {
        let inner = self.inner.lock().await;
        let member =
            inner.state.members.get(node_id).ok_or_else(|| {
                ControllerError::NotFound(format!("node {node_id} is not joined"))
            })?;
        Ok(NodeGatewayResponse {
            node_id: node_id.to_owned(),
            enabled: member.gateway_enabled,
        })
    }

    pub(super) async fn update_node_gateway(
        &self,
        node_id: &str,
        update: NodeGatewayUpdate,
    ) -> Result<NodeGatewayResponse, ControllerError> {
        let mut inner = self.inner.lock().await;
        let current = inner
            .state
            .members
            .get(node_id)
            .ok_or_else(|| ControllerError::NotFound(format!("node {node_id} is not joined")))?
            .gateway_enabled;
        if update.enabled == current {
            return Ok(NodeGatewayResponse {
                node_id: node_id.to_owned(),
                enabled: current,
            });
        }

        let previous = inner.state.clone();
        inner
            .state
            .members
            .get_mut(node_id)
            .expect("member was checked above")
            .gateway_enabled = update.enabled;
        if let Some(node) = inner.state.nodes.get_mut(node_id) {
            node.gateway_enabled = update.enabled;
        }
        if let Err(error) = self.commit_locked(&mut inner).await {
            inner.state = previous;
            return Err(error.into());
        }
        info!(node_id, enabled = update.enabled, "updated node gateway");
        Ok(NodeGatewayResponse {
            node_id: node_id.to_owned(),
            enabled: update.enabled,
        })
    }

    pub(super) async fn node_labels(
        &self,
        node_id: &str,
    ) -> Result<NodeLabelsResponse, ControllerError> {
        let inner = self.inner.lock().await;
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

    async fn update_node_label(
        &self,
        node_id: &str,
        key: String,
        value: Option<String>,
    ) -> Result<NodeLabelsResponse, ControllerError> {
        let mut inner = self.inner.lock().await;
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
