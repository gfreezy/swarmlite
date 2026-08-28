use super::*;

impl Controller {
    pub(super) fn gateway_assignment(
        &self,
        inner: &Inner,
        enabled: bool,
    ) -> Option<GatewayAssignment> {
        enabled.then(|| GatewayAssignment {
            generation: inner.gateway_generation,
            config: inner.gateway_config.clone(),
            recovery_snapshot: inner.gateway_snapshot.clone(),
        })
    }

    pub(super) fn rendered_gateway_config(&self, inner: &Inner) -> serde_json::Value {
        gateway::config(
            &inner.state,
            &inner.cluster.gateway.listen,
            self.config.advertise_url.clone(),
        )
    }

    pub(super) fn next_gateway_snapshot(
        &self,
        inner: &Inner,
    ) -> Result<(u64, serde_json::Value, GatewayRecoverySnapshot), StorageError> {
        let config = self.rendered_gateway_config(inner);
        let snapshot_changed = inner.gateway_snapshot.stacks != inner.state.gateway_routes;
        let generation = if config == inner.gateway_config && !snapshot_changed {
            inner.gateway_generation
        } else {
            inner
                .gateway_generation
                .checked_add(1)
                .ok_or_else(|| StorageError::Backend("gateway generation overflow".to_owned()))?
        };
        let snapshot = GatewayRecoverySnapshot::new(
            inner.cluster.cluster_id.clone(),
            generation,
            inner.state.gateway_routes.clone(),
        );
        Ok((generation, config, snapshot))
    }

    pub(super) fn refresh_gateway_snapshot(&self, inner: &mut Inner) -> Result<(), StorageError> {
        let (generation, config, snapshot) = self.next_gateway_snapshot(inner)?;
        inner.gateway_generation = generation;
        inner.gateway_config = config;
        inner.gateway_snapshot = snapshot;
        Ok(())
    }

    pub(super) async fn acknowledge_gateway_drains(
        &self,
        inner: &mut Inner,
    ) -> Result<(), ControllerError> {
        let enabled = inner
            .state
            .members
            .values()
            .filter(|member| member.gateway_enabled)
            .map(|member| member.id.as_str())
            .collect::<Vec<_>>();
        if enabled.is_empty()
            || !enabled.iter().all(|node_id| {
                inner.gateway_reports.get(*node_id).is_some_and(|report| {
                    report.error.is_none()
                        && report.applied_generation == Some(inner.gateway_generation)
                })
            })
        {
            return Ok(());
        }

        let deadline = unix_ms() + self.config.gateway_drain_timeout_seconds as i64 * 1000;
        let previous = inner.state.clone();
        let mut changed = false;
        for task in inner.state.tasks.values_mut() {
            if task.desired == DesiredTaskState::Draining && task.drain_until_unix_ms.is_none() {
                task.drain_until_unix_ms = Some(deadline);
                changed = true;
            }
        }
        if changed && let Err(error) = self.commit_gateway_ack_locked(inner).await {
            inner.state = previous;
            return Err(error.into());
        }
        Ok(())
    }
}
