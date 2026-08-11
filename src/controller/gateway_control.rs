use super::*;

impl Controller {
    pub(super) async fn gateway(&self) -> Result<gateway::HttpServer, ControllerError> {
        let inner = self.inner.lock().await;
        Ok(gateway::generate(
            &inner.state,
            &inner.cluster.gateway.listen,
        ))
    }

    pub(super) fn gateway_assignment(
        &self,
        inner: &Inner,
        enabled: bool,
    ) -> Result<Option<GatewayAssignment>, ControllerError> {
        if !enabled {
            return Ok(None);
        }
        let server = gateway::generate(&inner.state, &inner.cluster.gateway.listen);
        let storage = gateway::storage(self.config.advertise_url.clone());
        Ok(Some(GatewayAssignment {
            generation: inner.gateway_generation,
            server: serde_json::to_value(server)
                .map_err(|error| ControllerError::Invalid(error.to_string()))?,
            storage: serde_json::to_value(storage)
                .map_err(|error| ControllerError::Invalid(error.to_string()))?,
        }))
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

        let deadline = unix_ms() + self.config.gateway.drain_timeout_seconds as i64 * 1000;
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
