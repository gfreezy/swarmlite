use super::*;

impl Controller {
    pub(super) async fn apply(
        &self,
        stack_name: &str,
        parsed: ParsedStack,
    ) -> Result<u64, ControllerError> {
        validate_stack_name(stack_name)?;
        let ParsedStack {
            services,
            gateway: stack_gateway,
        } = parsed;
        let has_gateway = {
            let inner = self.inner.lock().await;
            inner
                .state
                .members
                .values()
                .any(|member| member.gateway_enabled)
        };
        if !has_gateway && !stack_gateway.http_routes.is_empty() {
            return Err(ControllerError::Invalid(
                "gateway routing is enabled but no node has its gateway enabled".to_owned(),
            ));
        }
        let mut inner = self.inner.lock().await;
        validate_gateway_hostname_ownership(&inner.state, stack_name, &stack_gateway)?;
        let previous = inner.state.clone();
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
                    if existing.spec == spec && !existing.deleted && !routing_ports_changed => {}
                Some(existing) => {
                    existing.revision += 1;
                    existing.spec = spec;
                    existing.deleted = false;
                }
                None => {
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
                applied_at_unix_ms: unix_ms(),
                services: desired_ids.into_iter().collect(),
                gateway: stack_gateway,
            },
        );
        adopt_unclaimed_tasks(&mut inner.state, stack_name);
        let live = current_live_nodes(&inner, self.config.node_timeout_seconds);
        scheduler::reconcile(&mut inner.state, &live);
        if let Err(error) = self.commit_locked(&mut inner).await {
            inner.state = previous;
            return Err(error.into());
        }
        Ok(inner.generation)
    }
}
