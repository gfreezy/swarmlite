use std::collections::BTreeSet;

use crate::model::{ClusterState, ServiceRecord, StackGatewaySpec};
use swarmlite_stack::ParsedStack;

use super::ControllerError;

pub(super) fn service_id(stack: &str, service: &str) -> String {
    format!("{stack}.{service}")
}

pub(super) fn resolve_service(
    state: &ClusterState,
    target: &str,
) -> Result<ServiceRecord, ControllerError> {
    state
        .services
        .get(target)
        .filter(|service| !service.deleted)
        .cloned()
        .ok_or_else(|| ControllerError::NotFound(format!("service {target:?} not found")))
}

pub(super) fn current_stack(
    state: &ClusterState,
    stack_name: &str,
) -> Result<ParsedStack, ControllerError> {
    let stack = state
        .stacks
        .get(stack_name)
        .ok_or_else(|| ControllerError::NotFound(format!("stack {stack_name:?} not found")))?;
    let services = state
        .services
        .values()
        .filter(|service| service.stack == stack_name && !service.deleted)
        .map(|service| (service.name.clone(), service.spec.clone()))
        .collect();
    Ok(ParsedStack {
        services,
        gateway: stack.gateway.clone(),
    })
}

pub(super) fn stack_is_active(state: &ClusterState, stack_name: &str) -> bool {
    state
        .services
        .values()
        .any(|service| service.stack == stack_name && !service.deleted)
        || state
            .stacks
            .get(stack_name)
            .is_some_and(|stack| !stack.gateway.http_routes.is_empty())
}

pub(super) fn validate_gateway_hostname_ownership(
    state: &ClusterState,
    stack_name: &str,
    gateway: &StackGatewaySpec,
) -> Result<(), ControllerError> {
    let requested = gateway
        .http_routes
        .iter()
        .flat_map(|route| route.hostnames.iter())
        .collect::<BTreeSet<_>>();
    for stack in state
        .stacks
        .values()
        .filter(|stack| stack.name != stack_name)
    {
        if let Some(hostname) = stack
            .gateway
            .http_routes
            .iter()
            .flat_map(|route| route.hostnames.iter())
            .find(|hostname| requested.contains(hostname))
        {
            return Err(ControllerError::Conflict(format!(
                "gateway hostname {hostname:?} is already owned by stack {:?}",
                stack.name
            )));
        }
    }
    Ok(())
}

pub(super) fn validate_stack_name(name: &str) -> Result<(), ControllerError> {
    swarmlite_stack::validate_stack_name(name)
        .map_err(|error| ControllerError::Invalid(error.to_string()))
}
