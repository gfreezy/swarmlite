use std::collections::{BTreeMap, BTreeSet};

use crate::model::{ClusterState, ServiceRecord, StackGatewaySpec};
use swarmlite_stack::ParsedStack;

use super::ControllerError;

pub(super) fn service_id(stack: &str, service: &str) -> String {
    format!("{stack}.{service}")
}

pub(super) fn resolve_service(
    state: &ClusterState,
    target: &str,
    operation: &str,
) -> Result<ServiceRecord, ControllerError> {
    if let Some(service) = state
        .services
        .get(target)
        .filter(|service| !service.deleted)
        .cloned()
    {
        return Ok(service);
    }

    if stack_is_active(state, target) {
        return Err(ControllerError::Invalid(format!(
            "{operation} expects a Service (STACK.SERVICE), but {target:?} is a Stack.{}",
            stack_service_hint(state, target)
        )));
    }
    if target_matches_task(state, target) {
        return Err(ControllerError::Invalid(format!(
            "{operation} expects a Service (STACK.SERVICE), but {target:?} identifies a Task; use the Task's parent Service instead"
        )));
    }

    Err(ControllerError::NotFound(format!(
        "Service {target:?} not found; {operation} expects STACK.SERVICE. Run `swarmlite ls` to list available Services"
    )))
}

pub(super) fn require_stack(
    state: &ClusterState,
    target: &str,
    operation: &str,
) -> Result<(), ControllerError> {
    if stack_is_active(state, target) {
        return Ok(());
    }
    if let Some(service) = state
        .services
        .get(target)
        .filter(|service| !service.deleted)
    {
        return Err(ControllerError::Invalid(format!(
            "{operation} expects a Stack name, but {target:?} is a Service in Stack {:?}; use {:?} instead",
            service.stack, service.stack
        )));
    }
    if target_matches_task(state, target) {
        return Err(ControllerError::Invalid(format!(
            "{operation} expects a Stack name, but {target:?} identifies a Task; use the Task's Stack instead"
        )));
    }

    Err(ControllerError::NotFound(format!(
        "Stack {target:?} not found; {operation} expects a Stack name. Run `swarmlite ls` to list available Stacks"
    )))
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
        || state.gateway_routes.contains_key(stack_name)
}

pub(super) fn target_matches_task(state: &ClusterState, target: &str) -> bool {
    state.tasks.contains_key(target)
        || (!target.is_empty() && state.tasks.keys().any(|id| id.starts_with(target)))
        || state
            .tasks
            .values()
            .any(|task| format!("{}.{}", task.service_id, task.slot.saturating_add(1)) == target)
}

pub(super) fn stack_service_hint(state: &ClusterState, stack: &str) -> String {
    const DISPLAY_LIMIT: usize = 8;

    let mut services = state
        .services
        .values()
        .filter(|service| service.stack == stack && !service.deleted)
        .map(|service| service.id.as_str())
        .collect::<Vec<_>>();
    services.sort_unstable();
    if services.is_empty() {
        return " This Stack has no active Services".into();
    }

    let hidden = services.len().saturating_sub(DISPLAY_LIMIT);
    services.truncate(DISPLAY_LIMIT);
    let mut hint = format!(" Available Services: {}", services.join(", "));
    if hidden > 0 {
        hint.push_str(&format!(" (and {hidden} more)"));
    }
    hint
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
    let mut owners = state
        .gateway_routes
        .iter()
        .map(|(owner, stack)| (owner.as_str(), &stack.gateway))
        .collect::<BTreeMap<_, _>>();
    for (owner, stack) in &state.stacks {
        owners.entry(owner).or_insert(&stack.gateway);
    }
    for (owner, gateway) in owners.into_iter().filter(|(owner, _)| *owner != stack_name) {
        if let Some(hostname) = gateway
            .http_routes
            .iter()
            .flat_map(|route| route.hostnames.iter())
            .find(|hostname| requested.contains(hostname))
        {
            return Err(ControllerError::Conflict(format!(
                "gateway hostname {hostname:?} is already owned by stack {:?}",
                owner
            )));
        }
    }
    Ok(())
}

pub(super) fn validate_stack_name(name: &str) -> Result<(), ControllerError> {
    swarmlite_stack::validate_stack_name(name)
        .map_err(|error| ControllerError::Invalid(error.to_string()))
}
