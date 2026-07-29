use std::collections::BTreeSet;

use crate::model::{ClusterState, StackGatewaySpec};

use super::ControllerError;

pub(super) fn service_id(stack: &str, service: &str) -> String {
    format!("{stack}.{service}")
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
    if name.is_empty()
        || !name
            .chars()
            .all(|value| value.is_ascii_alphanumeric() || matches!(value, '-' | '_' | '.'))
    {
        Err(ControllerError::Invalid(
            "stack name may contain only letters, numbers, '.', '-' and '_'".to_owned(),
        ))
    } else {
        Ok(())
    }
}
