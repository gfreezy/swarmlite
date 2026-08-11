use std::{
    collections::BTreeSet,
    time::{Duration, Instant},
};

use crate::model::ClusterState;

use super::{ControllerError, Inner};

pub(super) fn current_live_nodes(inner: &Inner, timeout_seconds: u64) -> BTreeSet<String> {
    let now = Instant::now();
    let timeout = Duration::from_secs(timeout_seconds);
    inner
        .live_nodes
        .iter()
        .filter(|(_, seen)| now.duration_since(**seen) <= timeout)
        .map(|(id, _)| id.clone())
        .collect()
}

pub(super) fn validate_node_label_key(key: &str) -> Result<(), ControllerError> {
    if key.is_empty()
        || key.len() > 256
        || key.trim() != key
        || key.contains('=')
        || key.chars().any(char::is_control)
    {
        return Err(ControllerError::Invalid(
            "node label key must contain 1 to 256 bytes without control characters, '=' or surrounding whitespace"
                .to_owned(),
        ));
    }
    Ok(())
}

pub(super) fn validate_node_label(key: &str, value: &str) -> Result<(), ControllerError> {
    validate_node_label_key(key)?;
    if value.len() > 4_096 || value.chars().any(char::is_control) {
        return Err(ControllerError::Invalid(
            "node label value must contain at most 4096 bytes without control characters"
                .to_owned(),
        ));
    }
    Ok(())
}

pub(super) fn gateway_endpoints(state: &ClusterState, admin_port: u16) -> Vec<String> {
    state
        .nodes
        .values()
        .filter(|node| node.gateway_enabled)
        .map(|node| format!("http://{}:{admin_port}", format_host(&node.address)))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn format_host(host: &str) -> String {
    if host.contains(':') && !(host.starts_with('[') && host.ends_with(']')) {
        format!("[{host}]")
    } else {
        host.to_owned()
    }
}
