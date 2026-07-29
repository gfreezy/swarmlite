use std::{
    collections::BTreeSet,
    time::{Duration, Instant},
};

use crate::model::{ClusterMode, ClusterState, ControllerRecord, NodeRole, NodeRoles, agent_roles};

use super::{CONTROLLER_TIMEOUT_MS, ControllerError, GatewaySyncState, Inner};

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

pub(super) fn pending_controller_set_acknowledgements(
    inner: &Inner,
    timeout_seconds: u64,
    controller_set_generation: u64,
) -> BTreeSet<String> {
    let now = Instant::now();
    let timeout = Duration::from_secs(timeout_seconds);
    let mut candidates = current_live_nodes(inner, timeout_seconds);
    candidates.extend(
        inner
            .controller_ack_candidates
            .iter()
            .filter(|(_, seen)| now.duration_since(**seen) <= timeout)
            .map(|(id, _)| id.clone()),
    );
    candidates
        .into_iter()
        .filter(|node_id| {
            inner
                .state
                .nodes
                .get(node_id)
                .is_none_or(|node| node.controller_set_generation < controller_set_generation)
        })
        .collect()
}

pub(super) fn pending_gateway_controller_set_acknowledgements(
    inner: &Inner,
    sync: &GatewaySyncState,
    timeout_seconds: u64,
    admin_port: u16,
    controller_set_generation: u64,
) -> BTreeSet<String> {
    current_live_nodes(inner, timeout_seconds)
        .into_iter()
        .filter_map(|node_id| inner.state.nodes.get(&node_id))
        .filter(|node| node.roles.contains(&NodeRole::Gateway))
        .map(|node| format!("http://{}:{admin_port}", format_host(&node.address)))
        .filter(|endpoint| {
            sync.applied_controller_set_generations
                .get(endpoint)
                .is_none_or(|generation| *generation < controller_set_generation)
        })
        .collect()
}

pub(super) fn normalized_roles(mut roles: NodeRoles) -> NodeRoles {
    roles.insert(NodeRole::Agent);
    roles
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

pub(super) fn role_count(state: &ClusterState, role: NodeRole, except_node: Option<&str>) -> usize {
    state
        .members
        .values()
        .filter(|member| Some(member.id.as_str()) != except_node)
        .filter(|member| member.roles.contains(&role))
        .count()
}

pub(super) fn validate_role_limits(
    state: &ClusterState,
    node_id: &str,
    roles: &NodeRoles,
    mode: ClusterMode,
) -> Result<(), ControllerError> {
    if !roles.contains(&NodeRole::Agent) {
        return Err(ControllerError::Invalid(
            "every node must have the agent role".to_owned(),
        ));
    }
    let controller_count = role_count(state, NodeRole::Controller, Some(node_id))
        + usize::from(roles.contains(&NodeRole::Controller));
    if controller_count > mode.controller_limit() {
        return Err(ControllerError::Conflict(format!(
            "{mode:?} allows at most {} controller role(s)",
            mode.controller_limit()
        )));
    }
    Ok(())
}

pub(super) fn automatic_join_roles(state: &ClusterState, mode: ClusterMode) -> NodeRoles {
    let mut roles = agent_roles();
    if mode == ClusterMode::Ha
        && role_count(state, NodeRole::Controller, None) < mode.controller_limit()
    {
        roles.insert(NodeRole::Controller);
    }
    roles
}

pub(super) fn fill_automatic_ha_controllers(state: &mut ClusterState) {
    let mut controller_count = role_count(state, NodeRole::Controller, None);
    let mut candidates = state
        .members
        .values()
        .filter(|member| member.automatic_roles && member.roles.contains(&NodeRole::Agent))
        .map(|member| (member.joined_at_unix_ms, member.id.clone()))
        .collect::<Vec<_>>();
    candidates.sort();
    for (_, node_id) in candidates {
        let member = state
            .members
            .get_mut(&node_id)
            .expect("role candidate must still exist");
        if controller_count < ClusterMode::Ha.controller_limit()
            && !member.roles.contains(&NodeRole::Controller)
        {
            member.roles.insert(NodeRole::Controller);
            controller_count += 1;
        }
        if controller_count == ClusterMode::Ha.controller_limit() {
            break;
        }
    }
}

pub(super) fn ensure_controller_record(
    state: &mut ClusterState,
    node_id: &str,
    now_unix_ms: i64,
) -> Result<bool, ControllerError> {
    let member = state
        .members
        .get(node_id)
        .ok_or_else(|| ControllerError::NotFound(format!("node {node_id} is not joined")))?;
    if member.controller_url.trim().is_empty() || member.raft_id == 0 || member.raft_url.is_empty()
    {
        return Err(ControllerError::Invalid(format!(
            "node {node_id} has an invalid controller identity"
        )));
    }
    let changed = state.controllers.get(node_id).is_none_or(|record| {
        record.advertise_url != member.controller_url
            || record.raft_id != member.raft_id
            || record.raft_url != member.raft_url
    });
    if changed {
        state.controllers.insert(
            node_id.to_owned(),
            ControllerRecord {
                node_id: node_id.to_owned(),
                advertise_url: member.controller_url.trim_end_matches('/').to_owned(),
                raft_id: member.raft_id,
                raft_url: member.raft_url.clone(),
                reserved_at_unix_ms: now_unix_ms,
            },
        );
    }
    Ok(changed)
}

pub(super) fn gateway_endpoints(state: &ClusterState, admin_port: u16) -> Vec<String> {
    state
        .nodes
        .values()
        .filter(|node| node.roles.contains(&NodeRole::Gateway))
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

pub(super) fn prune_controllers(
    state: &mut ClusterState,
    now_unix_ms: i64,
    voters: &BTreeSet<u64>,
) -> bool {
    let previous_len = state.controllers.len();
    state.controllers.retain(|_, record| {
        voters.contains(&record.raft_id)
            || now_unix_ms.saturating_sub(record.reserved_at_unix_ms) <= CONTROLLER_TIMEOUT_MS
    });
    state.controllers.len() != previous_len
}

pub(super) fn controller_urls(
    state: &ClusterState,
    fallback: Option<&str>,
    voters: &BTreeSet<u64>,
) -> Vec<String> {
    let mut urls = state
        .controllers
        .values()
        .filter(|record| voters.contains(&record.raft_id))
        .map(|record| record.advertise_url.trim_end_matches('/').to_owned())
        .collect::<BTreeSet<_>>();
    if urls.is_empty()
        && let Some(fallback) = fallback
    {
        urls.insert(fallback.trim_end_matches('/').to_owned());
    }
    urls.into_iter().collect()
}
