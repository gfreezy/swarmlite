use std::collections::{BTreeSet, HashSet};

use uuid::Uuid;

use crate::{
    gateway,
    model::{
        ClusterState, DesiredTaskState, NodeRecord, ObservedTaskState, PortBinding, ServicePort,
        ServiceRecord, ServiceSpec, TaskRecord, service_config_digests,
    },
};

pub fn reconcile(state: &mut ClusterState, live_nodes: &BTreeSet<String>) -> bool {
    let mut changed = false;

    for task in state.tasks.values_mut() {
        if matches!(
            task.desired,
            DesiredTaskState::Running | DesiredTaskState::Draining
        ) && (!live_nodes.contains(&task.node_id)
            || matches!(
                task.observed,
                ObservedTaskState::Failed | ObservedTaskState::Lost
            ))
        {
            task.desired = DesiredTaskState::Stopped;
            if !live_nodes.contains(&task.node_id) {
                task.observed = ObservedTaskState::Lost;
            }
            changed = true;
        }
    }

    let service_ids: Vec<String> = state.services.keys().cloned().collect();
    for service_id in service_ids {
        let service = state.services[&service_id].clone();
        if service.deleted || service.spec.replicas == 0 {
            changed |= stop_all_service_tasks(state, &service);
            continue;
        }
        changed |= reconcile_service(state, &service, live_nodes);
    }
    changed
}

fn reconcile_service(
    state: &mut ClusterState,
    service: &ServiceRecord,
    live_nodes: &BTreeSet<String>,
) -> bool {
    let mut changed = false;
    let task_ids: Vec<String> = state
        .tasks
        .values()
        .filter(|task| task.service_id == service.id)
        .map(|task| task.id.clone())
        .collect();

    // Constraints are hard requirements. A task from the active revision must
    // leave a node as soon as a current heartbeat shows that the node no longer
    // matches (for example after a node-label update). Missing soft node state
    // is not a mismatch: after a controller restart it is empty until the next
    // heartbeat. Retire before counting rollout capacity so a stopped or
    // draining container occupies its slot until the agent acknowledges its
    // removal.
    for id in &task_ids {
        let task = &state.tasks[id];
        if task.revision != service.revision
            || !matches!(
                task.desired,
                DesiredTaskState::Running | DesiredTaskState::Draining
            )
        {
            continue;
        }
        let node_violates_constraints = state
            .nodes
            .get(&task.node_id)
            .is_some_and(|node| !matches_constraints(node, &service.spec.constraints));
        if node_violates_constraints {
            changed |= retire_task(state, id, service);
        }
    }

    let mut current_running = task_ids
        .iter()
        .filter(|id| {
            let task = &state.tasks[*id];
            task.revision == service.revision
                && task.desired == DesiredTaskState::Running
                && live_nodes.contains(&task.node_id)
        })
        .cloned()
        .collect::<Vec<_>>();
    let mut old_running = task_ids
        .iter()
        .filter(|id| {
            let task = &state.tasks[*id];
            task.revision != service.revision
                && task.desired == DesiredTaskState::Running
                && live_nodes.contains(&task.node_id)
        })
        .cloned()
        .collect::<Vec<_>>();

    current_running.sort();
    old_running.sort();

    // Scale down surplus current-revision tasks first.
    while current_running.len() > service.spec.replicas as usize {
        if let Some(id) = current_running.pop() {
            changed |= retire_task(state, &id, service);
        }
    }

    let healthy_current = current_running
        .iter()
        .filter(|id| {
            let task = &state.tasks[*id];
            task.observed == ObservedTaskState::Healthy
                && task.ports.iter().all(|port| port.published.is_some())
        })
        .count();
    let old_to_keep = (service.spec.replicas as usize).saturating_sub(healthy_current);
    while old_running.len() > old_to_keep {
        if let Some(id) = old_running.pop() {
            changed |= retire_task(state, &id, service);
        }
    }

    // stop-first updates deliberately free one slot before starting its replacement.
    if service.spec.max_surge == 0
        && !old_running.is_empty()
        && current_running.len() < service.spec.replicas as usize
    {
        let id = old_running.remove(0);
        changed |= retire_task(state, &id, service);
    }

    let occupying = state
        .tasks
        .values()
        .filter(|task| {
            task.service_id == service.id
                && live_nodes.contains(&task.node_id)
                && !matches!(
                    task.observed,
                    ObservedTaskState::Failed | ObservedTaskState::Lost
                )
        })
        .count();
    let current_desired = state
        .tasks
        .values()
        .filter(|task| {
            task.service_id == service.id
                && task.revision == service.revision
                && task.desired == DesiredTaskState::Running
                && live_nodes.contains(&task.node_id)
        })
        .count();
    let has_old = state.tasks.values().any(|task| {
        task.service_id == service.id
            && task.revision != service.revision
            && task.desired == DesiredTaskState::Running
            && live_nodes.contains(&task.node_id)
    });
    let max_total = if has_old {
        service.spec.replicas as usize + service.spec.max_surge as usize
    } else {
        service.spec.replicas as usize
    };
    let can_create = max_total.saturating_sub(occupying);
    let needed = (service.spec.replicas as usize).saturating_sub(current_desired);

    for _ in 0..needed.min(can_create) {
        if let Some(task) = schedule_task(state, service, live_nodes) {
            state.tasks.insert(task.id.clone(), task);
            changed = true;
        } else {
            break;
        }
    }

    changed
}

fn stop_all_service_tasks(state: &mut ClusterState, service: &ServiceRecord) -> bool {
    let mut changed = false;
    let ids = state
        .tasks
        .values()
        .filter(|task| task.service_id == service.id)
        .map(|task| task.id.clone())
        .collect::<Vec<_>>();
    for id in ids {
        changed |= retire_task(state, &id, service);
    }
    changed
}

fn retire_task(state: &mut ClusterState, task_id: &str, service: &ServiceRecord) -> bool {
    let routed = gateway::is_service_routed(state, service);
    let task = state.tasks.get_mut(task_id).unwrap();
    if task.desired != DesiredTaskState::Running {
        return false;
    }
    task.desired = if routed && task.observed == ObservedTaskState::Healthy {
        DesiredTaskState::Draining
    } else {
        DesiredTaskState::Stopped
    };
    task.drain_until_unix_ms = None;
    true
}

pub fn finish_drains(state: &mut ClusterState, now_unix_ms: i64) -> bool {
    let mut changed = false;
    for task in state.tasks.values_mut() {
        if task.desired == DesiredTaskState::Draining
            && task
                .drain_until_unix_ms
                .is_some_and(|deadline| deadline <= now_unix_ms)
        {
            task.desired = DesiredTaskState::Stopped;
            changed = true;
        }
    }
    changed
}

fn schedule_task(
    state: &ClusterState,
    service: &ServiceRecord,
    live_nodes: &BTreeSet<String>,
) -> Option<TaskRecord> {
    let mut candidates = state
        .nodes
        .values()
        .filter(|node| live_nodes.contains(&node.id))
        .filter(|node| matches_constraints(node, &service.spec.constraints))
        .filter(|node| node_has_replica_capacity(state, node, service))
        .filter(|node| explicit_ports_available(state, node, &service.spec))
        .collect::<Vec<_>>();
    candidates.sort_by_key(|node| {
        let same_service = state
            .tasks
            .values()
            .filter(|task| {
                task.node_id == node.id
                    && task.service_id == service.id
                    && task.desired == DesiredTaskState::Running
            })
            .count();
        let total = state
            .tasks
            .values()
            .filter(|task| task.node_id == node.id && task.desired == DesiredTaskState::Running)
            .count();
        (same_service, total, node.id.as_str())
    });
    let node = candidates.first()?;
    let ports = allocate_ports(state, node, service)?;
    let used_slots: HashSet<u32> = state
        .tasks
        .values()
        .filter(|task| {
            task.service_id == service.id
                && task.revision == service.revision
                && task.desired == DesiredTaskState::Running
        })
        .map(|task| task.slot)
        .collect();
    let slot = (0..service.spec.replicas).find(|slot| !used_slots.contains(slot))?;
    Some(TaskRecord {
        id: Uuid::new_v4().to_string(),
        service_id: service.id.clone(),
        revision: service.revision,
        slot,
        node_id: node.id.clone(),
        desired: DesiredTaskState::Running,
        observed: ObservedTaskState::Pending,
        ports,
        config_digests: service_config_digests(&service.spec),
        container_id: None,
        drain_until_unix_ms: None,
        applied_generation: None,
        reconcile_error: None,
    })
}

fn node_has_replica_capacity(
    state: &ClusterState,
    node: &NodeRecord,
    service: &ServiceRecord,
) -> bool {
    service.spec.max_replicas_per_node.is_none_or(|limit| {
        if limit == 0 {
            return true;
        }

        let active = state
            .tasks
            .values()
            .filter(|task| {
                task.node_id == node.id
                    && task.service_id == service.id
                    && !matches!(
                        task.observed,
                        ObservedTaskState::Failed | ObservedTaskState::Lost
                    )
            })
            .count();
        if active < limit as usize {
            return true;
        }

        // A start-first replacement may temporarily share a node with the old task it
        // replaces. Each still-running old task grants at most one such surge slot;
        // ordinary scaling and stop-first updates continue to enforce the hard limit.
        let old_running = state
            .tasks
            .values()
            .filter(|task| {
                task.node_id == node.id
                    && task.service_id == service.id
                    && task.revision != service.revision
                    && task.desired == DesiredTaskState::Running
                    && !matches!(
                        task.observed,
                        ObservedTaskState::Failed | ObservedTaskState::Lost
                    )
            })
            .count();
        service.spec.max_surge > 0 && active < (limit as usize).saturating_add(old_running)
    })
}

fn matches_constraints(node: &NodeRecord, constraints: &[String]) -> bool {
    constraints.iter().all(|constraint| {
        let (left, operator, expected) = if let Some((left, right)) = constraint.split_once("!=") {
            (left.trim(), "!=", right.trim())
        } else if let Some((left, right)) = constraint.split_once("==") {
            (left.trim(), "==", right.trim())
        } else {
            return false;
        };
        let actual = if left == "node.id" || left == "node.hostname" {
            Some(node.id.as_str())
        } else if let Some(label) = left.strip_prefix("node.labels.") {
            node.labels.get(label).map(String::as_str)
        } else {
            None
        };
        match operator {
            "==" => actual == Some(expected),
            "!=" => actual != Some(expected),
            _ => false,
        }
    })
}

fn explicit_ports_available(state: &ClusterState, node: &NodeRecord, spec: &ServiceSpec) -> bool {
    let used = used_ports(state, &node.id);
    spec.ports
        .iter()
        .filter_map(|port| {
            port.published
                .map(|published| (published, port.protocol.as_str()))
        })
        .all(|(published, protocol)| {
            used.get(protocol)
                .is_none_or(|ports| !ports.contains(&published))
        })
}

fn allocate_ports(
    state: &ClusterState,
    _node: &NodeRecord,
    service: &ServiceRecord,
) -> Option<Vec<PortBinding>> {
    let mut requested = service.spec.ports.clone();
    for target in gateway::service_ports(state, service) {
        if !requested
            .iter()
            .any(|port| port.target == target && port.protocol == "tcp")
        {
            requested.push(ServicePort {
                target,
                published: None,
                protocol: "tcp".to_owned(),
            });
        }
    }

    let mut result = Vec::with_capacity(requested.len());
    for port in requested {
        result.push(PortBinding {
            target: port.target,
            published: port.published,
            protocol: port.protocol,
        });
    }
    Some(result)
}

fn used_ports(
    state: &ClusterState,
    node_id: &str,
) -> std::collections::BTreeMap<String, BTreeSet<u16>> {
    let mut used = std::collections::BTreeMap::<String, BTreeSet<u16>>::new();
    let assigned_ports = state
        .tasks
        .values()
        .filter(|task| {
            task.node_id == node_id
                && !matches!(
                    task.observed,
                    ObservedTaskState::Failed | ObservedTaskState::Lost
                )
        })
        .flat_map(|task| &task.ports);
    // Recovery deliberately leaves unmatched containers running and unclaimed.
    // Their bindings still belong to Docker even though they have no TaskRecord,
    // so keep those ports reserved until the containers disappear or are adopted.
    let unclaimed_ports = state
        .unclaimed_tasks
        .values()
        .filter(|task| task.node_id == node_id)
        .flat_map(|task| &task.ports);
    for port in assigned_ports.chain(unclaimed_ports) {
        if let Some(published) = port.published {
            used.entry(port.protocol.clone())
                .or_default()
                .insert(published);
        }
    }
    used
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::model::{
        HttpBackend, HttpBackendProtocol, HttpRouteRule, HttpRouteSpec, StackGatewaySpec,
        StackRecord,
    };

    use super::*;

    fn service(revision: u64, replicas: u32) -> ServiceRecord {
        ServiceRecord {
            id: "demo.web".into(),
            stack: "demo".into(),
            name: "web".into(),
            revision,
            deleted: false,
            spec: ServiceSpec {
                image: format!("example/web:v{revision}"),
                pull_policy: Default::default(),
                command: vec![],
                entrypoint: vec![],
                environment: vec![],
                expose: vec![],
                ports: vec![ServicePort {
                    target: 80,
                    published: None,
                    protocol: "tcp".into(),
                }],
                volumes: vec![],
                configs: vec![],
                container_labels: BTreeMap::new(),
                service_labels: BTreeMap::new(),
                healthcheck: None,
                replicas,
                constraints: vec![],
                max_replicas_per_node: None,
                max_surge: 1,
                stop_grace_period_seconds: 10,
            },
        }
    }

    fn state_with_nodes() -> (ClusterState, BTreeSet<String>) {
        let mut state = ClusterState::default();
        for id in ["node-a", "node-b"] {
            state.nodes.insert(
                id.into(),
                NodeRecord {
                    id: id.into(),
                    address: "127.0.0.1".into(),
                    swarmlite_version: None,
                    labels: BTreeMap::new(),
                    cpu_millis: 1000,
                    memory_bytes: 1024,
                    port_range_start: 20_000,
                    port_range_end: 20_010,
                    gateway_enabled: false,
                },
            );
        }
        let live = ["node-a".to_owned(), "node-b".to_owned()]
            .into_iter()
            .collect();
        (state, live)
    }

    #[test]
    fn schedules_and_spreads_initial_replicas() {
        let (mut state, live) = state_with_nodes();
        let service = service(1, 3);
        state.services.insert(service.id.clone(), service);
        assert!(reconcile(&mut state, &live));
        assert_eq!(state.tasks.len(), 3);
        let by_node = state.tasks.values().fold(BTreeMap::new(), |mut map, task| {
            *map.entry(task.node_id.clone()).or_insert(0) += 1;
            map
        });
        assert_eq!(by_node["node-a"], 2);
        assert_eq!(by_node["node-b"], 1);
    }

    #[test]
    fn max_replicas_per_node_leaves_excess_replicas_unscheduled() {
        let (mut state, live) = state_with_nodes();
        let mut limited = service(1, 3);
        limited.spec.max_replicas_per_node = Some(1);
        state.services.insert(limited.id.clone(), limited);

        assert!(reconcile(&mut state, &live));
        assert_eq!(state.tasks.len(), 2);
        assert_eq!(
            state
                .tasks
                .values()
                .map(|task| task.node_id.as_str())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["node-a", "node-b"])
        );
        assert!(!reconcile(&mut state, &live));
        assert_eq!(state.tasks.len(), 2);
    }

    #[test]
    fn start_first_update_temporarily_exceeds_max_replicas_per_node() {
        let (mut state, mut live) = state_with_nodes();
        state.nodes.remove("node-b");
        live.remove("node-b");
        let mut original = service(1, 1);
        original.spec.max_replicas_per_node = Some(1);
        state.services.insert(original.id.clone(), original.clone());
        assert!(reconcile(&mut state, &live));
        state.tasks.values_mut().for_each(|task| {
            task.observed = ObservedTaskState::Healthy;
        });

        let mut updated = original;
        updated.revision = 2;
        updated.spec.image = "example/web:v2".into();
        state.services.insert(updated.id.clone(), updated);

        assert!(reconcile(&mut state, &live));
        assert_eq!(state.tasks.len(), 2);
        assert!(state.tasks.values().all(|task| task.node_id == "node-a"));
        assert_eq!(
            state
                .tasks
                .values()
                .map(|task| task.revision)
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([1, 2])
        );

        let replacement = state
            .tasks
            .values_mut()
            .find(|task| task.revision == 2)
            .unwrap();
        replacement.observed = ObservedTaskState::Healthy;
        replacement.ports[0].published = Some(20_000);
        assert!(reconcile(&mut state, &live));
        assert_eq!(
            state
                .tasks
                .values()
                .find(|task| task.revision == 1)
                .unwrap()
                .desired,
            DesiredTaskState::Stopped
        );

        state
            .tasks
            .retain(|_, task| task.desired != DesiredTaskState::Stopped);
        assert!(!reconcile(&mut state, &live));
        assert_eq!(state.tasks.len(), 1);
        assert_eq!(state.tasks.values().next().unwrap().revision, 2);
    }

    #[test]
    fn start_first_rollout_adds_one_surge_task() {
        let (mut state, live) = state_with_nodes();
        let original = service(1, 2);
        state.services.insert(original.id.clone(), original.clone());
        reconcile(&mut state, &live);
        for task in state.tasks.values_mut() {
            task.observed = ObservedTaskState::Healthy;
        }
        state.services.insert(original.id.clone(), service(2, 2));
        assert!(reconcile(&mut state, &live));
        assert_eq!(
            state
                .tasks
                .values()
                .filter(|task| task.desired == DesiredTaskState::Running)
                .count(),
            3
        );
        assert_eq!(
            state
                .tasks
                .values()
                .filter(|task| task.revision == 2)
                .count(),
            1
        );
    }

    #[test]
    fn honors_label_constraint() {
        let (mut state, live) = state_with_nodes();
        state
            .nodes
            .get_mut("node-b")
            .unwrap()
            .labels
            .insert("role".into(), "database".into());
        let mut constrained = service(1, 1);
        constrained.spec.constraints = vec!["node.labels.role==database".into()];
        state.services.insert(constrained.id.clone(), constrained);
        reconcile(&mut state, &live);
        assert_eq!(state.tasks.values().next().unwrap().node_id, "node-b");
    }

    #[test]
    fn label_change_retires_noncompliant_task_and_replaces_it_on_matching_node() {
        let (mut state, live) = state_with_nodes();
        state
            .nodes
            .get_mut("node-a")
            .unwrap()
            .labels
            .insert("disk".into(), "ssd".into());
        state
            .nodes
            .get_mut("node-b")
            .unwrap()
            .labels
            .insert("disk".into(), "hdd".into());
        let mut constrained = service(1, 1);
        constrained.spec.constraints = vec!["node.labels.disk==ssd".into()];
        state.services.insert(constrained.id.clone(), constrained);
        reconcile(&mut state, &live);
        assert_eq!(state.tasks.values().next().unwrap().node_id, "node-a");

        state
            .nodes
            .get_mut("node-a")
            .unwrap()
            .labels
            .insert("disk".into(), "hdd".into());
        state
            .nodes
            .get_mut("node-b")
            .unwrap()
            .labels
            .insert("disk".into(), "ssd".into());

        assert!(reconcile(&mut state, &live));
        assert_eq!(
            state.tasks.values().next().unwrap().desired,
            DesiredTaskState::Stopped
        );
        assert_eq!(state.tasks.len(), 1, "wait for the stop acknowledgement");

        state
            .tasks
            .retain(|_, task| task.desired != DesiredTaskState::Stopped);
        assert!(reconcile(&mut state, &live));
        let replacement = state.tasks.values().next().unwrap();
        assert_eq!(replacement.node_id, "node-b");
        assert_eq!(replacement.desired, DesiredTaskState::Running);
    }

    #[test]
    fn label_change_without_matching_node_does_not_schedule_replacement() {
        let (mut state, live) = state_with_nodes();
        state
            .nodes
            .get_mut("node-a")
            .unwrap()
            .labels
            .insert("disk".into(), "ssd".into());
        state
            .nodes
            .get_mut("node-b")
            .unwrap()
            .labels
            .insert("disk".into(), "hdd".into());
        let mut constrained = service(1, 1);
        constrained.spec.constraints = vec!["node.labels.disk==ssd".into()];
        state.services.insert(constrained.id.clone(), constrained);
        reconcile(&mut state, &live);

        state
            .nodes
            .get_mut("node-a")
            .unwrap()
            .labels
            .insert("disk".into(), "hdd".into());
        assert!(reconcile(&mut state, &live));
        assert!(
            state
                .tasks
                .values()
                .all(|task| task.desired == DesiredTaskState::Stopped)
        );

        state.tasks.clear();
        assert!(!reconcile(&mut state, &live));
        assert!(state.tasks.is_empty());
    }

    #[test]
    fn missing_soft_node_state_after_restart_does_not_violate_constraints() {
        let (mut state, live) = state_with_nodes();
        state
            .nodes
            .get_mut("node-a")
            .unwrap()
            .labels
            .insert("disk".into(), "ssd".into());
        let mut constrained = service(1, 1);
        constrained.spec.constraints = vec!["node.labels.disk==ssd".into()];
        state.services.insert(constrained.id.clone(), constrained);
        reconcile(&mut state, &live);
        let task_id = state.tasks.keys().next().unwrap().clone();
        let node_id = state.tasks[&task_id].node_id.clone();

        state.nodes.remove(&node_id);
        assert!(!reconcile(&mut state, &live));
        assert_eq!(state.tasks[&task_id].desired, DesiredTaskState::Running);
    }

    #[test]
    fn gateway_rollout_drains_old_task_until_caddy_acknowledges_it() {
        let (mut state, live) = state_with_nodes();
        route_service(&mut state, "web", 80);
        let original = service(1, 1);
        state.services.insert(original.id.clone(), original.clone());
        reconcile(&mut state, &live);
        state.tasks.values_mut().for_each(|task| {
            task.observed = ObservedTaskState::Healthy;
        });

        let mut updated = original;
        updated.revision = 2;
        updated.spec.image = "example/web:v2".into();
        state.services.insert(updated.id.clone(), updated);
        reconcile(&mut state, &live);
        let new_id = state
            .tasks
            .values()
            .find(|task| task.revision == 2)
            .unwrap()
            .id
            .clone();
        let new_task = state.tasks.get_mut(&new_id).unwrap();
        new_task.observed = ObservedTaskState::Healthy;
        reconcile(&mut state, &live);
        assert_eq!(
            state
                .tasks
                .values()
                .find(|task| task.revision == 1)
                .unwrap()
                .desired,
            DesiredTaskState::Running,
            "keep the old task until Docker reports the replacement port"
        );
        let new_task = state.tasks.get_mut(&new_id).unwrap();
        new_task.ports[0].published = Some(20_005);
        reconcile(&mut state, &live);

        let old = state
            .tasks
            .values()
            .find(|task| task.revision == 1)
            .unwrap();
        assert_eq!(old.desired, DesiredTaskState::Draining);
        assert_eq!(old.drain_until_unix_ms, None);
    }

    #[test]
    fn gateway_backend_leaves_host_port_allocation_to_docker() {
        let (mut state, live) = state_with_nodes();
        let mut service = service(1, 1);
        service.spec.ports.clear();
        route_service(&mut state, "web", 8080);
        state.services.insert(service.id.clone(), service);

        reconcile(&mut state, &live);

        let binding = &state.tasks.values().next().unwrap().ports[0];
        assert_eq!(binding.target, 8080);
        assert_eq!(binding.published, None);
    }

    #[test]
    fn gateway_backend_uses_docker_allocation_with_unclaimed_ports() {
        let (mut state, mut live) = state_with_nodes();
        state.nodes.remove("node-b");
        live.remove("node-b");
        state.unclaimed_tasks.insert(
            "old-task".into(),
            crate::model::UnclaimedTask {
                id: "old-task".into(),
                stack: "demo".into(),
                service: "old-web".into(),
                slot: 0,
                revision: 1,
                spec_hash: "old-hash".into(),
                node_id: "node-a".into(),
                observed: ObservedTaskState::Failed,
                ports: vec![PortBinding {
                    target: 3000,
                    published: Some(20_000),
                    protocol: "tcp".into(),
                }],
                config_digests: Vec::new(),
                container_id: Some("old-container".into()),
            },
        );
        let mut service = service(1, 1);
        service.spec.ports.clear();
        route_service(&mut state, "web", 8080);
        state.services.insert(service.id.clone(), service);

        reconcile(&mut state, &live);

        let binding = &state.tasks.values().next().unwrap().ports[0];
        assert_eq!(binding.published, None);
    }

    #[test]
    fn allows_tcp_and_udp_to_share_a_published_port() {
        let (mut state, mut live) = state_with_nodes();
        state.nodes.remove("node-b");
        live.remove("node-b");

        let mut tcp = service(1, 1);
        tcp.id = "demo.tcp".into();
        tcp.name = "tcp".into();
        tcp.spec.ports[0] = ServicePort {
            target: 8080,
            published: Some(20_000),
            protocol: "tcp".into(),
        };
        let mut udp = service(1, 1);
        udp.id = "demo.udp".into();
        udp.name = "udp".into();
        udp.spec.ports[0] = ServicePort {
            target: 8080,
            published: Some(20_000),
            protocol: "udp".into(),
        };
        state.services.insert(tcp.id.clone(), tcp);
        state.services.insert(udp.id.clone(), udp);

        assert!(reconcile(&mut state, &live));
        assert_eq!(state.tasks.len(), 2);
        assert!(
            state
                .tasks
                .values()
                .all(|task| { task.ports.len() == 1 && task.ports[0].published == Some(20_000) })
        );
        assert_eq!(
            state
                .tasks
                .values()
                .map(|task| task.ports[0].protocol.as_str())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["tcp", "udp"])
        );
    }

    #[test]
    fn rejects_duplicate_published_port_for_the_same_protocol() {
        let (mut state, mut live) = state_with_nodes();
        state.nodes.remove("node-b");
        live.remove("node-b");

        let mut first = service(1, 1);
        first.id = "demo.first".into();
        first.name = "first".into();
        first.spec.ports[0].published = Some(20_000);
        let mut second = service(1, 1);
        second.id = "demo.second".into();
        second.name = "second".into();
        second.spec.ports[0].published = Some(20_000);
        state.services.insert(first.id.clone(), first);
        state.services.insert(second.id.clone(), second);

        assert!(reconcile(&mut state, &live));
        assert_eq!(state.tasks.len(), 1);
        assert_eq!(
            state.tasks.values().next().unwrap().ports[0].protocol,
            "tcp"
        );
    }

    fn route_service(state: &mut ClusterState, service: &str, port: u16) {
        state.stacks.insert(
            "demo".into(),
            StackRecord {
                name: "demo".into(),
                applied_at_unix_ms: 1,
                services: vec![format!("demo.{service}")],
                gateway: StackGatewaySpec {
                    http_routes: vec![HttpRouteSpec {
                        hostnames: vec!["example.com".into()],
                        canonical_hostname: None,
                        tls: None,
                        http: None,
                        trusted_proxies: None,
                        rules: vec![HttpRouteRule {
                            matches: Vec::new(),
                            rewrite: None,
                            cache: None,
                            backend: HttpBackend {
                                service: Some(service.into()),
                                host: None,
                                port,
                                protocol: HttpBackendProtocol::Http,
                                preserve_host: true,
                            },
                        }],
                    }],
                    ..Default::default()
                },
                deployment: None,
                deployment_history: BTreeMap::new(),
            },
        );
    }

    #[test]
    fn finishes_only_acknowledged_expired_drains() {
        let (mut state, live) = state_with_nodes();
        let service = service(1, 1);
        state.services.insert(service.id.clone(), service);
        reconcile(&mut state, &live);
        let task = state.tasks.values_mut().next().unwrap();
        task.desired = DesiredTaskState::Draining;
        task.drain_until_unix_ms = Some(100);
        assert!(!finish_drains(&mut state, 99));
        assert!(finish_drains(&mut state, 100));
        assert_eq!(
            state.tasks.values().next().unwrap().desired,
            DesiredTaskState::Stopped
        );
    }
}
