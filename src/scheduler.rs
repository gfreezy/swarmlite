use std::collections::{BTreeMap, BTreeSet, HashSet};

use uuid::Uuid;

use crate::model::{
    ClusterState, DesiredTaskState, NodeRecord, ObservedTaskState, PortBinding, ServicePort,
    ServiceRecord, ServiceSpec, TaskRecord,
};

pub fn reconcile(state: &mut ClusterState, live_nodes: &BTreeSet<String>) -> bool {
    let mut changed = false;

    for task in state.tasks.values_mut() {
        if task.desired == DesiredTaskState::Running
            && (!live_nodes.contains(&task.node_id)
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
            changed |= stop_all_service_tasks(state, &service_id);
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
            state.tasks.get_mut(&id).unwrap().desired = DesiredTaskState::Stopped;
            changed = true;
        }
    }

    let healthy_current = current_running
        .iter()
        .filter(|id| state.tasks[*id].observed == ObservedTaskState::Healthy)
        .count();
    let old_to_keep = (service.spec.replicas as usize).saturating_sub(healthy_current);
    while old_running.len() > old_to_keep {
        if let Some(id) = old_running.pop() {
            state.tasks.get_mut(&id).unwrap().desired = DesiredTaskState::Stopped;
            changed = true;
        }
    }

    // stop-first updates deliberately free one slot before starting its replacement.
    if service.spec.max_surge == 0
        && !old_running.is_empty()
        && current_running.len() < service.spec.replicas as usize
    {
        let id = old_running.remove(0);
        state.tasks.get_mut(&id).unwrap().desired = DesiredTaskState::Stopped;
        changed = true;
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

fn stop_all_service_tasks(state: &mut ClusterState, service_id: &str) -> bool {
    let mut changed = false;
    for task in state
        .tasks
        .values_mut()
        .filter(|task| task.service_id == service_id)
    {
        if task.desired != DesiredTaskState::Stopped {
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
    let ports = allocate_ports(state, node, &service.spec)?;
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
        container_id: None,
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
        .filter_map(|port| port.published)
        .all(|port| !used.contains(&port))
}

fn allocate_ports(
    state: &ClusterState,
    node: &NodeRecord,
    spec: &ServiceSpec,
) -> Option<Vec<PortBinding>> {
    let mut requested = spec.ports.clone();
    if let Some(target) = traefik_target_port(&spec.service_labels)
        && !requested.iter().any(|port| port.target == target)
    {
        requested.push(ServicePort {
            target,
            published: None,
            protocol: "tcp".to_owned(),
        });
    }

    let mut used = used_ports(state, &node.id);
    let mut result = Vec::with_capacity(requested.len());
    for port in requested {
        let published = if let Some(published) = port.published {
            if used.contains(&published) {
                return None;
            }
            published
        } else {
            (node.port_range_start..=node.port_range_end)
                .find(|candidate| !used.contains(candidate))?
        };
        used.insert(published);
        result.push(PortBinding {
            target: port.target,
            published,
            protocol: port.protocol,
        });
    }
    Some(result)
}

fn used_ports(state: &ClusterState, node_id: &str) -> BTreeSet<u16> {
    state
        .tasks
        .values()
        .filter(|task| {
            task.node_id == node_id
                && !matches!(
                    task.observed,
                    ObservedTaskState::Failed | ObservedTaskState::Lost
                )
        })
        .flat_map(|task| task.ports.iter().map(|port| port.published))
        .collect()
}

pub fn traefik_target_port(labels: &BTreeMap<String, String>) -> Option<u16> {
    labels.iter().find_map(|(key, value)| {
        if key.starts_with("traefik.http.services.") && key.ends_with(".loadbalancer.server.port") {
            value.parse().ok()
        } else {
            None
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn service(revision: u64, replicas: u32) -> ServiceRecord {
        ServiceRecord {
            id: "demo_web".into(),
            stack: "demo".into(),
            name: "web".into(),
            revision,
            deleted: false,
            spec: ServiceSpec {
                image: format!("example/web:v{revision}"),
                command: vec![],
                entrypoint: vec![],
                environment: vec![],
                ports: vec![ServicePort {
                    target: 80,
                    published: None,
                    protocol: "tcp".into(),
                }],
                volumes: vec![],
                container_labels: BTreeMap::new(),
                service_labels: BTreeMap::new(),
                healthcheck: None,
                replicas,
                constraints: vec![],
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
                    labels: BTreeMap::new(),
                    cpu_millis: 1000,
                    memory_bytes: 1024,
                    port_range_start: 20_000,
                    port_range_end: 20_010,
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
}
