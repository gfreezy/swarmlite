use std::collections::{BTreeMap, BTreeSet};

use tracing::info;

use crate::{
    gateway,
    model::{
        ClusterState, DesiredTaskState, ObservedTaskState, RecoveryStatus, TaskRecord,
        service_spec_hash,
    },
};

pub(super) fn adopt_unclaimed_tasks(state: &mut ClusterState, stack_name: &str) {
    let services = state
        .services
        .values()
        .filter(|service| service.stack == stack_name && !service.deleted)
        .cloned()
        .collect::<Vec<_>>();
    let mut adopted = 0_usize;
    for service in services {
        let spec_hash = service_spec_hash(&service.spec);
        let routed_ports = gateway::service_ports(state, &service);
        let mut occupied_slots = state
            .tasks
            .values()
            .filter(|task| {
                task.service_id == service.id && task.desired != DesiredTaskState::Stopped
            })
            .map(|task| task.slot)
            .collect::<BTreeSet<_>>();
        let mut candidates = state
            .unclaimed_tasks
            .values()
            .filter(|task| {
                task.stack == stack_name
                    && task.service == service.name
                    && task.spec_hash == spec_hash
                    && task.slot < service.spec.replicas
                    && routed_ports.iter().all(|target| {
                        task.ports
                            .iter()
                            .any(|port| port.target == *target && port.protocol == "tcp")
                    })
            })
            .cloned()
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| {
            left.slot
                .cmp(&right.slot)
                .then_with(|| {
                    recovery_task_priority(&right.observed)
                        .cmp(&recovery_task_priority(&left.observed))
                })
                .then_with(|| right.revision.cmp(&left.revision))
                .then_with(|| left.id.cmp(&right.id))
        });
        for candidate in candidates {
            if occupied_slots.contains(&candidate.slot) || state.tasks.contains_key(&candidate.id) {
                continue;
            }
            occupied_slots.insert(candidate.slot);
            state.tasks.insert(
                candidate.id.clone(),
                TaskRecord {
                    id: candidate.id.clone(),
                    service_id: service.id.clone(),
                    revision: service.revision,
                    slot: candidate.slot,
                    node_id: candidate.node_id,
                    desired: DesiredTaskState::Running,
                    observed: candidate.observed,
                    ports: candidate.ports,
                    container_id: candidate.container_id,
                    drain_until_unix_ms: None,
                    applied_generation: None,
                    reconcile_error: None,
                },
            );
            state.unclaimed_tasks.remove(&candidate.id);
            adopted += 1;
        }
    }
    if adopted > 0 {
        info!(
            stack = stack_name,
            adopted, "adopted existing task containers"
        );
    }
}

pub(super) fn recovery_status(state: &ClusterState) -> RecoveryStatus {
    let mut slots = BTreeMap::new();
    for task in state.unclaimed_tasks.values() {
        *slots
            .entry((task.stack.clone(), task.service.clone(), task.slot))
            .or_insert(0_usize) += 1;
    }
    RecoveryStatus {
        awaiting_adoption: state.unclaimed_tasks.len(),
        conflicting_slots: slots.values().filter(|count| **count > 1).count(),
    }
}

fn recovery_task_priority(state: &ObservedTaskState) -> u8 {
    match state {
        ObservedTaskState::Healthy => 5,
        ObservedTaskState::Running => 4,
        ObservedTaskState::Starting => 3,
        ObservedTaskState::Pending => 2,
        ObservedTaskState::Failed => 1,
        ObservedTaskState::Lost => 0,
    }
}
