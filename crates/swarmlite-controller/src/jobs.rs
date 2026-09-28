//! Scheduled jobs have durable trigger cursors and never use replica replacement.
use crate::model::{ClusterState, DesiredTaskState, JobCursor, JobExecution, ObservedTaskState};
use std::collections::BTreeSet;

pub(crate) fn skip_missed(state: &mut ClusterState, now: i64) -> bool {
    let mut changed = false;
    for service in state.services.values_mut() {
        if let (Some(job), Some(cursor)) = (&service.spec.job, &mut service.job_cursor)
            && cursor.next_at_unix_ms <= now
            && let Ok(next) = job.next_after(now)
        {
            cursor.next_at_unix_ms = next;
            changed = true;
        }
    }
    changed
}

pub(crate) fn reconcile(state: &mut ClusterState, live: &BTreeSet<String>, now: i64) -> bool {
    let mut changed = false;
    // Never reschedule a lost execution. Stopped assignments remain tombstones until
    // their node acknowledges removal, including across controller restarts.
    for task in state.tasks.values_mut().filter(|t| t.job.is_some()) {
        let job = task.job.as_ref().unwrap();
        let deleted = state
            .services
            .get(&task.service_id)
            .is_none_or(|s| s.deleted || s.spec.job.is_none());
        if (deleted
            || (now >= job.start_deadline_unix_ms
                && matches!(
                    task.observed,
                    ObservedTaskState::Pending | ObservedTaskState::Unknown
                )))
            && task.desired != DesiredTaskState::Stopped
        {
            task.desired = DesiredTaskState::Stopped;
            changed = true;
        }
        if !live.contains(&task.node_id)
            && !task.observed.is_job_terminal()
            && task.observed != ObservedTaskState::Unknown
        {
            task.observed = ObservedTaskState::Unknown;
            changed = true;
        }
    }
    let services = state
        .services
        .values()
        .filter(|s| !s.deleted && s.spec.job.is_some())
        .cloned()
        .collect::<Vec<_>>();
    for service in services {
        let job = service.spec.job.as_ref().unwrap();
        let Some(schedule) = &job.schedule else {
            // Keep the last-consumed watermark if a schedule is temporarily removed.
            continue;
        };
        let cursor = service.job_cursor.as_ref();
        let policy_changed =
            cursor.is_none_or(|c| c.schedule != *schedule || c.time_zone != job.time_zone);
        if policy_changed || job.suspend {
            // Advancing a suspended cursor prevents catch-up on resume. Preserve a
            // future high-water mark if the host clock moved backwards.
            let base = cursor
                .and_then(|c| c.last_scheduled_at_unix_ms)
                .unwrap_or(now)
                .max(now);
            let Ok(next) = job.next_after(base) else {
                continue;
            };
            let next = if policy_changed {
                next
            } else {
                cursor.map_or(next, |c| next.max(c.next_at_unix_ms))
            };
            let new_cursor = JobCursor {
                last_scheduled_at_unix_ms: cursor.and_then(|c| c.last_scheduled_at_unix_ms),
                schedule: schedule.clone(),
                time_zone: job.time_zone.clone(),
                next_at_unix_ms: next,
            };
            if cursor != Some(&new_cursor) {
                state.services.get_mut(&service.id).unwrap().job_cursor = Some(new_cursor);
                changed = true;
            }
            continue;
        }
        let scheduled = cursor.unwrap().next_at_unix_ms;
        if scheduled > now {
            continue;
        }
        let Ok(next) = job.next_after(now) else {
            continue;
        };
        let cursor = state
            .services
            .get_mut(&service.id)
            .unwrap()
            .job_cursor
            .as_mut()
            .unwrap();
        cursor.next_at_unix_ms = next;
        cursor.last_scheduled_at_unix_ms = Some(scheduled);
        changed = true;
        // Skip an entire missed minute (no backlog), but tolerate ordinary tick latency.
        if now.saturating_sub(scheduled) >= 60_000 {
            continue;
        }
        let mut grace_ms = 0;
        for old in state
            .tasks
            .values_mut()
            .filter(|t| t.service_id == service.id && t.job.is_some())
        {
            if !old.observed.is_job_terminal() {
                grace_ms = grace_ms.max(
                    old.job
                        .as_ref()
                        .unwrap()
                        .spec
                        .stop_grace_period_seconds
                        .saturating_mul(1000) as i64
                        + 5_000,
                );
            }
            old.desired = DesiredTaskState::Stopped;
        }
        // Node binding is fixed for this occurrence. With no eligible node the
        // occurrence is skipped, never queued for a later round or retried elsewhere.
        let eligible = live
            .iter()
            .filter(|id| state.nodes.get(*id).is_some_and(|n| n.supports_jobs))
            .cloned()
            .collect();
        let mut template = service.clone();
        template.spec.replicas = 1;
        if let Some(mut task) = crate::scheduler::schedule_task(state, &template, &eligible) {
            task.job = Some(JobExecution {
                runtime: None,
                job_id: service.id.clone(),
                scheduled_at_unix_ms: scheduled,
                not_before_unix_ms: now
                    .saturating_add(grace_ms.min(30_000).min(next.saturating_sub(now) / 2)),
                start_deadline_unix_ms: next,
                spec: Box::new(service.spec.clone()),
            });
            state.tasks.insert(task.id.clone(), task);
        }
    }
    let ids = state.services.keys().cloned().collect::<Vec<_>>();
    for id in ids {
        let mut finished = state
            .tasks
            .values()
            .filter(|t| t.service_id == id && t.job.is_some() && t.observed.is_job_terminal())
            .map(|t| (t.job.as_ref().unwrap().scheduled_at_unix_ms, t.id.clone()))
            .collect::<Vec<_>>();
        finished.sort();
        let count = finished.len().saturating_sub(20);
        for (_, task_id) in finished.into_iter().take(count) {
            state.tasks.remove(&task_id);
            changed = true;
        }
    }
    changed
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{NodeRecord, ServiceRecord};

    fn setup() -> (ClusterState, BTreeSet<String>) {
        let parsed = swarmlite_stack::parse_stack(
            "x-swarmlite-jobs:\n  backup:\n    image: busybox:1.37\n    schedule: '* * * * *'\n",
        )
        .unwrap();
        let mut state = ClusterState::default();
        state.services.insert(
            "demo.backup".into(),
            ServiceRecord {
                job_cursor: None,
                id: "demo.backup".into(),
                stack: "demo".into(),
                name: "backup".into(),
                revision: 1,
                spec: parsed.services["backup"].clone(),
                deleted: false,
            },
        );
        for id in ["a", "b"] {
            state.nodes.insert(
                id.into(),
                NodeRecord {
                    supports_jobs: true,
                    id: id.into(),
                    address: "127.0.0.1".into(),
                    swarmlite_version: None,
                    labels: Default::default(),
                    cpu_millis: 1000,
                    memory_bytes: 1000,
                    port_range_start: 20000,
                    port_range_end: 20010,
                    gateway_enabled: false,
                },
            );
        }
        (state, BTreeSet::from(["a".into(), "b".into()]))
    }

    #[test]
    fn job_occurrence_is_consumed_once_and_next_replaces_unknown_or_running() {
        for observed in [
            ObservedTaskState::Running,
            ObservedTaskState::Unknown,
            ObservedTaskState::Failed,
        ] {
            let (mut state, live) = setup();
            assert!(reconcile(&mut state, &live, 0));
            assert!(state.tasks.is_empty()); // deploy never runs immediately
            reconcile(&mut state, &live, 60_000);
            let first = state.tasks.keys().next().unwrap().clone();
            state.tasks.get_mut(&first).unwrap().observed = observed;
            reconcile(&mut state, &live, 60_001);
            assert_eq!(state.tasks.len(), 1);
            reconcile(&mut state, &live, 120_000);
            assert_eq!(state.tasks.len(), 2);
            assert_eq!(state.tasks[&first].desired, DesiredTaskState::Stopped);
            let next = state.tasks.values().find(|t| t.id != first).unwrap();
            assert_eq!(next.job.as_ref().unwrap().scheduled_at_unix_ms, 120_000);
            assert!(next.job.as_ref().unwrap().not_before_unix_ms < 180_000);
            reconcile(&mut state, &live, 60_000); // backwards clock must not duplicate
            assert_eq!(state.tasks.len(), 2);
        }
    }

    #[test]
    fn job_lost_node_is_not_replaced_but_next_occurrence_uses_a_live_node() {
        let (mut state, mut live) = setup();
        reconcile(&mut state, &live, 0);
        reconcile(&mut state, &live, 60_000);
        let first = state.tasks.values().next().unwrap().clone();
        live.remove(&first.node_id);
        reconcile(&mut state, &live, 65_000);
        assert_eq!(state.tasks.len(), 1);
        assert_eq!(state.tasks[&first.id].observed, ObservedTaskState::Unknown);
        reconcile(&mut state, &live, 120_000);
        assert_eq!(state.tasks.len(), 2);
        assert!(state.tasks.values().any(|t| t.node_id != first.node_id));
    }

    #[test]
    fn job_restart_skips_outage_and_keeps_current_execution_identity() {
        let (mut state, live) = setup();
        reconcile(&mut state, &live, 0);
        reconcile(&mut state, &live, 60_000);
        let first = state.tasks.keys().next().unwrap().clone();
        let serialized = serde_json::to_string(&state).unwrap();
        let mut recovered: ClusterState = serde_json::from_str(&serialized).unwrap();
        skip_missed(&mut recovered, 300_000);
        reconcile(&mut recovered, &live, 300_000);
        assert_eq!(recovered.tasks.len(), 1);
        assert!(recovered.tasks.contains_key(&first));
        reconcile(&mut recovered, &live, 360_000);
        assert_eq!(recovered.tasks.len(), 2);
    }

    #[test]
    fn job_suspend_skips_triggers_without_stopping_a_running_job() {
        let (mut state, live) = setup();
        reconcile(&mut state, &live, 0);
        reconcile(&mut state, &live, 60_000);
        let first = state.tasks.keys().next().unwrap().clone();
        state.tasks.get_mut(&first).unwrap().observed = ObservedTaskState::Running;
        state
            .services
            .get_mut("demo.backup")
            .unwrap()
            .spec
            .job
            .as_mut()
            .unwrap()
            .suspend = true;
        reconcile(&mut state, &live, 120_000);
        assert_eq!(state.tasks[&first].desired, DesiredTaskState::Running);
        assert_eq!(state.tasks.len(), 1);
        state
            .services
            .get_mut("demo.backup")
            .unwrap()
            .spec
            .job
            .as_mut()
            .unwrap()
            .suspend = false;
        reconcile(&mut state, &live, 120_001);
        assert_eq!(state.tasks.len(), 1);
        reconcile(&mut state, &live, 180_000);
        assert_eq!(state.tasks.len(), 2);
    }

    #[test]
    fn job_old_agents_are_ineligible_and_no_node_does_not_queue_a_retry() {
        let (mut state, live) = setup();
        for node in state.nodes.values_mut() {
            node.supports_jobs = false;
        }
        reconcile(&mut state, &live, 0);
        reconcile(&mut state, &live, 60_000);
        for node in state.nodes.values_mut() {
            node.supports_jobs = true;
        }
        reconcile(&mut state, &live, 65_000);
        assert!(state.tasks.is_empty());
        reconcile(&mut state, &live, 120_000);
        assert_eq!(state.tasks.len(), 1);
    }
}
