use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Result, bail};
use tracing::{error, info, warn};

use crate::{
    client::ControllerClient,
    config::AgentConfig,
    local_state::{AgentFence, FENCE_KEY, LocalState},
    model::{
        GatewayReport, HeartbeatResponse, NodeControl, NodeHeartbeat, NodeRecord,
        ObservedTaskState, TaskReconcilePhase, TaskReconcileReport, TaskReport,
    },
    runtime::{ContainerRuntime, DockerCompatibleRuntime, ManagedContainer},
};

pub(crate) async fn run_with_token_and_updates(
    config: AgentConfig,
    token: String,
    updates: tokio::sync::watch::Sender<NodeControl>,
    gateway_report: tokio::sync::watch::Receiver<GatewayReport>,
    local_state: LocalState,
    runtime: DockerCompatibleRuntime,
) -> Result<()> {
    run_with_runtime(config, token, updates, gateway_report, local_state, runtime).await
}

async fn run_with_runtime<R: ContainerRuntime>(
    config: AgentConfig,
    token: String,
    updates: tokio::sync::watch::Sender<NodeControl>,
    gateway_report: tokio::sync::watch::Receiver<GatewayReport>,
    local_state: LocalState,
    runtime: R,
) -> Result<()> {
    runtime.ping().await?;
    let system = runtime.system_info().await?;
    let mut fence = local_state
        .get::<AgentFence>(FENCE_KEY)?
        .unwrap_or_default();
    let mut node = NodeRecord {
        id: config.node_id.clone(),
        address: config.advertise_address.clone(),
        labels: config.labels.clone(),
        cpu_millis: system.cpu_millis,
        memory_bytes: system.memory_bytes,
        port_range_start: config.port_range.start,
        port_range_end: config.port_range.end,
        gateway_enabled: config.gateway_enabled,
    };
    let client = ControllerClient::new(&config.controller, token);
    let (assignments_tx, assignments_rx) = tokio::sync::watch::channel(None);
    let reconcile_results = Arc::new(Mutex::new(BTreeMap::<String, TaskReconcileReport>::new()));
    let (reconcile_events_tx, mut reconcile_events_rx) = tokio::sync::mpsc::unbounded_channel();
    let runtime = Arc::new(runtime);
    let reconcile_runtime = Arc::clone(&runtime);
    let reconcile_cluster_id = config.cluster_id.clone();
    let loop_results = Arc::clone(&reconcile_results);
    tokio::spawn(async move {
        reconciliation_loop(
            reconcile_runtime,
            assignments_rx,
            reconcile_cluster_id,
            loop_results,
            reconcile_events_tx,
        )
        .await;
    });
    let mut ticker = tokio::time::interval(Duration::from_secs(config.heartbeat_interval_seconds));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    info!(
        node_id = %config.node_id,
        runtime = %runtime.kind(),
        socket = runtime.socket(),
        "node agent started"
    );

    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            event = reconcile_events_rx.recv() => {
                if event.is_none() {
                    bail!("container reconciliation loop stopped unexpectedly");
                }
            }
        }
        let (containers, task_inventory_error) =
            match runtime.list_managed(&config.cluster_id).await {
                Ok(containers) => (containers, None),
                Err(error) => {
                    error!(%error, "failed to inspect managed containers");
                    (HashMap::new(), Some(format!("{error:#}")))
                }
            };
        let heartbeat = NodeHeartbeat {
            node: node.clone(),
            tasks: containers
                .values()
                .map(|container| TaskReport {
                    id: container.task_id.clone(),
                    observed: container.observed.clone(),
                    container_id: Some(container.id.clone()),
                    cluster_id: container.cluster_id.clone(),
                    stack: container.stack.clone(),
                    service: container.service.clone(),
                    slot: container.slot,
                    revision: container.revision,
                    spec_hash: container.spec_hash.clone(),
                    ports: container.ports.clone(),
                })
                .collect(),
            task_inventory_error,
            task_results: reconcile_results
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .values()
                .cloned()
                .collect(),
            gateway: gateway_report.borrow().clone(),
        };
        let response = match send_heartbeat(&client, &config.node_id, &heartbeat).await {
            Ok(response) => response,
            Err(error) => {
                warn!(%error, "controller is unavailable; leaving current containers unchanged");
                continue;
            }
        };
        if response.generation < fence.generation {
            warn!(
                received_generation = response.generation,
                current_generation = fence.generation,
                "rejected stale controller response"
            );
            continue;
        }
        fence.generation = response.generation;
        if let Err(error) = local_state.put(FENCE_KEY, &fence) {
            error!(%error, "failed to persist fencing state; refusing to change containers");
            continue;
        }
        let next_control = NodeControl {
            cluster: response.cluster.clone(),
            gateway_enabled: response.gateway_enabled,
            labels: response.labels.clone(),
            gateway_config: response.gateway_config.clone(),
        };
        let gateway_needs_apply = response.gateway_enabled
            && response.gateway_config.as_ref().is_some_and(|assignment| {
                heartbeat.gateway.error.is_some()
                    || heartbeat.gateway.applied_generation != Some(assignment.generation)
            });
        node.gateway_enabled = response.gateway_enabled;
        node.labels.clone_from(&response.labels);
        if gateway_needs_apply {
            updates.send_replace(next_control);
        } else {
            updates.send_if_modified(|current| {
                if *current == next_control {
                    false
                } else {
                    current.clone_from(&next_control);
                    true
                }
            });
        }
        if assignments_tx.send(Some(response)).is_err() {
            bail!("container reconciliation loop stopped unexpectedly");
        }
    }
}

async fn reconciliation_loop<R: ContainerRuntime>(
    runtime: Arc<R>,
    mut assignments: tokio::sync::watch::Receiver<Option<HeartbeatResponse>>,
    cluster_id: String,
    results: Arc<Mutex<BTreeMap<String, TaskReconcileReport>>>,
    events: tokio::sync::mpsc::UnboundedSender<()>,
) {
    while assignments.changed().await.is_ok() {
        let Some(response) = assignments.borrow_and_update().clone() else {
            continue;
        };
        let existing =
            match runtime.list_managed(&cluster_id).await {
                Ok(containers) => containers,
                Err(error) => {
                    error!(%error, "failed to inspect containers in reconciliation loop");
                    let message = format!("{error:#}");
                    let reports =
                        response
                            .assignments
                            .iter()
                            .map(|assignment| (&assignment.id, assignment.deployment_generation))
                            .chain(response.remove_tasks.iter().map(|assignment| {
                                (&assignment.id, assignment.deployment_generation)
                            }))
                            .map(|(task_id, desired_generation)| TaskReconcileReport {
                                task_id: task_id.clone(),
                                desired_generation,
                                applied_generation: None,
                                phase: TaskReconcilePhase::Inspect,
                                error: Some(message.clone()),
                            })
                            .collect();
                    publish_reconcile_results(&response, reports, &results, &events);
                    continue;
                }
            };
        let reports = reconcile_containers(runtime.as_ref(), &existing, &response).await;
        publish_reconcile_results(&response, reports, &results, &events);
    }
}

fn publish_reconcile_results(
    response: &HeartbeatResponse,
    reports: Vec<TaskReconcileReport>,
    results: &Mutex<BTreeMap<String, TaskReconcileReport>>,
    events: &tokio::sync::mpsc::UnboundedSender<()>,
) {
    let expected = response
        .assignments
        .iter()
        .map(|assignment| assignment.id.as_str())
        .chain(
            response
                .remove_tasks
                .iter()
                .map(|assignment| assignment.id.as_str()),
        )
        .collect::<std::collections::HashSet<_>>();
    let mut current = results
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let previous = current.clone();
    current.retain(|task_id, _| expected.contains(task_id.as_str()));
    for report in reports {
        current.insert(report.task_id.clone(), report);
    }
    if *current != previous {
        let _ = events.send(());
    }
}

async fn send_heartbeat(
    client: &ControllerClient,
    node_id: &str,
    heartbeat: &NodeHeartbeat,
) -> Result<HeartbeatResponse> {
    Ok(client
        .send_json(
            reqwest::Method::POST,
            &format!("/v1/nodes/{node_id}/heartbeat"),
            Some(heartbeat),
        )
        .await?)
}

async fn reconcile_containers<R: ContainerRuntime>(
    runtime: &R,
    existing: &HashMap<String, ManagedContainer>,
    response: &HeartbeatResponse,
) -> Vec<TaskReconcileReport> {
    let mut reports = Vec::new();
    for removal in &response.remove_tasks {
        let result = match existing.get(&removal.id) {
            Some(container) => runtime.remove_task(container).await,
            None => Ok(()),
        };
        reports.push(reconcile_report(
            &removal.id,
            removal.deployment_generation,
            TaskReconcilePhase::Remove,
            result,
        ));
    }

    for assignment in &response.assignments {
        if assignment.desired == crate::model::DesiredTaskState::Draining {
            reports.push(reconcile_report(
                &assignment.id,
                assignment.deployment_generation,
                TaskReconcilePhase::Verify,
                Ok(()),
            ));
            continue;
        }
        let (phase, result) = match existing.get(&assignment.id) {
            Some(container)
                if container.spec_hash.as_deref().map_or_else(
                    || container.revision != Some(assignment.revision),
                    |hash| hash != assignment.spec_hash,
                ) =>
            {
                let result = async {
                    runtime.remove_task(container).await?;
                    runtime.create_task(assignment).await
                }
                .await;
                (TaskReconcilePhase::Replace, result)
            }
            Some(container) if !container.running => (
                TaskReconcilePhase::Start,
                runtime.start_task(container).await,
            ),
            Some(container) if container.observed == ObservedTaskState::Failed => {
                let result = async {
                    runtime.remove_task(container).await?;
                    runtime.create_task(assignment).await
                }
                .await;
                (TaskReconcilePhase::Replace, result)
            }
            Some(_) => (TaskReconcilePhase::Verify, Ok(())),
            None => (
                TaskReconcilePhase::Create,
                runtime.create_task(assignment).await,
            ),
        };
        if let Err(error) = &result {
            error!(
                task_id = %assignment.id,
                error = %format!("{error:#}"),
                "container reconciliation failed"
            );
        }
        reports.push(reconcile_report(
            &assignment.id,
            assignment.deployment_generation,
            phase,
            result,
        ));
    }
    reports
}

fn reconcile_report(
    task_id: &str,
    desired_generation: u64,
    phase: TaskReconcilePhase,
    result: Result<()>,
) -> TaskReconcileReport {
    match result {
        Ok(()) => TaskReconcileReport {
            task_id: task_id.to_owned(),
            desired_generation,
            applied_generation: Some(desired_generation),
            phase,
            error: None,
        },
        Err(error) => TaskReconcileReport {
            task_id: task_id.to_owned(),
            desired_generation,
            applied_generation: None,
            phase,
            error: Some(format!("{error:#}")),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use crate::{
        config::RuntimeKind,
        model::{ClusterGatewayConfig, ClusterSettings, HeartbeatResponse},
        runtime::RuntimeSystemInfo,
    };

    use super::*;

    #[derive(Clone, Default)]
    struct FakeRuntime {
        removed: Arc<Mutex<Vec<String>>>,
        started: Arc<Mutex<Vec<String>>>,
        fail_create: bool,
    }

    impl ContainerRuntime for FakeRuntime {
        fn kind(&self) -> RuntimeKind {
            RuntimeKind::Docker
        }

        fn socket(&self) -> &str {
            "fake"
        }

        async fn ping(&self) -> Result<()> {
            Ok(())
        }

        async fn system_info(&self) -> Result<RuntimeSystemInfo> {
            Ok(RuntimeSystemInfo {
                cpu_millis: 1,
                memory_bytes: 1,
            })
        }

        async fn list_managed(
            &self,
            _cluster_id: &str,
        ) -> Result<HashMap<String, ManagedContainer>> {
            Ok(HashMap::new())
        }

        async fn create_task(&self, _assignment: &crate::model::TaskAssignment) -> Result<()> {
            if self.fail_create {
                bail!("image pull denied");
            }
            Ok(())
        }

        async fn start_task(&self, container: &ManagedContainer) -> Result<()> {
            self.started.lock().unwrap().push(container.task_id.clone());
            Ok(())
        }

        async fn remove_task(&self, container: &ManagedContainer) -> Result<()> {
            self.removed.lock().unwrap().push(container.task_id.clone());
            Ok(())
        }
    }

    fn managed(task_id: &str) -> ManagedContainer {
        ManagedContainer {
            id: format!("container-{task_id}"),
            task_id: task_id.to_owned(),
            revision: Some(1),
            running: true,
            observed: ObservedTaskState::Healthy,
            stop_grace_seconds: 10,
            cluster_id: Some("cluster-test".into()),
            stack: Some("demo".into()),
            service: Some("web".into()),
            slot: Some(0),
            spec_hash: Some("hash".into()),
            ports: Vec::new(),
        }
    }

    fn test_cluster() -> ClusterSettings {
        ClusterSettings {
            schema_version: crate::model::CLUSTER_SCHEMA_VERSION,
            cluster_id: "cluster-test".into(),
            controller_id: "controller-a".into(),
            controller_port: 8080,
            gateway: ClusterGatewayConfig::default(),
        }
    }

    #[tokio::test]
    async fn leaves_unknown_containers_untouched_until_explicitly_removed() {
        let runtime = FakeRuntime::default();
        let existing = HashMap::from([("old-task".into(), managed("old-task"))]);
        let mut response = HeartbeatResponse {
            generation: 1,
            cluster: test_cluster(),
            assignments: Vec::new(),
            gateway_enabled: false,
            labels: Default::default(),
            remove_tasks: Vec::new(),
            gateway_config: None,
        };

        let reports = reconcile_containers(&runtime, &existing, &response).await;
        assert!(reports.is_empty());
        assert!(runtime.removed.lock().unwrap().is_empty());

        response
            .remove_tasks
            .push(crate::model::TaskRemovalAssignment {
                id: "old-task".into(),
                deployment_generation: 1,
            });
        let reports = reconcile_containers(&runtime, &existing, &response).await;
        assert_eq!(&*runtime.removed.lock().unwrap(), &["old-task"]);
        assert_eq!(reports[0].applied_generation, Some(1));
        assert_eq!(reports[0].phase, TaskReconcilePhase::Remove);
    }

    #[tokio::test]
    async fn starts_a_matching_stopped_recovered_container_in_place() {
        let runtime = FakeRuntime::default();
        let mut container = managed("task-1");
        container.running = false;
        container.observed = ObservedTaskState::Failed;
        let existing = HashMap::from([("task-1".into(), container)]);
        let response = HeartbeatResponse {
            generation: 1,
            cluster: test_cluster(),
            assignments: vec![crate::model::TaskAssignment {
                id: "task-1".into(),
                cluster_id: "cluster-test".into(),
                stack: "demo".into(),
                service: "web".into(),
                service_id: "demo.web".into(),
                revision: 1,
                slot: 0,
                desired: crate::model::DesiredTaskState::Running,
                spec: crate::model::ServiceSpec {
                    image: "nginx:alpine".into(),
                    command: Vec::new(),
                    entrypoint: Vec::new(),
                    environment: Vec::new(),
                    ports: Vec::new(),
                    volumes: Vec::new(),
                    container_labels: Default::default(),
                    service_labels: Default::default(),
                    healthcheck: None,
                    replicas: 1,
                    constraints: Vec::new(),
                    max_surge: 1,
                    stop_grace_period_seconds: 10,
                },
                ports: Vec::new(),
                generation: 1,
                deployment_generation: 1,
                spec_hash: "hash".into(),
            }],
            gateway_enabled: false,
            labels: Default::default(),
            remove_tasks: Vec::new(),
            gateway_config: None,
        };

        let reports = reconcile_containers(&runtime, &existing, &response).await;
        assert_eq!(&*runtime.started.lock().unwrap(), &["task-1"]);
        assert!(runtime.removed.lock().unwrap().is_empty());
        assert_eq!(reports[0].phase, TaskReconcilePhase::Start);
        assert_eq!(reports[0].applied_generation, Some(1));
    }

    #[tokio::test]
    async fn reports_create_failures_with_the_assignment_generation() {
        let runtime = FakeRuntime {
            fail_create: true,
            ..Default::default()
        };
        let response = HeartbeatResponse {
            generation: 7,
            cluster: test_cluster(),
            assignments: vec![crate::model::TaskAssignment {
                id: "task-failed".into(),
                cluster_id: "cluster-test".into(),
                stack: "demo".into(),
                service: "web".into(),
                service_id: "demo.web".into(),
                revision: 1,
                slot: 0,
                desired: crate::model::DesiredTaskState::Running,
                spec: crate::model::ServiceSpec {
                    image: "private.example/web:latest".into(),
                    command: Vec::new(),
                    entrypoint: Vec::new(),
                    environment: Vec::new(),
                    ports: Vec::new(),
                    volumes: Vec::new(),
                    container_labels: Default::default(),
                    service_labels: Default::default(),
                    healthcheck: None,
                    replicas: 1,
                    constraints: Vec::new(),
                    max_surge: 1,
                    stop_grace_period_seconds: 10,
                },
                ports: Vec::new(),
                generation: 7,
                deployment_generation: 5,
                spec_hash: "hash".into(),
            }],
            gateway_enabled: false,
            labels: Default::default(),
            remove_tasks: Vec::new(),
            gateway_config: None,
        };

        let reports = reconcile_containers(&runtime, &HashMap::new(), &response).await;
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].desired_generation, 5);
        assert_eq!(reports[0].applied_generation, None);
        assert_eq!(reports[0].phase, TaskReconcilePhase::Create);
        assert!(
            reports[0]
                .error
                .as_deref()
                .unwrap()
                .contains("image pull denied")
        );
    }
}
