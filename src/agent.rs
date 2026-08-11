use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::{Result, bail};
use tracing::{error, info, warn};

use crate::{
    client::ControllerClient,
    config::AgentConfig,
    local_state::{AgentFence, FENCE_KEY, LocalState},
    model::{
        GatewayReport, HeartbeatResponse, NodeControl, NodeHeartbeat, NodeRecord,
        ObservedTaskState, TaskReport,
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
    let runtime = Arc::new(runtime);
    let reconcile_runtime = Arc::clone(&runtime);
    let reconcile_cluster_id = config.cluster_id.clone();
    tokio::spawn(async move {
        reconciliation_loop(reconcile_runtime, assignments_rx, reconcile_cluster_id).await;
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
        ticker.tick().await;
        let containers = match runtime.list_managed(&config.cluster_id).await {
            Ok(containers) => containers,
            Err(error) => {
                error!(%error, "failed to inspect managed containers");
                continue;
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
) {
    while assignments.changed().await.is_ok() {
        let Some(response) = assignments.borrow_and_update().clone() else {
            continue;
        };
        let existing = match runtime.list_managed(&cluster_id).await {
            Ok(containers) => containers,
            Err(error) => {
                error!(%error, "failed to inspect containers in reconciliation loop");
                continue;
            }
        };
        if let Err(error) = reconcile_containers(runtime.as_ref(), &existing, &response).await {
            error!(error = %format!("{error:#}"), "container reconciliation failed");
        }
    }
}

async fn send_heartbeat(
    client: &ControllerClient,
    node_id: &str,
    heartbeat: &NodeHeartbeat,
) -> Result<HeartbeatResponse> {
    client
        .send_json(
            reqwest::Method::POST,
            &format!("/v1/nodes/{node_id}/heartbeat"),
            Some(heartbeat),
        )
        .await
}

async fn reconcile_containers<R: ContainerRuntime>(
    runtime: &R,
    existing: &HashMap<String, ManagedContainer>,
    response: &HeartbeatResponse,
) -> Result<()> {
    for task_id in &response.remove_tasks {
        if let Some(container) = existing.get(task_id) {
            runtime.remove_task(container).await?;
        }
    }

    for assignment in &response.assignments {
        match existing.get(&assignment.id) {
            Some(container)
                if container.spec_hash.as_deref().map_or_else(
                    || container.revision != Some(assignment.revision),
                    |hash| hash != assignment.spec_hash,
                ) =>
            {
                runtime.remove_task(container).await?;
                runtime.create_task(assignment).await?;
            }
            Some(container) if !container.running => runtime.start_task(container).await?,
            Some(container) if container.observed == ObservedTaskState::Failed => {
                runtime.remove_task(container).await?;
                runtime.create_task(assignment).await?;
            }
            Some(_) => {}
            None => runtime.create_task(assignment).await?,
        }
    }
    Ok(())
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

        reconcile_containers(&runtime, &existing, &response)
            .await
            .unwrap();
        assert!(runtime.removed.lock().unwrap().is_empty());

        response.remove_tasks.push("old-task".into());
        reconcile_containers(&runtime, &existing, &response)
            .await
            .unwrap();
        assert_eq!(&*runtime.removed.lock().unwrap(), &["old-task"]);
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
                spec_hash: "hash".into(),
            }],
            gateway_enabled: false,
            labels: Default::default(),
            remove_tasks: Vec::new(),
            gateway_config: None,
        };

        reconcile_containers(&runtime, &existing, &response)
            .await
            .unwrap();
        assert_eq!(&*runtime.started.lock().unwrap(), &["task-1"]);
        assert!(runtime.removed.lock().unwrap().is_empty());
    }
}
