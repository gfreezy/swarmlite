use std::{
    collections::{BTreeSet, HashMap},
    future::Future,
};

use anyhow::{Context, Result};
use bollard::{
    API_DEFAULT_VERSION, Docker,
    models::{
        ContainerCreateBody, HealthConfig, HealthStatusEnum, HostConfig,
        PortBinding as DockerPortBinding, RestartPolicy, RestartPolicyNameEnum,
    },
    query_parameters::{
        CreateContainerOptionsBuilder, CreateImageOptionsBuilder, ListContainersOptionsBuilder,
        RemoveContainerOptionsBuilder, StopContainerOptionsBuilder,
    },
};
use futures_util::StreamExt;
use tracing::{info, warn};

use crate::{
    config::{ResolvedRuntimeConfig, RuntimeKind},
    model::{ObservedTaskState, PortBinding, TaskAssignment},
};

pub(crate) const MANAGED_LABEL: &str = "io.swarmlite.managed";
pub(crate) const CLUSTER_LABEL: &str = "io.swarmlite.cluster_id";
const TASK_LABEL: &str = "io.swarmlite.task_id";
const SERVICE_LABEL: &str = "io.swarmlite.service_id";
const STACK_LABEL: &str = "io.swarmlite.stack";
const SERVICE_NAME_LABEL: &str = "io.swarmlite.service";
const SLOT_LABEL: &str = "io.swarmlite.slot";
const SPEC_HASH_LABEL: &str = "io.swarmlite.spec_sha256";
const PORTS_LABEL: &str = "io.swarmlite.ports";
const REVISION_LABEL: &str = "io.swarmlite.revision";
const STOP_GRACE_LABEL: &str = "io.swarmlite.stop_grace_seconds";

#[derive(Debug, Clone, Copy)]
pub struct RuntimeSystemInfo {
    pub cpu_millis: u64,
    pub memory_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct ManagedContainer {
    pub id: String,
    pub task_id: String,
    pub revision: Option<u64>,
    pub running: bool,
    pub observed: ObservedTaskState,
    pub stop_grace_seconds: i32,
    pub cluster_id: Option<String>,
    pub stack: Option<String>,
    pub service: Option<String>,
    pub slot: Option<u32>,
    pub spec_hash: Option<String>,
    pub ports: Vec<PortBinding>,
}

#[derive(Debug, Default)]
pub(crate) struct ManagedClusterInventory {
    pub cluster_ids: BTreeSet<String>,
    pub unlabeled: usize,
}

pub trait ContainerRuntime: Send + Sync + 'static {
    fn kind(&self) -> RuntimeKind;

    fn socket(&self) -> &str;

    fn ping(&self) -> impl Future<Output = Result<()>> + Send;

    fn system_info(&self) -> impl Future<Output = Result<RuntimeSystemInfo>> + Send;

    fn list_managed(
        &self,
        cluster_id: &str,
    ) -> impl Future<Output = Result<HashMap<String, ManagedContainer>>> + Send;

    fn create_task(&self, assignment: &TaskAssignment) -> impl Future<Output = Result<()>> + Send;

    fn start_task(&self, container: &ManagedContainer) -> impl Future<Output = Result<()>> + Send;

    fn remove_task(&self, container: &ManagedContainer) -> impl Future<Output = Result<()>> + Send;
}

#[derive(Clone)]
pub struct DockerCompatibleRuntime {
    client: Docker,
    kind: RuntimeKind,
    socket: String,
}

impl DockerCompatibleRuntime {
    pub fn connect(config: &ResolvedRuntimeConfig) -> Result<Self> {
        let client = Docker::connect_with_socket(&config.socket, 120, API_DEFAULT_VERSION)
            .with_context(|| {
                format!(
                    "failed to connect to {} API at {}",
                    config.kind, config.socket
                )
            })?;
        Ok(Self {
            client,
            kind: config.kind,
            socket: config.socket.clone(),
        })
    }

    pub(crate) async fn managed_cluster_inventory(&self) -> Result<ManagedClusterInventory> {
        let summaries = self.list_managed_summaries().await?;
        let mut inventory = ManagedClusterInventory::default();
        for summary in summaries {
            let labels = summary.labels.unwrap_or_default();
            match labels.get(CLUSTER_LABEL).filter(|value| !value.is_empty()) {
                Some(cluster_id) => {
                    inventory.cluster_ids.insert(cluster_id.clone());
                }
                None => inventory.unlabeled += 1,
            }
        }
        Ok(inventory)
    }

    async fn list_managed_summaries(&self) -> Result<Vec<bollard::models::ContainerSummary>> {
        let filters = HashMap::from([("label".to_owned(), vec![format!("{MANAGED_LABEL}=true")])]);
        let options = ListContainersOptionsBuilder::default()
            .all(true)
            .filters(&filters)
            .build();
        self.client
            .list_containers(Some(options))
            .await
            .map_err(Into::into)
    }
}

impl ContainerRuntime for DockerCompatibleRuntime {
    fn kind(&self) -> RuntimeKind {
        self.kind
    }

    fn socket(&self) -> &str {
        &self.socket
    }

    async fn ping(&self) -> Result<()> {
        self.client
            .ping()
            .await
            .with_context(|| format!("{} API did not answer ping", self.kind))?;
        Ok(())
    }

    async fn system_info(&self) -> Result<RuntimeSystemInfo> {
        let system = self
            .client
            .info()
            .await
            .with_context(|| format!("failed to read {} system info", self.kind))?;
        Ok(RuntimeSystemInfo {
            cpu_millis: system.ncpu.unwrap_or(0).max(0) as u64 * 1000,
            memory_bytes: system.mem_total.unwrap_or(0).max(0) as u64,
        })
    }

    async fn list_managed(&self, cluster_id: &str) -> Result<HashMap<String, ManagedContainer>> {
        let summaries = self.list_managed_summaries().await?;
        let mut result = HashMap::new();
        for summary in summaries {
            let Some(id) = summary.id else { continue };
            let labels = summary.labels.unwrap_or_default();
            if labels.get(CLUSTER_LABEL).map(String::as_str) != Some(cluster_id) {
                continue;
            }
            let Some(task_id) = labels.get(TASK_LABEL).cloned() else {
                continue;
            };
            let inspect = self.client.inspect_container(&id, None).await?;
            let running = inspect
                .state
                .as_ref()
                .is_some_and(|state| state.running == Some(true));
            let observed = inspect
                .state
                .map(observed_state)
                .unwrap_or(ObservedTaskState::Failed);
            let revision = labels
                .get(REVISION_LABEL)
                .and_then(|value| value.parse().ok());
            let stop_grace_seconds = labels
                .get(STOP_GRACE_LABEL)
                .and_then(|value| value.parse().ok())
                .unwrap_or(10);
            let ports = labels
                .get(PORTS_LABEL)
                .and_then(|value| serde_json::from_str(value).ok())
                .unwrap_or_default();
            result.insert(
                task_id.clone(),
                ManagedContainer {
                    id,
                    task_id,
                    revision,
                    running,
                    observed,
                    stop_grace_seconds,
                    cluster_id: labels.get(CLUSTER_LABEL).cloned(),
                    stack: labels.get(STACK_LABEL).cloned(),
                    service: labels.get(SERVICE_NAME_LABEL).cloned(),
                    slot: labels.get(SLOT_LABEL).and_then(|value| value.parse().ok()),
                    spec_hash: labels.get(SPEC_HASH_LABEL).cloned(),
                    ports,
                },
            );
        }
        Ok(result)
    }

    async fn create_task(&self, assignment: &TaskAssignment) -> Result<()> {
        info!(
            task_id = %assignment.id,
            image = %assignment.spec.image,
            runtime = %self.kind,
            "creating task container"
        );
        if self
            .client
            .inspect_image(&assignment.spec.image)
            .await
            .is_err()
        {
            let options = CreateImageOptionsBuilder::default()
                .from_image(&assignment.spec.image)
                .build();
            let mut pull = self.client.create_image(Some(options), None, None);
            while let Some(item) = pull.next().await {
                item.with_context(|| format!("failed to pull {}", assignment.spec.image))?;
            }
        }

        let mut port_bindings = HashMap::new();
        let exposed_ports = assignment
            .ports
            .iter()
            .map(|port| format!("{}/{}", port.target, port.protocol))
            .collect::<Vec<_>>();
        for port in &assignment.ports {
            port_bindings.insert(
                format!("{}/{}", port.target, port.protocol),
                Some(vec![DockerPortBinding {
                    host_ip: Some("0.0.0.0".to_owned()),
                    host_port: Some(port.published.to_string()),
                }]),
            );
        }
        let labels = task_labels(assignment)?;
        let host_config = HostConfig {
            binds: (!assignment.spec.volumes.is_empty()).then_some(assignment.spec.volumes.clone()),
            port_bindings: (!port_bindings.is_empty()).then_some(port_bindings),
            restart_policy: Some(RestartPolicy {
                name: Some(RestartPolicyNameEnum::UNLESS_STOPPED),
                maximum_retry_count: None,
            }),
            ..Default::default()
        };
        let body = ContainerCreateBody {
            image: Some(assignment.spec.image.clone()),
            cmd: (!assignment.spec.command.is_empty()).then_some(assignment.spec.command.clone()),
            entrypoint: (!assignment.spec.entrypoint.is_empty())
                .then_some(assignment.spec.entrypoint.clone()),
            env: (!assignment.spec.environment.is_empty())
                .then_some(assignment.spec.environment.clone()),
            exposed_ports: (!exposed_ports.is_empty()).then_some(exposed_ports),
            labels: Some(labels),
            healthcheck: assignment
                .spec
                .healthcheck
                .as_ref()
                .map(|healthcheck| HealthConfig {
                    test: Some(healthcheck.test.clone()),
                    interval: healthcheck.interval_nanos,
                    timeout: healthcheck.timeout_nanos,
                    retries: healthcheck.retries,
                    start_period: healthcheck.start_period_nanos,
                    start_interval: healthcheck.start_interval_nanos,
                }),
            stop_timeout: Some(assignment.spec.stop_grace_period_seconds as i64),
            host_config: Some(host_config),
            ..Default::default()
        };
        let short = assignment.id.chars().take(8).collect::<String>();
        let name = format!(
            "swarmlite-{}-{}-{short}",
            sanitize_name(&assignment.service_id),
            assignment.slot
        );
        let create_options = CreateContainerOptionsBuilder::default().name(&name).build();
        let created = self
            .client
            .create_container(Some(create_options), body)
            .await
            .with_context(|| format!("failed to create task {}", assignment.id))?;
        self.client
            .start_container(&created.id, None)
            .await
            .with_context(|| format!("failed to start task {}", assignment.id))?;
        Ok(())
    }

    async fn remove_task(&self, container: &ManagedContainer) -> Result<()> {
        info!(
            task_id = %container.task_id,
            runtime = %self.kind,
            "removing obsolete task container"
        );
        let stop = StopContainerOptionsBuilder::default()
            .t(container.stop_grace_seconds)
            .build();
        if let Err(error) = self.client.stop_container(&container.id, Some(stop)).await {
            warn!(task_id = %container.task_id, %error, "graceful stop failed; forcing removal");
        }
        let remove = RemoveContainerOptionsBuilder::default().force(true).build();
        self.client
            .remove_container(&container.id, Some(remove))
            .await
            .with_context(|| format!("failed to remove task {}", container.task_id))?;
        Ok(())
    }

    async fn start_task(&self, container: &ManagedContainer) -> Result<()> {
        info!(
            task_id = %container.task_id,
            runtime = %self.kind,
            "starting recovered task container"
        );
        self.client
            .start_container(&container.id, None)
            .await
            .with_context(|| format!("failed to start recovered task {}", container.task_id))
    }
}

fn task_labels(assignment: &TaskAssignment) -> Result<HashMap<String, String>> {
    let mut labels = assignment
        .spec
        .container_labels
        .clone()
        .into_iter()
        .collect::<HashMap<_, _>>();
    labels.extend([
        (MANAGED_LABEL.to_owned(), "true".to_owned()),
        (CLUSTER_LABEL.to_owned(), assignment.cluster_id.clone()),
        (TASK_LABEL.to_owned(), assignment.id.clone()),
        (SERVICE_LABEL.to_owned(), assignment.service_id.clone()),
        (STACK_LABEL.to_owned(), assignment.stack.clone()),
        (SERVICE_NAME_LABEL.to_owned(), assignment.service.clone()),
        (SLOT_LABEL.to_owned(), assignment.slot.to_string()),
        (SPEC_HASH_LABEL.to_owned(), assignment.spec_hash.clone()),
        (
            PORTS_LABEL.to_owned(),
            serde_json::to_string(&assignment.ports)?,
        ),
        (REVISION_LABEL.to_owned(), assignment.revision.to_string()),
        (
            STOP_GRACE_LABEL.to_owned(),
            assignment.spec.stop_grace_period_seconds.to_string(),
        ),
    ]);
    Ok(labels)
}

fn observed_state(state: bollard::models::ContainerState) -> ObservedTaskState {
    if state.running != Some(true) {
        return if state.restarting == Some(true) {
            ObservedTaskState::Starting
        } else {
            ObservedTaskState::Failed
        };
    }
    match state.health.and_then(|health| health.status) {
        Some(HealthStatusEnum::STARTING) => ObservedTaskState::Starting,
        Some(HealthStatusEnum::UNHEALTHY) => ObservedTaskState::Failed,
        Some(HealthStatusEnum::HEALTHY) => ObservedTaskState::Healthy,
        _ => ObservedTaskState::Healthy,
    }
}

fn sanitize_name(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.') {
                character
            } else {
                '-'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::model::ServiceSpec;

    use super::*;

    #[test]
    fn sanitizes_runtime_container_names() {
        assert_eq!(sanitize_name("demo/web:v1"), "demo-web-v1");
    }

    #[test]
    fn adds_cluster_and_recovery_identity_labels() {
        let assignment = TaskAssignment {
            id: "task-1".into(),
            cluster_id: "cluster-old".into(),
            stack: "demo".into(),
            service: "web".into(),
            service_id: "demo.web".into(),
            revision: 2,
            slot: 0,
            spec: ServiceSpec {
                image: "nginx:alpine".into(),
                command: Vec::new(),
                entrypoint: Vec::new(),
                environment: Vec::new(),
                ports: Vec::new(),
                volumes: Vec::new(),
                container_labels: BTreeMap::from([(CLUSTER_LABEL.to_owned(), "user-value".into())]),
                service_labels: BTreeMap::new(),
                healthcheck: None,
                replicas: 1,
                constraints: Vec::new(),
                max_surge: 1,
                stop_grace_period_seconds: 10,
            },
            ports: Vec::new(),
            leader_term: 3,
            generation: 4,
            spec_hash: "abc123".into(),
        };

        let labels = task_labels(&assignment).unwrap();
        assert_eq!(labels.len(), 11);
        assert_eq!(labels[MANAGED_LABEL], "true");
        assert_eq!(labels[CLUSTER_LABEL], "cluster-old");
        assert_eq!(labels[STACK_LABEL], "demo");
        assert_eq!(labels[SERVICE_NAME_LABEL], "web");
        assert_eq!(labels[SLOT_LABEL], "0");
        assert_eq!(labels[SPEC_HASH_LABEL], "abc123");
        assert_eq!(labels[TASK_LABEL], "task-1");
        assert_eq!(labels[SERVICE_LABEL], "demo.web");
        assert_eq!(labels[PORTS_LABEL], "[]");
        assert_eq!(labels[REVISION_LABEL], "2");
        assert_eq!(labels[STOP_GRACE_LABEL], "10");
        assert!(!labels.contains_key("io.swarmlite.cluster_epoch"));
        assert!(!labels.contains_key("io.swarmlite.claim_signature"));
        assert!(!labels.contains_key("io.swarmlite.term"));
        assert!(!labels.contains_key("io.swarmlite.generation"));
    }
}
