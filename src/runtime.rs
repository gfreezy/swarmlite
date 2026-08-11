use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    future::Future,
};

use anyhow::{Context, Result, bail};
use bollard::{
    API_DEFAULT_VERSION, Docker,
    models::{
        ContainerCreateBody, ContainerSummaryStateEnum, HealthConfig, HealthStatusEnum, HostConfig,
        PortBinding as DockerPortBinding, RestartPolicy, RestartPolicyNameEnum,
    },
    query_parameters::{
        CreateContainerOptionsBuilder, CreateImageOptionsBuilder, ListContainersOptionsBuilder,
        RemoveContainerOptionsBuilder, RemoveVolumeOptionsBuilder, StopContainerOptionsBuilder,
    },
};
use futures_util::StreamExt;
use sha2::{Digest, Sha256};
use tracing::{info, warn};

use crate::{
    config::{ResolvedRuntimeConfig, RuntimeKind},
    gateway,
    model::{ClusterState, GatewayAssignment, ObservedTaskState, PortBinding, TaskAssignment},
};

#[cfg(test)]
use crate::model::DEFAULT_GATEWAY_IMAGE;

pub(crate) const MANAGED_LABEL: &str = "io.swarmlite.managed";
pub(crate) const CLUSTER_LABEL: &str = "io.swarmlite.cluster_id";
pub(crate) const SYSTEM_LABEL: &str = "io.swarmlite.system";
pub(crate) const COMPONENT_LABEL: &str = "io.swarmlite.component";
pub(crate) const GATEWAY_COMPONENT: &str = "gateway";
pub(crate) const GATEWAY_ADDRESS_LABEL: &str = "io.swarmlite.advertise_address";
const GATEWAY_SCHEMA_LABEL: &str = "io.swarmlite.gateway_schema";
const GATEWAY_IMAGE_LABEL: &str = "io.swarmlite.gateway_image";
const GATEWAY_LISTEN_LABEL: &str = "io.swarmlite.gateway_listen";
const GATEWAY_TOKEN_HASH_LABEL: &str = "io.swarmlite.gateway_token_sha256";
const GATEWAY_SCHEMA: &str = "4";
const GATEWAY_CONTAINER_NAME: &str = "swarmlite-gateway";
const GATEWAY_ADMIN_URL: &str = "http://127.0.0.1:2019";
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
    pub gateway_cluster_ids: BTreeSet<String>,
    pub gateway_listen: BTreeMap<String, Vec<String>>,
    pub gateway_images: BTreeMap<String, String>,
    pub unlabeled: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct GatewayContainerSpec {
    pub cluster_id: String,
    pub advertise_address: String,
    pub listen: Vec<String>,
    pub controller: String,
    pub token: String,
    pub image: String,
}

#[derive(Debug)]
struct ExistingGatewayContainer {
    id: String,
    cluster_id: Option<String>,
    advertise_address: Option<String>,
    image: Option<String>,
    listen: Option<String>,
    token_hash: Option<String>,
    schema: Option<String>,
    running: bool,
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
                    if is_gateway_system_container(&labels) {
                        inventory.gateway_cluster_ids.insert(cluster_id.clone());
                        if let Some(listen) = labels
                            .get(GATEWAY_LISTEN_LABEL)
                            .map(|value| {
                                value
                                    .split(',')
                                    .map(str::trim)
                                    .filter(|value| !value.is_empty())
                                    .map(ToOwned::to_owned)
                                    .collect::<Vec<_>>()
                            })
                            .filter(|listen| !listen.is_empty())
                        {
                            match inventory
                                .gateway_listen
                                .insert(cluster_id.clone(), listen.clone())
                            {
                                Some(existing) if existing != listen => bail!(
                                    "managed gateway containers for cluster {cluster_id} have conflicting listener labels"
                                ),
                                _ => {}
                            }
                        }
                        if let Some(image) = labels
                            .get(GATEWAY_IMAGE_LABEL)
                            .filter(|value| !value.is_empty())
                        {
                            match inventory
                                .gateway_images
                                .insert(cluster_id.clone(), image.clone())
                            {
                                Some(existing) if existing != *image => bail!(
                                    "managed gateway containers for cluster {cluster_id} have conflicting image labels"
                                ),
                                _ => {}
                            }
                        }
                    }
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

    pub(crate) async fn reconcile_gateway(
        &self,
        spec: &GatewayContainerSpec,
        enabled: bool,
    ) -> Result<()> {
        if enabled {
            gateway_ports(&spec.listen)?;
        }
        let gateways = self.gateway_containers().await?;
        if gateways.len() > 1 {
            bail!(
                "found multiple managed gateway containers; keep exactly one on this node before serving"
            );
        }
        let existing = gateways.into_iter().next();
        if let Some(existing) = &existing
            && existing.cluster_id.as_deref() != Some(&spec.cluster_id)
        {
            bail!(
                "managed gateway container belongs to cluster {:?}, not {}; recover the old cluster or remove that container",
                existing.cluster_id,
                spec.cluster_id
            );
        }

        if !enabled {
            if let Some(existing) = existing {
                self.remove_gateway(&existing).await?;
            }
            self.remove_gateway_volumes(&spec.cluster_id).await?;
            return Ok(());
        }

        if let Some(existing) = existing {
            if gateway_matches_spec(&existing, spec) {
                if !existing.running {
                    self.client
                        .start_container(&existing.id, None)
                        .await
                        .context("failed to start the managed gateway container")?;
                    info!(runtime = %self.kind, "started existing gateway container");
                }
                return Ok(());
            }
            self.ensure_image(&spec.image).await?;
            info!(
                previous_address = ?existing.advertise_address,
                address = %spec.advertise_address,
                "recreating gateway container for the current gateway settings"
            );
            self.remove_gateway(&existing).await?;
            return self.create_gateway(spec).await;
        }

        self.create_gateway(spec).await
    }

    pub(crate) async fn apply_gateway_config(&self, assignment: &GatewayAssignment) -> Result<()> {
        let client = reqwest::Client::new();
        self.post_gateway_config(&client, "/load", &assignment.config)
            .await?;
        info!(
            generation = assignment.generation,
            runtime = %self.kind,
            "applied local gateway configuration"
        );
        Ok(())
    }

    async fn post_gateway_config(
        &self,
        client: &reqwest::Client,
        path: &str,
        value: &serde_json::Value,
    ) -> Result<()> {
        let url = format!("{GATEWAY_ADMIN_URL}{path}");
        let response = client.post(&url).json(value).send().await?;
        if response.status().is_success() {
            return Ok(());
        }
        let status = response.status();
        let body = response
            .text()
            .await
            .unwrap_or_default()
            .chars()
            .take(512)
            .collect::<String>();
        bail!("Caddy Admin API {url} returned {status}: {body}")
    }

    async fn gateway_containers(&self) -> Result<Vec<ExistingGatewayContainer>> {
        let summaries = self.list_managed_summaries().await?;
        Ok(summaries
            .into_iter()
            .filter_map(|summary| {
                let labels = summary.labels.unwrap_or_default();
                if !is_gateway_system_container(&labels) {
                    return None;
                }
                Some(ExistingGatewayContainer {
                    id: summary.id?,
                    cluster_id: labels.get(CLUSTER_LABEL).cloned(),
                    advertise_address: labels.get(GATEWAY_ADDRESS_LABEL).cloned(),
                    image: labels.get(GATEWAY_IMAGE_LABEL).cloned(),
                    listen: labels.get(GATEWAY_LISTEN_LABEL).cloned(),
                    token_hash: labels.get(GATEWAY_TOKEN_HASH_LABEL).cloned(),
                    schema: labels.get(GATEWAY_SCHEMA_LABEL).cloned(),
                    running: summary.state == Some(ContainerSummaryStateEnum::RUNNING),
                })
            })
            .collect())
    }

    async fn create_gateway(&self, spec: &GatewayContainerSpec) -> Result<()> {
        self.ensure_image(&spec.image).await?;
        let bootstrap = gateway_bootstrap(spec)?;
        let ports = gateway_ports(&spec.listen)?;
        let mut port_bindings = HashMap::new();
        let mut exposed_ports = Vec::new();
        for port in ports {
            let key = format!("{port}/tcp");
            exposed_ports.push(key.clone());
            port_bindings.insert(
                key,
                Some(vec![DockerPortBinding {
                    host_ip: Some("0.0.0.0".to_owned()),
                    host_port: Some(port.to_string()),
                }]),
            );
            if port == 443 {
                let key = "443/udp".to_owned();
                exposed_ports.push(key.clone());
                port_bindings.insert(
                    key,
                    Some(vec![DockerPortBinding {
                        host_ip: Some("0.0.0.0".to_owned()),
                        host_port: Some("443".to_owned()),
                    }]),
                );
            }
        }
        let admin = "2019/tcp".to_owned();
        exposed_ports.push(admin.clone());
        port_bindings.insert(
            admin,
            Some(vec![DockerPortBinding {
                host_ip: Some("127.0.0.1".to_owned()),
                host_port: Some("2019".to_owned()),
            }]),
        );

        let [data_volume, config_volume] = gateway_volume_names(&spec.cluster_id);
        let host_config = HostConfig {
            binds: Some(vec![
                format!("{data_volume}:/data"),
                format!("{config_volume}:/config"),
            ]),
            port_bindings: Some(port_bindings),
            restart_policy: Some(RestartPolicy {
                name: Some(RestartPolicyNameEnum::UNLESS_STOPPED),
                maximum_retry_count: None,
            }),
            ..Default::default()
        };
        let labels = gateway_labels(spec);
        let body = ContainerCreateBody {
            image: Some(spec.image.clone()),
            entrypoint: Some(vec!["/bin/sh".to_owned(), "-ec".to_owned()]),
            cmd: Some(vec![
                "printf '%s' \"$SWARMLITE_CADDY_BOOTSTRAP\" > /config/bootstrap.json; exec caddy run --resume --config /config/bootstrap.json"
                    .to_owned(),
            ]),
            env: Some(vec![
                "XDG_CONFIG_HOME=/config".to_owned(),
                "XDG_DATA_HOME=/data".to_owned(),
                format!("SWARMLITE_TOKEN={}", spec.token),
                format!("SWARMLITE_CADDY_BOOTSTRAP={bootstrap}"),
            ]),
            exposed_ports: Some(exposed_ports),
            labels: Some(labels),
            stop_timeout: Some(10),
            host_config: Some(host_config),
            ..Default::default()
        };
        let options = CreateContainerOptionsBuilder::default()
            .name(GATEWAY_CONTAINER_NAME)
            .build();
        let created = self
            .client
            .create_container(Some(options), body)
            .await
            .with_context(|| {
                format!(
                    "failed to create gateway container {GATEWAY_CONTAINER_NAME}; remove any unrelated container using that name"
                )
            })?;
        self.client
            .start_container(&created.id, None)
            .await
            .context("failed to start the gateway container")?;
        info!(
            image = %spec.image,
            address = %spec.advertise_address,
            runtime = %self.kind,
            "started independent gateway container"
        );
        Ok(())
    }

    async fn remove_gateway(&self, container: &ExistingGatewayContainer) -> Result<()> {
        if container.running {
            let stop = StopContainerOptionsBuilder::default().t(10).build();
            if let Err(error) = self.client.stop_container(&container.id, Some(stop)).await {
                warn!(%error, "graceful gateway stop failed; forcing removal");
            }
        }
        let remove = RemoveContainerOptionsBuilder::default().force(true).build();
        self.client
            .remove_container(&container.id, Some(remove))
            .await
            .context("failed to remove the managed gateway container")?;
        info!(runtime = %self.kind, "removed gateway container");
        Ok(())
    }

    async fn remove_gateway_volumes(&self, cluster_id: &str) -> Result<()> {
        for volume in gateway_volume_names(cluster_id) {
            let options = RemoveVolumeOptionsBuilder::default().force(true).build();
            match self.client.remove_volume(&volume, Some(options)).await {
                Ok(())
                | Err(bollard::errors::Error::DockerResponseServerError {
                    status_code: 404, ..
                }) => {}
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("failed to remove gateway volume {volume}"));
                }
            }
        }
        info!(runtime = %self.kind, "removed gateway persistent volumes");
        Ok(())
    }

    async fn ensure_image(&self, image: &str) -> Result<()> {
        if self.client.inspect_image(image).await.is_ok() {
            return Ok(());
        }
        let options = CreateImageOptionsBuilder::default()
            .from_image(image)
            .build();
        let mut pull = self.client.create_image(Some(options), None, None);
        while let Some(item) = pull.next().await {
            item.with_context(|| format!("failed to pull {image}"))?;
        }
        Ok(())
    }
}

fn gateway_labels(spec: &GatewayContainerSpec) -> HashMap<String, String> {
    HashMap::from([
        (MANAGED_LABEL.to_owned(), "true".to_owned()),
        (CLUSTER_LABEL.to_owned(), spec.cluster_id.clone()),
        (SYSTEM_LABEL.to_owned(), "true".to_owned()),
        (COMPONENT_LABEL.to_owned(), GATEWAY_COMPONENT.to_owned()),
        (
            GATEWAY_ADDRESS_LABEL.to_owned(),
            spec.advertise_address.clone(),
        ),
        (GATEWAY_SCHEMA_LABEL.to_owned(), GATEWAY_SCHEMA.to_owned()),
        (GATEWAY_IMAGE_LABEL.to_owned(), spec.image.clone()),
        (GATEWAY_LISTEN_LABEL.to_owned(), spec.listen.join(",")),
        (
            GATEWAY_TOKEN_HASH_LABEL.to_owned(),
            gateway_token_hash(&spec.token),
        ),
    ])
}

fn gateway_token_hash(token: &str) -> String {
    format!("{:x}", Sha256::digest(token.as_bytes()))
}

fn gateway_volume_names(cluster_id: &str) -> [String; 2] {
    let prefix = format!("swarmlite-gateway-{cluster_id}");
    [format!("{prefix}-data"), format!("{prefix}-config")]
}

fn gateway_matches_spec(container: &ExistingGatewayContainer, spec: &GatewayContainerSpec) -> bool {
    container.advertise_address.as_deref() == Some(&spec.advertise_address)
        && container.image.as_deref() == Some(&spec.image)
        && container.listen.as_deref() == Some(&spec.listen.join(","))
        && container.token_hash.as_deref() == Some(&gateway_token_hash(&spec.token))
        && container.schema.as_deref() == Some(GATEWAY_SCHEMA)
}

fn gateway_ports(listen: &[String]) -> Result<BTreeSet<u16>> {
    listen
        .iter()
        .map(|address| {
            let port = address
                .rsplit_once(':')
                .map(|(_, port)| port)
                .unwrap_or_default()
                .parse::<u16>()
                .with_context(|| {
                    format!("gateway listen address {address:?} must end in a numeric TCP port")
                })?;
            if port == 2019 {
                bail!("gateway listen port 2019 is reserved for the Caddy admin API");
            }
            Ok(port)
        })
        .collect()
}

fn gateway_bootstrap(spec: &GatewayContainerSpec) -> Result<String> {
    Ok(serde_json::to_string(&gateway::config(
        &ClusterState::default(),
        &spec.listen,
        spec.controller.clone(),
    ))?)
}

fn is_gateway_system_container(labels: &HashMap<String, String>) -> bool {
    labels.get(SYSTEM_LABEL).map(String::as_str) == Some("true")
        && labels.get(COMPONENT_LABEL).map(String::as_str) == Some(GATEWAY_COMPONENT)
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
        self.ensure_image(&assignment.spec.image).await?;

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
    fn recognizes_gateway_system_container_labels() {
        let labels = HashMap::from([
            (MANAGED_LABEL.to_owned(), "true".to_owned()),
            (CLUSTER_LABEL.to_owned(), "cluster-old".to_owned()),
            (SYSTEM_LABEL.to_owned(), "true".to_owned()),
            (COMPONENT_LABEL.to_owned(), GATEWAY_COMPONENT.to_owned()),
        ]);
        assert!(is_gateway_system_container(&labels));

        let workload = HashMap::from([
            (MANAGED_LABEL.to_owned(), "true".to_owned()),
            (CLUSTER_LABEL.to_owned(), "cluster-old".to_owned()),
        ]);
        assert!(!is_gateway_system_container(&workload));
    }

    #[test]
    fn builds_gateway_recovery_labels() {
        let spec = GatewayContainerSpec {
            cluster_id: "cluster-old".into(),
            advertise_address: "10.0.0.21".into(),
            listen: vec![":80".into()],
            controller: "http://10.0.0.21:8080".into(),
            token: "0123456789abcdef".into(),
            image: DEFAULT_GATEWAY_IMAGE.into(),
        };
        let labels = gateway_labels(&spec);
        assert_eq!(labels[MANAGED_LABEL], "true");
        assert_eq!(labels[CLUSTER_LABEL], "cluster-old");
        assert_eq!(labels[SYSTEM_LABEL], "true");
        assert_eq!(labels[COMPONENT_LABEL], GATEWAY_COMPONENT);
        assert_eq!(labels[GATEWAY_ADDRESS_LABEL], "10.0.0.21");
        assert_eq!(labels[GATEWAY_SCHEMA_LABEL], GATEWAY_SCHEMA);
        assert_eq!(labels[GATEWAY_IMAGE_LABEL], DEFAULT_GATEWAY_IMAGE);
        assert_eq!(labels[GATEWAY_LISTEN_LABEL], ":80");
        assert_eq!(
            labels[GATEWAY_TOKEN_HASH_LABEL],
            gateway_token_hash("0123456789abcdef")
        );
        assert!(!labels.values().any(|value| value == "0123456789abcdef"));
        assert!(!labels.contains_key(TASK_LABEL));
    }

    #[test]
    fn replaces_a_gateway_when_the_cluster_image_changes() {
        let container = ExistingGatewayContainer {
            id: "gateway".into(),
            cluster_id: Some("cluster-old".into()),
            advertise_address: Some("10.0.0.21".into()),
            image: Some("custom-caddy:v1".into()),
            listen: Some(":80".into()),
            token_hash: Some(gateway_token_hash("0123456789abcdef")),
            schema: Some(GATEWAY_SCHEMA.into()),
            running: true,
        };
        let mut spec = GatewayContainerSpec {
            cluster_id: "cluster-old".into(),
            advertise_address: "10.0.0.21".into(),
            listen: vec![":80".into()],
            controller: "http://10.0.0.21:8080".into(),
            token: "0123456789abcdef".into(),
            image: DEFAULT_GATEWAY_IMAGE.into(),
        };
        assert!(!gateway_matches_spec(&container, &spec));
        spec.image = "custom-caddy:v1".into();
        assert!(gateway_matches_spec(&container, &spec));
    }

    #[test]
    fn maps_gateway_listeners_to_published_ports() {
        assert_eq!(
            gateway_ports(&[":80".into(), "0.0.0.0:443".into()]).unwrap(),
            BTreeSet::from([80, 443])
        );
        assert!(gateway_ports(&[":2019".into()]).is_err());
        assert!(gateway_ports(&["unix/socket".into()]).is_err());
    }

    #[test]
    fn scopes_gateway_volumes_to_the_cluster() {
        assert_eq!(
            gateway_volume_names("cluster-old"),
            [
                "swarmlite-gateway-cluster-old-data".to_owned(),
                "swarmlite-gateway-cluster-old-config".to_owned(),
            ]
        );
    }

    #[test]
    fn gateway_bootstrap_persists_admin_updates() {
        let spec = GatewayContainerSpec {
            cluster_id: "cluster-old".into(),
            advertise_address: "10.0.0.21".into(),
            listen: vec![":80".into()],
            controller: "http://10.0.0.21:8080".into(),
            token: "do-not-persist-this-token".into(),
            image: DEFAULT_GATEWAY_IMAGE.into(),
        };
        let encoded = gateway_bootstrap(&spec).unwrap();
        let value: serde_json::Value = serde_json::from_str(&encoded).unwrap();
        assert_eq!(value["admin"]["listen"], "0.0.0.0:2019");
        assert_eq!(value["admin"]["config"]["persist"], true);
        assert_eq!(value["storage"]["module"], "swarmlite");
        assert_eq!(value["storage"]["controller"], "http://10.0.0.21:8080");
        assert!(value["storage"].get("controllers").is_none());
        assert_eq!(value["storage"]["token_env"], "SWARMLITE_TOKEN");
        assert!(!encoded.contains("do-not-persist-this-token"));
        assert_eq!(
            value["apps"]["http"]["servers"]["swarmlite"]["listen"][0],
            ":80"
        );
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
            desired: crate::model::DesiredTaskState::Running,
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
            generation: 4,
            deployment_generation: 4,
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
