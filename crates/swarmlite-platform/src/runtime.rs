use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    future::Future,
    io::{Cursor, ErrorKind, Read},
    net::{Ipv4Addr, SocketAddrV4, TcpListener, UdpSocket},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, bail};
use bollard::{
    API_DEFAULT_VERSION, Docker,
    container::LogOutput,
    exec::{CreateExecOptions, StartExecResults},
    models::{
        ContainerCreateBody, ContainerSummaryStateEnum, HealthConfig, HealthStatusEnum, HostConfig,
        PortBinding as DockerPortBinding, RestartPolicy, RestartPolicyNameEnum,
    },
    query_parameters::{
        CreateContainerOptionsBuilder, CreateImageOptionsBuilder,
        DownloadFromContainerOptionsBuilder, ListContainersOptionsBuilder, LogsOptionsBuilder,
        RemoveContainerOptionsBuilder, RemoveVolumeOptionsBuilder, StopContainerOptionsBuilder,
        UploadToContainerOptionsBuilder,
    },
};
use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt};
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::{
    config::{ResolvedRuntimeConfig, RuntimeKind},
    config_files::config_mount_host_path,
    data_plane::MAX_DATA_PAYLOAD_BYTES,
    gateway,
    model::{
        ClusterGatewayConfig, ClusterState, DeploymentPolicy, GatewayAssignment,
        GatewayRecoverySnapshot, ImageResolutionStatus, ObservedTaskState, PortBinding, PullPolicy,
        TaskAssignment, TaskReconcilePhase,
    },
    registry::RegistryCredentialStore,
};

#[cfg(test)]
use crate::model::DEFAULT_GATEWAY_IMAGE;

pub const MANAGED_LABEL: &str = "io.swarmlite.managed";
pub const CLUSTER_LABEL: &str = "io.swarmlite.cluster_id";
pub const SYSTEM_LABEL: &str = "io.swarmlite.system";
pub const COMPONENT_LABEL: &str = "io.swarmlite.component";
pub const GATEWAY_COMPONENT: &str = "gateway";
pub const GATEWAY_ADDRESS_LABEL: &str = "io.swarmlite.advertise_address";
const GATEWAY_NODE_LABEL: &str = "io.swarmlite.node_id";
const GATEWAY_SCHEMA_LABEL: &str = "io.swarmlite.gateway_schema";
const GATEWAY_AUTOSAVE_SCHEMA_LABEL: &str = "io.swarmlite.gateway_autosave_schema";
const GATEWAY_IMAGE_LABEL: &str = "io.swarmlite.gateway_image";
const GATEWAY_LISTEN_LABEL: &str = "io.swarmlite.gateway_listen";
const GATEWAY_GRACE_PERIOD_LABEL: &str = "io.swarmlite.gateway_grace_period_seconds";
const GATEWAY_HTTP3_LABEL: &str = "io.swarmlite.gateway_http3_enabled";
const GATEWAY_TOKEN_HASH_LABEL: &str = "io.swarmlite.gateway_token_sha256";
const GATEWAY_SCHEMA: &str = "9";
const GATEWAY_AUTOSAVE_SCHEMA: &str = "2";
const GATEWAY_CONTAINER_NAME: &str = "swarmlite-gateway";
const GATEWAY_ADMIN_URL: &str = "http://127.0.0.1:2019";
const GATEWAY_RECOVERY_PATH: &str = "/config/swarmlite-recovery.json";
const GATEWAY_RECOVERY_TEMP_NAME: &str = ".swarmlite-recovery.json.tmp";
const MAX_GATEWAY_RECOVERY_SNAPSHOT_BYTES: usize = 16 * 1024 * 1024;
const TASK_LABEL: &str = "io.swarmlite.task_id";
const SERVICE_LABEL: &str = "io.swarmlite.service_id";
const STACK_LABEL: &str = "io.swarmlite.stack";
const SERVICE_NAME_LABEL: &str = "io.swarmlite.service";
const SLOT_LABEL: &str = "io.swarmlite.slot";
const SPEC_HASH_LABEL: &str = "io.swarmlite.spec_sha256";
const PORTS_LABEL: &str = "io.swarmlite.ports";
const REVISION_LABEL: &str = "io.swarmlite.revision";
const STOP_GRACE_LABEL: &str = "io.swarmlite.stop_grace_seconds";
const CONFIG_REFS_LABEL: &str = "io.swarmlite.config_refs";

#[derive(Debug, Clone, Copy)]
pub struct RuntimeSystemInfo {
    pub cpu_millis: u64,
    pub memory_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct ManagedContainer {
    pub id: String,
    pub image_id: Option<String>,
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
    pub config_digests: Vec<String>,
    pub config_cache_paths: Vec<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeLogChannel {
    Stdout,
    Stderr,
    Stdin,
    Console,
}

#[derive(Debug, Clone)]
pub struct RuntimeLogChunk {
    pub channel: RuntimeLogChannel,
    pub payload: Bytes,
}

#[derive(Debug, Default)]
pub struct ManagedClusterInventory {
    pub cluster_ids: BTreeSet<String>,
    pub gateway_cluster_ids: BTreeSet<String>,
    pub gateway_listen: BTreeMap<String, Vec<String>>,
    pub gateway_images: BTreeMap<String, String>,
    pub unlabeled: usize,
}

#[derive(Debug, Clone)]
pub struct GatewayContainerSpec {
    pub cluster_id: String,
    pub node_id: String,
    pub advertise_address: String,
    pub controller: String,
    pub token: String,
    pub gateway: ClusterGatewayConfig,
}

#[derive(Debug)]
struct ExistingGatewayContainer {
    id: String,
    cluster_id: Option<String>,
    node_id: Option<String>,
    advertise_address: Option<String>,
    image: Option<String>,
    listen: Option<String>,
    grace_period_seconds: Option<String>,
    http3_enabled: Option<String>,
    token_hash: Option<String>,
    schema: Option<String>,
    autosave_schema: Option<String>,
    running: bool,
}

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
struct NonRetryableGatewayError {
    message: String,
}

#[derive(Clone)]
pub struct RuntimeTaskProgress {
    callback: Arc<dyn Fn(RuntimeTaskProgressUpdate) + Send + Sync>,
}

#[derive(Debug, Clone, Copy)]
pub struct RuntimeTaskProgressUpdate {
    pub phase: TaskReconcilePhase,
    pub attempt: u32,
    pub current_bytes: Option<u64>,
    pub total_bytes: Option<u64>,
}

impl RuntimeTaskProgress {
    pub fn new(callback: impl Fn(RuntimeTaskProgressUpdate) + Send + Sync + 'static) -> Self {
        Self {
            callback: Arc::new(callback),
        }
    }

    pub fn report(&self, phase: TaskReconcilePhase) {
        (self.callback)(RuntimeTaskProgressUpdate {
            phase,
            attempt: 0,
            current_bytes: None,
            total_bytes: None,
        });
    }

    pub fn report_pull(&self, attempt: u32, current_bytes: Option<u64>, total_bytes: Option<u64>) {
        (self.callback)(RuntimeTaskProgressUpdate {
            phase: TaskReconcilePhase::Pull,
            attempt,
            current_bytes,
            total_bytes,
        });
    }
}

impl Default for RuntimeTaskProgress {
    fn default() -> Self {
        Self::new(|_| {})
    }
}

#[derive(Clone)]
pub struct RuntimeImageProgress {
    callback: Arc<dyn Fn(RuntimeImageProgressUpdate) + Send + Sync>,
}

#[derive(Debug, Clone, Copy)]
pub struct RuntimeImageProgressUpdate {
    pub status: ImageResolutionStatus,
    pub attempt: u32,
    pub current_bytes: Option<u64>,
    pub total_bytes: Option<u64>,
}

impl RuntimeImageProgress {
    pub fn new(callback: impl Fn(RuntimeImageProgressUpdate) + Send + Sync + 'static) -> Self {
        Self {
            callback: Arc::new(callback),
        }
    }

    pub fn report(&self, status: ImageResolutionStatus) {
        (self.callback)(RuntimeImageProgressUpdate {
            status,
            attempt: 0,
            current_bytes: None,
            total_bytes: None,
        });
    }

    pub fn report_pull(&self, attempt: u32, current_bytes: Option<u64>, total_bytes: Option<u64>) {
        (self.callback)(RuntimeImageProgressUpdate {
            status: ImageResolutionStatus::Pulling,
            attempt,
            current_bytes,
            total_bytes,
        });
    }
}

impl Default for RuntimeImageProgress {
    fn default() -> Self {
        Self::new(|_| {})
    }
}

pub trait ContainerRuntime: Send + Sync + 'static {
    fn kind(&self) -> RuntimeKind;

    fn socket(&self) -> &str;

    fn update_deployment_policy(&self, _policy: DeploymentPolicy) {}

    fn ping(&self) -> impl Future<Output = Result<()>> + Send;

    fn system_info(&self) -> impl Future<Output = Result<RuntimeSystemInfo>> + Send;

    fn list_managed(
        &self,
        cluster_id: &str,
    ) -> impl Future<Output = Result<HashMap<String, ManagedContainer>>> + Send;

    fn resolve_image(
        &self,
        image: &str,
        progress: &RuntimeImageProgress,
    ) -> impl Future<Output = Result<String>> + Send;

    fn create_task(
        &self,
        assignment: &TaskAssignment,
        progress: &RuntimeTaskProgress,
    ) -> impl Future<Output = Result<()>> + Send;

    fn start_task(
        &self,
        container: &ManagedContainer,
        progress: &RuntimeTaskProgress,
    ) -> impl Future<Output = Result<()>> + Send;

    fn remove_task(
        &self,
        container: &ManagedContainer,
        progress: &RuntimeTaskProgress,
    ) -> impl Future<Output = Result<()>> + Send;

    fn stream_task_logs(
        &self,
        container: &ManagedContainer,
        tail: u32,
        follow: bool,
        output: mpsc::Sender<RuntimeLogChunk>,
    ) -> impl Future<Output = Result<()>> + Send;
}

#[derive(Clone)]
pub struct DockerCompatibleRuntime {
    client: Docker,
    kind: RuntimeKind,
    socket: String,
    registry_credentials: Option<RegistryCredentialStore>,
    config_root: Option<PathBuf>,
    deployment_policy: Arc<std::sync::RwLock<DeploymentPolicy>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TaskNameConflictResolution {
    Recovered,
    RetryCreate,
}

impl DockerCompatibleRuntime {
    pub fn connect(config: &ResolvedRuntimeConfig) -> Result<Self> {
        Self::connect_inner(config, None, None, DeploymentPolicy::default())
    }

    pub fn connect_with_registry_credentials(
        config: &ResolvedRuntimeConfig,
        registry_credentials: RegistryCredentialStore,
        config_root: PathBuf,
        deployment_policy: DeploymentPolicy,
    ) -> Result<Self> {
        Self::connect_inner(
            config,
            Some(registry_credentials),
            Some(config_root),
            deployment_policy,
        )
    }

    fn connect_inner(
        config: &ResolvedRuntimeConfig,
        registry_credentials: Option<RegistryCredentialStore>,
        config_root: Option<PathBuf>,
        deployment_policy: DeploymentPolicy,
    ) -> Result<Self> {
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
            registry_credentials,
            config_root,
            deployment_policy: Arc::new(std::sync::RwLock::new(deployment_policy)),
        })
    }

    pub async fn managed_cluster_inventory(&self) -> Result<ManagedClusterInventory> {
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

    pub async fn reconcile_gateway(
        &self,
        spec: &GatewayContainerSpec,
        enabled: bool,
    ) -> Result<()> {
        let ports = enabled
            .then(|| {
                gateway_ports(&spec.gateway.listen).map_err(|error| NonRetryableGatewayError {
                    message: format!("invalid Gateway listener configuration: {error:#}"),
                })
            })
            .transpose()?;
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
                    ensure_gateway_ports_available(
                        ports.as_ref().expect("enabled Gateway ports"),
                        spec.gateway.http.http3_enabled.unwrap_or(true),
                    )?;
                    self.client
                        .start_container(&existing.id, None)
                        .await
                        .context("failed to start the managed gateway container")?;
                    info!(runtime = %self.kind, "started existing gateway container");
                }
                return Ok(());
            }
            self.ensure_image_if_missing(&spec.gateway.image).await?;
            info!(
                previous_address = ?existing.advertise_address,
                address = %spec.advertise_address,
                "recreating gateway container for the current gateway settings"
            );
            self.remove_gateway(&existing).await?;
            ensure_gateway_ports_available(
                ports.as_ref().expect("enabled Gateway ports"),
                spec.gateway.http.http3_enabled.unwrap_or(true),
            )?;
            return self.create_gateway(spec).await;
        }

        ensure_gateway_ports_available(
            ports.as_ref().expect("enabled Gateway ports"),
            spec.gateway.http.http3_enabled.unwrap_or(true),
        )?;
        self.create_gateway(spec).await
    }

    pub async fn gateway_image(&self) -> Result<Option<String>> {
        let gateways = self.gateway_containers().await?;
        if gateways.len() > 1 {
            bail!("found multiple managed Gateway containers on this node");
        }
        Ok(gateways
            .into_iter()
            .next()
            .and_then(|gateway| gateway.image))
    }

    pub async fn apply_gateway_config(&self, assignment: &GatewayAssignment) -> Result<()> {
        if assignment.recovery_snapshot.generation != assignment.generation {
            bail!(
                "Gateway assignment generation {} does not match recovery snapshot generation {}",
                assignment.generation,
                assignment.recovery_snapshot.generation
            );
        }
        let gateways = self.gateway_containers().await?;
        let gateway = match gateways.as_slice() {
            [gateway] => gateway,
            [] => bail!("managed Gateway container is missing"),
            _ => bail!("found multiple managed Gateway containers on this node"),
        };
        let cluster_id = gateway
            .cluster_id
            .as_deref()
            .context("managed Gateway container has no cluster identity")?;
        assignment
            .recovery_snapshot
            .validate_for_cluster(cluster_id)
            .map_err(anyhow::Error::msg)?;
        let client = reqwest::Client::new();
        self.post_gateway_config(&client, "/load", &assignment.config)
            .await?;
        self.persist_gateway_recovery_snapshot(&assignment.recovery_snapshot)
            .await?;
        info!(
            generation = assignment.generation,
            runtime = %self.kind,
            "applied local gateway configuration"
        );
        Ok(())
    }

    pub async fn gateway_recovery_snapshots(
        &self,
        cluster_id: &str,
    ) -> Result<Vec<GatewayRecoverySnapshot>> {
        let gateways = self.gateway_containers().await?;
        let mut snapshots = Vec::new();
        for gateway in gateways
            .iter()
            .filter(|gateway| gateway.cluster_id.as_deref() == Some(cluster_id))
        {
            match self.read_gateway_recovery_snapshot(&gateway.id).await {
                Ok(snapshot) => snapshots.push(snapshot),
                Err(error) => warn!(
                    container = %gateway.id,
                    %error,
                    "ignoring invalid or missing Gateway recovery snapshot"
                ),
            }
        }
        Ok(snapshots)
    }

    async fn read_gateway_recovery_snapshot(
        &self,
        container_id: &str,
    ) -> Result<GatewayRecoverySnapshot> {
        let options = DownloadFromContainerOptionsBuilder::default()
            .path(GATEWAY_RECOVERY_PATH)
            .build();
        let chunks = self
            .client
            .download_from_container(container_id, Some(options))
            .try_collect::<Vec<_>>()
            .await
            .context("failed to download the Gateway recovery snapshot")?;
        let archive_size = chunks.iter().map(Bytes::len).sum::<usize>();
        if archive_size > MAX_GATEWAY_RECOVERY_SNAPSHOT_BYTES + 1024 * 1024 {
            bail!("Gateway recovery snapshot archive is too large");
        }
        let mut archive_bytes = Vec::with_capacity(archive_size);
        for chunk in chunks {
            archive_bytes.extend_from_slice(&chunk);
        }
        let mut archive = tar::Archive::new(Cursor::new(archive_bytes));
        let mut entries = archive
            .entries()
            .context("invalid Gateway recovery snapshot archive")?;
        let mut entry = entries
            .next()
            .transpose()
            .context("invalid Gateway recovery snapshot entry")?
            .context("Gateway recovery snapshot archive is empty")?;
        if entry.size() > MAX_GATEWAY_RECOVERY_SNAPSHOT_BYTES as u64 {
            bail!("Gateway recovery snapshot is too large");
        }
        let mut payload = Vec::with_capacity(entry.size() as usize);
        entry
            .read_to_end(&mut payload)
            .context("failed to read Gateway recovery snapshot")?;
        serde_json::from_slice(&payload).context("invalid Gateway recovery snapshot JSON")
    }

    async fn persist_gateway_recovery_snapshot(
        &self,
        snapshot: &GatewayRecoverySnapshot,
    ) -> Result<()> {
        let payload = serde_json::to_vec(snapshot)?;
        if payload.len() > MAX_GATEWAY_RECOVERY_SNAPSHOT_BYTES {
            bail!("Gateway recovery snapshot is too large");
        }
        let archive = gateway_recovery_snapshot_archive(&payload)?;
        let gateway = self
            .gateway_containers()
            .await?
            .into_iter()
            .find(|gateway| gateway.cluster_id.as_deref() == Some(&snapshot.cluster_id))
            .context("managed Gateway container disappeared after Caddy accepted its config")?;
        let options = UploadToContainerOptionsBuilder::default()
            .path("/config")
            .build();
        self.client
            .upload_to_container(
                &gateway.id,
                Some(options),
                bollard::body_full(Bytes::from(archive)),
            )
            .await
            .context("failed to upload temporary Gateway recovery snapshot")?;

        let command = format!(
            "sync; mv -f /config/{GATEWAY_RECOVERY_TEMP_NAME} {GATEWAY_RECOVERY_PATH}; sync"
        );
        let exec = self
            .client
            .create_exec(
                &gateway.id,
                CreateExecOptions {
                    attach_stdout: Some(true),
                    attach_stderr: Some(true),
                    cmd: Some(vec!["/bin/sh".to_owned(), "-ec".to_owned(), command]),
                    ..Default::default()
                },
            )
            .await
            .context("failed to prepare atomic Gateway recovery snapshot replacement")?;
        let mut output_text = String::new();
        if let StartExecResults::Attached { mut output, .. } =
            self.client.start_exec(&exec.id, None).await?
        {
            while let Some(output) = output.next().await {
                output_text.push_str(&output?.to_string());
            }
        }
        let result = self.client.inspect_exec(&exec.id).await?;
        if result.exit_code != Some(0) {
            bail!(
                "atomic Gateway recovery snapshot replacement failed with exit code {:?}: {}",
                result.exit_code,
                output_text.trim()
            );
        }
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
                    node_id: labels.get(GATEWAY_NODE_LABEL).cloned(),
                    advertise_address: labels.get(GATEWAY_ADDRESS_LABEL).cloned(),
                    image: labels.get(GATEWAY_IMAGE_LABEL).cloned(),
                    listen: labels.get(GATEWAY_LISTEN_LABEL).cloned(),
                    grace_period_seconds: labels.get(GATEWAY_GRACE_PERIOD_LABEL).cloned(),
                    http3_enabled: labels.get(GATEWAY_HTTP3_LABEL).cloned(),
                    token_hash: labels.get(GATEWAY_TOKEN_HASH_LABEL).cloned(),
                    schema: labels.get(GATEWAY_SCHEMA_LABEL).cloned(),
                    autosave_schema: labels.get(GATEWAY_AUTOSAVE_SCHEMA_LABEL).cloned(),
                    running: summary.state == Some(ContainerSummaryStateEnum::RUNNING),
                })
            })
            .collect())
    }

    async fn create_gateway(&self, spec: &GatewayContainerSpec) -> Result<()> {
        self.ensure_image_if_missing(&spec.gateway.image).await?;
        let bootstrap = gateway_bootstrap(spec)?;
        let ports = gateway_ports(&spec.gateway.listen)?;
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
            if port == 443 && spec.gateway.http.http3_enabled.unwrap_or(true) {
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

        let [data_volume, config_volume, cache_volume] = gateway_volume_names(&spec.cluster_id);
        let host_config = HostConfig {
            binds: Some(vec![
                format!("{data_volume}:/data"),
                format!("{config_volume}:/config"),
                format!("{cache_volume}:/cache"),
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
            image: Some(spec.gateway.image.clone()),
            entrypoint: Some(vec!["/bin/sh".to_owned(), "-ec".to_owned()]),
            cmd: Some(vec![
                "printf '%s' \"$SWARMLITE_CADDY_BOOTSTRAP\" > /config/bootstrap.json; exec caddy run --resume --config /config/bootstrap.json"
                    .to_owned(),
            ]),
            env: Some(vec![
                format!("XDG_CONFIG_HOME={}", gateway_autosave_config_home()),
                "XDG_DATA_HOME=/data".to_owned(),
                format!("SWARMLITE_TOKEN={}", spec.token),
                format!("SWARMLITE_GATEWAY_ID={}", spec.node_id),
                format!("SWARMLITE_CADDY_BOOTSTRAP={bootstrap}"),
            ]),
            exposed_ports: Some(exposed_ports),
            labels: Some(labels),
            stop_timeout: Some(i64::from(gateway_stop_timeout(
                spec.gateway.shutdown.grace_period_seconds,
            ))),
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
        if let Err(error) = self.client.start_container(&created.id, None).await {
            let start_error =
                anyhow::Error::new(error).context("failed to start the gateway container");
            if let Err(cleanup_error) = self
                .remove_new_container_after_failed_start(&created.id, "gateway")
                .await
            {
                return Err(start_error.context(format!(
                    "also failed to remove the newly created gateway container: {cleanup_error:#}"
                )));
            }
            return Err(start_error);
        }
        info!(
            image = %spec.gateway.image,
            address = %spec.advertise_address,
            runtime = %self.kind,
            "started independent gateway container"
        );
        Ok(())
    }

    async fn remove_new_container_after_failed_start(
        &self,
        container_id: &str,
        kind: &str,
    ) -> Result<()> {
        let remove = RemoveContainerOptionsBuilder::default().force(true).build();
        match self
            .client
            .remove_container(container_id, Some(remove))
            .await
        {
            Ok(())
            | Err(bollard::errors::Error::DockerResponseServerError {
                status_code: 404, ..
            }) => {}
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "failed to remove newly created {kind} container {container_id} after it failed to start"
                    )
                });
            }
        }
        info!(
            container_id,
            kind, "removed newly created container after start failure"
        );
        Ok(())
    }

    async fn recover_task_name_conflict(
        &self,
        name: &str,
        assignment: &TaskAssignment,
        progress: &RuntimeTaskProgress,
    ) -> Result<TaskNameConflictResolution> {
        let inspect = {
            let mut inspect = None;
            for attempt in 0..3 {
                match self.client.inspect_container(name, None).await {
                    Ok(found) => {
                        inspect = Some(found);
                        break;
                    }
                    Err(error) if docker_not_found(&error) && attempt < 2 => {
                        tokio::time::sleep(std::time::Duration::from_millis(
                            10 * (attempt + 1) as u64,
                        ))
                        .await;
                    }
                    Err(error) if docker_not_found(&error) => {
                        return Ok(TaskNameConflictResolution::RetryCreate);
                    }
                    Err(error) => {
                        return Err(error).with_context(|| {
                            format!(
                                "failed to inspect conflicting task container {name} for {}",
                                assignment.id
                            )
                        });
                    }
                }
            }
            inspect.expect("task conflict inspection either succeeds or returns")
        };
        let labels = inspect
            .config
            .as_ref()
            .and_then(|config| config.labels.as_ref())
            .cloned()
            .unwrap_or_default();
        let managed = labels.get(MANAGED_LABEL).map(String::as_str) == Some("true");
        let cluster_id = labels.get(CLUSTER_LABEL).map(String::as_str);
        let task_id = labels.get(TASK_LABEL).map(String::as_str);
        if !managed
            || cluster_id != Some(assignment.cluster_id.as_str())
            || task_id != Some(assignment.id.as_str())
        {
            bail!(
                "task container name {name:?} is already owned by managed={managed}, cluster_id={cluster_id:?}, task_id={task_id:?}; refusing to replace it for task {}",
                assignment.id
            );
        }

        let container_id = inspect.id.as_deref().unwrap_or(name);
        let same_spec = labels.get(SPEC_HASH_LABEL).map(String::as_str)
            == Some(assignment.spec_hash.as_str())
            && labels
                .get(REVISION_LABEL)
                .and_then(|revision| revision.parse::<u64>().ok())
                == Some(assignment.revision);
        if !same_spec {
            let remove = RemoveContainerOptionsBuilder::default().force(true).build();
            match self
                .client
                .remove_container(container_id, Some(remove))
                .await
            {
                Ok(()) => {}
                Err(error) if docker_not_found(&error) => {}
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!(
                            "failed to remove stale task container {container_id} for {}",
                            assignment.id
                        )
                    });
                }
            }
            warn!(
                task_id = %assignment.id,
                container_id,
                "removed a stale same-task container after a name conflict"
            );
            return Ok(TaskNameConflictResolution::RetryCreate);
        }

        if inspect
            .state
            .as_ref()
            .is_some_and(|state| state.running == Some(true))
        {
            info!(
                task_id = %assignment.id,
                container_id,
                "adopted an already running task container after a name conflict"
            );
            return Ok(TaskNameConflictResolution::Recovered);
        }

        progress.report(TaskReconcilePhase::Start);
        match self.client.start_container(container_id, None).await {
            Ok(()) => {
                info!(
                    task_id = %assignment.id,
                    container_id,
                    "started an existing task container after a name conflict"
                );
                Ok(TaskNameConflictResolution::Recovered)
            }
            Err(error) if docker_already_running(&error) => {
                info!(
                    task_id = %assignment.id,
                    container_id,
                    "adopted an already running task container after a name conflict"
                );
                Ok(TaskNameConflictResolution::Recovered)
            }
            Err(error) if docker_not_found(&error) => Ok(TaskNameConflictResolution::RetryCreate),
            Err(error) => {
                let port_conflict = docker_port_conflict(&error);
                let start_error = anyhow::Error::new(error).context(format!(
                    "failed to start conflicting task container for {}",
                    assignment.id
                ));
                if let Err(cleanup_error) = self
                    .remove_new_container_after_failed_start(container_id, "task")
                    .await
                {
                    return Err(start_error.context(format!(
                        "also failed to remove the conflicting task container: {cleanup_error:#}"
                    )));
                }
                if port_conflict {
                    Ok(TaskNameConflictResolution::RetryCreate)
                } else {
                    Err(start_error)
                }
            }
        }
    }

    async fn remove_gateway(&self, container: &ExistingGatewayContainer) -> Result<()> {
        if container.running {
            let stop = StopContainerOptionsBuilder::default()
                .t(gateway_stop_timeout(
                    container
                        .grace_period_seconds
                        .as_deref()
                        .and_then(|value| value.parse().ok()),
                ))
                .build();
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

    async fn ensure_image(
        &self,
        image: &str,
        pull_policy: PullPolicy,
        progress: &RuntimeTaskProgress,
    ) -> Result<()> {
        match pull_policy {
            PullPolicy::Never => {
                return self
                    .client
                    .inspect_image(image)
                    .await
                    .map(|_| ())
                    .with_context(|| {
                        format!("pull_policy=never requires image {image} in the local cache")
                    });
            }
            PullPolicy::Missing if !pull_policy.refreshes_cached_image(image) => {
                if self.client.inspect_image(image).await.is_ok() {
                    return Ok(());
                }
            }
            PullPolicy::Always | PullPolicy::Missing => {}
        }
        self.pull_image(image, |attempt, current, total| {
            progress.report_pull(attempt, current, total);
        })
        .await
    }

    async fn ensure_image_if_missing(&self, image: &str) -> Result<()> {
        if self.client.inspect_image(image).await.is_ok() {
            return Ok(());
        }
        self.pull_image(image, |_, _, _| {}).await
    }

    async fn pull_image(
        &self,
        image: &str,
        mut report: impl FnMut(u32, Option<u64>, Option<u64>),
    ) -> Result<()> {
        let policy = self
            .deployment_policy
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let attempts = policy.image_pull_max_attempts.max(1);
        let mut backoff_seconds = policy.image_pull_initial_backoff_seconds;
        let maximum_backoff_seconds = policy.image_pull_max_backoff_seconds;
        let idle_timeout =
            std::time::Duration::from_secs(policy.image_pull_idle_timeout_seconds.max(1));
        let mut last_error = None;
        let mut attempted = 0;

        for attempt in 1..=attempts {
            attempted = attempt;
            report(attempt, None, None);
            match self
                .pull_image_once(image, attempt, idle_timeout, &mut report)
                .await
            {
                Ok(()) => return Ok(()),
                Err(error) => {
                    let retryable = image_pull_error_is_retryable(&error);
                    last_error = Some(error);
                    if attempt == attempts || !retryable {
                        break;
                    }
                    warn!(
                        image,
                        attempt, attempts, backoff_seconds, "image pull attempt failed; retrying"
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(backoff_seconds)).await;
                    backoff_seconds = backoff_seconds
                        .saturating_mul(2)
                        .min(maximum_backoff_seconds);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("failed to pull {image}")))
            .with_context(|| format!("failed to pull {image} after {attempted} attempt(s)"))
    }

    async fn pull_image_once(
        &self,
        image: &str,
        attempt: u32,
        idle_timeout: std::time::Duration,
        report: &mut impl FnMut(u32, Option<u64>, Option<u64>),
    ) -> Result<()> {
        let credentials = self
            .registry_credentials
            .as_ref()
            .map(|store| store.credentials_for_image(image))
            .transpose()?
            .flatten();
        let options = CreateImageOptionsBuilder::default()
            .from_image(image)
            .build();
        let mut pull = self.client.create_image(Some(options), None, credentials);
        let mut last_progress_at = std::time::Instant::now();
        let mut layer_progress = HashMap::<String, u64>::new();
        loop {
            let remaining = idle_timeout.saturating_sub(last_progress_at.elapsed());
            if remaining.is_zero() {
                bail!(
                    "image pull for {image} made no progress for {} seconds",
                    idle_timeout.as_secs()
                );
            }
            let item = tokio::time::timeout(remaining, pull.next())
                .await
                .with_context(|| {
                    format!(
                        "image pull for {image} made no progress for {} seconds",
                        idle_timeout.as_secs()
                    )
                })?;
            let Some(item) = item else {
                break;
            };
            let item = item.with_context(|| format!("failed to pull {image}"))?;
            if let Some(message) = item.error_detail.and_then(|detail| detail.message) {
                bail!("registry rejected image pull for {image}: {message}");
            }
            let layer = item.id.unwrap_or_default();
            let (current, total) = item.progress_detail.map_or((None, None), |detail| {
                (
                    detail.current.and_then(|value| u64::try_from(value).ok()),
                    detail.total.and_then(|value| u64::try_from(value).ok()),
                )
            });
            if let Some(current) = current {
                let previous = layer_progress.entry(layer).or_default();
                if current > *previous {
                    *previous = current;
                    last_progress_at = std::time::Instant::now();
                }
            }
            report(attempt, current, total);
        }
        Ok(())
    }

    async fn inspect_image_id(&self, image: &str) -> Result<String> {
        self.client
            .inspect_image(image)
            .await
            .with_context(|| format!("failed to inspect image {image}"))?
            .id
            .filter(|id| !id.is_empty())
            .ok_or_else(|| anyhow::anyhow!("runtime did not report an image ID for {image}"))
    }
}

fn image_pull_error_is_retryable(error: &anyhow::Error) -> bool {
    let message = format!("{error:#}").to_ascii_lowercase();
    ![
        "unauthorized",
        "authentication required",
        "denied",
        "forbidden",
        "no basic auth credentials",
        "manifest unknown",
        "not found",
        "invalid reference format",
    ]
    .iter()
    .any(|needle| message.contains(needle))
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
        (GATEWAY_NODE_LABEL.to_owned(), spec.node_id.clone()),
        (GATEWAY_SCHEMA_LABEL.to_owned(), GATEWAY_SCHEMA.to_owned()),
        (
            GATEWAY_AUTOSAVE_SCHEMA_LABEL.to_owned(),
            GATEWAY_AUTOSAVE_SCHEMA.to_owned(),
        ),
        (GATEWAY_IMAGE_LABEL.to_owned(), spec.gateway.image.clone()),
        (
            GATEWAY_LISTEN_LABEL.to_owned(),
            spec.gateway.listen.join(","),
        ),
        (
            GATEWAY_GRACE_PERIOD_LABEL.to_owned(),
            optional_label(spec.gateway.shutdown.grace_period_seconds),
        ),
        (
            GATEWAY_HTTP3_LABEL.to_owned(),
            optional_label(spec.gateway.http.http3_enabled),
        ),
        (
            GATEWAY_TOKEN_HASH_LABEL.to_owned(),
            gateway_token_hash(&spec.token),
        ),
    ])
}

fn gateway_token_hash(token: &str) -> String {
    format!("{:x}", Sha256::digest(token.as_bytes()))
}

fn optional_label<T: std::fmt::Display>(value: Option<T>) -> String {
    value.map_or_else(|| "unset".to_owned(), |value| value.to_string())
}

fn gateway_autosave_config_home() -> String {
    format!("/config/autosave-v{GATEWAY_AUTOSAVE_SCHEMA}")
}

fn gateway_stop_timeout(grace_period_seconds: Option<u64>) -> i32 {
    match grace_period_seconds {
        None => 10,
        Some(0) => -1,
        Some(seconds) => i32::try_from(seconds.saturating_add(5)).unwrap_or(i32::MAX),
    }
}

fn gateway_volume_names(cluster_id: &str) -> [String; 3] {
    let prefix = format!("swarmlite-gateway-{cluster_id}");
    [
        format!("{prefix}-data"),
        format!("{prefix}-config"),
        format!("{prefix}-cache"),
    ]
}

fn gateway_recovery_snapshot_archive(payload: &[u8]) -> Result<Vec<u8>> {
    let mut archive = tar::Builder::new(Vec::new());
    let mut header = tar::Header::new_gnu();
    header.set_path(GATEWAY_RECOVERY_TEMP_NAME)?;
    header.set_size(payload.len() as u64);
    header.set_mode(0o600);
    header.set_mtime(0);
    header.set_cksum();
    archive.append(&header, payload)?;
    archive.finish()?;
    archive.into_inner().map_err(Into::into)
}

fn gateway_matches_spec(container: &ExistingGatewayContainer, spec: &GatewayContainerSpec) -> bool {
    container.node_id.as_deref() == Some(&spec.node_id)
        && container.advertise_address.as_deref() == Some(&spec.advertise_address)
        && container.image.as_deref() == Some(&spec.gateway.image)
        && container.listen.as_deref() == Some(&spec.gateway.listen.join(","))
        && container.grace_period_seconds.as_deref()
            == Some(&optional_label(spec.gateway.shutdown.grace_period_seconds))
        && container.http3_enabled.as_deref()
            == Some(&optional_label(spec.gateway.http.http3_enabled))
        && container.token_hash.as_deref() == Some(&gateway_token_hash(&spec.token))
        && container.schema.as_deref() == Some(GATEWAY_SCHEMA)
        && container.autosave_schema.as_deref() == Some(GATEWAY_AUTOSAVE_SCHEMA)
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

fn ensure_gateway_ports_available(ports: &BTreeSet<u16>, http3_enabled: bool) -> Result<()> {
    for &port in ports {
        let address = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port);
        check_gateway_port(
            TcpListener::bind(address),
            format!("{address}/tcp"),
            "disable Gateway on this node, free the port, or change gateway-listen",
        )?;
        if port == 443 && http3_enabled {
            check_gateway_port(
                UdpSocket::bind(address),
                format!("{address}/udp"),
                "disable Gateway on this node, free the port, or change gateway-listen",
            )?;
        }
    }
    let admin = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 2019);
    check_gateway_port(
        TcpListener::bind(admin),
        format!("{admin}/tcp"),
        "free the local Caddy admin port or disable Gateway on this node",
    )?;
    Ok(())
}

fn check_gateway_port<T>(
    result: std::io::Result<T>,
    endpoint: String,
    recovery: &str,
) -> Result<()> {
    match result {
        Ok(bound) => {
            drop(bound);
            Ok(())
        }
        Err(error) if error.kind() == ErrorKind::AddrInUse => Err(NonRetryableGatewayError {
            message: format!("Gateway port conflict: cannot bind {endpoint} ({error}); {recovery}"),
        }
        .into()),
        Err(error) => {
            warn!(
                %endpoint,
                %error,
                "could not preflight a Gateway port; deferring the authoritative check to the container runtime"
            );
            Ok(())
        }
    }
}

pub fn gateway_error_is_retryable(error: &anyhow::Error) -> bool {
    error.downcast_ref::<NonRetryableGatewayError>().is_none() && !is_host_port_conflict(error)
}

fn gateway_bootstrap(spec: &GatewayContainerSpec) -> Result<String> {
    Ok(serde_json::to_string(&gateway::config(
        &ClusterState::default(),
        &spec.gateway,
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

    fn update_deployment_policy(&self, policy: DeploymentPolicy) {
        *self
            .deployment_policy
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = policy;
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
            let inspect = match self.client.inspect_container(&id, None).await {
                Ok(inspect) => inspect,
                Err(error) if docker_not_found(&error) => {
                    debug!(
                        container_id = %id,
                        task_id,
                        "managed container disappeared while building inventory"
                    );
                    continue;
                }
                Err(error) => return Err(error.into()),
            };
            let running = inspect
                .state
                .as_ref()
                .is_some_and(|state| state.running == Some(true));
            let observed = inspect
                .state
                .clone()
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
                .map(|ports| resolved_container_ports(&inspect, ports))
                .unwrap_or_default();
            let config_mounts = labels
                .get(CONFIG_REFS_LABEL)
                .and_then(|value| {
                    serde_json::from_str::<Vec<crate::model::ServiceConfigMount>>(value).ok()
                })
                .unwrap_or_default();
            let mut config_digests = config_mounts
                .iter()
                .map(|config| config.digest.clone())
                .collect::<BTreeSet<_>>();
            let mut config_cache_paths = BTreeSet::new();
            if let Some(config_root) = self.config_root.as_deref() {
                config_cache_paths.extend(
                    config_mounts
                        .iter()
                        .map(|config| config_mount_host_path(config_root, config)),
                );
                let mounts_root = config_root.join("mounts");
                for mount in inspect.mounts.as_deref().unwrap_or_default() {
                    let Some(source) = mount.source.as_deref() else {
                        continue;
                    };
                    let source = PathBuf::from(source);
                    if !source.starts_with(&mounts_root) {
                        continue;
                    }
                    if let Some(digest) = config_digest_from_cache_path(&source) {
                        config_digests.insert(digest);
                    }
                    config_cache_paths.insert(source);
                }
            }
            result.insert(
                task_id.clone(),
                ManagedContainer {
                    id,
                    image_id: inspect.image.clone(),
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
                    config_digests: config_digests.into_iter().collect(),
                    config_cache_paths: config_cache_paths.into_iter().collect(),
                },
            );
        }
        Ok(result)
    }

    async fn resolve_image(&self, image: &str, progress: &RuntimeImageProgress) -> Result<String> {
        progress.report(ImageResolutionStatus::Checking);
        progress.report(ImageResolutionStatus::Pulling);
        self.pull_image(image, |attempt, current, total| {
            progress.report_pull(attempt, current, total);
        })
        .await?;
        progress.report(ImageResolutionStatus::Comparing);
        self.inspect_image_id(image).await
    }

    async fn create_task(
        &self,
        assignment: &TaskAssignment,
        progress: &RuntimeTaskProgress,
    ) -> Result<()> {
        info!(
            task_id = %assignment.id,
            image = %assignment.spec.image,
            runtime = %self.kind,
            "creating task container"
        );
        if assignment.image_resolved {
            progress.report(TaskReconcilePhase::Inspect);
            self.inspect_image_id(&assignment.spec.image).await?;
        } else {
            progress.report(TaskReconcilePhase::Pull);
            self.ensure_image(
                &assignment.spec.image,
                assignment.spec.pull_policy,
                progress,
            )
            .await?;
        }

        let port_bindings = task_port_bindings(assignment);
        let exposed_ports = assignment
            .spec
            .expose
            .iter()
            .map(|port| format!("{}/{}", port.target, port.protocol))
            .chain(
                assignment
                    .ports
                    .iter()
                    .map(|port| format!("{}/{}", port.target, port.protocol)),
            )
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let labels = task_labels(assignment)?;
        let binds = task_binds(assignment, self.config_root.as_deref())?;
        let host_config = HostConfig {
            binds: (!binds.is_empty()).then_some(binds),
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
        for attempt in 0..3 {
            progress.report(TaskReconcilePhase::Create);
            let created = match self
                .client
                .create_container(Some(create_options.clone()), body.clone())
                .await
            {
                Ok(created) => created,
                Err(error) if attempt < 2 && docker_port_conflict(&error) => {
                    warn!(task_id = %assignment.id, %error, "Docker port allocation raced; retrying task creation");
                    continue;
                }
                Err(error) if docker_name_conflict(&error) => {
                    match self
                        .recover_task_name_conflict(&name, assignment, progress)
                        .await?
                    {
                        TaskNameConflictResolution::Recovered => return Ok(()),
                        TaskNameConflictResolution::RetryCreate if attempt < 2 => continue,
                        TaskNameConflictResolution::RetryCreate => {
                            return Err(error).with_context(|| {
                                format!(
                                    "failed to recover conflicting task container for {}",
                                    assignment.id
                                )
                            });
                        }
                    }
                }
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("failed to create task {}", assignment.id));
                }
            };
            progress.report(TaskReconcilePhase::Start);
            match self.client.start_container(&created.id, None).await {
                Ok(()) => return Ok(()),
                Err(error) => {
                    let port_conflict = docker_port_conflict(&error);
                    let start_error = anyhow::Error::new(error)
                        .context(format!("failed to start task {}", assignment.id));
                    if let Err(cleanup_error) = self
                        .remove_new_container_after_failed_start(&created.id, "task")
                        .await
                    {
                        return Err(start_error.context(format!(
                            "also failed to remove the newly created task container: {cleanup_error:#}"
                        )));
                    }
                    if attempt < 2 && port_conflict {
                        warn!(task_id = %assignment.id, %start_error, "Docker port allocation raced; recreating task container");
                        continue;
                    }
                    return Err(start_error);
                }
            }
        }
        unreachable!("task creation retry loop always returns")
    }

    async fn remove_task(
        &self,
        container: &ManagedContainer,
        progress: &RuntimeTaskProgress,
    ) -> Result<()> {
        info!(
            task_id = %container.task_id,
            runtime = %self.kind,
            "removing obsolete task container"
        );
        let stop = StopContainerOptionsBuilder::default()
            .t(container.stop_grace_seconds)
            .build();
        progress.report(TaskReconcilePhase::Stop);
        if let Err(error) = self.client.stop_container(&container.id, Some(stop)).await {
            warn!(task_id = %container.task_id, %error, "graceful stop failed; forcing removal");
        }
        progress.report(TaskReconcilePhase::Remove);
        let remove = RemoveContainerOptionsBuilder::default().force(true).build();
        match self
            .client
            .remove_container(&container.id, Some(remove))
            .await
        {
            Ok(()) => {}
            Err(error) if docker_not_found(&error) => {}
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to remove task {}", container.task_id));
            }
        }
        Ok(())
    }

    async fn start_task(
        &self,
        container: &ManagedContainer,
        progress: &RuntimeTaskProgress,
    ) -> Result<()> {
        info!(
            task_id = %container.task_id,
            runtime = %self.kind,
            "starting recovered task container"
        );
        progress.report(TaskReconcilePhase::Start);
        match self.client.start_container(&container.id, None).await {
            Ok(()) => Ok(()),
            Err(error) if docker_already_running(&error) => Ok(()),
            Err(error) => Err(error)
                .with_context(|| format!("failed to start recovered task {}", container.task_id)),
        }
    }

    async fn stream_task_logs(
        &self,
        container: &ManagedContainer,
        tail: u32,
        follow: bool,
        output: mpsc::Sender<RuntimeLogChunk>,
    ) -> Result<()> {
        let options = LogsOptionsBuilder::default()
            .follow(follow)
            .stdout(true)
            .stderr(true)
            .tail(&tail.to_string())
            .build();
        let mut stream = self.client.logs(&container.id, Some(options));
        while let Some(chunk) = stream.next().await {
            let chunk = chunk
                .with_context(|| format!("failed to read logs for task {}", container.task_id))?;
            let channel = match &chunk {
                LogOutput::StdOut { .. } => RuntimeLogChannel::Stdout,
                LogOutput::StdErr { .. } => RuntimeLogChannel::Stderr,
                LogOutput::StdIn { .. } => RuntimeLogChannel::Stdin,
                LogOutput::Console { .. } => RuntimeLogChannel::Console,
            };
            if !send_runtime_log_chunks(&output, channel, chunk.into_bytes()).await {
                return Ok(());
            }
        }
        Ok(())
    }
}

fn task_binds(assignment: &TaskAssignment, config_root: Option<&Path>) -> Result<Vec<String>> {
    let mut binds = assignment.spec.volumes.clone();
    if assignment.spec.configs.is_empty() {
        return Ok(binds);
    }
    let config_root = config_root
        .context("container runtime was not configured with a local Config cache for this Agent")?;
    for config in &assignment.spec.configs {
        let host_path = config_mount_host_path(config_root, config);
        let host_path = host_path
            .to_str()
            .with_context(|| format!("config cache path is not UTF-8: {}", host_path.display()))?;
        binds.push(format!("{host_path}:{}:ro", config.target));
    }
    Ok(binds)
}

fn resolved_container_ports(
    inspect: &bollard::models::ContainerInspectResponse,
    expected: Vec<PortBinding>,
) -> Vec<PortBinding> {
    let actual = inspect
        .network_settings
        .as_ref()
        .and_then(|settings| settings.ports.as_ref());
    expected
        .into_iter()
        .map(|mut port| {
            let key = format!("{}/{}", port.target, port.protocol);
            if let Some(published) = actual
                .and_then(|ports| ports.get(&key))
                .and_then(Option::as_ref)
                .and_then(|bindings| {
                    bindings
                        .iter()
                        .find_map(|binding| binding.host_port.as_ref())
                })
                .and_then(|port| port.parse().ok())
            {
                port.published = Some(published);
            }
            port
        })
        .collect()
}

fn task_port_bindings(
    assignment: &TaskAssignment,
) -> HashMap<String, Option<Vec<DockerPortBinding>>> {
    assignment
        .ports
        .iter()
        .map(|port| {
            let published = assignment
                .spec
                .ports
                .iter()
                .find(|requested| {
                    requested.target == port.target && requested.protocol == port.protocol
                })
                .and_then(|requested| requested.published);
            (
                format!("{}/{}", port.target, port.protocol),
                Some(vec![DockerPortBinding {
                    host_ip: Some("0.0.0.0".to_owned()),
                    host_port: Some(published.map_or_else(String::new, |port| port.to_string())),
                }]),
            )
        })
        .collect()
}

fn docker_port_conflict(error: &bollard::errors::Error) -> bool {
    matches!(
        error,
        bollard::errors::Error::DockerResponseServerError {
            status_code: 500,
            message,
        } if message.contains("port is already allocated")
            || message.contains("address already in use")
    )
}

fn docker_name_conflict(error: &bollard::errors::Error) -> bool {
    matches!(
        error,
        bollard::errors::Error::DockerResponseServerError {
            status_code: 409,
            message,
        } if message.contains("container name") && message.contains("already in use")
    )
}

fn docker_not_found(error: &bollard::errors::Error) -> bool {
    matches!(
        error,
        bollard::errors::Error::DockerResponseServerError {
            status_code: 404,
            ..
        }
    )
}

fn docker_already_running(error: &bollard::errors::Error) -> bool {
    matches!(
        error,
        bollard::errors::Error::DockerResponseServerError {
            status_code: 304,
            ..
        }
    )
}

pub fn is_host_port_conflict(error: &anyhow::Error) -> bool {
    error.chain().any(|source| {
        source
            .downcast_ref::<bollard::errors::Error>()
            .is_some_and(docker_port_conflict)
    })
}

async fn send_runtime_log_chunks(
    output: &mpsc::Sender<RuntimeLogChunk>,
    channel: RuntimeLogChannel,
    payload: Bytes,
) -> bool {
    for payload in payload.chunks(MAX_DATA_PAYLOAD_BYTES) {
        if output
            .send(RuntimeLogChunk {
                channel,
                payload: Bytes::copy_from_slice(payload),
            })
            .await
            .is_err()
        {
            return false;
        }
    }
    true
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
    if !assignment.spec.configs.is_empty() {
        labels.insert(
            CONFIG_REFS_LABEL.to_owned(),
            serde_json::to_string(&assignment.spec.configs)?,
        );
    }
    Ok(labels)
}

fn config_digest_from_cache_path(path: &Path) -> Option<String> {
    let name = path.file_name()?.to_str()?;
    let digest = name.get(..64)?;
    (digest.bytes().all(|byte| byte.is_ascii_hexdigit())).then(|| digest.to_ascii_lowercase())
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
    use std::{
        collections::BTreeMap,
        convert::Infallible,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use axum::{
        Router,
        body::Body,
        extract::State,
        http::{Method, StatusCode, Uri},
        routing::any,
    };

    use crate::model::{ServiceConfigMount, ServicePort, ServiceSpec};

    use super::*;

    fn test_gateway_spec() -> GatewayContainerSpec {
        GatewayContainerSpec {
            cluster_id: "cluster-old".into(),
            node_id: "node-a".into(),
            advertise_address: "10.0.0.21".into(),
            controller: "http://10.0.0.21:17080".into(),
            token: "0123456789abcdef".into(),
            gateway: ClusterGatewayConfig {
                listen: vec![":80".into()],
                image: DEFAULT_GATEWAY_IMAGE.into(),
                ..Default::default()
            },
        }
    }

    #[derive(Clone)]
    struct TaskNameConflictApiState {
        calls: Arc<Mutex<Vec<String>>>,
        owner_task_id: String,
    }

    async fn task_name_conflict_docker_api(
        State(state): State<TaskNameConflictApiState>,
        method: Method,
        uri: Uri,
    ) -> axum::response::Response {
        state
            .calls
            .lock()
            .unwrap()
            .push(format!("{method} {}", uri.path()));
        let path = uri.path();
        let (status, body) = if method == Method::GET && path.contains("/images/") {
            (StatusCode::OK, r#"{"Id":"sha256:test-image"}"#.into())
        } else if method == Method::POST && path.ends_with("/containers/create") {
            (
                StatusCode::CONFLICT,
                r#"{"message":"Conflict. The container name is already in use"}"#.into(),
            )
        } else if method == Method::GET
            && path.contains("/containers/swarmlite-demo.web-0-task-sta/json")
        {
            (
                StatusCode::OK,
                serde_json::json!({
                    "Id": "existing-container",
                    "Name": "/swarmlite-demo.web-0-task-sta",
                    "Config": {
                        "Labels": {
                            MANAGED_LABEL: "true",
                            CLUSTER_LABEL: "cluster-old",
                            TASK_LABEL: state.owner_task_id,
                            SPEC_HASH_LABEL: "hash",
                            REVISION_LABEL: "1"
                        }
                    },
                    "State": {"Running": false, "Status": "created"}
                })
                .to_string(),
            )
        } else if method == Method::POST && path.ends_with("/containers/existing-container/start") {
            (StatusCode::NO_CONTENT, String::new())
        } else {
            (
                StatusCode::NOT_FOUND,
                r#"{"message":"unexpected request"}"#.into(),
            )
        };
        axum::response::Response::builder()
            .status(status)
            .header(axum::http::header::CONTENT_TYPE, "application/json")
            .body(Body::from(body))
            .unwrap()
    }

    async fn disappearing_inventory_docker_api(
        method: Method,
        uri: Uri,
    ) -> axum::response::Response {
        let path = uri.path();
        let (status, body) = if method == Method::GET && path.ends_with("/containers/json") {
            (
                StatusCode::OK,
                serde_json::json!([
                    {
                        "Id": "gone-container",
                        "Names": ["/gone"],
                        "Image": "nginx:alpine",
                        "ImageID": "sha256:test-image",
                        "Command": "nginx",
                        "Created": 1,
                        "Ports": [],
                        "Labels": {
                            MANAGED_LABEL: "true",
                            CLUSTER_LABEL: "cluster-old",
                            TASK_LABEL: "task-gone"
                        },
                        "State": "exited",
                        "Status": "Exited"
                    },
                    {
                        "Id": "live-container",
                        "Names": ["/live"],
                        "Image": "nginx:alpine",
                        "ImageID": "sha256:test-image",
                        "Command": "nginx",
                        "Created": 1,
                        "Ports": [],
                        "Labels": {
                            MANAGED_LABEL: "true",
                            CLUSTER_LABEL: "cluster-old",
                            TASK_LABEL: "task-live",
                            REVISION_LABEL: "1",
                            SPEC_HASH_LABEL: "hash"
                        },
                        "State": "running",
                        "Status": "Up"
                    }
                ])
                .to_string(),
            )
        } else if method == Method::GET && path.ends_with("/containers/gone-container/json") {
            (
                StatusCode::NOT_FOUND,
                r#"{"message":"No such container"}"#.into(),
            )
        } else if method == Method::GET && path.ends_with("/containers/live-container/json") {
            (
                StatusCode::OK,
                serde_json::json!({
                    "Id": "live-container",
                    "Image": "sha256:test-image",
                    "State": {"Running": true, "Status": "running"},
                    "Mounts": []
                })
                .to_string(),
            )
        } else {
            (
                StatusCode::NOT_FOUND,
                r#"{"message":"unexpected request"}"#.into(),
            )
        };
        axum::response::Response::builder()
            .status(status)
            .header(axum::http::header::CONTENT_TYPE, "application/json")
            .body(Body::from(body))
            .unwrap()
    }

    async fn start_failure_docker_api(
        State(calls): State<Arc<Mutex<Vec<String>>>>,
        method: Method,
        uri: Uri,
    ) -> axum::response::Response {
        calls
            .lock()
            .unwrap()
            .push(format!("{method} {}", uri.path()));
        let path = uri.path();
        let (status, body) = if method == Method::GET && path.contains("/images/") {
            (StatusCode::OK, r#"{"Id":"sha256:test-image"}"#)
        } else if method == Method::POST && path.ends_with("/containers/create") {
            (
                StatusCode::CREATED,
                r#"{"Id":"created-container","Warnings":[]}"#,
            )
        } else if method == Method::POST && path.ends_with("/containers/created-container/start") {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                r#"{"message":"port is already allocated"}"#,
            )
        } else if method == Method::DELETE && path.ends_with("/containers/created-container") {
            (StatusCode::NO_CONTENT, "")
        } else {
            (StatusCode::NOT_FOUND, r#"{"message":"unexpected request"}"#)
        };
        axum::response::Response::builder()
            .status(status)
            .header(axum::http::header::CONTENT_TYPE, "application/json")
            .body(Body::from(body))
            .unwrap()
    }

    async fn retrying_pull_docker_api(
        State(calls): State<Arc<AtomicUsize>>,
        method: Method,
        uri: Uri,
    ) -> axum::response::Response {
        if method != Method::POST || !uri.path().ends_with("/images/create") {
            return axum::response::Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from(r#"{"message":"unexpected request"}"#))
                .unwrap();
        }
        let attempt = calls.fetch_add(1, Ordering::Relaxed);
        let body = if attempt == 0 {
            r#"{"errorDetail":{"message":"temporary registry failure"}}
"#
        } else {
            r#"{"status":"downloaded","progressDetail":{"current":64,"total":64}}
"#
        };
        axum::response::Response::builder()
            .status(StatusCode::OK)
            .header(axum::http::header::CONTENT_TYPE, "application/json")
            .body(Body::from(body))
            .unwrap()
    }

    async fn denied_pull_docker_api(
        State(calls): State<Arc<AtomicUsize>>,
        method: Method,
        uri: Uri,
    ) -> axum::response::Response {
        assert_eq!(method, Method::POST);
        assert!(uri.path().ends_with("/images/create"));
        calls.fetch_add(1, Ordering::Relaxed);
        axum::response::Response::builder()
            .status(StatusCode::OK)
            .header(axum::http::header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"errorDetail":{"message":"pull access denied"}}
"#,
            ))
            .unwrap()
    }

    async fn stalled_pull_docker_api(method: Method, uri: Uri) -> axum::response::Response {
        if method != Method::POST || !uri.path().ends_with("/images/create") {
            return axum::response::Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::empty())
                .unwrap();
        }
        let pending = futures_util::stream::pending::<Result<Bytes, Infallible>>();
        axum::response::Response::builder()
            .status(StatusCode::OK)
            .header(axum::http::header::CONTENT_TYPE, "application/json")
            .body(Body::from_stream(pending))
            .unwrap()
    }

    async fn noisy_stalled_pull_docker_api(method: Method, uri: Uri) -> axum::response::Response {
        if method != Method::POST || !uri.path().ends_with("/images/create") {
            return axum::response::Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::empty())
                .unwrap();
        }
        let retrying = futures_util::stream::unfold((), |()| async {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            Some((
                Ok::<_, Infallible>(Bytes::from_static(
                    b"{\"status\":\"Retrying in 1 second\"}\n",
                )),
                (),
            ))
        });
        axum::response::Response::builder()
            .status(StatusCode::OK)
            .header(axum::http::header::CONTENT_TYPE, "application/json")
            .body(Body::from_stream(retrying))
            .unwrap()
    }

    async fn runtime_for_pull_api(
        app: Router,
        policy: DeploymentPolicy,
    ) -> (DockerCompatibleRuntime, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let endpoint = format!("http://{address}");
        let client = Docker::connect_with_http(&endpoint, 5, API_DEFAULT_VERSION).unwrap();
        (
            DockerCompatibleRuntime {
                client,
                kind: RuntimeKind::Docker,
                socket: endpoint,
                registry_credentials: None,
                config_root: None,
                deployment_policy: Arc::new(std::sync::RwLock::new(policy)),
            },
            server,
        )
    }

    async fn runtime_with_start_failure_api() -> (
        DockerCompatibleRuntime,
        Arc<Mutex<Vec<String>>>,
        tokio::task::JoinHandle<()>,
    ) {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let app = Router::new()
            .fallback(any(start_failure_docker_api))
            .with_state(calls.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let endpoint = format!("http://{address}");
        let client = Docker::connect_with_http(&endpoint, 5, API_DEFAULT_VERSION).unwrap();
        (
            DockerCompatibleRuntime {
                client,
                kind: RuntimeKind::Docker,
                socket: endpoint,
                registry_credentials: None,
                config_root: None,
                deployment_policy: Arc::new(std::sync::RwLock::new(DeploymentPolicy::default())),
            },
            calls,
            server,
        )
    }

    fn start_failure_assignment() -> TaskAssignment {
        TaskAssignment {
            id: "task-start-failure".into(),
            cluster_id: "cluster-old".into(),
            stack: "demo".into(),
            service: "web".into(),
            service_id: "demo.web".into(),
            revision: 1,
            slot: 0,
            desired: crate::model::DesiredTaskState::Running,
            spec: ServiceSpec {
                image: "nginx:alpine".into(),
                pull_policy: Default::default(),
                command: Vec::new(),
                entrypoint: Vec::new(),
                environment: Vec::new(),
                expose: Vec::new(),
                ports: Vec::new(),
                volumes: Vec::new(),
                configs: Vec::new(),
                container_labels: BTreeMap::new(),
                service_labels: BTreeMap::new(),
                healthcheck: None,
                replicas: 1,
                constraints: Vec::new(),
                max_replicas_per_node: None,
                max_surge: 1,
                stop_grace_period_seconds: 10,
            },
            ports: Vec::new(),
            generation: 1,
            deployment_generation: 1,
            deployment_retry_revision: 0,
            spec_hash: "hash".into(),
            image_resolved: true,
        }
    }

    #[test]
    fn sanitizes_runtime_container_names() {
        assert_eq!(sanitize_name("demo/web:v1"), "demo-web-v1");
    }

    #[tokio::test]
    async fn adopts_the_same_task_container_after_a_create_name_conflict() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let app = Router::new()
            .fallback(any(task_name_conflict_docker_api))
            .with_state(TaskNameConflictApiState {
                calls: Arc::clone(&calls),
                owner_task_id: "task-start-failure".into(),
            });
        let (runtime, server) = runtime_for_pull_api(app, DeploymentPolicy::default()).await;

        runtime
            .create_task(&start_failure_assignment(), &RuntimeTaskProgress::default())
            .await
            .unwrap();
        server.abort();

        let calls = calls.lock().unwrap();
        assert_eq!(
            calls
                .iter()
                .filter(|call| call.ends_with("/containers/create"))
                .count(),
            1
        );
        assert_eq!(
            calls
                .iter()
                .filter(|call| call.ends_with("/containers/existing-container/start"))
                .count(),
            1
        );
        assert!(!calls.iter().any(|call| call.starts_with("DELETE ")));
    }

    #[tokio::test]
    async fn refuses_to_adopt_a_name_conflict_owned_by_another_task() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let app = Router::new()
            .fallback(any(task_name_conflict_docker_api))
            .with_state(TaskNameConflictApiState {
                calls: Arc::clone(&calls),
                owner_task_id: "another-task".into(),
            });
        let (runtime, server) = runtime_for_pull_api(app, DeploymentPolicy::default()).await;

        let error = runtime
            .create_task(&start_failure_assignment(), &RuntimeTaskProgress::default())
            .await
            .unwrap_err();
        server.abort();

        assert!(format!("{error:#}").contains("refusing to replace it"));
        let calls = calls.lock().unwrap();
        assert!(!calls.iter().any(|call| call.starts_with("DELETE ")));
        assert!(!calls.iter().any(|call| call.ends_with("/start")));
    }

    #[tokio::test]
    async fn skips_a_container_that_disappears_during_managed_inventory() {
        let app = Router::new().fallback(any(disappearing_inventory_docker_api));
        let (runtime, server) = runtime_for_pull_api(app, DeploymentPolicy::default()).await;

        let inventory = runtime.list_managed("cluster-old").await.unwrap();
        server.abort();

        assert_eq!(inventory.len(), 1);
        assert!(inventory.contains_key("task-live"));
        assert!(!inventory.contains_key("task-gone"));
    }

    #[tokio::test]
    async fn image_pull_retries_registry_stream_errors_and_reports_attempts() {
        let calls = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .fallback(any(retrying_pull_docker_api))
            .with_state(Arc::clone(&calls));
        let policy = DeploymentPolicy {
            image_pull_max_attempts: 2,
            image_pull_initial_backoff_seconds: 0,
            image_pull_max_backoff_seconds: 0,
            ..DeploymentPolicy::default()
        };
        let (runtime, server) = runtime_for_pull_api(app, policy).await;
        let reports = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&reports);
        runtime
            .pull_image(
                "example.invalid/demo:latest",
                move |attempt, current, total| {
                    captured.lock().unwrap().push((attempt, current, total));
                },
            )
            .await
            .unwrap();
        server.abort();

        assert_eq!(calls.load(Ordering::Relaxed), 2);
        let reports = reports.lock().unwrap();
        assert!(reports.iter().any(|report| report.0 == 1));
        assert!(
            reports
                .iter()
                .any(|report| report == &(2, Some(64), Some(64)))
        );
    }

    #[tokio::test]
    async fn image_pull_fails_when_the_stream_exceeds_its_idle_deadline() {
        let app = Router::new().fallback(any(stalled_pull_docker_api));
        let policy = DeploymentPolicy {
            image_pull_idle_timeout_seconds: 1,
            image_pull_max_attempts: 1,
            ..DeploymentPolicy::default()
        };
        let (runtime, server) = runtime_for_pull_api(app, policy).await;
        let error = runtime
            .pull_image("example.invalid/demo:latest", |_, _, _| {})
            .await
            .unwrap_err();
        server.abort();

        assert!(format!("{error:#}").contains("made no progress for 1 seconds"));
    }

    #[tokio::test]
    async fn image_pull_status_messages_do_not_reset_the_idle_deadline() {
        let app = Router::new().fallback(any(noisy_stalled_pull_docker_api));
        let policy = DeploymentPolicy {
            image_pull_idle_timeout_seconds: 1,
            image_pull_max_attempts: 1,
            ..DeploymentPolicy::default()
        };
        let (runtime, server) = runtime_for_pull_api(app, policy).await;
        let error = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            runtime.pull_image("example.invalid/demo:latest", |_, _, _| {}),
        )
        .await
        .expect("pull should respect its idle deadline")
        .unwrap_err();
        server.abort();

        assert!(format!("{error:#}").contains("made no progress for 1 seconds"));
    }

    #[tokio::test]
    async fn image_pull_does_not_retry_permanent_registry_rejections() {
        let calls = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .fallback(any(denied_pull_docker_api))
            .with_state(Arc::clone(&calls));
        let policy = DeploymentPolicy {
            image_pull_max_attempts: 5,
            image_pull_initial_backoff_seconds: 0,
            image_pull_max_backoff_seconds: 0,
            ..DeploymentPolicy::default()
        };
        let (runtime, server) = runtime_for_pull_api(app, policy).await;
        let error = runtime
            .pull_image("example.invalid/private:latest", |_, _, _| {})
            .await
            .unwrap_err();
        server.abort();

        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert!(format!("{error:#}").contains("after 1 attempt"));
    }

    #[tokio::test]
    async fn removes_every_new_task_container_when_start_fails() {
        let (runtime, calls, server) = runtime_with_start_failure_api().await;
        let error = runtime
            .create_task(&start_failure_assignment(), &RuntimeTaskProgress::default())
            .await
            .unwrap_err();
        server.abort();

        assert!(format!("{error:#}").contains("failed to start task task-start-failure"));
        let calls = calls.lock().unwrap();
        assert_eq!(
            calls
                .iter()
                .filter(|call| call.starts_with("POST ") && call.ends_with("/containers/create"))
                .count(),
            3
        );
        assert_eq!(
            calls
                .iter()
                .filter(|call| call.starts_with("DELETE "))
                .count(),
            3,
            "the final failed attempt must be cleaned up too: {calls:?}"
        );
    }

    #[tokio::test]
    async fn removes_a_new_gateway_container_when_start_fails() {
        let (runtime, calls, server) = runtime_with_start_failure_api().await;
        let error = runtime
            .create_gateway(&test_gateway_spec())
            .await
            .unwrap_err();
        server.abort();

        assert!(format!("{error:#}").contains("failed to start the gateway container"));
        assert!(!gateway_error_is_retryable(&error));
        let calls = calls.lock().unwrap();
        assert_eq!(
            calls
                .iter()
                .filter(|call| call.starts_with("DELETE "))
                .count(),
            1,
            "the failed gateway container must be cleaned up: {calls:?}"
        );
    }

    #[tokio::test]
    async fn runtime_log_queue_receives_bounded_chunks_without_data_loss() {
        let payload = (0..(MAX_DATA_PAYLOAD_BYTES * 2 + 17))
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let (sender, mut receiver) = mpsc::channel(4);

        assert!(
            send_runtime_log_chunks(
                &sender,
                RuntimeLogChannel::Stdout,
                Bytes::from(payload.clone()),
            )
            .await
        );
        drop(sender);

        let mut reconstructed = Vec::new();
        let mut lengths = Vec::new();
        while let Some(chunk) = receiver.recv().await {
            assert_eq!(chunk.channel, RuntimeLogChannel::Stdout);
            lengths.push(chunk.payload.len());
            reconstructed.extend_from_slice(&chunk.payload);
        }
        assert_eq!(
            lengths,
            vec![MAX_DATA_PAYLOAD_BYTES, MAX_DATA_PAYLOAD_BYTES, 17]
        );
        assert_eq!(reconstructed, payload);
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
        let spec = test_gateway_spec();
        let labels = gateway_labels(&spec);
        assert_eq!(labels[MANAGED_LABEL], "true");
        assert_eq!(labels[CLUSTER_LABEL], "cluster-old");
        assert_eq!(labels[SYSTEM_LABEL], "true");
        assert_eq!(labels[COMPONENT_LABEL], GATEWAY_COMPONENT);
        assert_eq!(labels[GATEWAY_ADDRESS_LABEL], "10.0.0.21");
        assert_eq!(labels[GATEWAY_NODE_LABEL], "node-a");
        assert_eq!(labels[GATEWAY_SCHEMA_LABEL], GATEWAY_SCHEMA);
        assert_eq!(
            labels[GATEWAY_AUTOSAVE_SCHEMA_LABEL],
            GATEWAY_AUTOSAVE_SCHEMA
        );
        assert_eq!(labels[GATEWAY_IMAGE_LABEL], DEFAULT_GATEWAY_IMAGE);
        assert_eq!(labels[GATEWAY_LISTEN_LABEL], ":80");
        assert_eq!(labels[GATEWAY_GRACE_PERIOD_LABEL], "unset");
        assert_eq!(labels[GATEWAY_HTTP3_LABEL], "unset");
        assert_eq!(
            labels[GATEWAY_TOKEN_HASH_LABEL],
            gateway_token_hash("0123456789abcdef")
        );
        assert!(!labels.values().any(|value| value == "0123456789abcdef"));
        assert!(!labels.contains_key(TASK_LABEL));
    }

    #[test]
    fn replaces_a_gateway_when_the_cluster_image_changes() {
        let mut container = ExistingGatewayContainer {
            id: "gateway".into(),
            cluster_id: Some("cluster-old".into()),
            node_id: Some("node-a".into()),
            advertise_address: Some("10.0.0.21".into()),
            image: Some("custom-caddy:v1".into()),
            listen: Some(":80".into()),
            grace_period_seconds: Some("unset".into()),
            http3_enabled: Some("unset".into()),
            token_hash: Some(gateway_token_hash("0123456789abcdef")),
            schema: Some(GATEWAY_SCHEMA.into()),
            autosave_schema: Some(GATEWAY_AUTOSAVE_SCHEMA.into()),
            running: true,
        };
        let mut spec = test_gateway_spec();
        assert!(!gateway_matches_spec(&container, &spec));
        spec.gateway.image = "custom-caddy:v1".into();
        assert!(gateway_matches_spec(&container, &spec));
        container.autosave_schema = None;
        assert!(!gateway_matches_spec(&container, &spec));
        container.autosave_schema = Some(GATEWAY_AUTOSAVE_SCHEMA.into());
        container.node_id = None;
        assert!(!gateway_matches_spec(&container, &spec));
    }

    #[test]
    fn isolates_caddy_autosaves_by_compatibility_schema() {
        assert_eq!(gateway_autosave_config_home(), "/config/autosave-v2");
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
    fn coordinates_gateway_stop_timeout_with_caddy_grace_period() {
        assert_eq!(gateway_stop_timeout(None), 10);
        assert_eq!(gateway_stop_timeout(Some(0)), -1);
        assert_eq!(gateway_stop_timeout(Some(20)), 25);
    }

    #[test]
    fn occupied_gateway_port_is_detected_before_docker_container_creation() {
        let occupied = TcpListener::bind((Ipv4Addr::UNSPECIFIED, 0)).unwrap();
        let port = occupied.local_addr().unwrap().port();

        let error = ensure_gateway_ports_available(&BTreeSet::from([port]), true).unwrap_err();

        assert!(format!("{error:#}").contains(&format!("0.0.0.0:{port}/tcp")));
        assert!(!gateway_error_is_retryable(&error));
    }

    #[test]
    fn scopes_gateway_volumes_to_the_cluster() {
        assert_eq!(
            gateway_volume_names("cluster-old"),
            [
                "swarmlite-gateway-cluster-old-data".to_owned(),
                "swarmlite-gateway-cluster-old-config".to_owned(),
                "swarmlite-gateway-cluster-old-cache".to_owned(),
            ]
        );
    }

    #[test]
    fn gateway_recovery_archive_contains_only_the_atomic_temporary_file() {
        let payload =
            br#"{"format_version":1,"cluster_id":"cluster-a","generation":9,"stacks":{}}"#;
        let encoded = gateway_recovery_snapshot_archive(payload).unwrap();
        let mut archive = tar::Archive::new(std::io::Cursor::new(encoded));
        let mut entries = archive.entries().unwrap();
        let mut entry = entries.next().unwrap().unwrap();
        assert_eq!(
            entry.path().unwrap().as_ref(),
            std::path::Path::new(GATEWAY_RECOVERY_TEMP_NAME)
        );
        let mut restored = Vec::new();
        std::io::Read::read_to_end(&mut entry, &mut restored).unwrap();
        assert_eq!(restored, payload);
        drop(entry);
        assert!(entries.next().is_none());
    }

    #[test]
    fn gateway_bootstrap_persists_admin_updates() {
        let mut spec = test_gateway_spec();
        spec.token = "do-not-persist-this-token".into();
        let encoded = gateway_bootstrap(&spec).unwrap();
        let value: serde_json::Value = serde_json::from_str(&encoded).unwrap();
        assert_eq!(value["admin"]["listen"], "0.0.0.0:2019");
        assert_eq!(value["admin"]["config"]["persist"], true);
        assert_eq!(value["storage"]["module"], "swarmlite");
        assert_eq!(value["storage"]["controller"], "http://10.0.0.21:17080");
        assert!(value["storage"].get("controllers").is_none());
        assert_eq!(value["storage"]["token_env"], "SWARMLITE_TOKEN");
        assert_eq!(value["storage"]["gateway_id_env"], "SWARMLITE_GATEWAY_ID");
        assert_eq!(value["storage"]["probe_timeout"], "2s");
        assert_eq!(value["storage"]["owner_cache_ttl"], "1m");
        assert!(value["apps"].get("cache").is_none());
        assert!(!encoded.contains("do-not-persist-this-token"));
        assert_eq!(
            value["apps"]["http"]["servers"]["swarmlite"]["listen"][0],
            ":80"
        );
        let probe = &value["apps"]["http"]["servers"]["swarmlite"]["routes"][0];
        assert_eq!(probe["@id"], "swarmlite-gateway-owner-probe");
        assert_eq!(probe["handle"][0]["handler"], "swarmlite_gateway_probe");
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
                pull_policy: Default::default(),
                command: Vec::new(),
                entrypoint: Vec::new(),
                environment: Vec::new(),
                expose: Vec::new(),
                ports: Vec::new(),
                volumes: Vec::new(),
                configs: Vec::new(),
                container_labels: BTreeMap::from([(CLUSTER_LABEL.to_owned(), "user-value".into())]),
                service_labels: BTreeMap::new(),
                healthcheck: None,
                replicas: 1,
                constraints: Vec::new(),
                max_replicas_per_node: None,
                max_surge: 1,
                stop_grace_period_seconds: 10,
            },
            ports: Vec::new(),
            generation: 4,
            deployment_generation: 4,
            deployment_retry_revision: 0,
            spec_hash: "abc123".into(),
            image_resolved: false,
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

    #[test]
    fn mounts_cached_stack_configs_read_only_alongside_volumes() {
        let digest = "a".repeat(64);
        let assignment = TaskAssignment {
            id: "task-1".into(),
            cluster_id: "cluster-old".into(),
            stack: "demo".into(),
            service: "web".into(),
            service_id: "demo.web".into(),
            revision: 1,
            slot: 0,
            desired: crate::model::DesiredTaskState::Running,
            spec: ServiceSpec {
                image: "nginx:alpine".into(),
                pull_policy: Default::default(),
                command: Vec::new(),
                entrypoint: Vec::new(),
                environment: Vec::new(),
                expose: Vec::new(),
                ports: Vec::new(),
                volumes: vec!["data:/data".into()],
                configs: vec![ServiceConfigMount {
                    source: "nginx-config".into(),
                    target: "/etc/nginx/conf.d/default.conf".into(),
                    uid: None,
                    gid: None,
                    mode: 0o444,
                    digest,
                }],
                container_labels: BTreeMap::new(),
                service_labels: BTreeMap::new(),
                healthcheck: None,
                replicas: 1,
                constraints: Vec::new(),
                max_replicas_per_node: None,
                max_surge: 1,
                stop_grace_period_seconds: 10,
            },
            ports: Vec::new(),
            generation: 1,
            deployment_generation: 1,
            deployment_retry_revision: 0,
            spec_hash: "hash".into(),
            image_resolved: false,
        };
        let root = Path::new("/var/lib/swarmlite/configs");

        let binds = task_binds(&assignment, Some(root)).unwrap();

        assert_eq!(binds[0], "data:/data");
        assert_eq!(
            binds[1],
            format!(
                "{}:/etc/nginx/conf.d/default.conf:ro",
                config_mount_host_path(root, &assignment.spec.configs[0]).display()
            )
        );
        let labels = task_labels(&assignment).unwrap();
        let persisted: Vec<ServiceConfigMount> =
            serde_json::from_str(&labels[CONFIG_REFS_LABEL]).unwrap();
        assert_eq!(persisted, assignment.spec.configs);
        assert!(task_binds(&assignment, None).is_err());

        let mut without_config = assignment;
        without_config.spec.configs.clear();
        assert_eq!(
            task_binds(&without_config, Some(root)).unwrap(),
            vec!["data:/data"]
        );
        assert!(
            !task_labels(&without_config)
                .unwrap()
                .contains_key(CONFIG_REFS_LABEL)
        );
    }

    #[test]
    fn lets_docker_allocate_and_then_reads_the_published_port() {
        let assignment = TaskAssignment {
            id: "task-1".into(),
            cluster_id: "cluster-old".into(),
            stack: "demo".into(),
            service: "web".into(),
            service_id: "demo.web".into(),
            revision: 1,
            slot: 0,
            desired: crate::model::DesiredTaskState::Running,
            spec: ServiceSpec {
                image: "nginx:alpine".into(),
                pull_policy: Default::default(),
                command: Vec::new(),
                entrypoint: Vec::new(),
                environment: Vec::new(),
                expose: Vec::new(),
                ports: vec![ServicePort {
                    target: 80,
                    published: None,
                    protocol: "tcp".into(),
                }],
                volumes: Vec::new(),
                configs: Vec::new(),
                container_labels: BTreeMap::new(),
                service_labels: BTreeMap::new(),
                healthcheck: None,
                replicas: 1,
                constraints: Vec::new(),
                max_replicas_per_node: None,
                max_surge: 1,
                stop_grace_period_seconds: 10,
            },
            ports: vec![PortBinding {
                target: 80,
                published: None,
                protocol: "tcp".into(),
            }],
            generation: 1,
            deployment_generation: 1,
            deployment_retry_revision: 0,
            spec_hash: "hash".into(),
            image_resolved: false,
        };
        let bindings = task_port_bindings(&assignment);
        assert_eq!(
            bindings["80/tcp"].as_ref().unwrap()[0].host_port.as_deref(),
            Some("")
        );

        let inspect = bollard::models::ContainerInspectResponse {
            network_settings: Some(bollard::models::NetworkSettings {
                ports: Some(HashMap::from([(
                    "80/tcp".into(),
                    Some(vec![DockerPortBinding {
                        host_ip: Some("0.0.0.0".into()),
                        host_port: Some("49152".into()),
                    }]),
                )])),
                ..Default::default()
            }),
            ..Default::default()
        };
        let resolved = resolved_container_ports(&inspect, assignment.ports);
        assert_eq!(resolved[0].published, Some(49_152));
    }
}
