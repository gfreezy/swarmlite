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
        RemoveContainerOptionsBuilder, RemoveImageOptionsBuilder, RemoveVolumeOptionsBuilder,
        StopContainerOptionsBuilder, TagImageOptionsBuilder, UploadToContainerOptionsBuilder,
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
const GATEWAY_IMAGE_LABEL: &str = "io.swarmlite.gateway_image";
const GATEWAY_LISTEN_LABEL: &str = "io.swarmlite.gateway_listen";
const GATEWAY_GRACE_PERIOD_LABEL: &str = "io.swarmlite.gateway_grace_period_seconds";
const GATEWAY_HTTP3_LABEL: &str = "io.swarmlite.gateway_http3_enabled";
const GATEWAY_TOKEN_HASH_LABEL: &str = "io.swarmlite.gateway_token_sha256";
const GATEWAY_SLOT_LABEL: &str = "io.swarmlite.gateway_slot";
const GATEWAY_RUNTIME_SPEC_LABEL: &str = "io.swarmlite.gateway_runtime_spec_sha256";
const GATEWAY_SYNC_COMPONENT: &str = "gateway-storage-sync";
const GATEWAY_ACTIVE_ADMIN_PORT: u16 = 2019;
const GATEWAY_STAGED_ADMIN_PORT: u16 = 2020;
const GATEWAY_SYNC_ADMIN_PORT: u16 = 2021;
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
    created: i64,
    image_id: Option<String>,
    cluster_id: Option<String>,
    image: Option<String>,
    grace_period_seconds: Option<String>,
    slot: Option<GatewaySlot>,
    runtime_spec_hash: Option<String>,
    running: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GatewaySlot {
    Blue,
    Green,
}

impl GatewaySlot {
    fn label(self) -> &'static str {
        match self {
            Self::Blue => "blue",
            Self::Green => "green",
        }
    }

    fn opposite(self) -> Self {
        match self {
            Self::Blue => Self::Green,
            Self::Green => Self::Blue,
        }
    }
}

impl std::str::FromStr for GatewaySlot {
    type Err = ();

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "blue" => Ok(Self::Blue),
            "green" => Ok(Self::Green),
            _ => Err(()),
        }
    }
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
    image_relay: Option<String>,
    relay_http: Option<reqwest::Client>,
    podman_http: Option<reqwest::Client>,
    prepared_images: Arc<std::sync::RwLock<HashMap<String, String>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TaskNameConflictResolution {
    Recovered,
    RetryCreate,
}

impl DockerCompatibleRuntime {
    pub fn connect(config: &ResolvedRuntimeConfig) -> Result<Self> {
        Self::connect_inner(config, None, None, DeploymentPolicy::default(), None)
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
            None,
        )
    }

    pub fn connect_with_image_relay(
        config: &ResolvedRuntimeConfig,
        registry_credentials: RegistryCredentialStore,
        config_root: PathBuf,
        deployment_policy: DeploymentPolicy,
        image_relay: String,
    ) -> Result<Self> {
        Self::connect_inner(
            config,
            Some(registry_credentials),
            Some(config_root),
            deployment_policy,
            Some(image_relay),
        )
    }

    fn connect_inner(
        config: &ResolvedRuntimeConfig,
        registry_credentials: Option<RegistryCredentialStore>,
        config_root: Option<PathBuf>,
        deployment_policy: DeploymentPolicy,
        image_relay: Option<String>,
    ) -> Result<Self> {
        let client = Docker::connect_with_socket(&config.socket, 120, API_DEFAULT_VERSION)
            .with_context(|| {
                format!(
                    "failed to connect to {} API at {}",
                    config.kind, config.socket
                )
            })?;
        let podman_http = (config.kind == RuntimeKind::Podman)
            .then(|| {
                reqwest::Client::builder()
                    .unix_socket(config.socket.clone())
                    .build()
            })
            .transpose()
            .context("failed to construct Podman image API client")?;
        let relay_http = image_relay
            .as_ref()
            .map(|_| reqwest::Client::builder().no_proxy().build())
            .transpose()
            .context("failed to construct local image relay client")?;
        Ok(Self {
            client,
            kind: config.kind,
            socket: config.socket.clone(),
            registry_credentials,
            config_root,
            deployment_policy: Arc::new(std::sync::RwLock::new(deployment_policy)),
            image_relay,
            relay_http,
            podman_http,
            prepared_images: Arc::new(std::sync::RwLock::new(HashMap::new())),
        })
    }

    pub async fn managed_cluster_inventory(&self) -> Result<ManagedClusterInventory> {
        let summaries = self.list_managed_summaries().await?;
        let mut inventory = ManagedClusterInventory::default();
        let mut newest_gateway = HashMap::<String, i64>::new();
        for summary in summaries {
            let created = summary.created.unwrap_or_default();
            let labels = summary.labels.unwrap_or_default();
            match labels.get(CLUSTER_LABEL).filter(|value| !value.is_empty()) {
                Some(cluster_id) => {
                    inventory.cluster_ids.insert(cluster_id.clone());
                    if is_gateway_system_container(&labels) {
                        inventory.gateway_cluster_ids.insert(cluster_id.clone());
                        let is_newest = newest_gateway
                            .get(cluster_id)
                            .is_none_or(|existing| created > *existing);
                        if is_newest {
                            newest_gateway.insert(cluster_id.clone(), created);
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
                                inventory.gateway_listen.insert(cluster_id.clone(), listen);
                            }
                            if let Some(image) = labels
                                .get(GATEWAY_IMAGE_LABEL)
                                .filter(|value| !value.is_empty())
                            {
                                inventory
                                    .gateway_images
                                    .insert(cluster_id.clone(), image.clone());
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
        assignment: Option<&GatewayAssignment>,
    ) -> Result<()> {
        let ports = enabled
            .then(|| {
                gateway_ports(&spec.gateway.listen).map_err(|error| NonRetryableGatewayError {
                    message: format!("invalid Gateway listener configuration: {error:#}"),
                })
            })
            .transpose()?;
        let mut gateways = self.gateway_containers().await?;
        for existing in &gateways {
            if existing.cluster_id.as_deref() != Some(&spec.cluster_id) {
                bail!(
                    "managed gateway container belongs to cluster {:?}, not {}; recover the old cluster or remove that container",
                    existing.cluster_id,
                    spec.cluster_id
                );
            }
        }

        if !enabled {
            let active_admin_port = self.gateway_public_admin_port().await;
            let active = gateways
                .iter()
                .find(|gateway| gateway.running)
                .or_else(|| gateways.first());
            if let Some(active) = active {
                self.commit_gateway_snapshot_before_removal(spec, active, active_admin_port)
                    .await?;
            }
            for existing in gateways {
                self.remove_gateway(&existing).await?;
            }
            self.remove_gateway_sync_helper(
                &gateway_sync_container_name(&spec.cluster_id),
                &spec.cluster_id,
            )
            .await?;
            self.remove_gateway_volumes(&spec.cluster_id).await?;
            return Ok(());
        }

        let assignment = assignment.context(
            "Controller did not provide a Gateway configuration; preserving the current Gateway",
        )?;
        validate_gateway_assignment(assignment, &spec.cluster_id)?;
        if gateways.len() > 2 {
            bail!("found more than two managed Gateway containers on this node");
        }

        let gateway_image = self.ensure_image_if_missing(&spec.gateway.image).await?;
        let desired_image_id = self.inspect_image_id(&gateway_image).await?;
        let runtime_spec_hash = gateway_runtime_spec_hash(spec)?;
        if let Some(index) = gateways.iter().position(|gateway| {
            gateway.image_id.as_deref() == Some(desired_image_id.as_str())
                && gateway.runtime_spec_hash.as_deref() == Some(runtime_spec_hash.as_str())
                && gateway.slot.is_some()
        }) {
            let desired = gateways.remove(index);
            let desired_is_staged = gateways
                .iter()
                .any(|stale| desired.created >= stale.created);
            if !desired.running {
                if gateways.is_empty() {
                    ensure_gateway_ports_available(
                        ports.as_ref().expect("enabled Gateway ports"),
                        spec.gateway.http.http3_enabled.unwrap_or(true),
                        GATEWAY_ACTIVE_ADMIN_PORT,
                    )?;
                    ensure_gateway_admin_port_available(GATEWAY_STAGED_ADMIN_PORT)?;
                }
                self.client
                    .start_container(&desired.id, None)
                    .await
                    .context("failed to start the managed Gateway container")?;
            }
            let desired_admin_port = if desired_is_staged {
                self.wait_gateway_admin(GATEWAY_STAGED_ADMIN_PORT).await?;
                GATEWAY_STAGED_ADMIN_PORT
            } else {
                self.wait_gateway_admin_any().await?
            };
            let has_public_config = self.gateway_has_public_config(desired_admin_port).await?;
            if let Some(legacy_index) = gateways.iter().position(|gateway| gateway.slot.is_none())
                && !has_public_config
            {
                let mut legacy = gateways.remove(legacy_index);
                if !legacy.running {
                    self.client
                        .start_container(&legacy.id, None)
                        .await
                        .context("failed to restart the legacy Gateway during upgrade recovery")?;
                    legacy.running = true;
                }
                self.remove_gateway(&desired).await?;
                self.remove_gateway_slot_volumes(&spec.cluster_id, desired.slot)
                    .await?;
                return self
                    .replace_gateway(
                        spec,
                        assignment,
                        Some(&legacy),
                        &gateway_image,
                        &runtime_spec_hash,
                    )
                    .await;
            }
            if !has_public_config {
                self.restore_gateway_certificates(
                    &reqwest::Client::new(),
                    desired_admin_port,
                    !gateways.is_empty(),
                )
                .await
                .context("failed to resume preparation of the staged Gateway")?;
            } else {
                self.post_gateway_action(
                    &reqwest::Client::new(),
                    desired_admin_port,
                    "/swarmlite/storage/resume",
                )
                .await
                .context("failed to release a previous Gateway replacement barrier")?;
            }
            let configured_admin_port = if desired_is_staged {
                GATEWAY_STAGED_ADMIN_PORT
            } else {
                GATEWAY_ACTIVE_ADMIN_PORT
            };
            self.apply_gateway_config_to(
                &desired,
                assignment,
                desired_admin_port,
                configured_admin_port,
            )
            .await?;
            for stale in gateways {
                self.remove_gateway(&stale).await?;
                if let Err(error) = self
                    .remove_gateway_slot_volumes(&spec.cluster_id, stale.slot)
                    .await
                {
                    warn!(%error, "failed to remove stale Gateway volumes");
                }
            }
            if configured_admin_port != GATEWAY_ACTIVE_ADMIN_PORT {
                self.move_gateway_admin(
                    &reqwest::Client::new(),
                    configured_admin_port,
                    GATEWAY_ACTIVE_ADMIN_PORT,
                )
                .await
                .context("failed to move the recovered Gateway admin API to the active port")?;
            }
            return Ok(());
        }

        if gateways.len() > 1 {
            bail!(
                "found two stale managed Gateway containers and neither matches the desired runtime"
            );
        }
        let existing = gateways.pop();
        if existing.is_none() {
            ensure_gateway_ports_available(
                ports.as_ref().expect("enabled Gateway ports"),
                spec.gateway.http.http3_enabled.unwrap_or(true),
                GATEWAY_ACTIVE_ADMIN_PORT,
            )?;
            ensure_gateway_admin_port_available(GATEWAY_STAGED_ADMIN_PORT)?;
        }

        self.replace_gateway(
            spec,
            assignment,
            existing.as_ref(),
            &gateway_image,
            &runtime_spec_hash,
        )
        .await
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

    async fn apply_gateway_config_to(
        &self,
        gateway: &ExistingGatewayContainer,
        assignment: &GatewayAssignment,
        current_admin_port: u16,
        configured_admin_port: u16,
    ) -> Result<()> {
        let cluster_id = gateway
            .cluster_id
            .as_deref()
            .context("managed Gateway container has no cluster identity")?;
        validate_gateway_assignment(assignment, cluster_id)?;
        let config = gateway_config_for_admin(&assignment.config, configured_admin_port);
        let client = reqwest::Client::new();
        self.post_gateway_config(&client, current_admin_port, "/load", &config)
            .await?;
        self.wait_gateway_admin(configured_admin_port).await?;
        self.persist_gateway_recovery_snapshot_to(&gateway.id, &assignment.recovery_snapshot)
            .await?;
        info!(
            generation = assignment.generation,
            runtime = %self.kind,
            "applied local Gateway configuration"
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

    async fn persist_gateway_recovery_snapshot_to(
        &self,
        container_id: &str,
        snapshot: &GatewayRecoverySnapshot,
    ) -> Result<()> {
        let payload = serde_json::to_vec(snapshot)?;
        if payload.len() > MAX_GATEWAY_RECOVERY_SNAPSHOT_BYTES {
            bail!("Gateway recovery snapshot is too large");
        }
        let archive = gateway_recovery_snapshot_archive(&payload)?;
        let options = UploadToContainerOptionsBuilder::default()
            .path("/config")
            .build();
        self.client
            .upload_to_container(
                container_id,
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
                container_id,
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
        admin_port: u16,
        path: &str,
        value: &serde_json::Value,
    ) -> Result<()> {
        let url = format!("{}{path}", gateway_admin_url(admin_port));
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

    async fn move_gateway_admin(
        &self,
        client: &reqwest::Client,
        current_admin_port: u16,
        target_admin_port: u16,
    ) -> Result<()> {
        if current_admin_port == target_admin_port {
            return Ok(());
        }
        let url = format!("{}/config/", gateway_admin_url(current_admin_port));
        let response = client.get(&url).send().await?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_default()
                .chars()
                .take(512)
                .collect::<String>();
            bail!("Caddy Admin API {url} returned {status}: {body}");
        }
        let mut config: serde_json::Value = response
            .json()
            .await
            .context("Caddy Admin API returned an invalid configuration")?;
        config["admin"]["listen"] = serde_json::json!(format!("127.0.0.1:{target_admin_port}"));
        self.post_gateway_config(client, current_admin_port, "/load", &config)
            .await?;
        self.wait_gateway_admin(target_admin_port).await
    }

    async fn post_gateway_action(
        &self,
        client: &reqwest::Client,
        admin_port: u16,
        path: &str,
    ) -> Result<()> {
        let url = format!("{}{path}", gateway_admin_url(admin_port));
        let response = client.post(&url).send().await?;
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

    async fn restore_gateway_certificates(
        &self,
        client: &reqwest::Client,
        admin_port: u16,
        required: bool,
    ) -> Result<bool> {
        let path = "/swarmlite/storage/restore";
        let url = format!("{}{path}", gateway_admin_url(admin_port));
        let response = client.post(&url).send().await?;
        if response.status().is_success() {
            return Ok(true);
        }
        if response.status() == reqwest::StatusCode::NOT_FOUND && !required {
            return Ok(false);
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

    async fn wait_gateway_admin(&self, admin_port: u16) -> Result<()> {
        let client = reqwest::Client::new();
        let url = format!("{}/config/", gateway_admin_url(admin_port));
        let mut last_error = None;
        for _ in 0..100 {
            match client.get(&url).send().await {
                Ok(response) if response.status().is_success() => return Ok(()),
                Ok(response) => last_error = Some(format!("HTTP {}", response.status())),
                Err(error) => last_error = Some(error.to_string()),
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        bail!(
            "Caddy Admin API {url} did not become ready: {}",
            last_error.unwrap_or_else(|| "no response".to_owned())
        )
    }

    async fn wait_gateway_admin_any(&self) -> Result<u16> {
        let client = reqwest::Client::new();
        let mut last_error = None;
        for _ in 0..100 {
            for admin_port in [GATEWAY_ACTIVE_ADMIN_PORT, GATEWAY_STAGED_ADMIN_PORT] {
                let url = format!("{}/config/", gateway_admin_url(admin_port));
                match client.get(&url).send().await {
                    Ok(response) if response.status().is_success() => return Ok(admin_port),
                    Ok(response) => {
                        last_error = Some(format!("{url} returned HTTP {}", response.status()))
                    }
                    Err(error) => last_error = Some(format!("{url}: {error}")),
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        bail!(
            "Caddy Admin API did not become ready on active port {} or staged port {}: {}",
            GATEWAY_ACTIVE_ADMIN_PORT,
            GATEWAY_STAGED_ADMIN_PORT,
            last_error.unwrap_or_else(|| "no response".to_owned())
        )
    }

    async fn gateway_public_admin_port(&self) -> Option<u16> {
        // During an interrupted overlap, the staged process contains the newly restored state.
        // Prefer it for a final snapshot; after a completed promotion only 2019 responds.
        for admin_port in [GATEWAY_STAGED_ADMIN_PORT, GATEWAY_ACTIVE_ADMIN_PORT] {
            if self
                .gateway_has_public_config(admin_port)
                .await
                .unwrap_or(false)
            {
                return Some(admin_port);
            }
        }
        None
    }

    async fn gateway_has_public_config(&self, admin_port: u16) -> Result<bool> {
        let url = format!(
            "{}/config/apps/http/servers/swarmlite/",
            gateway_admin_url(admin_port)
        );
        let response = reqwest::Client::new().get(&url).send().await?;
        match response.status() {
            status if status.is_success() => Ok(true),
            reqwest::StatusCode::NOT_FOUND => Ok(false),
            status => bail!("Caddy Admin API {url} returned {status}"),
        }
    }

    async fn gateway_containers(&self) -> Result<Vec<ExistingGatewayContainer>> {
        let summaries = self.list_managed_summaries().await?;
        Ok(summaries
            .into_iter()
            .filter_map(|summary| {
                let created = summary.created.unwrap_or_default();
                let labels = summary.labels.unwrap_or_default();
                if !is_gateway_system_container(&labels) {
                    return None;
                }
                Some(ExistingGatewayContainer {
                    id: summary.id?,
                    created,
                    image_id: summary.image_id,
                    cluster_id: labels.get(CLUSTER_LABEL).cloned(),
                    image: labels.get(GATEWAY_IMAGE_LABEL).cloned(),
                    grace_period_seconds: labels.get(GATEWAY_GRACE_PERIOD_LABEL).cloned(),
                    slot: labels
                        .get(GATEWAY_SLOT_LABEL)
                        .and_then(|value| value.parse().ok()),
                    runtime_spec_hash: labels.get(GATEWAY_RUNTIME_SPEC_LABEL).cloned(),
                    running: summary.state == Some(ContainerSummaryStateEnum::RUNNING),
                })
            })
            .collect())
    }

    async fn replace_gateway(
        &self,
        spec: &GatewayContainerSpec,
        assignment: &GatewayAssignment,
        existing: Option<&ExistingGatewayContainer>,
        gateway_image: &str,
        runtime_spec_hash: &str,
    ) -> Result<()> {
        let target_slot = existing.and_then(|gateway| gateway.slot).map_or_else(
            || {
                if existing.is_some() {
                    GatewaySlot::Green
                } else {
                    GatewaySlot::Blue
                }
            },
            GatewaySlot::opposite,
        );
        self.remove_gateway_slot_volumes(&spec.cluster_id, Some(target_slot))
            .await?;

        let client = reqwest::Client::new();
        let mut existing_admin_port = GATEWAY_ACTIVE_ADMIN_PORT;
        if existing.is_some_and(|gateway| gateway.running) {
            existing_admin_port = self.wait_gateway_admin_any().await?;
            if existing_admin_port == GATEWAY_STAGED_ADMIN_PORT {
                self.move_gateway_admin(
                    &client,
                    GATEWAY_STAGED_ADMIN_PORT,
                    GATEWAY_ACTIVE_ADMIN_PORT,
                )
                .await
                .context("failed to normalize the current Gateway admin API before replacement")?;
                existing_admin_port = GATEWAY_ACTIVE_ADMIN_PORT;
            }
        }
        let mut helper_id = None;
        let mut candidate = None;
        let mut candidate_public = false;
        let mut legacy_stopped = false;
        let mut active_quiesced = false;
        let mut retired_removed = existing.is_none();
        let result: Result<()> = async {
            if let Some(active) = existing {
                if active.slot.is_some() {
                    self.post_gateway_action(
                        &client,
                        existing_admin_port,
                        "/swarmlite/storage/push",
                    )
                    .await
                    .context("failed to commit the active Gateway certificate snapshot")?;
                    active_quiesced = true;
                } else {
                    let id = self
                        .create_gateway_sync_helper(spec, gateway_image, None)
                        .await?;
                    helper_id = Some(id);
                    self.post_gateway_action(
                        &client,
                        GATEWAY_SYNC_ADMIN_PORT,
                        "/swarmlite/storage/push",
                    )
                    .await
                    .context("failed to commit the legacy Gateway certificate snapshot")?;
                }
            }

            let green = self
                .create_gateway(spec, gateway_image, target_slot, runtime_spec_hash)
                .await?;
            candidate = Some(green);
            let green = candidate.as_ref().expect("candidate was just created");
            self.restore_gateway_certificates(
                &client,
                GATEWAY_STAGED_ADMIN_PORT,
                existing.is_some(),
            )
            .await
            .context("failed to restore the staged Gateway certificate snapshot")?;
            self.persist_gateway_recovery_snapshot_to(&green.id, &assignment.recovery_snapshot)
                .await?;

            if let Some(active) = existing.filter(|gateway| gateway.slot.is_none()) {
                self.stop_gateway(active).await?;
                legacy_stopped = true;
                self.post_gateway_action(
                    &client,
                    GATEWAY_SYNC_ADMIN_PORT,
                    "/swarmlite/storage/push",
                )
                    .await
                    .context("failed to finalize the stopped legacy Gateway certificate snapshot")?;
                self.post_gateway_action(
                    &client,
                    GATEWAY_STAGED_ADMIN_PORT,
                    "/swarmlite/storage/restore",
                )
                .await
                .context("failed to restore the final legacy Gateway certificate snapshot")?;
            }

            let config =
                gateway_config_for_admin(&assignment.config, GATEWAY_STAGED_ADMIN_PORT);
            self.post_gateway_config(
                &client,
                GATEWAY_STAGED_ADMIN_PORT,
                "/load",
                &config,
            )
            .await
            .context("failed to activate the staged Gateway public listeners")?;
            candidate_public = true;

            if let Some(active) = existing {
                if let Err(error) = self.remove_gateway(active).await {
                    warn!(%error, "new Gateway is active but the retired Gateway could not be removed");
                    return Err(error).context(
                        "the candidate is serving traffic on staged admin port 2020, but the retired Gateway still owns active admin port 2019",
                    );
                }
                retired_removed = true;
            }
            self.move_gateway_admin(
                &client,
                GATEWAY_STAGED_ADMIN_PORT,
                GATEWAY_ACTIVE_ADMIN_PORT,
            )
            .await
            .context("failed to move the promoted Gateway admin API to the active port")?;
            Ok(())
        }
        .await;

        if let Some(id) = helper_id.as_deref()
            && let Err(error) = self.remove_gateway_sync_helper(id, &spec.cluster_id).await
        {
            warn!(%error, "failed to clean up the Gateway certificate sync helper");
        }

        if let Err(error) = result {
            if active_quiesced
                && !retired_removed
                && existing.is_some()
                && let Err(resume_error) = self
                    .post_gateway_action(&client, existing_admin_port, "/swarmlite/storage/resume")
                    .await
            {
                warn!(%resume_error, "failed to resume certificate writes on the preserved Gateway");
            }
            if candidate_public {
                if retired_removed
                    && let Some(active) = existing
                    && let Err(cleanup_error) = self
                        .remove_gateway_slot_volumes(&spec.cluster_id, active.slot)
                        .await
                {
                    warn!(%cleanup_error, "failed to remove retired Gateway volumes after admin-port promotion was interrupted");
                }
                return Err(error).context(
                    "the replacement Gateway is preserving public traffic and will retry admin-port promotion",
                );
            }
            if let Some(green) = candidate.as_ref()
                && let Err(cleanup_error) = self.remove_gateway(green).await
            {
                warn!(%cleanup_error, "failed to remove the staged Gateway after upgrade failure");
            }
            if let Err(cleanup_error) = self
                .remove_gateway_slot_volumes(&spec.cluster_id, Some(target_slot))
                .await
            {
                warn!(%cleanup_error, "failed to remove staged Gateway volumes after upgrade failure");
            }
            if legacy_stopped
                && let Some(active) = existing
                && let Err(start_error) = self.client.start_container(&active.id, None).await
            {
                return Err(error.context(format!(
                    "the legacy Gateway was stopped and rollback also failed: {start_error}"
                )));
            }
            return Err(error);
        }

        if let Some(active) = existing
            && let Err(error) = self
                .remove_gateway_slot_volumes(&spec.cluster_id, active.slot)
                .await
        {
            warn!(%error, "failed to remove the retired Gateway volumes");
        }
        info!(
            image = %spec.gateway.image,
            slot = target_slot.label(),
            generation = assignment.generation,
            "completed the Gateway blue/green replacement"
        );
        Ok(())
    }

    async fn commit_gateway_snapshot_before_removal(
        &self,
        spec: &GatewayContainerSpec,
        active: &ExistingGatewayContainer,
        active_admin_port: Option<u16>,
    ) -> Result<()> {
        let client = reqwest::Client::new();
        if active.running
            && active.slot.is_some()
            && let Some(active_admin_port) = active_admin_port
        {
            return self
                .post_gateway_action(&client, active_admin_port, "/swarmlite/storage/push")
                .await
                .context("failed to commit certificates before removing the Gateway");
        }

        let gateway_image = self.ensure_image_if_missing(&spec.gateway.image).await?;
        let helper_id = self
            .create_gateway_sync_helper(spec, &gateway_image, active.slot)
            .await?;
        let mut stopped = false;
        let result: Result<()> = async {
            self.post_gateway_action(&client, GATEWAY_SYNC_ADMIN_PORT, "/swarmlite/storage/push")
                .await?;
            if active.running {
                self.stop_gateway(active).await?;
                stopped = true;
                self.post_gateway_action(
                    &client,
                    GATEWAY_SYNC_ADMIN_PORT,
                    "/swarmlite/storage/push",
                )
                .await?;
            }
            Ok(())
        }
        .await;
        if let Err(error) = self
            .remove_gateway_sync_helper(&helper_id, &spec.cluster_id)
            .await
        {
            warn!(%error, "failed to clean up the Gateway certificate sync helper");
        }
        if let Err(error) = result {
            if stopped && let Err(start_error) = self.client.start_container(&active.id, None).await
            {
                return Err(error.context(format!(
                    "certificate snapshot failed and the Gateway rollback also failed: {start_error}"
                )));
            }
            return Err(error).context("failed to commit certificates before removing the Gateway");
        }
        Ok(())
    }

    async fn create_gateway_sync_helper(
        &self,
        spec: &GatewayContainerSpec,
        gateway_image: &str,
        data_slot: Option<GatewaySlot>,
    ) -> Result<String> {
        let name = gateway_sync_container_name(&spec.cluster_id);
        let remove = RemoveContainerOptionsBuilder::default().force(true).build();
        match self.client.remove_container(&name, Some(remove)).await {
            Ok(()) => {}
            Err(error) if docker_not_found(&error) => {}
            Err(error) => {
                return Err(error).context("failed to remove a stale Gateway sync helper");
            }
        }
        self.remove_gateway_sync_volumes(&spec.cluster_id).await?;
        let [data_volume, _, _] = gateway_volume_names(&spec.cluster_id, data_slot);
        let [config_volume, cache_volume] = gateway_sync_volume_names(&spec.cluster_id);
        let bootstrap = gateway_bootstrap(spec, GATEWAY_SYNC_ADMIN_PORT, false)?;
        let body = ContainerCreateBody {
            image: Some(gateway_image.to_owned()),
            entrypoint: Some(vec!["/bin/sh".to_owned(), "-ec".to_owned()]),
            cmd: Some(vec![
                "printf '%s' \"$SWARMLITE_CADDY_BOOTSTRAP\" > /config/bootstrap.json; exec caddy run --config /config/bootstrap.json"
                    .to_owned(),
            ]),
            env: Some(vec![
                "XDG_CONFIG_HOME=/config".to_owned(),
                "XDG_DATA_HOME=/data".to_owned(),
                format!("SWARMLITE_TOKEN={}", spec.token),
                format!("SWARMLITE_GATEWAY_ID={}", spec.node_id),
                format!("SWARMLITE_CADDY_BOOTSTRAP={bootstrap}"),
            ]),
            labels: Some(HashMap::from([
                (MANAGED_LABEL.to_owned(), "true".to_owned()),
                (CLUSTER_LABEL.to_owned(), spec.cluster_id.clone()),
                (SYSTEM_LABEL.to_owned(), "true".to_owned()),
                (COMPONENT_LABEL.to_owned(), GATEWAY_SYNC_COMPONENT.to_owned()),
            ])),
            host_config: Some(HostConfig {
                binds: Some(vec![
                    format!("{data_volume}:/data"),
                    format!("{config_volume}:/config"),
                    format!("{cache_volume}:/cache"),
                ]),
                network_mode: Some("host".to_owned()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let options = CreateContainerOptionsBuilder::default().name(&name).build();
        let created = match self.client.create_container(Some(options), body).await {
            Ok(created) => created,
            Err(error) => {
                let _ = self.remove_gateway_sync_volumes(&spec.cluster_id).await;
                return Err(error).context("failed to create the Gateway sync helper");
            }
        };
        if let Err(error) = self.client.start_container(&created.id, None).await {
            let error =
                anyhow::Error::new(error).context("failed to start the Gateway sync helper");
            let _ = self
                .remove_new_container_after_failed_start(&created.id, "Gateway sync helper")
                .await;
            let _ = self.remove_gateway_sync_volumes(&spec.cluster_id).await;
            return Err(error);
        }
        if let Err(error) = self.wait_gateway_admin(GATEWAY_SYNC_ADMIN_PORT).await {
            let _ = self
                .remove_new_container_after_failed_start(&created.id, "Gateway sync helper")
                .await;
            let _ = self.remove_gateway_sync_volumes(&spec.cluster_id).await;
            return Err(error);
        }
        Ok(created.id)
    }

    async fn remove_gateway_sync_helper(&self, id: &str, cluster_id: &str) -> Result<()> {
        let remove = RemoveContainerOptionsBuilder::default().force(true).build();
        match self.client.remove_container(id, Some(remove)).await {
            Ok(()) => {}
            Err(error) if docker_not_found(&error) => {}
            Err(error) => return Err(error.into()),
        }
        self.remove_gateway_sync_volumes(cluster_id).await
    }

    async fn create_gateway(
        &self,
        spec: &GatewayContainerSpec,
        gateway_image: &str,
        slot: GatewaySlot,
        runtime_spec_hash: &str,
    ) -> Result<ExistingGatewayContainer> {
        let bootstrap = gateway_bootstrap(spec, GATEWAY_STAGED_ADMIN_PORT, false)?;
        let [data_volume, config_volume, cache_volume] =
            gateway_volume_names(&spec.cluster_id, Some(slot));
        let host_config = HostConfig {
            binds: Some(vec![
                format!("{data_volume}:/data"),
                format!("{config_volume}:/config"),
                format!("{cache_volume}:/cache"),
            ]),
            network_mode: Some("host".to_owned()),
            restart_policy: Some(RestartPolicy {
                name: Some(RestartPolicyNameEnum::UNLESS_STOPPED),
                maximum_retry_count: None,
            }),
            ..Default::default()
        };
        let labels = gateway_labels(spec, slot, runtime_spec_hash);
        let body = ContainerCreateBody {
            image: Some(gateway_image.to_owned()),
            entrypoint: Some(vec!["/bin/sh".to_owned(), "-ec".to_owned()]),
            cmd: Some(vec![
                "printf '%s' \"$SWARMLITE_CADDY_BOOTSTRAP\" > /config/bootstrap.json; exec caddy run --resume --config /config/bootstrap.json"
                    .to_owned(),
            ]),
            env: Some(vec![
                "XDG_CONFIG_HOME=/config".to_owned(),
                "XDG_DATA_HOME=/data".to_owned(),
                format!("SWARMLITE_TOKEN={}", spec.token),
                format!("SWARMLITE_GATEWAY_ID={}", spec.node_id),
                format!("SWARMLITE_CADDY_BOOTSTRAP={bootstrap}"),
            ]),
            labels: Some(labels),
            stop_timeout: Some(i64::from(gateway_stop_timeout(
                spec.gateway.shutdown.grace_period_seconds,
            ))),
            host_config: Some(host_config),
            ..Default::default()
        };
        let container_name = gateway_container_name(slot);
        let options = CreateContainerOptionsBuilder::default()
            .name(&container_name)
            .build();
        let created = self
            .client
            .create_container(Some(options), body)
            .await
            .with_context(|| {
                format!(
                    "failed to create Gateway container {container_name}; remove any unrelated container using that name"
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
            slot = slot.label(),
            "started staged Gateway container"
        );
        if let Err(error) = self.wait_gateway_admin(GATEWAY_STAGED_ADMIN_PORT).await {
            if let Err(cleanup_error) = self
                .remove_new_container_after_failed_start(&created.id, "Gateway")
                .await
            {
                return Err(error.context(format!(
                    "also failed to remove the staged Gateway: {cleanup_error:#}"
                )));
            }
            return Err(error);
        }
        Ok(ExistingGatewayContainer {
            id: created.id,
            created: i64::MAX,
            image_id: None,
            cluster_id: Some(spec.cluster_id.clone()),
            image: Some(spec.gateway.image.clone()),
            grace_period_seconds: Some(optional_label(spec.gateway.shutdown.grace_period_seconds)),
            slot: Some(slot),
            runtime_spec_hash: Some(runtime_spec_hash.to_owned()),
            running: true,
        })
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
        if container.running
            && let Err(error) = self.stop_gateway(container).await
        {
            warn!(%error, "graceful gateway stop failed; forcing removal");
        }
        let remove = RemoveContainerOptionsBuilder::default().force(true).build();
        self.client
            .remove_container(&container.id, Some(remove))
            .await
            .context("failed to remove the managed gateway container")?;
        info!(runtime = %self.kind, "removed gateway container");
        Ok(())
    }

    async fn stop_gateway(&self, container: &ExistingGatewayContainer) -> Result<()> {
        let stop = StopContainerOptionsBuilder::default()
            .t(gateway_stop_timeout(
                container
                    .grace_period_seconds
                    .as_deref()
                    .and_then(|value| value.parse().ok()),
            ))
            .build();
        match self.client.stop_container(&container.id, Some(stop)).await {
            Ok(()) => Ok(()),
            Err(error) if docker_not_found(&error) || docker_already_stopped(&error) => Ok(()),
            Err(error) => Err(error).context("failed to stop the managed Gateway container"),
        }
    }

    async fn remove_gateway_volumes(&self, cluster_id: &str) -> Result<()> {
        for slot in [None, Some(GatewaySlot::Blue), Some(GatewaySlot::Green)] {
            self.remove_gateway_slot_volumes(cluster_id, slot).await?;
        }
        self.remove_gateway_sync_volumes(cluster_id).await?;
        info!(runtime = %self.kind, "removed Gateway persistent volumes");
        Ok(())
    }

    async fn remove_gateway_slot_volumes(
        &self,
        cluster_id: &str,
        slot: Option<GatewaySlot>,
    ) -> Result<()> {
        self.remove_named_gateway_volumes(gateway_volume_names(cluster_id, slot))
            .await
    }

    async fn remove_gateway_sync_volumes(&self, cluster_id: &str) -> Result<()> {
        self.remove_named_gateway_volumes(gateway_sync_volume_names(cluster_id))
            .await
    }

    async fn remove_named_gateway_volumes<const N: usize>(
        &self,
        volumes: [String; N],
    ) -> Result<()> {
        for volume in volumes {
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
        Ok(())
    }

    async fn ensure_image(
        &self,
        image: &str,
        pull_policy: PullPolicy,
        progress: &RuntimeTaskProgress,
    ) -> Result<String> {
        match pull_policy {
            PullPolicy::Never => {
                let local_image = self.local_image_reference(image);
                self.client
                    .inspect_image(&local_image)
                    .await
                    .with_context(|| {
                        format!("pull_policy=never requires image {image} in the local cache")
                    })?;
                return Ok(local_image);
            }
            PullPolicy::Missing if !pull_policy.refreshes_cached_image(image) => {
                let local_image = self.local_image_reference(image);
                if self.client.inspect_image(&local_image).await.is_ok() {
                    return Ok(local_image);
                }
            }
            PullPolicy::Always | PullPolicy::Missing => {}
        }
        let image_id = self
            .pull_image(image, |attempt, current, total| {
                progress.report_pull(attempt, current, total);
            })
            .await?;
        Ok(self.local_image_reference_with_id(image, image_id))
    }

    async fn ensure_image_if_missing(&self, image: &str) -> Result<String> {
        let local_image = self.local_image_reference(image);
        if self.client.inspect_image(&local_image).await.is_ok() {
            return Ok(local_image);
        }
        let image_id = self.pull_image(image, |_, _, _| {}).await?;
        Ok(self.local_image_reference_with_id(image, image_id))
    }

    async fn pull_image(
        &self,
        image: &str,
        mut report: impl FnMut(u32, Option<u64>, Option<u64>),
    ) -> Result<String> {
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
                Ok(image_id) => return Ok(image_id),
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
    ) -> Result<String> {
        let mut parsed = self.proxy_reference(image, idle_timeout).await;
        let mut image_to_pull = parsed
            .as_ref()
            .map_or_else(|| image.to_owned(), |(temporary, _)| temporary.clone());
        let pull_result = if self.kind == RuntimeKind::Podman && parsed.is_some() {
            self.pull_podman_image(&image_to_pull, attempt, idle_timeout, report)
                .await
        } else {
            let credentials = if parsed.is_none() {
                self.registry_credentials
                    .as_ref()
                    .map(|store| store.credentials_for_image(image))
                    .transpose()?
                    .flatten()
            } else {
                None
            };
            self.pull_docker_image(
                &image_to_pull,
                image,
                attempt,
                idle_timeout,
                report,
                credentials,
            )
            .await
        };
        if let Err(error) = pull_result {
            if parsed.is_none() {
                return Err(error);
            }
            warn!(
                %error,
                image,
                "Controller image proxy pull failed; retrying with the runtime's default pull path"
            );
            let remove = RemoveImageOptionsBuilder::default().noprune(true).build();
            let _ = self
                .client
                .remove_image(&image_to_pull, Some(remove), None)
                .await;
            parsed = None;
            image_to_pull = image.to_owned();
            let credentials = self
                .registry_credentials
                .as_ref()
                .map(|store| store.credentials_for_image(image))
                .transpose()?
                .flatten();
            self.pull_docker_image(
                &image_to_pull,
                image,
                attempt,
                idle_timeout,
                report,
                credentials,
            )
            .await?;
        }
        let image_id = self.inspect_image_id(&image_to_pull).await?;
        if let Some((temporary, reference)) = parsed {
            if let Some((repository, tag)) = reference.tag_parts() {
                let options = TagImageOptionsBuilder::default()
                    .repo(&repository)
                    .tag(&tag)
                    .build();
                self.client
                    .tag_image(&image_id, Some(options))
                    .await
                    .with_context(|| format!("failed to restore original image tag {image}"))?;
                let remove = RemoveImageOptionsBuilder::default().noprune(true).build();
                if let Err(error) = self
                    .client
                    .remove_image(&temporary, Some(remove), None)
                    .await
                {
                    warn!(%error, image = %temporary, "failed to remove temporary image relay tag");
                }
            } else {
                self.prepared_images
                    .write()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(image.to_owned(), image_id.clone());
            }
        }
        Ok(image_id)
    }

    async fn proxy_reference(
        &self,
        image: &str,
        idle_timeout: std::time::Duration,
    ) -> Option<(String, swarmlite_registry::ImageReference)> {
        let relay = self.image_relay.as_deref()?;
        let client = self.relay_http.as_ref()?;
        let reference = swarmlite_registry::ImageReference::parse(image).ok()?;
        let probe_timeout = idle_timeout.min(std::time::Duration::from_secs(10));
        let ping = tokio::time::timeout(
            probe_timeout,
            client.head(format!("http://{relay}/v2/")).send(),
        )
        .await
        .ok()?
        .ok()?;
        if !ping.status().is_success()
            || ping
                .headers()
                .get("x-swarmlite-image-proxy")
                .and_then(|value| value.to_str().ok())
                != Some("enabled")
        {
            return None;
        }
        let probe = tokio::time::timeout(
            probe_timeout,
            client
                .head(format!("http://{relay}{}", reference.relay_manifest_path()))
                .header("x-swarmlite-proxy-probe", "1")
                .send(),
        )
        .await;
        match probe {
            Ok(Ok(response)) if response.status().is_success() => {
                Some((reference.relay_reference(relay), reference))
            }
            Ok(Ok(response)) => {
                warn!(
                    image,
                    status = %response.status(),
                    "Controller image proxy probe failed; using the runtime's default pull path"
                );
                None
            }
            Ok(Err(error)) => {
                warn!(%error, image, "Controller image proxy probe failed; using the runtime's default pull path");
                None
            }
            Err(_) => {
                warn!(
                    image,
                    "Controller image proxy probe timed out; using the runtime's default pull path"
                );
                None
            }
        }
    }

    async fn pull_docker_image(
        &self,
        image_to_pull: &str,
        original_image: &str,
        attempt: u32,
        idle_timeout: std::time::Duration,
        report: &mut impl FnMut(u32, Option<u64>, Option<u64>),
        credentials: Option<bollard::auth::DockerCredentials>,
    ) -> Result<()> {
        let options = CreateImageOptionsBuilder::default()
            .from_image(image_to_pull)
            .build();
        let mut pull = self.client.create_image(Some(options), None, credentials);
        let mut last_progress_at = std::time::Instant::now();
        let mut layer_progress = HashMap::<String, u64>::new();
        loop {
            let remaining = idle_timeout.saturating_sub(last_progress_at.elapsed());
            if remaining.is_zero() {
                bail!(
                    "image pull for {original_image} made no progress for {} seconds",
                    idle_timeout.as_secs()
                );
            }
            let item = tokio::time::timeout(remaining, pull.next())
                .await
                .with_context(|| {
                    format!(
                        "image pull for {original_image} made no progress for {} seconds",
                        idle_timeout.as_secs()
                    )
                })?;
            let Some(item) = item else {
                break;
            };
            let item = item.with_context(|| format!("failed to pull {original_image}"))?;
            if let Some(message) = item.error_detail.and_then(|detail| detail.message) {
                bail!("registry rejected image pull for {original_image}: {message}");
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

    async fn pull_podman_image(
        &self,
        image: &str,
        attempt: u32,
        idle_timeout: std::time::Duration,
        report: &mut impl FnMut(u32, Option<u64>, Option<u64>),
    ) -> Result<()> {
        let client = self
            .podman_http
            .as_ref()
            .context("Podman image API client is unavailable")?;
        let response = tokio::time::timeout(
            idle_timeout,
            client
                .post("http://localhost/libpod/images/pull")
                .query(&[("reference", image), ("tlsVerify", "false")])
                .send(),
        )
        .await
        .with_context(|| format!("Podman image pull for {image} timed out"))??;
        let status = response.status();
        let mut stream = response.bytes_stream();
        let mut response_body = Vec::new();
        let mut received = 0_u64;
        loop {
            let item = tokio::time::timeout(idle_timeout, stream.next())
                .await
                .with_context(|| {
                    format!(
                        "image pull for {image} made no progress for {} seconds",
                        idle_timeout.as_secs()
                    )
                })?;
            let Some(item) = item else { break };
            let bytes = item.with_context(|| format!("failed to pull {image} through Podman"))?;
            received = received.saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
            if response_body.len() < 1024 * 1024 {
                let remaining = 1024 * 1024 - response_body.len();
                response_body.extend_from_slice(&bytes[..bytes.len().min(remaining)]);
            }
            report(attempt, Some(received), None);
        }
        let body = String::from_utf8_lossy(&response_body);
        if !status.is_success() {
            bail!("Podman rejected image pull for {image}: HTTP {status}: {body}");
        }
        if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&response_body)
            && let Some(error) = value.get("error").and_then(|value| value.as_str())
            && !error.is_empty()
        {
            bail!("Podman rejected image pull for {image}: {error}");
        }
        Ok(())
    }

    fn local_image_reference(&self, image: &str) -> String {
        self.prepared_images
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(image)
            .cloned()
            .unwrap_or_else(|| image.to_owned())
    }

    fn local_image_reference_with_id(&self, image: &str, image_id: String) -> String {
        if self
            .prepared_images
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains_key(image)
        {
            image_id
        } else {
            image.to_owned()
        }
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

fn gateway_labels(
    spec: &GatewayContainerSpec,
    slot: GatewaySlot,
    runtime_spec_hash: &str,
) -> HashMap<String, String> {
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
        (GATEWAY_SLOT_LABEL.to_owned(), slot.label().to_owned()),
        (
            GATEWAY_RUNTIME_SPEC_LABEL.to_owned(),
            runtime_spec_hash.to_owned(),
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

fn gateway_stop_timeout(grace_period_seconds: Option<u64>) -> i32 {
    match grace_period_seconds {
        None => 10,
        Some(0) => -1,
        Some(seconds) => i32::try_from(seconds.saturating_add(5)).unwrap_or(i32::MAX),
    }
}

fn gateway_container_name(slot: GatewaySlot) -> String {
    format!("swarmlite-gateway-{}", slot.label())
}

fn gateway_sync_container_name(cluster_id: &str) -> String {
    format!("swarmlite-gateway-{cluster_id}-storage-sync")
}

fn gateway_volume_names(cluster_id: &str, slot: Option<GatewaySlot>) -> [String; 3] {
    let prefix = slot.map_or_else(
        || format!("swarmlite-gateway-{cluster_id}"),
        |slot| format!("swarmlite-gateway-{cluster_id}-{}", slot.label()),
    );
    [
        format!("{prefix}-data"),
        format!("{prefix}-config"),
        format!("{prefix}-cache"),
    ]
}

fn gateway_sync_volume_names(cluster_id: &str) -> [String; 2] {
    let prefix = format!("swarmlite-gateway-{cluster_id}-storage-sync");
    [format!("{prefix}-config"), format!("{prefix}-cache")]
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

fn gateway_runtime_spec_hash(spec: &GatewayContainerSpec) -> Result<String> {
    let material = serde_json::json!({
        "runtime": "host-network-blue-green-v2-fixed-active-admin",
        "cluster_id": spec.cluster_id,
        "node_id": spec.node_id,
        "token_sha256": gateway_token_hash(&spec.token),
        "shutdown_grace_period_seconds": spec.gateway.shutdown.grace_period_seconds,
        "xdg_config_home": "/config",
        "xdg_data_home": "/data",
    });
    Ok(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&material)?)
    ))
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
            if (2019..=2021).contains(&port) {
                bail!(
                    "gateway listen port {port} is reserved for Caddy administration and Gateway upgrades"
                );
            }
            Ok(port)
        })
        .collect()
}

fn ensure_gateway_ports_available(
    ports: &BTreeSet<u16>,
    http3_enabled: bool,
    admin_port: u16,
) -> Result<()> {
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
    ensure_gateway_admin_port_available(admin_port)
}

fn ensure_gateway_admin_port_available(admin_port: u16) -> Result<()> {
    let admin = SocketAddrV4::new(Ipv4Addr::LOCALHOST, admin_port);
    check_gateway_port(
        TcpListener::bind(admin),
        format!("{admin}/tcp"),
        "free the local Caddy admin port or disable Gateway on this node",
    )
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

fn gateway_bootstrap(
    spec: &GatewayContainerSpec,
    admin_port: u16,
    public_listeners: bool,
) -> Result<String> {
    let mut config = gateway::config(
        &ClusterState::default(),
        &spec.gateway,
        spec.controller.clone(),
    );
    config["admin"]["listen"] = serde_json::json!(format!("127.0.0.1:{admin_port}"));
    if !public_listeners && let Some(root) = config.as_object_mut() {
        root.remove("apps");
    }
    Ok(serde_json::to_string(&config)?)
}

fn gateway_config_for_admin(config: &serde_json::Value, admin_port: u16) -> serde_json::Value {
    let mut config = config.clone();
    config["admin"]["listen"] = serde_json::json!(format!("127.0.0.1:{admin_port}"));
    config
}

fn gateway_admin_url(admin_port: u16) -> String {
    format!("http://127.0.0.1:{admin_port}")
}

fn validate_gateway_assignment(assignment: &GatewayAssignment, cluster_id: &str) -> Result<()> {
    if assignment.recovery_snapshot.generation != assignment.generation {
        bail!(
            "Gateway assignment generation {} does not match recovery snapshot generation {}",
            assignment.generation,
            assignment.recovery_snapshot.generation
        );
    }
    assignment
        .recovery_snapshot
        .validate_for_cluster(cluster_id)
        .map_err(anyhow::Error::msg)
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
        let image_id = self
            .pull_image(image, |attempt, current, total| {
                progress.report_pull(attempt, current, total);
            })
            .await?;
        progress.report(ImageResolutionStatus::Comparing);
        Ok(image_id)
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
        let container_image = if assignment.image_resolved {
            progress.report(TaskReconcilePhase::Inspect);
            let local_image = self.local_image_reference(&assignment.spec.image);
            if self.inspect_image_id(&local_image).await.is_ok() {
                local_image
            } else {
                progress.report(TaskReconcilePhase::Pull);
                self.ensure_image(&assignment.spec.image, PullPolicy::Missing, progress)
                    .await?
            }
        } else {
            progress.report(TaskReconcilePhase::Pull);
            self.ensure_image(
                &assignment.spec.image,
                assignment.spec.pull_policy,
                progress,
            )
            .await?
        };

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
            image: Some(container_image),
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

fn docker_already_stopped(error: &bollard::errors::Error) -> bool {
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
        if method == Method::GET && uri.path().contains("/images/") && uri.path().ends_with("/json")
        {
            return axum::response::Response::builder()
                .status(StatusCode::OK)
                .header(axum::http::header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"Id":"sha256:resolved-image"}"#))
                .unwrap();
        }
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

    async fn relay_pull_docker_api(
        State(calls): State<Arc<Mutex<Vec<String>>>>,
        method: Method,
        uri: Uri,
    ) -> axum::response::Response {
        calls.lock().unwrap().push(format!("{method} {uri}"));
        let path = uri.path();
        let (status, body) = if method == Method::POST && path.ends_with("/images/create") {
            (
                StatusCode::OK,
                r#"{"status":"downloaded","progressDetail":{"current":64,"total":64}}
"#,
            )
        } else if method == Method::GET && path.contains("/images/") && path.ends_with("/json") {
            (StatusCode::OK, r#"{"Id":"sha256:proxied-image"}"#)
        } else if method == Method::POST && path.ends_with("/tag") {
            (StatusCode::CREATED, "")
        } else if method == Method::DELETE && path.contains("/images/") {
            (StatusCode::OK, "[]")
        } else {
            (StatusCode::NOT_FOUND, r#"{"message":"unexpected request"}"#)
        };
        axum::response::Response::builder()
            .status(status)
            .header(axum::http::header::CONTENT_TYPE, "application/json")
            .body(Body::from(body))
            .unwrap()
    }

    async fn failing_relay_pull_docker_api(
        State(calls): State<Arc<Mutex<Vec<String>>>>,
        method: Method,
        uri: Uri,
    ) -> axum::response::Response {
        calls.lock().unwrap().push(format!("{method} {uri}"));
        let path = uri.path();
        let (status, body) = if method == Method::POST && path.ends_with("/images/create") {
            let proxied = uri
                .query()
                .and_then(|query| {
                    url::form_urlencoded::parse(query.as_bytes())
                        .find(|(name, _)| name == "fromImage")
                })
                .is_some_and(|(_, image)| image.contains("/f/ghcr.io/"));
            if proxied {
                (
                    StatusCode::OK,
                    r#"{"errorDetail":{"message":"proxy blob download failed"}}
"#,
                )
            } else {
                (
                    StatusCode::OK,
                    r#"{"status":"downloaded","progressDetail":{"current":64,"total":64}}
"#,
                )
            }
        } else if method == Method::GET && path.contains("/images/") && path.ends_with("/json") {
            (StatusCode::OK, r#"{"Id":"sha256:direct-image"}"#)
        } else if method == Method::DELETE && path.contains("/images/") {
            (StatusCode::NOT_FOUND, r#"{"message":"not found"}"#)
        } else {
            (StatusCode::NOT_FOUND, r#"{"message":"unexpected request"}"#)
        };
        axum::response::Response::builder()
            .status(status)
            .header(axum::http::header::CONTENT_TYPE, "application/json")
            .body(Body::from(body))
            .unwrap()
    }

    async fn image_proxy_probe(method: Method, uri: Uri) -> axum::response::Response {
        let mut builder = axum::response::Response::builder().status(StatusCode::OK);
        if uri.path() == "/v2/" {
            builder = builder.header("x-swarmlite-image-proxy", "enabled");
        } else if method != Method::HEAD || !uri.path().contains("/manifests/") {
            builder = builder.status(StatusCode::NOT_FOUND);
        }
        builder.body(Body::empty()).unwrap()
    }

    async fn unavailable_image_proxy(method: Method, uri: Uri) -> axum::response::Response {
        let mut builder = axum::response::Response::builder();
        if uri.path() == "/v2/" {
            builder = builder
                .status(StatusCode::OK)
                .header("x-swarmlite-image-proxy", "enabled");
        } else if method == Method::HEAD && uri.path().contains("/manifests/") {
            builder = builder.status(StatusCode::BAD_GATEWAY);
        } else {
            builder = builder.status(StatusCode::NOT_FOUND);
        }
        builder.body(Body::empty()).unwrap()
    }

    async fn start_test_server(app: Router) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (address.to_string(), server)
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
                image_relay: None,
                relay_http: None,
                podman_http: None,
                prepared_images: Arc::new(std::sync::RwLock::new(HashMap::new())),
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
                image_relay: None,
                relay_http: None,
                podman_http: None,
                prepared_images: Arc::new(std::sync::RwLock::new(HashMap::new())),
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
    async fn reachable_configured_proxy_retags_and_removes_the_relay_reference() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let docker_app = Router::new()
            .fallback(any(relay_pull_docker_api))
            .with_state(calls.clone());
        let (mut runtime, docker_server) =
            runtime_for_pull_api(docker_app, DeploymentPolicy::default()).await;
        let (proxy, proxy_server) =
            start_test_server(Router::new().fallback(any(image_proxy_probe))).await;
        runtime.image_relay = Some(proxy.clone());
        runtime.relay_http = Some(reqwest::Client::builder().no_proxy().build().unwrap());

        runtime
            .pull_image("ghcr.io/acme/api:1.2", |_, _, _| {})
            .await
            .unwrap();
        docker_server.abort();
        proxy_server.abort();

        let calls = calls.lock().unwrap();
        let create = calls
            .iter()
            .find(|call| call.starts_with("POST ") && call.contains("/images/create?"))
            .unwrap();
        let query = create.split_once('?').unwrap().1;
        let from_image = url::form_urlencoded::parse(query.as_bytes())
            .find(|(name, _)| name == "fromImage")
            .map(|(_, value)| value.into_owned())
            .unwrap();
        assert_eq!(from_image, format!("{proxy}/f/ghcr.io/acme/api:1.2"));
        let tag = calls.iter().find(|call| call.contains("/tag?")).unwrap();
        assert!(tag.contains("repo=ghcr.io%2Facme%2Fapi"));
        assert!(tag.contains("tag=1.2"));
        assert!(calls.iter().any(|call| call.starts_with("DELETE ")));
    }

    #[tokio::test]
    async fn unreachable_proxy_uses_the_original_runtime_pull_path() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let docker_app = Router::new()
            .fallback(any(relay_pull_docker_api))
            .with_state(calls.clone());
        let (mut runtime, docker_server) =
            runtime_for_pull_api(docker_app, DeploymentPolicy::default()).await;
        let (proxy, proxy_server) =
            start_test_server(Router::new().fallback(any(unavailable_image_proxy))).await;
        runtime.image_relay = Some(proxy);
        runtime.relay_http = Some(reqwest::Client::builder().no_proxy().build().unwrap());

        runtime
            .pull_image("ghcr.io/acme/api:1.2", |_, _, _| {})
            .await
            .unwrap();
        docker_server.abort();
        proxy_server.abort();

        let calls = calls.lock().unwrap();
        let create = calls
            .iter()
            .find(|call| call.starts_with("POST ") && call.contains("/images/create?"))
            .unwrap();
        let query = create.split_once('?').unwrap().1;
        let from_image = url::form_urlencoded::parse(query.as_bytes())
            .find(|(name, _)| name == "fromImage")
            .map(|(_, value)| value.into_owned())
            .unwrap();
        assert_eq!(from_image, "ghcr.io/acme/api:1.2");
        assert!(!calls.iter().any(|call| call.contains("/tag?")));
        assert!(!calls.iter().any(|call| call.starts_with("DELETE ")));
    }

    #[tokio::test]
    async fn proxy_pull_failure_falls_back_to_the_original_runtime_pull() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let docker_app = Router::new()
            .fallback(any(failing_relay_pull_docker_api))
            .with_state(calls.clone());
        let (mut runtime, docker_server) =
            runtime_for_pull_api(docker_app, DeploymentPolicy::default()).await;
        let (proxy, proxy_server) =
            start_test_server(Router::new().fallback(any(image_proxy_probe))).await;
        runtime.image_relay = Some(proxy);
        runtime.relay_http = Some(reqwest::Client::builder().no_proxy().build().unwrap());

        runtime
            .pull_image("ghcr.io/acme/api:1.2", |_, _, _| {})
            .await
            .unwrap();
        docker_server.abort();
        proxy_server.abort();

        let calls = calls.lock().unwrap();
        let pulls = calls
            .iter()
            .filter(|call| call.starts_with("POST ") && call.contains("/images/create?"))
            .collect::<Vec<_>>();
        assert_eq!(pulls.len(), 2);
        assert!(pulls[0].contains("%2Ff%2Fghcr.io%2Facme%2Fapi"));
        assert!(pulls[1].contains("fromImage=ghcr.io%2Facme%2Fapi%3A1.2"));
        assert!(!calls.iter().any(|call| call.contains("/tag?")));
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
            .create_gateway(
                &test_gateway_spec(),
                "gateway-image-id",
                GatewaySlot::Blue,
                "runtime-spec-hash",
            )
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
        let runtime_hash = gateway_runtime_spec_hash(&spec).unwrap();
        let labels = gateway_labels(&spec, GatewaySlot::Blue, &runtime_hash);
        assert_eq!(labels[MANAGED_LABEL], "true");
        assert_eq!(labels[CLUSTER_LABEL], "cluster-old");
        assert_eq!(labels[SYSTEM_LABEL], "true");
        assert_eq!(labels[COMPONENT_LABEL], GATEWAY_COMPONENT);
        assert_eq!(labels[GATEWAY_ADDRESS_LABEL], "10.0.0.21");
        assert_eq!(labels[GATEWAY_NODE_LABEL], "node-a");
        assert_eq!(labels[GATEWAY_SLOT_LABEL], "blue");
        assert_eq!(labels[GATEWAY_RUNTIME_SPEC_LABEL], runtime_hash);
        assert!(!labels.contains_key("io.swarmlite.gateway_schema"));
        assert!(!labels.contains_key("io.swarmlite.gateway_autosave_schema"));
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
    fn gateway_runtime_hash_ignores_hot_config_but_tracks_immutable_inputs() {
        let mut spec = test_gateway_spec();
        let original = gateway_runtime_spec_hash(&spec).unwrap();
        spec.gateway.image = "custom-caddy:v1".into();
        assert_eq!(gateway_runtime_spec_hash(&spec).unwrap(), original);
        spec.gateway.listen.push(":443".into());
        spec.gateway.http.http3_enabled = Some(false);
        spec.controller = "http://10.0.0.22:17080".into();
        spec.advertise_address = "10.0.0.22".into();
        assert_eq!(gateway_runtime_spec_hash(&spec).unwrap(), original);
        spec.token = "fedcba9876543210".into();
        assert_ne!(gateway_runtime_spec_hash(&spec).unwrap(), original);
    }

    #[test]
    fn maps_gateway_listeners_to_published_ports() {
        assert_eq!(
            gateway_ports(&[":80".into(), "0.0.0.0:443".into()]).unwrap(),
            BTreeSet::from([80, 443])
        );
        assert!(gateway_ports(&[":2019".into()]).is_err());
        assert!(gateway_ports(&[":2020".into()]).is_err());
        assert!(gateway_ports(&[":2021".into()]).is_err());
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

        let error =
            ensure_gateway_ports_available(&BTreeSet::from([port]), true, 2019).unwrap_err();

        assert!(format!("{error:#}").contains(&format!("0.0.0.0:{port}/tcp")));
        assert!(!gateway_error_is_retryable(&error));
    }

    #[test]
    fn scopes_gateway_volumes_to_the_cluster() {
        assert_eq!(
            gateway_volume_names("cluster-old", Some(GatewaySlot::Blue)),
            [
                "swarmlite-gateway-cluster-old-blue-data".to_owned(),
                "swarmlite-gateway-cluster-old-blue-config".to_owned(),
                "swarmlite-gateway-cluster-old-blue-cache".to_owned(),
            ]
        );
        assert_eq!(
            gateway_volume_names("cluster-old", None)[0],
            "swarmlite-gateway-cluster-old-data"
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
    fn gateway_bootstrap_stages_only_the_admin_and_storage_planes() {
        let mut spec = test_gateway_spec();
        spec.token = "do-not-persist-this-token".into();
        let encoded = gateway_bootstrap(&spec, 2020, false).unwrap();
        let value: serde_json::Value = serde_json::from_str(&encoded).unwrap();
        assert_eq!(value["admin"]["listen"], "127.0.0.1:2020");
        assert_eq!(value["admin"]["config"]["persist"], true);
        assert_eq!(value["storage"]["module"], "swarmlite");
        assert_eq!(value["storage"]["controller"], "http://10.0.0.21:17080");
        assert!(value["storage"].get("controllers").is_none());
        assert_eq!(value["storage"]["token_env"], "SWARMLITE_TOKEN");
        assert_eq!(value["storage"]["gateway_id_env"], "SWARMLITE_GATEWAY_ID");
        assert_eq!(value["storage"]["probe_timeout"], "2s");
        assert_eq!(value["storage"]["owner_cache_ttl"], "1m");
        assert!(value.get("apps").is_none());
        assert!(!encoded.contains("do-not-persist-this-token"));
    }

    #[test]
    fn gateway_final_config_keeps_public_apps_and_uses_the_requested_admin_port() {
        let spec = test_gateway_spec();
        let config = gateway::config(
            &ClusterState::default(),
            &spec.gateway,
            spec.controller.clone(),
        );
        let value = gateway_config_for_admin(&config, 2020);
        assert_eq!(value["admin"]["listen"], "127.0.0.1:2020");
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
