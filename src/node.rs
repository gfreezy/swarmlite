use std::{
    collections::BTreeMap,
    env,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot, watch};
use tracing::{info, warn};
use url::Url;
use uuid::Uuid;

use crate::{
    agent,
    client::ControllerClient,
    config::{
        AgentConfig, ControllerConfig, DEFAULT_DEPLOYMENT_TIMEOUT_SECONDS,
        DEFAULT_GATEWAY_DRAIN_TIMEOUT_SECONDS, PortRangeConfig, RuntimeConfig, RuntimeKind,
    },
    controller,
    local_state::{AgentFence, DATABASE_FILE, FENCE_KEY, LocalState, NODE_KEY},
    model::{
        BootstrapResponse, CLUSTER_SCHEMA_VERSION, ClusterSettings, GatewayReport, JoinRequest,
        JoinResponse, NodeControl, valid_gateway_image,
    },
    registry::{RegistryCredentialStore, credentials_hash},
    runtime::{
        ContainerRuntime, DockerCompatibleRuntime, GatewayContainerSpec, ManagedClusterInventory,
    },
    storage::{StateRepository, control_plane_state_exists},
};

const NODE_LOCK_FILE: &str = "serve.lock";
const NODE_SETTINGS_SCHEMA_VERSION: u32 = 8;
const LEGACY_LOCAL_STATE_FILE: &str = "local.sqlite";
const LEGACY_CONTROL_PLANE_FILE: &str = "control-plane.sqlite";

#[derive(Debug, Clone)]
pub struct InitOptions {
    pub data_dir: PathBuf,
    pub cluster: ClusterSettings,
    pub token: Option<String>,
    pub advertise_address: Option<String>,
    pub runtime: Option<RuntimeKind>,
    pub runtime_socket: Option<String>,
    pub labels: BTreeMap<String, String>,
    pub recovery: bool,
    pub gateway_image_explicit: bool,
    pub gateway_enabled: bool,
}

#[derive(Debug, Clone)]
pub struct ServeOptions {
    pub data_dir: PathBuf,
    pub advertise_address: Option<String>,
    pub runtime: Option<RuntimeKind>,
    pub runtime_socket: Option<String>,
}

#[derive(Debug, Clone)]
pub struct JoinOptions {
    pub data_dir: PathBuf,
    pub controller: String,
    pub token: String,
    pub advertise_address: Option<String>,
    pub runtime: Option<RuntimeKind>,
    pub runtime_socket: Option<String>,
    pub labels: BTreeMap<String, String>,
    pub gateway_enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NodeSettings {
    schema_version: u32,
    gateway_enabled: bool,
    cluster: ClusterSettings,
    node_id: String,
    token: String,
    controller_url: String,
    advertise_address: Option<String>,
    runtime: Option<RuntimeConfig>,
    labels: BTreeMap<String, String>,
}

enum NodeEvent {
    Agent(Result<()>),
    Controller(Result<()>),
}

struct NodeSupervisor {
    data_dir: PathBuf,
    local_state: LocalState,
    settings: NodeSettings,
    public_controller: String,
    control_rx: watch::Receiver<NodeControl>,
    events_tx: mpsc::UnboundedSender<NodeEvent>,
    events_rx: mpsc::UnboundedReceiver<NodeEvent>,
    agent_handle: tokio::task::JoinHandle<()>,
    runtime: DockerCompatibleRuntime,
    gateway_report_tx: watch::Sender<GatewayReport>,
    advertise_address: String,
}

pub fn resolve_data_dir(explicit: Option<PathBuf>) -> Result<PathBuf> {
    if let Some(path) = explicit {
        return Ok(path);
    }
    if let Some(path) = nonempty_env("SWARMLITE_DATA_DIR") {
        return Ok(PathBuf::from(path));
    }
    if let Some(path) = nonempty_env("XDG_STATE_HOME") {
        return Ok(PathBuf::from(path).join("swarmlite"));
    }
    if let Some(path) = nonempty_env("HOME") {
        return Ok(PathBuf::from(path).join(".local/state/swarmlite"));
    }
    Ok(env::current_dir()
        .context("failed to determine the current directory")?
        .join(".swarmlite"))
}

pub async fn init(mut options: InitOptions) -> Result<String> {
    if let Some(address) = options.advertise_address.as_deref() {
        resolve_advertise_address(Some(address))?;
    }
    prepare_data_dir(&options.data_dir).await?;
    let _node_lock = acquire_node_lock(&options.data_dir, "init")?;
    ensure_controller_port_is_available(options.cluster.controller_port)?;
    let inventory = inspect_runtime_before_init(&options).await?;
    let local_state = if options.recovery {
        let local_cluster_id = recovery_local_cluster_id(&options.data_dir, &inventory).await?;
        options.cluster.cluster_id = recovery_cluster_id(&inventory, local_cluster_id.as_deref())?;
        recover_gateway_config(
            &mut options.cluster,
            &inventory,
            options.gateway_image_explicit,
        );
        validate_managed_inventory(&inventory, &options.cluster.cluster_id, true)?;
        archive_control_plane_state(&options.data_dir).await?;
        LocalState::open(&options.data_dir)?
    } else {
        ensure_control_plane_data_is_absent(&options.data_dir)?;
        let state = open_local_state(&options.data_dir).await?;
        if state.get::<NodeSettings>(NODE_KEY)?.is_some() {
            bail!(
                "{} is already initialized; run `swarmlite serve` instead of init",
                options.data_dir.display()
            );
        }
        validate_managed_inventory(&inventory, &options.cluster.cluster_id, false)?;
        state
    };
    let node_id = default_node_id();
    options.cluster.controller_id.clone_from(&node_id);
    validate_cluster(&options.cluster)?;

    let token = options
        .token
        .or_else(|| nonempty_env("SWARMLITE_TOKEN"))
        .unwrap_or_else(generate_token);
    if token.len() < 16 {
        bail!("the cluster token must contain at least 16 bytes");
    }
    let settings = NodeSettings {
        schema_version: NODE_SETTINGS_SCHEMA_VERSION,
        gateway_enabled: options.gateway_enabled,
        cluster: options.cluster.clone(),
        node_id,
        token: token.clone(),
        controller_url: String::new(),
        advertise_address: options.advertise_address,
        runtime: requested_runtime(options.runtime, options.runtime_socket.as_deref()),
        labels: options.labels,
    };
    local_state.put_pair((NODE_KEY, &settings), (FENCE_KEY, &AgentFence::default()))?;
    Ok(format!(
        "initialized {}single-controller cluster {} as {}; gateway {}; run `swarmlite serve` (join token: {token})",
        if options.recovery { "recovered " } else { "" },
        settings.cluster.cluster_id,
        settings.node_id,
        if settings.gateway_enabled {
            "enabled"
        } else {
            "disabled"
        },
    ))
}

pub async fn run(options: ServeOptions) -> Result<()> {
    prepare_data_dir(&options.data_dir).await?;
    let _node_lock = acquire_node_lock(&options.data_dir, "serve")?;
    let local_state = open_local_state(&options.data_dir).await?;
    let mut settings = load_node_settings_from(&local_state)?;

    let mut changed = false;
    if let Some(address) = &options.advertise_address {
        resolve_advertise_address(Some(address))?;
        settings.advertise_address = Some(address.clone());
        changed = true;
    }
    if let Some(runtime) = requested_runtime(options.runtime, options.runtime_socket.as_deref()) {
        settings.runtime = Some(runtime);
        changed = true;
    }
    if changed {
        local_state.put(NODE_KEY, &settings)?;
    }

    let advertise_address = resolve_advertise_address(settings.advertise_address.as_deref())?;
    let public_controller = controller_url(&advertise_address, settings.cluster.controller_port);
    let local_controller = format!("http://127.0.0.1:{}", settings.cluster.controller_port);
    let runtime_config = resolve_runtime(
        options.runtime,
        options.runtime_socket.as_deref(),
        settings.runtime.as_ref(),
    );
    let registry_credentials = RegistryCredentialStore::new(local_state.clone());
    let runtime = DockerCompatibleRuntime::connect_with_registry_credentials(
        &runtime_config.resolve()?,
        registry_credentials.clone(),
    )?;
    runtime
        .reconcile_gateway(
            &gateway_container_spec(&settings, &advertise_address, &public_controller)?,
            settings.gateway_enabled,
        )
        .await?;
    let agent_controller = if is_controller(&settings) {
        local_controller
    } else {
        settings.controller_url.clone()
    };
    if agent_controller.is_empty() {
        bail!("node configuration has no controller addresses");
    }

    let agent_config = AgentConfig {
        cluster_id: settings.cluster.cluster_id.clone(),
        node_id: settings.node_id.clone(),
        advertise_address: advertise_address.clone(),
        controller: agent_controller,
        labels: settings.labels.clone(),
        heartbeat_interval_seconds: 2,
        port_range: PortRangeConfig::default(),
        gateway_enabled: settings.gateway_enabled,
    };
    let initial_control = NodeControl {
        cluster: settings.cluster.clone(),
        gateway_enabled: settings.gateway_enabled,
        labels: settings.labels.clone(),
        gateway_config: None,
        registry_credentials_hash: credentials_hash(&registry_credentials.snapshot()?),
    };
    let (control_tx, control_rx) = watch::channel(initial_control);
    let (gateway_report_tx, gateway_report_rx) = watch::channel(GatewayReport::default());
    let (events_tx, events_rx) = mpsc::unbounded_channel();
    let token = settings.token.clone();
    let agent_events = events_tx.clone();
    let agent_local_state = local_state.clone();
    let agent_runtime = runtime.clone();
    let agent_handle = tokio::spawn(async move {
        let result = agent::run_with_token_and_updates(
            agent_config,
            token,
            control_tx,
            gateway_report_rx,
            agent_local_state,
            agent_runtime,
        )
        .await;
        let _ = agent_events.send(NodeEvent::Agent(result));
    });

    info!(
        node_id = %settings.node_id,
        controller = is_controller(&settings),
        gateway_enabled = settings.gateway_enabled,
        address = %advertise_address,
        "starting node service"
    );
    NodeSupervisor {
        data_dir: options.data_dir,
        local_state,
        settings,
        public_controller,
        control_rx,
        events_tx,
        events_rx,
        agent_handle,
        runtime,
        gateway_report_tx,
        advertise_address,
    }
    .run()
    .await
}

impl NodeSupervisor {
    async fn run(self) -> Result<()> {
        let Self {
            data_dir,
            local_state,
            mut settings,
            public_controller,
            mut control_rx,
            events_tx,
            mut events_rx,
            agent_handle,
            runtime,
            gateway_report_tx,
            advertise_address,
        } = self;
        let has_controller = is_controller(&settings);
        let mut controller_shutdown = None;
        if has_controller {
            controller_shutdown = Some(
                start_controller(&data_dir, &settings, &public_controller, events_tx.clone())
                    .await?,
            );
        }
        if settings.gateway_enabled {
            info!("gateway enabled; independent Caddy container is running");
        }

        loop {
            tokio::select! {
                signal = tokio::signal::ctrl_c() => {
                    if let Err(error) = signal {
                        warn!(%error, "failed to listen for shutdown signal");
                    }
                    if let Some(shutdown) = controller_shutdown.take() {
                        let _ = shutdown.send(());
                    }
                    agent_handle.abort();
                    return Ok(());
                }
                changed = control_rx.changed() => {
                    if changed.is_err() {
                        bail!("node agent stopped publishing control updates");
                    }
                    let control = control_rx.borrow_and_update().clone();
                    let had_gateway = settings.gateway_enabled;
                    if control.cluster.controller_id != settings.cluster.controller_id {
                        bail!("controller identity changes are not supported; replace the cluster to move the controller");
                    }
                    settings.gateway_enabled = control.gateway_enabled;
                    settings.labels = control.labels;
                    settings.cluster = control.cluster;
                    local_state.put(NODE_KEY, &settings)?;
                    let has_gateway = settings.gateway_enabled;
                    if has_gateway {
                        runtime
                            .reconcile_gateway(
                                &gateway_container_spec(
                                    &settings,
                                    &advertise_address,
                                    &public_controller,
                                )?,
                                true,
                            )
                            .await?;
                        if let Some(assignment) = &control.gateway_config {
                            let report = match runtime.apply_gateway_config(assignment).await {
                                Ok(()) => GatewayReport {
                                    applied_generation: Some(assignment.generation),
                                    error: None,
                                },
                                Err(error) => {
                                    let error = format!("{error:#}");
                                    warn!(%error, "failed to apply local gateway configuration");
                                    GatewayReport {
                                        applied_generation: None,
                                        error: Some(error),
                                    }
                                }
                            };
                            gateway_report_tx.send_replace(report);
                        }
                        if !had_gateway {
                            info!("gateway enabled; started independent Caddy container");
                        }
                    } else if had_gateway && !has_gateway {
                        runtime
                            .reconcile_gateway(
                                &gateway_container_spec(
                                    &settings,
                                    &advertise_address,
                                    &public_controller,
                                )?,
                                false,
                            )
                            .await?;
                        info!("gateway disabled; removed independent Caddy container");
                        gateway_report_tx.send_replace(GatewayReport::default());
                    }
                }
                event = events_rx.recv() => {
                    match event.context("node task event channel closed")? {
                        NodeEvent::Agent(result) => {
                            if let Some(shutdown) = controller_shutdown.take() {
                                let _ = shutdown.send(());
                            }
                            agent_handle.abort();
                            return result.context("node agent stopped");
                        }
                        NodeEvent::Controller(result) => {
                            drop(controller_shutdown.take());
                            result.context("controller stopped")?;
                            bail!("controller stopped unexpectedly");
                        }
                    }
                }
            }
        }
    }
}

fn gateway_container_spec(
    settings: &NodeSettings,
    advertise_address: &str,
    public_controller: &str,
) -> Result<GatewayContainerSpec> {
    let controller = if is_controller(settings) {
        public_controller.to_owned()
    } else {
        settings.controller_url.clone()
    };
    Ok(GatewayContainerSpec {
        cluster_id: settings.cluster.cluster_id.clone(),
        advertise_address: advertise_address.to_owned(),
        listen: settings.cluster.gateway.listen.clone(),
        controller,
        token: settings.token.clone(),
        image: settings.cluster.gateway.image.clone(),
    })
}

async fn start_controller(
    data_dir: &Path,
    settings: &NodeSettings,
    public_controller: &str,
    events: mpsc::UnboundedSender<NodeEvent>,
) -> Result<oneshot::Sender<()>> {
    let cluster = settings.cluster.clone();
    let repository =
        StateRepository::open(data_dir, cluster.clone()).map_err(anyhow::Error::msg)?;
    let config = ControllerConfig {
        gateway_enabled: settings.gateway_enabled,
        labels: settings.labels.clone(),
        listen: SocketAddr::new(
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            settings.cluster.controller_port,
        ),
        advertise_url: public_controller.to_owned(),
        node_timeout_seconds: 20,
        reconcile_interval_seconds: 1,
        gateway_drain_timeout_seconds: DEFAULT_GATEWAY_DRAIN_TIMEOUT_SECONDS,
        deployment_timeout_seconds: DEFAULT_DEPLOYMENT_TIMEOUT_SECONDS,
        cluster,
    };
    let token = settings.token.clone();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        let result = controller::run_with_repository_and_token_until(
            config,
            token,
            repository,
            async move {
                let _ = shutdown_rx.await;
            },
        )
        .await;
        let _ = events.send(NodeEvent::Controller(result));
    });
    Ok(shutdown_tx)
}

pub async fn join(options: JoinOptions) -> Result<String> {
    prepare_data_dir(&options.data_dir).await?;
    let _node_lock = acquire_node_lock(&options.data_dir, "join")?;
    let seed = normalize_controller_url(&options.controller)?;
    if options.token.len() < 16 {
        bail!("the cluster token must contain at least 16 bytes");
    }
    let bootstrap = fetch_bootstrap(&seed, &options.token).await?;
    validate_cluster(&bootstrap.cluster)?;

    let mut local_state = open_local_state(&options.data_dir).await?;
    let existing = local_state.get::<NodeSettings>(NODE_KEY)?;
    if let Some(existing) = &existing
        && existing.cluster.cluster_id != bootstrap.cluster.cluster_id
    {
        bail!(
            "this node belongs to cluster {}; refusing to join cluster {}",
            existing.cluster.cluster_id,
            bootstrap.cluster.cluster_id
        );
    }
    let recovery_rebind = existing
        .as_ref()
        .is_some_and(|settings| settings.token != options.token);
    let stale_control_plane_without_identity =
        existing.is_none() && control_plane_data_is_present(&options.data_dir)?;
    if recovery_rebind || stale_control_plane_without_identity {
        drop(local_state);
        archive_control_plane_state(&options.data_dir).await?;
        local_state = LocalState::open(&options.data_dir)?;
    }

    let advertise_address = resolve_advertise_address(
        options
            .advertise_address
            .as_deref()
            .or_else(|| existing.as_ref()?.advertise_address.as_deref()),
    )?;
    let preserve_identity = existing.as_ref().filter(|_| !recovery_rebind);
    let node_id =
        preserve_identity.map_or_else(default_node_id, |settings| settings.node_id.clone());
    let runtime = requested_runtime(options.runtime, options.runtime_socket.as_deref())
        .or_else(|| preserve_identity.and_then(|settings| settings.runtime.clone()));
    let recovered_gateway = if !options.gateway_enabled
        && !preserve_identity.is_some_and(|settings| settings.gateway_enabled)
    {
        let detection_runtime = resolve_runtime(
            options.runtime,
            options.runtime_socket.as_deref(),
            runtime.as_ref(),
        );
        detect_recovered_gateway(&detection_runtime, &bootstrap.cluster.cluster_id).await
    } else {
        false
    };
    let gateway_enabled = options.gateway_enabled
        || preserve_identity.is_some_and(|settings| settings.gateway_enabled)
        || recovered_gateway;
    let request = JoinRequest {
        node_id: node_id.clone(),
        address: advertise_address.clone(),
        gateway_enabled,
        labels: options.labels.clone(),
    };
    let response = send_join(&seed, &options.token, &request).await?;
    if response.cluster != bootstrap.cluster {
        bail!("cluster settings changed during join; retry the command");
    }
    RegistryCredentialStore::new(local_state.clone()).replace(&response.registry_credentials)?;
    let settings = NodeSettings {
        schema_version: NODE_SETTINGS_SCHEMA_VERSION,
        gateway_enabled: response.gateway_enabled,
        cluster: response.cluster,
        node_id: node_id.clone(),
        token: options.token.clone(),
        controller_url: seed,
        advertise_address: Some(advertise_address),
        runtime,
        labels: response.labels,
    };
    if recovery_rebind || existing.is_none() {
        local_state.put_pair((NODE_KEY, &settings), (FENCE_KEY, &AgentFence::default()))?;
    } else {
        local_state.put(NODE_KEY, &settings)?;
    }
    Ok(format!(
        "{} cluster {} as {node_id}; gateway {}; run `swarmlite serve`",
        if recovery_rebind {
            "rejoined recovered"
        } else {
            "joined"
        },
        settings.cluster.cluster_id,
        if settings.gateway_enabled {
            "enabled"
        } else {
            "disabled"
        }
    ))
}

async fn detect_recovered_gateway(runtime: &RuntimeConfig, cluster_id: &str) -> bool {
    let resolved = match runtime.resolve() {
        Ok(resolved) => resolved,
        Err(error) => {
            warn!(%error, "could not inspect system containers while joining");
            return false;
        }
    };
    if !Path::new(&resolved.socket).exists() {
        return false;
    }
    let runtime = match DockerCompatibleRuntime::connect(&resolved) {
        Ok(runtime) => runtime,
        Err(error) => {
            warn!(%error, "could not connect to the runtime while collecting system containers");
            return false;
        }
    };
    let inventory = match runtime.managed_cluster_inventory().await {
        Ok(inventory) => inventory,
        Err(error) => {
            warn!(%error, "could not inspect system containers while joining");
            return false;
        }
    };
    if inventory.gateway_cluster_ids.contains(cluster_id) {
        info!(cluster_id, "collected an existing gateway system container");
        true
    } else {
        false
    }
}

pub async fn join_command(data_dir: &Path) -> Result<String> {
    let settings = read_node_settings(data_dir).await?;
    let controller = if is_controller(&settings) {
        let address = resolve_advertise_address(settings.advertise_address.as_deref())?;
        controller_url(&address, settings.cluster.controller_port)
    } else {
        (!settings.controller_url.is_empty())
            .then_some(settings.controller_url.clone())
            .context("the node does not know a controller join address")?
    };
    Ok(format!(
        "swarmlite join {} --token {}",
        controller, settings.token
    ))
}

pub async fn resolve_connection(
    data_dir: &Path,
    controller: Option<String>,
    token: Option<String>,
) -> Result<(String, String)> {
    if let (Some(controller), Some(token)) = (controller.as_ref(), token.as_ref()) {
        return Ok((normalize_controller_url(controller)?, token.clone()));
    }
    let settings = read_node_settings(data_dir).await.with_context(|| {
        "controller or token was omitted and local state is unavailable; run init/join first or pass both options"
    })?;
    let saved_controller = if is_controller(&settings) {
        format!("http://127.0.0.1:{}", settings.cluster.controller_port)
    } else {
        (!settings.controller_url.is_empty())
            .then_some(settings.controller_url.clone())
            .context("local state does not contain a controller address")?
    };
    Ok((
        normalize_controller_url(controller.as_deref().unwrap_or(&saved_controller))?,
        token.unwrap_or(settings.token),
    ))
}

pub fn parse_labels(values: Vec<String>) -> Result<BTreeMap<String, String>> {
    values
        .into_iter()
        .map(|value| {
            let (key, value) = value
                .split_once('=')
                .with_context(|| format!("label must use KEY=VALUE syntax: {value}"))?;
            if key.is_empty()
                || key.len() > 256
                || key.trim() != key
                || key.chars().any(char::is_control)
            {
                bail!(
                    "label key must contain 1 to 256 bytes without control characters or surrounding whitespace"
                );
            }
            if value.len() > 4_096 || value.chars().any(char::is_control) {
                bail!("label value must contain at most 4096 bytes without control characters");
            }
            Ok((key.to_owned(), value.to_owned()))
        })
        .collect()
}

pub fn new_cluster_id() -> String {
    format!("cluster-{}", Uuid::new_v4().simple())
}

async fn load_node_settings(data_dir: &Path) -> Result<NodeSettings> {
    let local_state = open_local_state(data_dir).await?;
    load_node_settings_from(&local_state)
}

async fn read_node_settings(data_dir: &Path) -> Result<NodeSettings> {
    if let Some(settings) = LocalState::get_read_only::<NodeSettings>(data_dir, NODE_KEY)? {
        validate_node_settings(&settings)?;
        return Ok(settings);
    }
    load_node_settings(data_dir).await
}

fn load_node_settings_from(local_state: &LocalState) -> Result<NodeSettings> {
    let settings = local_state
        .get::<NodeSettings>(NODE_KEY)?
        .context("this node is not initialized; run `swarmlite init` or `swarmlite join` first")?;
    validate_node_settings(&settings)?;
    Ok(settings)
}

async fn fetch_bootstrap(controller: &str, token: &str) -> Result<BootstrapResponse> {
    Ok(ControllerClient::new(controller, token)
        .get_json("/v1/cluster")
        .await?)
}

async fn send_join(controller: &str, token: &str, request: &JoinRequest) -> Result<JoinResponse> {
    Ok(ControllerClient::new(controller, token)
        .send_json(
            reqwest::Method::PUT,
            &format!("/v1/nodes/{}/join", request.node_id),
            Some(request),
        )
        .await?)
}

fn validate_cluster(cluster: &ClusterSettings) -> Result<()> {
    if cluster.schema_version != CLUSTER_SCHEMA_VERSION
        || !valid_cluster_id(&cluster.cluster_id)
        || cluster.controller_id.trim().is_empty()
    {
        bail!("invalid cluster identity or schema version");
    }
    if cluster.controller_port == 0 {
        bail!("controller port must be greater than zero");
    }
    if cluster.gateway.listen.is_empty()
        || cluster
            .gateway
            .listen
            .iter()
            .any(|listen| listen.trim().is_empty())
    {
        bail!("gateway.listen must contain at least one non-empty address");
    }
    if !valid_gateway_image(&cluster.gateway.image) {
        bail!("gateway.image must be a non-empty OCI image reference without whitespace");
    }
    Ok(())
}

fn valid_cluster_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
}

fn ensure_control_plane_data_is_absent(data_dir: &Path) -> Result<()> {
    if control_plane_data_is_present(data_dir)? {
        bail!(
            "{} contains control-plane data; run `swarmlite serve`, or use `swarmlite init --recover` after stopping the old cluster",
            data_dir.display()
        );
    }
    Ok(())
}

fn ensure_controller_port_is_available(port: u16) -> Result<()> {
    let listener = std::net::TcpListener::bind((Ipv4Addr::UNSPECIFIED, port)).with_context(|| {
        format!(
            "controller port {port} is already in use; another Swarmlite controller may already be running"
        )
    })?;
    drop(listener);
    Ok(())
}

async fn inspect_runtime_before_init(options: &InitOptions) -> Result<ManagedClusterInventory> {
    let requested = requested_runtime(options.runtime, options.runtime_socket.as_deref());
    let runtime = resolve_runtime(
        options.runtime,
        options.runtime_socket.as_deref(),
        requested.as_ref(),
    );
    let resolved = runtime.resolve()?;
    if !Path::new(&resolved.socket).exists() {
        if options.runtime.is_some() || options.runtime_socket.is_some() {
            bail!(
                "container runtime socket {} does not exist",
                resolved.socket
            );
        }
        return Ok(ManagedClusterInventory::default());
    }
    let runtime = DockerCompatibleRuntime::connect(&resolved)?;
    runtime.ping().await?;
    runtime.managed_cluster_inventory().await
}

fn validate_managed_inventory(
    inventory: &ManagedClusterInventory,
    cluster_id: &str,
    recovery: bool,
) -> Result<()> {
    if inventory.unlabeled > 0 {
        bail!(
            "found {} managed container(s) without the required cluster_id label; move or remove them before init",
            inventory.unlabeled
        );
    }
    if recovery {
        let mismatched = inventory
            .cluster_ids
            .iter()
            .filter(|existing| existing.as_str() != cluster_id)
            .cloned()
            .collect::<Vec<_>>();
        if !mismatched.is_empty() {
            bail!(
                "managed containers belong to different cluster(s): {}; recovery requested {}",
                mismatched.join(", "),
                cluster_id
            );
        }
    } else if !inventory.cluster_ids.is_empty() {
        let clusters = inventory.cluster_ids.iter().cloned().collect::<Vec<_>>();
        bail!(
            "found existing Swarmlite container(s) for cluster(s) {}; recover them with `swarmlite init --recover`",
            clusters.join(", ")
        );
    }
    Ok(())
}

fn recovery_cluster_id(
    inventory: &ManagedClusterInventory,
    local_cluster_id: Option<&str>,
) -> Result<String> {
    if inventory.unlabeled > 0 {
        bail!(
            "found {} managed container(s) without the required cluster_id label; recovery cannot identify them safely",
            inventory.unlabeled
        );
    }
    if inventory.cluster_ids.len() > 1 {
        bail!(
            "managed containers belong to multiple clusters: {}; isolate one cluster before recovery",
            inventory
                .cluster_ids
                .iter()
                .cloned()
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    let runtime_cluster_id = inventory.cluster_ids.first().map(String::as_str);
    if let (Some(runtime), Some(local)) = (runtime_cluster_id, local_cluster_id)
        && runtime != local
    {
        bail!("local state belongs to cluster {local}, but managed containers belong to {runtime}");
    }
    runtime_cluster_id
        .or(local_cluster_id)
        .map(ToOwned::to_owned)
        .context(
            "recovery could not find a cluster ID in local state or managed containers; run ordinary `swarmlite init` for a new cluster",
        )
}

fn recover_gateway_config(
    cluster: &mut ClusterSettings,
    inventory: &ManagedClusterInventory,
    image_explicit: bool,
) {
    if let Some(listen) = inventory.gateway_listen.get(&cluster.cluster_id) {
        cluster.gateway.listen.clone_from(listen);
    }
    if !image_explicit && let Some(image) = inventory.gateway_images.get(&cluster.cluster_id) {
        cluster.gateway.image.clone_from(image);
    }
}

async fn existing_local_cluster_id(data_dir: &Path) -> Result<Option<String>> {
    if let Some(local_state) = LocalState::open_existing(data_dir)?
        && let Some(settings) = local_state.get::<NodeSettings>(NODE_KEY)?
    {
        validate_node_settings(&settings)?;
        return Ok(Some(settings.cluster.cluster_id));
    }
    Ok(None)
}

async fn recovery_local_cluster_id(
    data_dir: &Path,
    inventory: &ManagedClusterInventory,
) -> Result<Option<String>> {
    match existing_local_cluster_id(data_dir).await {
        Ok(cluster_id) => Ok(cluster_id),
        Err(error) if inventory.unlabeled == 0 && inventory.cluster_ids.len() == 1 => {
            warn!(
                error = %error,
                "local state is unreadable; recovering from the managed containers' cluster ID"
            );
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

async fn open_local_state(data_dir: &Path) -> Result<LocalState> {
    LocalState::open(data_dir)
}

async fn archive_control_plane_state(data_dir: &Path) -> Result<()> {
    let names = [
        DATABASE_FILE,
        "swarmlite.sqlite-wal",
        "swarmlite.sqlite-shm",
        LEGACY_LOCAL_STATE_FILE,
        "local.sqlite-wal",
        "local.sqlite-shm",
        LEGACY_CONTROL_PLANE_FILE,
        "control-plane.sqlite-wal",
        "control-plane.sqlite-shm",
        "local.redb",
        "raft",
    ];
    let existing = names
        .into_iter()
        .filter(|name| data_dir.join(name).exists())
        .collect::<Vec<_>>();
    if existing.is_empty() {
        return Ok(());
    }
    let backup = data_dir
        .join("recovery-backup")
        .join(Uuid::new_v4().simple().to_string());
    tokio::fs::create_dir_all(&backup)
        .await
        .with_context(|| format!("failed to create {}", backup.display()))?;
    for name in existing {
        tokio::fs::rename(data_dir.join(name), backup.join(name))
            .await
            .with_context(|| format!("failed to archive control-plane state {name}"))?;
    }
    info!(backup = %backup.display(), "archived the previous local control plane before recovery");
    Ok(())
}

fn control_plane_data_is_present(data_dir: &Path) -> Result<bool> {
    if control_plane_state_exists(data_dir)? {
        return Ok(true);
    }
    if [
        LEGACY_LOCAL_STATE_FILE,
        LEGACY_CONTROL_PLANE_FILE,
        "local.redb",
    ]
    .into_iter()
    .any(|name| data_dir.join(name).exists())
    {
        return Ok(true);
    }
    let raft_dir = data_dir.join("raft");
    let mut entries = match std::fs::read_dir(&raft_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => {
            return Err(error).with_context(|| format!("failed to inspect {}", raft_dir.display()));
        }
    };
    Ok(entries.next().transpose()?.is_some())
}

fn validate_node_settings(settings: &NodeSettings) -> Result<()> {
    if settings.schema_version != NODE_SETTINGS_SCHEMA_VERSION
        || settings.node_id.trim().is_empty()
        || settings.token.len() < 16
    {
        bail!("unsupported or invalid node settings; run init/join with a fresh data directory");
    }
    if settings.cluster.schema_version != CLUSTER_SCHEMA_VERSION
        || settings.cluster.cluster_id.trim().is_empty()
        || settings.cluster.controller_id.trim().is_empty()
        || settings.cluster.controller_port == 0
        || !valid_gateway_image(&settings.cluster.gateway.image)
    {
        bail!("invalid local cluster identity");
    }
    if !is_controller(settings) && settings.controller_url.is_empty() {
        bail!("agent node is missing its fixed controller address");
    }
    Ok(())
}

fn requested_runtime(kind: Option<RuntimeKind>, socket: Option<&str>) -> Option<RuntimeConfig> {
    (kind.is_some() || socket.is_some()).then(|| RuntimeConfig {
        kind: kind.unwrap_or_default(),
        socket: socket.map(ToOwned::to_owned),
    })
}

fn resolve_runtime(
    kind: Option<RuntimeKind>,
    socket: Option<&str>,
    persisted: Option<&RuntimeConfig>,
) -> RuntimeConfig {
    if let Some(requested) = requested_runtime(kind, socket) {
        return requested;
    }
    if let Some(persisted) = persisted {
        return persisted.clone();
    }
    for (kind, path) in runtime_candidates() {
        if path.exists() {
            return RuntimeConfig {
                kind,
                socket: Some(path.to_string_lossy().into_owned()),
            };
        }
    }
    RuntimeConfig {
        kind: RuntimeKind::Docker,
        socket: None,
    }
}

fn runtime_candidates() -> Vec<(RuntimeKind, PathBuf)> {
    let mut candidates = vec![
        (RuntimeKind::Docker, PathBuf::from("/var/run/docker.sock")),
        (
            RuntimeKind::Podman,
            PathBuf::from("/run/podman/podman.sock"),
        ),
    ];
    if let Some(home) = nonempty_env("HOME") {
        candidates.push((
            RuntimeKind::Docker,
            PathBuf::from(&home).join(".orbstack/run/docker.sock"),
        ));
        candidates.push((
            RuntimeKind::Docker,
            PathBuf::from(home).join(".docker/run/docker.sock"),
        ));
    }
    if let Some(runtime_dir) = nonempty_env("XDG_RUNTIME_DIR") {
        candidates.push((
            RuntimeKind::Podman,
            PathBuf::from(runtime_dir).join("podman/podman.sock"),
        ));
    }
    candidates
}

fn resolve_advertise_address(explicit: Option<&str>) -> Result<String> {
    if let Some(explicit) = explicit {
        let value = explicit.trim();
        if value.is_empty() || value.contains('/') || value.chars().any(char::is_whitespace) {
            bail!("--advertise-address must be an IP address or hostname without a port");
        }
        if value.parse::<IpAddr>().is_ok() || valid_hostname(value) {
            return Ok(value.to_owned());
        }
        bail!("--advertise-address must be an IP address or hostname without a port");
    }
    detect_advertise_address()
        .map(|address| address.to_string())
        .context(
            "could not automatically detect a reachable node address; pass --advertise-address <IP-or-hostname>",
        )
}

fn valid_hostname(value: &str) -> bool {
    value.len() <= 253
        && value.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && label
                    .chars()
                    .all(|character| character.is_ascii_alphanumeric() || character == '-')
                && label
                    .chars()
                    .next()
                    .is_some_and(|character| character.is_ascii_alphanumeric())
                && label
                    .chars()
                    .next_back()
                    .is_some_and(|character| character.is_ascii_alphanumeric())
        })
}

pub fn detect_advertise_address() -> Option<IpAddr> {
    detect_from_route(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)), 80),
    )
    .or_else(|| {
        detect_from_route(
            SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0),
            "[2606:4700:4700::1111]:80".parse().ok()?,
        )
    })
}

fn detect_from_route(bind: SocketAddr, destination: SocketAddr) -> Option<IpAddr> {
    let socket = UdpSocket::bind(bind).ok()?;
    socket.connect(destination).ok()?;
    let address = socket.local_addr().ok()?.ip();
    usable_address(address).then_some(address)
}

fn usable_address(address: IpAddr) -> bool {
    if address.is_unspecified() || address.is_loopback() || address.is_multicast() {
        return false;
    }
    match address {
        IpAddr::V4(address) => !address.is_link_local() && !address.is_broadcast(),
        IpAddr::V6(address) => !address.is_unicast_link_local(),
    }
}

fn default_node_id() -> String {
    let hostname = nonempty_env("HOSTNAME")
        .or_else(|| nonempty_env("COMPUTERNAME"))
        .or_else(|| {
            std::fs::read_to_string("/etc/hostname")
                .ok()
                .map(|value| value.trim().to_owned())
                .filter(|value| !value.is_empty())
        })
        .unwrap_or_else(|| "node".to_owned());
    format!(
        "{}-{}",
        sanitize_node_id(&hostname),
        &Uuid::new_v4().simple().to_string()[..8]
    )
}

fn sanitize_node_id(value: &str) -> String {
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

fn controller_url(address: &str, port: u16) -> String {
    format!("http://{}:{port}", format_host(address))
}

fn normalize_controller_url(value: &str) -> Result<String> {
    let parsed = Url::parse(value).context("controller must be an absolute HTTP(S) URL")?;
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.host_str().is_none()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        bail!("controller must be an absolute HTTP(S) URL without query or fragment");
    }
    Ok(value.trim_end_matches('/').to_owned())
}

fn format_host(host: &str) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]")
    } else {
        host.to_owned()
    }
}

fn generate_token() -> String {
    format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple())
}

fn is_controller(settings: &NodeSettings) -> bool {
    settings.node_id == settings.cluster.controller_id
}

fn nonempty_env(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

async fn prepare_data_dir(path: &Path) -> Result<()> {
    tokio::fs::create_dir_all(path)
        .await
        .with_context(|| format!("failed to create {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
            .await
            .with_context(|| format!("failed to protect {}", path.display()))?;
    }
    Ok(())
}

fn acquire_node_lock(data_dir: &Path, command: &str) -> Result<std::fs::File> {
    let path = data_dir.join(NODE_LOCK_FILE);
    let file = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(&path)
        .with_context(|| format!("failed to open {}", path.display()))?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(std::fs::TryLockError::WouldBlock) => bail!(
            "another `swarmlite serve`, init, or join process is using {}; stop it before running `{command}`",
            data_dir.display()
        ),
        Err(std::fs::TryLockError::Error(error)) => {
            Err(error).with_context(|| format!("failed to lock {}", path.display()))
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::{
        Json, Router,
        extract::State,
        routing::{get, put},
    };

    use super::*;

    #[derive(Clone)]
    struct MockJoinState {
        cluster: ClusterSettings,
        registry_credentials: BTreeMap<String, crate::model::RegistryCredential>,
    }

    fn assert_no_json_state(data_dir: &Path) {
        let has_json = std::fs::read_dir(data_dir)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .any(|entry| {
                entry
                    .path()
                    .extension()
                    .is_some_and(|value| value == "json")
            });
        assert!(!has_json);
    }

    #[test]
    fn validates_explicit_advertise_addresses_without_ports() {
        assert_eq!(
            resolve_advertise_address(Some("node-a.internal")).unwrap(),
            "node-a.internal"
        );
        assert_eq!(
            resolve_advertise_address(Some("2001:db8::21")).unwrap(),
            "2001:db8::21"
        );
        assert!(resolve_advertise_address(Some("10.0.0.21:8080")).is_err());
        assert!(resolve_advertise_address(Some("bad/name")).is_err());
    }

    #[test]
    fn parses_initial_node_labels_without_silently_rewriting_them() {
        assert_eq!(
            parse_labels(vec!["region=cn-east".into(), "disk=nvme".into()]).unwrap(),
            BTreeMap::from([
                ("disk".to_owned(), "nvme".to_owned()),
                ("region".to_owned(), "cn-east".to_owned()),
            ])
        );
        assert!(parse_labels(vec!["region".into()]).is_err());
        assert!(parse_labels(vec![" region=cn-east".into()]).is_err());
        assert!(parse_labels(vec!["region=cn\neast".into()]).is_err());
    }

    #[test]
    fn validates_single_controller_cluster() {
        let cluster = ClusterSettings {
            schema_version: CLUSTER_SCHEMA_VERSION,
            cluster_id: "test".into(),
            controller_id: "controller-a".into(),
            controller_port: crate::config::DEFAULT_CONTROLLER_PORT,
            gateway: Default::default(),
        };
        assert!(validate_cluster(&cluster).is_ok());
    }

    #[test]
    fn validates_managed_container_clusters_before_init() {
        let inventory = ManagedClusterInventory {
            cluster_ids: ["cluster-old".to_owned()].into_iter().collect(),
            unlabeled: 0,
            ..Default::default()
        };
        assert!(validate_managed_inventory(&inventory, "cluster-new", false).is_err());
        assert!(validate_managed_inventory(&inventory, "cluster-old", true).is_ok());
        assert!(validate_managed_inventory(&inventory, "cluster-new", true).is_err());
        let unlabeled = ManagedClusterInventory {
            unlabeled: 1,
            ..Default::default()
        };
        assert!(validate_managed_inventory(&unlabeled, "cluster-old", true).is_err());
    }

    #[test]
    fn recovery_uses_the_single_runtime_cluster_and_cross_checks_local_state() {
        let inventory = ManagedClusterInventory {
            cluster_ids: ["cluster-old".to_owned()].into_iter().collect(),
            unlabeled: 0,
            ..Default::default()
        };
        assert_eq!(
            recovery_cluster_id(&inventory, None).unwrap(),
            "cluster-old"
        );
        assert_eq!(
            recovery_cluster_id(&inventory, Some("cluster-old")).unwrap(),
            "cluster-old"
        );
        assert!(recovery_cluster_id(&inventory, Some("cluster-other")).is_err());
        assert!(
            recovery_cluster_id(&ManagedClusterInventory::default(), Some("cluster-local")).is_ok()
        );
        assert!(recovery_cluster_id(&ManagedClusterInventory::default(), None).is_err());
    }

    #[test]
    fn recovery_keeps_the_gateway_image_from_the_existing_container() {
        let mut cluster = ClusterSettings {
            schema_version: CLUSTER_SCHEMA_VERSION,
            cluster_id: "cluster-old".into(),
            controller_id: "controller-a".into(),
            controller_port: crate::config::DEFAULT_CONTROLLER_PORT,
            gateway: Default::default(),
        };
        let inventory = ManagedClusterInventory {
            gateway_listen: BTreeMap::from([(
                "cluster-old".into(),
                vec![":80".into(), ":443".into()],
            )]),
            gateway_images: BTreeMap::from([(
                "cluster-old".into(),
                "registry.example.com/caddy:old".into(),
            )]),
            ..Default::default()
        };

        recover_gateway_config(&mut cluster, &inventory, false);
        assert_eq!(cluster.gateway.listen, [":80", ":443"]);
        assert_eq!(cluster.gateway.image, "registry.example.com/caddy:old");

        cluster.gateway.image = "registry.example.com/caddy:new".into();
        recover_gateway_config(&mut cluster, &inventory, true);
        assert_eq!(cluster.gateway.image, "registry.example.com/caddy:new");
    }

    #[tokio::test]
    async fn recovery_can_use_container_identity_when_local_sqlite_is_corrupt() {
        let directory = tempfile::tempdir().unwrap();
        tokio::fs::write(
            directory.path().join(DATABASE_FILE),
            b"not a sqlite database",
        )
        .await
        .unwrap();
        let inventory = ManagedClusterInventory {
            cluster_ids: ["cluster-from-containers".to_owned()].into_iter().collect(),
            unlabeled: 0,
            ..Default::default()
        };

        let local_cluster_id = recovery_local_cluster_id(directory.path(), &inventory)
            .await
            .unwrap();
        assert_eq!(local_cluster_id, None);
        assert_eq!(
            recovery_cluster_id(&inventory, local_cluster_id.as_deref()).unwrap(),
            "cluster-from-containers"
        );
        assert!(
            recovery_local_cluster_id(directory.path(), &ManagedClusterInventory::default())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn init_persists_all_local_state_in_sqlite() {
        let directory = tempfile::tempdir().unwrap();
        let cluster = ClusterSettings {
            schema_version: CLUSTER_SCHEMA_VERSION,
            cluster_id: "test".into(),
            controller_id: "controller-a".into(),
            controller_port: 18080,
            gateway: Default::default(),
        };
        init(InitOptions {
            data_dir: directory.path().to_owned(),
            cluster,
            token: Some("0123456789abcdef".into()),
            advertise_address: None,
            runtime: None,
            runtime_socket: None,
            labels: BTreeMap::new(),
            recovery: false,
            gateway_image_explicit: false,
            gateway_enabled: true,
        })
        .await
        .unwrap();
        let node = load_node_settings(directory.path()).await.unwrap();
        assert!(is_controller(&node));
        assert!(node.gateway_enabled);
        assert_eq!(node.token, "0123456789abcdef");
        assert!(directory.path().join(DATABASE_FILE).exists());
        assert_no_json_state(directory.path());
        let local_state = LocalState::open(directory.path()).unwrap();
        assert!(local_state.get::<NodeSettings>(NODE_KEY).unwrap().is_some());
        assert_eq!(
            local_state.get::<AgentFence>(FENCE_KEY).unwrap(),
            Some(AgentFence::default())
        );
        assert!(!control_plane_data_is_present(directory.path()).unwrap());
        let connection = resolve_connection(directory.path(), None, None)
            .await
            .unwrap();
        assert_eq!(connection.0, "http://127.0.0.1:18080");
        assert_eq!(connection.1, "0123456789abcdef");

        let duplicate = init(InitOptions {
            data_dir: directory.path().to_owned(),
            cluster: ClusterSettings {
                schema_version: CLUSTER_SCHEMA_VERSION,
                cluster_id: "other".into(),
                controller_id: "controller-b".into(),
                controller_port: 18080,
                gateway: Default::default(),
            },
            token: Some("0123456789abcdef".into()),
            advertise_address: None,
            runtime: None,
            runtime_socket: None,
            labels: BTreeMap::new(),
            recovery: false,
            gateway_image_explicit: false,
            gateway_enabled: true,
        })
        .await
        .unwrap_err();
        assert!(duplicate.to_string().contains("serve"));
    }

    #[tokio::test]
    async fn rejects_init_over_unified_and_legacy_control_plane_data() {
        let directory = tempfile::tempdir().unwrap();
        let cluster = ClusterSettings {
            schema_version: CLUSTER_SCHEMA_VERSION,
            cluster_id: "existing-controller".into(),
            controller_id: "controller-a".into(),
            controller_port: 18080,
            gateway: Default::default(),
        };
        let repository = StateRepository::open(directory.path(), cluster.clone()).unwrap();
        repository.initialize_with_cluster(&cluster).await.unwrap();
        assert!(ensure_control_plane_data_is_absent(directory.path()).is_err());

        let directory = tempfile::tempdir().unwrap();
        tokio::fs::write(
            directory.path().join(LEGACY_CONTROL_PLANE_FILE),
            b"existing",
        )
        .await
        .unwrap();
        assert!(ensure_control_plane_data_is_absent(directory.path()).is_err());
        tokio::fs::remove_file(directory.path().join(LEGACY_CONTROL_PLANE_FILE))
            .await
            .unwrap();

        let raft = directory.path().join("raft");
        tokio::fs::create_dir_all(&raft).await.unwrap();
        tokio::fs::write(raft.join("raft.redb"), b"existing")
            .await
            .unwrap();
        assert!(ensure_control_plane_data_is_absent(directory.path()).is_err());
    }

    #[tokio::test]
    async fn recovery_init_archives_local_control_plane_and_keeps_cluster_identity() {
        let directory = tempfile::tempdir().unwrap();
        let cluster = ClusterSettings {
            schema_version: CLUSTER_SCHEMA_VERSION,
            cluster_id: "recover-test".into(),
            controller_id: "controller-a".into(),
            controller_port: 18081,
            gateway: Default::default(),
        };
        init(InitOptions {
            data_dir: directory.path().to_owned(),
            cluster: cluster.clone(),
            token: Some("old-token-0123456".into()),
            advertise_address: None,
            runtime: None,
            runtime_socket: None,
            labels: BTreeMap::new(),
            recovery: false,
            gateway_image_explicit: false,
            gateway_enabled: true,
        })
        .await
        .unwrap();
        let raft = directory.path().join("raft");
        tokio::fs::create_dir_all(&raft).await.unwrap();
        tokio::fs::write(raft.join("raft.redb"), b"old-raft")
            .await
            .unwrap();
        tokio::fs::write(
            directory.path().join(LEGACY_CONTROL_PLANE_FILE),
            b"old-sqlite",
        )
        .await
        .unwrap();

        let message = init(InitOptions {
            data_dir: directory.path().to_owned(),
            cluster: ClusterSettings {
                cluster_id: new_cluster_id(),
                ..cluster
            },
            token: Some("new-token-0123456".into()),
            advertise_address: None,
            runtime: None,
            runtime_socket: None,
            labels: BTreeMap::new(),
            recovery: true,
            gateway_image_explicit: false,
            gateway_enabled: true,
        })
        .await
        .unwrap();

        assert!(message.contains("recovered single-controller cluster recover-test"));
        let settings = load_node_settings(directory.path()).await.unwrap();
        assert_eq!(settings.cluster.cluster_id, "recover-test");
        assert_eq!(settings.token, "new-token-0123456");
        assert!(!directory.path().join("raft").exists());
        let backups = std::fs::read_dir(directory.path().join("recovery-backup"))
            .unwrap()
            .collect::<std::io::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(backups.len(), 1);
        assert!(backups[0].path().join(DATABASE_FILE).exists());
        assert!(backups[0].path().join(LEGACY_CONTROL_PLANE_FILE).exists());
        assert!(backups[0].path().join("raft/raft.redb").exists());
    }

    #[tokio::test]
    async fn join_pulls_cluster_settings_and_persists_gateway_setting() {
        let cluster = ClusterSettings {
            schema_version: CLUSTER_SCHEMA_VERSION,
            cluster_id: "joined-test".into(),
            controller_id: "controller-a".into(),
            controller_port: 18080,
            gateway: Default::default(),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let controller = format!("http://{address}");
        let state = MockJoinState {
            cluster: cluster.clone(),
            registry_credentials: BTreeMap::from([(
                "ghcr.io".into(),
                crate::model::RegistryCredential {
                    username: "octocat".into(),
                    password: "private-token".into(),
                },
            )]),
        };
        let app = Router::new()
            .route("/v1/cluster", get(mock_bootstrap))
            .route("/v1/nodes/{node_id}/join", put(mock_join))
            .with_state(state);
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let directory = tempfile::tempdir().unwrap();

        let result = join(JoinOptions {
            data_dir: directory.path().to_owned(),
            controller: controller.clone(),
            token: "0123456789abcdef".into(),
            advertise_address: Some("10.0.0.22".into()),
            runtime: None,
            runtime_socket: None,
            labels: BTreeMap::from([("region".into(), "cn-east".into())]),
            gateway_enabled: false,
        })
        .await
        .unwrap();
        assert!(result.contains("gateway disabled"));
        let settings = load_node_settings(directory.path()).await.unwrap();
        assert_eq!(settings.cluster, cluster.clone());
        assert!(!settings.gateway_enabled);
        assert_eq!(
            settings.labels,
            BTreeMap::from([("region".into(), "cn-east".into())])
        );
        assert_eq!(settings.controller_url, controller);
        let registry_credentials =
            RegistryCredentialStore::new(LocalState::open(directory.path()).unwrap())
                .snapshot()
                .unwrap();
        assert_eq!(registry_credentials["ghcr.io"].password, "private-token");
        assert!(directory.path().join(DATABASE_FILE).exists());
        assert_no_json_state(directory.path());
        server.abort();
    }

    #[tokio::test]
    async fn join_rebinds_same_cluster_when_recovery_rotates_the_token() {
        let cluster = ClusterSettings {
            schema_version: CLUSTER_SCHEMA_VERSION,
            cluster_id: "rejoined-test".into(),
            controller_id: "controller-a".into(),
            controller_port: 18082,
            gateway: Default::default(),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let controller = format!("http://{address}");
        let app = Router::new()
            .route("/v1/cluster", get(mock_bootstrap))
            .route("/v1/nodes/{node_id}/join", put(mock_join))
            .with_state(MockJoinState {
                cluster,
                registry_credentials: BTreeMap::new(),
            });
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let directory = tempfile::tempdir().unwrap();

        join(JoinOptions {
            data_dir: directory.path().to_owned(),
            controller: controller.clone(),
            token: "old-token-0123456".into(),
            advertise_address: Some("10.0.0.22".into()),
            runtime: None,
            runtime_socket: None,
            labels: BTreeMap::new(),
            gateway_enabled: false,
        })
        .await
        .unwrap();
        let old = load_node_settings(directory.path()).await.unwrap();
        let local_state = LocalState::open(directory.path()).unwrap();
        local_state
            .put(FENCE_KEY, &AgentFence { generation: 11 })
            .unwrap();
        drop(local_state);
        tokio::fs::create_dir_all(directory.path().join("raft"))
            .await
            .unwrap();
        tokio::fs::write(directory.path().join("raft/raft.redb"), b"stale")
            .await
            .unwrap();

        let message = join(JoinOptions {
            data_dir: directory.path().to_owned(),
            controller,
            token: "new-token-0123456".into(),
            advertise_address: Some("10.0.0.22".into()),
            runtime: None,
            runtime_socket: None,
            labels: BTreeMap::new(),
            gateway_enabled: false,
        })
        .await
        .unwrap();
        let current = load_node_settings(directory.path()).await.unwrap();
        assert!(message.contains("rejoined recovered cluster"));
        assert_ne!(current.node_id, old.node_id);
        assert_eq!(current.token, "new-token-0123456");
        let local_state = LocalState::open(directory.path()).unwrap();
        assert_eq!(
            local_state.get::<AgentFence>(FENCE_KEY).unwrap(),
            Some(AgentFence::default())
        );
        assert!(!directory.path().join("raft").exists());
        assert!(directory.path().join("recovery-backup").exists());
        server.abort();
    }

    #[tokio::test]
    async fn rejects_unsupported_node_schema() {
        let directory = tempfile::tempdir().unwrap();
        let local_state = LocalState::open(directory.path()).unwrap();
        local_state
            .put(
                NODE_KEY,
                &NodeSettings {
                    schema_version: 4,
                    gateway_enabled: false,
                    cluster: ClusterSettings {
                        schema_version: CLUSTER_SCHEMA_VERSION,
                        cluster_id: "unsupported-test".into(),
                        controller_id: "controller-a".into(),
                        controller_port: crate::config::DEFAULT_CONTROLLER_PORT,
                        gateway: Default::default(),
                    },
                    node_id: "unsupported-node".into(),
                    token: "0123456789abcdef".into(),
                    controller_url: String::new(),
                    advertise_address: None,
                    runtime: None,
                    labels: BTreeMap::new(),
                },
            )
            .unwrap();

        assert!(load_node_settings(directory.path()).await.is_err());
    }

    async fn mock_bootstrap(State(state): State<MockJoinState>) -> Json<BootstrapResponse> {
        Json(BootstrapResponse {
            cluster: state.cluster,
        })
    }

    async fn mock_join(
        State(state): State<MockJoinState>,
        Json(request): Json<JoinRequest>,
    ) -> Json<JoinResponse> {
        Json(JoinResponse {
            cluster: state.cluster,
            gateway_enabled: request.gateway_enabled,
            labels: request.labels,
            registry_credentials: state.registry_credentials,
        })
    }
}
