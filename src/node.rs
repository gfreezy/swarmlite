use std::{
    collections::BTreeMap,
    env,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs, UdpSocket},
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use swarmlite_raft::{ControllerNode, NodeConfig, RaftNode};
use tokio::sync::{mpsc, oneshot, watch};
use tracing::{info, warn};
use url::Url;
use uuid::Uuid;

use crate::{
    agent,
    config::{
        AgentConfig, ControllerConfig, GatewayConfig, PortRangeConfig, RuntimeConfig, RuntimeKind,
    },
    controller,
    local_state::{
        AgentControllerSet, AgentFence, CONTROLLER_SET_KEY, FENCE_KEY, LOCAL_STATE_FILE,
        LocalState, NODE_KEY,
    },
    model::{
        BootstrapResponse, CLUSTER_SCHEMA_VERSION, ClusterGatewayConfig, ClusterMode,
        ClusterSettings, JoinRequest, JoinResponse, NodeControl, NodeRole, NodeRoles,
        initial_roles, valid_gateway_image,
    },
    runtime::{
        ContainerRuntime, DockerCompatibleRuntime, GatewayContainerSpec, ManagedClusterInventory,
    },
    storage::StateRepository,
};

const NODE_LOCK_FILE: &str = "serve.lock";

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
}

#[derive(Debug, Clone)]
pub struct ServeOptions {
    pub data_dir: PathBuf,
    pub advertise_address: Option<String>,
    pub runtime: Option<RuntimeKind>,
    pub runtime_socket: Option<String>,
    pub labels: BTreeMap<String, String>,
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
    pub requested_roles: Option<NodeRoles>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NodeSettings {
    schema_version: u32,
    roles: NodeRoles,
    cluster: LocalClusterSettings,
    node_id: String,
    raft_id: u64,
    raft_bootstrap: bool,
    token: String,
    controller_urls: Vec<String>,
    advertise_address: Option<String>,
    runtime: Option<RuntimeConfig>,
    labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct LocalClusterSettings {
    schema_version: u32,
    cluster_id: String,
    mode: ClusterMode,
    controller_port: u16,
    gateway: ClusterGatewayConfig,
}

impl LocalClusterSettings {
    fn from_cluster(cluster: &ClusterSettings) -> Self {
        Self {
            schema_version: cluster.schema_version,
            cluster_id: cluster.cluster_id.clone(),
            mode: cluster.mode,
            controller_port: cluster.controller_port,
            gateway: cluster.gateway.clone(),
        }
    }

    fn raft_seed(&self) -> ClusterSettings {
        ClusterSettings {
            schema_version: self.schema_version,
            cluster_id: self.cluster_id.clone(),
            mode: self.mode,
            controller_port: self.controller_port,
            gateway: self.gateway.clone(),
        }
    }
}

enum NodeEvent {
    Agent(Result<()>),
    Controller(Result<()>),
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
        ensure_raft_data_is_absent(&options.data_dir).await?;
        LocalState::open(&options.data_dir)?
    } else {
        let state = open_local_state(&options.data_dir).await?;
        if state.get::<NodeSettings>(NODE_KEY)?.is_some() {
            bail!(
                "{} is already initialized; run `swarmlite serve` instead of init",
                options.data_dir.display()
            );
        }
        validate_managed_inventory(&inventory, &options.cluster.cluster_id, false)?;
        ensure_raft_data_is_absent(&options.data_dir).await?;
        state
    };
    validate_cluster(&options.cluster)?;

    let token = options
        .token
        .or_else(|| nonempty_env("SWARMLITE_TOKEN"))
        .unwrap_or_else(generate_token);
    if token.len() < 16 {
        bail!("the cluster token must contain at least 16 bytes");
    }
    let settings = NodeSettings {
        schema_version: 6,
        roles: initial_roles(),
        cluster: LocalClusterSettings::from_cluster(&options.cluster),
        node_id: default_node_id(),
        raft_id: new_raft_id(),
        raft_bootstrap: true,
        token: token.clone(),
        controller_urls: Vec::new(),
        advertise_address: options.advertise_address,
        runtime: requested_runtime(options.runtime, options.runtime_socket.as_deref()),
        labels: options.labels,
    };
    local_state.put_triple(
        (NODE_KEY, &settings),
        (FENCE_KEY, &AgentFence::default()),
        (CONTROLLER_SET_KEY, &AgentControllerSet::default()),
    )?;
    Ok(format!(
        "initialized {}{} cluster {} as {} with roles {}; run `swarmlite serve` (join token: {token})",
        if options.recovery { "recovered " } else { "" },
        mode_name(options.cluster.mode),
        settings.cluster.cluster_id,
        settings.node_id,
        role_names(&settings.roles),
    ))
}

pub async fn run(options: ServeOptions) -> Result<()> {
    prepare_data_dir(&options.data_dir).await?;
    let _node_lock = acquire_node_lock(&options.data_dir, "serve")?;
    let local_state = open_local_state(&options.data_dir).await?;
    let mut settings = load_node_settings_from(&local_state)?;

    let mut changed = false;
    let controller_set = local_state
        .get::<AgentControllerSet>(CONTROLLER_SET_KEY)?
        .filter(|controller_set| !controller_set.controllers.is_empty())
        .unwrap_or_else(|| AgentControllerSet {
            generation: 0,
            controllers: settings.controller_urls.clone(),
        });
    if settings.controller_urls != controller_set.controllers {
        settings
            .controller_urls
            .clone_from(&controller_set.controllers);
        changed = true;
    }
    if let Some(address) = &options.advertise_address {
        resolve_advertise_address(Some(address))?;
        settings.advertise_address = Some(address.clone());
        changed = true;
    }
    if let Some(runtime) = requested_runtime(options.runtime, options.runtime_socket.as_deref()) {
        settings.runtime = Some(runtime);
        changed = true;
    }
    if !options.labels.is_empty() {
        settings.labels.extend(options.labels);
        changed = true;
    }
    if changed {
        local_state.put(NODE_KEY, &settings)?;
    }

    let advertise_address = resolve_advertise_address(settings.advertise_address.as_deref())?;
    let public_controller = controller_url(&advertise_address, settings.cluster.controller_port);
    let public_raft = raft_url(&public_controller);
    let local_controller = format!("http://127.0.0.1:{}", settings.cluster.controller_port);
    let runtime = resolve_runtime(
        options.runtime,
        options.runtime_socket.as_deref(),
        settings.runtime.as_ref(),
    );
    let gateway_runtime = DockerCompatibleRuntime::connect(&runtime.resolve()?)?;
    gateway_runtime
        .reconcile_gateway(
            &gateway_container_spec(&settings, &advertise_address, &public_controller)?,
            settings.roles.contains(&NodeRole::Gateway),
        )
        .await?;
    let mut agent_controllers = settings.controller_urls.clone();
    if settings.roles.contains(&NodeRole::Controller) {
        agent_controllers.insert(0, local_controller.clone());
    }
    normalize_controller_list(&mut agent_controllers);
    if agent_controllers.is_empty() {
        bail!("node configuration has no controller addresses");
    }

    let agent_config = AgentConfig {
        cluster_id: settings.cluster.cluster_id.clone(),
        node_id: settings.node_id.clone(),
        advertise_address: advertise_address.clone(),
        controllers: agent_controllers.clone(),
        controller_set_generation: controller_set.generation,
        runtime: Some(runtime),
        labels: settings.labels.clone(),
        heartbeat_interval_seconds: 2,
        port_range: PortRangeConfig::default(),
        roles: settings.roles.clone(),
        controller_url: public_controller.clone(),
        raft_id: settings.raft_id,
        raft_url: public_raft,
    };
    let initial_control = NodeControl {
        cluster: settings.cluster.raft_seed(),
        roles: settings.roles.clone(),
        controllers: settings.controller_urls.clone(),
    };
    let (control_tx, control_rx) = watch::channel(initial_control);
    let (events_tx, events_rx) = mpsc::unbounded_channel();
    let token = settings.token.clone();
    let agent_events = events_tx.clone();
    let agent_local_state = local_state.clone();
    let agent_handle = tokio::spawn(async move {
        let result =
            agent::run_with_token_and_updates(agent_config, token, control_tx, agent_local_state)
                .await;
        let _ = agent_events.send(NodeEvent::Agent(result));
    });

    info!(
        node_id = %settings.node_id,
        roles = %role_names(&settings.roles),
        address = %advertise_address,
        "starting node service"
    );
    supervise(
        &options.data_dir,
        local_state,
        settings,
        public_controller,
        control_rx,
        events_tx,
        events_rx,
        agent_handle,
        gateway_runtime,
        advertise_address,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn supervise(
    data_dir: &Path,
    local_state: LocalState,
    mut settings: NodeSettings,
    public_controller: String,
    mut control_rx: watch::Receiver<NodeControl>,
    events_tx: mpsc::UnboundedSender<NodeEvent>,
    mut events_rx: mpsc::UnboundedReceiver<NodeEvent>,
    agent_handle: tokio::task::JoinHandle<()>,
    gateway_runtime: DockerCompatibleRuntime,
    advertise_address: String,
) -> Result<()> {
    let mut desired_roles = settings.roles.clone();
    let mut controller_running = false;
    let mut controller_stopping = false;
    let mut controller_shutdown = None;
    if desired_roles.contains(&NodeRole::Controller) {
        controller_shutdown = Some(
            start_controller(data_dir, &settings, &public_controller, events_tx.clone()).await?,
        );
        controller_running = true;
        if settings.raft_bootstrap {
            settings.raft_bootstrap = false;
            local_state.put(NODE_KEY, &settings)?;
            info!("completed one-time Raft bootstrap and sealed local node state");
        }
    }
    if desired_roles.contains(&NodeRole::Gateway) {
        info!("gateway role assigned; independent Caddy container is running");
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
                let had_gateway = settings.roles.contains(&NodeRole::Gateway);
                desired_roles = control.roles;
                settings.roles.clone_from(&desired_roles);
                settings.cluster = LocalClusterSettings::from_cluster(&control.cluster);
                settings.controller_urls = control.controllers;
                normalize_controller_list(&mut settings.controller_urls);
                local_state.put(NODE_KEY, &settings)?;
                match (desired_roles.contains(&NodeRole::Controller), controller_running) {
                    (true, false) => {
                        controller_shutdown = Some(
                            start_controller(data_dir, &settings, &public_controller, events_tx.clone()).await?
                        );
                        controller_running = true;
                        controller_stopping = false;
                        info!("node promoted to controller");
                    }
                    (false, true) => {
                        if let Some(shutdown) = controller_shutdown.take() {
                            let _ = shutdown.send(());
                        }
                        controller_stopping = true;
                        info!("controller role removed; stopping controller");
                    }
                    _ => {}
                }
                let has_gateway = desired_roles.contains(&NodeRole::Gateway);
                if has_gateway {
                    gateway_runtime
                        .reconcile_gateway(
                            &gateway_container_spec(
                                &settings,
                                &advertise_address,
                                &public_controller,
                            )?,
                            true,
                        )
                        .await?;
                    if !had_gateway {
                        info!("gateway role added; started independent Caddy container");
                    }
                } else if had_gateway && !has_gateway {
                    gateway_runtime
                        .reconcile_gateway(
                            &gateway_container_spec(
                                &settings,
                                &advertise_address,
                                &public_controller,
                            )?,
                            false,
                        )
                        .await?;
                    info!("gateway role removed; removed independent Caddy container");
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
                        controller_running = false;
                        controller_shutdown = None;
                        if desired_roles.contains(&NodeRole::Controller) {
                            if controller_stopping && result.is_ok() {
                                controller_shutdown = Some(
                                    start_controller(data_dir, &settings, &public_controller, events_tx.clone()).await?
                                );
                                controller_running = true;
                                controller_stopping = false;
                                continue;
                            }
                            result.context("assigned controller stopped")?;
                            bail!("assigned controller stopped unexpectedly");
                        }
                        controller_stopping = false;
                        if let Err(error) = result {
                            warn!(%error, "demoted controller stopped with an error");
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
    let mut controllers = settings.controller_urls.clone();
    if settings.roles.contains(&NodeRole::Controller) {
        controllers.push(public_controller.to_owned());
    }
    normalize_controller_list(&mut controllers);
    Ok(GatewayContainerSpec {
        cluster_id: settings.cluster.cluster_id.clone(),
        advertise_address: advertise_address.to_owned(),
        admin_bind_address: resolve_gateway_bind_address(advertise_address)?,
        listen: settings.cluster.gateway.listen.clone(),
        controllers,
        token: settings.token.clone(),
        image: settings.cluster.gateway.image.clone(),
    })
}

fn resolve_gateway_bind_address(advertise_address: &str) -> Result<String> {
    if let Ok(address) = advertise_address.parse::<IpAddr>() {
        return Ok(address.to_string());
    }
    (advertise_address, 0)
        .to_socket_addrs()
        .with_context(|| {
            format!("failed to resolve advertise address {advertise_address:?} for Caddy")
        })?
        .next()
        .map(|address| address.ip().to_string())
        .with_context(|| {
            format!("advertise address {advertise_address:?} resolved to no IP addresses")
        })
}

async fn start_controller(
    data_dir: &Path,
    settings: &NodeSettings,
    public_controller: &str,
    events: mpsc::UnboundedSender<NodeEvent>,
) -> Result<oneshot::Sender<()>> {
    let cluster = settings.cluster.raft_seed();
    let controller_node = ControllerNode {
        raft_url: raft_url(public_controller),
        api_url: public_controller.to_owned(),
    };
    let raft = RaftNode::open(NodeConfig::new(
        settings.raft_id,
        controller_node,
        data_dir.join("raft"),
        cluster.cluster_id.clone(),
        settings.token.clone(),
    ))
    .await
    .map_err(anyhow::Error::msg)?;
    if settings.raft_bootstrap && raft.voter_ids().is_empty() {
        raft.initialize().await.map_err(anyhow::Error::msg)?;
        raft.raft()
            .wait(Some(Duration::from_secs(10)))
            .current_leader(settings.raft_id, "initial controller becomes Raft leader")
            .await
            .map_err(anyhow::Error::msg)?;
    }
    let repository = StateRepository::new(raft, cluster.clone());
    repository
        .initialize_with_cluster(&cluster)
        .await
        .map_err(anyhow::Error::msg)?;
    let gateway = GatewayConfig {
        listen: if settings.cluster.gateway.listen.is_empty() {
            vec![":80".to_owned()]
        } else {
            settings.cluster.gateway.listen.clone()
        },
        ..GatewayConfig::default()
    };
    let config = ControllerConfig {
        controller_id: settings.node_id.clone(),
        roles: settings.roles.clone(),
        listen: SocketAddr::new(
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            settings.cluster.controller_port,
        ),
        advertise_url: public_controller.to_owned(),
        node_timeout_seconds: 20,
        reconcile_interval_seconds: 1,
        gateway,
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
    let stale_raft_without_identity =
        existing.is_none() && raft_data_is_present(&options.data_dir)?;
    if recovery_rebind || stale_raft_without_identity {
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
    let raft_id = preserve_identity.map_or_else(new_raft_id, |settings| settings.raft_id);
    let runtime = requested_runtime(options.runtime, options.runtime_socket.as_deref())
        .or_else(|| preserve_identity.and_then(|settings| settings.runtime.clone()));
    let recovered_roles = if options.requested_roles.is_none() {
        let detection_runtime = resolve_runtime(
            options.runtime,
            options.runtime_socket.as_deref(),
            runtime.as_ref(),
        );
        detect_recovered_system_roles(&detection_runtime, &bootstrap.cluster.cluster_id).await
    } else {
        NodeRoles::new()
    };
    let public_controller = controller_url(&advertise_address, bootstrap.cluster.controller_port);
    let request = JoinRequest {
        node_id: node_id.clone(),
        address: advertise_address.clone(),
        requested_roles: options.requested_roles.clone(),
        recovered_roles,
        controller_url: public_controller.clone(),
        raft_id,
        raft_url: raft_url(&public_controller),
        labels: options.labels.clone(),
    };
    let response = send_join(&seed, &options.token, &request).await?;
    if response.cluster != bootstrap.cluster {
        bail!("cluster settings changed during join; retry the command");
    }
    let (mut controllers, controller_set_generation) = if !response.controllers.is_empty() {
        (response.controllers, response.controller_set_generation)
    } else if !bootstrap.controllers.is_empty() {
        (bootstrap.controllers, bootstrap.controller_set_generation)
    } else {
        (vec![seed.clone()], 0)
    };
    normalize_controller_list(&mut controllers);
    let controller_set = AgentControllerSet {
        generation: controller_set_generation,
        controllers: controllers.clone(),
    };
    let mut labels = preserve_identity
        .map(|settings| settings.labels.clone())
        .unwrap_or_default();
    labels.extend(options.labels);
    let settings = NodeSettings {
        schema_version: 6,
        roles: response.roles,
        cluster: LocalClusterSettings::from_cluster(&response.cluster),
        node_id: node_id.clone(),
        raft_id,
        raft_bootstrap: false,
        token: options.token.clone(),
        controller_urls: controllers.clone(),
        advertise_address: Some(advertise_address),
        runtime,
        labels,
    };
    if recovery_rebind || existing.is_none() {
        local_state.put_triple(
            (NODE_KEY, &settings),
            (FENCE_KEY, &AgentFence::default()),
            (CONTROLLER_SET_KEY, &controller_set),
        )?;
    } else {
        local_state.put_pair((NODE_KEY, &settings), (CONTROLLER_SET_KEY, &controller_set))?;
    }
    Ok(format!(
        "{} cluster {} as {node_id} with roles {}; run `swarmlite serve`",
        if recovery_rebind {
            "rejoined recovered"
        } else {
            "joined"
        },
        settings.cluster.cluster_id,
        role_names(&settings.roles)
    ))
}

async fn detect_recovered_system_roles(runtime: &RuntimeConfig, cluster_id: &str) -> NodeRoles {
    let resolved = match runtime.resolve() {
        Ok(resolved) => resolved,
        Err(error) => {
            warn!(%error, "could not inspect system containers while joining");
            return NodeRoles::new();
        }
    };
    if !Path::new(&resolved.socket).exists() {
        return NodeRoles::new();
    }
    let runtime = match DockerCompatibleRuntime::connect(&resolved) {
        Ok(runtime) => runtime,
        Err(error) => {
            warn!(%error, "could not connect to the runtime while collecting system containers");
            return NodeRoles::new();
        }
    };
    let inventory = match runtime.managed_cluster_inventory().await {
        Ok(inventory) => inventory,
        Err(error) => {
            warn!(%error, "could not inspect system containers while joining");
            return NodeRoles::new();
        }
    };
    if inventory.gateway_cluster_ids.contains(cluster_id) {
        info!(cluster_id, "collected an existing gateway system container");
        NodeRoles::from([NodeRole::Gateway])
    } else {
        NodeRoles::new()
    }
}

pub async fn join_command(data_dir: &Path) -> Result<String> {
    let settings = read_node_settings(data_dir).await?;
    let controller = if settings.roles.contains(&NodeRole::Controller) {
        let address = resolve_advertise_address(settings.advertise_address.as_deref())?;
        controller_url(&address, settings.cluster.controller_port)
    } else {
        settings
            .controller_urls
            .first()
            .cloned()
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
    let saved_controller = if settings.roles.contains(&NodeRole::Controller) {
        format!("http://127.0.0.1:{}", settings.cluster.controller_port)
    } else {
        settings
            .controller_urls
            .first()
            .cloned()
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
            if key.trim().is_empty() {
                bail!("label key must not be empty");
            }
            Ok((key.trim().to_owned(), value.trim().to_owned()))
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
    request_json_with_redirect(
        reqwest::Method::GET,
        format!("{controller}/v1/cluster"),
        token,
        None::<&JoinRequest>,
    )
    .await
}

async fn send_join(controller: &str, token: &str, request: &JoinRequest) -> Result<JoinResponse> {
    request_json_with_redirect(
        reqwest::Method::PUT,
        format!("{controller}/v1/nodes/{}/join", request.node_id),
        token,
        Some(request),
    )
    .await
}

async fn request_json_with_redirect<T, B>(
    method: reqwest::Method,
    initial_url: String,
    token: &str,
    body: Option<&B>,
) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
    B: Serialize + ?Sized,
{
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let mut url = initial_url;
    for _ in 0..3 {
        let mut request = client.request(method.clone(), &url).bearer_auth(token);
        if let Some(body) = body {
            request = request.json(body);
        }
        let response = request.send().await?;
        if response.status() == reqwest::StatusCode::TEMPORARY_REDIRECT {
            url = response
                .headers()
                .get(reqwest::header::LOCATION)
                .and_then(|value| value.to_str().ok())
                .context("controller redirect omitted a valid Location header")?
                .to_owned();
            continue;
        }
        let status = response.status();
        let body = response.text().await?;
        if !status.is_success() {
            bail!("controller returned {status}: {body}");
        }
        return serde_json::from_str(&body).context("controller returned invalid JSON");
    }
    bail!("too many controller redirects")
}

fn validate_cluster(cluster: &ClusterSettings) -> Result<()> {
    if cluster.schema_version != CLUSTER_SCHEMA_VERSION || !valid_cluster_id(&cluster.cluster_id) {
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

async fn ensure_raft_data_is_absent(data_dir: &Path) -> Result<()> {
    let raft_dir = data_dir.join("raft");
    let mut entries = match tokio::fs::read_dir(&raft_dir).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(error).with_context(|| format!("failed to inspect {}", raft_dir.display()));
        }
    };
    if entries.next_entry().await?.is_some() {
        bail!(
            "{} contains existing Raft data; restore {LOCAL_STATE_FILE} and run `swarmlite serve`, or use `swarmlite init --recover` after stopping the old control plane",
            raft_dir.display()
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
    let names = [LOCAL_STATE_FILE, "raft"];
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

fn raft_data_is_present(data_dir: &Path) -> Result<bool> {
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
    if settings.schema_version != 6
        || settings.node_id.trim().is_empty()
        || settings.raft_id == 0
        || settings.token.len() < 16
    {
        bail!("unsupported or invalid node settings; run init/join with a fresh data directory");
    }
    if settings.cluster.schema_version != CLUSTER_SCHEMA_VERSION
        || settings.cluster.cluster_id.trim().is_empty()
        || settings.cluster.controller_port == 0
        || !valid_gateway_image(&settings.cluster.gateway.image)
    {
        bail!("invalid local cluster identity");
    }
    if !settings.roles.contains(&NodeRole::Agent) {
        bail!("every node must have the agent role");
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

fn raft_url(controller: &str) -> String {
    format!("{}/internal/raft", controller.trim_end_matches('/'))
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

fn normalize_controller_list(values: &mut Vec<String>) {
    for value in values.iter_mut() {
        *value = value.trim_end_matches('/').to_owned();
    }
    values.sort();
    values.dedup();
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

fn new_raft_id() -> u64 {
    let value = Uuid::new_v4().as_u128() as u64;
    value.max(1)
}

fn role_names(roles: &NodeRoles) -> String {
    roles
        .iter()
        .map(|role| match role {
            NodeRole::Controller => "controller",
            NodeRole::Agent => "agent",
            NodeRole::Gateway => "gateway",
        })
        .collect::<Vec<_>>()
        .join(",")
}

const fn mode_name(mode: ClusterMode) -> &'static str {
    match mode {
        ClusterMode::Standalone => "standalone",
        ClusterMode::Ha => "HA",
    }
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
        controller: String,
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
    fn validates_cluster_modes() {
        let standalone = ClusterSettings {
            schema_version: CLUSTER_SCHEMA_VERSION,
            cluster_id: "test".into(),
            mode: ClusterMode::Standalone,
            controller_port: 8080,
            gateway: Default::default(),
        };
        assert!(validate_cluster(&standalone).is_ok());
        let ha = ClusterSettings {
            mode: ClusterMode::Ha,
            ..standalone
        };
        assert!(validate_cluster(&ha).is_ok());
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
            mode: ClusterMode::Standalone,
            controller_port: 8080,
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
    async fn recovery_can_use_container_identity_when_local_redb_is_corrupt() {
        let directory = tempfile::tempdir().unwrap();
        tokio::fs::write(
            directory.path().join(LOCAL_STATE_FILE),
            b"not a redb database",
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
    async fn init_persists_all_local_state_in_redb() {
        let directory = tempfile::tempdir().unwrap();
        let cluster = ClusterSettings {
            schema_version: CLUSTER_SCHEMA_VERSION,
            cluster_id: "test".into(),
            mode: ClusterMode::Standalone,
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
        })
        .await
        .unwrap();
        let node = load_node_settings(directory.path()).await.unwrap();
        assert_eq!(node.roles, initial_roles());
        assert_eq!(node.token, "0123456789abcdef");
        assert!(directory.path().join(LOCAL_STATE_FILE).exists());
        assert_no_json_state(directory.path());
        let local_state = LocalState::open(directory.path()).unwrap();
        assert!(local_state.get::<NodeSettings>(NODE_KEY).unwrap().is_some());
        assert_eq!(
            local_state.get::<AgentFence>(FENCE_KEY).unwrap(),
            Some(AgentFence::default())
        );
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
                mode: ClusterMode::Standalone,
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
        })
        .await
        .unwrap_err();
        assert!(duplicate.to_string().contains("serve"));
    }

    #[tokio::test]
    async fn rejects_init_over_existing_raft_data() {
        let directory = tempfile::tempdir().unwrap();
        let raft = directory.path().join("raft");
        tokio::fs::create_dir_all(&raft).await.unwrap();
        tokio::fs::write(raft.join("raft.redb"), b"existing")
            .await
            .unwrap();
        assert!(ensure_raft_data_is_absent(directory.path()).await.is_err());
    }

    #[tokio::test]
    async fn recovery_init_archives_local_control_plane_and_keeps_cluster_identity() {
        let directory = tempfile::tempdir().unwrap();
        let cluster = ClusterSettings {
            schema_version: CLUSTER_SCHEMA_VERSION,
            cluster_id: "recover-test".into(),
            mode: ClusterMode::Standalone,
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
        })
        .await
        .unwrap();
        let raft = directory.path().join("raft");
        tokio::fs::create_dir_all(&raft).await.unwrap();
        tokio::fs::write(raft.join("raft.redb"), b"old-raft")
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
        })
        .await
        .unwrap();

        assert!(message.contains("recovered standalone cluster recover-test"));
        let settings = load_node_settings(directory.path()).await.unwrap();
        assert_eq!(settings.cluster.cluster_id, "recover-test");
        assert_eq!(settings.token, "new-token-0123456");
        assert!(!directory.path().join("raft").exists());
        let backups = std::fs::read_dir(directory.path().join("recovery-backup"))
            .unwrap()
            .collect::<std::io::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(backups.len(), 1);
        assert!(backups[0].path().join(LOCAL_STATE_FILE).exists());
        assert!(backups[0].path().join("raft/raft.redb").exists());
    }

    #[tokio::test]
    async fn join_pulls_cluster_settings_and_persists_agent_role() {
        let cluster = ClusterSettings {
            schema_version: CLUSTER_SCHEMA_VERSION,
            cluster_id: "joined-test".into(),
            mode: ClusterMode::Standalone,
            controller_port: 18080,
            gateway: Default::default(),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let controller = format!("http://{address}");
        let state = MockJoinState {
            cluster: cluster.clone(),
            controller: controller.clone(),
        };
        let app = Router::new()
            .route("/v1/cluster", get(mock_bootstrap))
            .route("/v1/nodes/{node_id}/join", put(mock_join))
            .with_state(state);
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let directory = tempfile::tempdir().unwrap();

        let result = join(JoinOptions {
            data_dir: directory.path().to_owned(),
            controller,
            token: "0123456789abcdef".into(),
            advertise_address: Some("10.0.0.22".into()),
            runtime: None,
            runtime_socket: None,
            labels: BTreeMap::new(),
            requested_roles: None,
        })
        .await
        .unwrap();
        assert!(result.contains("agent"));
        let settings = load_node_settings(directory.path()).await.unwrap();
        assert_eq!(
            settings.cluster,
            LocalClusterSettings::from_cluster(&cluster)
        );
        assert_eq!(settings.roles, crate::model::agent_roles());
        let controller_set = LocalState::open(directory.path())
            .unwrap()
            .get::<AgentControllerSet>(CONTROLLER_SET_KEY)
            .unwrap()
            .unwrap();
        assert_eq!(controller_set.generation, 1);
        assert_eq!(controller_set.controllers, settings.controller_urls);
        assert!(directory.path().join(LOCAL_STATE_FILE).exists());
        assert_no_json_state(directory.path());
        server.abort();
    }

    #[tokio::test]
    async fn join_rebinds_same_cluster_when_recovery_rotates_the_token() {
        let cluster = ClusterSettings {
            schema_version: CLUSTER_SCHEMA_VERSION,
            cluster_id: "rejoined-test".into(),
            mode: ClusterMode::Standalone,
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
                controller: controller.clone(),
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
            requested_roles: None,
        })
        .await
        .unwrap();
        let old = load_node_settings(directory.path()).await.unwrap();
        let local_state = LocalState::open(directory.path()).unwrap();
        local_state
            .put(
                FENCE_KEY,
                &AgentFence {
                    term: 9,
                    generation: 11,
                },
            )
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
            requested_roles: None,
        })
        .await
        .unwrap();
        let current = load_node_settings(directory.path()).await.unwrap();
        assert!(message.contains("rejoined recovered cluster"));
        assert_ne!(current.node_id, old.node_id);
        assert_ne!(current.raft_id, old.raft_id);
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
                    roles: initial_roles(),
                    cluster: LocalClusterSettings {
                        schema_version: CLUSTER_SCHEMA_VERSION,
                        cluster_id: "unsupported-test".into(),
                        mode: ClusterMode::Standalone,
                        controller_port: 8080,
                        gateway: Default::default(),
                    },
                    node_id: "unsupported-node".into(),
                    raft_id: 42,
                    raft_bootstrap: false,
                    token: "0123456789abcdef".into(),
                    controller_urls: Vec::new(),
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
            controllers: vec![state.controller],
            controller_set_generation: 1,
        })
    }

    async fn mock_join(
        State(state): State<MockJoinState>,
        Json(_request): Json<JoinRequest>,
    ) -> Json<JoinResponse> {
        Json(JoinResponse {
            cluster: state.cluster,
            roles: crate::model::agent_roles(),
            controllers: vec![state.controller],
            controller_set_generation: 1,
        })
    }
}
