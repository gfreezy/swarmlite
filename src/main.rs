use std::{
    collections::{BTreeMap, BTreeSet},
    io::{IsTerminal, Write},
    path::{Path, PathBuf},
    process::ExitCode,
};

use anyhow::{Context, Result, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use clap::{Args, Parser, Subcommand, ValueEnum};
use swarmlite::{
    client::ControllerClient,
    config::{DEFAULT_CONTROLLER_PORT, InstalledNodeConfig, RuntimeKind, SYSTEM_CONFIG_PATH},
    model::{
        CLUSTER_SCHEMA_VERSION, ClusterConfigResponse, ClusterConfigUpdate, ClusterGatewayConfig,
        ClusterSettings, ConfigBlobCheckRequest, ConfigBlobCheckResponse, DEFAULT_GATEWAY_IMAGE,
        MAX_CONFIG_FILE_BYTES, MAX_STACK_CONFIG_BYTES, NodeGatewayResponse, NodeGatewayUpdate,
        NodeLabelRemoveRequest, NodeLabelSetRequest, NodeLabelsResponse, RegistryLoginRequest,
        RegistryLoginResponse, StackApplyRequest, StackConfigPayload, StackDeploymentListResponse,
        StackDeploymentResponse, StackDeploymentStatus, StackRollbackRequest,
        StackValidationResponse, StatusResponse, config_digest,
    },
    node, registry,
};
use tokio::io::AsyncReadExt;

mod cluster_cli;
mod connection;
mod upgrade;

#[derive(Debug, Parser)]
#[command(name = "swarmlite", version, about)]
struct Cli {
    /// Directory for generated node identity, state, and CLI connection settings.
    #[arg(long, global = true, env = "SWARMLITE_DATA_DIR")]
    data_dir: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Initialize a new single-controller cluster on this node.
    Init {
        #[command(flatten)]
        options: InitArgs,
    },
    /// Run this node's fixed agent and optional controller/gateway components.
    Serve {
        /// Address other machines use to reach containers on this node.
        #[arg(long, env = "SWARMLITE_ADVERTISE_ADDRESS")]
        advertise_address: Option<String>,
        #[arg(long, value_enum, env = "SWARMLITE_RUNTIME")]
        runtime: Option<RuntimeKind>,
        #[arg(long, env = "SWARMLITE_RUNTIME_SOCKET")]
        runtime_socket: Option<String>,
    },
    /// Pull cluster settings and configure this machine to join an existing cluster.
    Join {
        controller: String,
        #[arg(long, env = "SWARMLITE_TOKEN")]
        token: String,
        #[arg(long, env = "SWARMLITE_ADVERTISE_ADDRESS")]
        advertise_address: Option<String>,
        #[arg(long, value_enum, env = "SWARMLITE_RUNTIME")]
        runtime: Option<RuntimeKind>,
        #[arg(long, env = "SWARMLITE_RUNTIME_SOCKET")]
        runtime_socket: Option<String>,
        #[arg(long = "label")]
        labels: Vec<String>,
        /// Enable the gateway on this node after joining.
        #[arg(long)]
        gateway: bool,
    },
    /// Print a join command for this node's cluster.
    JoinToken,
    /// Print the Controller address and cluster token stored on this node.
    ConnectionInfo {
        /// Emit one machine-readable JSON object.
        #[arg(long)]
        json: bool,
    },
    /// Upgrade Swarmlite using an official GitHub Release.
    Upgrade {
        /// Release to install, such as v0.2.0.
        #[arg(long, default_value = "latest", value_parser = upgrade::validate_version)]
        version: String,
    },
    /// Read or update cluster-wide configuration.
    Config {
        #[command(subcommand)]
        action: ConfigCommand,
    },
    /// Read or change whether the gateway is enabled on a node.
    Gateway {
        #[command(subcommand)]
        action: GatewayCommand,
    },
    /// Read or change metadata assigned to joined nodes.
    Node {
        #[command(subcommand)]
        action: NodeCommand,
    },
    /// Manage cluster-wide private container registry credentials.
    Registry {
        #[command(subcommand)]
        action: RegistryCommand,
    },
    /// Deploy a new Stack or update an existing Stack.
    Deploy {
        #[command(flatten)]
        options: cluster_cli::DeployArgs,
    },
    /// Inspect, attach to, retry, or roll back Stack deployments.
    Deployment {
        #[command(subcommand)]
        action: DeploymentCommand,
    },
    /// List cluster services, optionally limited to one Stack.
    Ls {
        #[command(flatten)]
        options: cluster_cli::ListArgs,
    },
    /// List tasks, optionally limited to a Stack or Service.
    Ps {
        #[command(flatten)]
        options: cluster_cli::PsArgs,
    },
    /// Display detailed information about a Service.
    Inspect {
        #[command(flatten)]
        options: cluster_cli::InspectArgs,
    },
    /// Fetch logs from a Service, Task name, or Task ID.
    Logs {
        #[command(flatten)]
        options: cluster_cli::LogsArgs,
    },
    /// Scale one or more Services.
    Scale {
        #[command(flatten)]
        options: cluster_cli::ScaleArgs,
    },
    /// Perform a rolling restart of a Service.
    Restart {
        #[command(flatten)]
        options: cluster_cli::RestartArgs,
    },
    /// Remove one or more Stacks.
    Rm {
        #[command(flatten)]
        options: cluster_cli::RemoveArgs,
    },
    /// Display the current cluster state.
    Status {
        /// Emit the complete machine-readable status response.
        #[arg(long)]
        json: bool,
        /// HTTP(S) Controller URL or ssh://[user@]host[:port].
        #[arg(short = 'u', long)]
        controller: Option<String>,
        #[arg(long, env = "SWARMLITE_TOKEN")]
        token: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
enum ConfigCommand {
    /// Print the current cluster configuration.
    Get {
        #[command(flatten)]
        connection: ConnectionArgs,
    },
    /// Update mutable cluster configuration.
    Set {
        /// Configuration key to update.
        key: ConfigKey,
        /// New value for the configuration key.
        value: String,
        #[command(flatten)]
        connection: ConnectionArgs,
    },
}

#[derive(Debug, Subcommand)]
enum GatewayCommand {
    /// Print whether the gateway is enabled on a node.
    Status {
        node_id: String,
        #[command(flatten)]
        connection: ConnectionArgs,
    },
    /// Enable the gateway on a node.
    Enable {
        node_id: String,
        #[command(flatten)]
        connection: ConnectionArgs,
    },
    /// Disable the gateway on a node.
    Disable {
        node_id: String,
        #[command(flatten)]
        connection: ConnectionArgs,
    },
}

#[derive(Debug, Subcommand)]
enum NodeCommand {
    /// Read or change a node's authoritative placement labels.
    Label {
        #[command(subcommand)]
        action: NodeLabelCommand,
    },
}

#[derive(Debug, Subcommand)]
enum NodeLabelCommand {
    /// Print all labels assigned to a node.
    Get {
        node_id: String,
        #[command(flatten)]
        connection: ConnectionArgs,
    },
    /// Set or replace one label.
    Set {
        node_id: String,
        key: String,
        value: String,
        #[command(flatten)]
        connection: ConnectionArgs,
    },
    /// Remove one label.
    Remove {
        node_id: String,
        key: String,
        #[command(flatten)]
        connection: ConnectionArgs,
    },
}

#[derive(Debug, Subcommand)]
enum RegistryCommand {
    /// Store credentials used by every node when pulling private images.
    Login {
        /// Registry hostname with an optional port, such as ghcr.io.
        registry: String,
        #[arg(short = 'u', long)]
        username: String,
        /// Read the registry password or token from standard input.
        #[arg(long, required = true)]
        password_stdin: bool,
        #[command(flatten)]
        connection: RegistryConnectionArgs,
    },
}

#[derive(Debug, Subcommand)]
enum DeploymentCommand {
    /// Show one deployment; defaults to the current generation.
    Status {
        stack: String,
        #[arg(long)]
        generation: Option<u64>,
        #[arg(long)]
        json: bool,
        #[command(flatten)]
        connection: ConnectionArgs,
    },
    /// List the current deployment and archived generations.
    History {
        stack: String,
        #[arg(long)]
        json: bool,
        #[command(flatten)]
        connection: ConnectionArgs,
    },
    /// Follow one deployment; defaults to the current generation.
    Attach {
        stack: String,
        #[arg(long)]
        generation: Option<u64>,
        #[arg(long)]
        json: bool,
        #[command(flatten)]
        connection: ConnectionArgs,
    },
    /// Retry the current stalled, blocked, or failed generation.
    Retry {
        stack: String,
        #[arg(short = 'd', long)]
        detach: bool,
        #[arg(long)]
        json: bool,
        #[command(flatten)]
        connection: ConnectionArgs,
    },
    /// Create a new deployment from a previous snapshot.
    Rollback {
        stack: String,
        /// Generation to restore; defaults to the latest previous healthy generation.
        #[arg(long = "to-generation")]
        generation: Option<u64>,
        #[arg(short = 'd', long)]
        detach: bool,
        #[arg(long)]
        json: bool,
        #[command(flatten)]
        connection: ConnectionArgs,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ConfigKey {
    GatewayImage,
    DeploymentProgressDeadlineSeconds,
    ImagePullIdleTimeoutSeconds,
    ImagePullMaxAttempts,
    ImagePullInitialBackoffSeconds,
    ImagePullMaxBackoffSeconds,
}

#[derive(Debug, Args)]
struct ConnectionArgs {
    /// HTTP(S) Controller URL or ssh://[user@]host[:port].
    #[arg(short = 'u', long)]
    controller: Option<String>,
    #[arg(long, env = "SWARMLITE_TOKEN")]
    token: Option<String>,
}

#[derive(Debug, Args)]
struct RegistryConnectionArgs {
    /// HTTP(S) Controller URL or ssh://[user@]host[:port].
    #[arg(long)]
    controller: Option<String>,
    #[arg(long, env = "SWARMLITE_TOKEN")]
    token: Option<String>,
}

#[derive(Debug, Args)]
struct InitArgs {
    #[arg(long, env = "SWARMLITE_TOKEN")]
    token: Option<String>,
    #[arg(long, default_value_t = DEFAULT_CONTROLLER_PORT)]
    controller_port: u16,
    /// Rebuild the control plane and collect containers from the previous cluster.
    #[arg(long)]
    recover: bool,
    #[arg(long, env = "SWARMLITE_ADVERTISE_ADDRESS")]
    advertise_address: Option<String>,
    #[arg(long, value_enum, env = "SWARMLITE_RUNTIME")]
    runtime: Option<RuntimeKind>,
    #[arg(long, env = "SWARMLITE_RUNTIME_SOCKET")]
    runtime_socket: Option<String>,
    #[arg(long = "label")]
    labels: Vec<String>,
    #[arg(long = "gateway-listen", default_values = [":80", ":443"])]
    gateway_listen: Vec<String>,
    /// OCI image containing Caddy and caddy.storage.swarmlite.
    /// Defaults to ghcr.io/gfreezy/swarmlite-caddy:latest.
    #[arg(long = "gateway-image")]
    gateway_image: Option<String>,
    /// Initialize without running a gateway on the controller node.
    #[arg(long)]
    no_gateway: bool,
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("Error: {error:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "swarmlite=info,tower_http=info".into()),
        )
        .init();

    let cli = Cli::parse();
    if let Command::Upgrade { version } = &cli.command {
        return upgrade::run(version).await;
    }
    let installed = InstalledNodeConfig::load_if_exists(SYSTEM_CONFIG_PATH)?;
    let data_dir = node::resolve_data_dir(cli.data_dir.or_else(|| installed.data_dir.clone()))?;
    match cli.command {
        Command::Init { options } => {
            let (runtime, runtime_socket) =
                installed.runtime_options(options.runtime, options.runtime_socket);
            let gateway_image_explicit = options.gateway_image.is_some();
            let cluster = ClusterSettings {
                schema_version: CLUSTER_SCHEMA_VERSION,
                cluster_id: node::new_cluster_id(),
                controller_id: String::new(),
                controller_port: options.controller_port,
                gateway: ClusterGatewayConfig {
                    listen: options.gateway_listen.clone(),
                    image: options
                        .gateway_image
                        .unwrap_or_else(|| DEFAULT_GATEWAY_IMAGE.to_owned()),
                },
                deployment: Default::default(),
            };
            let message = node::init(node::InitOptions {
                data_dir,
                cluster,
                token: options.token,
                advertise_address: options.advertise_address,
                runtime,
                runtime_socket,
                labels: node::parse_labels(options.labels)?,
                recovery: options.recover,
                gateway_image_explicit,
                gateway_enabled: !options.no_gateway,
            })
            .await?;
            println!("{message}");
            Ok(())
        }
        Command::Serve {
            advertise_address,
            runtime,
            runtime_socket,
        } => {
            let (runtime, runtime_socket) = installed.runtime_options(runtime, runtime_socket);
            node::run(node::ServeOptions {
                data_dir,
                advertise_address,
                runtime,
                runtime_socket,
            })
            .await
        }
        Command::Join {
            controller,
            token,
            advertise_address,
            runtime,
            runtime_socket,
            labels,
            gateway,
        } => {
            let (runtime, runtime_socket) = installed.runtime_options(runtime, runtime_socket);
            let message = node::join(node::JoinOptions {
                data_dir,
                controller,
                token,
                advertise_address,
                runtime,
                runtime_socket,
                labels: node::parse_labels(labels)?,
                gateway_enabled: gateway,
            })
            .await?;
            println!("{message}");
            Ok(())
        }
        Command::JoinToken => {
            println!("{}", node::join_command(&data_dir).await?);
            Ok(())
        }
        Command::ConnectionInfo { json } => {
            let info = node::connection_info(&data_dir).await?;
            if json {
                println!("{}", serde_json::to_string(&info)?);
            } else {
                println!("controller: {}", info.controller);
                println!("token: {}", info.token);
            }
            Ok(())
        }
        Command::Upgrade { .. } => unreachable!("upgrade returned before loading node state"),
        Command::Config { action } => {
            let (connection, update) = match action {
                ConfigCommand::Get { connection } => (connection, None),
                ConfigCommand::Set {
                    key,
                    value,
                    connection,
                } => {
                    let mut update = ClusterConfigUpdate {
                        gateway_image: None,
                        deployment_progress_deadline_seconds: None,
                        image_pull_idle_timeout_seconds: None,
                        image_pull_max_attempts: None,
                        image_pull_initial_backoff_seconds: None,
                        image_pull_max_backoff_seconds: None,
                    };
                    match key {
                        ConfigKey::GatewayImage => update.gateway_image = Some(value),
                        ConfigKey::DeploymentProgressDeadlineSeconds => update
                            .deployment_progress_deadline_seconds =
                            Some(value.parse().context(
                                "deployment-progress-deadline-seconds must be a positive integer",
                            )?),
                        ConfigKey::ImagePullIdleTimeoutSeconds => {
                            update.image_pull_idle_timeout_seconds = Some(value.parse().context(
                                "image-pull-idle-timeout-seconds must be a positive integer",
                            )?)
                        }
                        ConfigKey::ImagePullMaxAttempts => {
                            update.image_pull_max_attempts =
                                Some(value.parse().context(
                                    "image-pull-max-attempts must be a positive integer",
                                )?)
                        }
                        ConfigKey::ImagePullInitialBackoffSeconds => {
                            update.image_pull_initial_backoff_seconds =
                                Some(value.parse().context(
                                    "image-pull-initial-backoff-seconds must be an integer",
                                )?)
                        }
                        ConfigKey::ImagePullMaxBackoffSeconds => {
                            update.image_pull_max_backoff_seconds = Some(
                                value
                                    .parse()
                                    .context("image-pull-max-backoff-seconds must be an integer")?,
                            )
                        }
                    };
                    (connection, Some(update))
                }
            };
            let client =
                connection::resolve(&data_dir, connection.controller, connection.token).await?;
            let response = cluster_config(&client, update.as_ref()).await?;
            println!("{}", serde_json::to_string_pretty(&response)?);
            Ok(())
        }
        Command::Gateway { action } => {
            let (node_id, enabled, connection) = match action {
                GatewayCommand::Status {
                    node_id,
                    connection,
                } => (node_id, None, connection),
                GatewayCommand::Enable {
                    node_id,
                    connection,
                } => (node_id, Some(true), connection),
                GatewayCommand::Disable {
                    node_id,
                    connection,
                } => (node_id, Some(false), connection),
            };
            let client =
                connection::resolve(&data_dir, connection.controller, connection.token).await?;
            let response = node_gateway(&client, &node_id, enabled).await?;
            println!("{}", serde_json::to_string_pretty(&response)?);
            Ok(())
        }
        Command::Node { action } => match action {
            NodeCommand::Label { action } => {
                let (node_id, method, body, connection) = match action {
                    NodeLabelCommand::Get {
                        node_id,
                        connection,
                    } => (node_id, reqwest::Method::GET, None, connection),
                    NodeLabelCommand::Set {
                        node_id,
                        key,
                        value,
                        connection,
                    } => (
                        node_id,
                        reqwest::Method::PUT,
                        Some(serde_json::to_value(NodeLabelSetRequest { key, value })?),
                        connection,
                    ),
                    NodeLabelCommand::Remove {
                        node_id,
                        key,
                        connection,
                    } => (
                        node_id,
                        reqwest::Method::DELETE,
                        Some(serde_json::to_value(NodeLabelRemoveRequest { key })?),
                        connection,
                    ),
                };
                let client =
                    connection::resolve(&data_dir, connection.controller, connection.token).await?;
                let response = node_labels(&client, &node_id, method, body.as_ref()).await?;
                println!("{}", serde_json::to_string_pretty(&response)?);
                Ok(())
            }
        },
        Command::Registry { action } => match action {
            RegistryCommand::Login {
                registry: registry_host,
                username,
                password_stdin: _,
                connection,
            } => {
                let password = read_registry_password().await?;
                let client =
                    connection::resolve(&data_dir, connection.controller, connection.token).await?;
                let response: RegistryLoginResponse = client
                    .send_json(
                        reqwest::Method::PUT,
                        "/v1/registry-credentials",
                        Some(&RegistryLoginRequest {
                            registry: registry_host,
                            username,
                            password,
                        }),
                    )
                    .await?;
                println!(
                    "stored credentials for {} as {} across the cluster",
                    response.registry, response.username
                );
                Ok(())
            }
        },
        Command::Deploy { options } => cluster_cli::run_deploy(&data_dir, options).await,
        Command::Deployment { action } => run_deployment_command(&data_dir, action).await,
        Command::Ls { options } => cluster_cli::run_list(&data_dir, options).await,
        Command::Ps { options } => cluster_cli::run_ps(&data_dir, options).await,
        Command::Inspect { options } => cluster_cli::run_inspect(&data_dir, options).await,
        Command::Logs { options } => cluster_cli::run_logs(&data_dir, options).await,
        Command::Scale { options } => cluster_cli::run_scale(&data_dir, options).await,
        Command::Restart { options } => cluster_cli::run_restart(&data_dir, options).await,
        Command::Rm { options } => cluster_cli::run_remove(&data_dir, options).await,
        Command::Status {
            json,
            controller,
            token,
        } => {
            let client = connection::resolve(&data_dir, controller, token).await?;
            status(&client, json).await
        }
    }
}

async fn read_registry_password() -> Result<String> {
    let mut password = String::new();
    tokio::io::stdin()
        .take((registry::MAX_PASSWORD_BYTES + 1) as u64)
        .read_to_string(&mut password)
        .await
        .context("failed to read registry password from stdin")?;
    let password = password.trim_end_matches(['\r', '\n']).to_owned();
    if password.len() > registry::MAX_PASSWORD_BYTES {
        bail!(
            "registry password must contain at most {} bytes",
            registry::MAX_PASSWORD_BYTES
        );
    }
    Ok(password)
}

async fn run_deployment_command(data_dir: &Path, action: DeploymentCommand) -> Result<()> {
    let (stack, connection) = match &action {
        DeploymentCommand::Status {
            stack, connection, ..
        }
        | DeploymentCommand::History {
            stack, connection, ..
        }
        | DeploymentCommand::Attach {
            stack, connection, ..
        }
        | DeploymentCommand::Retry {
            stack, connection, ..
        }
        | DeploymentCommand::Rollback {
            stack, connection, ..
        } => (stack.clone(), connection),
    };
    let client = connection::resolve(
        data_dir,
        connection.controller.clone(),
        connection.token.clone(),
    )
    .await?;
    let encoded_stack = url::form_urlencoded::byte_serialize(stack.as_bytes()).collect::<String>();
    match action {
        DeploymentCommand::Status {
            generation, json, ..
        } => {
            let deployment = get_deployment(&client, &encoded_stack, generation).await?;
            print_deployment(&deployment, json)?;
        }
        DeploymentCommand::History { json, .. } => {
            let deployments: StackDeploymentListResponse = client
                .get_json(&format!("/v1/stacks/{encoded_stack}/deployments"))
                .await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&deployments)?);
            } else {
                println!("GENERATION\tSTATUS\tRETRIES\tSTARTED\tLAST PROGRESS\tFINISHED");
                if let Some(current) = deployments.current {
                    println!(
                        "{}\t{:?} (current)\t{}\t{}\t{}\t{}",
                        current.generation,
                        current.status,
                        current.retry_revision,
                        current.started_at_unix_ms,
                        current.last_progress_at_unix_ms,
                        current
                            .finished_at_unix_ms
                            .map_or_else(|| "-".into(), |value| value.to_string())
                    );
                }
                for deployment in deployments.history {
                    let status = deployment.superseded_by.map_or_else(
                        || format!("{:?}", deployment.status),
                        |generation| format!("{:?} by {generation}", deployment.status),
                    );
                    println!(
                        "{}\t{}\t{}\t{}\t{}\t{}",
                        deployment.generation,
                        status,
                        deployment.retry_revision,
                        deployment.started_at_unix_ms,
                        deployment.last_progress_at_unix_ms,
                        deployment
                            .finished_at_unix_ms
                            .map_or_else(|| "-".into(), |value| value.to_string())
                    );
                }
            }
        }
        DeploymentCommand::Attach {
            generation, json, ..
        } => {
            let deployment = get_deployment(&client, &encoded_stack, generation).await?;
            let deployment = attach_deployment(&client, deployment).await?;
            print_deployment(&deployment, json)?;
        }
        DeploymentCommand::Retry { detach, json, .. } => {
            let deployment: StackDeploymentResponse = client
                .send_json::<_, ()>(
                    reqwest::Method::POST,
                    &format!("/v1/stacks/{encoded_stack}/deployment/retry"),
                    None,
                )
                .await?;
            let deployment =
                finish_deployment(&client, deployment, detach, DeploymentOperation::Retry).await?;
            print_deployment(&deployment, json)?;
        }
        DeploymentCommand::Rollback {
            generation,
            detach,
            json,
            ..
        } => {
            let deployment: StackDeploymentResponse = client
                .send_json(
                    reqwest::Method::POST,
                    &format!("/v1/stacks/{encoded_stack}/rollback"),
                    Some(&StackRollbackRequest { generation }),
                )
                .await?;
            let deployment =
                finish_deployment(&client, deployment, detach, DeploymentOperation::Rollback)
                    .await?;
            print_deployment(&deployment, json)?;
        }
    }
    Ok(())
}

async fn get_deployment(
    client: &ControllerClient,
    encoded_stack: &str,
    generation: Option<u64>,
) -> Result<StackDeploymentResponse> {
    let query = generation.map_or_else(String::new, |generation| {
        format!("?generation={generation}&wait_seconds=0")
    });
    let query = if query.is_empty() {
        "?wait_seconds=0".to_owned()
    } else {
        query
    };
    Ok(client
        .get_json(&format!("/v1/stacks/{encoded_stack}/deployment{query}"))
        .await?)
}

async fn attach_deployment(
    client: &ControllerClient,
    deployment: StackDeploymentResponse,
) -> Result<StackDeploymentResponse> {
    let mut progress = DeploymentProgressRenderer::new();
    let started = tokio::time::Instant::now();
    progress.render(DeploymentOperation::Deploy, &deployment, started.elapsed());
    let stack = deployment.stack.clone();
    let deployment = wait_for_deployment(
        client,
        &stack,
        deployment,
        DeploymentOperation::Deploy,
        started,
        &mut progress,
    )
    .await?;
    ensure_deployment_succeeded(&deployment)?;
    Ok(deployment)
}

fn print_deployment(deployment: &StackDeploymentResponse, json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(deployment)?);
    } else {
        println!(
            "{}",
            deployment_progress_summary(
                DeploymentOperation::Deploy,
                deployment,
                std::time::Duration::ZERO,
            )
        );
        println!(
            "last progress: {}; progress deadline: {}s; retry revision: {}",
            deployment.last_progress_at_unix_ms,
            deployment.progress_deadline_seconds,
            deployment.retry_revision
        );
        if let Some(generation) = deployment.superseded_by {
            println!("superseded by generation {generation}");
        }
        for condition in deployment
            .conditions
            .iter()
            .filter(|condition| condition.resolved_at_unix_ms.is_none())
        {
            println!("condition {:?}: {}", condition.kind, condition.message);
        }
    }
    Ok(())
}

async fn finish_deployment(
    client: &ControllerClient,
    mut deployment: StackDeploymentResponse,
    detach: bool,
    operation: DeploymentOperation,
) -> Result<StackDeploymentResponse> {
    let mut progress = DeploymentProgressRenderer::new();
    progress.accepted(operation, &deployment, detach);
    if detach {
        return Ok(deployment);
    }
    let started = tokio::time::Instant::now();
    progress.render(operation, &deployment, started.elapsed());
    let stack = deployment.stack.clone();
    deployment = wait_for_deployment(
        client,
        &stack,
        deployment,
        operation,
        started,
        &mut progress,
    )
    .await?;
    ensure_deployment_succeeded(&deployment)?;
    Ok(deployment)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeploymentOperation {
    Deploy,
    Retry,
    Rollback,
    Scale,
    Restart,
    Remove,
}

impl DeploymentOperation {
    fn label(self) -> &'static str {
        match self {
            Self::Deploy => "deploy",
            Self::Retry => "retry",
            Self::Rollback => "rollback",
            Self::Scale => "scale",
            Self::Restart => "restart",
            Self::Remove => "remove",
        }
    }
}

const PROGRESS_SPINNER: [&str; 10] = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];

struct DeploymentProgressRenderer {
    interactive: bool,
    color: bool,
    frame: usize,
    line_active: bool,
}

impl DeploymentProgressRenderer {
    fn new() -> Self {
        let interactive = std::io::stderr().is_terminal()
            && std::env::var("TERM").ok().as_deref() != Some("dumb");
        Self::for_output(interactive, std::env::var_os("NO_COLOR").is_some())
    }

    fn for_output(interactive: bool, no_color: bool) -> Self {
        Self {
            interactive,
            color: interactive && !no_color,
            frame: 0,
            line_active: false,
        }
    }

    fn refresh_interval(&self) -> std::time::Duration {
        if self.interactive {
            std::time::Duration::from_millis(120)
        } else {
            std::time::Duration::from_secs(10)
        }
    }

    fn accepted(
        &mut self,
        operation: DeploymentOperation,
        deployment: &StackDeploymentResponse,
        detach: bool,
    ) {
        if !self.interactive {
            eprintln!(
                "{}: {} generation {} accepted{}",
                deployment.stack,
                operation.label(),
                deployment.generation,
                if detach { " (detached)" } else { "" }
            );
            return;
        }

        let accepted = if detach {
            "accepted (detached)"
        } else {
            "accepted"
        };
        let line = format!(
            "{} {} · {} {}",
            ansi(self.color, "32", "✓"),
            ansi(self.color, "1", compact_name(&deployment.stack)),
            ansi(self.color, "36", operation.label()),
            ansi(self.color, "32", accepted)
        );
        let mut stderr = std::io::stderr().lock();
        let _ = writeln!(stderr, "{line}");
        let _ = stderr.flush();
    }

    fn render(
        &mut self,
        operation: DeploymentOperation,
        deployment: &StackDeploymentResponse,
        elapsed: std::time::Duration,
    ) {
        if !self.interactive {
            eprintln!(
                "{}",
                deployment_progress_summary(operation, deployment, elapsed)
            );
            return;
        }

        let complete = deployment.status != StackDeploymentStatus::Reconciling;
        let line = deployment_terminal_progress_summary(
            operation, deployment, elapsed, self.frame, self.color,
        );
        self.frame = self.frame.wrapping_add(1);
        let mut stderr = std::io::stderr().lock();
        let _ = write_terminal_progress(&mut stderr, &line, complete);
        self.line_active = !complete;
    }

    fn warning(&mut self, message: &str) {
        if !self.interactive {
            eprintln!("{message}");
            return;
        }

        let mut stderr = std::io::stderr().lock();
        if self.line_active {
            let _ = write!(stderr, "\r\x1b[2K");
            self.line_active = false;
        }
        let _ = writeln!(stderr, "{} {message}", ansi(self.color, "33", "!"));
        let _ = stderr.flush();
    }
}

impl Drop for DeploymentProgressRenderer {
    fn drop(&mut self) {
        if self.line_active {
            let mut stderr = std::io::stderr().lock();
            let _ = writeln!(stderr);
            let _ = stderr.flush();
            self.line_active = false;
        }
    }
}

fn write_terminal_progress(
    writer: &mut impl Write,
    line: &str,
    complete: bool,
) -> std::io::Result<()> {
    write!(writer, "\r\x1b[2K{line}")?;
    if complete {
        writeln!(writer)?;
    }
    writer.flush()
}

fn deployment_terminal_progress_summary(
    operation: DeploymentOperation,
    deployment: &StackDeploymentResponse,
    elapsed: std::time::Duration,
    frame: usize,
    color: bool,
) -> String {
    let (symbol, symbol_color) = match deployment.status {
        StackDeploymentStatus::Reconciling => {
            (PROGRESS_SPINNER[frame % PROGRESS_SPINNER.len()], "36")
        }
        StackDeploymentStatus::Healthy => ("✓", "32"),
        StackDeploymentStatus::Stalled | StackDeploymentStatus::Blocked => ("!", "33"),
        StackDeploymentStatus::Failed => ("✗", "31"),
        StackDeploymentStatus::Superseded => ("↷", "2"),
    };
    let action = match deployment.status {
        StackDeploymentStatus::Reconciling => match operation {
            DeploymentOperation::Deploy => "deploying",
            DeploymentOperation::Retry => "retrying",
            DeploymentOperation::Rollback => "rolling back",
            DeploymentOperation::Scale => "scaling",
            DeploymentOperation::Restart => "restarting",
            DeploymentOperation::Remove => "removing",
        },
        StackDeploymentStatus::Healthy => match operation {
            DeploymentOperation::Deploy => "deploy complete",
            DeploymentOperation::Retry => "retry complete",
            DeploymentOperation::Rollback => "rollback complete",
            DeploymentOperation::Scale => "scale complete",
            DeploymentOperation::Restart => "restart complete",
            DeploymentOperation::Remove => "remove complete",
        },
        StackDeploymentStatus::Failed => "failed",
        StackDeploymentStatus::Stalled => "stalled",
        StackDeploymentStatus::Blocked => "blocked",
        StackDeploymentStatus::Superseded => "superseded",
    };
    let mut parts = vec![format!(
        "{} {}",
        ansi(color, symbol_color, symbol),
        ansi(color, "1", compact_name(&deployment.stack))
    )];
    if deployment.status == StackDeploymentStatus::Reconciling {
        parts.push(ansi(color, "36", deployment_stage(operation, deployment)));
    } else {
        parts.push(ansi(color, symbol_color, action));
    }

    if operation == DeploymentOperation::Remove {
        parts.push(ansi(
            color,
            if deployment.pending_removals == 0 {
                "32"
            } else {
                "33"
            },
            format!(
                "{} {} remaining",
                deployment.pending_removals,
                plural(deployment.pending_removals, "container", "containers")
            ),
        ));
    } else {
        let replicas = deployment
            .services
            .iter()
            .map(|service| service.replicas)
            .sum::<u32>();
        let healthy = deployment
            .services
            .iter()
            .map(|service| service.healthy)
            .sum::<u32>();
        parts.push(ansi(
            color,
            if healthy >= replicas { "32" } else { "33" },
            format!("{healthy}/{replicas} containers ready"),
        ));
        if deployment.pending_removals > 0 {
            parts.push(ansi(
                color,
                "33",
                format!(
                    "{} old {}",
                    deployment.pending_removals,
                    plural(deployment.pending_removals, "container", "containers")
                ),
            ));
        }
    }

    if let Some((summary, color_code)) = image_resolution_summary(deployment) {
        parts.push(ansi(color, color_code, summary));
    }

    if let Some(gateway) = &deployment.gateway {
        let gateway_status = if gateway.applied_nodes >= gateway.total_nodes {
            "gateway ready".to_owned()
        } else if gateway.applied_nodes == 0 {
            format!(
                "waiting for {} gateway {}",
                gateway.total_nodes,
                plural(gateway.total_nodes, "node", "nodes")
            )
        } else {
            format!(
                "{} of {} gateway nodes ready",
                gateway.applied_nodes, gateway.total_nodes
            )
        };
        if gateway.applied_nodes < gateway.total_nodes
            || deployment.status != StackDeploymentStatus::Reconciling
        {
            parts.push(ansi(
                color,
                if gateway.applied_nodes >= gateway.total_nodes {
                    "32"
                } else {
                    "35"
                },
                gateway_status,
            ));
        }
        if !gateway.errors.is_empty() {
            parts.push(ansi(
                color,
                "31",
                format!("{} gateway error(s)", gateway.errors.len()),
            ));
        }
    }

    parts.push(ansi(color, "2", format!("{:.1}s", elapsed.as_secs_f64())));
    parts.join(" · ")
}

fn deployment_stage(
    operation: DeploymentOperation,
    deployment: &StackDeploymentResponse,
) -> &'static str {
    use swarmlite::model::{ImageResolutionStatus, TaskReconcilePhase};

    let reached = |phase| {
        deployment
            .task_phases
            .iter()
            .any(|progress| progress.phase == phase)
    };
    let replicas = deployment
        .services
        .iter()
        .map(|service| service.replicas)
        .sum::<u32>();
    let applied = deployment
        .services
        .iter()
        .map(|service| service.applied)
        .sum::<u32>();
    let healthy = deployment
        .services
        .iter()
        .map(|service| service.healthy)
        .sum::<u32>();
    let gateway_waiting = deployment
        .gateway
        .as_ref()
        .is_some_and(|gateway| gateway.applied_nodes < gateway.total_nodes);

    if operation == DeploymentOperation::Remove {
        if deployment.pending_removals > 0 {
            if reached(TaskReconcilePhase::Remove) {
                return "removing containers";
            }
            if reached(TaskReconcilePhase::Stop) {
                return "stopping containers";
            }
            return "waiting for agents";
        }
        return if gateway_waiting {
            "updating gateway"
        } else {
            "finishing"
        };
    }

    if deployment
        .image_resolutions
        .iter()
        .any(|image| image.status == ImageResolutionStatus::Pulling)
    {
        return "pulling images";
    }
    if deployment
        .image_resolutions
        .iter()
        .any(|image| image.status == ImageResolutionStatus::Comparing)
    {
        return "comparing image IDs";
    }
    if deployment
        .image_resolutions
        .iter()
        .any(|image| image.status == ImageResolutionStatus::Checking)
    {
        return "checking images";
    }
    if deployment
        .image_resolutions
        .iter()
        .any(|image| image.status == ImageResolutionStatus::Changed)
        && (applied < replicas || healthy < replicas || deployment.pending_removals > 0)
    {
        return "image changed; updating";
    }

    if reached(TaskReconcilePhase::Remove) && deployment.pending_removals > 0 {
        return "removing old containers";
    }
    if reached(TaskReconcilePhase::Stop) && deployment.pending_removals > 0 {
        return "stopping old containers";
    }
    if healthy >= replicas && deployment.pending_removals > 0 {
        return "draining old containers";
    }
    if healthy >= replicas && gateway_waiting {
        return "updating gateway";
    }
    if reached(TaskReconcilePhase::Verify) && healthy < replicas {
        return "checking container health";
    }
    if reached(TaskReconcilePhase::Start) && applied < replicas {
        return "starting containers";
    }
    if reached(TaskReconcilePhase::Replace) {
        return "replacing containers";
    }
    if reached(TaskReconcilePhase::Create) {
        return "creating containers";
    }
    if reached(TaskReconcilePhase::Pull) {
        return "pulling images";
    }
    if reached(TaskReconcilePhase::Config) {
        return "preparing configs";
    }
    if reached(TaskReconcilePhase::Inspect) {
        return "inspecting runtime";
    }
    if applied < replicas {
        return "scheduling containers";
    }
    if healthy < replicas {
        return "checking container health";
    }
    if gateway_waiting {
        return "updating gateway";
    }
    "finishing"
}

fn image_resolution_summary(
    deployment: &StackDeploymentResponse,
) -> Option<(String, &'static str)> {
    use swarmlite::model::ImageResolutionStatus;

    if deployment.image_resolutions.is_empty()
        || deployment.image_resolutions.iter().any(|image| {
            matches!(
                image.status,
                ImageResolutionStatus::Checking
                    | ImageResolutionStatus::Pulling
                    | ImageResolutionStatus::Comparing
            )
        })
    {
        return None;
    }
    let changed = deployment
        .image_resolutions
        .iter()
        .filter(|image| image.status == ImageResolutionStatus::Changed)
        .count();
    let unchanged = deployment
        .image_resolutions
        .iter()
        .filter(|image| image.status == ImageResolutionStatus::Unchanged)
        .count();
    let skipped = deployment
        .image_resolutions
        .iter()
        .filter(|image| image.status == ImageResolutionStatus::Skipped)
        .count();
    let failed = deployment
        .image_resolutions
        .iter()
        .filter(|image| image.status == ImageResolutionStatus::Failed)
        .count();
    if failed > 0 {
        return Some((format!("{failed} image check(s) failed"), "31"));
    }
    if deployment.image_resolutions.len() == 1 {
        let image = &deployment.image_resolutions[0];
        let outcome = match image.status {
            ImageResolutionStatus::Changed => "image changed",
            ImageResolutionStatus::Unchanged => "image unchanged",
            ImageResolutionStatus::Skipped => "image check skipped",
            _ => return None,
        };
        return Some((
            format!("{} {outcome}", image.service),
            if changed > 0 { "33" } else { "32" },
        ));
    }
    let mut outcomes = Vec::new();
    if changed > 0 {
        outcomes.push(format!("{changed} changed"));
    }
    if unchanged > 0 {
        outcomes.push(format!("{unchanged} unchanged"));
    }
    if skipped > 0 {
        outcomes.push(format!("{skipped} skipped"));
    }
    Some((
        format!("images {}", outcomes.join(", ")),
        if changed > 0 { "33" } else { "32" },
    ))
}

fn plural<'a>(count: u32, singular: &'a str, plural: &'a str) -> &'a str {
    if count == 1 { singular } else { plural }
}

fn compact_name(value: &str) -> String {
    const MAX_CHARS: usize = 24;
    if value.chars().count() <= MAX_CHARS {
        return value.to_owned();
    }
    let mut compact = value.chars().take(MAX_CHARS - 1).collect::<String>();
    compact.push('…');
    compact
}

fn ansi(color: bool, code: &str, value: impl std::fmt::Display) -> String {
    if color {
        format!("\x1b[{code}m{value}\x1b[0m")
    } else {
        value.to_string()
    }
}

fn deployment_progress_summary(
    operation: DeploymentOperation,
    deployment: &StackDeploymentResponse,
    elapsed: std::time::Duration,
) -> String {
    let elapsed = format!("{:.1}s", elapsed.as_secs_f64());
    let status = match deployment.status {
        StackDeploymentStatus::Reconciling => "deploying",
        StackDeploymentStatus::Healthy => "healthy",
        StackDeploymentStatus::Failed => "failed",
        StackDeploymentStatus::Stalled => "stalled",
        StackDeploymentStatus::Blocked => "blocked",
        StackDeploymentStatus::Superseded => "superseded",
    };
    let phases = deployment
        .task_phases
        .iter()
        .map(|progress| format!("{}={}", task_phase_label(progress.phase), progress.tasks))
        .collect::<Vec<_>>()
        .join(",");
    let phases = (!phases.is_empty()).then(|| format!("; phases {phases}"));
    let images = (!deployment.image_resolutions.is_empty()).then(|| {
        let statuses = deployment
            .image_resolutions
            .iter()
            .map(|image| {
                format!(
                    "{}={}",
                    image.service,
                    image_resolution_status_label(image.status)
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        format!("; images {statuses}")
    });
    let gateway = deployment.gateway.as_ref().map(|gateway| {
        let errors = gateway
            .errors
            .iter()
            .map(|(node, error)| format!("{node}={error}"))
            .collect::<Vec<_>>()
            .join("; ");
        format!(
            "; gateway {}/{} applied{}",
            gateway.applied_nodes,
            gateway.total_nodes,
            (!errors.is_empty())
                .then(|| format!("; errors [{errors}]"))
                .as_deref()
                .unwrap_or_default()
        )
    });
    if operation == DeploymentOperation::Remove {
        let status = match deployment.status {
            StackDeploymentStatus::Reconciling => "removing",
            StackDeploymentStatus::Healthy => "complete",
            StackDeploymentStatus::Failed => "failed",
            StackDeploymentStatus::Stalled => "stalled",
            StackDeploymentStatus::Blocked => "blocked",
            StackDeploymentStatus::Superseded => "superseded",
        };
        return format!(
            "{}: remove generation {} {}: {} task(s) remaining{}{}{} ({elapsed})",
            deployment.stack,
            deployment.generation,
            status,
            deployment.pending_removals,
            phases.as_deref().unwrap_or_default(),
            images.as_deref().unwrap_or_default(),
            gateway.as_deref().unwrap_or_default()
        );
    }

    let replicas = deployment
        .services
        .iter()
        .map(|service| service.replicas)
        .sum::<u32>();
    let applied = deployment
        .services
        .iter()
        .map(|service| service.applied)
        .sum::<u32>();
    let healthy = deployment
        .services
        .iter()
        .map(|service| service.healthy)
        .sum::<u32>();
    let services = deployment
        .services
        .iter()
        .map(|service| {
            format!(
                "{}={}/{} applied,{}/{} healthy",
                service.service,
                service.applied,
                service.replicas,
                service.healthy,
                service.replicas
            )
        })
        .collect::<Vec<_>>()
        .join("; ");
    let removals = (deployment.pending_removals > 0)
        .then(|| format!("; {} task(s) pending removal", deployment.pending_removals));
    format!(
        "{}: {} generation {} {status}: {applied}/{replicas} applied, {healthy}/{replicas} healthy [{}]{}{}{}{} ({elapsed})",
        deployment.stack,
        operation.label(),
        deployment.generation,
        services,
        removals.as_deref().unwrap_or_default(),
        phases.as_deref().unwrap_or_default(),
        images.as_deref().unwrap_or_default(),
        gateway.as_deref().unwrap_or_default()
    )
}

fn image_resolution_status_label(status: swarmlite::model::ImageResolutionStatus) -> &'static str {
    use swarmlite::model::ImageResolutionStatus;
    match status {
        ImageResolutionStatus::Checking => "checking",
        ImageResolutionStatus::Pulling => "pulling",
        ImageResolutionStatus::Comparing => "comparing",
        ImageResolutionStatus::Unchanged => "unchanged",
        ImageResolutionStatus::Changed => "changed/updating",
        ImageResolutionStatus::Skipped => "skipped",
        ImageResolutionStatus::Failed => "failed",
    }
}

fn task_phase_label(phase: swarmlite::model::TaskReconcilePhase) -> &'static str {
    use swarmlite::model::TaskReconcilePhase;
    match phase {
        TaskReconcilePhase::Inspect => "inspect",
        TaskReconcilePhase::Config => "config",
        TaskReconcilePhase::Pull => "pull",
        TaskReconcilePhase::Create => "create",
        TaskReconcilePhase::Replace => "replace",
        TaskReconcilePhase::Start => "start",
        TaskReconcilePhase::Stop => "stop",
        TaskReconcilePhase::Remove => "remove",
        TaskReconcilePhase::Verify => "verify",
    }
}

fn ensure_deployment_succeeded(deployment: &StackDeploymentResponse) -> Result<()> {
    if !matches!(
        deployment.status,
        StackDeploymentStatus::Failed
            | StackDeploymentStatus::Stalled
            | StackDeploymentStatus::Blocked
            | StackDeploymentStatus::Superseded
    ) {
        return Ok(());
    }
    let mut details = deployment
        .errors
        .iter()
        .map(|error| {
            format!(
                "{} on {} during {:?}: {}",
                error.service, error.node_id, error.phase, error.message
            )
        })
        .collect::<Vec<_>>();
    if let Some(gateway) = &deployment.gateway
        && gateway.applied_nodes < gateway.total_nodes
    {
        if gateway.errors.is_empty() {
            details.push(format!(
                "gateway configuration reached {}/{} nodes",
                gateway.applied_nodes, gateway.total_nodes
            ));
        } else {
            details.extend(
                gateway
                    .errors
                    .iter()
                    .map(|(node, error)| format!("gateway on {node}: {error}")),
            );
        }
    }
    let details = details.join("; ");
    anyhow::bail!(
        "stack {:?} deployment {}: {}",
        deployment.stack,
        match deployment.status {
            StackDeploymentStatus::Failed => "failed",
            StackDeploymentStatus::Stalled => "stalled",
            StackDeploymentStatus::Blocked => "blocked",
            StackDeploymentStatus::Superseded => "was superseded",
            _ => unreachable!(),
        },
        if details.is_empty() {
            "no task reached the required healthy state".to_owned()
        } else {
            details
        }
    )
}

async fn node_labels(
    client: &ControllerClient,
    node_id: &str,
    method: reqwest::Method,
    body: Option<&serde_json::Value>,
) -> Result<NodeLabelsResponse> {
    Ok(client
        .send_json(method, &format!("/v1/nodes/{node_id}/labels"), body)
        .await?)
}

async fn node_gateway(
    client: &ControllerClient,
    node_id: &str,
    enabled: Option<bool>,
) -> Result<NodeGatewayResponse> {
    let update = enabled.map(|enabled| NodeGatewayUpdate { enabled });
    let method = if update.is_some() {
        reqwest::Method::PUT
    } else {
        reqwest::Method::GET
    };
    Ok(client
        .send_json(
            method,
            &format!("/v1/nodes/{node_id}/gateway"),
            update.as_ref(),
        )
        .await?)
}

async fn cluster_config(
    client: &ControllerClient,
    update: Option<&ClusterConfigUpdate>,
) -> Result<ClusterConfigResponse> {
    let method = if update.is_some() {
        reqwest::Method::PATCH
    } else {
        reqwest::Method::GET
    };
    Ok(client.send_json(method, "/v1/config", update).await?)
}

async fn deploy(
    client: &ControllerClient,
    name: Option<String>,
    file: PathBuf,
    detach: bool,
    dry_run: bool,
    json: bool,
    replace: bool,
) -> Result<()> {
    let stack = tokio::fs::read_to_string(&file)
        .await
        .with_context(|| format!("failed to read Stack file {}", file.display()))?;
    let document = swarmlite_stack::parse_stack_document(&stack)?;
    let request = stack_apply_request(client, &file, stack, &document.configs).await?;
    let name = resolve_stack_name(name, document.name)?;
    if dry_run {
        let validation: StackValidationResponse = client
            .send_json(
                reqwest::Method::PUT,
                &format!("/v1/stacks/{name}/validate"),
                Some(&request),
            )
            .await?;
        println!("{}", serde_json::to_string_pretty(&validation)?);
        return Ok(());
    }
    let deployment: StackDeploymentResponse = client
        .send_json(
            reqwest::Method::PUT,
            &format!(
                "/v1/stacks/{name}{}",
                if replace { "?replace=true" } else { "" }
            ),
            Some(&request),
        )
        .await?;
    let deployment =
        finish_deployment(client, deployment, detach, DeploymentOperation::Deploy).await?;
    if json {
        println!("{}", serde_json::to_string_pretty(&deployment)?);
    }
    Ok(())
}

async fn stack_apply_request(
    client: &ControllerClient,
    stack_file: &Path,
    yaml: String,
    configs: &BTreeMap<String, swarmlite_stack::StackConfigSource>,
) -> Result<StackApplyRequest> {
    let mut request = local_stack_apply_request(stack_file, yaml, configs).await?;
    if request.configs.is_empty() {
        return Ok(request);
    }
    let check = ConfigBlobCheckRequest {
        digests: request
            .configs
            .values()
            .map(|payload| payload.digest.clone())
            .collect(),
    };
    let response: ConfigBlobCheckResponse = client
        .send_json(reqwest::Method::POST, "/v1/configs/check", Some(&check))
        .await?;
    retain_missing_config_contents(&mut request, &response.missing);
    Ok(request)
}

fn retain_missing_config_contents(request: &mut StackApplyRequest, missing: &BTreeSet<String>) {
    let mut included = BTreeSet::new();
    for payload in request.configs.values_mut() {
        if !missing.contains(&payload.digest) || !included.insert(payload.digest.clone()) {
            payload.data_base64 = None;
        }
    }
}

async fn local_stack_apply_request(
    stack_file: &Path,
    yaml: String,
    configs: &BTreeMap<String, swarmlite_stack::StackConfigSource>,
) -> Result<StackApplyRequest> {
    let base = stack_file
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let mut payloads = BTreeMap::new();
    let mut total_bytes = 0_usize;
    for (name, config) in configs {
        let declared = Path::new(&config.file);
        let path = if declared.is_absolute() {
            declared.to_owned()
        } else {
            base.join(declared)
        };
        let metadata = tokio::fs::metadata(&path)
            .await
            .with_context(|| format!("failed to read config {name:?} from {}", path.display()))?;
        if !metadata.is_file() {
            bail!(
                "config {name:?} must reference a regular file: {}",
                path.display()
            );
        }
        let size = usize::try_from(metadata.len())
            .with_context(|| format!("config {name:?} is too large"))?;
        if size > MAX_CONFIG_FILE_BYTES {
            bail!(
                "config {name:?} is {size} bytes; each config may contain at most {MAX_CONFIG_FILE_BYTES} bytes"
            );
        }
        total_bytes = total_bytes
            .checked_add(size)
            .context("total config size overflow")?;
        if total_bytes > MAX_STACK_CONFIG_BYTES {
            bail!(
                "Stack configs contain {total_bytes} bytes; at most {MAX_STACK_CONFIG_BYTES} bytes may be uploaded per deployment"
            );
        }
        let contents = tokio::fs::read(&path)
            .await
            .with_context(|| format!("failed to read config {name:?} from {}", path.display()))?;
        if contents.len() != size {
            bail!("config {name:?} changed while it was being read; retry the deployment");
        }
        payloads.insert(
            name.clone(),
            StackConfigPayload {
                digest: config_digest(&contents),
                data_base64: Some(BASE64_STANDARD.encode(contents)),
            },
        );
    }
    Ok(StackApplyRequest {
        yaml,
        configs: payloads,
    })
}

fn resolve_stack_name(command_line: Option<String>, document: Option<String>) -> Result<String> {
    command_line
        .or(document)
        .context("stack name is required; pass STACK to `swarmlite deploy` or set x-swarmlite.name")
}

async fn wait_for_deployment(
    client: &ControllerClient,
    stack_name: &str,
    mut deployment: StackDeploymentResponse,
    operation: DeploymentOperation,
    started: tokio::time::Instant,
    progress: &mut DeploymentProgressRenderer,
) -> Result<StackDeploymentResponse> {
    const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(35);
    let mut progress_tick = tokio::time::interval(progress.refresh_interval());
    progress_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    progress_tick.tick().await;
    while deployment.status == StackDeploymentStatus::Reconciling {
        let path = format!(
            "/v1/stacks/{stack_name}/deployment?generation={}&after_revision={}&wait_seconds=25",
            deployment.generation, deployment.revision
        );
        let request = tokio::time::timeout(
            REQUEST_TIMEOUT,
            client.get_json::<StackDeploymentResponse>(&path),
        );
        tokio::pin!(request);
        let response = loop {
            tokio::select! {
                response = &mut request => break response,
                _ = progress_tick.tick() => {
                    progress.render(operation, &deployment, started.elapsed());
                }
            }
        };
        match response {
            Ok(Ok(next)) => {
                let progress_changed = deployment.status != next.status
                    || deployment.services != next.services
                    || deployment.pending_removals != next.pending_removals
                    || deployment.task_phases != next.task_phases
                    || deployment.image_resolutions != next.image_resolutions
                    || deployment.gateway != next.gateway
                    || deployment.errors != next.errors;
                deployment = next;
                if progress_changed {
                    progress.render(operation, &deployment, started.elapsed());
                }
            }
            Ok(Err(error)) if error.is_retryable() => {
                let error: anyhow::Error = error.into();
                progress.warning(&format!(
                    "{stack_name}: controller unavailable; retrying: {error}"
                ));
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
            Ok(Err(error)) => return Err(error.into()),
            Err(_) => {
                let error = anyhow::anyhow!("controller deployment watch timed out");
                progress.warning(&format!("{stack_name}: {error}; retrying"));
            }
        }
    }
    Ok(deployment)
}

async fn status(client: &ControllerClient, json: bool) -> Result<()> {
    let value: serde_json::Value = client.get_json("/v1/status").await?;
    if json {
        println!("{}", serde_json::to_string_pretty(&value)?);
    } else {
        let response: StatusResponse = serde_json::from_value(value)
            .context("controller returned an invalid status response")?;
        let color = std::io::stdout().is_terminal()
            && std::env::var("TERM").ok().as_deref() != Some("dumb")
            && std::env::var_os("NO_COLOR").is_none();
        print!("{}", format_status(&response, color));
    }
    Ok(())
}

fn format_status(response: &StatusResponse, color: bool) -> String {
    use std::fmt::Write as _;

    let state = &response.state;
    let active_services = state
        .services
        .values()
        .filter(|service| !service.deleted)
        .count();

    let mut output = String::new();
    writeln!(output, "{}", ansi(color, "1;36", "Cluster")).unwrap();
    writeln!(
        output,
        "  ID:         {}",
        ansi(color, "1", &response.cluster_id)
    )
    .unwrap();
    writeln!(output, "  Generation: {}", response.generation).unwrap();
    writeln!(
        output,
        "  Controller: {}",
        ansi(color, "1", &response.controller_id)
    )
    .unwrap();

    writeln!(output, "\n{}", ansi(color, "1;36", "Resources")).unwrap();
    writeln!(output, "  Nodes:           {}", state.members.len()).unwrap();
    writeln!(output, "  Stacks:          {}", state.stacks.len()).unwrap();
    writeln!(output, "  Services:        {active_services}").unwrap();
    let task_summary = format_task_summary(response);
    writeln!(
        output,
        "  Tasks:           {}",
        ansi(color, task_summary_color(response), task_summary)
    )
    .unwrap();
    let deployment_summary = format_deployment_summary(response);
    writeln!(
        output,
        "  Deployments:     {}",
        ansi(
            color,
            deployment_summary_color(response),
            deployment_summary
        )
    )
    .unwrap();
    writeln!(
        output,
        "  Unclaimed tasks: {}",
        ansi(
            color,
            attention_count_color(state.unclaimed_tasks.len()),
            state.unclaimed_tasks.len()
        )
    )
    .unwrap();

    let gateway_status = if !response.gateway.enabled {
        "disabled"
    } else if !response.gateway.endpoint_errors.is_empty() {
        "degraded"
    } else if response.gateway.applied_generation == Some(response.gateway.desired_generation) {
        "ready"
    } else {
        "pending"
    };
    let gateway_generation = if !response.gateway.enabled {
        "-".to_owned()
    } else if let Some(applied) = response.gateway.applied_generation {
        format!("{applied}/{} applied", response.gateway.desired_generation)
    } else {
        format!("pending (desired {})", response.gateway.desired_generation)
    };
    let gateway_color = match gateway_status {
        "ready" => "32",
        "degraded" => "31",
        "pending" => "33",
        _ => "2",
    };
    writeln!(output, "\n{}", ansi(color, "1;36", "Gateway")).unwrap();
    writeln!(
        output,
        "  Status:     {}",
        ansi(color, gateway_color, gateway_status)
    )
    .unwrap();
    writeln!(output, "  Generation: {gateway_generation}").unwrap();
    writeln!(
        output,
        "  Errors:     {}",
        ansi(
            color,
            attention_count_color(response.gateway.endpoint_errors.len()),
            response.gateway.endpoint_errors.len()
        )
    )
    .unwrap();

    let recovery_status =
        if response.recovery.awaiting_adoption == 0 && response.recovery.conflicting_slots == 0 {
            "clean"
        } else {
            "needs attention"
        };
    let recovery_color = if recovery_status == "clean" {
        "32"
    } else {
        "31"
    };
    writeln!(output, "\n{}", ansi(color, "1;36", "Recovery")).unwrap();
    writeln!(
        output,
        "  Status:            {}",
        ansi(color, recovery_color, recovery_status)
    )
    .unwrap();
    writeln!(
        output,
        "  Awaiting adoption: {}",
        ansi(
            color,
            attention_count_color(response.recovery.awaiting_adoption),
            response.recovery.awaiting_adoption
        )
    )
    .unwrap();
    writeln!(
        output,
        "  Conflicting slots: {}",
        ansi(
            color,
            attention_count_color(response.recovery.conflicting_slots),
            response.recovery.conflicting_slots
        )
    )
    .unwrap();

    writeln!(output, "\n{}", ansi(color, "1;36", "Nodes")).unwrap();
    if state.members.is_empty() {
        writeln!(output, "  {}", ansi(color, "2", "none")).unwrap();
    } else {
        let rows = state
            .members
            .values()
            .map(|member| {
                let tasks = state
                    .tasks
                    .values()
                    .filter(|task| task.node_id.as_str() == member.id.as_str())
                    .count();
                vec![
                    member.id.clone(),
                    member.address.clone(),
                    if member.gateway_enabled {
                        ansi(color, "32", "enabled")
                    } else {
                        ansi(color, "2", "disabled")
                    },
                    tasks.to_string(),
                    format_labels(&member.labels),
                ]
            })
            .collect::<Vec<_>>();
        append_table(
            &mut output,
            &["ID", "ADDRESS", "GATEWAY", "TASKS", "LABELS"],
            &rows,
            color,
        );
    }

    let mut issues = response
        .gateway
        .endpoint_errors
        .iter()
        .map(|(node_id, error)| {
            vec![
                ansi(color, "31", "gateway"),
                ansi(color, "1", node_id),
                ansi(color, "31", single_line(error)),
            ]
        })
        .collect::<Vec<_>>();
    issues.extend(state.tasks.values().filter_map(|task| {
        task.reconcile_error.as_ref().map(|error| {
            vec![
                ansi(color, "31", "task"),
                ansi(color, "1", task.id.chars().take(12).collect::<String>()),
                ansi(
                    color,
                    "31",
                    format!(
                        "{} on {}: {}",
                        format!("{:?}", error.phase).to_ascii_lowercase(),
                        task.node_id,
                        single_line(&error.message)
                    ),
                ),
            ]
        })
    }));
    if !issues.is_empty() {
        writeln!(output, "\n{}", ansi(color, "1;31", "Issues")).unwrap();
        append_table(&mut output, &["TYPE", "RESOURCE", "DETAIL"], &issues, color);
    }

    output
}

fn format_task_summary(response: &StatusResponse) -> String {
    use swarmlite::model::ObservedTaskState;

    let states = [
        (ObservedTaskState::Healthy, "healthy"),
        (ObservedTaskState::Running, "running"),
        (ObservedTaskState::Starting, "starting"),
        (ObservedTaskState::Pending, "pending"),
        (ObservedTaskState::Failed, "failed"),
        (ObservedTaskState::Lost, "lost"),
    ];
    let details = states
        .iter()
        .filter_map(|(state, label)| {
            let count = response
                .state
                .tasks
                .values()
                .filter(|task| &task.observed == state)
                .count();
            (count > 0).then(|| format!("{count} {label}"))
        })
        .collect::<Vec<_>>();
    let total = response.state.tasks.len();
    if details.is_empty() {
        total.to_string()
    } else {
        format!("{total} ({})", details.join(", "))
    }
}

fn task_summary_color(response: &StatusResponse) -> &'static str {
    use swarmlite::model::ObservedTaskState;

    if response.state.tasks.values().any(|task| {
        matches!(
            &task.observed,
            ObservedTaskState::Failed | ObservedTaskState::Lost
        )
    }) {
        "31"
    } else if response.state.tasks.values().any(|task| {
        matches!(
            &task.observed,
            ObservedTaskState::Pending | ObservedTaskState::Starting
        )
    }) {
        "33"
    } else if response.state.tasks.is_empty() {
        "2"
    } else {
        "32"
    }
}

fn format_deployment_summary(response: &StatusResponse) -> String {
    let states = [
        (StackDeploymentStatus::Healthy, "healthy"),
        (StackDeploymentStatus::Reconciling, "deploying"),
        (StackDeploymentStatus::Failed, "failed"),
        (StackDeploymentStatus::Stalled, "stalled"),
        (StackDeploymentStatus::Blocked, "blocked"),
        (StackDeploymentStatus::Superseded, "superseded"),
    ];
    let details = states
        .iter()
        .filter_map(|(status, label)| {
            let count = response
                .state
                .stacks
                .values()
                .filter_map(|stack| stack.deployment.as_ref())
                .filter(|deployment| deployment.status == *status)
                .count();
            (count > 0).then(|| format!("{count} {label}"))
        })
        .collect::<Vec<_>>();
    if details.is_empty() {
        "none".to_owned()
    } else {
        details.join(", ")
    }
}

fn deployment_summary_color(response: &StatusResponse) -> &'static str {
    let deployments = response
        .state
        .stacks
        .values()
        .filter_map(|stack| stack.deployment.as_ref())
        .collect::<Vec<_>>();
    if deployments.iter().any(|deployment| {
        matches!(
            deployment.status,
            StackDeploymentStatus::Failed | StackDeploymentStatus::Blocked
        )
    }) {
        "31"
    } else if deployments.iter().any(|deployment| {
        matches!(
            deployment.status,
            StackDeploymentStatus::Reconciling | StackDeploymentStatus::Stalled
        )
    }) {
        "33"
    } else if deployments.is_empty() {
        "2"
    } else {
        "32"
    }
}

fn attention_count_color(count: usize) -> &'static str {
    if count == 0 { "32" } else { "31" }
}

fn format_labels(labels: &BTreeMap<String, String>) -> String {
    if labels.is_empty() {
        "-".to_owned()
    } else {
        labels
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join(",")
    }
}

fn single_line(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn append_table(output: &mut String, headers: &[&str], rows: &[Vec<String>], color: bool) {
    use std::fmt::Write as _;

    let mut widths = headers
        .iter()
        .map(|header| header.chars().count())
        .collect::<Vec<_>>();
    for row in rows {
        for (index, value) in row.iter().enumerate() {
            widths[index] = widths[index].max(display_width(value));
        }
    }
    let headers = headers
        .iter()
        .map(|header| ansi(color, "1", header))
        .collect::<Vec<_>>();
    for row in std::iter::once(&headers).chain(rows) {
        for (index, value) in row.iter().enumerate() {
            write!(output, "{value}").unwrap();
            if index + 1 < row.len() {
                let padding = widths[index].saturating_sub(display_width(value)) + 2;
                output.push_str(&" ".repeat(padding));
            }
        }
        output.push('\n');
    }
}

fn display_width(value: &str) -> usize {
    let mut escape = false;
    value
        .chars()
        .filter(|character| {
            if escape {
                if *character == 'm' {
                    escape = false;
                }
                false
            } else if *character == '\x1b' {
                escape = true;
                false
            } else {
                true
            }
        })
        .count()
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        path::PathBuf,
    };

    use base64::Engine as _;
    use clap::{CommandFactory, Parser};
    use swarmlite::{
        config::DEFAULT_CONTROLLER_PORT,
        model::{
            ClusterState, DesiredTaskState, GatewayStatus, ImageResolutionStatus, NodeMember,
            ObservedTaskState, RecoveryStatus, StackDeploymentGatewayProgress,
            StackDeploymentImageProgress, StackDeploymentResponse, StackDeploymentServiceProgress,
            StackDeploymentStatus, StackDeploymentTaskPhaseProgress, StatusResponse,
            TaskReconcileError, TaskReconcilePhase, TaskRecord,
        },
    };

    use super::{
        Cli, Command, DeploymentOperation, DeploymentProgressRenderer, deployment_progress_summary,
        deployment_terminal_progress_summary, display_width, format_status,
        local_stack_apply_request, resolve_stack_name, retain_missing_config_contents,
        write_terminal_progress,
    };

    #[test]
    fn cli_definition_is_valid() {
        Cli::command().debug_assert();
    }

    #[test]
    fn deploy_waits_by_default_and_supports_detach() {
        let parse = |extra: &[&str]| {
            let mut arguments = vec!["swarmlite", "deploy"];
            arguments.extend_from_slice(extra);
            Cli::try_parse_from(arguments).unwrap()
        };
        let Command::Deploy { options } = parse(&[]).command else {
            panic!("expected deploy command");
        };
        assert_eq!(options.file, PathBuf::from("swarmlite.yaml"));
        assert_eq!(options.stack, None);
        assert!(!options.dry_run);
        assert!(!options.json);
        assert!(!options.replace);
        assert!(matches!(
            parse(&["--detach"]).command,
            Command::Deploy { .. }
        ));
        let Command::Deploy { options } = parse(&["--dry-run"]).command else {
            panic!("expected deploy command");
        };
        assert!(options.dry_run);
        assert!(Cli::try_parse_from(["swarmlite", "deploy", "--dry-run", "--detach"]).is_err());
        assert!(Cli::try_parse_from(["swarmlite", "deploy", "--dry-run", "--replace"]).is_err());
        let Command::Deploy { options } = parse(&["--replace"]).command else {
            panic!("expected deploy command");
        };
        assert!(options.replace);
        let Command::Deploy { options } = parse(&["--json"]).command else {
            panic!("expected deploy command");
        };
        assert!(options.json);
        let Command::Deploy { options } = parse(&["demo", "-c", "production.yaml"]).command else {
            panic!("expected deploy command");
        };
        assert_eq!(options.file, PathBuf::from("production.yaml"));
        assert_eq!(options.stack.as_deref(), Some("demo"));
    }

    #[test]
    fn deployment_progress_summarizes_service_and_removal_state() {
        let mut deployment = StackDeploymentResponse {
            stack: "demo".into(),
            generation: 7,
            revision: 1,
            status: StackDeploymentStatus::Reconciling,
            started_at_unix_ms: 0,
            last_progress_at_unix_ms: 0,
            progress_deadline_seconds: 300,
            finished_at_unix_ms: None,
            superseded_by: None,
            retry_revision: 0,
            services: vec![StackDeploymentServiceProgress {
                service: "web".into(),
                replicas: 3,
                applied: 2,
                healthy: 1,
            }],
            pending_removals: 1,
            task_phases: vec![StackDeploymentTaskPhaseProgress {
                phase: TaskReconcilePhase::Pull,
                tasks: 1,
            }],
            image_resolutions: Vec::new(),
            gateway: Some(StackDeploymentGatewayProgress {
                generation: 8,
                applied_nodes: 0,
                total_nodes: 1,
                errors: Default::default(),
            }),
            conditions: Vec::new(),
            errors: Vec::new(),
        };
        assert_eq!(
            deployment_progress_summary(
                DeploymentOperation::Deploy,
                &deployment,
                std::time::Duration::from_millis(1_200),
            ),
            "demo: deploy generation 7 deploying: 2/3 applied, 1/3 healthy [web=2/3 applied,1/3 healthy]; 1 task(s) pending removal; phases pull=1; gateway 0/1 applied (1.2s)"
        );
        assert_eq!(
            deployment_terminal_progress_summary(
                DeploymentOperation::Deploy,
                &deployment,
                std::time::Duration::from_millis(1_200),
                0,
                false,
            ),
            "⠋ demo · pulling images · 1/3 containers ready · 1 old container · waiting for 1 gateway node · 1.2s"
        );
        let colored = deployment_terminal_progress_summary(
            DeploymentOperation::Deploy,
            &deployment,
            std::time::Duration::from_millis(1_200),
            1,
            true,
        );
        assert!(colored.contains("\x1b[36m⠙\x1b[0m"));
        assert!(colored.contains("\x1b[33m1/3 containers ready\x1b[0m"));
        assert!(colored.contains("\x1b[35mwaiting for 1 gateway node\x1b[0m"));
        assert!(!colored.contains("#7"));
        assert!(!colored.contains("pull 1/3"));

        deployment.services.clear();
        deployment.task_phases.clear();
        deployment.gateway = None;
        assert_eq!(
            deployment_progress_summary(
                DeploymentOperation::Remove,
                &deployment,
                std::time::Duration::from_secs(2),
            ),
            "demo: remove generation 7 removing: 1 task(s) remaining (2.0s)"
        );
        assert_eq!(
            deployment_terminal_progress_summary(
                DeploymentOperation::Remove,
                &deployment,
                std::time::Duration::from_secs(2),
                0,
                false,
            ),
            "⠋ demo · waiting for agents · 1 container remaining · 2.0s"
        );
        deployment.status = StackDeploymentStatus::Healthy;
        deployment.pending_removals = 0;
        assert_eq!(
            deployment_progress_summary(
                DeploymentOperation::Remove,
                &deployment,
                std::time::Duration::from_millis(2_500),
            ),
            "demo: remove generation 7 complete: 0 task(s) remaining (2.5s)"
        );
        assert_eq!(
            deployment_terminal_progress_summary(
                DeploymentOperation::Remove,
                &deployment,
                std::time::Duration::from_millis(2_500),
                0,
                false,
            ),
            "✓ demo · remove complete · 0 containers remaining · 2.5s"
        );
    }

    #[test]
    fn terminal_progress_rewrites_one_line_and_finishes_with_a_newline() {
        let mut output = Vec::new();
        write_terminal_progress(&mut output, "first", false).unwrap();
        write_terminal_progress(&mut output, "done", true).unwrap();
        assert_eq!(output, b"\r\x1b[2Kfirst\r\x1b[2Kdone\n");
    }

    #[test]
    fn image_progress_is_readable_in_tty_and_plain_output() {
        let mut deployment = StackDeploymentResponse {
            stack: "demo".into(),
            generation: 2,
            revision: 1,
            status: StackDeploymentStatus::Reconciling,
            started_at_unix_ms: 0,
            last_progress_at_unix_ms: 0,
            progress_deadline_seconds: 300,
            finished_at_unix_ms: None,
            superseded_by: None,
            retry_revision: 0,
            services: vec![StackDeploymentServiceProgress {
                service: "web".into(),
                replicas: 1,
                applied: 1,
                healthy: 1,
            }],
            pending_removals: 0,
            task_phases: Vec::new(),
            image_resolutions: vec![StackDeploymentImageProgress {
                service: "web".into(),
                image: "nginx:latest".into(),
                status: ImageResolutionStatus::Pulling,
                completed_nodes: 0,
                total_nodes: 1,
            }],
            gateway: None,
            conditions: Vec::new(),
            errors: Vec::new(),
        };
        let plain = deployment_progress_summary(
            DeploymentOperation::Deploy,
            &deployment,
            std::time::Duration::from_secs(1),
        );
        assert!(plain.contains("images web=pulling"));
        assert!(!plain.contains("\x1b["));
        assert!(
            deployment_terminal_progress_summary(
                DeploymentOperation::Deploy,
                &deployment,
                std::time::Duration::from_secs(1),
                0,
                false,
            )
            .contains("pulling images")
        );

        deployment.image_resolutions[0].status = ImageResolutionStatus::Unchanged;
        let colored = deployment_terminal_progress_summary(
            DeploymentOperation::Deploy,
            &deployment,
            std::time::Duration::from_secs(2),
            0,
            true,
        );
        assert!(colored.contains("web image unchanged"));
        assert!(colored.contains("\x1b[32m"));

        let no_color = DeploymentProgressRenderer::for_output(true, true);
        let redirected = DeploymentProgressRenderer::for_output(false, false);
        assert!(no_color.interactive);
        assert!(!no_color.color);
        assert!(!redirected.interactive);
        assert!(!redirected.color);
    }

    #[test]
    fn command_line_stack_name_overrides_document_name() {
        assert_eq!(
            resolve_stack_name(Some("override".into()), Some("embedded".into())).unwrap(),
            "override"
        );
        assert_eq!(
            resolve_stack_name(None, Some("embedded".into())).unwrap(),
            "embedded"
        );
        assert!(resolve_stack_name(None, None).is_err());
    }

    #[tokio::test]
    async fn stack_configs_are_loaded_relative_to_the_stack_file() {
        let directory = tempfile::tempdir().unwrap();
        let stack_file = directory.path().join("nested/swarmlite.yaml");
        let config_file = directory.path().join("nested/config/app.yaml");
        tokio::fs::create_dir_all(config_file.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&config_file, b"environment: production\n")
            .await
            .unwrap();
        let request = local_stack_apply_request(
            &stack_file,
            "services: {}\n".into(),
            &BTreeMap::from([(
                "app-config".into(),
                swarmlite_stack::StackConfigSource {
                    file: "./config/app.yaml".into(),
                },
            )]),
        )
        .await
        .unwrap();

        assert_eq!(request.yaml, "services: {}\n");
        assert_eq!(
            super::BASE64_STANDARD
                .decode(request.configs["app-config"].data_base64.as_ref().unwrap(),)
                .unwrap(),
            b"environment: production\n"
        );
        assert_eq!(
            request.configs["app-config"].digest,
            swarmlite::model::config_digest(b"environment: production\n")
        );
    }

    #[test]
    fn repeated_config_digests_upload_only_when_missing_and_only_once() {
        let missing_digest = "a".repeat(64);
        let known_digest = "b".repeat(64);
        let payload = |digest: String| swarmlite::model::StackConfigPayload {
            digest,
            data_base64: Some("Y29uZmln".into()),
        };
        let mut request = swarmlite::model::StackApplyRequest {
            yaml: "services: {}\n".into(),
            configs: BTreeMap::from([
                ("first".into(), payload(missing_digest.clone())),
                ("known".into(), payload(known_digest)),
                ("second".into(), payload(missing_digest.clone())),
            ]),
        };

        retain_missing_config_contents(&mut request, &BTreeSet::from([missing_digest.clone()]));

        assert!(request.configs["first"].data_base64.is_some());
        assert!(request.configs["known"].data_base64.is_none());
        assert!(request.configs["second"].data_base64.is_none());
    }

    #[test]
    fn init_uses_the_swarmlite_controller_port_by_default() {
        let cli = Cli::try_parse_from(["swarmlite", "init"]).unwrap();
        let Command::Init { options } = cli.command else {
            panic!("expected init command");
        };
        assert_eq!(options.controller_port, DEFAULT_CONTROLLER_PORT);
    }

    #[test]
    fn exposes_flat_cluster_commands() {
        assert!(Cli::try_parse_from(["swarmlite", "ls"]).is_ok());
        assert!(Cli::try_parse_from(["swarmlite", "ls", "demo"]).is_ok());
        assert!(Cli::try_parse_from(["swarmlite", "ps"]).is_ok());
        assert!(Cli::try_parse_from(["swarmlite", "ps", "demo"]).is_ok());
        assert!(Cli::try_parse_from(["swarmlite", "ps", "demo.web"]).is_ok());
        assert!(Cli::try_parse_from(["swarmlite", "inspect", "demo.web"]).is_ok());
        assert!(Cli::try_parse_from(["swarmlite", "logs", "--tail", "20", "demo.web",]).is_ok());
        assert!(Cli::try_parse_from(["swarmlite", "logs", "--follow", "demo.web",]).is_ok());
        assert!(Cli::try_parse_from(["swarmlite", "logs", "--raw", "task-id",]).is_ok());
        assert!(Cli::try_parse_from(["swarmlite", "scale", "--detach", "demo.web=3",]).is_ok());
        assert!(Cli::try_parse_from(["swarmlite", "restart", "demo.web",]).is_ok());
        assert!(Cli::try_parse_from(["swarmlite", "rm", "demo", "other",]).is_ok());
        assert!(Cli::try_parse_from(["swarmlite", "rm", "--json", "demo",]).is_ok());
        assert!(Cli::try_parse_from(["swarmlite", "deployment", "status", "demo"]).is_ok());
        assert!(
            Cli::try_parse_from([
                "swarmlite",
                "deployment",
                "attach",
                "demo",
                "--generation",
                "42",
            ])
            .is_ok()
        );
        assert!(Cli::try_parse_from(["swarmlite", "deployment", "history", "demo"]).is_ok());
        assert!(Cli::try_parse_from(["swarmlite", "deployment", "retry", "demo"]).is_ok());
        assert!(
            Cli::try_parse_from([
                "swarmlite",
                "deployment",
                "rollback",
                "demo",
                "--to-generation",
                "40",
            ])
            .is_ok()
        );
        assert!(Cli::try_parse_from(["swarmlite", "stack", "ls"]).is_err());
        assert!(Cli::try_parse_from(["swarmlite", "service", "ls"]).is_err());
    }

    #[test]
    fn exposes_only_the_unified_node_runtime() {
        let command = Cli::command();
        let names = command
            .get_subcommands()
            .map(|subcommand| subcommand.get_name())
            .collect::<Vec<_>>();
        assert!(names.contains(&"init"));
        assert!(names.contains(&"join"));
        assert!(names.contains(&"serve"));
        assert!(names.contains(&"connection-info"));
        assert!(names.contains(&"upgrade"));
        assert!(names.contains(&"config"));
        assert!(names.contains(&"gateway"));
        assert!(names.contains(&"node"));
        assert!(names.contains(&"registry"));
        for name in [
            "deploy",
            "deployment",
            "ls",
            "ps",
            "inspect",
            "logs",
            "scale",
            "restart",
            "rm",
        ] {
            assert!(names.contains(&name));
        }
        assert!(!names.contains(&"stack"));
        assert!(!names.contains(&"service"));
        assert!(!names.contains(&"role"));
        assert!(!names.contains(&"controller"));
        assert!(!names.contains(&"agent"));
    }

    #[test]
    fn connection_info_supports_machine_readable_output() {
        assert!(Cli::try_parse_from(["swarmlite", "connection-info"]).is_ok());
        assert!(Cli::try_parse_from(["swarmlite", "connection-info", "--json"]).is_ok());
    }

    #[test]
    fn status_defaults_to_human_output_and_supports_json() {
        let Command::Status { json, .. } = Cli::try_parse_from(["swarmlite", "status"])
            .unwrap()
            .command
        else {
            panic!("expected status command");
        };
        assert!(!json);

        let Command::Status { json, .. } = Cli::try_parse_from(["swarmlite", "status", "--json"])
            .unwrap()
            .command
        else {
            panic!("expected status command");
        };
        assert!(json);
    }

    #[test]
    fn human_status_highlights_cluster_health_and_issues() {
        let mut state = ClusterState::default();
        state.members.insert(
            "node-a".into(),
            NodeMember {
                id: "node-a".into(),
                address: "10.0.0.1".into(),
                gateway_enabled: true,
                labels: BTreeMap::from([("region".into(), "east".into())]),
                joined_at_unix_ms: 1,
            },
        );
        state.members.insert(
            "node-b".into(),
            NodeMember {
                id: "node-b".into(),
                address: "10.0.0.2".into(),
                gateway_enabled: false,
                labels: BTreeMap::new(),
                joined_at_unix_ms: 2,
            },
        );
        state.tasks.insert(
            "1234567890abcdef".into(),
            TaskRecord {
                id: "1234567890abcdef".into(),
                service_id: "demo.web".into(),
                revision: 1,
                slot: 0,
                node_id: "node-a".into(),
                desired: DesiredTaskState::Running,
                observed: ObservedTaskState::Failed,
                ports: Vec::new(),
                config_digests: Vec::new(),
                container_id: None,
                drain_until_unix_ms: None,
                applied_generation: None,
                reconcile_error: Some(TaskReconcileError {
                    phase: TaskReconcilePhase::Start,
                    message: "container exited".into(),
                }),
            },
        );
        let response = StatusResponse {
            cluster_id: "cluster-1".into(),
            generation: 7,
            controller_id: "node-a".into(),
            gateway: GatewayStatus {
                enabled: true,
                desired_generation: 3,
                applied_generation: None,
                endpoint_errors: BTreeMap::from([(
                    "node-a".into(),
                    "failed to load configuration".into(),
                )]),
            },
            recovery: RecoveryStatus {
                awaiting_adoption: 1,
                conflicting_slots: 0,
            },
            state,
        };

        let output = format_status(&response, false);
        assert!(!output.contains("\x1b["));
        assert!(output.contains("Nodes:           2"));
        assert!(output.contains("Tasks:           1 (1 failed)"));
        assert!(output.contains("Status:     degraded"));
        assert!(output.contains("Status:            needs attention"));
        assert!(output.contains("node-a  10.0.0.1  enabled"));
        assert!(output.contains("node-b  10.0.0.2  disabled"));
        assert!(output.contains("region=east"));
        assert!(output.contains("gateway  node-a"));
        assert!(output.contains("task     1234567890ab"));
        assert!(output.contains("start on node-a: container exited"));

        let colored = format_status(&response, true);
        assert!(colored.contains("\x1b[1;36mCluster\x1b[0m"));
        assert!(colored.contains("\x1b[31mdegraded\x1b[0m"));
        assert!(colored.contains("\x1b[31mneeds attention\x1b[0m"));
        assert_eq!(display_width("\x1b[32menabled\x1b[0m"), 7);
    }

    #[test]
    fn upgrade_defaults_to_latest_and_accepts_a_pinned_version() {
        let cli = Cli::try_parse_from(["swarmlite", "upgrade"]).unwrap();
        let Command::Upgrade { version } = cli.command else {
            panic!("expected upgrade command");
        };
        assert_eq!(version, "latest");

        let cli = Cli::try_parse_from(["swarmlite", "upgrade", "--version", "v0.2.0"]).unwrap();
        let Command::Upgrade { version } = cli.command else {
            panic!("expected upgrade command");
        };
        assert_eq!(version, "v0.2.0");
        assert!(Cli::try_parse_from(["swarmlite", "upgrade", "--version", "../latest"]).is_err());
    }

    #[test]
    fn registry_login_requires_username_and_password_stdin() {
        assert!(
            Cli::try_parse_from([
                "swarmlite",
                "registry",
                "login",
                "ghcr.io",
                "--username",
                "octocat",
                "--password-stdin",
            ])
            .is_ok()
        );
        assert!(
            Cli::try_parse_from([
                "swarmlite",
                "registry",
                "login",
                "ghcr.io",
                "--username",
                "octocat",
            ])
            .is_err()
        );
        assert!(
            Cli::try_parse_from([
                "swarmlite",
                "registry",
                "login",
                "ghcr.io",
                "--password-stdin",
            ])
            .is_err()
        );
    }

    #[test]
    fn config_exposes_only_get_and_set() {
        let command = Cli::command();
        let config = command
            .get_subcommands()
            .find(|subcommand| subcommand.get_name() == "config")
            .unwrap();
        let names = config
            .get_subcommands()
            .map(|subcommand| subcommand.get_name())
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["get", "set"]);
        let set = config
            .get_subcommands()
            .find(|subcommand| subcommand.get_name() == "set")
            .unwrap();
        let arguments = set
            .get_arguments()
            .map(|argument| argument.get_id().as_str())
            .collect::<Vec<_>>();
        assert!(arguments.contains(&"key"));
        assert!(arguments.contains(&"value"));
        assert!(!arguments.contains(&"controllers"));
    }

    #[test]
    fn config_set_accepts_key_value_arguments() {
        assert!(Cli::try_parse_from(["swarmlite", "config", "set", "mode", "ha"]).is_err());
        assert!(
            Cli::try_parse_from([
                "swarmlite",
                "config",
                "set",
                "gateway-image",
                "ghcr.io/example/caddy:v1",
            ])
            .is_ok()
        );
        assert!(
            Cli::try_parse_from([
                "swarmlite",
                "config",
                "set",
                "deployment-progress-deadline-seconds",
                "600",
            ])
            .is_ok()
        );
        assert!(
            Cli::try_parse_from(["swarmlite", "config", "set", "image-pull-max-attempts", "5",])
                .is_ok()
        );
        assert!(Cli::try_parse_from(["swarmlite", "config", "set", "unknown", "3"]).is_err());
    }

    #[test]
    fn init_has_no_cluster_mode() {
        let command = Cli::command();
        let init = command
            .get_subcommands()
            .find(|subcommand| subcommand.get_name() == "init")
            .unwrap();
        assert_eq!(init.get_subcommands().count(), 0);
        let arguments = init
            .get_arguments()
            .map(|argument| argument.get_id().as_str())
            .collect::<Vec<_>>();
        assert!(!arguments.contains(&"mode"));
        assert!(arguments.contains(&"recover"));
        assert!(arguments.contains(&"gateway_image"));
        assert!(arguments.contains(&"no_gateway"));
        assert!(!arguments.iter().any(|argument| argument.contains("s3")));
    }

    #[test]
    fn gateway_supports_status_enable_and_disable() {
        for action in ["status", "enable", "disable"] {
            assert!(Cli::try_parse_from(["swarmlite", "gateway", action, "node-a"]).is_ok());
        }
        assert!(
            Cli::try_parse_from([
                "swarmlite",
                "join",
                "http://127.0.0.1:17080",
                "--token",
                "0123456789abcdef",
                "--gateway",
            ])
            .is_ok()
        );
        assert!(
            Cli::try_parse_from([
                "swarmlite",
                "join",
                "http://127.0.0.1:17080",
                "--token",
                "0123456789abcdef",
                "--roles",
                "gateway",
            ])
            .is_err()
        );
    }

    #[test]
    fn node_labels_support_get_set_and_remove() {
        assert!(Cli::try_parse_from(["swarmlite", "node", "label", "get", "node-a"]).is_ok());
        assert!(
            Cli::try_parse_from([
                "swarmlite",
                "node",
                "label",
                "set",
                "node-a",
                "region",
                "cn-east",
            ])
            .is_ok()
        );
        assert!(
            Cli::try_parse_from(["swarmlite", "node", "label", "remove", "node-a", "region",])
                .is_ok()
        );
    }

    #[test]
    fn labels_are_initial_only_on_init_and_join() {
        assert!(Cli::try_parse_from(["swarmlite", "init", "--label", "region=cn-east"]).is_ok());
        assert!(
            Cli::try_parse_from([
                "swarmlite",
                "join",
                "http://127.0.0.1:17080",
                "--token",
                "0123456789abcdef",
                "--label",
                "region=cn-east",
            ])
            .is_ok()
        );
        assert!(Cli::try_parse_from(["swarmlite", "serve", "--label", "region=cn-east"]).is_err());
    }

    #[test]
    fn init_exposes_one_recovery_switch() {
        assert!(Cli::try_parse_from(["swarmlite", "init", "--recover"]).is_ok());
        assert!(Cli::try_parse_from(["swarmlite", "init", "--cluster-id", "old"]).is_err());
    }
}
