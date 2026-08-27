use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand, ValueEnum};
use swarmlite::{
    client::ControllerClient,
    config::{DEFAULT_CONTROLLER_PORT, InstalledNodeConfig, RuntimeKind, SYSTEM_CONFIG_PATH},
    model::{
        CLUSTER_SCHEMA_VERSION, ClusterConfigResponse, ClusterConfigUpdate, ClusterGatewayConfig,
        ClusterSettings, DEFAULT_GATEWAY_IMAGE, NodeGatewayResponse, NodeGatewayUpdate,
        NodeLabelRemoveRequest, NodeLabelSetRequest, NodeLabelsResponse, RegistryLoginRequest,
        RegistryLoginResponse, StackDeploymentResponse, StackDeploymentStatus,
    },
    node, registry,
};
use tokio::io::AsyncReadExt;

mod cluster_cli;
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
    /// List cluster services, optionally limited to one Stack.
    Ls {
        #[command(flatten)]
        options: cluster_cli::ListArgs,
    },
    /// List tasks belonging to a Stack or Service.
    Ps {
        #[command(flatten)]
        options: cluster_cli::PsArgs,
    },
    /// Display detailed information about a Service.
    Inspect {
        #[command(flatten)]
        options: cluster_cli::InspectArgs,
    },
    /// Fetch logs from a Service or Task.
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

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ConfigKey {
    GatewayImage,
}

#[derive(Debug, Args)]
struct ConnectionArgs {
    #[arg(short = 'u', long)]
    controller: Option<String>,
    #[arg(long, env = "SWARMLITE_TOKEN")]
    token: Option<String>,
}

#[derive(Debug, Args)]
struct RegistryConnectionArgs {
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
async fn main() -> Result<()> {
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
        Command::Upgrade { .. } => unreachable!("upgrade returned before loading node state"),
        Command::Config { action } => {
            let (connection, update) = match action {
                ConfigCommand::Get { connection } => (connection, None),
                ConfigCommand::Set {
                    key,
                    value,
                    connection,
                } => {
                    let update = match key {
                        ConfigKey::GatewayImage => ClusterConfigUpdate {
                            gateway_image: Some(value),
                        },
                    };
                    (connection, Some(update))
                }
            };
            let (controller, token) =
                node::resolve_connection(&data_dir, connection.controller, connection.token)
                    .await?;
            let response = cluster_config(controller, token, update.as_ref()).await?;
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
            let (controller, token) =
                node::resolve_connection(&data_dir, connection.controller, connection.token)
                    .await?;
            let response = node_gateway(controller, token, &node_id, enabled).await?;
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
                let (controller, token) =
                    node::resolve_connection(&data_dir, connection.controller, connection.token)
                        .await?;
                let response =
                    node_labels(controller, token, &node_id, method, body.as_ref()).await?;
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
                let (controller, token) =
                    node::resolve_connection(&data_dir, connection.controller, connection.token)
                        .await?;
                let response: RegistryLoginResponse = ControllerClient::new(controller, token)
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
        Command::Ls { options } => cluster_cli::run_list(&data_dir, options).await,
        Command::Ps { options } => cluster_cli::run_ps(&data_dir, options).await,
        Command::Inspect { options } => cluster_cli::run_inspect(&data_dir, options).await,
        Command::Logs { options } => cluster_cli::run_logs(&data_dir, options).await,
        Command::Scale { options } => cluster_cli::run_scale(&data_dir, options).await,
        Command::Restart { options } => cluster_cli::run_restart(&data_dir, options).await,
        Command::Rm { options } => cluster_cli::run_remove(&data_dir, options).await,
        Command::Status { controller, token } => {
            let (controller, token) =
                node::resolve_connection(&data_dir, controller, token).await?;
            status(controller, token).await
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

async fn finish_deployment(
    client: &ControllerClient,
    mut deployment: StackDeploymentResponse,
    detach: bool,
) -> Result<StackDeploymentResponse> {
    if !detach {
        let stack = deployment.stack.clone();
        deployment = wait_for_deployment(client, &stack, deployment).await?;
        ensure_deployment_succeeded(&deployment)?;
    }
    Ok(deployment)
}

fn ensure_deployment_succeeded(deployment: &StackDeploymentResponse) -> Result<()> {
    if !matches!(
        deployment.status,
        StackDeploymentStatus::Failed | StackDeploymentStatus::TimedOut
    ) {
        return Ok(());
    }
    let details = deployment
        .errors
        .iter()
        .map(|error| {
            format!(
                "{} on {} during {:?}: {}",
                error.service, error.node_id, error.phase, error.message
            )
        })
        .collect::<Vec<_>>()
        .join("; ");
    anyhow::bail!(
        "stack {:?} deployment {}: {}",
        deployment.stack,
        match deployment.status {
            StackDeploymentStatus::Failed => "failed",
            StackDeploymentStatus::TimedOut => "timed out",
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
    controller: String,
    token: String,
    node_id: &str,
    method: reqwest::Method,
    body: Option<&serde_json::Value>,
) -> Result<NodeLabelsResponse> {
    Ok(ControllerClient::new(controller, token)
        .send_json(method, &format!("/v1/nodes/{node_id}/labels"), body)
        .await?)
}

async fn node_gateway(
    controller: String,
    token: String,
    node_id: &str,
    enabled: Option<bool>,
) -> Result<NodeGatewayResponse> {
    let update = enabled.map(|enabled| NodeGatewayUpdate { enabled });
    let method = if update.is_some() {
        reqwest::Method::PUT
    } else {
        reqwest::Method::GET
    };
    Ok(ControllerClient::new(controller, token)
        .send_json(
            method,
            &format!("/v1/nodes/{node_id}/gateway"),
            update.as_ref(),
        )
        .await?)
}

async fn cluster_config(
    controller: String,
    token: String,
    update: Option<&ClusterConfigUpdate>,
) -> Result<ClusterConfigResponse> {
    let method = if update.is_some() {
        reqwest::Method::PATCH
    } else {
        reqwest::Method::GET
    };
    Ok(ControllerClient::new(controller, token)
        .send_json(method, "/v1/config", update)
        .await?)
}

async fn deploy(
    controller: String,
    name: String,
    file: PathBuf,
    token: String,
    detach: bool,
) -> Result<()> {
    let stack = tokio::fs::read_to_string(file).await?;
    let client = ControllerClient::new(controller, token);
    let body = client
        .send_text(
            reqwest::Method::PUT,
            &format!("/v1/stacks/{name}"),
            Some("application/x-yaml"),
            Some(stack),
        )
        .await?;
    let deployment: StackDeploymentResponse = serde_json::from_str(&body)?;
    let deployment = finish_deployment(&client, deployment, detach).await?;
    println!("{}", serde_json::to_string_pretty(&deployment)?);
    Ok(())
}

async fn wait_for_deployment(
    client: &ControllerClient,
    stack_name: &str,
    mut deployment: StackDeploymentResponse,
) -> Result<StackDeploymentResponse> {
    const CLIENT_WAIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(330);
    const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(35);
    let deadline = tokio::time::Instant::now() + CLIENT_WAIT_TIMEOUT;
    let mut last_error: Option<anyhow::Error> = None;
    while deployment.status == StackDeploymentStatus::Deploying {
        if tokio::time::Instant::now() >= deadline {
            return Err(last_error.unwrap_or_else(|| {
                anyhow::anyhow!("timed out waiting for stack {stack_name:?} deployment")
            }));
        }
        let path = format!(
            "/v1/stacks/{stack_name}/deployment?generation={}&after_revision={}&wait_seconds=25",
            deployment.generation, deployment.revision
        );
        match tokio::time::timeout(
            REQUEST_TIMEOUT,
            client.get_json::<StackDeploymentResponse>(&path),
        )
        .await
        {
            Ok(Ok(next)) => {
                deployment = next;
                last_error = None;
            }
            Ok(Err(error)) if error.is_retryable() => {
                last_error = Some(error.into());
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
            Ok(Err(error)) => return Err(error.into()),
            Err(_) => {
                last_error = Some(anyhow::anyhow!("controller deployment watch timed out"));
            }
        }
    }
    Ok(deployment)
}

async fn status(controller: String, token: String) -> Result<()> {
    let value: serde_json::Value = ControllerClient::new(controller, token)
        .get_json("/v1/status")
        .await?;
    println!("{}", serde_json::to_string_pretty(&value)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use clap::{CommandFactory, Parser};
    use swarmlite::config::DEFAULT_CONTROLLER_PORT;

    use super::{Cli, Command};

    #[test]
    fn cli_definition_is_valid() {
        Cli::command().debug_assert();
    }

    #[test]
    fn deploy_waits_by_default_and_supports_detach() {
        let parse = |extra: &[&str]| {
            let mut arguments = vec!["swarmlite", "deploy", "-c", "stack.yaml", "demo"];
            arguments.extend_from_slice(extra);
            Cli::try_parse_from(arguments).unwrap()
        };
        assert!(matches!(parse(&[]).command, Command::Deploy { .. }));
        assert!(matches!(
            parse(&["--detach"]).command,
            Command::Deploy { .. }
        ));
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
        assert!(Cli::try_parse_from(["swarmlite", "ps", "demo"]).is_ok());
        assert!(Cli::try_parse_from(["swarmlite", "ps", "demo.web"]).is_ok());
        assert!(Cli::try_parse_from(["swarmlite", "inspect", "demo.web"]).is_ok());
        assert!(Cli::try_parse_from(["swarmlite", "logs", "--tail", "20", "demo.web",]).is_ok());
        assert!(Cli::try_parse_from(["swarmlite", "logs", "--follow", "demo.web",]).is_ok());
        assert!(Cli::try_parse_from(["swarmlite", "logs", "--raw", "task-id",]).is_ok());
        assert!(Cli::try_parse_from(["swarmlite", "scale", "--detach", "demo.web=3",]).is_ok());
        assert!(Cli::try_parse_from(["swarmlite", "restart", "demo.web",]).is_ok());
        assert!(Cli::try_parse_from(["swarmlite", "rm", "demo", "other",]).is_ok());
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
        assert!(names.contains(&"upgrade"));
        assert!(names.contains(&"config"));
        assert!(names.contains(&"gateway"));
        assert!(names.contains(&"node"));
        assert!(names.contains(&"registry"));
        for name in [
            "deploy", "ls", "ps", "inspect", "logs", "scale", "restart", "rm",
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
