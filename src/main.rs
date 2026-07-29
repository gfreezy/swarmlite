use std::path::PathBuf;

use anyhow::Result;
use clap::{Args, Parser, Subcommand, ValueEnum};
use swarmlite::{
    config::RuntimeKind,
    model::{
        CLUSTER_SCHEMA_VERSION, ClusterConfigResponse, ClusterConfigUpdate, ClusterGatewayConfig,
        ClusterMode, ClusterSettings, DEFAULT_GATEWAY_IMAGE, NodeRole, NodeRolesResponse,
        NodeRolesUpdate,
    },
    node,
};

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
    /// Initialize a new Raft cluster on this node.
    Init {
        #[command(flatten)]
        options: InitArgs,
    },
    /// Run this node according to the role assigned by init or join.
    Serve {
        /// Address other machines use to reach containers on this node.
        #[arg(long, env = "SWARMLITE_ADVERTISE_ADDRESS")]
        advertise_address: Option<String>,
        #[arg(long, value_enum)]
        runtime: Option<RuntimeKind>,
        #[arg(long)]
        runtime_socket: Option<String>,
        /// Persistent node label in KEY=VALUE form.
        #[arg(long = "label")]
        labels: Vec<String>,
    },
    /// Pull cluster settings and configure this machine to join an existing cluster.
    Join {
        controller: String,
        #[arg(long, env = "SWARMLITE_TOKEN")]
        token: String,
        #[arg(long, env = "SWARMLITE_ADVERTISE_ADDRESS")]
        advertise_address: Option<String>,
        #[arg(long, value_enum)]
        runtime: Option<RuntimeKind>,
        #[arg(long)]
        runtime_socket: Option<String>,
        #[arg(long = "label")]
        labels: Vec<String>,
        /// Exact requested roles. Agent is always included. Omit for automatic assignment.
        #[arg(long, value_enum, value_delimiter = ',')]
        roles: Option<Vec<NodeRole>>,
    },
    /// Print a join command for this node's cluster.
    JoinToken,
    /// Read or update cluster-wide configuration.
    Config {
        #[command(subcommand)]
        action: ConfigCommand,
    },
    /// Read or change the roles assigned to a joined node.
    Role {
        #[command(subcommand)]
        action: RoleCommand,
    },
    /// Deploy or update a Swarm-style stack file.
    Deploy {
        #[arg(short = 'u', long)]
        controller: Option<String>,
        #[arg(short = 'n', long)]
        name: String,
        #[arg(short = 'c', long)]
        file: PathBuf,
        #[arg(long, env = "SWARMLITE_TOKEN")]
        token: Option<String>,
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
enum RoleCommand {
    /// Print a node's assigned roles.
    Get {
        node_id: String,
        #[command(flatten)]
        connection: ConnectionArgs,
    },
    /// Replace a node's assigned roles. Agent is always included.
    Set {
        node_id: String,
        #[arg(value_enum, value_delimiter = ',', required = true)]
        roles: Vec<NodeRole>,
        #[command(flatten)]
        connection: ConnectionArgs,
    },
    /// Add roles to a node.
    Add {
        node_id: String,
        #[arg(value_enum, value_delimiter = ',', required = true)]
        roles: Vec<NodeRole>,
        #[command(flatten)]
        connection: ConnectionArgs,
    },
    /// Remove roles from a node. Agent cannot be removed.
    Remove {
        node_id: String,
        #[arg(value_enum, value_delimiter = ',', required = true)]
        roles: Vec<NodeRole>,
        #[command(flatten)]
        connection: ConnectionArgs,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ConfigKey {
    Mode,
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
struct InitArgs {
    #[arg(long, env = "SWARMLITE_TOKEN")]
    token: Option<String>,
    #[arg(long, default_value_t = 8080)]
    controller_port: u16,
    /// Control-plane mode. HA has three controller slots.
    #[arg(long, value_enum, default_value_t = ClusterMode::Standalone)]
    mode: ClusterMode,
    /// Rebuild the control plane and collect containers from the previous cluster.
    #[arg(long)]
    recover: bool,
    #[arg(long, env = "SWARMLITE_ADVERTISE_ADDRESS")]
    advertise_address: Option<String>,
    #[arg(long, value_enum)]
    runtime: Option<RuntimeKind>,
    #[arg(long)]
    runtime_socket: Option<String>,
    #[arg(long = "label")]
    labels: Vec<String>,
    #[arg(long = "gateway-listen", default_value = ":80")]
    gateway_listen: Vec<String>,
    /// OCI image containing Caddy and caddy.storage.swarmlite.
    /// Defaults to ghcr.io/swarmlite/swarmlite-caddy:latest.
    #[arg(long = "gateway-image")]
    gateway_image: Option<String>,
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
    let data_dir = node::resolve_data_dir(cli.data_dir)?;
    match cli.command {
        Command::Init { options } => {
            let gateway_image_explicit = options.gateway_image.is_some();
            let cluster = ClusterSettings {
                schema_version: CLUSTER_SCHEMA_VERSION,
                cluster_id: node::new_cluster_id(),
                mode: options.mode,
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
                runtime: options.runtime,
                runtime_socket: options.runtime_socket,
                labels: node::parse_labels(options.labels)?,
                recovery: options.recover,
                gateway_image_explicit,
            })
            .await?;
            println!("{message}");
            Ok(())
        }
        Command::Serve {
            advertise_address,
            runtime,
            runtime_socket,
            labels,
        } => {
            node::run(node::ServeOptions {
                data_dir,
                advertise_address,
                runtime,
                runtime_socket,
                labels: node::parse_labels(labels)?,
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
            roles,
        } => {
            let message = node::join(node::JoinOptions {
                data_dir,
                controller,
                token,
                advertise_address,
                runtime,
                runtime_socket,
                labels: node::parse_labels(labels)?,
                requested_roles: roles.map(IntoIterator::into_iter).map(Iterator::collect),
            })
            .await?;
            println!("{message}");
            Ok(())
        }
        Command::JoinToken => {
            println!("{}", node::join_command(&data_dir).await?);
            Ok(())
        }
        Command::Config { action } => {
            let (connection, update) = match action {
                ConfigCommand::Get { connection } => (connection, None),
                ConfigCommand::Set {
                    key,
                    value,
                    connection,
                } => {
                    let update = match key {
                        ConfigKey::Mode => ClusterConfigUpdate {
                            mode: Some(
                                <ClusterMode as ValueEnum>::from_str(&value, true)
                                    .map_err(anyhow::Error::msg)?,
                            ),
                            gateway_image: None,
                        },
                        ConfigKey::GatewayImage => ClusterConfigUpdate {
                            mode: None,
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
        Command::Role { action } => {
            let (node_id, roles, method, connection) = match action {
                RoleCommand::Get {
                    node_id,
                    connection,
                } => (node_id, None, reqwest::Method::GET, connection),
                RoleCommand::Set {
                    node_id,
                    roles,
                    connection,
                } => (node_id, Some(roles), reqwest::Method::PUT, connection),
                RoleCommand::Add {
                    node_id,
                    roles,
                    connection,
                } => (node_id, Some(roles), reqwest::Method::PATCH, connection),
                RoleCommand::Remove {
                    node_id,
                    roles,
                    connection,
                } => (node_id, Some(roles), reqwest::Method::DELETE, connection),
            };
            let (controller, token) =
                node::resolve_connection(&data_dir, connection.controller, connection.token)
                    .await?;
            let response = node_roles(controller, token, &node_id, method, roles).await?;
            println!("{}", serde_json::to_string_pretty(&response)?);
            Ok(())
        }
        Command::Deploy {
            controller,
            name,
            file,
            token,
        } => {
            let (controller, token) =
                node::resolve_connection(&data_dir, controller, token).await?;
            deploy(controller, name, file, token).await
        }
        Command::Status { controller, token } => {
            let (controller, token) =
                node::resolve_connection(&data_dir, controller, token).await?;
            status(controller, token).await
        }
    }
}

async fn node_roles(
    controller: String,
    token: String,
    node_id: &str,
    method: reqwest::Method,
    roles: Option<Vec<NodeRole>>,
) -> Result<NodeRolesResponse> {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let mut url = format!(
        "{}/v1/nodes/{node_id}/roles",
        controller.trim_end_matches('/')
    );
    let update = roles.map(|roles| NodeRolesUpdate {
        roles: roles.into_iter().collect(),
    });
    for _ in 0..3 {
        let mut request = client.request(method.clone(), &url).bearer_auth(&token);
        if let Some(update) = &update {
            request = request.json(update);
        }
        let response = request.send().await?;
        if response.status() == reqwest::StatusCode::TEMPORARY_REDIRECT {
            url = response
                .headers()
                .get(reqwest::header::LOCATION)
                .and_then(|value| value.to_str().ok())
                .ok_or_else(|| anyhow::anyhow!("controller redirect omitted Location"))?
                .to_owned();
            continue;
        }
        let status = response.status();
        let body = response.text().await?;
        if !status.is_success() {
            anyhow::bail!("controller returned {status}: {body}");
        }
        return serde_json::from_str(&body).map_err(Into::into);
    }
    anyhow::bail!("too many controller redirects")
}

async fn cluster_config(
    controller: String,
    token: String,
    update: Option<&ClusterConfigUpdate>,
) -> Result<ClusterConfigResponse> {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let method = if update.is_some() {
        reqwest::Method::PATCH
    } else {
        reqwest::Method::GET
    };
    let mut url = format!("{}/v1/config", controller.trim_end_matches('/'));
    for _ in 0..3 {
        let mut request = client.request(method.clone(), &url).bearer_auth(&token);
        if let Some(update) = update {
            request = request.json(update);
        }
        let response = request.send().await?;
        if response.status() == reqwest::StatusCode::TEMPORARY_REDIRECT {
            url = response
                .headers()
                .get(reqwest::header::LOCATION)
                .and_then(|value| value.to_str().ok())
                .ok_or_else(|| anyhow::anyhow!("controller redirect omitted Location"))?
                .to_owned();
            continue;
        }
        let status = response.status();
        let body = response.text().await?;
        if !status.is_success() {
            anyhow::bail!("controller returned {status}: {body}");
        }
        return serde_json::from_str(&body).map_err(Into::into);
    }
    anyhow::bail!("too many controller redirects")
}

async fn deploy(controller: String, name: String, file: PathBuf, token: String) -> Result<()> {
    let stack = tokio::fs::read_to_string(file).await?;
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let mut url = format!("{}/v1/stacks/{}", controller.trim_end_matches('/'), name);
    let mut response = None;
    for _ in 0..3 {
        let candidate = client
            .put(&url)
            .bearer_auth(&token)
            .header(reqwest::header::CONTENT_TYPE, "application/x-yaml")
            .body(stack.clone())
            .send()
            .await?;
        if candidate.status() != reqwest::StatusCode::TEMPORARY_REDIRECT {
            response = Some(candidate);
            break;
        }
        url = candidate
            .headers()
            .get(reqwest::header::LOCATION)
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| anyhow::anyhow!("controller redirect omitted Location"))?
            .to_owned();
    }
    let response = response.ok_or_else(|| anyhow::anyhow!("too many controller redirects"))?;
    let status = response.status();
    let body = response.text().await?;
    if !status.is_success() {
        anyhow::bail!("controller returned {status}: {body}");
    }
    println!("{body}");
    Ok(())
}

async fn status(controller: String, token: String) -> Result<()> {
    let response = reqwest::Client::new()
        .get(format!("{}/v1/status", controller.trim_end_matches('/')))
        .bearer_auth(token)
        .send()
        .await?;
    let status = response.status();
    let body = response.text().await?;
    if !status.is_success() {
        anyhow::bail!("controller returned {status}: {body}");
    }
    let value: serde_json::Value = serde_json::from_str(&body)?;
    println!("{}", serde_json::to_string_pretty(&value)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use clap::{CommandFactory, Parser};

    use super::Cli;

    #[test]
    fn cli_definition_is_valid() {
        Cli::command().debug_assert();
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
        assert!(names.contains(&"config"));
        assert!(names.contains(&"role"));
        assert!(!names.contains(&"controller"));
        assert!(!names.contains(&"agent"));
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
        assert!(Cli::try_parse_from(["swarmlite", "config", "set", "mode", "ha"]).is_ok());
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
    fn init_uses_a_cluster_mode() {
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
        assert!(arguments.contains(&"mode"));
        assert!(arguments.contains(&"recover"));
        assert!(arguments.contains(&"gateway_image"));
        assert!(!arguments.iter().any(|argument| argument.contains("s3")));
    }

    #[test]
    fn role_supports_get_set_add_and_remove() {
        for action in ["get", "set", "add", "remove"] {
            let mut args = vec!["swarmlite", "role", action, "node-a"];
            if action != "get" {
                args.push("gateway");
            }
            assert!(Cli::try_parse_from(args).is_ok());
        }
        assert!(
            Cli::try_parse_from([
                "swarmlite",
                "join",
                "http://127.0.0.1:8080",
                "--token",
                "0123456789abcdef",
                "--roles",
                "controller,gateway",
            ])
            .is_ok()
        );
    }

    #[test]
    fn init_exposes_one_recovery_switch() {
        assert!(Cli::try_parse_from(["swarmlite", "init", "--recover"]).is_ok());
        assert!(Cli::try_parse_from(["swarmlite", "init", "--cluster-id", "old"]).is_err());
    }
}
