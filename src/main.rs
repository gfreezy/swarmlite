use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use swarmlite::{agent, config, controller};

#[derive(Debug, Parser)]
#[command(name = "swarmlite", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run a controller candidate. Exactly one candidate is active per cluster.
    Controller {
        #[arg(short, long)]
        config: PathBuf,
    },
    /// Run the node agent and reconcile containers through the local Docker socket.
    Agent {
        #[arg(short, long)]
        config: PathBuf,
    },
    /// Deploy or update a Swarm-style stack file.
    Deploy {
        #[arg(short = 'u', long)]
        controller: String,
        #[arg(short = 'n', long)]
        name: String,
        #[arg(short = 'c', long)]
        file: PathBuf,
        #[arg(long, env = "SWARMLITE_TOKEN")]
        token: String,
    },
    /// Display the current cluster state.
    Status {
        #[arg(short = 'u', long)]
        controller: String,
        #[arg(long, env = "SWARMLITE_TOKEN")]
        token: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "swarmlite=info,tower_http=info".into()),
        )
        .init();

    match Cli::parse().command {
        Command::Controller { config: path } => {
            controller::run(config::load_controller(&path)?).await
        }
        Command::Agent { config: path } => agent::run(config::load_agent(&path)?).await,
        Command::Deploy {
            controller,
            name,
            file,
            token,
        } => deploy(controller, name, file, token).await,
        Command::Status { controller, token } => status(controller, token).await,
    }
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
    use clap::CommandFactory;

    use super::Cli;

    #[test]
    fn cli_definition_is_valid() {
        Cli::command().debug_assert();
    }
}
