use std::{collections::BTreeMap, net::SocketAddr, path::Path};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControllerConfig {
    pub cluster_id: String,
    pub controller_id: String,
    pub listen: SocketAddr,
    pub advertise_url: String,
    #[serde(default)]
    pub auth_token: Option<String>,
    pub storage: S3Config,
    #[serde(default)]
    pub lease: LeaseConfig,
    #[serde(default = "default_node_timeout")]
    pub node_timeout_seconds: u64,
    #[serde(default = "default_reconcile_interval")]
    pub reconcile_interval_seconds: u64,
}

impl ControllerConfig {
    pub fn token(&self) -> Result<String> {
        resolve_token(self.auth_token.as_deref())
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3Config {
    pub bucket: String,
    #[serde(default)]
    pub prefix: String,
    #[serde(default = "default_region")]
    pub region: String,
    #[serde(default)]
    pub endpoint_url: Option<String>,
    #[serde(default)]
    pub force_path_style: bool,
}

impl S3Config {
    pub fn key(&self, suffix: &str) -> String {
        let prefix = self.prefix.trim_matches('/');
        if prefix.is_empty() {
            suffix.trim_start_matches('/').to_owned()
        } else {
            format!("{prefix}/{}", suffix.trim_start_matches('/'))
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LeaseConfig {
    #[serde(default = "default_lease_duration")]
    pub duration_seconds: u64,
    #[serde(default = "default_renew_interval")]
    pub renew_interval_seconds: u64,
    #[serde(default = "default_clock_skew")]
    pub clock_skew_seconds: u64,
}

impl Default for LeaseConfig {
    fn default() -> Self {
        Self {
            duration_seconds: default_lease_duration(),
            renew_interval_seconds: default_renew_interval(),
            clock_skew_seconds: default_clock_skew(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentConfig {
    pub node_id: String,
    pub advertise_address: String,
    pub controllers: Vec<String>,
    #[serde(default)]
    pub auth_token: Option<String>,
    #[serde(default = "default_docker_socket")]
    pub docker_socket: String,
    #[serde(default = "default_agent_state_file")]
    pub state_file: String,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval_seconds: u64,
    #[serde(default)]
    pub port_range: PortRangeConfig,
}

impl AgentConfig {
    pub fn token(&self) -> Result<String> {
        resolve_token(self.auth_token.as_deref())
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortRangeConfig {
    #[serde(default = "default_port_start")]
    pub start: u16,
    #[serde(default = "default_port_end")]
    pub end: u16,
}

impl Default for PortRangeConfig {
    fn default() -> Self {
        Self {
            start: default_port_start(),
            end: default_port_end(),
        }
    }
}

pub fn load_controller(path: &Path) -> Result<ControllerConfig> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read controller config {}", path.display()))?;
    let config: ControllerConfig = serde_yaml::from_str(&text)
        .with_context(|| format!("invalid controller config {}", path.display()))?;
    validate_controller(&config)?;
    Ok(config)
}

pub fn load_agent(path: &Path) -> Result<AgentConfig> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read agent config {}", path.display()))?;
    let config: AgentConfig = serde_yaml::from_str(&text)
        .with_context(|| format!("invalid agent config {}", path.display()))?;
    validate_agent(&config)?;
    Ok(config)
}

fn validate_controller(config: &ControllerConfig) -> Result<()> {
    if config.cluster_id.trim().is_empty() || config.controller_id.trim().is_empty() {
        bail!("cluster_id and controller_id must not be empty");
    }
    if config.lease.renew_interval_seconds >= config.lease.duration_seconds {
        bail!("lease.renew_interval_seconds must be less than lease.duration_seconds");
    }
    if config.storage.bucket.trim().is_empty() {
        bail!("storage.bucket must not be empty");
    }
    url::Url::parse(&config.advertise_url).context("advertise_url must be an absolute URL")?;
    config.token()?;
    Ok(())
}

fn validate_agent(config: &AgentConfig) -> Result<()> {
    if config.node_id.trim().is_empty() {
        bail!("node_id must not be empty");
    }
    if config.controllers.is_empty() {
        bail!("at least one controller URL is required");
    }
    if config.port_range.start > config.port_range.end {
        bail!("port_range.start must be less than or equal to port_range.end");
    }
    for controller in &config.controllers {
        url::Url::parse(controller)
            .with_context(|| format!("invalid controller URL {controller}"))?;
    }
    config.token()?;
    Ok(())
}

fn resolve_token(configured: Option<&str>) -> Result<String> {
    let token = std::env::var("SWARMLITE_TOKEN")
        .ok()
        .or_else(|| configured.map(ToOwned::to_owned));
    match token {
        Some(value) if !value.trim().is_empty() => Ok(value),
        _ => bail!("set SWARMLITE_TOKEN or auth_token in the config file"),
    }
}

fn default_region() -> String {
    "auto".to_owned()
}
fn default_docker_socket() -> String {
    "/var/run/docker.sock".to_owned()
}
fn default_agent_state_file() -> String {
    "/var/lib/swarmlite/agent-state.json".to_owned()
}
const fn default_lease_duration() -> u64 {
    30
}
const fn default_renew_interval() -> u64 {
    10
}
const fn default_clock_skew() -> u64 {
    3
}
const fn default_node_timeout() -> u64 {
    20
}
const fn default_reconcile_interval() -> u64 {
    2
}
const fn default_heartbeat_interval() -> u64 {
    5
}
const fn default_port_start() -> u16 {
    20_000
}
const fn default_port_end() -> u16 {
    29_999
}
