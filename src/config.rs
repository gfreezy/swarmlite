use std::{collections::BTreeMap, fmt, net::SocketAddr};

use anyhow::{Result, bail};
use clap::ValueEnum;
use serde::{Deserialize, Serialize};

use crate::model::ClusterSettings;

#[derive(Debug, Clone)]
pub struct ControllerConfig {
    pub controller_id: String,
    pub listen: SocketAddr,
    pub advertise_url: String,
    pub node_timeout_seconds: u64,
    pub reconcile_interval_seconds: u64,
    pub caddy: CaddyConfig,
    pub cluster: ClusterSettings,
}

#[derive(Debug, Clone)]
pub struct CaddyConfig {
    pub admin_endpoints: Vec<String>,
    pub server_name: String,
    pub listen: Vec<String>,
    pub request_timeout_seconds: u64,
    pub resync_interval_seconds: u64,
    pub retry_interval_seconds: u64,
    pub drain_timeout_seconds: u64,
}

impl Default for CaddyConfig {
    fn default() -> Self {
        Self {
            admin_endpoints: Vec::new(),
            server_name: default_caddy_server_name(),
            listen: default_caddy_listen(),
            request_timeout_seconds: default_caddy_request_timeout(),
            resync_interval_seconds: default_caddy_resync_interval(),
            retry_interval_seconds: default_caddy_retry_interval(),
            drain_timeout_seconds: default_caddy_drain_timeout(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AgentConfig {
    pub cluster_id: String,
    pub node_id: String,
    pub advertise_address: String,
    pub controllers: Vec<String>,
    pub runtime: Option<RuntimeConfig>,
    pub labels: BTreeMap<String, String>,
    pub heartbeat_interval_seconds: u64,
    pub port_range: PortRangeConfig,
    pub controller_capable: bool,
    pub controller_url: Option<String>,
    pub raft_id: Option<u64>,
    pub raft_url: Option<String>,
}

impl AgentConfig {
    pub fn resolved_runtime(&self) -> Result<ResolvedRuntimeConfig> {
        match &self.runtime {
            Some(runtime) => runtime.resolve(),
            None => resolve_runtime(RuntimeKind::Docker, None),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeConfig {
    #[serde(rename = "type")]
    pub kind: RuntimeKind,
    pub socket: Option<String>,
}

impl RuntimeConfig {
    pub(crate) fn resolve(&self) -> Result<ResolvedRuntimeConfig> {
        resolve_runtime(self.kind, self.socket.as_deref())
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum RuntimeKind {
    #[default]
    Docker,
    Podman,
}

impl RuntimeKind {
    fn default_socket(self) -> &'static str {
        match self {
            Self::Docker => "/var/run/docker.sock",
            Self::Podman => "/run/podman/podman.sock",
        }
    }
}

impl fmt::Display for RuntimeKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Docker => formatter.write_str("Docker"),
            Self::Podman => formatter.write_str("Podman"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedRuntimeConfig {
    pub kind: RuntimeKind,
    pub socket: String,
}

fn resolve_runtime(kind: RuntimeKind, socket: Option<&str>) -> Result<ResolvedRuntimeConfig> {
    let socket = socket.unwrap_or_else(|| kind.default_socket());
    if socket.trim().is_empty() {
        bail!("runtime.socket must not be empty");
    }
    Ok(ResolvedRuntimeConfig {
        kind,
        socket: socket.to_owned(),
    })
}

#[derive(Debug, Clone, Copy)]
pub struct PortRangeConfig {
    pub start: u16,
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

fn default_caddy_server_name() -> String {
    "swarmlite".to_owned()
}
fn default_caddy_listen() -> Vec<String> {
    vec![":80".to_owned()]
}
const fn default_caddy_request_timeout() -> u64 {
    5
}
const fn default_caddy_resync_interval() -> u64 {
    30
}
const fn default_caddy_retry_interval() -> u64 {
    2
}
const fn default_caddy_drain_timeout() -> u64 {
    10
}
const fn default_port_start() -> u16 {
    20_000
}
const fn default_port_end() -> u16 {
    29_999
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_docker_and_podman_runtime_sockets() {
        assert_eq!(
            resolve_runtime(RuntimeKind::Docker, None).unwrap(),
            ResolvedRuntimeConfig {
                kind: RuntimeKind::Docker,
                socket: "/var/run/docker.sock".into(),
            }
        );
        assert_eq!(
            resolve_runtime(RuntimeKind::Podman, None).unwrap(),
            ResolvedRuntimeConfig {
                kind: RuntimeKind::Podman,
                socket: "/run/podman/podman.sock".into(),
            }
        );
        assert_eq!(
            resolve_runtime(RuntimeKind::Docker, Some("/custom/docker.sock"))
                .unwrap()
                .socket,
            "/custom/docker.sock"
        );
    }
}
