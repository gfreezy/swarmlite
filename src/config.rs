use std::{
    collections::BTreeMap,
    fmt, fs,
    io::ErrorKind,
    net::SocketAddr,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use clap::ValueEnum;
use serde::{Deserialize, Serialize};

use crate::model::ClusterSettings;

pub const DEFAULT_GATEWAY_DRAIN_TIMEOUT_SECONDS: u64 = 10;
pub const DEFAULT_DEPLOYMENT_TIMEOUT_SECONDS: u64 = 300;
pub const SYSTEM_CONFIG_PATH: &str = "/etc/swarmlite/runtime.env";

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InstalledNodeConfig {
    pub data_dir: Option<PathBuf>,
    pub runtime: Option<RuntimeKind>,
    pub runtime_socket: Option<String>,
}

impl InstalledNodeConfig {
    pub fn load_if_exists(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let contents = match fs::read_to_string(path) {
            Ok(contents) => contents,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Self::default()),
            Err(error) => {
                return Err(error).with_context(|| format!("failed to read {}", path.display()));
            }
        };
        Self::parse(&contents).with_context(|| format!("invalid {}", path.display()))
    }

    fn parse(contents: &str) -> Result<Self> {
        let mut config = Self::default();
        for (index, raw_line) in contents.lines().enumerate() {
            let line = raw_line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (key, value) = line
                .split_once('=')
                .with_context(|| format!("line {} is not KEY=VALUE", index + 1))?;
            let key = key.trim();
            let value = value.trim();
            match key {
                "SWARMLITE_DATA_DIR" => {
                    if value.is_empty() {
                        bail!("SWARMLITE_DATA_DIR must not be empty");
                    }
                    config.data_dir = Some(PathBuf::from(value));
                }
                "SWARMLITE_RUNTIME" => {
                    config.runtime = Some(match value {
                        "docker" => RuntimeKind::Docker,
                        "podman" => RuntimeKind::Podman,
                        _ => bail!("SWARMLITE_RUNTIME must be docker or podman"),
                    });
                }
                "SWARMLITE_RUNTIME_SOCKET" => {
                    if value.is_empty() {
                        bail!("SWARMLITE_RUNTIME_SOCKET must not be empty");
                    }
                    config.runtime_socket = Some(value.to_owned());
                }
                _ => {}
            }
        }
        Ok(config)
    }

    pub fn runtime_options(
        &self,
        runtime: Option<RuntimeKind>,
        runtime_socket: Option<String>,
    ) -> (Option<RuntimeKind>, Option<String>) {
        if runtime.is_some() || runtime_socket.is_some() {
            return (runtime, runtime_socket);
        }
        (self.runtime, self.runtime_socket.clone())
    }
}

#[derive(Debug, Clone)]
pub struct ControllerConfig {
    pub gateway_enabled: bool,
    pub labels: BTreeMap<String, String>,
    pub listen: SocketAddr,
    pub advertise_url: String,
    pub node_timeout_seconds: u64,
    pub reconcile_interval_seconds: u64,
    pub gateway_drain_timeout_seconds: u64,
    pub deployment_timeout_seconds: u64,
    pub cluster: ClusterSettings,
}

#[derive(Debug, Clone)]
pub struct AgentConfig {
    pub cluster_id: String,
    pub node_id: String,
    pub advertise_address: String,
    pub controller: String,
    pub labels: BTreeMap<String, String>,
    pub heartbeat_interval_seconds: u64,
    pub port_range: PortRangeConfig,
    pub gateway_enabled: bool,
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

const fn default_port_start() -> u16 {
    20_000
}
const fn default_port_end() -> u16 {
    29_999
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn installed_node_config_reads_installer_environment() {
        let config = InstalledNodeConfig::parse(
            r#"
                # Managed by the Swarmlite installer.
                SWARMLITE_DATA_DIR=/var/lib/swarmlite
                SWARMLITE_RUNTIME=podman
                SWARMLITE_RUNTIME_SOCKET=/run/podman/podman.sock
            "#,
        )
        .unwrap();

        assert_eq!(config.data_dir, Some(PathBuf::from("/var/lib/swarmlite")));
        assert_eq!(config.runtime, Some(RuntimeKind::Podman));
        assert_eq!(
            config.runtime_socket.as_deref(),
            Some("/run/podman/podman.sock")
        );
    }

    #[test]
    fn explicit_runtime_options_override_installed_pair() {
        let config = InstalledNodeConfig {
            runtime: Some(RuntimeKind::Podman),
            runtime_socket: Some("/run/podman/podman.sock".into()),
            ..Default::default()
        };

        assert_eq!(
            config.runtime_options(None, None),
            (
                Some(RuntimeKind::Podman),
                Some("/run/podman/podman.sock".into())
            )
        );
        assert_eq!(
            config.runtime_options(Some(RuntimeKind::Docker), None),
            (Some(RuntimeKind::Docker), None)
        );
    }

    #[test]
    fn installed_node_config_rejects_invalid_managed_values() {
        assert!(InstalledNodeConfig::parse("SWARMLITE_RUNTIME=containerd").is_err());
        assert!(InstalledNodeConfig::parse("SWARMLITE_DATA_DIR=").is_err());
        assert!(InstalledNodeConfig::parse("not-an-assignment").is_err());
    }

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
