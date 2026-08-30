use std::{
    net::{Ipv4Addr, SocketAddr, TcpListener},
    ops::Deref,
    path::Path,
    process::Stdio,
    time::Duration,
};

use crate::swarmlite::{
    client::ControllerClient,
    node::{self, ConnectionInfo},
};
use anyhow::{Context, Result, bail};
use tokio::{net::TcpStream, process::Child};
use url::Url;

const REMOTE_CONNECTION_INFO_COMMAND: &str = "if [ \"$(id -u)\" -eq 0 ]; then exec /usr/local/bin/swarmlite connection-info --json; else exec sudo -n /usr/local/bin/swarmlite connection-info --json; fi";
const MAX_CONNECTION_INFO_BYTES: usize = 64 * 1024;
const SSH_TUNNEL_START_TIMEOUT: Duration = Duration::from_secs(10);

/// A Controller client together with any transport process it depends on.
pub(crate) struct ControllerConnection {
    client: ControllerClient,
    _ssh_tunnel: Option<SshTunnel>,
}

impl Deref for ControllerConnection {
    type Target = ControllerClient;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

pub(crate) async fn resolve(
    data_dir: &Path,
    controller: Option<String>,
    token: Option<String>,
) -> Result<ControllerConnection> {
    let Some(ssh_controller) = controller.as_deref().filter(|value| has_ssh_scheme(value)) else {
        let (controller, token) = node::resolve_connection(data_dir, controller, token).await?;
        return Ok(ControllerConnection {
            client: ControllerClient::new(controller, token),
            _ssh_tunnel: None,
        });
    };

    let target = SshTarget::parse(ssh_controller)?;
    let remote_info = target.read_connection_info().await?;
    let remote_controller = RemoteController::parse(&remote_info.controller)?;
    let token = token.unwrap_or(remote_info.token);
    let tunnel = SshTunnel::start(&target, &remote_controller).await?;
    let local_controller = format!("http://{}", tunnel.local_address());

    Ok(ControllerConnection {
        client: ControllerClient::new(local_controller, token),
        _ssh_tunnel: Some(tunnel),
    })
}

fn has_ssh_scheme(value: &str) -> bool {
    value
        .split_once(':')
        .is_some_and(|(scheme, _)| scheme.eq_ignore_ascii_case("ssh"))
}

struct SshTarget {
    host: String,
    user: Option<String>,
    port: Option<u16>,
}

impl SshTarget {
    fn parse(value: &str) -> Result<Self> {
        let parsed = Url::parse(value).context("SSH Controller must be an absolute ssh:// URL")?;
        if parsed.scheme() != "ssh"
            || parsed.host_str().is_none()
            || parsed.password().is_some()
            || !matches!(parsed.path(), "" | "/")
            || parsed.query().is_some()
            || parsed.fragment().is_some()
        {
            bail!(
                "SSH Controller must use ssh://[user@]host[:port] without password, path, query, or fragment"
            );
        }

        let host = normalized_host(&parsed).expect("checked above");
        if host.starts_with('-') {
            bail!("SSH Controller host must not start with '-'");
        }
        let user = (!parsed.username().is_empty()).then(|| parsed.username().to_owned());
        if user.as_deref().is_some_and(|user| {
            !user
                .chars()
                .all(|character| character.is_ascii_alphanumeric() || "-_.".contains(character))
        }) {
            bail!("SSH Controller user contains unsupported characters");
        }
        if parsed.port() == Some(0) {
            bail!("SSH Controller port must be greater than zero");
        }

        Ok(Self {
            host,
            user,
            port: parsed.port(),
        })
    }

    fn command(&self) -> tokio::process::Command {
        let mut command = tokio::process::Command::new("ssh");
        if let Some(port) = self.port {
            command.arg("-p").arg(port.to_string());
        }
        if let Some(user) = &self.user {
            command.arg("-l").arg(user);
        }
        command
    }

    async fn read_connection_info(&self) -> Result<ConnectionInfo> {
        let mut command = self.command();
        let output = command
            .arg("-T")
            .arg(&self.host)
            .arg(REMOTE_CONNECTION_INFO_COMMAND)
            .stdin(Stdio::inherit())
            .output()
            .await
            .with_context(|| format!("failed to execute ssh for {}", self.host))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stderr = stderr.trim();
            let details = if stderr.is_empty() {
                String::new()
            } else {
                format!(": {}", stderr.chars().take(4_096).collect::<String>())
            };
            bail!(
                "failed to read remote Swarmlite connection info over SSH ({}). The remote user must be root or have NOPASSWD access to `swarmlite connection-info --json`{details}",
                output.status
            );
        }
        if output.stdout.len() > MAX_CONNECTION_INFO_BYTES {
            bail!("remote Swarmlite connection info exceeded {MAX_CONNECTION_INFO_BYTES} bytes");
        }
        let info: ConnectionInfo = serde_json::from_slice(&output.stdout)
            .context("remote `swarmlite connection-info --json` returned invalid JSON")?;
        if info.token.len() < 16 {
            bail!("remote Swarmlite connection info returned an invalid cluster token");
        }
        Ok(info)
    }
}

struct RemoteController {
    host: String,
    port: u16,
}

impl RemoteController {
    fn parse(value: &str) -> Result<Self> {
        let parsed =
            Url::parse(value).context("remote connection info has an invalid Controller URL")?;
        if parsed.scheme() != "http"
            || parsed.host_str().is_none()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || !matches!(parsed.path(), "" | "/")
            || parsed.query().is_some()
            || parsed.fragment().is_some()
        {
            bail!(
                "remote connection info must contain an absolute HTTP Controller URL without credentials, path, query, or fragment"
            );
        }
        Ok(Self {
            host: normalized_host(&parsed).expect("checked above"),
            port: parsed
                .port_or_known_default()
                .expect("HTTP has a default port"),
        })
    }

    fn forward_destination(&self) -> String {
        let host = if self.host.contains(':') {
            format!("[{}]", self.host)
        } else {
            self.host.clone()
        };
        format!("{host}:{}", self.port)
    }
}

fn normalized_host(url: &Url) -> Option<String> {
    url.host().map(|host| match host {
        url::Host::Domain(host) => host.to_owned(),
        url::Host::Ipv4(host) => host.to_string(),
        url::Host::Ipv6(host) => host.to_string(),
    })
}

struct SshTunnel {
    child: Child,
    local_address: SocketAddr,
}

impl SshTunnel {
    async fn start(target: &SshTarget, remote: &RemoteController) -> Result<Self> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .context("failed to reserve a local port for the SSH Controller tunnel")?;
        let local_address = listener.local_addr()?;
        drop(listener);

        let forward = format!(
            "{}:{}:{}",
            local_address.ip(),
            local_address.port(),
            remote.forward_destination()
        );
        let mut command = target.command();
        let mut child = command
            .arg("-T")
            .arg("-N")
            .arg("-o")
            .arg("ExitOnForwardFailure=yes")
            .arg("-L")
            .arg(forward)
            .arg(&target.host)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| {
                format!(
                    "failed to start SSH Controller tunnel through {}",
                    target.host
                )
            })?;

        let deadline = tokio::time::Instant::now() + SSH_TUNNEL_START_TIMEOUT;
        loop {
            if let Some(status) = child
                .try_wait()
                .context("failed to inspect SSH tunnel process")?
            {
                bail!("SSH Controller tunnel exited before it was ready ({status})");
            }
            if matches!(
                tokio::time::timeout(
                    Duration::from_millis(100),
                    TcpStream::connect(local_address)
                )
                .await,
                Ok(Ok(_))
            ) {
                break;
            }
            if tokio::time::Instant::now() >= deadline {
                let _ = child.start_kill();
                bail!("timed out waiting for the SSH Controller tunnel to start");
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        Ok(Self {
            child,
            local_address,
        })
    }

    fn local_address(&self) -> SocketAddr {
        self.local_address
    }
}

impl Drop for SshTunnel {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_ssh_controller_target() {
        let target = SshTarget::parse("ssh://deploy@example.com:2222").unwrap();
        assert_eq!(target.host, "example.com");
        assert_eq!(target.user.as_deref(), Some("deploy"));
        assert_eq!(target.port, Some(2222));

        let alias = SshTarget::parse("ssh://production").unwrap();
        assert_eq!(alias.host, "production");
        assert_eq!(alias.user, None);
        assert_eq!(alias.port, None);
    }

    #[test]
    fn rejects_ambiguous_or_unsafe_ssh_controller_targets() {
        for target in [
            "ssh://user:password@example.com",
            "ssh://example.com/path",
            "ssh://example.com?option=value",
            "ssh://example.com#fragment",
            "ssh://example.com:0",
        ] {
            assert!(SshTarget::parse(target).is_err(), "accepted {target}");
        }
    }

    #[test]
    fn parses_remote_http_controller_for_forwarding() {
        let controller = RemoteController::parse("http://[::1]:17080").unwrap();
        assert_eq!(controller.host, "::1");
        assert_eq!(controller.port, 17080);
        assert_eq!(controller.forward_destination(), "[::1]:17080");
        assert!(RemoteController::parse("https://controller.example:17080").is_err());
        assert!(RemoteController::parse("http://controller.example:17080/v1").is_err());
    }
}
