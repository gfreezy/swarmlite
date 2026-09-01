use anyhow::{Context, Result, bail};
use swarmlite_registry::OutboundProxyConfig;
use tokio::io::AsyncWriteExt;

use super::{ansi, stderr_color};

const RELEASES_URL: &str = "https://github.com/gfreezy/swarmlite/releases";
const MAX_INSTALLER_BYTES: u64 = 1024 * 1024;

pub(super) async fn run(version: &str) -> Result<()> {
    let installer_url = installer_url(version)?;
    eprintln!(
        "{}",
        ansi(
            stderr_color(),
            "36",
            format!("downloading the Swarmlite {version} installer")
        )
    );

    let response = download(&installer_url).await?;

    if response
        .content_length()
        .is_some_and(|length| length > MAX_INSTALLER_BYTES)
    {
        bail!("refusing to run an installer larger than {MAX_INSTALLER_BYTES} bytes");
    }

    let installer = response
        .bytes()
        .await
        .context("failed to read the downloaded installer")?;
    if installer.len() as u64 > MAX_INSTALLER_BYTES {
        bail!("refusing to run an installer larger than {MAX_INSTALLER_BYTES} bytes");
    }

    let mut child = tokio::process::Command::new("sh")
        .args(["-s", "--", "--version", version])
        .stdin(std::process::Stdio::piped())
        .spawn()
        .context("failed to start the Swarmlite installer with sh")?;
    let mut stdin = child
        .stdin
        .take()
        .context("failed to open the Swarmlite installer standard input")?;
    stdin
        .write_all(&installer)
        .await
        .context("failed to pass the downloaded installer to sh")?;
    drop(stdin);

    let status = child
        .wait()
        .await
        .context("failed to wait for the Swarmlite installer")?;
    if !status.success() {
        bail!("Swarmlite upgrade failed with {status}");
    }
    Ok(())
}

async fn download(url: &str) -> Result<reqwest::Response> {
    let proxy = match OutboundProxyConfig::from_env() {
        Ok(proxy) => proxy,
        Err(error) => {
            eprintln!(
                "Swarmlite proxy configuration is invalid ({error}); retrying the upgrade download directly"
            );
            return direct_download(url).await;
        }
    };
    if proxy.enabled() {
        let response = match proxy_client(&proxy) {
            Ok(client) => client.get(url).send().await,
            Err(error) => {
                eprintln!(
                    "Swarmlite proxy configuration is invalid ({error}); retrying the upgrade download directly"
                );
                return direct_download(url).await;
            }
        };
        match response {
            Ok(response) if response.status().is_success() => return Ok(response),
            Ok(response) if proxy_failure_status(response.status()) => {
                eprintln!(
                    "Swarmlite proxy returned HTTP {}; retrying the upgrade download directly",
                    response.status()
                );
            }
            Ok(response) => {
                return response
                    .error_for_status()
                    .with_context(|| format!("failed to download installer from {url}"));
            }
            Err(error) => {
                eprintln!(
                    "Swarmlite proxy could not download the upgrade ({error}); retrying directly"
                );
            }
        }
    }
    direct_download(url).await
}

async fn direct_download(url: &str) -> Result<reqwest::Response> {
    reqwest::Client::builder()
        .no_proxy()
        .build()?
        .get(url)
        .send()
        .await
        .with_context(|| format!("failed to download installer from {url}"))?
        .error_for_status()
        .with_context(|| format!("failed to download installer from {url}"))
}

fn proxy_failure_status(status: reqwest::StatusCode) -> bool {
    status == reqwest::StatusCode::PROXY_AUTHENTICATION_REQUIRED
        || status == reqwest::StatusCode::BAD_GATEWAY
        || status == reqwest::StatusCode::SERVICE_UNAVAILABLE
        || status == reqwest::StatusCode::GATEWAY_TIMEOUT
}

fn proxy_client(proxy: &OutboundProxyConfig) -> Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder()
        .no_proxy()
        .connect_timeout(std::time::Duration::from_secs(10))
        .read_timeout(std::time::Duration::from_secs(30));
    for proxy in proxy.reqwest_proxies()? {
        builder = builder.proxy(proxy);
    }
    builder.build().map_err(Into::into)
}

pub(super) fn validate_version(value: &str) -> std::result::Result<String, String> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err("version may contain only letters, numbers, '.', '_', and '-'".to_owned());
    }
    Ok(value.to_owned())
}

fn installer_url(version: &str) -> Result<String> {
    let version = validate_version(version).map_err(anyhow::Error::msg)?;
    if version == "latest" {
        Ok(format!("{RELEASES_URL}/latest/download/install.sh"))
    } else {
        Ok(format!("{RELEASES_URL}/download/{version}/install.sh"))
    }
}

#[cfg(test)]
mod tests {
    use super::{installer_url, validate_version};

    #[test]
    fn resolves_latest_and_pinned_installer_urls() {
        assert_eq!(
            installer_url("latest").unwrap(),
            "https://github.com/gfreezy/swarmlite/releases/latest/download/install.sh"
        );
        assert_eq!(
            installer_url("v0.2.0").unwrap(),
            "https://github.com/gfreezy/swarmlite/releases/download/v0.2.0/install.sh"
        );
    }

    #[test]
    fn rejects_versions_that_could_change_the_download_url() {
        for version in ["", "../latest", "v1/other", "v1?x=1", "v1 2"] {
            assert!(validate_version(version).is_err(), "accepted {version:?}");
            assert!(installer_url(version).is_err(), "accepted {version:?}");
        }
    }
}
