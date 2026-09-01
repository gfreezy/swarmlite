use std::env;

use anyhow::{Context, Result, bail};

pub const HTTP_PROXY_ENV: &str = "SWARMLITE_HTTP_PROXY";
pub const HTTPS_PROXY_ENV: &str = "SWARMLITE_HTTPS_PROXY";
pub const SOCKS5_PROXY_ENV: &str = "SWARMLITE_SOCKS5_PROXY";
pub const NO_PROXY_ENV: &str = "SWARMLITE_NO_PROXY";

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OutboundProxyConfig {
    http_proxy_url: Option<String>,
    https_proxy_url: Option<String>,
    no_proxy: Option<String>,
}

impl OutboundProxyConfig {
    pub fn from_env() -> Result<Self> {
        Self::new(
            nonempty_env(HTTP_PROXY_ENV),
            nonempty_env(HTTPS_PROXY_ENV),
            nonempty_env(SOCKS5_PROXY_ENV),
            nonempty_env(NO_PROXY_ENV),
        )
    }

    pub fn new(
        http_proxy: Option<String>,
        https_proxy: Option<String>,
        socks5_proxy: Option<String>,
        no_proxy: Option<String>,
    ) -> Result<Self> {
        let http_proxy = nonempty(http_proxy);
        let https_proxy = nonempty(https_proxy);
        let socks5_proxy = nonempty(socks5_proxy);
        let no_proxy = nonempty(no_proxy);
        if socks5_proxy.is_some() && (http_proxy.is_some() || https_proxy.is_some()) {
            bail!(
                "{SOCKS5_PROXY_ENV} cannot be combined with {HTTP_PROXY_ENV} or {HTTPS_PROXY_ENV}"
            );
        }
        if let Some(proxy) = http_proxy.as_deref() {
            validate_proxy_url(HTTP_PROXY_ENV, proxy, &["http", "https"])?;
        }
        if let Some(proxy) = https_proxy.as_deref() {
            validate_proxy_url(HTTPS_PROXY_ENV, proxy, &["http", "https"])?;
        }
        if let Some(proxy) = socks5_proxy.as_deref() {
            validate_proxy_url(SOCKS5_PROXY_ENV, proxy, &["socks5", "socks5h"])?;
        }
        let (http_proxy_url, https_proxy_url) = if let Some(socks5_proxy) = socks5_proxy {
            (Some(socks5_proxy.clone()), Some(socks5_proxy))
        } else {
            (http_proxy.clone(), https_proxy.or(http_proxy))
        };
        Ok(Self {
            http_proxy_url,
            https_proxy_url,
            no_proxy,
        })
    }

    pub fn enabled(&self) -> bool {
        self.http_proxy_url.is_some() || self.https_proxy_url.is_some()
    }

    pub fn http_proxy_url(&self) -> Option<&str> {
        self.http_proxy_url.as_deref()
    }

    pub fn https_proxy_url(&self) -> Option<&str> {
        self.https_proxy_url.as_deref()
    }

    pub fn no_proxy(&self) -> Option<&str> {
        self.no_proxy.as_deref()
    }

    pub fn reqwest_proxies(&self) -> Result<Vec<reqwest::Proxy>> {
        let no_proxy = self.no_proxy().and_then(reqwest::NoProxy::from_string);
        let mut proxies = Vec::with_capacity(2);
        if let Some(proxy_url) = self.http_proxy_url() {
            proxies.push(
                reqwest::Proxy::http(proxy_url)
                    .context("invalid Swarmlite HTTP proxy")?
                    .no_proxy(no_proxy.clone()),
            );
        }
        if let Some(proxy_url) = self.https_proxy_url() {
            proxies.push(
                reqwest::Proxy::https(proxy_url)
                    .context("invalid Swarmlite HTTPS proxy")?
                    .no_proxy(no_proxy),
            );
        }
        Ok(proxies)
    }
}

fn validate_proxy_url(name: &str, value: &str, schemes: &[&str]) -> Result<()> {
    let url = reqwest::Url::parse(value)
        .with_context(|| format!("{name} must be an absolute proxy URL"))?;
    if !schemes.contains(&url.scheme()) || url.host().is_none() {
        bail!(
            "{name} must use one of these URL schemes: {}",
            schemes.join(", ")
        );
    }
    reqwest::Proxy::all(value).with_context(|| format!("invalid {name}"))?;
    Ok(())
}

fn nonempty_env(name: &str) -> Option<String> {
    nonempty(env::var(name).ok())
}

fn nonempty(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_http_https_and_socks5_proxy_urls() {
        let http = OutboundProxyConfig::new(
            Some("http://proxy.example.com:3128".to_owned()),
            None,
            None,
            Some("registry.internal.example.com".to_owned()),
        )
        .unwrap();
        assert_eq!(http.http_proxy_url(), Some("http://proxy.example.com:3128"));
        assert_eq!(
            http.https_proxy_url(),
            Some("http://proxy.example.com:3128")
        );
        assert_eq!(http.no_proxy(), Some("registry.internal.example.com"));
        assert_eq!(http.reqwest_proxies().unwrap().len(), 2);

        let split = OutboundProxyConfig::new(
            Some("http://http-proxy.example.com:3128".to_owned()),
            Some("https://https-proxy.example.com:3129".to_owned()),
            None,
            None,
        )
        .unwrap();
        assert_eq!(
            split.https_proxy_url(),
            Some("https://https-proxy.example.com:3129")
        );

        for scheme in ["socks5", "socks5h"] {
            let socks = OutboundProxyConfig::new(
                None,
                None,
                Some(format!("{scheme}://127.0.0.1:1080")),
                None,
            )
            .unwrap();
            assert!(socks.enabled());
            assert_eq!(socks.reqwest_proxies().unwrap().len(), 2);
        }
    }

    #[test]
    fn rejects_conflicts_and_mismatched_proxy_schemes() {
        assert!(
            OutboundProxyConfig::new(
                Some("http://127.0.0.1:3128".to_owned()),
                None,
                Some("socks5h://127.0.0.1:1080".to_owned()),
                None,
            )
            .is_err()
        );
        assert!(
            OutboundProxyConfig::new(
                Some("socks5h://127.0.0.1:1080".to_owned()),
                None,
                None,
                None,
            )
            .is_err()
        );
        assert!(
            OutboundProxyConfig::new(None, None, Some("http://127.0.0.1:3128".to_owned()), None,)
                .is_err()
        );
    }

    #[test]
    fn no_proxy_alone_does_not_enable_proxying() {
        let config = OutboundProxyConfig::new(
            None,
            None,
            None,
            Some("registry.internal.example.com".to_owned()),
        )
        .unwrap();
        assert!(!config.enabled());
        assert!(config.reqwest_proxies().unwrap().is_empty());
    }
}
