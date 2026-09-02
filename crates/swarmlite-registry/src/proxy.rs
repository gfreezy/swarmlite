use std::env;

use anyhow::{Context, Result, bail};

const HTTP_PROXY_NAMES: [&str; 2] = ["http_proxy", "HTTP_PROXY"];
const HTTPS_PROXY_NAMES: [&str; 2] = ["https_proxy", "HTTPS_PROXY"];
const ALL_PROXY_NAMES: [&str; 2] = ["all_proxy", "ALL_PROXY"];
const NO_PROXY_NAMES: [&str; 2] = ["no_proxy", "NO_PROXY"];
const PROXY_SCHEMES: [&str; 4] = ["http", "https", "socks5", "socks5h"];

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OutboundProxyConfig {
    http_proxy: Option<String>,
    https_proxy: Option<String>,
    all_proxy: Option<String>,
    no_proxy: Option<String>,
}

impl OutboundProxyConfig {
    /// Reads the conventional proxy environment variables. Lowercase values
    /// take precedence when both cases are present because curl requires the
    /// lowercase form of `http_proxy`.
    pub fn from_env() -> Result<Self> {
        Self::from_lookup(|name| env::var(name).ok())
    }

    pub fn new(
        http_proxy: Option<String>,
        https_proxy: Option<String>,
        all_proxy: Option<String>,
        no_proxy: Option<String>,
    ) -> Result<Self> {
        let http_proxy = nonempty(http_proxy);
        let https_proxy = nonempty(https_proxy);
        let all_proxy = nonempty(all_proxy);
        let no_proxy = nonempty(no_proxy);
        if let Some(proxy) = http_proxy.as_deref() {
            validate_proxy_url("proxy.http", proxy)?;
        }
        if let Some(proxy) = https_proxy.as_deref() {
            validate_proxy_url("proxy.https", proxy)?;
        }
        if let Some(proxy) = all_proxy.as_deref() {
            validate_proxy_url("proxy.all", proxy)?;
        }
        Ok(Self {
            http_proxy,
            https_proxy,
            all_proxy,
            no_proxy,
        })
    }

    pub fn enabled(&self) -> bool {
        self.http_proxy.is_some() || self.https_proxy.is_some() || self.all_proxy.is_some()
    }

    pub fn http_proxy(&self) -> Option<&str> {
        self.http_proxy.as_deref()
    }

    pub fn https_proxy(&self) -> Option<&str> {
        self.https_proxy.as_deref()
    }

    pub fn all_proxy(&self) -> Option<&str> {
        self.all_proxy.as_deref()
    }

    pub fn http_proxy_url(&self) -> Option<&str> {
        self.http_proxy().or_else(|| self.all_proxy())
    }

    pub fn https_proxy_url(&self) -> Option<&str> {
        self.https_proxy().or_else(|| self.all_proxy())
    }

    pub fn no_proxy(&self) -> Option<&str> {
        self.no_proxy.as_deref()
    }

    /// Returns a normalized environment for child processes. Both cases are
    /// exported so libraries and curl observe the same effective settings.
    pub fn environment_variables(&self) -> Vec<(&'static str, &str)> {
        let mut variables = Vec::with_capacity(8);
        append_env_pair(&mut variables, HTTP_PROXY_NAMES, self.http_proxy());
        append_env_pair(&mut variables, HTTPS_PROXY_NAMES, self.https_proxy());
        append_env_pair(&mut variables, ALL_PROXY_NAMES, self.all_proxy());
        append_env_pair(&mut variables, NO_PROXY_NAMES, self.no_proxy());
        variables
    }

    pub fn reqwest_proxies(&self) -> Result<Vec<reqwest::Proxy>> {
        let no_proxy = self.no_proxy().and_then(reqwest::NoProxy::from_string);
        let mut proxies = Vec::with_capacity(3);
        if let Some(proxy_url) = self.http_proxy() {
            proxies.push(
                reqwest::Proxy::http(proxy_url)
                    .context("invalid HTTP proxy")?
                    .no_proxy(no_proxy.clone()),
            );
        }
        if let Some(proxy_url) = self.https_proxy() {
            proxies.push(
                reqwest::Proxy::https(proxy_url)
                    .context("invalid HTTPS proxy")?
                    .no_proxy(no_proxy.clone()),
            );
        }
        if let Some(proxy_url) = self.all_proxy() {
            proxies.push(
                reqwest::Proxy::all(proxy_url)
                    .context("invalid all-protocol proxy")?
                    .no_proxy(no_proxy),
            );
        }
        Ok(proxies)
    }

    fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self> {
        let lookup_pair = |lookup: &mut dyn FnMut(&str) -> Option<String>, names: [&str; 2]| {
            names.into_iter().find_map(|name| nonempty(lookup(name)))
        };
        Self::new(
            lookup_pair(&mut lookup, HTTP_PROXY_NAMES),
            lookup_pair(&mut lookup, HTTPS_PROXY_NAMES),
            lookup_pair(&mut lookup, ALL_PROXY_NAMES),
            lookup_pair(&mut lookup, NO_PROXY_NAMES),
        )
    }
}

fn append_env_pair<'a>(
    variables: &mut Vec<(&'static str, &'a str)>,
    names: [&'static str; 2],
    value: Option<&'a str>,
) {
    if let Some(value) = value {
        variables.extend(names.map(|name| (name, value)));
    }
}

fn validate_proxy_url(name: &str, value: &str) -> Result<()> {
    let url = reqwest::Url::parse(value)
        .with_context(|| format!("{name} must be an absolute proxy URL"))?;
    if !PROXY_SCHEMES.contains(&url.scheme()) || url.host().is_none() {
        bail!(
            "{name} must use one of these URL schemes: {}",
            PROXY_SCHEMES.join(", ")
        );
    }
    reqwest::Proxy::all(value).with_context(|| format!("invalid {name}"))?;
    Ok(())
}

fn nonempty(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn accepts_protocol_specific_and_all_proxy_urls() {
        let split = OutboundProxyConfig::new(
            Some("http://http-proxy.example.com:3128".to_owned()),
            Some("https://https-proxy.example.com:3129".to_owned()),
            Some("socks5h://127.0.0.1:1080".to_owned()),
            Some("registry.internal.example.com".to_owned()),
        )
        .unwrap();
        assert_eq!(
            split.http_proxy_url(),
            Some("http://http-proxy.example.com:3128")
        );
        assert_eq!(
            split.https_proxy_url(),
            Some("https://https-proxy.example.com:3129")
        );
        assert_eq!(split.all_proxy(), Some("socks5h://127.0.0.1:1080"));
        assert_eq!(split.no_proxy(), Some("registry.internal.example.com"));
        assert_eq!(split.reqwest_proxies().unwrap().len(), 3);

        let all =
            OutboundProxyConfig::new(None, None, Some("socks5://127.0.0.1:1080".to_owned()), None)
                .unwrap();
        assert_eq!(all.http_proxy_url(), Some("socks5://127.0.0.1:1080"));
        assert_eq!(all.https_proxy_url(), Some("socks5://127.0.0.1:1080"));

        for scheme in ["socks5", "socks5h"] {
            let url = format!("{scheme}://127.0.0.1:1080");
            assert!(
                OutboundProxyConfig::new(Some(url.clone()), None, None, None)
                    .unwrap()
                    .reqwest_proxies()
                    .is_ok()
            );
            assert!(
                OutboundProxyConfig::new(None, Some(url), None, None)
                    .unwrap()
                    .reqwest_proxies()
                    .is_ok()
            );
        }
    }

    #[test]
    fn reads_both_environment_cases_and_prefers_lowercase() {
        let values = BTreeMap::from([
            ("HTTP_PROXY", "http://upper.example.com:3128"),
            ("http_proxy", "http://lower.example.com:3128"),
            ("HTTPS_PROXY", "http://secure.example.com:3128"),
            ("all_proxy", "socks5h://127.0.0.1:1080"),
            ("NO_PROXY", "localhost"),
        ]);
        let config = OutboundProxyConfig::from_lookup(|name| {
            values.get(name).map(|value| (*value).to_owned())
        })
        .unwrap();
        assert_eq!(config.http_proxy(), Some("http://lower.example.com:3128"));
        assert_eq!(config.https_proxy(), Some("http://secure.example.com:3128"));
        assert_eq!(config.all_proxy(), Some("socks5h://127.0.0.1:1080"));
        assert_eq!(config.no_proxy(), Some("localhost"));

        let environment = config
            .environment_variables()
            .into_iter()
            .collect::<BTreeMap<_, _>>();
        assert_eq!(environment["HTTP_PROXY"], "http://lower.example.com:3128");
        assert_eq!(environment["http_proxy"], "http://lower.example.com:3128");
        assert_eq!(environment["NO_PROXY"], "localhost");
        assert_eq!(environment["no_proxy"], "localhost");
    }

    #[test]
    fn rejects_relative_or_unknown_proxy_urls() {
        assert!(OutboundProxyConfig::new(Some("proxy:3128".to_owned()), None, None, None).is_err());
        assert!(
            OutboundProxyConfig::new(None, None, Some("ftp://127.0.0.1:21".to_owned()), None)
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
