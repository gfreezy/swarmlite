use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet},
    net::IpAddr,
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

mod compose;

pub use compose::{
    ParsedStack, ParsedStackDocument, StackConfigSource, StackRegistryCredential, parse_stack,
    parse_stack_document, resolve_config_digests,
};

pub fn validate_stack_name(name: &str) -> Result<()> {
    if name.is_empty()
        || !name
            .chars()
            .all(|value| value.is_ascii_alphanumeric() || matches!(value, '-' | '_' | '.'))
    {
        bail!("stack name may contain only letters, numbers, '.', '-' and '_'");
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServiceSpec {
    pub image: String,
    #[serde(default, skip_serializing_if = "PullPolicy::is_missing")]
    pub pull_policy: PullPolicy,
    pub command: Vec<String>,
    pub entrypoint: Vec<String>,
    pub environment: Vec<String>,
    #[serde(default)]
    pub expose: Vec<ServicePort>,
    pub ports: Vec<ServicePort>,
    pub volumes: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub configs: Vec<ServiceConfigMount>,
    pub container_labels: BTreeMap<String, String>,
    pub service_labels: BTreeMap<String, String>,
    pub healthcheck: Option<HealthcheckSpec>,
    pub replicas: u32,
    pub constraints: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_replicas_per_node: Option<u32>,
    pub max_surge: u32,
    pub stop_grace_period_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServiceConfigMount {
    pub source: String,
    pub target: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uid: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gid: Option<u32>,
    pub mode: u32,
    /// Resolved by the Controller from the contents uploaded with the Stack.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub digest: String,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PullPolicy {
    Always,
    #[default]
    #[serde(alias = "if_not_present")]
    Missing,
    Never,
}

impl PullPolicy {
    pub const fn is_missing(&self) -> bool {
        matches!(self, Self::Missing)
    }

    pub fn refreshes_cached_image(self, image: &str) -> bool {
        match self {
            Self::Always => true,
            Self::Missing => image_uses_latest_tag(image),
            Self::Never => false,
        }
    }
}

fn image_uses_latest_tag(image: &str) -> bool {
    if image.contains('@') {
        return false;
    }
    let name = image.rsplit('/').next().unwrap_or(image);
    match name.rsplit_once(':') {
        Some((_, tag)) => tag == "latest",
        None => true,
    }
}

pub fn service_spec_hash(spec: &ServiceSpec) -> String {
    let encoded = serde_json::to_vec(spec).expect("ServiceSpec serialization cannot fail");
    let digest = Sha256::digest(encoded);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub fn config_digest(contents: &[u8]) -> String {
    let digest = Sha256::digest(contents);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod pull_policy_tests {
    use super::*;

    #[test]
    fn missing_policy_refreshes_only_effective_latest_tags() {
        for image in [
            "nginx",
            "nginx:latest",
            "docker.io/library/nginx",
            "localhost:5000/nginx:latest",
        ] {
            assert!(PullPolicy::Missing.refreshes_cached_image(image), "{image}");
        }
        for image in [
            "nginx:1.29",
            "localhost:5000/nginx:1.29",
            "nginx@sha256:0123456789abcdef",
        ] {
            assert!(
                !PullPolicy::Missing.refreshes_cached_image(image),
                "{image}"
            );
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HealthcheckSpec {
    pub test: Vec<String>,
    pub interval_nanos: Option<i64>,
    pub timeout_nanos: Option<i64>,
    pub retries: Option<i64>,
    pub start_period_nanos: Option<i64>,
    pub start_interval_nanos: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServicePort {
    pub target: u16,
    pub published: Option<u16>,
    pub protocol: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct StackGatewaySpec {
    #[serde(default)]
    pub tls: GatewayTlsMode,
    #[serde(default)]
    pub http: GatewayHttpMode,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub trusted_proxies: Vec<String>,
    #[serde(default)]
    pub http_routes: Vec<HttpRouteSpec>,
}

impl Default for StackGatewaySpec {
    fn default() -> Self {
        Self {
            tls: GatewayTlsMode::Serve,
            http: GatewayHttpMode::Redirect,
            trusted_proxies: Vec::new(),
            http_routes: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GatewayTlsMode {
    #[default]
    Serve,
    Disabled,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GatewayHttpMode {
    #[default]
    Redirect,
    Serve,
    Disabled,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HttpRouteSpec {
    pub hostnames: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub canonical_hostname: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<GatewayTlsMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http: Option<GatewayHttpMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trusted_proxies: Option<Vec<String>>,
    pub rules: Vec<HttpRouteRule>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HttpRouteRule {
    #[serde(default)]
    pub matches: Vec<HttpPathMatch>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rewrite: Option<HttpPathRewrite>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache: Option<HttpCacheSpec>,
    pub backend: HttpBackend,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(try_from = "HttpCacheSpecInput")]
pub struct HttpCacheSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allowed_http_verbs: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_cacheable_body_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_request_body_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<HttpCacheKeySpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_codes: Option<Vec<u16>>,
    // Persisted Stack snapshots may contain cache-handler fields removed when
    // the native response cache replaced Souin. Retain their names long enough
    // for normal Stack validation to reject them, but omit them when trusted
    // historical state is serialized again.
    #[serde(default, flatten, skip_serializing)]
    ignored_legacy_fields: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq, Default)]
pub struct HttpCacheKeySpec {
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub disable_query: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub hash: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub headers: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct HttpCacheSpecInput {
    #[serde(default)]
    ttl: Option<String>,
    #[serde(default)]
    allowed_http_verbs: Option<Vec<String>>,
    #[serde(default)]
    max_cacheable_body_bytes: Option<u64>,
    #[serde(default)]
    max_request_body_bytes: Option<u64>,
    #[serde(default)]
    status_codes: Option<Vec<u16>>,
    #[serde(default)]
    key: Option<HttpCacheKeySpecInput>,
    #[serde(default, flatten)]
    ignored_legacy_fields: BTreeMap<String, Value>,
}

#[derive(Debug, Deserialize)]
struct HttpCacheKeySpecInput {
    #[serde(default)]
    headers: Vec<String>,
    #[serde(default)]
    disable_query: bool,
    // The native cache always hashes its complete key. This setting is
    // accepted only so pre-native Stack files and snapshots remain readable.
    #[serde(default)]
    hash: bool,
    #[serde(default, flatten)]
    ignored_fields: BTreeMap<String, Value>,
}

impl TryFrom<HttpCacheSpecInput> for HttpCacheSpec {
    type Error = String;

    fn try_from(mut input: HttpCacheSpecInput) -> std::result::Result<Self, Self::Error> {
        let key = if let Some(key) = input.key {
            input.ignored_legacy_fields.extend(
                key.ignored_fields
                    .into_iter()
                    .map(|(field, value)| (format!("key.{field}"), value)),
            );
            Some(HttpCacheKeySpec {
                disable_query: key.disable_query,
                hash: key.hash,
                headers: key.headers,
            })
        } else {
            None
        };
        Ok(Self {
            ttl: input.ttl,
            allowed_http_verbs: input.allowed_http_verbs,
            max_cacheable_body_bytes: input.max_cacheable_body_bytes,
            max_request_body_bytes: input.max_request_body_bytes,
            key,
            status_codes: input.status_codes,
            ignored_legacy_fields: input.ignored_legacy_fields,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HttpPathMatch {
    pub path: String,
    #[serde(rename = "type", default)]
    pub kind: HttpPathMatchType,
    #[serde(default)]
    pub ignore_case: bool,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HttpPathMatchType {
    Exact,
    #[default]
    Prefix,
    Regex,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HttpPathRewrite {
    #[serde(default)]
    pub strip_prefix: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replace_prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replace_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HttpBackend {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    #[serde(default, deserialize_with = "deserialize_backend_port")]
    pub port: u16,
    #[serde(default)]
    pub protocol: HttpBackendProtocol,
    #[serde(default = "default_true")]
    pub preserve_host: bool,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HttpBackendProtocol {
    #[default]
    Http,
    Https,
    H2c,
}

const fn default_true() -> bool {
    true
}

fn deserialize_backend_port<'de, D>(deserializer: D) -> std::result::Result<u16, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let port = u16::deserialize(deserializer)?;
    if port == 0 {
        return Err(serde::de::Error::custom("port must be between 1 and 65535"));
    }
    Ok(port)
}

#[derive(Debug, Clone, Serialize)]
pub struct HttpServer {
    pub listen: Vec<String>,
    pub routes: Vec<Route>,
    pub automatic_https: AutomaticHttps,
}

#[derive(Debug, Clone, Serialize)]
pub struct AutomaticHttps {
    pub disable_redirects: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub skip: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StorageConfig {
    pub module: &'static str,
    pub controller: String,
    pub token_env: &'static str,
    pub gateway_id_env: &'static str,
    pub timeout: &'static str,
    pub probe_timeout: &'static str,
    pub owner_cache_ttl: &'static str,
    pub lock_lease: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct Route {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "match", skip_serializing_if = "Vec::is_empty")]
    pub matchers: Vec<RequestMatcher>,
    pub handle: Vec<Value>,
    pub terminal: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct RequestMatcher {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub host: Vec<String>,
    pub protocol: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path_regexp: Option<PathRegexpMatcher>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PathRegexpMatcher {
    pub pattern: String,
}

pub fn validate_and_normalize(
    gateway: &mut StackGatewaySpec,
    services: &BTreeMap<String, ServiceSpec>,
) -> Result<()> {
    normalize_trusted_proxies(&mut gateway.trusted_proxies).context("invalid trusted_proxies")?;
    let mut seen_hostnames = BTreeSet::new();
    for (route_index, route) in gateway.http_routes.iter_mut().enumerate() {
        if route.hostnames.is_empty() {
            bail!("http_routes[{route_index}].hostnames must not be empty");
        }
        for hostname in &mut route.hostnames {
            *hostname = normalize_hostname(hostname, true).with_context(|| {
                format!("http_routes[{route_index}].hostnames contains an invalid host")
            })?;
            if !seen_hostnames.insert(hostname.clone()) {
                bail!("hostname {hostname:?} appears in more than one route");
            }
        }
        route.hostnames.sort();
        route.hostnames.dedup();

        if let Some(canonical_hostname) = &mut route.canonical_hostname {
            *canonical_hostname =
                normalize_hostname(canonical_hostname, false).with_context(|| {
                    format!(
                        "http_routes[{route_index}].canonical_hostname contains an invalid host"
                    )
                })?;
            if !route.hostnames.contains(canonical_hostname) {
                bail!("http_routes[{route_index}].canonical_hostname must appear in hostnames");
            }
        }

        let tls = route.tls.unwrap_or(gateway.tls);
        let http = route.http.unwrap_or(gateway.http);
        if tls == GatewayTlsMode::Disabled && http == GatewayHttpMode::Disabled {
            bail!("http_routes[{route_index}] disables both TLS and HTTP");
        }
        if http == GatewayHttpMode::Redirect && tls != GatewayTlsMode::Serve {
            bail!("http_routes[{route_index}].http=redirect requires tls=serve");
        }
        if let Some(trusted_proxies) = &mut route.trusted_proxies {
            normalize_trusted_proxies(trusted_proxies)
                .with_context(|| format!("invalid http_routes[{route_index}].trusted_proxies"))?;
        }
        if route.rules.is_empty() {
            bail!("http_routes[{route_index}].rules must not be empty");
        }

        let mut fallback_seen = false;
        for (rule_index, rule) in route.rules.iter_mut().enumerate() {
            if rule.matches.is_empty() {
                if fallback_seen {
                    bail!("http_routes[{route_index}] contains more than one fallback rule");
                }
                fallback_seen = true;
            }
            for path_match in &mut rule.matches {
                normalize_path_match(path_match).with_context(|| {
                    format!("invalid http_routes[{route_index}].rules[{rule_index}] match")
                })?;
            }
            validate_rewrite(rule.rewrite.as_ref(), &rule.matches).with_context(|| {
                format!("invalid http_routes[{route_index}].rules[{rule_index}].rewrite")
            })?;
            validate_cache(rule.cache.as_ref()).with_context(|| {
                format!("invalid http_routes[{route_index}].rules[{rule_index}].cache")
            })?;
            normalize_backend(&mut rule.backend, services).with_context(|| {
                format!("invalid http_routes[{route_index}].rules[{rule_index}].backend")
            })?;
        }
    }
    Ok(())
}

pub fn generate<'a>(
    stacks: impl IntoIterator<Item = (&'a str, &'a StackGatewaySpec)>,
    listen: &[String],
    mut resolve_service: impl FnMut(&str, &str, u16, HttpBackendProtocol) -> Vec<String>,
) -> HttpServer {
    let mut routes = Vec::new();
    let mut skip_certificates = BTreeSet::new();

    for (stack_name, gateway) in stacks {
        for (route_index, route) in gateway.http_routes.iter().enumerate() {
            let tls = route.tls.unwrap_or(gateway.tls);
            let http = route.http.unwrap_or(gateway.http);
            let trusted_proxies = route
                .trusted_proxies
                .as_deref()
                .unwrap_or(&gateway.trusted_proxies);
            let proxy_hostnames = route.canonical_hostname.as_ref().map_or_else(
                || route.hostnames.clone(),
                |hostname| vec![hostname.clone()],
            );
            let alias_hostnames = route
                .canonical_hostname
                .as_ref()
                .map(|canonical_hostname| {
                    route
                        .hostnames
                        .iter()
                        .filter(|hostname| *hostname != canonical_hostname)
                        .cloned()
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            if tls == GatewayTlsMode::Disabled {
                skip_certificates.extend(route.hostnames.iter().cloned());
            }
            if http == GatewayHttpMode::Redirect {
                let target_host = route
                    .canonical_hostname
                    .as_deref()
                    .map(format_host)
                    .unwrap_or_else(|| "{http.request.host}".to_owned());
                routes.push(redirect_route(
                    stack_name,
                    route_index,
                    "redirect",
                    &route.hostnames,
                    "http",
                    "https",
                    &target_host,
                ));
            }
            if tls == GatewayTlsMode::Serve {
                if let Some(canonical_hostname) = route.canonical_hostname.as_deref()
                    && !alias_hostnames.is_empty()
                {
                    routes.push(redirect_route(
                        stack_name,
                        route_index,
                        "canonical-https",
                        &alias_hostnames,
                        "https",
                        "https",
                        &format_host(canonical_hostname),
                    ));
                }
                build_proxy_routes(
                    ProxyRouteConfig {
                        stack_name,
                        route_index,
                        hostnames: &proxy_hostnames,
                        rules: &route.rules,
                        trusted_proxies,
                        request_protocol: "https",
                    },
                    &mut resolve_service,
                    &mut routes,
                );
            }
            if http == GatewayHttpMode::Serve {
                if let Some(canonical_hostname) = route.canonical_hostname.as_deref()
                    && !alias_hostnames.is_empty()
                {
                    routes.push(redirect_route(
                        stack_name,
                        route_index,
                        "canonical-http",
                        &alias_hostnames,
                        "http",
                        "http",
                        &format_host(canonical_hostname),
                    ));
                }
                build_proxy_routes(
                    ProxyRouteConfig {
                        stack_name,
                        route_index,
                        hostnames: &proxy_hostnames,
                        rules: &route.rules,
                        trusted_proxies,
                        request_protocol: "http",
                    },
                    &mut resolve_service,
                    &mut routes,
                );
            }
        }
    }

    routes.push(Route {
        id: "swarmlite-unmatched".to_owned(),
        matchers: Vec::new(),
        handle: vec![static_response(404)],
        terminal: true,
    });
    HttpServer {
        listen: listen.to_vec(),
        routes,
        automatic_https: AutomaticHttps {
            disable_redirects: true,
            skip: skip_certificates.into_iter().collect(),
        },
    }
}

pub fn storage(controller: String) -> StorageConfig {
    StorageConfig {
        module: "swarmlite",
        controller,
        token_env: "SWARMLITE_TOKEN",
        gateway_id_env: "SWARMLITE_GATEWAY_ID",
        timeout: "500ms",
        probe_timeout: "2s",
        owner_cache_ttl: "1m",
        lock_lease: "30s",
    }
}

pub fn routed_service_ports(gateway: &StackGatewaySpec, service_name: &str) -> BTreeSet<u16> {
    gateway
        .http_routes
        .iter()
        .flat_map(|route| &route.rules)
        .filter_map(|rule| {
            (rule.backend.service.as_deref() == Some(service_name)).then_some(rule.backend.port)
        })
        .collect()
}

const PRIVATE_PROXY_RANGES: [&str; 6] = [
    "192.168.0.0/16",
    "172.16.0.0/12",
    "10.0.0.0/8",
    "127.0.0.1/8",
    "fd00::/8",
    "::1",
];

fn normalize_trusted_proxies(proxies: &mut Vec<String>) -> Result<()> {
    let mut normalized = BTreeSet::new();
    for proxy in std::mem::take(proxies) {
        let proxy = proxy.trim();
        if proxy == "private_ranges" {
            normalized.extend(PRIVATE_PROXY_RANGES.into_iter().map(ToOwned::to_owned));
            continue;
        }
        let (address, prefix) = match proxy.split_once('/') {
            Some((address, prefix)) => (address, Some(prefix)),
            None => (proxy, None),
        };
        let address = address
            .parse::<IpAddr>()
            .with_context(|| format!("{proxy:?} is not an IP address or CIDR range"))?;
        let Some(prefix) = prefix else {
            normalized.insert(address.to_string());
            continue;
        };
        let prefix = prefix
            .parse::<u8>()
            .with_context(|| format!("{proxy:?} has an invalid CIDR prefix"))?;
        let maximum = if address.is_ipv4() { 32 } else { 128 };
        if prefix > maximum {
            bail!("{proxy:?} has a CIDR prefix greater than {maximum}");
        }
        normalized.insert(format!("{address}/{prefix}"));
    }
    *proxies = normalized.into_iter().collect();
    Ok(())
}

fn normalize_hostname(value: &str, allow_wildcard: bool) -> Result<String> {
    let value = value.trim().to_ascii_lowercase();
    if value.is_empty() || value.len() > 253 {
        bail!("hostname must contain between 1 and 253 characters");
    }
    if value.parse::<IpAddr>().is_ok() {
        return Ok(value);
    }
    if !allow_wildcard && value.starts_with("*.") {
        bail!("backend host must not contain a wildcard");
    }
    let name = if allow_wildcard {
        value.strip_prefix("*.").unwrap_or(&value)
    } else {
        &value
    };
    if name.is_empty()
        || name.ends_with('.')
        || name.split('.').any(|label| {
            label.is_empty()
                || label.len() > 63
                || label.starts_with('-')
                || label.ends_with('-')
                || !label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
    {
        bail!("invalid DNS hostname {value:?}");
    }
    Ok(value)
}

fn normalize_path_match(path_match: &mut HttpPathMatch) -> Result<()> {
    if path_match.path.is_empty() || path_match.path.len() > 2048 {
        bail!("path must contain between 1 and 2048 characters");
    }
    if path_match.path.chars().any(char::is_control) {
        bail!("path must not contain control characters");
    }
    match path_match.kind {
        HttpPathMatchType::Exact | HttpPathMatchType::Prefix => {
            validate_rewrite_path(&path_match.path)?;
            if path_match.kind == HttpPathMatchType::Prefix && path_match.path.len() > 1 {
                let length = path_match.path.trim_end_matches('/').len();
                path_match.path.truncate(length);
            }
        }
        HttpPathMatchType::Regex => {
            regex::Regex::new(&path_match.path).with_context(|| {
                format!("invalid RE2-compatible path regex {:?}", path_match.path)
            })?;
        }
    }
    Ok(())
}

fn validate_rewrite(rewrite: Option<&HttpPathRewrite>, matches: &[HttpPathMatch]) -> Result<()> {
    let Some(rewrite) = rewrite else {
        return Ok(());
    };
    let operation_count = usize::from(rewrite.strip_prefix)
        + usize::from(rewrite.replace_prefix.is_some())
        + usize::from(rewrite.replace_path.is_some());
    if operation_count != 1 {
        bail!("exactly one of strip_prefix, replace_prefix, or replace_path must be set");
    }
    if rewrite.strip_prefix || rewrite.replace_prefix.is_some() {
        if matches.is_empty() {
            bail!("prefix rewrites require at least one path match");
        }
        if matches
            .iter()
            .any(|path_match| path_match.kind != HttpPathMatchType::Prefix)
        {
            bail!("strip_prefix and replace_prefix require type=prefix");
        }
    }
    if let Some(path) = rewrite.replace_prefix.as_deref() {
        validate_rewrite_path(path)?;
    }
    if let Some(path) = rewrite.replace_path.as_deref() {
        validate_rewrite_path(path)?;
    }
    Ok(())
}

fn validate_rewrite_path(path: &str) -> Result<()> {
    if !path.starts_with('/') {
        bail!("path must start with /");
    }
    if path.contains('?') || path.contains('#') || path.chars().any(char::is_control) {
        bail!("path must not contain a query, fragment, or control character");
    }
    Ok(())
}

fn validate_cache(cache: Option<&HttpCacheSpec>) -> Result<()> {
    let Some(cache) = cache else {
        return Ok(());
    };

    if let Some(field) = cache.ignored_legacy_fields.keys().next() {
        bail!("unknown field `{field}`");
    }

    if let Some(ttl) = cache.ttl.as_deref() {
        let duration = humantime::parse_duration(ttl)
            .with_context(|| format!("cache ttl {ttl:?} is invalid"))?;
        if duration.is_zero() {
            bail!("cache ttl must be positive");
        }
    }
    if cache.max_cacheable_body_bytes == Some(0) {
        bail!("cache max_cacheable_body_bytes must be positive");
    }
    if cache
        .max_cacheable_body_bytes
        .is_some_and(|value| value > i64::MAX as u64)
    {
        bail!("cache max_cacheable_body_bytes is too large");
    }
    if cache.max_request_body_bytes == Some(0) {
        bail!("cache max_request_body_bytes must be positive");
    }
    if cache
        .max_request_body_bytes
        .is_some_and(|value| value > i64::MAX as u64)
    {
        bail!("cache max_request_body_bytes is too large");
    }
    if let Some(methods) = cache.allowed_http_verbs.as_ref() {
        if methods.is_empty() {
            bail!("cache allowed_http_verbs must not be empty");
        }
        for method in methods {
            if !valid_http_header_name(method) || method.eq_ignore_ascii_case("CONNECT") {
                bail!("cache method {method:?} is unsupported");
            }
        }
    }
    if let Some(key) = cache.key.as_ref() {
        for header in &key.headers {
            if !valid_http_header_name(header) {
                bail!("cache key header {header:?} is invalid");
            }
        }
    }
    if let Some(statuses) = cache.status_codes.as_ref() {
        if statuses.is_empty() {
            bail!("cache status_codes must not be empty");
        }
        for status in statuses {
            if *status < 200 || *status > 599 || *status == 304 {
                bail!("cache status code {status} is unsupported");
            }
        }
    }

    Ok(())
}

fn valid_http_header_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte))
}

fn normalize_backend(
    backend: &mut HttpBackend,
    services: &BTreeMap<String, ServiceSpec>,
) -> Result<()> {
    match (&mut backend.service, &mut backend.host) {
        (Some(service), None) => {
            *service = service.trim().to_owned();
            let Some(spec) = services.get(service) else {
                bail!("service {service:?} does not exist in this stack");
            };
            let targets = declared_tcp_targets(spec);
            if backend.port == 0 {
                match targets.as_slice() {
                    [target] => backend.port = *target,
                    [] => bail!(
                        "service {service:?} declares no TCP target ports; backend.port is required"
                    ),
                    _ => bail!(
                        "service {service:?} declares multiple TCP target ports: {}; backend.port is required",
                        targets
                            .iter()
                            .map(u16::to_string)
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                }
            } else if !targets.contains(&backend.port) {
                bail!(
                    "service {service:?} does not declare TCP target port {} in expose or ports",
                    backend.port
                );
            }
        }
        (None, Some(host)) => {
            if backend.port == 0 {
                bail!("port is required for a host backend");
            }
            *host = normalize_hostname(host, false)?;
        }
        (Some(_), Some(_)) => bail!("service and host are mutually exclusive"),
        (None, None) => bail!("one of service or host is required"),
    }
    Ok(())
}

fn declared_tcp_targets(service: &ServiceSpec) -> Vec<u16> {
    service
        .expose
        .iter()
        .chain(&service.ports)
        .filter(|port| port.protocol == "tcp")
        .map(|port| port.target)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

struct ProxyRouteConfig<'a> {
    stack_name: &'a str,
    route_index: usize,
    hostnames: &'a [String],
    rules: &'a [HttpRouteRule],
    trusted_proxies: &'a [String],
    request_protocol: &'static str,
}

fn build_proxy_routes(
    config: ProxyRouteConfig<'_>,
    resolve_service: &mut impl FnMut(&str, &str, u16, HttpBackendProtocol) -> Vec<String>,
    routes: &mut Vec<Route>,
) {
    let mut expanded = config
        .rules
        .iter()
        .enumerate()
        .flat_map(|(rule_index, rule)| {
            if rule.matches.is_empty() {
                vec![(rule_index, None, rule)]
            } else {
                rule.matches
                    .iter()
                    .enumerate()
                    .map(|(match_index, path_match)| {
                        (rule_index, Some((match_index, path_match)), rule)
                    })
                    .collect()
            }
        })
        .collect::<Vec<_>>();
    expanded.sort_by_key(|(rule_index, path_match, _)| {
        let (priority, prefix_length, match_index) = match path_match {
            Some((index, path_match)) => match path_match.kind {
                HttpPathMatchType::Exact => (0, Reverse(0), *index),
                HttpPathMatchType::Prefix => (1, Reverse(path_match.path.len()), *index),
                HttpPathMatchType::Regex => (2, Reverse(0), *index),
            },
            None => (3, Reverse(0), 0),
        };
        (priority, prefix_length, *rule_index, match_index)
    });

    for (rule_index, path_match, rule) in expanded {
        let match_index = path_match.map_or(0, |(index, _)| index);
        let path_match = path_match.map(|(_, path_match)| path_match);
        // Caddy handlers wrap the handlers that follow them. Keeping encode first
        // lets the cache store the upstream representation and compresses only
        // the response sent to each client.
        let mut handle = vec![encode_handler()];
        handle.extend(rule.cache.as_ref().map(cache_handler));
        handle.extend(rewrite_handlers(rule.rewrite.as_ref(), path_match));
        handle.extend(backend_handlers(
            config.stack_name,
            &rule.backend,
            config.trusted_proxies,
            resolve_service,
        ));
        routes.push(Route {
            id: format!(
                "swarmlite-{}-route-{}-rule-{}-match-{}-{}",
                sanitize_id(config.stack_name),
                config.route_index + 1,
                rule_index + 1,
                match_index + 1,
                config.request_protocol
            ),
            matchers: vec![RequestMatcher {
                host: config.hostnames.to_vec(),
                protocol: config.request_protocol,
                path_regexp: path_match.map(|path_match| PathRegexpMatcher {
                    pattern: path_pattern(path_match),
                }),
            }],
            handle,
            terminal: true,
        });
    }
}

fn encode_handler() -> Value {
    json!({
        "handler": "encode",
        "encodings": {
            "zstd": {},
            "gzip": {},
        },
        "prefer": ["zstd", "gzip"],
        "minimum_length": 512,
    })
}

fn cache_handler(cache: &HttpCacheSpec) -> Value {
    let mut handler = serde_json::to_value(cache)
        .expect("HTTP cache settings serialize")
        .as_object()
        .expect("HTTP cache settings serialize as an object")
        .clone();
    handler.insert("handler".to_owned(), json!("cache"));
    handler.insert("path".to_owned(), json!("/cache/native-v1/cache.db"));
    handler.insert("max_size_bytes".to_owned(), json!(1_073_741_824_u64));
    handler.insert("mmap_size_bytes".to_owned(), json!(268_435_456_u64));
    handler.insert("read_connections".to_owned(), json!(4));
    handler.insert("cleanup_interval".to_owned(), json!("5m"));
    handler.insert("journal_size_limit".to_owned(), json!(67_108_864));
    Value::Object(handler)
}

fn redirect_route(
    stack_name: &str,
    route_index: usize,
    id_suffix: &str,
    hostnames: &[String],
    request_protocol: &'static str,
    target_protocol: &str,
    target_host: &str,
) -> Route {
    Route {
        id: format!(
            "swarmlite-{}-route-{}-{}",
            sanitize_id(stack_name),
            route_index + 1,
            id_suffix
        ),
        matchers: vec![RequestMatcher {
            host: hostnames.to_vec(),
            protocol: request_protocol,
            path_regexp: None,
        }],
        handle: vec![json!({
            "handler": "static_response",
            "status_code": 308,
            "headers": {
                "Location": [format!(
                    "{target_protocol}://{target_host}{{http.request.uri}}"
                )]
            }
        })],
        terminal: true,
    }
}

fn rewrite_handlers(
    rewrite: Option<&HttpPathRewrite>,
    path_match: Option<&HttpPathMatch>,
) -> Vec<Value> {
    let Some(rewrite) = rewrite else {
        return Vec::new();
    };
    if let Some(path) = rewrite.replace_path.as_deref() {
        return vec![json!({ "handler": "rewrite", "uri": path })];
    }
    let Some(path_match) = path_match else {
        return Vec::new();
    };
    if rewrite.strip_prefix {
        return vec![json!({
            "handler": "rewrite",
            "strip_path_prefix": path_match.path
        })];
    }
    if let Some(replacement) = rewrite.replace_prefix.as_deref() {
        let replacement = if replacement == "/" {
            ""
        } else {
            replacement.trim_end_matches('/')
        };
        let case_flag = if path_match.ignore_case { "(?i)" } else { "" };
        return vec![json!({
            "handler": "rewrite",
            "path_regexp": [{
                "find": format!("{case_flag}^{}", regex::escape(&path_match.path)),
                "replace": replacement
            }]
        })];
    }
    Vec::new()
}

fn backend_handlers(
    stack_name: &str,
    backend: &HttpBackend,
    trusted_proxies: &[String],
    resolve_service: &mut impl FnMut(&str, &str, u16, HttpBackendProtocol) -> Vec<String>,
) -> Vec<Value> {
    let (upstreams, upstream_name) = match (&backend.service, &backend.host) {
        (Some(service_name), None) => (
            resolve_service(stack_name, service_name, backend.port, backend.protocol)
                .into_iter()
                .map(|dial| json!({ "dial": dial }))
                .collect::<Vec<_>>(),
            service_name.as_str(),
        ),
        (None, Some(host)) => (
            vec![json!({
                "dial": format!("{}:{}", format_host(host), backend.port)
            })],
            host.as_str(),
        ),
        _ => return vec![static_response(503)],
    };
    if upstreams.is_empty() {
        return vec![static_response(503)];
    }

    let transport = match backend.protocol {
        HttpBackendProtocol::Http => None,
        HttpBackendProtocol::Https => Some(json!({
            "protocol": "http",
            "tls": { "server_name": upstream_name }
        })),
        HttpBackendProtocol::H2c => Some(json!({
            "protocol": "http",
            "versions": ["h2c"]
        })),
    };
    let host_header = if backend.preserve_host {
        (backend.protocol == HttpBackendProtocol::Https).then_some("{http.request.host}")
    } else {
        Some(upstream_name)
    };

    let mut handler = serde_json::Map::from_iter([
        ("handler".to_owned(), json!("reverse_proxy")),
        ("upstreams".to_owned(), json!(upstreams)),
    ]);
    if !trusted_proxies.is_empty() {
        handler.insert("trusted_proxies".to_owned(), json!(trusted_proxies));
    }
    if let Some(transport) = transport {
        handler.insert("transport".to_owned(), transport);
    }
    if let Some(host_header) = host_header {
        handler.insert(
            "headers".to_owned(),
            json!({ "request": { "set": { "Host": [host_header] } } }),
        );
    }
    vec![Value::Object(handler)]
}

fn static_response(status_code: u16) -> Value {
    json!({
        "handler": "static_response",
        "status_code": status_code
    })
}

fn path_pattern(path_match: &HttpPathMatch) -> String {
    let case_flag = if path_match.ignore_case { "(?i)" } else { "" };
    match path_match.kind {
        HttpPathMatchType::Exact => {
            format!("{case_flag}^{}$", regex::escape(&path_match.path))
        }
        HttpPathMatchType::Prefix if path_match.path == "/" => format!("{case_flag}^/.*$"),
        HttpPathMatchType::Prefix => {
            format!("{case_flag}^{}(?:/.*)?$", regex::escape(&path_match.path))
        }
        HttpPathMatchType::Regex => format!("{case_flag}{}", path_match.path),
    }
}

fn format_host(host: &str) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]")
    } else {
        host.to_owned()
    }
}

fn sanitize_id(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.') {
                character
            } else {
                '-'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn service_with_tcp_ports(targets: &[u16]) -> ServiceSpec {
        ServiceSpec {
            image: "example/service:latest".into(),
            pull_policy: PullPolicy::Missing,
            command: Vec::new(),
            entrypoint: Vec::new(),
            environment: Vec::new(),
            expose: targets
                .iter()
                .map(|target| ServicePort {
                    target: *target,
                    published: None,
                    protocol: "tcp".into(),
                })
                .collect(),
            ports: Vec::new(),
            volumes: Vec::new(),
            configs: Vec::new(),
            container_labels: BTreeMap::new(),
            service_labels: BTreeMap::new(),
            healthcheck: None,
            replicas: 1,
            constraints: Vec::new(),
            max_replicas_per_node: None,
            max_surge: 1,
            stop_grace_period_seconds: 10,
        }
    }

    #[test]
    fn validates_normalizes_and_collects_service_ports() {
        let mut spec = StackGatewaySpec {
            http_routes: vec![HttpRouteSpec {
                hostnames: vec!["EXAMPLE.COM".into()],
                canonical_hostname: None,
                tls: None,
                http: None,
                trusted_proxies: None,
                rules: vec![HttpRouteRule {
                    matches: vec![HttpPathMatch {
                        path: "/api/".into(),
                        kind: HttpPathMatchType::Prefix,
                        ignore_case: true,
                    }],
                    rewrite: Some(HttpPathRewrite {
                        strip_prefix: true,
                        ..Default::default()
                    }),
                    cache: None,
                    backend: HttpBackend {
                        service: Some("api".into()),
                        host: None,
                        port: 8080,
                        protocol: HttpBackendProtocol::Http,
                        preserve_host: true,
                    },
                }],
            }],
            ..Default::default()
        };
        validate_and_normalize(
            &mut spec,
            &BTreeMap::from([("api".into(), service_with_tcp_ports(&[8080]))]),
        )
        .unwrap();
        assert_eq!(spec.http_routes[0].hostnames, ["example.com"]);
        assert_eq!(spec.http_routes[0].rules[0].matches[0].path, "/api");
        assert_eq!(routed_service_ports(&spec, "api"), BTreeSet::from([8080]));
    }

    #[test]
    fn rejects_missing_service_and_invalid_rewrite_combination() {
        let mut spec = StackGatewaySpec {
            http_routes: vec![HttpRouteSpec {
                hostnames: vec!["example.com".into()],
                canonical_hostname: None,
                tls: None,
                http: None,
                trusted_proxies: None,
                rules: vec![HttpRouteRule {
                    matches: vec![HttpPathMatch {
                        path: "/health".into(),
                        kind: HttpPathMatchType::Exact,
                        ignore_case: false,
                    }],
                    rewrite: Some(HttpPathRewrite {
                        strip_prefix: true,
                        ..Default::default()
                    }),
                    cache: None,
                    backend: HttpBackend {
                        service: Some("missing".into()),
                        host: None,
                        port: 8080,
                        protocol: HttpBackendProtocol::Http,
                        preserve_host: true,
                    },
                }],
            }],
            ..Default::default()
        };
        let error = validate_and_normalize(&mut spec, &BTreeMap::new()).unwrap_err();
        assert!(error.to_string().contains("rewrite"));
    }

    #[test]
    fn renders_internal_and_external_caddy_routes() {
        let gateway: StackGatewaySpec = serde_json::from_value(json!({
            "tls": "serve",
            "http": "redirect",
            "http_routes": [{
                "hostnames": ["example.com"],
                "rules": [
                    {
                        "matches": [{"path": "/api", "ignore_case": true}],
                        "rewrite": {"strip_prefix": true},
                        "backend": {"service": "api", "port": 8080}
                    },
                    {
                        "matches": [{"path": "/openai"}],
                        "rewrite": {"replace_prefix": "/"},
                        "backend": {
                            "host": "api.openai.com",
                            "port": 443,
                            "protocol": "https",
                            "preserve_host": false
                        }
                    }
                ]
            }]
        }))
        .unwrap();
        let server = generate(
            [("demo", &gateway)],
            &[":80".into(), ":443".into()],
            |_, service, port, _| vec![format!("10.0.0.21:{port}-{service}")],
        );
        let value = serde_json::to_value(server).unwrap();
        assert_eq!(value["routes"][0]["handle"][0]["status_code"], 308);
        let routes = value["routes"].as_array().unwrap();
        let external = routes
            .iter()
            .find(|route| {
                route["handle"].as_array().is_some_and(|handlers| {
                    handlers.last().is_some_and(|handler| {
                        handler["upstreams"][0]["dial"] == "api.openai.com:443"
                    })
                })
            })
            .unwrap();
        let handler = external["handle"].as_array().unwrap().last().unwrap();
        assert_eq!(handler["transport"]["tls"]["server_name"], "api.openai.com");
        assert_eq!(
            handler["headers"]["request"]["set"]["Host"][0],
            "api.openai.com"
        );
    }

    #[test]
    fn enables_fixed_response_compression_for_every_proxy_route() {
        let default = parse_stack(
            r#"
services:
  web:
    image: nginx
    expose: [80]
x-swarmlite:
  tls: disabled
  http: serve
  http_routes:
    - hostnames: [compressed.example.com]
      rules:
        - backend: { service: web }
"#,
        )
        .unwrap();

        let value = serde_json::to_value(generate(
            [("compressed", &default.gateway)],
            &[":80".into()],
            |_, _, port, _| vec![format!("10.0.0.21:{port}")],
        ))
        .unwrap();
        let handlers = value["routes"][0]["handle"].as_array().unwrap();
        assert_eq!(handlers[0]["handler"], "encode");
        assert_eq!(handlers[0]["encodings"], json!({"zstd": {}, "gzip": {}}));
        assert_eq!(handlers[0]["prefer"], json!(["zstd", "gzip"]));
        assert_eq!(handlers[0]["minimum_length"], 512);
        assert_eq!(handlers[1]["handler"], "reverse_proxy");
    }

    #[test]
    fn inherits_and_overrides_stack_trusted_proxies() {
        let parsed = parse_stack(
            r#"
services:
  web:
    image: nginx
x-swarmlite:
  tls: disabled
  http: serve
  trusted_proxies:
    - private_ranges
    - 192.0.2.10
  http_routes:
    - hostnames: [inherited.example.com]
      rules:
        - backend: { host: upstream.example.com, port: 80 }
    - hostnames: [overridden.example.com]
      trusted_proxies: [203.0.113.0/24]
      rules:
        - backend: { host: upstream.example.com, port: 80 }
    - hostnames: [disabled.example.com]
      trusted_proxies: []
      rules:
        - backend: { host: upstream.example.com, port: 80 }
"#,
        )
        .unwrap();

        assert!(
            parsed
                .gateway
                .trusted_proxies
                .contains(&"10.0.0.0/8".to_owned())
        );
        assert!(
            parsed
                .gateway
                .trusted_proxies
                .contains(&"192.0.2.10".to_owned())
        );
        let value = serde_json::to_value(generate(
            [("demo", &parsed.gateway)],
            &[":80".into()],
            |_, _, _, _| Vec::new(),
        ))
        .unwrap();
        let proxy_for = |hostname: &str| {
            value["routes"]
                .as_array()
                .unwrap()
                .iter()
                .find(|route| route["match"][0]["host"] == json!([hostname]))
                .unwrap()["handle"]
                .as_array()
                .unwrap()
                .last()
                .unwrap()
                .clone()
        };

        let inherited = proxy_for("inherited.example.com");
        assert!(
            inherited["trusted_proxies"]
                .as_array()
                .unwrap()
                .contains(&json!("fd00::/8"))
        );
        assert_eq!(
            proxy_for("overridden.example.com")["trusted_proxies"],
            json!(["203.0.113.0/24"])
        );
        assert!(
            proxy_for("disabled.example.com")
                .get("trusted_proxies")
                .is_none()
        );
    }

    #[test]
    fn rejects_invalid_stack_and_route_trusted_proxies() {
        for (yaml, expected) in [
            (
                r#"
services:
  web:
    image: nginx
x-swarmlite:
  trusted_proxies: [example.com]
  http_routes:
    - hostnames: [example.com]
      rules:
        - backend: { host: upstream.example.com, port: 80 }
"#,
                "invalid trusted_proxies",
            ),
            (
                r#"
services:
  web:
    image: nginx
x-swarmlite:
  http_routes:
    - hostnames: [example.com]
      trusted_proxies: [10.0.0.0/33]
      rules:
        - backend: { host: upstream.example.com, port: 80 }
"#,
                "http_routes[0].trusted_proxies",
            ),
        ] {
            let error = parse_stack(yaml).unwrap_err();
            assert!(format!("{error:#}").contains(expected), "{error:#}");
        }
    }

    #[test]
    fn places_response_compression_outside_cache_rewrite_and_proxy() {
        let parsed = parse_stack(
            r#"
services:
  api:
    image: example/api
    expose: [8080]
x-swarmlite:
  tls: disabled
  http: serve
  http_routes:
    - hostnames: [cache.example.com]
      rules:
        - matches:
            - path: /public
          rewrite:
            strip_prefix: true
          cache:
            ttl: 5m
            allowed_http_verbs: [GET, POST]
            max_cacheable_body_bytes: 1048576
            max_request_body_bytes: 65536
            key:
              headers: [Accept-Language]
            status_codes: [200, 404]
          backend:
            service: api
"#,
        )
        .unwrap();

        let value = serde_json::to_value(generate(
            [("demo", &parsed.gateway)],
            &[":80".into()],
            |_, _, port, _| vec![format!("10.0.0.21:{port}")],
        ))
        .unwrap();
        let handlers = value["routes"][0]["handle"].as_array().unwrap();

        assert_eq!(handlers[0]["handler"], "encode");
        assert_eq!(handlers[1]["handler"], "cache");
        assert_eq!(handlers[1]["ttl"], "5m");
        assert_eq!(handlers[1]["allowed_http_verbs"], json!(["GET", "POST"]));
        assert_eq!(handlers[1]["max_cacheable_body_bytes"], 1_048_576);
        assert_eq!(handlers[1]["max_request_body_bytes"], 65_536);
        assert_eq!(handlers[1]["key"]["headers"], json!(["Accept-Language"]));
        assert_eq!(handlers[1]["status_codes"], json!([200, 404]));
        assert_eq!(handlers[1]["path"], "/cache/native-v1/cache.db");
        assert_eq!(handlers[1]["max_size_bytes"], 1_073_741_824_u64);
        assert_eq!(handlers[1]["mmap_size_bytes"], 268_435_456_u64);
        assert_eq!(handlers[2]["handler"], "rewrite");
        assert_eq!(handlers[3]["handler"], "reverse_proxy");
    }

    #[test]
    fn preserves_souin_cache_settings_when_serializing() {
        let cache: HttpCacheSpec = serde_json::from_value(json!({
            "ttl": "24h",
            "allowed_http_verbs": ["GET", "HEAD"],
            "key": {
                "disable_query": true,
                "hash": true,
                "headers": ["accept-encoding"]
            }
        }))
        .unwrap();

        assert_eq!(
            cache.allowed_http_verbs.as_deref(),
            Some(["GET".to_owned(), "HEAD".to_owned()].as_slice())
        );
        assert_eq!(
            serde_json::to_value(cache).unwrap(),
            json!({
                "ttl": "24h",
                "allowed_http_verbs": ["GET", "HEAD"],
                "key": {
                    "disable_query": true,
                    "hash": true,
                    "headers": ["accept-encoding"]
                }
            })
        );
    }

    #[test]
    fn accepts_souin_cache_key_settings_in_stack_configuration() {
        let parsed = parse_stack(
            r#"
services:
  web:
    image: nginx
    expose: [80]
x-swarmlite:
  http_routes:
    - hostnames: [example.com]
      rules:
        - cache:
            allowed_http_verbs: [GET, HEAD]
            key:
              hash: true
              disable_query: true
              headers: [x-preferred-languages, x-app-language]
          backend:
            service: web
"#,
        )
        .unwrap();

        let cache = parsed.gateway.http_routes[0].rules[0]
            .cache
            .as_ref()
            .unwrap();
        assert_eq!(
            cache.allowed_http_verbs.as_deref(),
            Some(["GET".to_owned(), "HEAD".to_owned()].as_slice())
        );
        assert_eq!(
            cache.key.as_ref().unwrap().headers,
            ["x-preferred-languages", "x-app-language"]
        );
        assert!(cache.key.as_ref().unwrap().disable_query);
        assert!(cache.key.as_ref().unwrap().hash);
    }

    #[test]
    fn rejects_fields_from_the_removed_cache_handler_abstraction() {
        let error = parse_stack(
            r#"
services:
  web:
    image: nginx
    expose: [80]
x-swarmlite:
  http_routes:
    - hostnames: [example.com]
      rules:
        - cache:
            handler: reverse_proxy
          backend:
            service: web
"#,
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains("unknown field `handler`"));
    }

    #[test]
    fn rejects_removed_souin_cache_modes() {
        let error = parse_stack(
            r#"
services:
  web:
    image: nginx
    expose: [80]
x-swarmlite:
  http_routes:
    - hostnames: [example.com]
      rules:
        - cache:
            mode: strict
          backend:
            service: web
"#,
        )
        .unwrap_err();

        assert!(format!("{error:#}").contains("unknown field `mode`"));
    }

    #[test]
    fn redirects_aliases_to_the_canonical_hostname_in_one_hop() {
        let mut gateway: StackGatewaySpec = serde_json::from_value(json!({
            "tls": "serve",
            "http": "redirect",
            "http_routes": [{
                "hostnames": ["www.ieltsbao.com", "IELTSBAO.COM"],
                "canonical_hostname": "IELTSBAO.COM",
                "rules": [{
                    "backend": {"host": "upstream.example.com", "port": 80}
                }]
            }]
        }))
        .unwrap();
        validate_and_normalize(&mut gateway, &BTreeMap::new()).unwrap();

        assert_eq!(
            gateway.http_routes[0].canonical_hostname.as_deref(),
            Some("ieltsbao.com")
        );
        let value = serde_json::to_value(generate(
            [("demo", &gateway)],
            &[":80".into(), ":443".into()],
            |_, _, _, _| Vec::new(),
        ))
        .unwrap();
        let routes = value["routes"].as_array().unwrap();

        assert_eq!(
            routes[0]["match"][0]["host"],
            json!(["ieltsbao.com", "www.ieltsbao.com"])
        );
        assert_eq!(routes[0]["match"][0]["protocol"], "http");
        assert_eq!(
            routes[0]["handle"][0]["headers"]["Location"][0],
            "https://ieltsbao.com{http.request.uri}"
        );
        assert_eq!(routes[1]["match"][0]["host"], json!(["www.ieltsbao.com"]));
        assert_eq!(routes[1]["match"][0]["protocol"], "https");
        assert_eq!(
            routes[1]["handle"][0]["headers"]["Location"][0],
            "https://ieltsbao.com{http.request.uri}"
        );
        assert_eq!(routes[2]["match"][0]["host"], json!(["ieltsbao.com"]));
        assert_eq!(routes[2]["handle"][0]["handler"], "encode");
        assert_eq!(routes[2]["handle"][1]["handler"], "reverse_proxy");
    }

    #[test]
    fn requires_the_canonical_hostname_to_belong_to_the_route() {
        let mut gateway: StackGatewaySpec = serde_json::from_value(json!({
            "http_routes": [{
                "hostnames": ["www.ieltsbao.com"],
                "canonical_hostname": "ieltsbao.com",
                "rules": [{
                    "backend": {"host": "upstream.example.com", "port": 80}
                }]
            }]
        }))
        .unwrap();

        let error = validate_and_normalize(&mut gateway, &BTreeMap::new()).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("canonical_hostname must appear in hostnames")
        );
    }

    #[test]
    fn orders_every_match_type_and_renders_every_rewrite_type() {
        let mut gateway: StackGatewaySpec = serde_json::from_value(json!({
            "tls": "disabled",
            "http": "serve",
            "http_routes": [{
                "hostnames": ["routes.example.com"],
                "rules": [
                    {
                        "backend": {"host": "upstream.example.com", "port": 80}
                    },
                    {
                        "matches": [{"path": "^/items/[0-9]+$", "type": "regex"}],
                        "backend": {"host": "upstream.example.com", "port": 80}
                    },
                    {
                        "matches": [{"path": "/api"}],
                        "rewrite": {"strip_prefix": true},
                        "backend": {"host": "upstream.example.com", "port": 80}
                    },
                    {
                        "matches": [{"path": "/api/admin"}],
                        "rewrite": {"replace_prefix": "/internal"},
                        "backend": {"host": "upstream.example.com", "port": 80}
                    },
                    {
                        "matches": [{"path": "/api/admin", "type": "exact"}],
                        "rewrite": {"replace_path": "/health"},
                        "backend": {"host": "upstream.example.com", "port": 80}
                    }
                ]
            }]
        }))
        .unwrap();
        validate_and_normalize(&mut gateway, &BTreeMap::new()).unwrap();

        let value = serde_json::to_value(generate(
            [("demo", &gateway)],
            &[":80".into()],
            |_, _, _, _| Vec::new(),
        ))
        .unwrap();
        let routes = value["routes"].as_array().unwrap();
        let patterns = routes[..5]
            .iter()
            .map(|route| {
                route["match"][0]["path_regexp"]["pattern"]
                    .as_str()
                    .map(ToOwned::to_owned)
            })
            .collect::<Vec<_>>();
        assert_eq!(
            patterns,
            [
                Some("^/api/admin$".into()),
                Some("^/api/admin(?:/.*)?$".into()),
                Some("^/api(?:/.*)?$".into()),
                Some("^/items/[0-9]+$".into()),
                None,
            ]
        );
        assert_eq!(routes[0]["handle"][0]["handler"], "encode");
        assert_eq!(routes[0]["handle"][1]["uri"], "/health");
        assert_eq!(
            routes[1]["handle"][1]["path_regexp"][0]["replace"],
            "/internal"
        );
        assert_eq!(routes[2]["handle"][1]["strip_path_prefix"], "/api");
        assert_eq!(
            value["automatic_https"]["skip"],
            json!(["routes.example.com"])
        );
    }

    #[test]
    fn supports_preserve_host_for_https_services_and_h2c_for_external_hosts() {
        let mut gateway: StackGatewaySpec = serde_json::from_value(json!({
            "tls": "disabled",
            "http": "serve",
            "http_routes": [{
                "hostnames": ["protocols.example.com"],
                "rules": [
                    {
                        "matches": [{"path": "/secure"}],
                        "backend": {
                            "service": "secure_api",
                            "port": 8443,
                            "protocol": "https",
                            "preserve_host": true
                        }
                    },
                    {
                        "backend": {
                            "host": "h2c.example.net",
                            "port": 8080,
                            "protocol": "h2c",
                            "preserve_host": false
                        }
                    }
                ]
            }]
        }))
        .unwrap();
        validate_and_normalize(
            &mut gateway,
            &BTreeMap::from([("secure_api".into(), service_with_tcp_ports(&[8443]))]),
        )
        .unwrap();
        let value = serde_json::to_value(generate(
            [("demo", &gateway)],
            &[":80".into()],
            |_, _, _, _| vec!["10.0.0.21:28443".into()],
        ))
        .unwrap();
        let routes = value["routes"].as_array().unwrap();
        let secure = routes[0]["handle"].as_array().unwrap().last().unwrap();
        assert_eq!(secure["transport"]["tls"]["server_name"], "secure_api");
        assert_eq!(
            secure["headers"]["request"]["set"]["Host"][0],
            "{http.request.host}"
        );
        let h2c = routes[1]["handle"].as_array().unwrap().last().unwrap();
        assert_eq!(h2c["transport"]["versions"], json!(["h2c"]));
        assert_eq!(
            h2c["headers"]["request"]["set"]["Host"][0],
            "h2c.example.net"
        );
    }
}
