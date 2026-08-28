use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use serde_yaml::Value;

use crate::{
    GatewayHttpMode, GatewayTlsMode, HealthcheckSpec, HttpRouteSpec, PullPolicy,
    ServiceConfigMount, ServicePort, ServiceSpec, StackGatewaySpec,
};

#[derive(Debug, Clone)]
pub struct ParsedStack {
    pub services: BTreeMap<String, ServiceSpec>,
    pub gateway: StackGatewaySpec,
}

#[derive(Debug, Clone)]
pub struct ParsedStackDocument {
    pub name: Option<String>,
    pub stack: ParsedStack,
    pub configs: BTreeMap<String, StackConfigSource>,
    pub registries: BTreeMap<String, StackRegistryCredential>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StackConfigSource {
    pub file: String,
}

#[derive(Clone, PartialEq, Eq)]
pub struct StackRegistryCredential {
    pub username: String,
    pub password: String,
}

impl fmt::Debug for StackRegistryCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StackRegistryCredential")
            .field("username", &self.username)
            .field("password", &"[REDACTED]")
            .finish()
    }
}

#[derive(Debug, Deserialize)]
struct RawStack {
    services: BTreeMap<String, RawService>,
    #[serde(default)]
    configs: BTreeMap<String, RawConfigSource>,
    #[serde(rename = "x-swarmlite", default)]
    swarmlite: RawSwarmlite,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfigSource {
    file: String,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawSwarmlite {
    name: Option<String>,
    #[serde(default)]
    registries: BTreeMap<String, RawRegistryCredential>,
    #[serde(default)]
    tls: GatewayTlsMode,
    #[serde(default)]
    http: GatewayHttpMode,
    #[serde(default)]
    trusted_proxies: Vec<String>,
    #[serde(default)]
    http_routes: Vec<HttpRouteSpec>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRegistryCredential {
    username: String,
    password: String,
}

impl RawSwarmlite {
    fn gateway(self) -> StackGatewaySpec {
        StackGatewaySpec {
            tls: self.tls,
            http: self.http,
            trusted_proxies: self.trusted_proxies,
            http_routes: self.http_routes,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawService {
    image: Option<String>,
    #[serde(default)]
    pull_policy: PullPolicy,
    command: Option<StringOrList>,
    entrypoint: Option<StringOrList>,
    #[serde(default)]
    environment: StringMapOrList,
    #[serde(default)]
    labels: StringMapOrList,
    #[serde(default)]
    expose: Vec<ExposeValue>,
    #[serde(default)]
    ports: Vec<PortValue>,
    #[serde(default)]
    volumes: Vec<VolumeValue>,
    #[serde(default)]
    configs: Vec<ConfigValue>,
    #[serde(default)]
    deploy: RawDeploy,
    healthcheck: Option<RawHealthcheck>,
    stop_grace_period: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawDeploy {
    mode: Option<String>,
    replicas: Option<u32>,
    #[serde(default)]
    labels: StringMapOrList,
    #[serde(default)]
    placement: RawPlacement,
    #[serde(default)]
    update_config: RawUpdateConfig,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPlacement {
    #[serde(default)]
    constraints: Vec<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawUpdateConfig {
    parallelism: Option<u32>,
    order: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawHealthcheck {
    test: Option<StringOrList>,
    #[serde(default)]
    disable: bool,
    interval: Option<String>,
    timeout: Option<String>,
    retries: Option<i64>,
    start_period: Option<String>,
    start_interval: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum StringOrList {
    String(String),
    List(Vec<String>),
}

impl StringOrList {
    fn into_vec(self, field: &str) -> Result<Vec<String>> {
        match self {
            Self::List(items) => Ok(items),
            Self::String(value) => shell_words::split(&value)
                .with_context(|| format!("invalid shell-style {field}: {value}")),
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(untagged)]
enum StringMapOrList {
    Map(BTreeMap<String, Value>),
    List(Vec<String>),
    #[default]
    Empty,
}

impl StringMapOrList {
    fn into_map(self) -> Result<BTreeMap<String, String>> {
        match self {
            Self::Map(items) => items
                .into_iter()
                .map(|(key, value)| Ok((key, scalar_to_string(value)?)))
                .collect(),
            Self::List(items) => items
                .into_iter()
                .map(|item| {
                    let (key, value) = item.split_once('=').unwrap_or((&item, ""));
                    Ok((key.to_owned(), value.to_owned()))
                })
                .collect(),
            Self::Empty => Ok(BTreeMap::new()),
        }
    }

    fn into_environment(self) -> Result<Vec<String>> {
        match self {
            Self::Map(items) => items
                .into_iter()
                .map(|(key, value)| match value {
                    Value::Null => Ok(key),
                    other => Ok(format!("{key}={}", scalar_to_string(other)?)),
                })
                .collect(),
            Self::List(items) => Ok(items),
            Self::Empty => Ok(Vec::new()),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum PortValue {
    Number(u16),
    Short(String),
    Long(LongPort),
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ExposeValue {
    Number(u16),
    String(String),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LongPort {
    target: u16,
    published: Option<u16>,
    protocol: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum VolumeValue {
    Short(String),
    Long(LongVolume),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LongVolume {
    source: Option<String>,
    target: String,
    #[serde(default)]
    read_only: bool,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ConfigValue {
    Short(String),
    Long(LongConfig),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LongConfig {
    source: String,
    target: Option<String>,
    uid: Option<String>,
    gid: Option<String>,
    mode: Option<Value>,
}

pub fn parse_stack(yaml: &str) -> Result<ParsedStack> {
    Ok(parse_stack_document(yaml)?.stack)
}

pub fn parse_stack_document(yaml: &str) -> Result<ParsedStackDocument> {
    let mut raw: RawStack = serde_yaml::from_str(yaml).context("invalid stack YAML")?;
    if raw.services.is_empty() {
        bail!("stack must contain at least one service");
    }

    if let Some(name) = raw.swarmlite.name.as_deref() {
        crate::validate_stack_name(name).context("invalid x-swarmlite.name")?;
    }

    let configs = raw
        .configs
        .into_iter()
        .map(|(name, source)| normalize_config_source(&name, source).map(|source| (name, source)))
        .collect::<Result<BTreeMap<_, _>>>()?;
    let services: BTreeMap<String, ServiceSpec> = raw
        .services
        .into_iter()
        .map(|(name, service)| normalize_service(&name, service).map(|spec| (name, spec)))
        .collect::<Result<_>>()?;
    validate_service_configs(&services, &configs)?;
    let name = raw.swarmlite.name.take();
    let registries = std::mem::take(&mut raw.swarmlite.registries)
        .into_iter()
        .map(|(registry, credential)| {
            normalize_registry_credential(&registry, credential)
                .map(|credential| (registry, credential))
        })
        .collect::<Result<_>>()?;
    let mut gateway = raw.swarmlite.gateway();
    crate::validate_and_normalize(&mut gateway, &services)
        .context("invalid x-swarmlite configuration")?;
    Ok(ParsedStackDocument {
        name,
        stack: ParsedStack { services, gateway },
        configs,
        registries,
    })
}

fn normalize_registry_credential(
    registry: &str,
    raw: RawRegistryCredential,
) -> Result<StackRegistryCredential> {
    if registry.is_empty() || registry.trim() != registry {
        bail!("x-swarmlite.registries contains an empty or whitespace-padded registry name");
    }
    if raw.username.is_empty() {
        bail!("x-swarmlite.registries[{registry:?}].username must not be empty");
    }
    if raw.password.is_empty() {
        bail!("x-swarmlite.registries[{registry:?}].password must not be empty");
    }
    Ok(StackRegistryCredential {
        username: raw.username,
        password: raw.password,
    })
}

fn normalize_service(name: &str, raw: RawService) -> Result<ServiceSpec> {
    let image = raw
        .image
        .filter(|value| !value.trim().is_empty())
        .with_context(|| format!("service {name} must define image"))?;

    if raw
        .deploy
        .mode
        .as_deref()
        .is_some_and(|mode| mode != "replicated")
    {
        bail!("service {name}: only deploy.mode=replicated is currently supported");
    }

    let order = raw
        .deploy
        .update_config
        .order
        .as_deref()
        .unwrap_or("start-first");
    if order != "start-first" && order != "stop-first" {
        bail!("service {name}: deploy.update_config.order must be start-first or stop-first");
    }
    // The first implementation always protects availability. Swarm's parallelism maps
    // naturally to the number of temporary surge tasks created during start-first updates.
    let max_surge = if order == "start-first" {
        raw.deploy.update_config.parallelism.unwrap_or(1).max(1)
    } else {
        0
    };

    let expose = raw
        .expose
        .into_iter()
        .map(normalize_expose)
        .collect::<Result<Vec<_>>>()?;
    let ports = raw
        .ports
        .into_iter()
        .map(normalize_port)
        .collect::<Result<Vec<_>>>()?;
    let volumes = raw
        .volumes
        .into_iter()
        .map(normalize_volume)
        .collect::<Result<Vec<_>>>()?;
    let configs = raw
        .configs
        .into_iter()
        .map(|value| normalize_config_mount(name, value))
        .collect::<Result<Vec<_>>>()?;
    let stop_grace_period_seconds = raw
        .stop_grace_period
        .as_deref()
        .map(humantime::parse_duration)
        .transpose()
        .with_context(|| format!("service {name}: invalid stop_grace_period"))?
        .unwrap_or(Duration::from_secs(10))
        .as_secs();

    let spec = ServiceSpec {
        image,
        pull_policy: raw.pull_policy,
        command: raw
            .command
            .map(|value| value.into_vec("command"))
            .transpose()?
            .unwrap_or_default(),
        entrypoint: raw
            .entrypoint
            .map(|value| value.into_vec("entrypoint"))
            .transpose()?
            .unwrap_or_default(),
        environment: raw.environment.into_environment()?,
        expose,
        ports,
        volumes,
        configs,
        container_labels: raw.labels.into_map()?,
        service_labels: raw.deploy.labels.into_map()?,
        healthcheck: raw
            .healthcheck
            .map(|healthcheck| normalize_healthcheck(name, healthcheck))
            .transpose()?,
        replicas: raw.deploy.replicas.unwrap_or(1),
        constraints: raw.deploy.placement.constraints,
        max_surge,
        stop_grace_period_seconds,
    };
    Ok(spec)
}

fn normalize_config_source(name: &str, raw: RawConfigSource) -> Result<StackConfigSource> {
    validate_config_name(name)?;
    if raw.file.trim().is_empty() {
        bail!("config {name}: file must not be empty");
    }
    Ok(StackConfigSource { file: raw.file })
}

fn normalize_config_mount(service: &str, value: ConfigValue) -> Result<ServiceConfigMount> {
    let (source, target, uid, gid, mode) = match value {
        ConfigValue::Short(source) => {
            let target = format!("/{source}");
            (source, target, None, None, 0o444)
        }
        ConfigValue::Long(config) => {
            let target = config
                .target
                .unwrap_or_else(|| format!("/{}", config.source));
            let uid = parse_config_owner(service, "uid", config.uid)?;
            let gid = parse_config_owner(service, "gid", config.gid)?;
            let mode = parse_config_mode(service, config.mode)?;
            (config.source, target, uid, gid, mode)
        }
    };
    validate_config_name(&source)
        .with_context(|| format!("service {service}: invalid config source"))?;
    validate_config_target(service, &target)?;
    Ok(ServiceConfigMount {
        source,
        target,
        uid,
        gid,
        // Compose Configs are immutable; writable bits must be ignored.
        mode: mode & !0o222,
        digest: String::new(),
    })
}

fn parse_config_owner(service: &str, field: &str, value: Option<String>) -> Result<Option<u32>> {
    value
        .map(|value| {
            value
                .parse::<u32>()
                .with_context(|| format!("service {service}: config {field} must be a numeric ID"))
        })
        .transpose()
}

fn parse_config_mode(service: &str, value: Option<Value>) -> Result<u32> {
    let Some(value) = value else {
        return Ok(0o444);
    };
    let invalid = || {
        anyhow::anyhow!(
            "service {service}: config mode must be octal permissions between 0000 and 7777"
        )
    };
    let mode = match value {
        Value::String(value) => {
            let value = value
                .strip_prefix("0o")
                .or_else(|| value.strip_prefix("0O"))
                .unwrap_or(&value);
            u32::from_str_radix(value, 8).map_err(|_| invalid())?
        }
        Value::Number(value) => {
            let value = value.as_u64().ok_or_else(invalid)?;
            let digits = value.to_string();
            if digits.bytes().all(|digit| matches!(digit, b'0'..=b'7')) {
                u32::from_str_radix(&digits, 8).map_err(|_| invalid())?
            } else {
                u32::try_from(value).map_err(|_| invalid())?
            }
        }
        _ => return Err(invalid()),
    };
    if mode > 0o7777 {
        return Err(invalid());
    }
    Ok(mode)
}

fn validate_config_name(name: &str) -> Result<()> {
    if name.is_empty()
        || !name
            .chars()
            .all(|value| value.is_ascii_alphanumeric() || matches!(value, '-' | '_' | '.'))
    {
        bail!("config name may contain only letters, numbers, '.', '-' and '_'");
    }
    Ok(())
}

fn validate_config_target(service: &str, target: &str) -> Result<()> {
    let path = std::path::Path::new(target);
    if target == "/"
        || !path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        bail!(
            "service {service}: config target must be an absolute container file path without '..': {target}"
        );
    }
    Ok(())
}

fn validate_service_configs(
    services: &BTreeMap<String, ServiceSpec>,
    configs: &BTreeMap<String, StackConfigSource>,
) -> Result<()> {
    for (service_name, service) in services {
        let mut targets = BTreeSet::new();
        for config in &service.configs {
            if !configs.contains_key(&config.source) {
                bail!(
                    "service {service_name}: config {:?} is not defined in the top-level configs",
                    config.source
                );
            }
            if !targets.insert(config.target.as_str()) {
                bail!(
                    "service {service_name}: duplicate config target {:?}",
                    config.target
                );
            }
            if service
                .volumes
                .iter()
                .any(|volume| volume_target(volume) == config.target)
            {
                bail!(
                    "service {service_name}: config and volume both target {:?}",
                    config.target
                );
            }
        }
    }
    Ok(())
}

fn volume_target(volume: &str) -> &str {
    let mut parts = volume.rsplit(':');
    let last = parts.next().unwrap_or(volume);
    if matches!(last, "ro" | "rw") {
        parts.next().unwrap_or(last)
    } else if volume.contains(':') {
        last
    } else {
        volume
    }
}

pub fn resolve_config_digests(
    stack: &mut ParsedStack,
    digests: &BTreeMap<String, String>,
) -> Result<()> {
    for (service_name, service) in &mut stack.services {
        for config in &mut service.configs {
            let digest = digests.get(&config.source).with_context(|| {
                format!(
                    "service {service_name}: uploaded content for config {:?} is missing",
                    config.source
                )
            })?;
            if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                bail!(
                    "service {service_name}: config {:?} has an invalid SHA-256 digest",
                    config.source
                );
            }
            config.digest = digest.to_ascii_lowercase();
        }
    }
    Ok(())
}

fn normalize_expose(value: ExposeValue) -> Result<ServicePort> {
    let value = match value {
        ExposeValue::Number(port) => port.to_string(),
        ExposeValue::String(value) => value,
    };
    let (target, protocol) = value
        .rsplit_once('/')
        .map_or((value.as_str(), "tcp"), |(target, protocol)| {
            (target, protocol)
        });
    if protocol != "tcp" && protocol != "udp" {
        bail!("unsupported expose protocol in {value}");
    }
    if target.contains(':') {
        bail!("expose accepts only container target ports: {value}");
    }
    Ok(ServicePort {
        target: parse_port_number(target, &value)?,
        published: None,
        protocol: protocol.to_owned(),
    })
}

fn normalize_healthcheck(name: &str, raw: RawHealthcheck) -> Result<HealthcheckSpec> {
    if raw.retries.is_some_and(|retries| retries < 0) {
        bail!("service {name}: healthcheck.retries must be non-negative");
    }
    let test = if raw.disable {
        vec!["NONE".to_owned()]
    } else {
        match raw.test {
            Some(StringOrList::List(test)) => test,
            Some(StringOrList::String(command)) => vec!["CMD-SHELL".to_owned(), command],
            None => Vec::new(),
        }
    };
    Ok(HealthcheckSpec {
        test,
        interval_nanos: parse_optional_duration(name, "healthcheck.interval", raw.interval)?,
        timeout_nanos: parse_optional_duration(name, "healthcheck.timeout", raw.timeout)?,
        retries: raw.retries,
        start_period_nanos: parse_optional_duration(
            name,
            "healthcheck.start_period",
            raw.start_period,
        )?,
        start_interval_nanos: parse_optional_duration(
            name,
            "healthcheck.start_interval",
            raw.start_interval,
        )?,
    })
}

fn parse_optional_duration(name: &str, field: &str, value: Option<String>) -> Result<Option<i64>> {
    value
        .map(|value| {
            let duration = humantime::parse_duration(&value)
                .with_context(|| format!("service {name}: invalid {field}"))?;
            i64::try_from(duration.as_nanos())
                .with_context(|| format!("service {name}: {field} is too large"))
        })
        .transpose()
}

fn normalize_port(value: PortValue) -> Result<ServicePort> {
    match value {
        PortValue::Number(target) => {
            ensure_nonzero_port(target, &target.to_string())?;
            Ok(ServicePort {
                target,
                published: None,
                protocol: "tcp".to_owned(),
            })
        }
        PortValue::Long(port) => {
            ensure_nonzero_port(port.target, "long port target")?;
            if port.published.is_some() {
                bail!(
                    "ports.published is not supported; omit it to let Docker allocate the host port"
                );
            }
            let protocol = port.protocol.unwrap_or_else(|| "tcp".to_owned());
            if protocol != "tcp" && protocol != "udp" {
                bail!("unsupported long port protocol {protocol}");
            }
            Ok(ServicePort {
                target: port.target,
                published: None,
                protocol,
            })
        }
        PortValue::Short(value) => parse_short_port(&value),
    }
}

fn parse_short_port(value: &str) -> Result<ServicePort> {
    let (address, protocol) = value
        .rsplit_once('/')
        .map_or((value, "tcp"), |(address, protocol)| (address, protocol));
    if protocol != "tcp" && protocol != "udp" {
        bail!("unsupported port protocol in {value}");
    }
    let parts: Vec<&str> = address.rsplit(':').collect();
    let target = match parts.as_slice() {
        [target] => parse_port_number(target, value)?,
        [_, _] => bail!(
            "published ports are not supported in {value}; use target[/protocol] and let Docker allocate the host port"
        ),
        _ => bail!("invalid port mapping {value}"),
    };
    Ok(ServicePort {
        target,
        published: None,
        protocol: protocol.to_owned(),
    })
}

fn parse_port_number(value: &str, original: &str) -> Result<u16> {
    if value.contains('-') {
        bail!("port ranges are not supported yet: {original}");
    }
    let port = value
        .parse()
        .with_context(|| format!("invalid port mapping {original}"))?;
    ensure_nonzero_port(port, original)?;
    Ok(port)
}

fn ensure_nonzero_port(port: u16, original: &str) -> Result<()> {
    if port == 0 {
        bail!("port must be between 1 and 65535: {original}");
    }
    Ok(())
}

fn normalize_volume(value: VolumeValue) -> Result<String> {
    match value {
        VolumeValue::Short(value) => {
            if value.is_empty() {
                bail!("volume short syntax cannot be empty");
            }
            Ok(value)
        }
        VolumeValue::Long(volume) => {
            if volume.target.is_empty() {
                bail!("volume target cannot be empty");
            }
            if volume.source.as_deref().is_some_and(str::is_empty) {
                bail!("volume source cannot be empty");
            }
            let mut result = match volume.source {
                Some(source) => format!("{source}:{}", volume.target),
                None => volume.target,
            };
            if volume.read_only {
                result.push_str(":ro");
            }
            Ok(result)
        }
    }
}

fn scalar_to_string(value: Value) -> Result<String> {
    match value {
        Value::Null => Ok(String::new()),
        Value::Bool(value) => Ok(value.to_string()),
        Value::Number(value) => Ok(value.to_string()),
        Value::String(value) => Ok(value),
        other => bail!("expected a scalar value, got {other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_all_checked_in_stack_examples_without_a_version_field() {
        for yaml in [
            include_str!("../../../examples/stack.yaml"),
            include_str!("../../../examples/stack-standalone.yaml"),
            include_str!("../../../examples/routing-all.yaml"),
            include_str!("../../../examples/services-all.yaml"),
            include_str!("../../../examples/configs.yaml"),
        ] {
            assert!(!yaml.lines().any(|line| line.starts_with("version:")));
            parse_stack(yaml).unwrap();
        }
    }

    #[test]
    fn parses_and_validates_the_optional_stack_name() {
        let document = parse_stack_document(
            r#"
services:
  web:
    image: nginx
x-swarmlite:
  name: demo.production
"#,
        )
        .unwrap();
        assert_eq!(document.name.as_deref(), Some("demo.production"));
        assert!(document.stack.services.contains_key("web"));

        let error = parse_stack_document(
            r#"
services:
  web:
    image: nginx
x-swarmlite:
  name: "invalid name"
"#,
        )
        .unwrap_err();
        assert!(format!("{error:#}").contains("invalid x-swarmlite.name"));
    }

    #[test]
    fn parses_registry_credentials_without_exposing_passwords_in_debug_output() {
        let document = parse_stack_document(
            r#"
services:
  web:
    image: ghcr.io/example/private:latest
x-swarmlite:
  registries:
    ghcr.io:
      username: octocat
      password: private-token
"#,
        )
        .unwrap();

        assert_eq!(document.registries["ghcr.io"].username, "octocat");
        assert_eq!(document.registries["ghcr.io"].password, "private-token");
        let debug = format!("{document:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("private-token"));
    }

    #[test]
    fn rejects_empty_or_malformed_registry_credentials() {
        for (field, value) in [
            ("username", "username: ''\n      password: token"),
            ("password", "username: user\n      password: ''"),
            (
                "email",
                "username: user\n      password: token\n      email: user@example.com",
            ),
        ] {
            let yaml = format!(
                "services:\n  web:\n    image: nginx\nx-swarmlite:\n  registries:\n    ghcr.io:\n      {value}\n"
            );
            let error = parse_stack_document(&yaml).unwrap_err();
            assert!(format!("{error:#}").contains(field), "{error:#}");
        }
    }

    #[test]
    fn parses_swarm_style_stack() {
        let stack = parse_stack(
            r#"
services:
  web:
    image: nginx:1.29-alpine
    pull_policy: always
    command: ["--name", "demo"]
    environment:
      MODE: production
      DEBUG: false
    ports:
      - "80"
    volumes:
      - data:/data
    stop_grace_period: 20s
    healthcheck:
      test: ["CMD", "wget", "-qO-", "http://127.0.0.1/"]
      interval: 5s
      timeout: 2s
      retries: 3
    deploy:
      replicas: 3
      placement:
        constraints:
          - node.labels.role==app
      update_config:
        parallelism: 2
        order: start-first
x-swarmlite:
  http_routes:
    - hostnames: [example.com]
      rules:
        - matches:
            - path: /api
              ignore_case: true
          rewrite:
            strip_prefix: true
          backend:
            service: web
"#,
        )
        .unwrap();

        let web = &stack.services["web"];
        assert_eq!(web.pull_policy, PullPolicy::Always);
        assert_eq!(web.replicas, 3);
        assert_eq!(web.max_surge, 2);
        assert_eq!(web.stop_grace_period_seconds, 20);
        assert_eq!(
            web.healthcheck.as_ref().unwrap().interval_nanos,
            Some(5_000_000_000)
        );
        assert_eq!(web.ports[0].published, None);
        assert_eq!(web.environment, ["DEBUG=false", "MODE=production"]);
        let rule = &stack.gateway.http_routes[0].rules[0];
        assert_eq!(rule.backend.service.as_deref(), Some("web"));
        assert_eq!(rule.backend.port, 80);
        assert_eq!(rule.matches[0].path, "/api");
    }

    #[test]
    fn parses_compose_config_short_and_long_syntax() {
        let mut document = parse_stack_document(
            r#"
services:
  web:
    image: nginx
    configs:
      - default-config
      - source: executable-config
        target: /usr/local/bin/configure
        uid: "103"
        gid: "104"
        mode: 0555
configs:
  default-config:
    file: ./default.conf
  executable-config:
    file: ./configure
"#,
        )
        .unwrap();

        assert_eq!(document.configs["default-config"].file, "./default.conf");
        let configs = &document.stack.services["web"].configs;
        assert_eq!(configs[0].source, "default-config");
        assert_eq!(configs[0].target, "/default-config");
        assert_eq!(configs[0].mode, 0o444);
        assert_eq!(configs[1].target, "/usr/local/bin/configure");
        assert_eq!(configs[1].uid, Some(103));
        assert_eq!(configs[1].gid, Some(104));
        assert_eq!(configs[1].mode, 0o555);
        assert!(configs.iter().all(|config| config.digest.is_empty()));

        resolve_config_digests(
            &mut document.stack,
            &BTreeMap::from([
                ("default-config".into(), "a".repeat(64)),
                ("executable-config".into(), "b".repeat(64)),
            ]),
        )
        .unwrap();
        assert_eq!(
            document.stack.services["web"].configs[0].digest,
            "a".repeat(64)
        );
    }

    #[test]
    fn rejects_invalid_compose_config_references_and_targets() {
        for (yaml, expected) in [
            (
                r#"
services:
  web:
    image: nginx
    configs: [missing]
"#,
                "is not defined in the top-level configs",
            ),
            (
                r#"
services:
  web:
    image: nginx
    configs:
      - source: app-config
        target: relative.conf
configs:
  app-config:
    file: ./app.conf
"#,
                "must be an absolute container file path",
            ),
            (
                r#"
services:
  web:
    image: nginx
    volumes: [data:/etc/app.conf]
    configs:
      - source: app-config
        target: /etc/app.conf
configs:
  app-config:
    file: ./app.conf
"#,
                "config and volume both target",
            ),
        ] {
            let error = parse_stack_document(yaml).unwrap_err();
            assert!(
                format!("{error:#}").contains(expected),
                "unexpected error: {error:#}"
            );
        }
    }

    #[test]
    fn config_content_digest_participates_in_the_service_spec_hash() {
        let yaml = r#"
services:
  web:
    image: nginx
    configs: [index-html]
configs:
  index-html:
    file: ./index.html
"#;
        let mut first = parse_stack_document(yaml).unwrap().stack;
        let mut second = first.clone();
        resolve_config_digests(
            &mut first,
            &BTreeMap::from([("index-html".into(), "a".repeat(64))]),
        )
        .unwrap();
        resolve_config_digests(
            &mut second,
            &BTreeMap::from([("index-html".into(), "b".repeat(64))]),
        )
        .unwrap();

        assert_ne!(
            crate::service_spec_hash(&first.services["web"]),
            crate::service_spec_hash(&second.services["web"])
        );
    }

    #[test]
    fn parses_expose_and_infers_the_only_tcp_backend_port() {
        let stack = parse_stack(
            r#"
services:
  api:
    image: example/api
    expose:
      - 8080
      - "5353/udp"
x-swarmlite:
  http_routes:
    - hostnames: [api.example.com]
      rules:
        - backend:
            service: api
"#,
        )
        .unwrap();

        assert_eq!(stack.services["api"].expose.len(), 2);
        assert_eq!(stack.services["api"].expose[0].target, 8080);
        assert_eq!(stack.services["api"].expose[1].protocol, "udp");
        assert_eq!(stack.gateway.http_routes[0].rules[0].backend.port, 8080);
    }

    #[test]
    fn requires_a_backend_port_when_the_service_has_zero_or_multiple_tcp_targets() {
        for (expose, expected) in [
            ("", "declares no TCP target ports"),
            (
                "    expose: [8080, 9090]\n",
                "multiple TCP target ports: 8080, 9090",
            ),
        ] {
            let yaml = format!(
                r#"
services:
  api:
    image: example/api
{expose}x-swarmlite:
  http_routes:
    - hostnames: [api.example.com]
      rules:
        - backend:
            service: api
"#
            );
            let error = parse_stack(&yaml).unwrap_err();
            assert!(format!("{error:#}").contains(expected));
        }
    }

    #[test]
    fn requires_an_explicit_service_backend_port_to_be_declared() {
        let error = parse_stack(
            r#"
services:
  api:
    image: example/api
    expose: [8080]
x-swarmlite:
  http_routes:
    - hostnames: [api.example.com]
      rules:
        - backend:
            service: api
            port: 9090
"#,
        )
        .unwrap_err();
        assert!(
            format!("{error:#}")
                .contains("does not declare TCP target port 9090 in expose or ports")
        );
    }

    #[test]
    fn requires_external_host_backends_to_keep_an_explicit_port() {
        let error = parse_stack(
            r#"
services:
  api:
    image: example/api
x-swarmlite:
  http_routes:
    - hostnames: [api.example.com]
      rules:
        - backend:
            host: upstream.example.com
"#,
        )
        .unwrap_err();
        assert!(format!("{error:#}").contains("port is required for a host backend"));
    }

    #[test]
    fn rejects_an_explicit_zero_backend_port_instead_of_treating_it_as_omitted() {
        let error = parse_stack(
            r#"
services:
  api:
    image: example/api
    expose: [8080]
x-swarmlite:
  http_routes:
    - hostnames: [api.example.com]
      rules:
        - backend:
            service: api
            port: 0
"#,
        )
        .unwrap_err();
        assert!(format!("{error:#}").contains("port must be between 1 and 65535"));
    }

    #[test]
    fn defaults_pull_policy_to_missing_and_accepts_compatibility_alias() {
        let default = parse_stack(
            r#"
services:
  web:
    image: nginx
"#,
        )
        .unwrap();
        assert_eq!(default.services["web"].pull_policy, PullPolicy::Missing);

        let alias = parse_stack(
            r#"
services:
  web:
    image: nginx
    pull_policy: if_not_present
"#,
        )
        .unwrap();
        assert_eq!(alias.services["web"].pull_policy, PullPolicy::Missing);
    }

    #[test]
    fn rejects_unsupported_pull_policy() {
        let error = parse_stack(
            r#"
services:
  web:
    image: nginx
    pull_policy: daily
"#,
        )
        .unwrap_err();
        assert!(format!("{error:#}").contains("pull_policy"));
    }

    #[test]
    fn rejects_global_mode() {
        let error = parse_stack(
            r#"
services:
  web:
    image: nginx
    deploy:
      mode: global
"#,
        )
        .unwrap_err();
        assert!(error.to_string().contains("only deploy.mode=replicated"));
    }

    #[test]
    fn rejects_unknown_service_fields_including_compose_compatibility_fields() {
        for yaml in [
            r#"
services:
  web:
    image: nginx
    restart: always
"#,
            r#"
services:
  web:
    image: nginx
    ports:
      - target: 80
        mode: host
"#,
            r#"
services:
  web:
    image: nginx
    volumes:
      - type: bind
        source: /srv/web
        target: /run
"#,
        ] {
            assert!(parse_stack(yaml).is_err(), "expected rejection for {yaml}");
        }
    }

    #[test]
    fn rejects_fixed_published_ports() {
        for port in ["\"8080:80\"", "{ target: 80, published: 8080 }"] {
            let yaml = format!("services:\n  web:\n    image: nginx\n    ports:\n      - {port}\n");
            let error = parse_stack(&yaml).unwrap_err();
            assert!(
                format!("{error:#}").contains("not supported"),
                "unexpected error: {error:#}"
            );
        }
    }

    #[test]
    fn rejects_unknown_gateway_service() {
        let error = parse_stack(
            r#"
services:
  web:
    image: nginx
x-swarmlite:
  http_routes:
    - hostnames: [example.com]
      rules:
        - backend:
            service: missing
            port: 80
"#,
        )
        .unwrap_err();
        assert!(format!("{error:#}").contains("service \"missing\" does not exist"));
    }
}
