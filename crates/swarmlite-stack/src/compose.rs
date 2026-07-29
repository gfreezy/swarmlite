use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use serde_yaml::Value;

use crate::{HealthcheckSpec, ServicePort, ServiceSpec, StackGatewaySpec};

#[derive(Debug, Clone)]
pub struct ParsedStack {
    pub services: BTreeMap<String, ServiceSpec>,
    pub gateway: StackGatewaySpec,
}

#[derive(Debug, Deserialize)]
struct RawStack {
    services: BTreeMap<String, RawService>,
    #[serde(rename = "x-swarmlite", default)]
    gateway: StackGatewaySpec,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawService {
    image: Option<String>,
    command: Option<StringOrList>,
    entrypoint: Option<StringOrList>,
    #[serde(default)]
    environment: StringMapOrList,
    #[serde(default)]
    labels: StringMapOrList,
    #[serde(default)]
    ports: Vec<PortValue>,
    #[serde(default)]
    volumes: Vec<VolumeValue>,
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

pub fn parse_stack(yaml: &str) -> Result<ParsedStack> {
    let mut raw: RawStack = serde_yaml::from_str(yaml).context("invalid stack YAML")?;
    if raw.services.is_empty() {
        bail!("stack must contain at least one service");
    }

    let services: BTreeMap<String, ServiceSpec> = raw
        .services
        .into_iter()
        .map(|(name, service)| normalize_service(&name, service).map(|spec| (name, spec)))
        .collect::<Result<_>>()?;
    crate::validate_and_normalize(
        &mut raw.gateway,
        &services.keys().cloned().collect::<BTreeSet<_>>(),
    )
    .context("invalid x-swarmlite configuration")?;
    Ok(ParsedStack {
        services,
        gateway: raw.gateway,
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
        ports,
        volumes,
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
            if let Some(published) = port.published {
                ensure_nonzero_port(published, "long port published")?;
            }
            let protocol = port.protocol.unwrap_or_else(|| "tcp".to_owned());
            if protocol != "tcp" && protocol != "udp" {
                bail!("unsupported long port protocol {protocol}");
            }
            Ok(ServicePort {
                target: port.target,
                published: port.published,
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
    let (target, published) = match parts.as_slice() {
        [target] => (parse_port_number(target, value)?, None),
        [target, published] => (
            parse_port_number(target, value)?,
            Some(parse_port_number(published, value)?),
        ),
        _ => bail!("invalid port mapping {value}"),
    };
    Ok(ServicePort {
        target,
        published,
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
        ] {
            assert!(!yaml.lines().any(|line| line.starts_with("version:")));
            parse_stack(yaml).unwrap();
        }
    }

    #[test]
    fn parses_swarm_style_stack() {
        let stack = parse_stack(
            r#"
services:
  web:
    image: nginx:1.29-alpine
    command: ["--name", "demo"]
    environment:
      MODE: production
      DEBUG: false
    ports:
      - "8080:80"
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
            port: 80
"#,
        )
        .unwrap();

        let web = &stack.services["web"];
        assert_eq!(web.replicas, 3);
        assert_eq!(web.max_surge, 2);
        assert_eq!(web.stop_grace_period_seconds, 20);
        assert_eq!(
            web.healthcheck.as_ref().unwrap().interval_nanos,
            Some(5_000_000_000)
        );
        assert_eq!(web.ports[0].published, Some(8080));
        assert_eq!(web.environment, ["DEBUG=false", "MODE=production"]);
        let rule = &stack.gateway.http_routes[0].rules[0];
        assert_eq!(rule.backend.service.as_deref(), Some("web"));
        assert_eq!(rule.matches[0].path, "/api");
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
