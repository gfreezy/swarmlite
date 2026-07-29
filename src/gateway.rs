use serde::Serialize;

use crate::model::{ClusterState, DesiredTaskState, ObservedTaskState, ServiceRecord, ServiceSpec};

pub const ENABLE_LABEL: &str = "swarmlite.gateway.enable";
pub const HOST_LABEL: &str = "swarmlite.gateway.host";
pub const PORT_LABEL: &str = "swarmlite.gateway.port";
pub const SCHEME_LABEL: &str = "swarmlite.gateway.scheme";

#[derive(Debug, Clone, Serialize)]
pub struct HttpServer {
    pub listen: Vec<String>,
    pub routes: Vec<Route>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StorageConfig {
    pub module: &'static str,
    pub controllers: Vec<String>,
    pub token_env: &'static str,
    pub timeout: &'static str,
    pub lock_lease: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct Route {
    #[serde(rename = "@id")]
    pub id: String,
    #[serde(rename = "match")]
    pub matchers: Vec<RequestMatcher>,
    pub handle: Vec<ReverseProxyHandler>,
    pub terminal: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct RequestMatcher {
    pub host: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReverseProxyHandler {
    pub handler: &'static str,
    pub upstreams: Vec<Upstream>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transport: Option<HttpTransport>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Upstream {
    pub dial: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct HttpTransport {
    pub protocol: &'static str,
    pub tls: EmptyObject,
}

#[derive(Debug, Clone, Serialize)]
pub struct EmptyObject {}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GatewaySpec {
    hosts: Vec<String>,
    target_port: u16,
    scheme: String,
}

pub fn generate(state: &ClusterState, listen: &[String]) -> HttpServer {
    let routes = state
        .services
        .values()
        .filter(|service| !service.deleted)
        .filter_map(|service| build_route(state, service))
        .collect();
    HttpServer {
        listen: listen.to_vec(),
        routes,
    }
}

pub fn storage(controllers: Vec<String>) -> StorageConfig {
    StorageConfig {
        module: "swarmlite",
        controllers,
        token_env: "SWARMLITE_TOKEN",
        timeout: "500ms",
        lock_lease: "30s",
    }
}

pub fn is_enabled(spec: &ServiceSpec) -> bool {
    spec.service_labels
        .get(ENABLE_LABEL)
        .is_some_and(|value| value.eq_ignore_ascii_case("true"))
}

pub fn target_port(spec: &ServiceSpec) -> Option<u16> {
    if !is_enabled(spec) {
        return None;
    }
    spec.service_labels
        .get(PORT_LABEL)
        .and_then(|value| value.parse().ok())
        .or_else(|| {
            spec.ports
                .iter()
                .find(|port| port.protocol == "tcp")
                .map(|port| port.target)
        })
}

pub fn validate_service(name: &str, spec: &ServiceSpec) -> Result<(), String> {
    if !is_enabled(spec) {
        return Ok(());
    }
    parse_gateway(spec)
        .map(|_| ())
        .map_err(|error| format!("service {name}: {error}"))
}

fn build_route(state: &ClusterState, service: &ServiceRecord) -> Option<Route> {
    let gateway = parse_gateway(&service.spec).ok()?;
    let upstreams = state
        .tasks
        .values()
        .filter(|task| {
            task.service_id == service.id
                && task.desired == DesiredTaskState::Running
                && task.observed == ObservedTaskState::Healthy
        })
        .filter_map(|task| {
            let node = state.nodes.get(&task.node_id)?;
            let port = task
                .ports
                .iter()
                .find(|port| port.target == gateway.target_port && port.protocol == "tcp")?;
            Some(Upstream {
                dial: format!("{}:{}", format_host(&node.address), port.published),
            })
        })
        .collect();
    let transport = (gateway.scheme == "https").then_some(HttpTransport {
        protocol: "http",
        tls: EmptyObject {},
    });
    Some(Route {
        id: format!("swarmlite-{}", sanitize_id(&service.id)),
        matchers: vec![RequestMatcher {
            host: gateway.hosts,
        }],
        handle: vec![ReverseProxyHandler {
            handler: "reverse_proxy",
            upstreams,
            transport,
        }],
        terminal: true,
    })
}

fn parse_gateway(spec: &ServiceSpec) -> Result<GatewaySpec, String> {
    let hosts = spec
        .service_labels
        .get(HOST_LABEL)
        .map(|value| split_csv(value))
        .filter(|hosts| !hosts.is_empty())
        .ok_or_else(|| format!("{HOST_LABEL} must contain at least one host"))?;
    let target_port = match spec.service_labels.get(PORT_LABEL) {
        Some(value) => value
            .parse::<u16>()
            .map_err(|_| format!("{PORT_LABEL} must be a valid port number"))?,
        None => spec
            .ports
            .iter()
            .find(|port| port.protocol == "tcp")
            .map(|port| port.target)
            .ok_or_else(|| {
                format!(
                    "{PORT_LABEL} or a TCP service port is required when gateway routing is enabled"
                )
            })?,
    };
    let scheme = spec
        .service_labels
        .get(SCHEME_LABEL)
        .map(|value| value.to_ascii_lowercase())
        .unwrap_or_else(|| "http".to_owned());
    if scheme != "http" && scheme != "https" {
        return Err(format!("{SCHEME_LABEL} must be either http or https"));
    }
    Ok(GatewaySpec {
        hosts,
        target_port,
        scheme,
    })
}

fn split_csv(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect()
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
    use std::collections::BTreeMap;

    use crate::model::{NodeRecord, PortBinding, ServicePort, TaskRecord, agent_roles};

    use super::*;

    #[test]
    fn generates_routes_from_healthy_running_tasks_across_revisions() {
        let mut state = ClusterState::default();
        state
            .nodes
            .insert("node-a".into(), node("node-a", "10.0.0.21"));
        state
            .nodes
            .insert("node-b".into(), node("node-b", "2001:db8::2"));
        state.services.insert("demo.web".into(), service());
        state.tasks.insert(
            "old".into(),
            task("old", 1, "node-a", DesiredTaskState::Running, 20_001),
        );
        state.tasks.insert(
            "new".into(),
            task("new", 2, "node-b", DesiredTaskState::Running, 20_002),
        );
        state.tasks.insert(
            "draining".into(),
            task("draining", 1, "node-a", DesiredTaskState::Draining, 20_003),
        );

        let server = generate(&state, &[":80".into()]);
        assert_eq!(server.routes.len(), 1);
        let route = &server.routes[0];
        assert_eq!(route.matchers[0].host, ["example.com", "www.example.com"]);
        let mut upstreams = route.handle[0]
            .upstreams
            .iter()
            .map(|upstream| upstream.dial.as_str())
            .collect::<Vec<_>>();
        upstreams.sort_unstable();
        assert_eq!(upstreams, ["10.0.0.21:20001", "[2001:db8::2]:20002"]);
    }

    #[test]
    fn validates_required_gateway_labels() {
        let mut service = service().spec;
        service.service_labels.remove(HOST_LABEL);
        let error = validate_service("web", &service).unwrap_err();
        assert!(error.contains(HOST_LABEL));
    }

    #[test]
    fn configures_tls_for_https_upstreams() {
        let mut state = ClusterState::default();
        state
            .nodes
            .insert("node-a".into(), node("node-a", "10.0.0.21"));
        let mut service = service();
        service
            .spec
            .service_labels
            .insert(SCHEME_LABEL.into(), "https".into());
        state.services.insert(service.id.clone(), service);
        state.tasks.insert(
            "task".into(),
            task("task", 2, "node-a", DesiredTaskState::Running, 20_001),
        );
        let server = generate(&state, &[":443".into()]);
        assert!(server.routes[0].handle[0].transport.is_some());
    }

    fn service() -> ServiceRecord {
        ServiceRecord {
            id: "demo.web".into(),
            stack: "demo".into(),
            name: "web".into(),
            revision: 2,
            spec: ServiceSpec {
                image: "example/web:v2".into(),
                command: Vec::new(),
                entrypoint: Vec::new(),
                environment: Vec::new(),
                ports: vec![ServicePort {
                    target: 80,
                    published: None,
                    protocol: "tcp".into(),
                }],
                volumes: Vec::new(),
                container_labels: BTreeMap::new(),
                service_labels: BTreeMap::from([
                    (ENABLE_LABEL.into(), "true".into()),
                    (HOST_LABEL.into(), "example.com,www.example.com".into()),
                    (PORT_LABEL.into(), "80".into()),
                ]),
                healthcheck: None,
                replicas: 2,
                constraints: Vec::new(),
                max_surge: 1,
                stop_grace_period_seconds: 10,
            },
            deleted: false,
        }
    }

    fn node(id: &str, address: &str) -> NodeRecord {
        NodeRecord {
            id: id.into(),
            address: address.into(),
            labels: BTreeMap::new(),
            cpu_millis: 1000,
            memory_bytes: 1024,
            port_range_start: 20_000,
            port_range_end: 29_999,
            roles: agent_roles(),
            controller_url: String::new(),
            raft_id: 1,
            raft_url: String::new(),
            controller_set_generation: 0,
        }
    }

    fn task(
        id: &str,
        revision: u64,
        node_id: &str,
        desired: DesiredTaskState,
        published: u16,
    ) -> TaskRecord {
        TaskRecord {
            id: id.into(),
            service_id: "demo.web".into(),
            revision,
            slot: 0,
            node_id: node_id.into(),
            desired,
            observed: ObservedTaskState::Healthy,
            ports: vec![PortBinding {
                target: 80,
                published,
                protocol: "tcp".into(),
            }],
            container_id: None,
            drain_until_unix_ms: None,
        }
    }
}
