use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;

use crate::{
    model::{ClusterState, DesiredTaskState, ObservedTaskState, ServiceRecord},
    scheduler::traefik_target_port,
};

#[derive(Debug, Clone, Serialize, Default)]
pub struct DynamicConfiguration {
    pub http: HttpConfiguration,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct HttpConfiguration {
    pub routers: BTreeMap<String, Router>,
    pub services: BTreeMap<String, LoadBalancerService>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Router {
    pub rule: String,
    pub service: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub entry_points: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub middlewares: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tls: Option<Tls>,
}

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Tls {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cert_resolver: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LoadBalancerService {
    pub load_balancer: LoadBalancer,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LoadBalancer {
    pub servers: Vec<Server>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pass_host_header: Option<bool>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Server {
    pub url: String,
}

#[derive(Debug, Default)]
struct RouterLabels {
    rule: Option<String>,
    service: Option<String>,
    entry_points: Vec<String>,
    middlewares: Vec<String>,
    priority: Option<u32>,
    tls: bool,
    cert_resolver: Option<String>,
}

pub fn generate(state: &ClusterState) -> DynamicConfiguration {
    let mut result = DynamicConfiguration::default();
    for service in state.services.values().filter(|service| !service.deleted) {
        if !is_enabled(service) {
            continue;
        }
        add_service_configuration(&mut result, state, service);
    }
    result
}

fn is_enabled(service: &ServiceRecord) -> bool {
    service
        .spec
        .service_labels
        .get("traefik.enable")
        .or_else(|| service.spec.container_labels.get("traefik.enable"))
        .is_some_and(|value| value.eq_ignore_ascii_case("true"))
}

fn add_service_configuration(
    output: &mut DynamicConfiguration,
    state: &ClusterState,
    service: &ServiceRecord,
) {
    let labels = &service.spec.service_labels;
    let routers = parse_routers(labels);
    let declared_services = declared_traefik_services(labels);
    let default_traefik_service = if declared_services.len() == 1 {
        declared_services.iter().next().cloned()
    } else {
        None
    };

    for (name, labels) in routers {
        let Some(rule) = labels.rule else {
            continue;
        };
        let backend = labels
            .service
            .or_else(|| default_traefik_service.clone())
            .unwrap_or_else(|| name.clone());
        output.http.routers.insert(
            name,
            Router {
                rule,
                service: backend.clone(),
                entry_points: labels.entry_points,
                middlewares: labels.middlewares,
                priority: labels.priority,
                tls: labels.tls.then_some(Tls {
                    cert_resolver: labels.cert_resolver,
                }),
            },
        );
        output
            .http
            .services
            .entry(backend.clone())
            .or_insert_with(|| build_backend(state, service, &backend));
    }

    for backend in declared_services {
        output
            .http
            .services
            .entry(backend.clone())
            .or_insert_with(|| build_backend(state, service, &backend));
    }
}

fn parse_routers(labels: &BTreeMap<String, String>) -> BTreeMap<String, RouterLabels> {
    let prefix = "traefik.http.routers.";
    let mut result: BTreeMap<String, RouterLabels> = BTreeMap::new();
    for (key, value) in labels {
        let Some(remainder) = key.strip_prefix(prefix) else {
            continue;
        };
        let Some((name, field)) = remainder.split_once('.') else {
            continue;
        };
        let router = result.entry(name.to_owned()).or_default();
        match field {
            "rule" => router.rule = Some(value.clone()),
            "service" => router.service = Some(value.clone()),
            "entrypoints" => router.entry_points = split_csv(value),
            "middlewares" => router.middlewares = split_csv(value),
            "priority" => router.priority = value.parse().ok(),
            "tls" => router.tls = value.eq_ignore_ascii_case("true"),
            "tls.certresolver" => {
                router.tls = true;
                router.cert_resolver = Some(value.clone());
            }
            _ => {}
        }
    }
    result
}

fn declared_traefik_services(labels: &BTreeMap<String, String>) -> BTreeSet<String> {
    labels
        .keys()
        .filter_map(|key| {
            key.strip_prefix("traefik.http.services.")
                .and_then(|value| value.split_once('.').map(|(name, _)| name.to_owned()))
        })
        .collect()
}

fn build_backend(
    state: &ClusterState,
    service: &ServiceRecord,
    traefik_service: &str,
) -> LoadBalancerService {
    let port_label = format!("traefik.http.services.{traefik_service}.loadbalancer.server.port");
    let scheme_label =
        format!("traefik.http.services.{traefik_service}.loadbalancer.server.scheme");
    let pass_host_label =
        format!("traefik.http.services.{traefik_service}.loadbalancer.passhostheader");
    let target = service
        .spec
        .service_labels
        .get(&port_label)
        .and_then(|value| value.parse::<u16>().ok())
        .or_else(|| traefik_target_port(&service.spec.service_labels))
        .or_else(|| service.spec.ports.first().map(|port| port.target));
    let scheme = service
        .spec
        .service_labels
        .get(&scheme_label)
        .map(String::as_str)
        .unwrap_or("http");
    let servers = target
        .map(|target| {
            state
                .tasks
                .values()
                .filter(|task| {
                    task.service_id == service.id
                        && task.revision == service.revision
                        && task.desired == DesiredTaskState::Running
                        && task.observed == ObservedTaskState::Healthy
                })
                .filter_map(|task| {
                    let node = state.nodes.get(&task.node_id)?;
                    let port = task.ports.iter().find(|port| port.target == target)?;
                    Some(Server {
                        url: format!(
                            "{scheme}://{}:{}",
                            format_host(&node.address),
                            port.published
                        ),
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    let pass_host_header = service
        .spec
        .service_labels
        .get(&pass_host_label)
        .and_then(|value| value.parse().ok());
    LoadBalancerService {
        load_balancer: LoadBalancer {
            servers,
            pass_host_header,
        },
    }
}

fn format_host(host: &str) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]")
    } else {
        host.to_owned()
    }
}

fn split_csv(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{NodeRecord, PortBinding, ServiceSpec, TaskRecord};

    #[test]
    fn builds_http_provider_configuration_from_swarm_labels() {
        let mut state = ClusterState::default();
        state.nodes.insert(
            "node-a".into(),
            NodeRecord {
                id: "node-a".into(),
                address: "10.0.0.2".into(),
                labels: BTreeMap::new(),
                cpu_millis: 1000,
                memory_bytes: 1024,
                port_range_start: 20_000,
                port_range_end: 29_999,
            },
        );
        let labels = BTreeMap::from([
            ("traefik.enable".into(), "true".into()),
            (
                "traefik.http.routers.web.rule".into(),
                "Host(`example.com`)".into(),
            ),
            (
                "traefik.http.routers.web.entrypoints".into(),
                "websecure".into(),
            ),
            ("traefik.http.routers.web.tls".into(), "true".into()),
            (
                "traefik.http.services.web.loadbalancer.server.port".into(),
                "8080".into(),
            ),
        ]);
        state.services.insert(
            "demo_web".into(),
            ServiceRecord {
                id: "demo_web".into(),
                stack: "demo".into(),
                name: "web".into(),
                revision: 1,
                deleted: false,
                spec: ServiceSpec {
                    image: "web:v1".into(),
                    command: vec![],
                    entrypoint: vec![],
                    environment: vec![],
                    ports: vec![],
                    volumes: vec![],
                    container_labels: BTreeMap::new(),
                    service_labels: labels,
                    healthcheck: None,
                    replicas: 1,
                    constraints: vec![],
                    max_surge: 1,
                    stop_grace_period_seconds: 10,
                },
            },
        );
        state.tasks.insert(
            "task-1".into(),
            TaskRecord {
                id: "task-1".into(),
                service_id: "demo_web".into(),
                revision: 1,
                slot: 0,
                node_id: "node-a".into(),
                desired: DesiredTaskState::Running,
                observed: ObservedTaskState::Healthy,
                ports: vec![PortBinding {
                    target: 8080,
                    published: 20_001,
                    protocol: "tcp".into(),
                }],
                container_id: Some("container".into()),
            },
        );

        let value = serde_json::to_value(generate(&state)).unwrap();
        assert_eq!(
            value["http"]["services"]["web"]["loadBalancer"]["servers"][0]["url"],
            "http://10.0.0.2:20001"
        );
        assert_eq!(
            value["http"]["routers"]["web"]["rule"],
            "Host(`example.com`)"
        );
    }
}
