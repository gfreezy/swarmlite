use std::collections::BTreeSet;

use serde_json::{Value, json};

use crate::model::{ClusterState, DesiredTaskState, ObservedTaskState, ServiceRecord};

pub use swarmlite_stack::{HttpServer, StorageConfig, routed_service_ports, storage};

pub fn generate(state: &ClusterState, listen: &[String]) -> HttpServer {
    swarmlite_stack::generate(
        state
            .stacks
            .values()
            .map(|stack| (stack.name.as_str(), &stack.gateway)),
        listen,
        |stack_name, service_name, target_port| {
            let service_id = format!("{stack_name}.{service_name}");
            state
                .tasks
                .values()
                .filter(|task| {
                    task.service_id == service_id
                        && task.desired == DesiredTaskState::Running
                        && task.observed == ObservedTaskState::Healthy
                })
                .filter_map(|task| {
                    let node = state.nodes.get(&task.node_id)?;
                    let port = task
                        .ports
                        .iter()
                        .find(|port| port.target == target_port && port.protocol == "tcp")?;
                    Some(format!(
                        "{}:{}",
                        format_host(&node.address),
                        port.published?
                    ))
                })
                .collect()
        },
    )
}

pub fn config(state: &ClusterState, listen: &[String], controller: String) -> Value {
    let server = generate(state, listen);
    let storage = storage(controller);
    json!({
        "admin": {
            "listen": "0.0.0.0:2019",
            "config": { "persist": true }
        },
        "storage": storage,
        "apps": {
            "http": {
                "servers": {
                    "swarmlite": server
                }
            }
        }
    })
}

pub fn service_ports(state: &ClusterState, service: &ServiceRecord) -> BTreeSet<u16> {
    state
        .stacks
        .get(&service.stack)
        .map(|stack| routed_service_ports(&stack.gateway, &service.name))
        .unwrap_or_default()
}

pub fn is_service_routed(state: &ClusterState, service: &ServiceRecord) -> bool {
    !service_ports(state, service).is_empty()
}

fn format_host(host: &str) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]")
    } else {
        host.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::model::{
        NodeRecord, PortBinding, ServicePort, ServiceSpec, StackRecord, TaskRecord,
    };
    use swarmlite_stack::parse_stack;

    use super::*;

    #[test]
    fn resolves_only_healthy_running_internal_tasks() {
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
        - backend: { service: web, port: 80 }
"#,
        )
        .unwrap();
        let mut state = ClusterState::default();
        state.stacks.insert(
            "demo".into(),
            StackRecord {
                name: "demo".into(),
                applied_at_unix_ms: 1,
                services: vec!["demo.web".into()],
                gateway: parsed.gateway,
                deployment: None,
            },
        );
        state.services.insert("demo.web".into(), service());
        state
            .nodes
            .insert("node-a".into(), node("node-a", "10.0.0.21"));
        state
            .nodes
            .insert("node-b".into(), node("node-b", "2001:db8::2"));
        state.tasks.insert(
            "healthy".into(),
            task(
                "healthy",
                "node-a",
                DesiredTaskState::Running,
                ObservedTaskState::Healthy,
                20_001,
            ),
        );
        state.tasks.insert(
            "starting".into(),
            task(
                "starting",
                "node-b",
                DesiredTaskState::Running,
                ObservedTaskState::Starting,
                20_002,
            ),
        );
        let mut unresolved = task(
            "unresolved",
            "node-b",
            DesiredTaskState::Running,
            ObservedTaskState::Healthy,
            20_003,
        );
        unresolved.ports[0].published = None;
        state.tasks.insert("unresolved".into(), unresolved);

        let value = serde_json::to_value(generate(&state, &[":80".into(), ":443".into()])).unwrap();
        let proxy = value["routes"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|route| route["handle"].as_array().into_iter().flatten())
            .find(|handler| handler["handler"] == "reverse_proxy")
            .unwrap();
        assert_eq!(
            proxy["upstreams"],
            serde_json::json!([{"dial": "10.0.0.21:20001"}])
        );
    }

    fn service() -> ServiceRecord {
        ServiceRecord {
            id: "demo.web".into(),
            stack: "demo".into(),
            name: "web".into(),
            revision: 1,
            spec: ServiceSpec {
                image: "nginx".into(),
                pull_policy: Default::default(),
                command: Vec::new(),
                entrypoint: Vec::new(),
                environment: Vec::new(),
                expose: Vec::new(),
                ports: vec![ServicePort {
                    target: 80,
                    published: None,
                    protocol: "tcp".into(),
                }],
                volumes: Vec::new(),
                configs: Vec::new(),
                container_labels: BTreeMap::new(),
                service_labels: BTreeMap::new(),
                healthcheck: None,
                replicas: 1,
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
            gateway_enabled: false,
        }
    }

    fn task(
        id: &str,
        node_id: &str,
        desired: DesiredTaskState,
        observed: ObservedTaskState,
        published: u16,
    ) -> TaskRecord {
        TaskRecord {
            id: id.into(),
            service_id: "demo.web".into(),
            revision: 1,
            slot: 0,
            node_id: node_id.into(),
            desired,
            observed,
            ports: vec![PortBinding {
                target: 80,
                published: Some(published),
                protocol: "tcp".into(),
            }],
            config_digests: Vec::new(),
            container_id: None,
            drain_until_unix_ms: None,
            applied_generation: None,
            reconcile_error: None,
        }
    }
}
