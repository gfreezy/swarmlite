use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Value, json};

use crate::model::{
    ClusterState, DesiredTaskState, ObservedTaskState, RecoveredStackGateway, ServicePortKey,
    ServiceRecord,
};

pub use swarmlite_stack::{HttpServer, StorageConfig, routed_service_ports, storage};

pub fn generate(state: &ClusterState, listen: &[String]) -> HttpServer {
    swarmlite_stack::generate(
        state
            .gateway_routes
            .iter()
            .map(|(stack_key, stack)| (stack_key.as_str(), &stack.gateway)),
        listen,
        |stack_name, service_name, target_port, protocol| {
            state
                .gateway_routes
                .get(stack_name)
                .and_then(|stack| {
                    stack
                        .upstreams
                        .get(&ServicePortKey::new(service_name, target_port, protocol))
                })
                .cloned()
                .unwrap_or_default()
        },
    )
}

pub fn config(state: &ClusterState, listen: &[String], controller: String) -> Value {
    let server = generate(state, listen);
    let storage = storage(controller);
    let mut apps = json!({
        "http": {
            "servers": {
                "swarmlite": server
            }
        }
    });
    if state.gateway_routes.values().any(|stack| {
        stack
            .gateway
            .http_routes
            .iter()
            .flat_map(|route| &route.rules)
            .any(|rule| rule.cache.is_some())
    }) {
        apps.as_object_mut()
            .expect("apps is an object")
            .insert("cache".to_owned(), cache_app());
    }

    json!({
        "admin": {
            "listen": "0.0.0.0:2019",
            "config": { "persist": true }
        },
        "storage": storage,
        "apps": apps
    })
}

pub fn replace_stack_route(state: &mut ClusterState, stack_name: &str) -> bool {
    let next = stack_fragment(state, stack_name);
    match next {
        Some(next) if state.gateway_routes.get(stack_name) != Some(&next) => {
            state.gateway_routes.insert(stack_name.to_owned(), next);
            true
        }
        Some(_) => false,
        None => state.gateway_routes.remove(stack_name).is_some(),
    }
}

/// Refresh route fragments only after every currently running routed Task has
/// reported its node identity. This preserves recovered last-known-good
/// upstreams during Controller startup while Agents are still reconnecting.
pub fn refresh_ready_stack_routes(state: &mut ClusterState) -> bool {
    let stack_names = state.stacks.keys().cloned().collect::<Vec<_>>();
    let mut changed = false;
    for stack_name in stack_names {
        let Some(stack) = state.stacks.get(&stack_name) else {
            continue;
        };
        let routed_services = stack
            .gateway
            .http_routes
            .iter()
            .flat_map(|route| &route.rules)
            .filter_map(|rule| rule.backend.service.as_deref())
            .map(|service| format!("{stack_name}.{service}"))
            .collect::<BTreeSet<_>>();
        let all_running_tasks_reported = state
            .tasks
            .values()
            .filter(|task| {
                routed_services.contains(&task.service_id)
                    && task.desired == DesiredTaskState::Running
            })
            .all(|task| state.nodes.contains_key(&task.node_id));
        if all_running_tasks_reported {
            changed |= replace_stack_route(state, &stack_name);
        }
    }
    changed
}

fn stack_fragment(state: &ClusterState, stack_name: &str) -> Option<RecoveredStackGateway> {
    let stack = state.stacks.get(stack_name)?;
    if stack.gateway.http_routes.is_empty() {
        return None;
    }
    let mut upstreams = BTreeMap::new();
    for rule in stack
        .gateway
        .http_routes
        .iter()
        .flat_map(|route| &route.rules)
    {
        let Some(service_name) = rule.backend.service.as_deref() else {
            continue;
        };
        let service_id = format!("{stack_name}.{service_name}");
        let addresses = state
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
                    .find(|port| port.target == rule.backend.port && port.protocol == "tcp")?;
                Some(format!(
                    "{}:{}",
                    format_host(&node.address),
                    port.published?
                ))
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        upstreams.insert(
            ServicePortKey::new(service_name, rule.backend.port, rule.backend.protocol),
            addresses,
        );
    }
    Some(RecoveredStackGateway {
        gateway: stack.gateway.clone(),
        upstreams,
    })
}

fn cache_app() -> Value {
    json!({
        "badger": {
            "found": true,
            "configuration": {
                "Dir": "/cache/badger",
                "ValueDir": "/cache/badger"
            }
        },
        "mode": "bypass"
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
                deployment_history: BTreeMap::new(),
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
        assert!(replace_stack_route(&mut state, "demo"));

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

    #[test]
    fn only_configures_the_cache_app_for_cached_routes() {
        let uncached = config(
            &ClusterState::default(),
            &[":80".into()],
            "controller".into(),
        );
        assert!(uncached["apps"].get("cache").is_none());

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
            ttl: 5m
          backend: { service: web }
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
                deployment_history: BTreeMap::new(),
            },
        );
        assert!(replace_stack_route(&mut state, "demo"));

        let cached = config(&state, &[":80".into()], "controller".into());
        assert_eq!(cached["apps"]["cache"]["mode"], "bypass");
        assert_eq!(
            cached["apps"]["cache"]["badger"]["configuration"]["Dir"],
            "/cache/badger"
        );
        assert_eq!(
            cached["apps"]["cache"]["badger"]["configuration"]["ValueDir"],
            "/cache/badger"
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
                max_replicas_per_node: None,
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
