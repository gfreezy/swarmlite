use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Value, json};
use swarmlite_stack::{PathRegexpMatcher, RequestMatcher, Route};

use crate::model::{
    ClusterGatewayConfig, ClusterState, DesiredTaskState, ObservedTaskState, RecoveredStackGateway,
    ServicePortKey, ServiceRecord,
};

pub use swarmlite_stack::{HttpServer, StorageConfig, routed_service_ports, storage};

const ACCESS_LOG_NAME: &str = "swarmlite_access";
const ACCESS_LOG_NAMESPACE: &str = "http.log.access.swarmlite_access";

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

pub fn config(state: &ClusterState, gateway: &ClusterGatewayConfig, controller: String) -> Value {
    let mut server = generate(state, &gateway.listen);
    server.routes.insert(0, gateway_probe_route());
    let mut server = serde_json::to_value(server).expect("HTTP server serializes to JSON");
    let server_object = server.as_object_mut().expect("HTTP server is an object");
    if gateway.logging.access.enabled == Some(true) {
        server_object.insert(
            "logs".to_owned(),
            json!({ "default_logger_name": ACCESS_LOG_NAME }),
        );
    }
    if let Some(seconds) = gateway.http.timeouts.read_header_seconds {
        server_object.insert("read_header_timeout".to_owned(), duration(seconds));
    }
    if let Some(seconds) = gateway.http.timeouts.read_body_seconds {
        server_object.insert("read_timeout".to_owned(), duration(seconds));
    }
    if let Some(seconds) = gateway.http.timeouts.write_seconds {
        server_object.insert("write_timeout".to_owned(), duration(seconds));
    }
    if let Some(seconds) = gateway.http.timeouts.idle_seconds {
        server_object.insert("idle_timeout".to_owned(), duration(seconds));
    }
    if let Some(bytes) = gateway.http.max_header_bytes {
        server_object.insert("max_header_bytes".to_owned(), json!(bytes));
    }
    if let Some(enabled) = gateway.http.http3_enabled {
        let protocols = if enabled {
            json!(["h1", "h2", "h3"])
        } else {
            json!(["h1", "h2"])
        };
        server_object.insert("protocols".to_owned(), protocols);
    }

    let storage = storage(controller);
    let mut http = json!({
        "servers": {
            "swarmlite": server
        }
    });
    let http_object = http.as_object_mut().expect("HTTP app is an object");
    if gateway.metrics.enabled == Some(true) {
        let mut metrics = serde_json::Map::new();
        if let Some(per_host) = gateway.metrics.per_host {
            metrics.insert("per_host".to_owned(), json!(per_host));
        }
        http_object.insert("metrics".to_owned(), Value::Object(metrics));
    }
    if let Some(seconds) = gateway.shutdown.grace_period_seconds {
        http_object.insert("grace_period".to_owned(), duration(seconds));
    }
    let mut apps = json!({ "http": http });
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

    let mut config = json!({
        "admin": {
            "listen": "0.0.0.0:2019",
            "config": { "persist": true }
        },
        "storage": storage,
        "apps": apps
    });
    if let Some(logging) = logging_config(gateway) {
        config
            .as_object_mut()
            .expect("Caddy config is an object")
            .insert("logging".to_owned(), logging);
    }
    config
}

fn duration(seconds: u64) -> Value {
    Value::String(format!("{seconds}s"))
}

fn logging_config(gateway: &ClusterGatewayConfig) -> Option<Value> {
    let runtime_level = gateway.logging.runtime.level;
    let access_enabled = gateway.logging.access.enabled == Some(true);
    if runtime_level.is_none() && !access_enabled {
        return None;
    }

    let mut default_log =
        serde_json::Map::from_iter([("writer".to_owned(), json!({ "output": "stderr" }))]);
    if let Some(level) = runtime_level {
        default_log.insert("level".to_owned(), json!(level.as_caddy_str()));
    }

    let mut logs = serde_json::Map::new();
    if access_enabled {
        default_log.insert("exclude".to_owned(), json!([ACCESS_LOG_NAMESPACE]));
        let mut access_log = serde_json::Map::from_iter([
            ("writer".to_owned(), json!({ "output": "stdout" })),
            ("include".to_owned(), json!([ACCESS_LOG_NAMESPACE])),
        ]);
        if let Some(format) = gateway.logging.access.format {
            access_log.insert(
                "encoder".to_owned(),
                json!({ "format": format.as_caddy_str() }),
            );
        }
        if gateway.logging.access.sampling.enabled == Some(true) {
            let mut sampling =
                serde_json::Map::from_iter([("interval".to_owned(), json!(1_000_000_000_u64))]);
            if let Some(first) = gateway.logging.access.sampling.first {
                sampling.insert("first".to_owned(), json!(first));
            }
            if let Some(thereafter) = gateway.logging.access.sampling.thereafter {
                sampling.insert("thereafter".to_owned(), json!(thereafter));
            }
            access_log.insert("sampling".to_owned(), Value::Object(sampling));
        }
        logs.insert(ACCESS_LOG_NAME.to_owned(), Value::Object(access_log));
    }
    logs.insert("default".to_owned(), Value::Object(default_log));
    Some(json!({ "logs": logs }))
}

fn gateway_probe_route() -> Route {
    Route {
        id: "swarmlite-gateway-owner-probe".to_owned(),
        matchers: vec![RequestMatcher {
            host: Vec::new(),
            protocol: "http",
            path_regexp: Some(PathRegexpMatcher {
                pattern: r"^/\.well-known/swarmlite/gateway-owner$".to_owned(),
            }),
        }],
        handle: vec![json!({ "handler": "swarmlite_gateway_probe" })],
        terminal: true,
    }
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
        // cache-handler v0.16.0 only dispatches a fixed provider list. The
        // Gateway image registers its SQLite storer through the SimpleFS slot
        // until cache-handler accepts native third-party provider names.
        "simplefs": {
            "found": true,
            "path": "/cache/sqlite/cache.db",
            "configuration": {
                "read_connections": 4,
                "cleanup_interval": "5m",
                "mapping_scan_interval": "1m",
                "journal_size_limit": 67108864
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

    fn gateway_with_listen(listen: &[&str]) -> ClusterGatewayConfig {
        ClusterGatewayConfig {
            listen: listen.iter().map(|value| (*value).to_owned()).collect(),
            ..Default::default()
        }
    }

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
            &gateway_with_listen(&[":80"]),
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

        let cached = config(&state, &gateway_with_listen(&[":80"]), "controller".into());
        assert_eq!(cached["apps"]["cache"]["mode"], "bypass");
        assert_eq!(
            cached["apps"]["cache"]["simplefs"]["path"],
            "/cache/sqlite/cache.db"
        );
        assert!(
            cached["apps"]["cache"]["simplefs"]["configuration"]
                .get("cache_size_kib")
                .is_none()
        );
        assert_eq!(
            cached["apps"]["cache"]["simplefs"]["configuration"]["read_connections"],
            4
        );
        assert_eq!(
            cached["apps"]["cache"]["simplefs"]["configuration"]["mapping_scan_interval"],
            "1m"
        );
    }

    #[test]
    fn gateway_owner_probe_precedes_user_routes_and_uses_http_only() {
        let value = config(
            &ClusterState::default(),
            &gateway_with_listen(&[":80", ":443"]),
            "http://controller:17080".into(),
        );
        let route = &value["apps"]["http"]["servers"]["swarmlite"]["routes"][0];
        assert_eq!(route["@id"], "swarmlite-gateway-owner-probe");
        assert_eq!(route["match"][0]["protocol"], "http");
        assert_eq!(
            route["match"][0]["path_regexp"]["pattern"],
            r"^/\.well-known/swarmlite/gateway-owner$"
        );
        assert!(route["match"][0].get("host").is_none());
        assert_eq!(route["handle"][0]["handler"], "swarmlite_gateway_probe");
        assert_eq!(route["terminal"], true);
        assert_eq!(value["storage"]["gateway_id_env"], "SWARMLITE_GATEWAY_ID");
        assert_eq!(value["storage"]["probe_timeout"], "2s");
        assert_eq!(value["storage"]["owner_cache_ttl"], "1m");
    }

    #[test]
    fn renders_gateway_metrics_logging_timeouts_and_protocols() {
        let mut gateway = gateway_with_listen(&[":80", ":443"]);
        gateway.metrics.enabled = Some(true);
        gateway.metrics.per_host = Some(false);
        gateway.logging.runtime.level = Some(crate::model::GatewayLogLevel::Warn);
        gateway.logging.access.enabled = Some(true);
        gateway.logging.access.format = Some(crate::model::GatewayAccessLogFormat::Json);
        gateway.logging.access.sampling.enabled = Some(true);
        gateway.logging.access.sampling.first = Some(0);
        gateway.logging.access.sampling.thereafter = Some(25);
        gateway.shutdown.grace_period_seconds = Some(0);
        gateway.http.timeouts.read_header_seconds = Some(0);
        gateway.http.timeouts.read_body_seconds = Some(30);
        gateway.http.timeouts.write_seconds = Some(45);
        gateway.http.timeouts.idle_seconds = Some(300);
        gateway.http.max_header_bytes = Some(0);
        gateway.http.http3_enabled = Some(false);

        let value = config(&ClusterState::default(), &gateway, "controller".into());
        let http = &value["apps"]["http"];
        let server = &http["servers"]["swarmlite"];
        assert_eq!(http["metrics"]["per_host"], false);
        assert_eq!(http["grace_period"], "0s");
        assert_eq!(server["read_header_timeout"], "0s");
        assert_eq!(server["read_timeout"], "30s");
        assert_eq!(server["write_timeout"], "45s");
        assert_eq!(server["idle_timeout"], "300s");
        assert_eq!(server["max_header_bytes"], 0);
        assert_eq!(server["protocols"], json!(["h1", "h2"]));
        assert_eq!(server["logs"]["default_logger_name"], ACCESS_LOG_NAME);

        let logs = &value["logging"]["logs"];
        assert_eq!(logs["default"]["writer"]["output"], "stderr");
        assert_eq!(logs["default"]["level"], "WARN");
        assert_eq!(logs["default"]["exclude"], json!([ACCESS_LOG_NAMESPACE]));
        assert_eq!(logs[ACCESS_LOG_NAME]["writer"]["output"], "stdout");
        assert_eq!(
            logs[ACCESS_LOG_NAME]["include"],
            json!([ACCESS_LOG_NAMESPACE])
        );
        assert_eq!(logs[ACCESS_LOG_NAME]["encoder"]["format"], "json");
        assert_eq!(
            logs[ACCESS_LOG_NAME]["sampling"],
            json!({ "interval": 1_000_000_000_u64, "first": 0, "thereafter": 25 })
        );
    }

    #[test]
    fn omits_unset_optional_gateway_fields() {
        let value = config(
            &ClusterState::default(),
            &gateway_with_listen(&[":80"]),
            "controller".into(),
        );
        let http = &value["apps"]["http"];
        let server = &http["servers"]["swarmlite"];
        assert!(http.get("metrics").is_none());
        assert!(http.get("grace_period").is_none());
        assert!(server.get("read_header_timeout").is_none());
        assert!(server.get("logs").is_none());
        assert!(server.get("protocols").is_none());
        assert!(value.get("logging").is_none());
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
            swarmlite_version: None,
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
