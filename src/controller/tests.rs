use base64::{Engine as _, engine::general_purpose::STANDARD};
use std::collections::BTreeMap;
use swarmlite_stack::{ParsedStack, parse_stack};

use crate::{
    config::GatewayConfig,
    model::{
        CLUSTER_SCHEMA_VERSION, ClusterGatewayConfig, KvLockStatus, NodeRecord, PortBinding,
        ServicePort, ServiceSpec, StackGatewaySpec, TaskRecord, TaskReport,
    },
};

use super::*;

fn test_cluster(id: &str) -> ClusterSettings {
    ClusterSettings {
        schema_version: CLUSTER_SCHEMA_VERSION,
        cluster_id: id.into(),
        controller_id: "controller-a".into(),
        controller_port: 8080,
        gateway: ClusterGatewayConfig::default(),
    }
}

fn test_controller_config(cluster: &ClusterSettings) -> ControllerConfig {
    ControllerConfig {
        gateway_enabled: true,
        labels: BTreeMap::new(),
        listen: "127.0.0.1:0".parse().unwrap(),
        advertise_url: "http://10.0.0.10:8080".into(),
        node_timeout_seconds: 20,
        reconcile_interval_seconds: 1,
        gateway: GatewayConfig::default(),
        cluster: cluster.clone(),
    }
}

async fn test_controller(id: &str) -> (Controller, StateRepository, tempfile::TempDir) {
    let cluster = test_cluster(id);
    let directory = tempfile::tempdir().unwrap();
    let repository = StateRepository::open(directory.path(), cluster.clone()).unwrap();
    let controller = Controller::new(
        test_controller_config(&cluster),
        "0123456789abcdef".into(),
        repository.clone(),
    )
    .await
    .unwrap();
    (controller, repository, directory)
}

fn test_join_request(node_id: &str, address: &str) -> JoinRequest {
    JoinRequest {
        node_id: node_id.to_owned(),
        address: address.to_owned(),
        gateway_enabled: false,
        labels: BTreeMap::new(),
    }
}

fn test_node() -> NodeRecord {
    NodeRecord {
        id: "node-a".into(),
        address: "127.0.0.1".into(),
        labels: BTreeMap::new(),
        cpu_millis: 1000,
        memory_bytes: 1024,
        port_range_start: 20_000,
        port_range_end: 29_999,
        gateway_enabled: false,
    }
}

#[test]
fn rejects_a_gateway_hostname_owned_by_another_stack() {
    let gateway = parse_stack(
        r#"
services:
  web:
    image: nginx
x-swarmlite:
  http_routes:
    - hostnames: [EXAMPLE.com]
      rules:
        - backend: { service: web, port: 80 }
"#,
    )
    .unwrap()
    .gateway;
    let mut state = ClusterState::default();
    state.stacks.insert(
        "first".into(),
        StackRecord {
            name: "first".into(),
            applied_at_unix_ms: 1,
            services: vec!["first.web".into()],
            gateway: gateway.clone(),
        },
    );
    let error = validate_gateway_hostname_ownership(&state, "second", &gateway).unwrap_err();
    assert!(matches!(
        error,
        ControllerError::Conflict(message)
            if message.contains("example.com") && message.contains("first")
    ));
}

#[tokio::test]
async fn persists_kv_and_fences_locks_in_sqlite() {
    let (controller, repository, _directory) = test_controller("kv-test").await;
    let control_generation = repository.load().await.unwrap().generation;
    controller
        .put_kv(KvPutRequest {
            key: "apps/demo/config".into(),
            value_base64: STANDARD.encode("new"),
        })
        .await
        .unwrap();
    controller
        .put_kv(KvPutRequest {
            key: "apps/demo/config".into(),
            value_base64: STANDARD.encode("latest"),
        })
        .await
        .unwrap();
    assert_eq!(
        STANDARD
            .decode(
                controller
                    .kv_object("apps/demo/config")
                    .await
                    .unwrap()
                    .value_base64
            )
            .unwrap(),
        b"latest"
    );
    assert_eq!(
        repository.load().await.unwrap().generation,
        control_generation
    );

    let acquired = controller
        .acquire_kv_lock(KvLockAcquireRequest {
            name: "jobs/demo".into(),
            owner_id: "writer-a".into(),
            lease_millis: 30_000,
        })
        .await
        .unwrap();
    let token = acquired.fencing_token.unwrap();
    assert_eq!(acquired.status, KvLockStatus::Acquired);
    assert_eq!(
        controller
            .acquire_kv_lock(KvLockAcquireRequest {
                name: "jobs/demo".into(),
                owner_id: "writer-b".into(),
                lease_millis: 30_000,
            })
            .await
            .unwrap()
            .status,
        KvLockStatus::Busy
    );
    assert!(
        controller
            .release_kv_lock(KvLockMutationRequest {
                name: "jobs/demo".into(),
                owner_id: "writer-b".into(),
                fencing_token: token,
                lease_millis: None,
            })
            .await
            .is_err()
    );
}

#[tokio::test]
async fn gateway_is_the_only_mutable_component_setting() {
    let (controller, _, _directory) = test_controller("gateway-test").await;
    let joined = controller
        .join_node("node-a", test_join_request("node-a", "10.0.0.11"))
        .await
        .unwrap();
    assert!(!joined.gateway_enabled);

    assert!(matches!(
        controller
            .join_node(
                "controller-a",
                test_join_request("controller-a", "10.0.0.12")
            )
            .await,
        Err(ControllerError::Conflict(_))
    ));
    let enabled = controller
        .update_node_gateway("node-a", NodeGatewayUpdate { enabled: true })
        .await
        .unwrap();
    assert!(enabled.enabled);
    let disabled = controller
        .update_node_gateway("controller-a", NodeGatewayUpdate { enabled: false })
        .await
        .unwrap();
    assert!(!disabled.enabled);
}

#[tokio::test]
async fn node_labels_are_authoritative_and_persisted() {
    let cluster = test_cluster("node-label-test");
    let directory = tempfile::tempdir().unwrap();
    let repository = StateRepository::open(directory.path(), cluster.clone()).unwrap();
    let observer = repository.clone();
    let mut config = test_controller_config(&cluster);
    config.labels = BTreeMap::from([("region".into(), "cn-east".into())]);
    let controller = Controller::new(config, "secret".into(), repository)
        .await
        .unwrap();
    let mut request = test_join_request("node-a", "127.0.0.1");
    request.labels = BTreeMap::from([("disk".into(), "ssd".into())]);
    controller.join_node("node-a", request).await.unwrap();

    let mut reported = test_node();
    reported.labels = BTreeMap::from([("disk".into(), "hdd".into())]);
    let response = controller
        .heartbeat(
            "node-a",
            NodeHeartbeat {
                node: reported,
                tasks: Vec::new(),
                gateway: GatewayReport::default(),
            },
        )
        .await
        .unwrap();
    assert_eq!(response.labels["disk"], "ssd");

    let labels = controller
        .set_node_label(
            "node-a",
            NodeLabelSetRequest {
                key: "region".into(),
                value: "cn-north".into(),
            },
        )
        .await
        .unwrap();
    assert_eq!(labels.labels["region"], "cn-north");
    assert_eq!(
        observer.load().await.unwrap().state.members["node-a"].labels,
        labels.labels
    );
}

#[tokio::test]
async fn caddy_acknowledgement_starts_drain_deadline() {
    let mut cluster = test_cluster("caddy-publisher-test");
    cluster.gateway.listen = vec![":18089".into()];
    let directory = tempfile::tempdir().unwrap();
    let repository = StateRepository::open(directory.path(), cluster.clone()).unwrap();
    let mut config = test_controller_config(&cluster);
    config.gateway_enabled = false;
    config.gateway.drain_timeout_seconds = 3;
    let controller = Controller::new(config, "test-token".into(), repository)
        .await
        .unwrap();
    let mut request = test_join_request("node-a", "127.0.0.1");
    request.gateway_enabled = true;
    controller.join_node("node-a", request).await.unwrap();
    {
        let mut inner = controller.inner.lock().await;
        inner
            .state
            .services
            .insert("demo.web".into(), test_service());
        inner.state.tasks.insert("old-task".into(), draining_task());
        controller.commit_locked(&mut inner).await.unwrap();
    }

    let report = TaskReport {
        id: "old-task".into(),
        observed: ObservedTaskState::Healthy,
        container_id: Some("container-old".into()),
        cluster_id: Some("caddy-publisher-test".into()),
        stack: Some("demo".into()),
        service: Some("web".into()),
        slot: Some(0),
        revision: Some(1),
        spec_hash: Some(service_spec_hash(&test_service().spec)),
        ports: vec![PortBinding {
            target: 80,
            published: 20_001,
            protocol: "tcp".into(),
        }],
    };
    let desired = controller
        .heartbeat(
            "node-a",
            NodeHeartbeat {
                node: test_node(),
                tasks: vec![report.clone()],
                gateway: GatewayReport::default(),
            },
        )
        .await
        .unwrap()
        .gateway_config
        .unwrap();
    assert_eq!(desired.storage["controller"], "http://10.0.0.10:8080");
    assert!(desired.storage.get("controllers").is_none());
    assert_eq!(desired.server["listen"][0], ":18089");

    controller
        .heartbeat(
            "node-a",
            NodeHeartbeat {
                node: test_node(),
                tasks: vec![report],
                gateway: GatewayReport {
                    applied_generation: Some(desired.generation),
                    error: None,
                },
            },
        )
        .await
        .unwrap();

    let inner = controller.inner.lock().await;
    assert!(inner.state.tasks["old-task"].drain_until_unix_ms.is_some());
    assert_eq!(
        inner.gateway_reports["node-a"].applied_generation,
        Some(desired.generation)
    );
}

fn test_service() -> ServiceRecord {
    ServiceRecord {
        id: "demo.web".into(),
        stack: "demo".into(),
        name: "web".into(),
        revision: 2,
        spec: ServiceSpec {
            image: "nginx:1.29-alpine".into(),
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

#[tokio::test]
async fn heartbeat_then_deploy_adopts_the_existing_container() {
    let (controller, _, _directory) = test_controller("recovery-test").await;
    controller
        .join_node("node-a", test_join_request("node-a", "127.0.0.1"))
        .await
        .unwrap();
    let mut service = test_service();
    service.spec.service_labels.clear();
    let spec_hash = service_spec_hash(&service.spec);
    controller
        .heartbeat(
            "node-a",
            NodeHeartbeat {
                node: test_node(),
                tasks: vec![TaskReport {
                    id: "existing-task".into(),
                    observed: ObservedTaskState::Healthy,
                    container_id: Some("container-existing".into()),
                    cluster_id: Some("recovery-test".into()),
                    stack: Some("demo".into()),
                    service: Some("web".into()),
                    slot: Some(0),
                    revision: Some(7),
                    spec_hash: Some(spec_hash),
                    ports: vec![PortBinding {
                        target: 80,
                        published: 20_001,
                        protocol: "tcp".into(),
                    }],
                }],
                gateway: GatewayReport::default(),
            },
        )
        .await
        .unwrap();
    controller
        .apply(
            "demo",
            ParsedStack {
                services: BTreeMap::from([("web".into(), service.spec)]),
                gateway: StackGatewaySpec::default(),
            },
        )
        .await
        .unwrap();
    let inner = controller.inner.lock().await;
    assert_eq!(
        inner.state.tasks["existing-task"].container_id.as_deref(),
        Some("container-existing")
    );
    assert!(inner.state.unclaimed_tasks.is_empty());
}

fn draining_task() -> TaskRecord {
    TaskRecord {
        id: "old-task".into(),
        service_id: "demo.web".into(),
        revision: 1,
        slot: 0,
        node_id: "node-a".into(),
        desired: DesiredTaskState::Draining,
        observed: ObservedTaskState::Healthy,
        ports: vec![PortBinding {
            target: 80,
            published: 20_001,
            protocol: "tcp".into(),
        }],
        container_id: Some("container-old".into()),
        drain_until_unix_ms: None,
    }
}
