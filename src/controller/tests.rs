use std::collections::BTreeMap;
use std::time::Duration;

use axum::{Json, Router, extract::State, http::StatusCode, routing::post};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use swarmlite_raft::{ControllerNode, NodeConfig, RaftNode};
use swarmlite_stack::parse_stack;

use crate::{
    config::GatewayConfig,
    model::{
        CLUSTER_SCHEMA_VERSION, ClusterGatewayConfig, KvVersion, NodeRecord, NodeRoles,
        PortBinding, ServicePort, ServiceSpec, StackGatewaySpec, TaskRecord, TaskReport,
        agent_roles, initial_roles,
    },
};

use super::*;

fn test_controller_config(cluster: &ClusterSettings) -> ControllerConfig {
    ControllerConfig {
        controller_id: "controller-a".into(),
        roles: initial_roles(),
        labels: BTreeMap::new(),
        listen: "127.0.0.1:0".parse().unwrap(),
        advertise_url: "http://10.0.0.10:8080".into(),
        node_timeout_seconds: 20,
        reconcile_interval_seconds: 1,
        gateway: GatewayConfig::default(),
        cluster: cluster.clone(),
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
    validate_gateway_hostname_ownership(&state, "first", &gateway).unwrap();
}

#[tokio::test]
async fn kv_is_lww_and_locks_are_fenced() {
    let cluster = ClusterSettings {
        schema_version: CLUSTER_SCHEMA_VERSION,
        cluster_id: "kv-test".into(),
        mode: ClusterMode::Standalone,
        controller_port: 8080,
        gateway: ClusterGatewayConfig::default(),
    };
    let (repository, _raft, _directory) = test_repository(&cluster).await;
    let controller = Controller::new(
        test_controller_config(&cluster),
        "secret".into(),
        repository.clone(),
    )
    .await
    .unwrap();
    controller.tick().await.unwrap();

    let new_version = KvVersion {
        physical_unix_ms: 20,
        logical: 0,
        replica_id: "writer-a".into(),
    };
    assert!(
        controller
            .put_kv(KvPutRequest {
                key: "apps/demo/config".into(),
                value_base64: STANDARD.encode("new"),
                version: new_version.clone(),
                modified_at_unix_ms: 20,
            })
            .await
            .unwrap()
            .applied
    );
    assert!(
        !controller
            .put_kv(KvPutRequest {
                key: "apps/demo/config".into(),
                value_base64: STANDARD.encode("old"),
                version: KvVersion {
                    physical_unix_ms: 10,
                    logical: 0,
                    replica_id: "writer-b".into(),
                },
                modified_at_unix_ms: 10,
            })
            .await
            .unwrap()
            .applied
    );
    assert_eq!(
        controller
            .kv_object("apps/demo/config")
            .await
            .unwrap()
            .value_base64,
        STANDARD.encode("new")
    );
    assert_eq!(
        repository.load_consistent().await.unwrap().kv.objects["apps/demo/config"].version,
        new_version
    );

    let acquired = controller
        .acquire_kv_lock(KvLockAcquireRequest {
            name: "jobs/demo".into(),
            owner_id: "writer-a".into(),
            lease_millis: 30_000,
        })
        .await
        .unwrap();
    assert_eq!(acquired.status, KvLockStatus::Acquired);
    let token = acquired.fencing_token.unwrap();
    let busy = controller
        .acquire_kv_lock(KvLockAcquireRequest {
            name: "jobs/demo".into(),
            owner_id: "writer-b".into(),
            lease_millis: 30_000,
        })
        .await
        .unwrap();
    assert_eq!(busy.status, KvLockStatus::Busy);
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
    controller
        .release_kv_lock(KvLockMutationRequest {
            name: "jobs/demo".into(),
            owner_id: "writer-a".into(),
            fencing_token: token,
            lease_millis: None,
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn ha_automatically_fills_only_controller_roles() {
    let cluster = ClusterSettings {
        schema_version: CLUSTER_SCHEMA_VERSION,
        cluster_id: "controller-assignment-test".into(),
        mode: ClusterMode::Ha,
        controller_port: 8080,
        gateway: ClusterGatewayConfig::default(),
    };
    let (repository, _raft, _directory) = test_repository(&cluster).await;
    let config = test_controller_config(&cluster);
    let controller = Controller::new(config, "secret".into(), repository)
        .await
        .unwrap();
    controller.tick().await.unwrap();

    let first = controller
        .join_node("node-b", test_join_request("node-b", "10.0.0.22", 2))
        .await
        .unwrap();
    assert_eq!(
        first.roles,
        BTreeSet::from([NodeRole::Controller, NodeRole::Agent])
    );

    let second = controller
        .join_node("node-c", test_join_request("node-c", "10.0.0.23", 3))
        .await
        .unwrap();
    assert_eq!(
        second.roles,
        BTreeSet::from([NodeRole::Controller, NodeRole::Agent])
    );

    let third = controller
        .join_node("node-d", test_join_request("node-d", "10.0.0.24", 4))
        .await
        .unwrap();
    assert_eq!(third.roles, agent_roles());
    assert_eq!(controller.inner.lock().await.state.controllers.len(), 3);
    assert_eq!(
        role_count(
            &controller.inner.lock().await.state,
            NodeRole::Gateway,
            None
        ),
        1
    );

    let mut explicit = test_join_request("node-e", "10.0.0.25", 5);
    explicit.requested_roles = Some(BTreeSet::from([NodeRole::Gateway]));
    let joined = controller.join_node("node-e", explicit).await.unwrap();
    assert_eq!(
        joined.roles,
        BTreeSet::from([NodeRole::Agent, NodeRole::Gateway])
    );
    let error = controller
        .update_node_roles(
            "controller-a",
            NodeRolesUpdate {
                roles: BTreeSet::from([NodeRole::Controller]),
            },
            RoleOperation::Remove,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        ControllerError::Conflict(message) if message.contains("last active controller voter")
    ));
    controller
        .update_node_roles(
            "controller-a",
            NodeRolesUpdate {
                roles: BTreeSet::from([NodeRole::Gateway]),
            },
            RoleOperation::Remove,
        )
        .await
        .unwrap();
    assert!(matches!(
        controller
            .update_node_roles(
                "node-e",
                NodeRolesUpdate {
                    roles: BTreeSet::from([NodeRole::Gateway]),
                },
                RoleOperation::Remove,
            )
            .await,
        Err(ControllerError::Conflict(_))
    ));

    let mut conflicting = test_join_request("node-f", "10.0.0.26", 6);
    conflicting.requested_roles = Some(BTreeSet::from([NodeRole::Controller]));
    assert!(matches!(
        controller.join_node("node-f", conflicting).await,
        Err(ControllerError::Conflict(_))
    ));
}

#[tokio::test]
async fn standalone_keeps_one_controller_and_allows_unlimited_gateways() {
    let cluster = ClusterSettings {
        schema_version: CLUSTER_SCHEMA_VERSION,
        cluster_id: "standalone-role-test".into(),
        mode: ClusterMode::Standalone,
        controller_port: 8080,
        gateway: ClusterGatewayConfig::default(),
    };
    let (repository, _raft, _directory) = test_repository(&cluster).await;
    let controller = Controller::new(
        test_controller_config(&cluster),
        "secret".into(),
        repository,
    )
    .await
    .unwrap();
    controller.tick().await.unwrap();

    for (node_id, address, raft_id) in [
        ("gateway-b", "10.0.0.22", 2),
        ("gateway-c", "10.0.0.23", 3),
        ("gateway-d", "10.0.0.24", 4),
    ] {
        let mut request = test_join_request(node_id, address, raft_id);
        if node_id == "gateway-b" {
            request.recovered_roles = BTreeSet::from([NodeRole::Gateway]);
        } else {
            request.requested_roles = Some(BTreeSet::from([NodeRole::Gateway]));
        }
        let joined = controller.join_node(node_id, request).await.unwrap();
        assert_eq!(
            joined.roles,
            BTreeSet::from([NodeRole::Agent, NodeRole::Gateway])
        );
    }

    let joined = controller
        .join_node("agent-e", test_join_request("agent-e", "10.0.0.25", 5))
        .await
        .unwrap();
    assert_eq!(joined.roles, agent_roles());

    let mut request = test_join_request("controller-f", "10.0.0.26", 6);
    request.requested_roles = Some(BTreeSet::from([NodeRole::Controller]));
    assert!(matches!(
        controller.join_node("controller-f", request).await,
        Err(ControllerError::Conflict(_))
    ));
}

#[tokio::test]
async fn switches_standalone_to_ha_but_not_back() {
    let cluster = ClusterSettings {
        schema_version: CLUSTER_SCHEMA_VERSION,
        cluster_id: "config-update-test".into(),
        mode: ClusterMode::Standalone,
        controller_port: 8080,
        gateway: ClusterGatewayConfig::default(),
    };
    let (repository, _raft, _directory) = test_repository(&cluster).await;
    let observer = repository.clone();
    let config = test_controller_config(&cluster);
    let controller = Controller::new(config, "secret".into(), repository)
        .await
        .unwrap();
    controller.tick().await.unwrap();

    let updated = controller
        .update_cluster_config(ClusterConfigUpdate {
            mode: Some(ClusterMode::Ha),
            gateway_image: None,
        })
        .await
        .unwrap();
    assert_eq!(updated.config.mode, ClusterMode::Ha);
    assert_eq!(
        observer.load_consistent().await.unwrap().cluster.mode,
        ClusterMode::Ha
    );
    assert_eq!(controller.get_cluster_config().await.unwrap(), updated);

    let joined = controller
        .join_node("node-b", test_join_request("node-b", "10.0.0.22", 2))
        .await
        .unwrap();
    assert!(joined.roles.contains(&NodeRole::Controller));

    let image = "ghcr.io/example/swarmlite-caddy:v2";
    let updated = controller
        .update_cluster_config(ClusterConfigUpdate {
            mode: None,
            gateway_image: Some(image.to_owned()),
        })
        .await
        .unwrap();
    assert_eq!(updated.config.gateway.image, image);
    assert_eq!(
        observer
            .load_consistent()
            .await
            .unwrap()
            .cluster
            .gateway
            .image,
        image
    );
    let error = controller
        .update_cluster_config(ClusterConfigUpdate {
            mode: None,
            gateway_image: Some("bad image".to_owned()),
        })
        .await
        .unwrap_err();
    assert!(matches!(error, ControllerError::Invalid(_)));
    assert_eq!(
        controller
            .get_cluster_config()
            .await
            .unwrap()
            .config
            .gateway
            .image,
        image
    );

    let error = controller
        .update_cluster_config(ClusterConfigUpdate {
            mode: Some(ClusterMode::Standalone),
            gateway_image: None,
        })
        .await
        .unwrap_err();
    assert!(matches!(error, ControllerError::Conflict(_)));
}

#[tokio::test]
async fn node_labels_are_authoritative_and_persisted() {
    let cluster = ClusterSettings {
        schema_version: CLUSTER_SCHEMA_VERSION,
        cluster_id: "node-label-test".into(),
        mode: ClusterMode::Standalone,
        controller_port: 8080,
        gateway: ClusterGatewayConfig::default(),
    };
    let (repository, _raft, _directory) = test_repository(&cluster).await;
    let observer = repository.clone();
    let mut config = test_controller_config(&cluster);
    config.labels = BTreeMap::from([("region".into(), "cn-east".into())]);
    let controller = Controller::new(config, "secret".into(), repository)
        .await
        .unwrap();
    controller.tick().await.unwrap();
    assert_eq!(
        observer.load_consistent().await.unwrap().state.members["controller-a"].labels,
        BTreeMap::from([("region".into(), "cn-east".into())])
    );

    let mut request = test_join_request("node-a", "127.0.0.1", 2);
    request.labels = BTreeMap::from([("disk".into(), "ssd".into())]);
    let joined = controller.join_node("node-a", request).await.unwrap();
    assert_eq!(
        joined.labels,
        BTreeMap::from([("disk".into(), "ssd".into())])
    );

    let mut conflicting = test_join_request("node-a", "127.0.0.1", 2);
    conflicting.labels = BTreeMap::from([("disk".into(), "hdd".into())]);
    assert!(matches!(
        controller.join_node("node-a", conflicting).await,
        Err(ControllerError::Conflict(_))
    ));

    let mut reported = test_node();
    reported.labels = BTreeMap::from([
        ("disk".into(), "hdd".into()),
        ("untrusted".into(), "value".into()),
    ]);
    let response = controller
        .heartbeat(
            "node-a",
            NodeHeartbeat {
                node: reported,
                tasks: Vec::new(),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        response.labels,
        BTreeMap::from([("disk".into(), "ssd".into())])
    );
    assert_eq!(
        controller.inner.lock().await.state.nodes["node-a"].labels,
        response.labels
    );

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
        observer.load_consistent().await.unwrap().state.members["node-a"].labels,
        labels.labels
    );

    let labels = controller
        .remove_node_label("node-a", NodeLabelRemoveRequest { key: "disk".into() })
        .await
        .unwrap();
    assert_eq!(
        labels.labels,
        BTreeMap::from([("region".into(), "cn-north".into())])
    );
    assert!(matches!(
        controller
            .set_node_label(
                "node-a",
                NodeLabelSetRequest {
                    key: " bad".into(),
                    value: "value".into(),
                },
            )
            .await,
        Err(ControllerError::Invalid(_))
    ));
}

#[tokio::test]
async fn promotes_reserved_controller_through_raft() {
    let caddy_received = Arc::new(Mutex::new(Vec::<serde_json::Value>::new()));
    let caddy_app = Router::new()
        .route("/config/storage", post(capture_gateway_config))
        .route(
            "/config/apps/http/servers/swarmlite",
            post(capture_gateway_config),
        )
        .with_state(caddy_received.clone());
    let caddy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let caddy_address = caddy_listener.local_addr().unwrap();
    let caddy_server =
        tokio::spawn(async move { axum::serve(caddy_listener, caddy_app).await.unwrap() });

    let cluster = ClusterSettings {
        schema_version: CLUSTER_SCHEMA_VERSION,
        cluster_id: "controller-promotion-test".into(),
        mode: ClusterMode::Ha,
        controller_port: 8080,
        gateway: ClusterGatewayConfig::default(),
    };
    let (repository, leader_raft, _leader_directory) = test_repository(&cluster).await;
    let mut config = test_controller_config(&cluster);
    config.advertise_url = "http://127.0.0.1:19090".into();
    config.gateway.admin_port = caddy_address.port();
    let controller = Controller::new(
        config,
        "0123456789abcdef0123456789abcdef".into(),
        repository,
    )
    .await
    .unwrap();
    controller.tick().await.unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let api_url = format!("http://{address}");
    let raft_url = format!("{api_url}/internal/raft");
    let follower_directory = tempfile::tempdir().unwrap();
    let follower_raft = RaftNode::open(NodeConfig::new(
        2,
        ControllerNode {
            raft_url: raft_url.clone(),
            api_url: api_url.clone(),
        },
        follower_directory.path(),
        cluster.cluster_id.clone(),
        "0123456789abcdef0123456789abcdef",
    ))
    .await
    .unwrap();
    let app = Router::new().nest("/internal/raft", follower_raft.rpc_router());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let joined = controller
        .join_node(
            "controller-b",
            JoinRequest {
                node_id: "controller-b".into(),
                address: address.ip().to_string(),
                requested_roles: None,
                recovered_roles: NodeRoles::new(),
                controller_url: api_url.clone(),
                raft_id: 2,
                raft_url: raft_url.clone(),
                labels: BTreeMap::new(),
            },
        )
        .await
        .unwrap();
    assert!(joined.roles.contains(&NodeRole::Controller));

    let joined_controller_set_generation = joined.controller_set_generation;
    let mut heartbeat_node = NodeRecord {
        id: "controller-b".into(),
        address: address.ip().to_string(),
        labels: BTreeMap::new(),
        cpu_millis: 1000,
        memory_bytes: 1024,
        port_range_start: 20_000,
        port_range_end: 29_999,
        roles: joined.roles,
        controller_url: api_url.clone(),
        raft_id: 2,
        raft_url,
        controller_set_generation: joined_controller_set_generation,
    };
    let response = controller
        .heartbeat(
            "controller-b",
            NodeHeartbeat {
                node: heartbeat_node.clone(),
                tasks: Vec::new(),
            },
        )
        .await
        .unwrap();

    assert!(response.roles.contains(&NodeRole::Controller));
    assert!(response.controllers.contains(&api_url));
    assert!(response.controller_set_generation > joined_controller_set_generation);
    assert!(leader_raft.voter_ids().contains(&2));
    heartbeat_node.controller_set_generation = response.controller_set_generation;
    controller
        .heartbeat(
            "controller-b",
            NodeHeartbeat {
                node: heartbeat_node,
                tasks: Vec::new(),
            },
        )
        .await
        .unwrap();
    assert!(
        controller
            .inner
            .lock()
            .await
            .state
            .controllers
            .contains_key("controller-b")
    );
    assert_eq!(
        controller.inner.lock().await.state.nodes["controller-b"].controller_set_generation,
        response.controller_set_generation
    );

    let current_controller_set_generation = response.controller_set_generation;
    let mut agent_request = test_join_request("node-c", "127.0.0.2", 3);
    agent_request.requested_roles = Some(BTreeSet::from([NodeRole::Agent, NodeRole::Gateway]));
    let joined_agent = controller.join_node("node-c", agent_request).await.unwrap();
    let mut agent_node = test_node();
    agent_node.id = "node-c".into();
    agent_node.address = caddy_address.ip().to_string();
    agent_node.roles = joined_agent.roles;
    agent_node.raft_id = 3;
    agent_node.controller_set_generation = current_controller_set_generation - 1;
    let agent_response = controller
        .heartbeat(
            "node-c",
            NodeHeartbeat {
                node: agent_node.clone(),
                tasks: Vec::new(),
            },
        )
        .await
        .unwrap();
    let error = controller
        .update_node_roles(
            "controller-b",
            NodeRolesUpdate {
                roles: BTreeSet::from([NodeRole::Controller]),
            },
            RoleOperation::Remove,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        ControllerError::Conflict(message)
            if message.contains("generation") && message.contains("node-c")
    ));

    agent_node.controller_set_generation = agent_response.controller_set_generation;
    controller
        .heartbeat(
            "node-c",
            NodeHeartbeat {
                node: agent_node,
                tasks: Vec::new(),
            },
        )
        .await
        .unwrap();
    let error = controller
        .update_node_roles(
            "controller-b",
            NodeRolesUpdate {
                roles: BTreeSet::from([NodeRole::Controller]),
            },
            RoleOperation::Remove,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        ControllerError::Conflict(message)
            if message.contains("Caddy gateways")
                && message.contains(&caddy_address.to_string())
    ));

    controller.sync_gateway_once().await.unwrap();
    assert_eq!(
        controller
            .gateway_sync
            .lock()
            .await
            .applied_controller_set_generations[&format!("http://{caddy_address}")],
        current_controller_set_generation
    );
    controller
        .update_node_roles(
            "controller-b",
            NodeRolesUpdate {
                roles: BTreeSet::from([NodeRole::Controller]),
            },
            RoleOperation::Remove,
        )
        .await
        .unwrap();
    assert!(!leader_raft.voter_ids().contains(&2));

    leader_raft.shutdown().await.unwrap();
    follower_raft.shutdown().await.unwrap();
    server.abort();
    caddy_server.abort();
}

#[tokio::test]
async fn caddy_acknowledgement_starts_drain_deadline() {
    let received = Arc::new(Mutex::new(Vec::<serde_json::Value>::new()));
    let app = Router::new()
        .route("/config/storage", post(capture_gateway_config))
        .route(
            "/config/apps/http/servers/swarmlite",
            post(capture_gateway_config),
        )
        .with_state(received.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let cluster = ClusterSettings {
        schema_version: CLUSTER_SCHEMA_VERSION,
        cluster_id: "caddy-publisher-test".into(),
        mode: ClusterMode::Standalone,
        controller_port: 8080,
        gateway: ClusterGatewayConfig::default(),
    };
    let mut config = test_controller_config(&cluster);
    config.controller_id = "controller-test".into();
    config.gateway.admin_port = address.port();
    config.gateway.listen = vec![":18089".into()];
    config.gateway.drain_timeout_seconds = 3;
    let (repository, _raft, _directory) = test_repository(&cluster).await;
    let controller = Controller::new(config, "test-token".into(), repository)
        .await
        .unwrap();
    {
        let mut inner = controller.inner.lock().await;
        controller.try_acquire_locked(&mut inner).await.unwrap();
        let mut node = test_node();
        node.roles.insert(NodeRole::Gateway);
        inner.state.nodes.insert("node-a".into(), node);
        inner
            .state
            .services
            .insert("demo.web".into(), test_service());
        inner.state.stacks.insert(
            "demo".into(),
            StackRecord {
                name: "demo".into(),
                applied_at_unix_ms: unix_ms(),
                services: vec!["demo.web".into()],
                gateway: parse_stack(
                    r#"
services:
  web:
    image: nginx:1.29-alpine
x-swarmlite:
  http_routes:
    - hostnames: [example.com]
      rules:
        - backend: { service: web, port: 80 }
"#,
                )
                .unwrap()
                .gateway,
            },
        );
        inner.state.tasks.insert("old-task".into(), draining_task());
        controller.commit_locked(&mut inner).await.unwrap();
    }

    let controller_set_generation = controller.repository.controller_set().0;
    controller.sync_gateway_once().await.unwrap();

    let inner = controller.inner.lock().await;
    let task = &inner.state.tasks["old-task"];
    assert_eq!(task.desired, DesiredTaskState::Draining);
    assert!(
        task.drain_until_unix_ms
            .is_some_and(|value| value > unix_ms())
    );
    drop(inner);
    let requests = received.lock().await;
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0]["module"], "swarmlite");
    assert_eq!(requests[0]["token_env"], "SWARMLITE_TOKEN");
    assert_eq!(
        requests[0]["controller_set_generation"],
        controller_set_generation
    );
    assert!(!requests[0]["controllers"].as_array().unwrap().is_empty());
    assert_eq!(requests[1]["listen"][0], ":18089");
    assert!(
        requests[1]["routes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|route| {
                route["handle"].as_array().is_some_and(|handlers| {
                    handlers.iter().any(|handler| handler["status_code"] == 503)
                })
            })
    );
    server.abort();
}

async fn test_repository(
    cluster: &ClusterSettings,
) -> (StateRepository, Arc<RaftNode>, tempfile::TempDir) {
    let directory = tempfile::tempdir().unwrap();
    let raft = RaftNode::open(NodeConfig::new(
        1,
        ControllerNode {
            raft_url: "http://127.0.0.1:19090/internal/raft".into(),
            api_url: "http://127.0.0.1:19090".into(),
        },
        directory.path(),
        cluster.cluster_id.clone(),
        "0123456789abcdef0123456789abcdef",
    ))
    .await
    .unwrap();
    raft.initialize().await.unwrap();
    raft.raft()
        .wait(Some(Duration::from_secs(5)))
        .current_leader(1, "test controller becomes leader")
        .await
        .unwrap();
    (
        StateRepository::new(raft.clone(), cluster.clone()),
        raft,
        directory,
    )
}

fn test_join_request(node_id: &str, address: &str, raft_id: u64) -> JoinRequest {
    let controller_url = format!("http://{address}:8080");
    JoinRequest {
        node_id: node_id.to_owned(),
        address: address.to_owned(),
        requested_roles: None,
        recovered_roles: NodeRoles::new(),
        controller_url: controller_url.clone(),
        raft_id,
        raft_url: format!("{controller_url}/internal/raft"),
        labels: BTreeMap::new(),
    }
}

async fn capture_gateway_config(
    State(received): State<Arc<Mutex<Vec<serde_json::Value>>>>,
    Json(body): Json<serde_json::Value>,
) -> StatusCode {
    received.lock().await.push(body);
    StatusCode::OK
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
        roles: agent_roles(),
        controller_url: "http://127.0.0.1:8080".into(),
        raft_id: 2,
        raft_url: "http://127.0.0.1:8080/internal/raft".into(),
        controller_set_generation: 0,
    }
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
    let cluster = ClusterSettings {
        schema_version: CLUSTER_SCHEMA_VERSION,
        cluster_id: "recovery-test".into(),
        mode: ClusterMode::Standalone,
        controller_port: 8080,
        gateway: ClusterGatewayConfig::default(),
    };
    let (repository, _raft, _directory) = test_repository(&cluster).await;
    let config = test_controller_config(&cluster);
    let controller = Controller::new(config, "secret".into(), repository)
        .await
        .unwrap();
    controller.tick().await.unwrap();
    controller
        .join_node("node-a", test_join_request("node-a", "127.0.0.1", 2))
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
                    cluster_id: Some(cluster.cluster_id.clone()),
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
            },
        )
        .await
        .unwrap();
    assert!(
        controller
            .inner
            .lock()
            .await
            .state
            .unclaimed_tasks
            .contains_key("existing-task")
    );

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
    assert_eq!(inner.state.tasks.len(), 1);
    let task = &inner.state.tasks["existing-task"];
    assert_eq!(task.container_id.as_deref(), Some("container-existing"));
    assert_eq!(task.ports[0].published, 20_001);
    assert!(inner.state.unclaimed_tasks.is_empty());
}

#[test]
fn adopts_matching_unclaimed_container_by_stack_service_and_slot() {
    let service = test_service();
    let spec_hash = service_spec_hash(&service.spec);
    let mut state = ClusterState::default();
    state.services.insert(service.id.clone(), service);
    state.unclaimed_tasks.insert(
        "existing-task".into(),
        UnclaimedTask {
            id: "existing-task".into(),
            stack: "demo".into(),
            service: "web".into(),
            slot: 0,
            revision: 7,
            spec_hash,
            node_id: "node-a".into(),
            observed: ObservedTaskState::Healthy,
            ports: vec![PortBinding {
                target: 80,
                published: 20_001,
                protocol: "tcp".into(),
            }],
            container_id: Some("container-existing".into()),
        },
    );

    adopt_unclaimed_tasks(&mut state, "demo");

    let task = &state.tasks["existing-task"];
    assert_eq!(task.service_id, "demo.web");
    assert_eq!(task.slot, 0);
    assert_eq!(task.container_id.as_deref(), Some("container-existing"));
    assert!(state.unclaimed_tasks.is_empty());
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
