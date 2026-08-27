use base64::{Engine as _, engine::general_purpose::STANDARD};
use futures_util::{SinkExt, StreamExt};
use std::collections::BTreeMap;
use swarmlite_stack::{ParsedStack, parse_stack};

use crate::{
    config::{
        DEFAULT_CONTROLLER_PORT, DEFAULT_DEPLOYMENT_TIMEOUT_SECONDS,
        DEFAULT_GATEWAY_DRAIN_TIMEOUT_SECONDS,
    },
    model::{
        CLUSTER_SCHEMA_VERSION, ClusterGatewayConfig, KvLockStatus, NodeRecord, PortBinding,
        PullPolicy, ServicePort, ServiceSpec, StackGatewaySpec, TaskRecord, TaskReport,
    },
};

use super::*;

fn test_cluster(id: &str) -> ClusterSettings {
    ClusterSettings {
        schema_version: CLUSTER_SCHEMA_VERSION,
        cluster_id: id.into(),
        controller_id: "controller-a".into(),
        controller_port: DEFAULT_CONTROLLER_PORT,
        gateway: ClusterGatewayConfig::default(),
    }
}

fn test_controller_config(cluster: &ClusterSettings) -> ControllerConfig {
    ControllerConfig {
        gateway_enabled: true,
        labels: BTreeMap::new(),
        listen: "127.0.0.1:0".parse().unwrap(),
        advertise_url: "http://10.0.0.10:17080".into(),
        node_timeout_seconds: 20,
        reconcile_interval_seconds: 1,
        gateway_drain_timeout_seconds: DEFAULT_GATEWAY_DRAIN_TIMEOUT_SECONDS,
        deployment_timeout_seconds: DEFAULT_DEPLOYMENT_TIMEOUT_SECONDS,
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

#[tokio::test]
async fn registry_login_is_persisted_and_synchronized_to_nodes() {
    let (controller, repository, _directory) = test_controller("registry-login-test").await;
    let response = controller
        .set_registry_credential(RegistryLoginRequest {
            registry: "GHCR.IO".into(),
            username: "octocat".into(),
            password: "private-token".into(),
        })
        .await
        .unwrap();
    assert_eq!(response.registry, "ghcr.io");
    assert_eq!(response.username, "octocat");
    assert!(
        !serde_json::to_string(&response)
            .unwrap()
            .contains("private-token")
    );

    let persisted = repository.load().await.unwrap();
    assert_eq!(
        persisted.state.registry_credentials["ghcr.io"].password,
        "private-token"
    );

    let joined = controller
        .join_node("node-a", test_join_request("node-a", "127.0.0.1"))
        .await
        .unwrap();
    assert_eq!(joined.registry_credentials["ghcr.io"].username, "octocat");
    let heartbeat = controller
        .heartbeat(
            "node-a",
            NodeHeartbeat {
                node: test_node(),
                tasks: Vec::new(),
                task_inventory_error: None,
                task_results: Vec::new(),
                gateway: GatewayReport::default(),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        heartbeat.registry_credentials["ghcr.io"].password,
        "private-token"
    );
    assert_eq!(heartbeat.registry_credentials_hash.len(), 64);
}

#[tokio::test]
async fn allows_only_one_inflight_deployment_per_stack() {
    let (controller, _, _directory) = test_controller("deployment-lock-test").await;
    let first = controller.begin_stack_deployment("demo").unwrap();
    assert!(matches!(
        controller.begin_stack_deployment("demo"),
        Err(ControllerError::Conflict(message))
            if message == "stack \"demo\" already has a deployment in progress"
    ));

    let other = controller.begin_stack_deployment("other").unwrap();
    drop(other);
    drop(first);
    assert!(controller.begin_stack_deployment("demo").is_ok());
}

#[tokio::test]
async fn deployment_validation_does_not_change_cluster_state() {
    let (controller, repository, _directory) = test_controller("deployment-validation-test").await;
    register_live_node(&controller).await;
    let before = repository.load().await.unwrap();

    controller
        .validate_apply("demo", &parsed_test_stack())
        .await
        .unwrap();

    let after = repository.load().await.unwrap();
    assert_eq!(after.generation, before.generation);
    assert_eq!(
        serde_json::to_value(after.state).unwrap(),
        serde_json::to_value(before.state).unwrap()
    );
    assert!(controller.begin_stack_deployment("demo").is_ok());
}

#[tokio::test]
async fn deployment_validation_runs_cluster_dependent_checks() {
    let (controller, repository, _directory) = test_controller("deployment-preflight-test").await;
    controller
        .update_node_gateway("controller-a", NodeGatewayUpdate { enabled: false })
        .await
        .unwrap();
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
    let before = repository.load().await.unwrap();

    assert!(matches!(
        controller.validate_apply("demo", &parsed).await,
        Err(ControllerError::Invalid(message))
            if message.contains("no node has its gateway enabled")
    ));

    let after = repository.load().await.unwrap();
    assert_eq!(after.generation, before.generation);
    assert!(after.state.stacks.is_empty());
    assert!(after.state.services.is_empty());
}

#[tokio::test]
async fn swarm_style_mutations_reuse_the_stack_deployment_state_machine() {
    let (controller, _, _directory) = test_controller("service-mutation-test").await;
    register_live_node(&controller).await;
    controller.apply("demo", parsed_test_stack()).await.unwrap();
    assert_eq!(
        controller.target_tasks("demo").await.unwrap().tasks.len(),
        1
    );
    assert_eq!(
        controller
            .target_tasks("demo.web")
            .await
            .unwrap()
            .tasks
            .len(),
        1
    );
    let initial_task_id = {
        let mut inner = controller.inner.lock().await;
        inner
            .state
            .stacks
            .get_mut("demo")
            .unwrap()
            .deployment
            .as_mut()
            .unwrap()
            .status = StackDeploymentStatus::Healthy;
        inner.state.tasks.values().next().unwrap().id.clone()
    };

    let initial_revision = controller
        .inner
        .lock()
        .await
        .state
        .services
        .get("demo.web")
        .unwrap()
        .revision;
    let scaled = controller.scale_service("demo.web", 3).await.unwrap();
    assert_eq!(scaled.stack, "demo");
    assert_eq!(scaled.services[0].replicas, 3);
    let scaled_revision = {
        let mut inner = controller.inner.lock().await;
        let service = inner.state.services.get("demo.web").unwrap();
        assert_eq!(service.spec.replicas, 3);
        assert_eq!(service.revision, initial_revision);
        assert_eq!(inner.state.tasks.len(), 3);
        assert_eq!(
            inner.state.tasks.get(&initial_task_id).unwrap().desired,
            DesiredTaskState::Running
        );
        let revision = service.revision;
        inner
            .state
            .stacks
            .get_mut("demo")
            .unwrap()
            .deployment
            .as_mut()
            .unwrap()
            .status = StackDeploymentStatus::Healthy;
        revision
    };

    controller.force_update_service("demo.web").await.unwrap();
    {
        let mut inner = controller.inner.lock().await;
        assert_eq!(
            inner.state.services.get("demo.web").unwrap().revision,
            scaled_revision + 1
        );
        inner
            .state
            .stacks
            .get_mut("demo")
            .unwrap()
            .deployment
            .as_mut()
            .unwrap()
            .status = StackDeploymentStatus::Healthy;
    }

    let removed = controller.remove_stack("demo").await.unwrap();
    assert!(removed.services.is_empty());
    assert!(controller.list_stacks().await.stacks.is_empty());
    let inner = controller.inner.lock().await;
    assert!(inner.state.services.get("demo.web").unwrap().deleted);
}

#[tokio::test]
async fn task_target_rejects_a_stack_and_service_name_collision() {
    let (controller, _, _directory) = test_controller("task-target-conflict-test").await;
    register_live_node(&controller).await;
    controller.apply("demo", parsed_test_stack()).await.unwrap();
    controller
        .apply("demo.web", parsed_test_stack())
        .await
        .unwrap();

    assert!(matches!(
        controller.target_tasks("demo.web").await,
        Err(ControllerError::Conflict(message))
            if message == "target \"demo.web\" matches both a Stack and a Service"
    ));
}

#[tokio::test]
async fn log_data_session_is_dispatched_over_the_agent_command_channel() {
    let (controller, _, _directory) = test_controller("command-log-test").await;
    register_live_node(&controller).await;
    controller.apply("demo", parsed_test_stack()).await.unwrap();

    let session = controller
        .create_data_session(crate::model::DataSessionOperation::Logs {
            target: "demo.web".into(),
            tail: 25,
            follow: true,
        })
        .await
        .unwrap();
    let command = controller
        .commands
        .next("node-a", std::time::Duration::from_secs(1))
        .await
        .unwrap();
    assert!(matches!(
        &command.operation,
        crate::model::AgentCommandOperation::OpenDataSession {
            session_id,
            streams,
            ..
        } if session_id == &session.session_id
            && matches!(
                streams.as_slice(),
                [crate::model::AgentDataStream {
                    operation: crate::model::AgentDataStreamOperation::Logs {
                        tail: 25,
                        follow: true,
                    },
                    ..
                }]
            )
    ));
    assert!(controller.commands.complete(
        "node-a",
        &command.id,
        crate::model::AgentCommandResult { error: None },
    ));
    assert_eq!(session.streams.len(), 1);
}

#[tokio::test]
async fn websocket_data_session_relays_binary_frames_without_conversion() {
    let (controller, _, _directory) = test_controller("binary-relay-test").await;
    register_live_node(&controller).await;
    controller.apply("demo", parsed_test_stack()).await.unwrap();
    let controller = std::sync::Arc::new(controller);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server_controller = controller.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, api::router(server_controller))
            .await
            .unwrap();
    });
    let client =
        crate::client::ControllerClient::new(format!("http://{address}"), "0123456789abcdef");
    let session: crate::model::DataSessionCreateResponse = client
        .send_json(
            reqwest::Method::POST,
            "/v1/data-sessions",
            Some(&crate::model::DataSessionOperation::Logs {
                target: "demo.web".into(),
                tail: 10,
                follow: false,
            }),
        )
        .await
        .unwrap();
    let command = controller
        .commands
        .next("node-a", std::time::Duration::from_secs(1))
        .await
        .unwrap();
    let (upload_token, stream_id) = match &command.operation {
        crate::model::AgentCommandOperation::OpenDataSession {
            upload_token,
            streams,
            ..
        } => (upload_token.clone(), streams[0].stream_id),
    };
    let mut agent = client
        .connect_data_websocket(
            &format!("/v1/data-sessions/{}/nodes/node-a", session.session_id),
            &upload_token,
        )
        .await
        .unwrap();
    let mut cli = client
        .connect_data_websocket(
            &format!("/v1/data-sessions/{}/client", session.session_id),
            &session.attach_token,
        )
        .await
        .unwrap();
    assert!(controller.commands.complete(
        "node-a",
        &command.id,
        crate::model::AgentCommandResult { error: None },
    ));

    let payload = bytes::Bytes::from_static(&[0, 255, b'\n', 128, 0]);
    let frame = crate::data_plane::DataFrame::data(
        stream_id,
        0,
        crate::data_plane::DataChannel::Stdout,
        payload.clone(),
    );
    agent
        .send(tokio_tungstenite::tungstenite::Message::Binary(
            frame.encode().unwrap().into(),
        ))
        .await
        .unwrap();

    let relayed = match cli.next().await.unwrap().unwrap() {
        tokio_tungstenite::tungstenite::Message::Binary(encoded) => {
            crate::data_plane::DataFrame::decode(&encoded).unwrap()
        }
        message => panic!("expected binary frame, got {message:?}"),
    };
    assert_eq!(relayed.payload, payload);

    let _ = cli.close(None).await;
    let agent_closed = tokio::time::timeout(std::time::Duration::from_secs(1), agent.next())
        .await
        .expect("client disconnect should cancel the Agent data stream");
    assert!(matches!(
        agent_closed,
        None | Some(Ok(tokio_tungstenite::tungstenite::Message::Close(_))) | Some(Err(_))
    ));
    server.abort();
}

async fn register_live_node(controller: &Controller) {
    controller
        .join_node("node-a", test_join_request("node-a", "127.0.0.1"))
        .await
        .unwrap();
    controller
        .heartbeat(
            "node-a",
            NodeHeartbeat {
                node: test_node(),
                tasks: Vec::new(),
                task_inventory_error: None,
                task_results: Vec::new(),
                gateway: GatewayReport::default(),
            },
        )
        .await
        .unwrap();
}

fn parsed_test_stack() -> ParsedStack {
    ParsedStack {
        services: BTreeMap::from([("web".into(), test_service().spec)]),
        gateway: StackGatewaySpec::default(),
    }
}

#[tokio::test]
async fn redeploy_rolls_a_service_whose_pull_policy_refreshes_cached_images() {
    let (controller, _, _directory) = test_controller("pull-policy-redeploy-test").await;
    register_live_node(&controller).await;
    let mut parsed = parsed_test_stack();
    parsed.services.get_mut("web").unwrap().pull_policy = PullPolicy::Always;

    controller.apply("demo", parsed.clone()).await.unwrap();
    let initial_revision = {
        let mut inner = controller.inner.lock().await;
        inner
            .state
            .stacks
            .get_mut("demo")
            .unwrap()
            .deployment
            .as_mut()
            .unwrap()
            .status = StackDeploymentStatus::Healthy;
        inner.state.services["demo.web"].revision
    };

    controller.apply("demo", parsed).await.unwrap();
    assert_eq!(
        controller.inner.lock().await.state.services["demo.web"].revision,
        initial_revision + 1
    );
}

#[tokio::test]
async fn deployment_waits_for_agent_application_and_health() {
    let (controller, _, _directory) = test_controller("deployment-success-test").await;
    register_live_node(&controller).await;
    let accepted = controller.apply("demo", parsed_test_stack()).await.unwrap();
    assert_eq!(accepted.status, StackDeploymentStatus::Deploying);
    assert_eq!(accepted.services[0].healthy, 0);

    let task = {
        let inner = controller.inner.lock().await;
        inner.state.tasks.values().next().unwrap().clone()
    };
    let service = test_service();
    controller
        .heartbeat(
            "node-a",
            NodeHeartbeat {
                node: test_node(),
                tasks: vec![TaskReport {
                    id: task.id.clone(),
                    observed: ObservedTaskState::Healthy,
                    container_id: Some("container-new".into()),
                    cluster_id: Some("deployment-success-test".into()),
                    stack: Some("demo".into()),
                    service: Some("web".into()),
                    slot: Some(task.slot),
                    revision: Some(task.revision),
                    spec_hash: Some(service_spec_hash(&service.spec)),
                    ports: task.ports,
                }],
                task_inventory_error: None,
                task_results: vec![TaskReconcileReport {
                    task_id: task.id,
                    desired_generation: accepted.generation,
                    applied_generation: Some(accepted.generation),
                    phase: crate::model::TaskReconcilePhase::Create,
                    error: None,
                }],
                gateway: GatewayReport::default(),
            },
        )
        .await
        .unwrap();

    let completed = controller
        .wait_for_deployment("demo", accepted.generation, None, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(completed.status, StackDeploymentStatus::Healthy);
    assert_eq!(completed.services[0].applied, 1);
    assert_eq!(completed.services[0].healthy, 1);
}

#[tokio::test]
async fn deployment_failure_is_persisted_and_releases_stack_name() {
    let (controller, repository, _directory) = test_controller("deployment-failure-test").await;
    register_live_node(&controller).await;
    let accepted = controller.apply("demo", parsed_test_stack()).await.unwrap();
    let task_id = controller
        .inner
        .lock()
        .await
        .state
        .tasks
        .keys()
        .next()
        .unwrap()
        .clone();

    assert!(matches!(
        controller.apply("demo", parsed_test_stack()).await,
        Err(ControllerError::Conflict(_))
    ));
    controller
        .heartbeat(
            "node-a",
            NodeHeartbeat {
                node: test_node(),
                tasks: Vec::new(),
                task_inventory_error: None,
                task_results: vec![TaskReconcileReport {
                    task_id,
                    desired_generation: accepted.generation,
                    applied_generation: None,
                    phase: crate::model::TaskReconcilePhase::Create,
                    error: Some("image pull denied".into()),
                }],
                gateway: GatewayReport::default(),
            },
        )
        .await
        .unwrap();

    let failed = controller
        .wait_for_deployment(
            "demo",
            accepted.generation,
            Some(accepted.revision),
            Duration::ZERO,
        )
        .await
        .unwrap();
    assert_eq!(failed.status, StackDeploymentStatus::Failed);
    assert_eq!(failed.errors[0].message, "image pull denied");
    let persisted = repository.load().await.unwrap();
    assert_eq!(
        persisted.state.stacks["demo"]
            .deployment
            .as_ref()
            .unwrap()
            .status,
        StackDeploymentStatus::Failed
    );
    assert!(controller.apply("demo", parsed_test_stack()).await.is_ok());
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
    expose: [80]
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
            deployment: None,
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
                task_inventory_error: None,
                task_results: Vec::new(),
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
    config.gateway_drain_timeout_seconds = 3;
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
                task_inventory_error: None,
                task_results: Vec::new(),
                gateway: GatewayReport::default(),
            },
        )
        .await
        .unwrap()
        .gateway_config
        .unwrap();
    assert_eq!(
        desired.config["storage"]["controller"],
        "http://10.0.0.10:17080"
    );
    assert!(desired.config["storage"].get("controllers").is_none());
    assert_eq!(
        desired.config["apps"]["http"]["servers"]["swarmlite"]["listen"][0],
        ":18089"
    );
    assert_eq!(desired.config["admin"]["listen"], "0.0.0.0:2019");

    controller
        .heartbeat(
            "node-a",
            NodeHeartbeat {
                node: test_node(),
                tasks: vec![report],
                task_inventory_error: None,
                task_results: Vec::new(),
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

#[tokio::test]
async fn gateway_generation_tracks_rendered_config_only() {
    let (controller, _, _directory) = test_controller("gateway-generation-test").await;
    let mut inner = controller.inner.lock().await;
    let initial_generation = inner.gateway_generation;

    inner.state.nodes.insert("node-a".into(), test_node());
    controller.refresh_gateway_snapshot(&mut inner).unwrap();
    assert_eq!(inner.gateway_generation, initial_generation);

    inner.cluster.gateway.listen = vec![":18090".into()];
    controller.refresh_gateway_snapshot(&mut inner).unwrap();
    assert_eq!(inner.gateway_generation, initial_generation + 1);
    assert_eq!(
        inner.gateway_config["apps"]["http"]["servers"]["swarmlite"]["listen"][0],
        ":18090"
    );

    controller.refresh_gateway_snapshot(&mut inner).unwrap();
    assert_eq!(inner.gateway_generation, initial_generation + 1);
}

fn test_service() -> ServiceRecord {
    ServiceRecord {
        id: "demo.web".into(),
        stack: "demo".into(),
        name: "web".into(),
        revision: 2,
        spec: ServiceSpec {
            image: "nginx:1.29-alpine".into(),
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
                task_inventory_error: None,
                task_results: Vec::new(),
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
    assert_eq!(inner.state.services["demo.web"].revision, 7);
    assert_eq!(inner.state.tasks["existing-task"].revision, 7);
    assert!(inner.state.unclaimed_tasks.is_empty());
}

#[tokio::test]
async fn recovery_restores_the_latest_service_revision_and_leaves_old_tasks_unclaimed() {
    let (controller, _, _directory) = test_controller("revision-recovery-test").await;
    controller
        .join_node("node-a", test_join_request("node-a", "127.0.0.1"))
        .await
        .unwrap();
    let mut service = test_service();
    service.spec.replicas = 3;
    service.spec.service_labels.clear();
    let spec_hash = service_spec_hash(&service.spec);
    let report = |id: &str, slot: u32, revision: u64, observed: ObservedTaskState| TaskReport {
        id: id.into(),
        observed,
        container_id: Some(format!("container-{id}")),
        cluster_id: Some("revision-recovery-test".into()),
        stack: Some("demo".into()),
        service: Some("web".into()),
        slot: Some(slot),
        revision: Some(revision),
        spec_hash: Some(spec_hash.clone()),
        ports: vec![PortBinding {
            target: 80,
            published: 20_001 + u16::try_from(slot).unwrap(),
            protocol: "tcp".into(),
        }],
    };
    controller
        .heartbeat(
            "node-a",
            NodeHeartbeat {
                node: test_node(),
                tasks: vec![
                    report("revision-7-slot-0", 0, 7, ObservedTaskState::Healthy),
                    report("revision-7-slot-1", 1, 7, ObservedTaskState::Running),
                    report("revision-6-slot-2", 2, 6, ObservedTaskState::Healthy),
                    report("revision-99-slot-9", 9, 99, ObservedTaskState::Healthy),
                ],
                task_inventory_error: None,
                task_results: Vec::new(),
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
    assert_eq!(inner.state.services["demo.web"].revision, 7);
    assert!(inner.state.tasks.contains_key("revision-7-slot-0"));
    assert!(inner.state.tasks.contains_key("revision-7-slot-1"));
    assert!(!inner.state.tasks.contains_key("revision-6-slot-2"));
    assert!(
        inner
            .state
            .unclaimed_tasks
            .contains_key("revision-6-slot-2")
    );
    assert!(
        inner
            .state
            .unclaimed_tasks
            .contains_key("revision-99-slot-9")
    );
    let replacement = inner
        .state
        .tasks
        .values()
        .find(|task| task.service_id == "demo.web" && task.slot == 2)
        .unwrap();
    assert_eq!(replacement.revision, 7);
}

#[tokio::test]
async fn recovery_ignores_invalid_service_revisions() {
    let (controller, _, _directory) = test_controller("invalid-revision-recovery-test").await;
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
                tasks: [("revision-zero", 0_u64), ("revision-max", u64::MAX)]
                    .into_iter()
                    .map(|(id, revision)| TaskReport {
                        id: id.into(),
                        observed: ObservedTaskState::Healthy,
                        container_id: Some(format!("container-{id}")),
                        cluster_id: Some("invalid-revision-recovery-test".into()),
                        stack: Some("demo".into()),
                        service: Some("web".into()),
                        slot: Some(0),
                        revision: Some(revision),
                        spec_hash: Some(spec_hash.clone()),
                        ports: vec![PortBinding {
                            target: 80,
                            published: 20_001,
                            protocol: "tcp".into(),
                        }],
                    })
                    .collect(),
                task_inventory_error: None,
                task_results: Vec::new(),
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
    assert_eq!(inner.state.services["demo.web"].revision, 1);
    assert!(inner.state.unclaimed_tasks.contains_key("revision-zero"));
    assert!(inner.state.unclaimed_tasks.contains_key("revision-max"));
    assert!(!inner.state.tasks.contains_key("revision-zero"));
    assert!(!inner.state.tasks.contains_key("revision-max"));
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
        applied_generation: None,
        reconcile_error: None,
    }
}
