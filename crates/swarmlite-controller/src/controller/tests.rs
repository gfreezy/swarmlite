use base64::{Engine as _, engine::general_purpose::STANDARD};
use futures_util::{SinkExt, StreamExt};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};
use swarmlite_stack::{ParsedStack, parse_stack};

use crate::{
    config::{DEFAULT_CONTROLLER_PORT, DEFAULT_GATEWAY_DRAIN_TIMEOUT_SECONDS},
    model::{
        CLUSTER_SCHEMA_VERSION, ClusterGatewayConfig, HttpBackendProtocol,
        ImageResolutionServiceReport, KvLockStatus, NodeRecord, PortBinding, PullPolicy,
        ServiceConfigMount, ServicePort, ServicePortKey, ServiceSpec, StackGatewaySpec, TaskRecord,
        TaskReport,
    },
};

use super::*;

fn test_cluster(id: &str) -> ClusterSettings {
    ClusterSettings {
        schema_version: CLUSTER_SCHEMA_VERSION,
        cluster_id: id.into(),
        controller_id: "controller-a".into(),
        controller_port: DEFAULT_CONTROLLER_PORT,
        proxy: Default::default(),
        gateway: ClusterGatewayConfig::default(),
        deployment: Default::default(),
    }
}

fn test_controller_config(cluster: &ClusterSettings, directory: &Path) -> ControllerConfig {
    ControllerConfig {
        gateway_enabled: true,
        labels: BTreeMap::new(),
        listen: "127.0.0.1:0".parse().unwrap(),
        advertise_url: "http://10.0.0.10:17080".into(),
        node_timeout_seconds: 20,
        reconcile_interval_seconds: 1,
        gateway_drain_timeout_seconds: DEFAULT_GATEWAY_DRAIN_TIMEOUT_SECONDS,
        image_cache_dir: directory.join("image-cache"),
        cluster: cluster.clone(),
    }
}

async fn test_controller(id: &str) -> (Controller, StateRepository, tempfile::TempDir) {
    let cluster = test_cluster(id);
    let directory = tempfile::tempdir().unwrap();
    let repository = StateRepository::open(directory.path(), cluster.clone()).unwrap();
    let controller = Controller::new(
        test_controller_config(&cluster, directory.path()),
        "0123456789abcdef".into(),
        repository.clone(),
    )
    .await
    .unwrap();
    (controller, repository, directory)
}

#[tokio::test]
async fn image_registry_v2_reuses_controller_authentication() {
    let (controller, _, _directory) = test_controller("image-registry-api-test").await;
    let app = super::api::router(std::sync::Arc::new(controller));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = reqwest::Client::new();

    let unauthorized = client
        .get(format!("http://{address}/v2/"))
        .send()
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), reqwest::StatusCode::UNAUTHORIZED);

    let authorized = client
        .head(format!("http://{address}/v2/"))
        .bearer_auth("0123456789abcdef")
        .send()
        .await
        .unwrap();
    server.abort();
    assert_eq!(authorized.status(), reqwest::StatusCode::OK);
    assert_eq!(
        authorized
            .headers()
            .get("docker-distribution-api-version")
            .unwrap(),
        "registry/2.0"
    );
    assert!(authorized.headers().contains_key("x-swarmlite-image-proxy"));
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
                task_progress: Vec::new(),
                image_results: Vec::new(),
                image_progress: Vec::new(),
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

    let status = controller.status().await;
    assert!(status.state.registry_credentials.is_empty());
    assert!(
        !serde_json::to_string(&status)
            .unwrap()
            .contains("private-token")
    );
}

#[tokio::test]
async fn stack_registry_credentials_are_atomically_persisted_and_not_removed_when_omitted() {
    let (controller, repository, _directory) = test_controller("stack-registry-test").await;
    controller
        .apply_with_registry_credentials(
            "demo",
            parsed_test_stack(),
            BTreeMap::from([(
                "ghcr.io".into(),
                RegistryCredential {
                    username: "octocat".into(),
                    password: "private-token".into(),
                },
            )]),
        )
        .await
        .unwrap();

    let persisted = repository.load().await.unwrap();
    assert_eq!(
        persisted.state.registry_credentials["ghcr.io"].password,
        "private-token"
    );
    let joined = controller
        .join_node("node-a", test_join_request("node-a", "127.0.0.1"))
        .await
        .unwrap();
    assert_eq!(
        joined.registry_credentials["ghcr.io"].password,
        "private-token"
    );

    controller
        .apply("other", parsed_test_stack())
        .await
        .unwrap();
    let persisted = repository.load().await.unwrap();
    assert_eq!(
        persisted.state.registry_credentials["ghcr.io"].password,
        "private-token"
    );
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
    assert_eq!(controller.list_tasks().await.tasks.len(), 1);
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
async fn resource_type_mismatches_explain_expected_targets() {
    let (controller, _, _directory) = test_controller("typed-target-errors-test").await;
    register_live_node(&controller).await;
    controller.apply("demo", parsed_test_stack()).await.unwrap();
    let task_id = controller
        .inner
        .lock()
        .await
        .state
        .tasks
        .values()
        .next()
        .unwrap()
        .id
        .clone();

    for error in [
        controller.inspect_service("demo").await.unwrap_err(),
        controller.scale_service("demo", 2).await.unwrap_err(),
        controller.force_update_service("demo").await.unwrap_err(),
    ] {
        assert!(matches!(
            error,
            ControllerError::Invalid(message)
                if message.contains("expects a Service (STACK.SERVICE)")
                    && message.contains("\"demo\" is a Stack")
                    && message.contains("demo.web")
        ));
    }

    assert!(matches!(
        controller.list_services(Some("demo.web")).await,
        Err(ControllerError::Invalid(message))
            if message.contains("ls expects a Stack name")
                && message.contains("\"demo.web\" is a Service")
                && message.contains("use \"demo\" instead")
    ));
    assert!(matches!(
        controller.remove_stack("demo.web").await,
        Err(ControllerError::Invalid(message))
            if message.contains("rm expects a Stack name")
                && message.contains("use \"demo\" instead")
    ));
    assert!(matches!(
        controller.target_tasks(&task_id).await,
        Err(ControllerError::Invalid(message))
            if message.contains("ps expects a Stack or Service")
                && message.contains("identifies a Task")
    ));
    assert!(matches!(
        controller
            .create_data_session(crate::model::DataSessionOperation::Logs {
                target: "demo".into(),
                tail: 10,
                follow: false,
            })
            .await,
        Err(ControllerError::Invalid(message))
            if message.contains("logs expects a Service or Task")
                && message.contains("\"demo\" is a Stack")
                && message.contains("swarmlite ps demo")
    ));
    assert!(matches!(
        controller.inspect_service(&task_id).await,
        Err(ControllerError::Invalid(message))
            if message.contains("inspect expects a Service")
                && message.contains("identifies a Task")
    ));
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
                task_progress: Vec::new(),
                image_results: Vec::new(),
                image_progress: Vec::new(),
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

fn recovered_task_report(
    cluster_id: &str,
    stack: &str,
    id: &str,
    revision: u64,
    spec: &ServiceSpec,
    published_port: u16,
) -> TaskReport {
    TaskReport {
        id: id.into(),
        observed: ObservedTaskState::Healthy,
        container_id: Some(format!("container-{id}")),
        image_id: None,
        cluster_id: Some(cluster_id.into()),
        stack: Some(stack.into()),
        service: Some("web".into()),
        slot: Some(0),
        revision: Some(revision),
        spec_hash: Some(service_spec_hash(spec)),
        ports: vec![PortBinding {
            target: 80,
            published: Some(published_port),
            protocol: "tcp".into(),
        }],
        config_digests: Vec::new(),
    }
}

fn image_test_heartbeat(
    task: &TaskRecord,
    desired_generation: Option<u64>,
    image_result: Option<ImageResolutionReport>,
) -> NodeHeartbeat {
    image_test_heartbeat_on(
        test_node(),
        task,
        "sha256:current",
        desired_generation,
        image_result,
    )
}

fn image_test_heartbeat_on(
    node: NodeRecord,
    task: &TaskRecord,
    image_id: &str,
    desired_generation: Option<u64>,
    image_result: Option<ImageResolutionReport>,
) -> NodeHeartbeat {
    NodeHeartbeat {
        node,
        tasks: vec![TaskReport {
            id: task.id.clone(),
            observed: ObservedTaskState::Healthy,
            container_id: Some("container-web".into()),
            image_id: Some(image_id.into()),
            cluster_id: Some("image-resolution-test".into()),
            stack: Some("demo".into()),
            service: Some("web".into()),
            slot: Some(task.slot),
            revision: Some(task.revision),
            spec_hash: Some("current-spec".into()),
            ports: task.ports.clone(),
            config_digests: task.config_digests.clone(),
        }],
        task_inventory_error: None,
        task_results: desired_generation
            .map(|generation| {
                vec![TaskReconcileReport {
                    task_id: task.id.clone(),
                    desired_generation: generation,
                    applied_generation: Some(generation),
                    phase: crate::model::TaskReconcilePhase::Verify,
                    error: None,
                }]
            })
            .unwrap_or_default(),
        task_progress: Vec::new(),
        image_results: image_result.into_iter().collect(),
        image_progress: Vec::new(),
        gateway: GatewayReport::default(),
    }
}

async fn prepare_image_redeploy(
    controller: &Controller,
    pull_policy: PullPolicy,
    image: &str,
) -> (ParsedStack, TaskRecord, u64) {
    let mut parsed = parsed_test_stack();
    let service = parsed.services.get_mut("web").unwrap();
    service.image = image.to_owned();
    service.pull_policy = pull_policy;
    controller.apply("demo", parsed.clone()).await.unwrap();
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
    let revision = inner.state.services["demo.web"].revision;
    let task = inner
        .state
        .tasks
        .values_mut()
        .find(|task| task.service_id == "demo.web")
        .unwrap();
    task.observed = ObservedTaskState::Healthy;
    task.container_id = Some("container-web".into());
    (parsed, task.clone(), revision)
}

#[tokio::test]
async fn config_digest_changes_roll_the_service_but_identical_content_does_not() {
    let (controller, _, _directory) = test_controller("config-rollout-test").await;
    register_live_node(&controller).await;
    let mut parsed = parsed_test_stack();
    parsed.services.get_mut("web").unwrap().configs = vec![ServiceConfigMount {
        source: "index-html".into(),
        target: "/usr/share/nginx/html/index.html".into(),
        uid: None,
        gid: None,
        mode: 0o444,
        digest: "a".repeat(64),
    }];
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

    controller.apply("demo", parsed.clone()).await.unwrap();
    {
        let mut inner = controller.inner.lock().await;
        assert_eq!(inner.state.services["demo.web"].revision, initial_revision);
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

    parsed.services.get_mut("web").unwrap().configs[0].digest = "b".repeat(64);
    controller.apply("demo", parsed).await.unwrap();

    let inner = controller.inner.lock().await;
    assert_eq!(
        inner.state.services["demo.web"].revision,
        initial_revision + 1
    );
    let referenced = lifecycle::referenced_config_digests(&inner.state);
    assert!(referenced.contains(&"a".repeat(64)));
    assert!(referenced.contains(&"b".repeat(64)));
    assert!(inner.state.tasks.values().any(|task| {
        task.revision == initial_revision && task.config_digests == vec!["a".repeat(64)]
    }));
}

#[test]
fn config_blob_references_include_offline_and_recovery_containers() {
    let mut state = ClusterState::default();
    let mut service = test_service();
    service.spec.configs = vec![ServiceConfigMount {
        source: "current".into(),
        target: "/etc/current".into(),
        uid: None,
        gid: None,
        mode: 0o444,
        digest: "c".repeat(64),
    }];
    state.services.insert(service.id.clone(), service);
    let mut offline = draining_task();
    offline.desired = DesiredTaskState::Stopped;
    offline.observed = ObservedTaskState::Lost;
    offline.config_digests = vec!["a".repeat(64)];
    state.tasks.insert(offline.id.clone(), offline);
    state.unclaimed_tasks.insert(
        "rollback-container".into(),
        UnclaimedTask {
            id: "rollback-container".into(),
            stack: "demo".into(),
            service: "web".into(),
            slot: 0,
            revision: 1,
            spec_hash: "old-spec".into(),
            node_id: "offline-node".into(),
            observed: ObservedTaskState::Healthy,
            ports: Vec::new(),
            config_digests: vec!["b".repeat(64)],
            container_id: Some("container-old".into()),
        },
    );

    assert_eq!(
        lifecycle::referenced_config_digests(&state),
        BTreeSet::from(["a".repeat(64), "b".repeat(64), "c".repeat(64)])
    );
}

#[tokio::test]
async fn latest_redeploy_keeps_revision_when_pulled_image_id_is_unchanged() {
    let (controller, _, _directory) = test_controller("image-unchanged-test").await;
    register_live_node(&controller).await;
    let (parsed, task, initial_revision) =
        prepare_image_redeploy(&controller, PullPolicy::Missing, "nginx:latest").await;

    let accepted = controller.apply("demo", parsed).await.unwrap();
    let assignment = controller
        .heartbeat("node-a", image_test_heartbeat(&task, None, None))
        .await
        .unwrap();
    assert_eq!(assignment.image_assignments.len(), 1);
    let report = ImageResolutionReport {
        deployment_generation: accepted.generation,
        image: "nginx:latest".into(),
        resolved_image_id: Some("sha256:current".into()),
        services: vec![ImageResolutionServiceReport {
            service_id: "demo.web".into(),
            old_image_ids: BTreeMap::from([(task.id.clone(), "sha256:current".into())]),
            changed: false,
        }],
        error: None,
    };
    let response = controller
        .heartbeat(
            "node-a",
            image_test_heartbeat(&task, Some(accepted.generation), Some(report)),
        )
        .await
        .unwrap();

    let inner = controller.inner.lock().await;
    assert_eq!(inner.state.services["demo.web"].revision, initial_revision);
    assert_eq!(inner.state.tasks.len(), 1);
    assert!(response.image_assignments.is_empty());
    assert_eq!(
        inner.state.stacks["demo"]
            .deployment
            .as_ref()
            .unwrap()
            .image_resolutions["demo.web"]
            .status,
        ImageResolutionStatus::Unchanged
    );
}

#[tokio::test]
async fn image_ids_are_compared_per_node_for_heterogeneous_runtimes() {
    let (controller, _, _directory) = test_controller("heterogeneous-images-test").await;
    register_live_node(&controller).await;
    let mut node_b = test_node();
    node_b.id = "node-b".into();
    node_b.address = "127.0.0.2".into();
    controller
        .join_node("node-b", test_join_request("node-b", "127.0.0.2"))
        .await
        .unwrap();
    controller
        .heartbeat(
            "node-b",
            NodeHeartbeat {
                node: node_b.clone(),
                tasks: Vec::new(),
                task_inventory_error: None,
                task_results: Vec::new(),
                task_progress: Vec::new(),
                image_results: Vec::new(),
                image_progress: Vec::new(),
                gateway: GatewayReport::default(),
            },
        )
        .await
        .unwrap();
    let mut parsed = parsed_test_stack();
    let service = parsed.services.get_mut("web").unwrap();
    service.image = "nginx:latest".into();
    service.pull_policy = PullPolicy::Missing;
    service.replicas = 2;
    controller.apply("demo", parsed.clone()).await.unwrap();
    let (tasks, initial_revision) = {
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
        for task in inner.state.tasks.values_mut() {
            task.observed = ObservedTaskState::Healthy;
            task.container_id = Some(format!("container-{}", task.id));
        }
        (
            inner
                .state
                .tasks
                .values()
                .map(|task| (task.node_id.clone(), task.clone()))
                .collect::<BTreeMap<_, _>>(),
            inner.state.services["demo.web"].revision,
        )
    };
    assert_eq!(tasks.len(), 2);
    let accepted = controller.apply("demo", parsed).await.unwrap();

    for (node_id, old_image_id) in [
        ("node-a", "sha256:amd64-image"),
        ("node-b", "sha256:arm64-image"),
    ] {
        let task = &tasks[node_id];
        let report = ImageResolutionReport {
            deployment_generation: accepted.generation,
            image: "nginx:latest".into(),
            resolved_image_id: Some(old_image_id.into()),
            services: vec![ImageResolutionServiceReport {
                service_id: "demo.web".into(),
                old_image_ids: BTreeMap::from([(task.id.clone(), old_image_id.into())]),
                changed: false,
            }],
            error: None,
        };
        let node = if node_id == "node-a" {
            test_node()
        } else {
            node_b.clone()
        };
        controller
            .heartbeat(
                node_id,
                image_test_heartbeat_on(
                    node,
                    task,
                    old_image_id,
                    Some(accepted.generation),
                    Some(report),
                ),
            )
            .await
            .unwrap();
        assert_eq!(
            controller.inner.lock().await.state.services["demo.web"].revision,
            initial_revision
        );
    }

    let inner = controller.inner.lock().await;
    let resolution = &inner.state.stacks["demo"]
        .deployment
        .as_ref()
        .unwrap()
        .image_resolutions["demo.web"];
    assert_eq!(resolution.status, ImageResolutionStatus::Unchanged);
    assert_eq!(
        resolution.nodes["node-a"].resolved_image_id.as_deref(),
        Some("sha256:amd64-image")
    );
    assert_eq!(
        resolution.nodes["node-b"].resolved_image_id.as_deref(),
        Some("sha256:arm64-image")
    );
}

#[test]
fn one_service_image_result_does_not_wait_for_other_services() {
    let mut state = ClusterState::default();
    for name in ["api", "worker"] {
        let mut service = test_service();
        service.id = format!("demo.{name}");
        service.name = name.into();
        service.revision = 1;
        service.spec.image = format!("example/{name}:latest");
        state.services.insert(service.id.clone(), service);
    }
    let resolution = |name: &str, node_id: &str, task_id: &str| DeploymentImageResolutionRecord {
        service_id: format!("demo.{name}"),
        service: name.into(),
        image: format!("example/{name}:latest"),
        baseline_revision: 1,
        status: ImageResolutionStatus::Checking,
        nodes: BTreeMap::from([(
            node_id.into(),
            DeploymentImageResolutionNodeRecord {
                task_ids: vec![task_id.into()],
                status: ImageResolutionStatus::Checking,
                old_image_ids: BTreeMap::new(),
                resolved_image_id: None,
                error: None,
            },
        )]),
    };
    state.stacks.insert(
        "demo".into(),
        StackRecord {
            name: "demo".into(),
            applied_at_unix_ms: 1,
            services: vec!["demo.api".into(), "demo.worker".into()],
            gateway: Default::default(),
            deployment: Some(StackDeploymentRecord {
                generation: 2,
                status: StackDeploymentStatus::Reconciling,
                started_at_unix_ms: 1,
                last_progress_at_unix_ms: 1,
                progress_deadline_seconds: 300,
                wait_for_gateway: false,
                finished_at_unix_ms: None,
                superseded_by: None,
                retry_revision: 0,
                errors: Vec::new(),
                image_resolutions: BTreeMap::from([
                    ("demo.api".into(), resolution("api", "node-a", "task-api")),
                    (
                        "demo.worker".into(),
                        resolution("worker", "node-b", "task-worker"),
                    ),
                ]),
                conditions: Vec::new(),
                snapshot: Default::default(),
            }),
            deployment_history: BTreeMap::new(),
        },
    );

    let changed = apply_image_resolution_report(
        &mut state,
        "node-a",
        &ImageResolutionReport {
            deployment_generation: 2,
            image: "example/api:latest".into(),
            resolved_image_id: Some("sha256:api-new".into()),
            services: vec![ImageResolutionServiceReport {
                service_id: "demo.api".into(),
                old_image_ids: BTreeMap::from([("task-api".into(), "sha256:api-old".into())]),
                changed: true,
            }],
            error: None,
        },
    );

    assert!(changed);
    assert_eq!(state.services["demo.api"].revision, 2);
    assert_eq!(state.services["demo.worker"].revision, 1);
    let deployment = state.stacks["demo"].deployment.as_ref().unwrap();
    assert_eq!(
        deployment.image_resolutions["demo.api"].status,
        ImageResolutionStatus::Changed
    );
    assert_eq!(
        deployment.image_resolutions["demo.worker"].status,
        ImageResolutionStatus::Checking
    );
    assert_eq!(deployment.status, StackDeploymentStatus::Reconciling);
}

#[test]
fn progress_deadline_stalls_without_finishing_and_progress_resumes_it() {
    let mut state = ClusterState::default();
    state.stacks.insert(
        "demo".into(),
        StackRecord {
            name: "demo".into(),
            applied_at_unix_ms: 1_000,
            services: vec!["demo.web".into()],
            gateway: Default::default(),
            deployment: Some(StackDeploymentRecord {
                generation: 2,
                status: StackDeploymentStatus::Reconciling,
                started_at_unix_ms: 1_000,
                last_progress_at_unix_ms: 1_000,
                progress_deadline_seconds: 5,
                wait_for_gateway: false,
                finished_at_unix_ms: None,
                superseded_by: None,
                retry_revision: 0,
                errors: Vec::new(),
                image_resolutions: BTreeMap::new(),
                conditions: Vec::new(),
                snapshot: Default::default(),
            }),
            deployment_history: BTreeMap::new(),
        },
    );

    assert!(!deployment::refresh_stack_deployments(
        &mut state, 5_999, false
    ));
    assert!(deployment::refresh_stack_deployments(
        &mut state, 6_000, false
    ));
    let stalled = state.stacks["demo"].deployment.as_ref().unwrap();
    assert_eq!(stalled.status, StackDeploymentStatus::Stalled);
    assert!(stalled.finished_at_unix_ms.is_none());
    assert_eq!(
        stalled.conditions[0].kind,
        StackDeploymentConditionKind::ProgressDeadlineExceeded
    );

    assert!(mark_deployment_progress(&mut state, "demo", 2, 7_000));
    let resumed = state.stacks["demo"].deployment.as_ref().unwrap();
    assert_eq!(resumed.status, StackDeploymentStatus::Reconciling);
    assert_eq!(resumed.last_progress_at_unix_ms, 7_000);
    assert!(resumed.conditions[0].resolved_at_unix_ms.is_some());
}

#[tokio::test]
async fn latest_redeploy_rolls_only_after_pulled_image_id_changes() {
    let (controller, _, _directory) = test_controller("image-changed-test").await;
    register_live_node(&controller).await;
    let (parsed, task, initial_revision) =
        prepare_image_redeploy(&controller, PullPolicy::Missing, "nginx:latest").await;

    let accepted = controller.apply("demo", parsed).await.unwrap();
    controller
        .heartbeat("node-a", image_test_heartbeat(&task, None, None))
        .await
        .unwrap();
    let report = ImageResolutionReport {
        deployment_generation: accepted.generation,
        image: "nginx:latest".into(),
        resolved_image_id: Some("sha256:new".into()),
        services: vec![ImageResolutionServiceReport {
            service_id: "demo.web".into(),
            old_image_ids: BTreeMap::from([(task.id.clone(), "sha256:current".into())]),
            changed: true,
        }],
        error: None,
    };
    let response = controller
        .heartbeat(
            "node-a",
            image_test_heartbeat(&task, Some(accepted.generation), Some(report)),
        )
        .await
        .unwrap();

    let inner = controller.inner.lock().await;
    assert_eq!(
        inner.state.services["demo.web"].revision,
        initial_revision + 1
    );
    assert_eq!(
        inner
            .state
            .tasks
            .values()
            .filter(|candidate| candidate.revision == initial_revision)
            .count(),
        1
    );
    assert!(inner.state.tasks.contains_key(&task.id));
    assert!(
        response
            .assignments
            .iter()
            .filter(|assignment| assignment.revision == initial_revision + 1)
            .all(|assignment| assignment.image_resolved)
    );
}

#[tokio::test]
async fn unchanged_never_and_fixed_missing_services_skip_image_pull() {
    for (name, pull_policy, image) in [
        ("never", PullPolicy::Never, "nginx:stable"),
        ("fixed-missing", PullPolicy::Missing, "nginx:1.27"),
    ] {
        let (controller, _, _directory) = test_controller(name).await;
        register_live_node(&controller).await;
        let (parsed, task, initial_revision) =
            prepare_image_redeploy(&controller, pull_policy, image).await;
        let accepted = controller.apply("demo", parsed).await.unwrap();
        let response = controller
            .heartbeat(
                "node-a",
                image_test_heartbeat(&task, Some(accepted.generation), None),
            )
            .await
            .unwrap();
        let inner = controller.inner.lock().await;
        assert!(response.image_assignments.is_empty(), "{name}");
        assert_eq!(
            inner.state.services["demo.web"].revision, initial_revision,
            "{name}"
        );
        assert_eq!(
            inner.state.stacks["demo"]
                .deployment
                .as_ref()
                .unwrap()
                .image_resolutions["demo.web"]
                .status,
            ImageResolutionStatus::Skipped,
            "{name}"
        );
    }
}

#[tokio::test]
async fn image_pull_failure_fails_deployment_without_replacing_running_task() {
    let (controller, _, _directory) = test_controller("image-pull-failure-test").await;
    register_live_node(&controller).await;
    let (parsed, task, initial_revision) =
        prepare_image_redeploy(&controller, PullPolicy::Always, "nginx:latest").await;
    let accepted = controller.apply("demo", parsed).await.unwrap();
    controller
        .heartbeat("node-a", image_test_heartbeat(&task, None, None))
        .await
        .unwrap();
    let report = ImageResolutionReport {
        deployment_generation: accepted.generation,
        image: "nginx:latest".into(),
        resolved_image_id: None,
        services: Vec::new(),
        error: Some("registry unavailable".into()),
    };
    let response = controller
        .heartbeat(
            "node-a",
            image_test_heartbeat(&task, Some(accepted.generation), Some(report)),
        )
        .await
        .unwrap();
    let inner = controller.inner.lock().await;
    assert_eq!(inner.state.services["demo.web"].revision, initial_revision);
    assert_eq!(inner.state.tasks.len(), 1);
    assert_eq!(
        inner.state.tasks[&task.id].desired,
        DesiredTaskState::Running
    );
    assert!(response.remove_tasks.is_empty());
    assert_eq!(
        inner.state.stacks["demo"]
            .deployment
            .as_ref()
            .unwrap()
            .status,
        StackDeploymentStatus::Failed
    );
}

#[tokio::test]
async fn restart_forces_revision_without_image_resolution() {
    let (controller, _, _directory) = test_controller("forced-restart-test").await;
    register_live_node(&controller).await;
    let (_parsed, _task, initial_revision) =
        prepare_image_redeploy(&controller, PullPolicy::Always, "nginx:latest").await;

    let response = controller.force_update_service("demo.web").await.unwrap();
    let inner = controller.inner.lock().await;
    assert_eq!(
        inner.state.services["demo.web"].revision,
        initial_revision + 1
    );
    assert!(response.image_resolutions.is_empty());
}

#[tokio::test]
async fn deployment_waits_for_agent_application_and_health() {
    let (controller, _, _directory) = test_controller("deployment-success-test").await;
    register_live_node(&controller).await;
    let accepted = controller.apply("demo", parsed_test_stack()).await.unwrap();
    assert_eq!(accepted.status, StackDeploymentStatus::Reconciling);
    assert_eq!(accepted.services[0].healthy, 0);

    let task = {
        let inner = controller.inner.lock().await;
        inner.state.tasks.values().next().unwrap().clone()
    };
    controller
        .heartbeat(
            "node-a",
            NodeHeartbeat {
                node: test_node(),
                tasks: Vec::new(),
                task_inventory_error: None,
                task_results: Vec::new(),
                task_progress: vec![TaskReconcileProgress {
                    task_id: task.id.clone(),
                    desired_generation: accepted.generation,
                    phase: crate::model::TaskReconcilePhase::Pull,
                    attempt: 1,
                    current_bytes: Some(128),
                    total_bytes: Some(1_024),
                    updated_at_unix_ms: unix_ms(),
                }],
                image_results: Vec::new(),
                image_progress: Vec::new(),
                gateway: GatewayReport::default(),
            },
        )
        .await
        .unwrap();
    let pulling = controller
        .wait_for_deployment("demo", Some(accepted.generation), None, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(pulling.task_phases.len(), 1);
    assert_eq!(
        pulling.task_phases[0].phase,
        crate::model::TaskReconcilePhase::Pull
    );
    assert_eq!(pulling.task_phases[0].tasks, 1);

    let service = test_service();
    let reported_ports = task
        .ports
        .iter()
        .cloned()
        .map(|mut port| {
            port.published = Some(49_152);
            port
        })
        .collect();
    controller
        .heartbeat(
            "node-a",
            NodeHeartbeat {
                node: test_node(),
                tasks: vec![TaskReport {
                    id: task.id.clone(),
                    observed: ObservedTaskState::Healthy,
                    container_id: Some("container-new".into()),
                    image_id: None,
                    cluster_id: Some("deployment-success-test".into()),
                    stack: Some("demo".into()),
                    service: Some("web".into()),
                    slot: Some(task.slot),
                    revision: Some(task.revision),
                    spec_hash: Some(service_spec_hash(&service.spec)),
                    ports: reported_ports,
                    config_digests: task.config_digests.clone(),
                }],
                task_inventory_error: None,
                task_results: vec![TaskReconcileReport {
                    task_id: task.id,
                    desired_generation: accepted.generation,
                    applied_generation: Some(accepted.generation),
                    phase: crate::model::TaskReconcilePhase::Create,
                    error: None,
                }],
                task_progress: Vec::new(),
                image_results: Vec::new(),
                image_progress: Vec::new(),
                gateway: GatewayReport::default(),
            },
        )
        .await
        .unwrap();

    let completed = controller
        .wait_for_deployment("demo", Some(accepted.generation), None, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(completed.status, StackDeploymentStatus::Healthy);
    assert_eq!(completed.services[0].applied, 1);
    assert_eq!(completed.services[0].healthy, 1);
    assert_eq!(
        controller
            .inner
            .lock()
            .await
            .state
            .tasks
            .values()
            .next()
            .unwrap()
            .ports[0]
            .published,
        Some(49_152)
    );
}

#[tokio::test]
async fn routed_deployment_waits_for_gateway_application() {
    let cluster = test_cluster("deployment-gateway-wait-test");
    let directory = tempfile::tempdir().unwrap();
    let repository = StateRepository::open(directory.path(), cluster.clone()).unwrap();
    let mut config = test_controller_config(&cluster, directory.path());
    config.gateway_enabled = false;
    let controller = Controller::new(config, "test-token".into(), repository)
        .await
        .unwrap();
    let mut request = test_join_request("node-a", "127.0.0.1");
    request.gateway_enabled = true;
    controller.join_node("node-a", request).await.unwrap();
    controller
        .heartbeat(
            "node-a",
            NodeHeartbeat {
                node: test_node(),
                tasks: Vec::new(),
                task_inventory_error: None,
                task_results: Vec::new(),
                task_progress: Vec::new(),
                image_results: Vec::new(),
                image_progress: Vec::new(),
                gateway: GatewayReport::default(),
            },
        )
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
    let accepted = controller.apply("demo", parsed).await.unwrap();
    let (task, spec_hash) = {
        let inner = controller.inner.lock().await;
        (
            inner.state.tasks.values().next().unwrap().clone(),
            service_spec_hash(&inner.state.services["demo.web"].spec),
        )
    };
    let reported_ports = task
        .ports
        .iter()
        .cloned()
        .map(|mut port| {
            port.published = Some(49_153);
            port
        })
        .collect::<Vec<_>>();
    let task_report = TaskReport {
        id: task.id.clone(),
        observed: ObservedTaskState::Healthy,
        container_id: Some("container-routed".into()),
        image_id: None,
        cluster_id: Some("deployment-gateway-wait-test".into()),
        stack: Some("demo".into()),
        service: Some("web".into()),
        slot: Some(task.slot),
        revision: Some(task.revision),
        spec_hash: Some(spec_hash),
        ports: reported_ports,
        config_digests: task.config_digests.clone(),
    };
    let task_result = TaskReconcileReport {
        task_id: task.id,
        desired_generation: accepted.generation,
        applied_generation: Some(accepted.generation),
        phase: crate::model::TaskReconcilePhase::Create,
        error: None,
    };

    let desired = controller
        .heartbeat(
            "node-a",
            NodeHeartbeat {
                node: test_node(),
                tasks: vec![task_report.clone()],
                task_inventory_error: None,
                task_results: vec![task_result.clone()],
                task_progress: Vec::new(),
                image_results: Vec::new(),
                image_progress: Vec::new(),
                gateway: GatewayReport::default(),
            },
        )
        .await
        .unwrap()
        .gateway_config
        .unwrap();
    let recovered_route = &desired.recovery_snapshot.stacks["demo"];
    assert_eq!(desired.recovery_snapshot.generation, desired.generation);
    assert_eq!(
        recovered_route.upstreams[&ServicePortKey::new("web", 80, HttpBackendProtocol::Http)],
        ["127.0.0.1:49153"]
    );
    assert_eq!(
        desired.config["apps"]["http"]["servers"]["swarmlite"]["routes"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|route| route["handle"].as_array().into_iter().flatten())
            .find(|handler| handler["handler"] == "reverse_proxy")
            .unwrap()["upstreams"],
        serde_json::json!([{"dial": "127.0.0.1:49153"}])
    );
    let waiting = controller
        .wait_for_deployment("demo", Some(accepted.generation), None, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(waiting.status, StackDeploymentStatus::Reconciling);
    assert_eq!(waiting.gateway.as_ref().unwrap().applied_nodes, 0);
    assert_eq!(waiting.gateway.as_ref().unwrap().total_nodes, 1);

    controller
        .heartbeat(
            "node-a",
            NodeHeartbeat {
                node: test_node(),
                tasks: vec![task_report],
                task_inventory_error: None,
                task_results: vec![task_result],
                task_progress: Vec::new(),
                image_results: Vec::new(),
                image_progress: Vec::new(),
                gateway: GatewayReport {
                    applied_generation: Some(desired.generation),
                    image: None,
                    error: None,
                    retryable: true,
                },
            },
        )
        .await
        .unwrap();
    let completed = controller
        .wait_for_deployment("demo", Some(accepted.generation), None, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(completed.status, StackDeploymentStatus::Healthy);
    assert_eq!(completed.gateway.as_ref().unwrap().applied_nodes, 1);
}

#[tokio::test]
async fn ordinary_stack_rm_removes_a_route_only_recovered_stack() {
    let cluster = test_cluster("route-only-rm-test");
    let directory = tempfile::tempdir().unwrap();
    let repository = StateRepository::open(directory.path(), cluster.clone()).unwrap();
    let parsed = parse_stack(
        r#"
services:
  web:
    image: nginx
    expose: [80]
x-swarmlite:
  http_routes:
    - hostnames: [recovered.example.com]
      rules:
        - backend: { service: web, port: 80 }
"#,
    )
    .unwrap();
    let recovered_spec = parsed.services["web"].clone();
    let snapshot = GatewayRecoverySnapshot::new(
        cluster.cluster_id.clone(),
        41,
        BTreeMap::from([(
            "demo".into(),
            crate::model::RecoveredStackGateway {
                gateway: parsed.gateway,
                upstreams: BTreeMap::from([(
                    ServicePortKey::new("web", 80, HttpBackendProtocol::Http),
                    vec!["10.0.0.8:32080".into()],
                )]),
            },
        )]),
    );
    repository
        .initialize_from_gateway_recovery(&snapshot)
        .unwrap();
    let mut config = test_controller_config(&cluster, directory.path());
    config.gateway_enabled = false;
    let controller = Controller::new(config, "0123456789abcdef".into(), repository.clone())
        .await
        .unwrap();
    let mut request = test_join_request("node-a", "127.0.0.1");
    request.gateway_enabled = true;
    controller.join_node("node-a", request).await.unwrap();
    let recovered_task = recovered_task_report(
        &cluster.cluster_id,
        "demo",
        "recovered-task",
        4,
        &recovered_spec,
        32_080,
    );
    controller
        .heartbeat(
            "node-a",
            NodeHeartbeat {
                node: test_node(),
                tasks: vec![recovered_task.clone()],
                task_inventory_error: None,
                task_results: Vec::new(),
                task_progress: Vec::new(),
                image_results: Vec::new(),
                image_progress: Vec::new(),
                gateway: GatewayReport {
                    applied_generation: Some(snapshot.generation),
                    image: None,
                    error: None,
                    retryable: true,
                },
            },
        )
        .await
        .unwrap();

    assert_eq!(controller.list_stacks().await.stacks[0].name, "demo");
    let removal = controller.remove_stack("demo").await.unwrap();
    assert_eq!(removal.pending_removals, 1);

    let waiting = controller
        .heartbeat(
            "node-a",
            NodeHeartbeat {
                node: test_node(),
                tasks: vec![recovered_task.clone()],
                task_inventory_error: None,
                task_results: Vec::new(),
                task_progress: Vec::new(),
                image_results: Vec::new(),
                image_progress: Vec::new(),
                gateway: GatewayReport {
                    applied_generation: Some(snapshot.generation),
                    image: None,
                    error: None,
                    retryable: true,
                },
            },
        )
        .await
        .unwrap();
    assert!(waiting.remove_tasks.is_empty());
    let gateway_generation = waiting.gateway_config.unwrap().generation;

    let cleanup = controller
        .heartbeat(
            "node-a",
            NodeHeartbeat {
                node: test_node(),
                tasks: vec![recovered_task],
                task_inventory_error: None,
                task_results: Vec::new(),
                task_progress: Vec::new(),
                image_results: Vec::new(),
                image_progress: Vec::new(),
                gateway: GatewayReport {
                    applied_generation: Some(gateway_generation),
                    image: None,
                    error: None,
                    retryable: true,
                },
            },
        )
        .await
        .unwrap();
    assert_eq!(cleanup.remove_tasks.len(), 1);
    assert_eq!(cleanup.remove_tasks[0].id, "recovered-task");

    controller
        .heartbeat(
            "node-a",
            NodeHeartbeat {
                node: test_node(),
                tasks: Vec::new(),
                task_inventory_error: None,
                task_results: vec![TaskReconcileReport {
                    task_id: "recovered-task".into(),
                    desired_generation: removal.generation,
                    applied_generation: Some(removal.generation),
                    phase: crate::model::TaskReconcilePhase::Remove,
                    error: None,
                }],
                task_progress: Vec::new(),
                image_results: Vec::new(),
                image_progress: Vec::new(),
                gateway: GatewayReport {
                    applied_generation: Some(gateway_generation),
                    image: None,
                    error: None,
                    retryable: true,
                },
            },
        )
        .await
        .unwrap();

    let completed = controller
        .wait_for_deployment("demo", Some(removal.generation), None, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(completed.status, StackDeploymentStatus::Healthy);
    assert_eq!(completed.pending_removals, 0);

    let inner = controller.inner.lock().await;
    assert!(!inner.state.gateway_routes.contains_key("demo"));
    assert!(!inner.gateway_snapshot.stacks.contains_key("demo"));
    assert!(inner.gateway_generation > snapshot.generation);
    drop(inner);
    assert!(
        !repository
            .load()
            .await
            .unwrap()
            .state
            .gateway_routes
            .contains_key("demo")
    );
}

#[tokio::test]
async fn ordinary_deploy_replaces_only_its_recovered_stack_route_fragment() {
    let cluster = test_cluster("recovered-fragment-replace-test");
    let directory = tempfile::tempdir().unwrap();
    let repository = StateRepository::open(directory.path(), cluster.clone()).unwrap();
    let old_demo = parse_stack(
        r#"
services:
  web: { image: nginx, expose: [80] }
x-swarmlite:
  http_routes:
    - hostnames: [old.example.com]
      rules: [{ backend: { service: web, port: 80 } }]
"#,
    )
    .unwrap();
    let stable = parse_stack(
        r#"
services:
  api: { image: nginx, expose: [8080] }
x-swarmlite:
  http_routes:
    - hostnames: [stable.example.com]
      rules: [{ backend: { service: api, port: 8080 } }]
"#,
    )
    .unwrap();
    let stable_route = crate::model::RecoveredStackGateway {
        gateway: stable.gateway,
        upstreams: BTreeMap::from([(
            ServicePortKey::new("api", 8080, HttpBackendProtocol::Http),
            vec!["10.0.0.9:32100".into()],
        )]),
    };
    let snapshot = GatewayRecoverySnapshot::new(
        cluster.cluster_id.clone(),
        52,
        BTreeMap::from([
            (
                "demo".into(),
                crate::model::RecoveredStackGateway {
                    gateway: old_demo.gateway,
                    upstreams: BTreeMap::from([(
                        ServicePortKey::new("web", 80, HttpBackendProtocol::Http),
                        vec!["10.0.0.8:32080".into()],
                    )]),
                },
            ),
            ("stable".into(), stable_route.clone()),
        ]),
    );
    repository
        .initialize_from_gateway_recovery(&snapshot)
        .unwrap();
    let controller = Controller::new(
        test_controller_config(&cluster, directory.path()),
        "0123456789abcdef".into(),
        repository,
    )
    .await
    .unwrap();
    let replacement = parse_stack(
        r#"
services:
  web: { image: nginx, expose: [80] }
x-swarmlite:
  http_routes:
    - hostnames: [new.example.com]
      rules: [{ backend: { service: web, port: 80 } }]
"#,
    )
    .unwrap();

    controller.apply("demo", replacement).await.unwrap();

    let inner = controller.inner.lock().await;
    assert_eq!(inner.state.gateway_routes["stable"], stable_route);
    let demo = &inner.state.gateway_routes["demo"];
    assert_eq!(demo.gateway.http_routes[0].hostnames, ["new.example.com"]);
    assert!(demo.upstreams[&ServicePortKey::new("web", 80, HttpBackendProtocol::Http)].is_empty());
}

#[tokio::test]
async fn deployment_block_is_persisted_and_can_be_retried() {
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
                task_progress: Vec::new(),
                image_results: Vec::new(),
                image_progress: Vec::new(),
                gateway: GatewayReport::default(),
            },
        )
        .await
        .unwrap();

    let failed = controller
        .wait_for_deployment(
            "demo",
            Some(accepted.generation),
            Some(accepted.revision),
            Duration::ZERO,
        )
        .await
        .unwrap();
    assert_eq!(failed.status, StackDeploymentStatus::Blocked);
    assert_eq!(failed.errors[0].message, "image pull denied");
    let persisted = repository.load().await.unwrap();
    assert_eq!(
        persisted.state.stacks["demo"]
            .deployment
            .as_ref()
            .unwrap()
            .status,
        StackDeploymentStatus::Blocked
    );
    let retried = controller.retry_stack_deployment("demo").await.unwrap();
    assert_eq!(retried.generation, accepted.generation);
    assert_eq!(retried.retry_revision, 1);
    assert_eq!(retried.status, StackDeploymentStatus::Reconciling);
    let assignments = controller
        .heartbeat(
            "node-a",
            NodeHeartbeat {
                node: test_node(),
                tasks: Vec::new(),
                task_inventory_error: None,
                task_results: Vec::new(),
                task_progress: Vec::new(),
                image_results: Vec::new(),
                image_progress: Vec::new(),
                gateway: GatewayReport::default(),
            },
        )
        .await
        .unwrap();
    assert_eq!(assignments.assignments[0].deployment_retry_revision, 1);
}

#[tokio::test]
async fn replace_supersedes_and_archives_the_active_generation() {
    let (controller, _, _directory) = test_controller("deployment-replace-test").await;
    register_live_node(&controller).await;
    let first = controller.apply("demo", parsed_test_stack()).await.unwrap();
    let mut replacement = parsed_test_stack();
    replacement.services.get_mut("web").unwrap().image = "nginx:replacement".into();
    let second = controller
        .apply_with_registry_credentials_mode("demo", replacement, BTreeMap::new(), true)
        .await
        .unwrap();

    assert!(second.generation > first.generation);
    let archived = controller
        .wait_for_deployment("demo", Some(first.generation), None, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(archived.status, StackDeploymentStatus::Superseded);
    let inner = controller.inner.lock().await;
    let archived = &inner.state.stacks["demo"].deployment_history[&first.generation];
    assert_eq!(archived.superseded_by, Some(second.generation));
}

#[tokio::test]
async fn lists_deployments_for_all_stacks_in_name_order() {
    let (controller, _, _directory) = test_controller("deployment-list-test").await;
    register_live_node(&controller).await;
    controller.apply("zeta", parsed_test_stack()).await.unwrap();
    controller
        .apply("alpha", parsed_test_stack())
        .await
        .unwrap();

    let deployments = controller.list_deployments().await.unwrap();

    assert_eq!(
        deployments
            .stacks
            .iter()
            .map(|stack| stack.stack.as_str())
            .collect::<Vec<_>>(),
        ["alpha", "zeta"]
    );
    assert!(
        deployments
            .stacks
            .iter()
            .all(|stack| stack.current.is_some())
    );
}

#[tokio::test]
async fn rollback_restores_the_latest_healthy_snapshot_as_a_new_generation() {
    let (controller, _, _directory) = test_controller("deployment-rollback-test").await;
    register_live_node(&controller).await;
    let first = controller.apply("demo", parsed_test_stack()).await.unwrap();
    {
        let mut inner = controller.inner.lock().await;
        let deployment = inner
            .state
            .stacks
            .get_mut("demo")
            .unwrap()
            .deployment
            .as_mut()
            .unwrap();
        deployment.status = StackDeploymentStatus::Healthy;
        deployment.finished_at_unix_ms = Some(unix_ms());
    }
    let mut second_stack = parsed_test_stack();
    second_stack.services.get_mut("web").unwrap().image = "nginx:broken".into();
    let second = controller.apply("demo", second_stack).await.unwrap();

    let rollback = controller.rollback_stack("demo", None).await.unwrap();
    assert!(rollback.generation > second.generation);
    let inner = controller.inner.lock().await;
    let stack = &inner.state.stacks["demo"];
    assert_eq!(
        stack.deployment.as_ref().unwrap().snapshot.services["web"].image,
        "nginx:1.29-alpine"
    );
    assert_eq!(
        stack.deployment_history[&second.generation].status,
        StackDeploymentStatus::Superseded
    );
    assert_eq!(
        stack.deployment_history[&first.generation].status,
        StackDeploymentStatus::Healthy
    );
}

#[tokio::test]
async fn failed_container_inventory_does_not_fail_stack_removal() {
    let (controller, _, _directory) = test_controller("removal-inventory-race-test").await;
    register_live_node(&controller).await;
    controller.apply("demo", parsed_test_stack()).await.unwrap();
    let task = {
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
        inner.state.tasks.values().next().unwrap().clone()
    };

    let removal = controller.remove_stack("demo").await.unwrap();
    assert_eq!(removal.pending_removals, 1);
    let response = controller
        .heartbeat(
            "node-a",
            NodeHeartbeat {
                node: test_node(),
                tasks: vec![TaskReport {
                    id: task.id.clone(),
                    observed: ObservedTaskState::Failed,
                    container_id: Some("container-failed".into()),
                    image_id: None,
                    cluster_id: Some("removal-inventory-race-test".into()),
                    stack: Some("demo".into()),
                    service: Some("web".into()),
                    slot: Some(task.slot),
                    revision: Some(task.revision),
                    spec_hash: Some(service_spec_hash(&test_service().spec)),
                    ports: task.ports,
                    config_digests: task.config_digests,
                }],
                task_inventory_error: None,
                task_results: Vec::new(),
                task_progress: Vec::new(),
                image_results: Vec::new(),
                image_progress: Vec::new(),
                gateway: GatewayReport::default(),
            },
        )
        .await
        .unwrap();

    assert_eq!(response.remove_tasks.len(), 1);
    assert_eq!(response.remove_tasks[0].id, task.id);
    let removing = controller
        .wait_for_deployment("demo", Some(removal.generation), None, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(removing.status, StackDeploymentStatus::Reconciling);
    assert_eq!(removing.pending_removals, 1);

    controller
        .heartbeat(
            "node-a",
            NodeHeartbeat {
                node: test_node(),
                tasks: Vec::new(),
                task_inventory_error: None,
                task_results: vec![TaskReconcileReport {
                    task_id: task.id,
                    desired_generation: removal.generation,
                    applied_generation: Some(removal.generation),
                    phase: crate::model::TaskReconcilePhase::Remove,
                    error: None,
                }],
                task_progress: Vec::new(),
                image_results: Vec::new(),
                image_progress: Vec::new(),
                gateway: GatewayReport::default(),
            },
        )
        .await
        .unwrap();

    let completed = controller
        .wait_for_deployment("demo", Some(removal.generation), None, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(completed.status, StackDeploymentStatus::Healthy);
    assert_eq!(completed.pending_removals, 0);
    assert!(completed.errors.is_empty());
    assert!(controller.inner.lock().await.state.tasks.is_empty());
}

#[tokio::test]
async fn failed_container_inventory_keeps_running_tasks_and_node_lease_alive() {
    let (controller, _, _directory) = test_controller("inventory-timeout-test").await;
    register_live_node(&controller).await;
    controller.apply("demo", parsed_test_stack()).await.unwrap();
    let original = {
        let mut inner = controller.inner.lock().await;
        let task = inner.state.tasks.values_mut().next().unwrap();
        task.observed = ObservedTaskState::Healthy;
        task.container_id = Some("container-web".into());
        task.clone()
    };

    controller
        .heartbeat(
            "node-a",
            NodeHeartbeat {
                node: test_node(),
                tasks: Vec::new(),
                task_inventory_error: Some("Docker container inventory timed out".into()),
                task_results: Vec::new(),
                task_progress: Vec::new(),
                image_results: Vec::new(),
                image_progress: Vec::new(),
                gateway: GatewayReport::default(),
            },
        )
        .await
        .unwrap();
    controller.tick().await.unwrap();

    let inner = controller.inner.lock().await;
    assert!(inner.live_nodes.contains_key("node-a"));
    assert_eq!(inner.state.tasks.len(), 1);
    let preserved = &inner.state.tasks[&original.id];
    assert_eq!(preserved.node_id, original.node_id);
    assert_eq!(preserved.observed, ObservedTaskState::Healthy);
    assert_eq!(preserved.container_id.as_deref(), Some("container-web"));
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
        swarmlite_version: Some("0.1.25".into()),
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
            deployment_history: BTreeMap::new(),
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
async fn migrates_the_legacy_gateway_image_and_preserves_explicit_pins() {
    let mut cluster = test_cluster("gateway-image-migration-test");
    cluster.gateway.image = crate::model::LEGACY_DEFAULT_GATEWAY_IMAGE.into();
    cluster.gateway.managed_image = false;
    let directory = tempfile::tempdir().unwrap();
    let repository = StateRepository::open(directory.path(), cluster.clone()).unwrap();
    let mut before = repository.initialize_with_cluster(&cluster).await.unwrap();
    before.state.gateway_generation = 7;
    repository
        .replace(before.generation, &before.cluster, &before.state)
        .await
        .unwrap();
    let controller = Controller::new(
        test_controller_config(&cluster, directory.path()),
        "0123456789abcdef".into(),
        repository.clone(),
    )
    .await
    .unwrap();

    let migrated = repository.load().await.unwrap();
    assert_eq!(
        migrated.cluster.gateway.image,
        crate::model::DEFAULT_GATEWAY_IMAGE
    );
    assert!(migrated.cluster.gateway.managed_image);
    assert_eq!(migrated.state.gateway_generation, 8);
    assert_eq!(controller.inner.lock().await.gateway_generation, 8);

    let pinned = controller
        .update_cluster_config(ClusterConfigUpdate {
            gateway_image: Some(crate::model::DEFAULT_GATEWAY_IMAGE.into()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(
        pinned.config.gateway.image,
        crate::model::DEFAULT_GATEWAY_IMAGE
    );
    assert!(!pinned.config.gateway.managed_image);
    assert!(
        !repository
            .load()
            .await
            .unwrap()
            .cluster
            .gateway
            .managed_image
    );
}

#[tokio::test]
async fn deployment_policy_updates_are_validated_and_persisted() {
    let (controller, repository, _directory) = test_controller("deployment-policy-test").await;
    let updated = controller
        .update_cluster_config(ClusterConfigUpdate {
            deployment_progress_deadline_seconds: Some(600),
            image_pull_idle_timeout_seconds: Some(90),
            image_pull_max_attempts: Some(7),
            image_pull_initial_backoff_seconds: Some(3),
            image_pull_max_backoff_seconds: Some(30),
            ..ClusterConfigUpdate::default()
        })
        .await
        .unwrap();
    assert_eq!(updated.config.deployment.progress_deadline_seconds, 600);
    assert_eq!(
        updated.config.deployment.image_pull_idle_timeout_seconds,
        90
    );
    assert_eq!(updated.config.deployment.image_pull_max_attempts, 7);
    assert_eq!(
        repository.load().await.unwrap().cluster.deployment,
        updated.config.deployment
    );
    assert!(matches!(
        controller
            .update_cluster_config(ClusterConfigUpdate {
                image_pull_max_attempts: Some(0),
                ..ClusterConfigUpdate::default()
            })
            .await,
        Err(ControllerError::Invalid(message)) if message.contains("greater than zero")
    ));
}

#[tokio::test]
async fn proxy_configuration_is_validated_persisted_and_hot_reloaded() {
    let (controller, repository, _directory) = test_controller("proxy-config-test").await;
    assert_eq!(
        controller.image_registry.ping().headers()["x-swarmlite-image-proxy"],
        "disabled"
    );

    let updated = controller
        .update_cluster_config(ClusterConfigUpdate {
            proxy_all: Some("socks5h://127.0.0.1:1080".into()),
            proxy_no_proxy: Some("localhost,.internal.example.com".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(
        updated.config.proxy.all.as_deref(),
        Some("socks5h://127.0.0.1:1080")
    );
    assert_eq!(
        repository.load().await.unwrap().cluster.proxy,
        updated.config.proxy
    );
    assert_eq!(
        controller.image_registry.ping().headers()["x-swarmlite-image-proxy"],
        "enabled"
    );

    assert!(matches!(
        controller
            .update_cluster_config(ClusterConfigUpdate {
                proxy_https: Some("ftp://proxy.example.com:21".into()),
                ..Default::default()
            })
            .await,
        Err(ControllerError::Invalid(message)) if message.contains("invalid proxy configuration")
    ));

    let cleared = controller
        .update_cluster_config(ClusterConfigUpdate {
            unset: BTreeSet::from([
                ClusterConfigField::ProxyAll,
                ClusterConfigField::ProxyNoProxy,
            ]),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(cleared.config.proxy.is_empty());
    assert_eq!(
        controller.image_registry.ping().headers()["x-swarmlite-image-proxy"],
        "disabled"
    );
}

#[tokio::test]
async fn gateway_cache_config_rejects_unsafe_values() {
    let (controller, _repository, _directory) = test_controller("gateway-cache-validation").await;
    for update in [
        ClusterConfigUpdate {
            gateway_cache_max_size_bytes: Some(0),
            ..Default::default()
        },
        ClusterConfigUpdate {
            gateway_cache_low_water_percent: Some(100),
            ..Default::default()
        },
        ClusterConfigUpdate {
            gateway_cache_hit_sample_ratio: Some(0),
            ..Default::default()
        },
        ClusterConfigUpdate {
            gateway_cache_sqlite_mmap_size_bytes: Some(i64::MAX as u64 + 1),
            ..Default::default()
        },
        ClusterConfigUpdate {
            gateway_cache_sqlite_read_connections: Some(17),
            ..Default::default()
        },
        ClusterConfigUpdate {
            gateway_cache_sqlite_cleanup_interval_seconds: Some(0),
            ..Default::default()
        },
    ] {
        assert!(matches!(
            controller.update_cluster_config(update).await,
            Err(ControllerError::Invalid(_))
        ));
    }
}

#[tokio::test]
async fn gateway_config_preserves_explicit_values_and_unsets_them() {
    let (controller, repository, _directory) = test_controller("gateway-config-test").await;
    let updated = controller
        .update_cluster_config(ClusterConfigUpdate {
            gateway_listen: Some(vec![":8080".into()]),
            gateway_metrics_enabled: Some(false),
            gateway_metrics_per_host: Some(true),
            gateway_cache_max_size_bytes: Some(2_147_483_648),
            gateway_cache_low_water_percent: Some(80),
            gateway_cache_hit_sample_ratio: Some(16),
            gateway_cache_access_update_interval_seconds: Some(120),
            gateway_cache_sqlite_cache_size_kib: Some(8_192),
            gateway_cache_sqlite_mmap_size_bytes: Some(134_217_728),
            gateway_cache_sqlite_read_connections: Some(6),
            gateway_cache_sqlite_busy_timeout_seconds: Some(3),
            gateway_cache_sqlite_cleanup_interval_seconds: Some(60),
            gateway_cache_sqlite_journal_size_limit_bytes: Some(33_554_432),
            gateway_logging_runtime_level: Some(crate::model::GatewayLogLevel::Debug),
            gateway_logging_access_enabled: Some(true),
            gateway_logging_access_format: Some(crate::model::GatewayAccessLogFormat::Console),
            gateway_logging_access_sampling_enabled: Some(true),
            gateway_logging_access_sampling_first: Some(0),
            gateway_logging_access_sampling_thereafter: Some(10),
            gateway_shutdown_grace_period_seconds: Some(0),
            gateway_http_read_header_timeout_seconds: Some(0),
            gateway_http_read_body_timeout_seconds: Some(30),
            gateway_http_write_timeout_seconds: Some(45),
            gateway_http_idle_timeout_seconds: Some(300),
            gateway_http_max_header_bytes: Some(0),
            gateway_http_http3_enabled: Some(false),
            ..Default::default()
        })
        .await
        .unwrap();
    let gateway = &updated.config.gateway;
    assert_eq!(gateway.listen, vec![":8080"]);
    assert_eq!(gateway.metrics.enabled, Some(false));
    assert_eq!(gateway.metrics.per_host, Some(true));
    assert_eq!(gateway.cache.max_size_bytes, Some(2_147_483_648));
    assert_eq!(gateway.cache.low_water_percent, Some(80));
    assert_eq!(gateway.cache.hit_sample_ratio, Some(16));
    assert_eq!(gateway.cache.access_update_interval_seconds, Some(120));
    assert_eq!(gateway.cache.sqlite.cache_size_kib, Some(8_192));
    assert_eq!(gateway.cache.sqlite.mmap_size_bytes, Some(134_217_728));
    assert_eq!(gateway.cache.sqlite.read_connections, Some(6));
    assert_eq!(gateway.logging.access.sampling.first, Some(0));
    assert_eq!(gateway.shutdown.grace_period_seconds, Some(0));
    assert_eq!(gateway.http.timeouts.read_header_seconds, Some(0));
    assert_eq!(gateway.http.max_header_bytes, Some(0));
    assert_eq!(gateway.http.http3_enabled, Some(false));
    assert_eq!(repository.load().await.unwrap().cluster.gateway, *gateway);

    let cleared = controller
        .update_cluster_config(ClusterConfigUpdate {
            unset: BTreeSet::from([
                ClusterConfigField::GatewayListen,
                ClusterConfigField::GatewayMetricsEnabled,
                ClusterConfigField::GatewayMetricsPerHost,
                ClusterConfigField::GatewayCacheMaxSizeBytes,
                ClusterConfigField::GatewayCacheLowWaterPercent,
                ClusterConfigField::GatewayCacheHitSampleRatio,
                ClusterConfigField::GatewayCacheAccessUpdateIntervalSeconds,
                ClusterConfigField::GatewayCacheSqliteCacheSizeKib,
                ClusterConfigField::GatewayCacheSqliteMmapSizeBytes,
                ClusterConfigField::GatewayCacheSqliteReadConnections,
                ClusterConfigField::GatewayCacheSqliteBusyTimeoutSeconds,
                ClusterConfigField::GatewayCacheSqliteCleanupIntervalSeconds,
                ClusterConfigField::GatewayCacheSqliteJournalSizeLimitBytes,
                ClusterConfigField::GatewayLoggingRuntimeLevel,
                ClusterConfigField::GatewayLoggingAccessEnabled,
                ClusterConfigField::GatewayLoggingAccessFormat,
                ClusterConfigField::GatewayLoggingAccessSamplingEnabled,
                ClusterConfigField::GatewayLoggingAccessSamplingFirst,
                ClusterConfigField::GatewayLoggingAccessSamplingThereafter,
                ClusterConfigField::GatewayShutdownGracePeriodSeconds,
                ClusterConfigField::GatewayHttpReadHeaderTimeoutSeconds,
                ClusterConfigField::GatewayHttpReadBodyTimeoutSeconds,
                ClusterConfigField::GatewayHttpWriteTimeoutSeconds,
                ClusterConfigField::GatewayHttpIdleTimeoutSeconds,
                ClusterConfigField::GatewayHttpMaxHeaderBytes,
                ClusterConfigField::GatewayHttpHttp3Enabled,
            ]),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(cleared.config.gateway, ClusterGatewayConfig::default());
}

#[tokio::test]
async fn node_labels_are_authoritative_and_persisted() {
    let cluster = test_cluster("node-label-test");
    let directory = tempfile::tempdir().unwrap();
    let repository = StateRepository::open(directory.path(), cluster.clone()).unwrap();
    let observer = repository.clone();
    let mut config = test_controller_config(&cluster, directory.path());
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
                task_progress: Vec::new(),
                image_results: Vec::new(),
                image_progress: Vec::new(),
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
    let mut config = test_controller_config(&cluster, directory.path());
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
        image_id: None,
        cluster_id: Some("caddy-publisher-test".into()),
        stack: Some("demo".into()),
        service: Some("web".into()),
        slot: Some(0),
        revision: Some(1),
        spec_hash: Some(service_spec_hash(&test_service().spec)),
        ports: vec![PortBinding {
            target: 80,
            published: Some(20_001),
            protocol: "tcp".into(),
        }],
        config_digests: Vec::new(),
    };
    let desired = controller
        .heartbeat(
            "node-a",
            NodeHeartbeat {
                node: test_node(),
                tasks: vec![report.clone()],
                task_inventory_error: None,
                task_results: Vec::new(),
                task_progress: Vec::new(),
                image_results: Vec::new(),
                image_progress: Vec::new(),
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
                task_progress: Vec::new(),
                image_results: Vec::new(),
                image_progress: Vec::new(),
                gateway: GatewayReport {
                    applied_generation: Some(desired.generation),
                    image: Some("ghcr.io/feichao/swarmlite-gateway:v0.1.25".into()),
                    error: None,
                    retryable: true,
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
async fn gateway_failures_are_exposed_in_status() {
    let (controller, _, _directory) = test_controller("gateway-error-status-test").await;
    let mut request = test_join_request("node-a", "127.0.0.1");
    request.gateway_enabled = true;
    controller.join_node("node-a", request).await.unwrap();

    controller
        .heartbeat(
            "node-a",
            NodeHeartbeat {
                node: test_node(),
                tasks: Vec::new(),
                task_inventory_error: None,
                task_results: Vec::new(),
                task_progress: Vec::new(),
                image_results: Vec::new(),
                image_progress: Vec::new(),
                gateway: GatewayReport {
                    applied_generation: None,
                    image: Some("ghcr.io/feichao/swarmlite-gateway:v0.1.22".into()),
                    error: Some("failed to bind gateway port 80".into()),
                    retryable: false,
                },
            },
        )
        .await
        .unwrap();

    let status = controller.status().await;
    assert_eq!(status.gateway.applied_generation, None);
    assert_eq!(
        status
            .gateway
            .endpoint_errors
            .get("node-a")
            .map(String::as_str),
        Some("failed to bind gateway port 80")
    );

    let gateway_status = controller.gateway_status().await;
    let node = gateway_status
        .nodes
        .iter()
        .find(|node| node.node_id == "node-a")
        .unwrap();
    assert!(node.enabled);
    assert_eq!(node.status, GatewayNodeStatusKind::Error);
    assert_eq!(
        node.desired_generation,
        Some(gateway_status.desired_generation)
    );
    assert_eq!(node.applied_generation, None);
    assert_eq!(node.swarmlite_version.as_deref(), Some("0.1.25"));
    assert_eq!(
        node.image.as_deref(),
        Some("ghcr.io/feichao/swarmlite-gateway:v0.1.22")
    );
    assert_eq!(node.retryable, Some(false));
    assert_eq!(
        node.error.as_deref(),
        Some("failed to bind gateway port 80")
    );
    let json = serde_json::to_value(gateway_status).unwrap();
    assert_eq!(
        json["config"]["metrics"]["enabled"],
        serde_json::Value::Null
    );
    assert_eq!(
        json["config"]["http"]["timeouts"]["read_header_seconds"],
        serde_json::Value::Null
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
                    image_id: None,
                    cluster_id: Some("recovery-test".into()),
                    stack: Some("demo".into()),
                    service: Some("web".into()),
                    slot: Some(0),
                    revision: Some(7),
                    spec_hash: Some(spec_hash),
                    ports: vec![PortBinding {
                        target: 80,
                        published: Some(20_001),
                        protocol: "tcp".into(),
                    }],
                    config_digests: Vec::new(),
                }],
                task_inventory_error: None,
                task_results: Vec::new(),
                task_progress: Vec::new(),
                image_results: Vec::new(),
                image_progress: Vec::new(),
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
async fn changed_deploy_removes_only_its_stale_recovered_tasks_after_replacement_is_ready() {
    let cluster_id = "stale-recovery-cleanup-test";
    let (controller, _, _directory) = test_controller(cluster_id).await;
    controller
        .join_node("node-a", test_join_request("node-a", "127.0.0.1"))
        .await
        .unwrap();
    let mut recovered_spec = test_service().spec;
    recovered_spec.service_labels.clear();
    let alpha_old =
        recovered_task_report(cluster_id, "alpha", "alpha-old", 7, &recovered_spec, 20_001);
    let beta_old =
        recovered_task_report(cluster_id, "beta", "beta-old", 7, &recovered_spec, 20_002);
    controller
        .heartbeat(
            "node-a",
            NodeHeartbeat {
                node: test_node(),
                tasks: vec![alpha_old.clone(), beta_old.clone()],
                task_inventory_error: None,
                task_results: Vec::new(),
                task_progress: Vec::new(),
                image_results: Vec::new(),
                image_progress: Vec::new(),
                gateway: GatewayReport::default(),
            },
        )
        .await
        .unwrap();

    let mut replacement_spec = recovered_spec.clone();
    replacement_spec.environment.push("RECOVERY_TEST=v2".into());
    let accepted = controller
        .apply(
            "alpha",
            ParsedStack {
                services: BTreeMap::from([("web".into(), replacement_spec.clone())]),
                gateway: StackGatewaySpec::default(),
            },
        )
        .await
        .unwrap();
    assert_eq!(accepted.pending_removals, 1);

    let waiting = controller
        .heartbeat(
            "node-a",
            NodeHeartbeat {
                node: test_node(),
                tasks: vec![alpha_old.clone(), beta_old.clone()],
                task_inventory_error: None,
                task_results: Vec::new(),
                task_progress: Vec::new(),
                image_results: Vec::new(),
                image_progress: Vec::new(),
                gateway: GatewayReport::default(),
            },
        )
        .await
        .unwrap();
    assert!(waiting.remove_tasks.is_empty());

    let replacement = {
        let inner = controller.inner.lock().await;
        inner
            .state
            .tasks
            .values()
            .find(|task| task.service_id == "alpha.web")
            .unwrap()
            .clone()
    };
    let replacement_report = recovered_task_report(
        cluster_id,
        "alpha",
        &replacement.id,
        replacement.revision,
        &replacement_spec,
        20_003,
    );
    let cleanup = controller
        .heartbeat(
            "node-a",
            NodeHeartbeat {
                node: test_node(),
                tasks: vec![
                    alpha_old.clone(),
                    beta_old.clone(),
                    replacement_report.clone(),
                ],
                task_inventory_error: None,
                task_results: vec![TaskReconcileReport {
                    task_id: replacement.id.clone(),
                    desired_generation: accepted.generation,
                    applied_generation: Some(accepted.generation),
                    phase: crate::model::TaskReconcilePhase::Verify,
                    error: None,
                }],
                task_progress: Vec::new(),
                image_results: Vec::new(),
                image_progress: Vec::new(),
                gateway: GatewayReport::default(),
            },
        )
        .await
        .unwrap();
    assert_eq!(cleanup.remove_tasks.len(), 1);
    assert_eq!(cleanup.remove_tasks[0].id, "alpha-old");

    controller
        .heartbeat(
            "node-a",
            NodeHeartbeat {
                node: test_node(),
                tasks: vec![beta_old, replacement_report],
                task_inventory_error: None,
                task_results: vec![TaskReconcileReport {
                    task_id: "alpha-old".into(),
                    desired_generation: accepted.generation,
                    applied_generation: Some(accepted.generation),
                    phase: crate::model::TaskReconcilePhase::Remove,
                    error: None,
                }],
                task_progress: Vec::new(),
                image_results: Vec::new(),
                image_progress: Vec::new(),
                gateway: GatewayReport::default(),
            },
        )
        .await
        .unwrap();

    let completed = controller
        .wait_for_deployment("alpha", Some(accepted.generation), None, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(completed.status, StackDeploymentStatus::Healthy);
    assert_eq!(completed.pending_removals, 0);
    let inner = controller.inner.lock().await;
    assert!(!inner.state.unclaimed_tasks.contains_key("alpha-old"));
    assert!(inner.state.unclaimed_tasks.contains_key("beta-old"));
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
        image_id: None,
        cluster_id: Some("revision-recovery-test".into()),
        stack: Some("demo".into()),
        service: Some("web".into()),
        slot: Some(slot),
        revision: Some(revision),
        spec_hash: Some(spec_hash.clone()),
        ports: vec![PortBinding {
            target: 80,
            published: Some(20_001 + u16::try_from(slot).unwrap()),
            protocol: "tcp".into(),
        }],
        config_digests: Vec::new(),
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
                task_progress: Vec::new(),
                image_results: Vec::new(),
                image_progress: Vec::new(),
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
                        image_id: None,
                        cluster_id: Some("invalid-revision-recovery-test".into()),
                        stack: Some("demo".into()),
                        service: Some("web".into()),
                        slot: Some(0),
                        revision: Some(revision),
                        spec_hash: Some(spec_hash.clone()),
                        ports: vec![PortBinding {
                            target: 80,
                            published: Some(20_001),
                            protocol: "tcp".into(),
                        }],
                        config_digests: Vec::new(),
                    })
                    .collect(),
                task_inventory_error: None,
                task_results: Vec::new(),
                task_progress: Vec::new(),
                image_results: Vec::new(),
                image_progress: Vec::new(),
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
            published: Some(20_001),
            protocol: "tcp".into(),
        }],
        config_digests: Vec::new(),
        container_id: Some("container-old".into()),
        drain_until_unix_ms: None,
        applied_generation: None,
        reconcile_error: None,
    }
}
