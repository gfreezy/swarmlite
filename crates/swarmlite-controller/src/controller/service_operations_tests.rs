use super::*;
use swarmlite_core::service_operations::*;

const ADMIN: &str = "0123456789abcdef";

async fn ready_service(
    id: &str,
    pull: PullPolicy,
) -> (Controller, StateRepository, tempfile::TempDir, TaskRecord) {
    let (controller, repository, directory) = test_controller(id).await;
    register_live_node(&controller).await;
    let (_, task, _) = prepare_image_redeploy(&controller, pull, "nginx:latest").await;
    {
        let mut inner = controller.inner.lock().await;
        inner
            .state
            .nodes
            .get_mut("node-a")
            .unwrap()
            .supports_service_operations = true;
        inner.state.tasks.get_mut(&task.id).unwrap().image_id = Some("sha256:current".into());
        controller.commit_locked(&mut inner).await.unwrap();
    }
    (controller, repository, directory, task)
}

fn operation_heartbeat(
    task: &TaskRecord,
    generation: Option<u64>,
    image: Option<ImageResolutionReport>,
) -> NodeHeartbeat {
    let mut node = test_node();
    node.supports_service_operations = true;
    image_test_heartbeat_on(node, task, "sha256:current", generation, image)
}

#[tokio::test]
async fn permanent_token_can_be_viewed_revoked_and_used_after_image_change() {
    let (controller, repository, _directory, _) =
        ready_service("permanent-token", PullPolicy::Never).await;
    let token = controller
        .create_deploy_token(DeployTokenCreate {
            name: "ci".into(),
            service: "demo.web".into(),
            expires_in: "never".into(),
        })
        .await
        .unwrap();
    assert_eq!(token.expires_at_unix_ms, None);
    let secret = token.token.as_deref().unwrap();
    assert!(controller.deploy_principal(secret, false).is_ok());
    assert!(
        controller.list_deploy_tokens(false).unwrap()[0]
            .token
            .is_none()
    );
    assert_eq!(
        controller.list_deploy_tokens(true).unwrap()[0]
            .token
            .as_deref(),
        Some(secret)
    );
    // Entry configuration is not part of credential authorization.
    for domain in ["deploy.example.com", "new.example.com"] {
        controller
            .update_cluster_config(ClusterConfigUpdate {
                gateway_deploy_domain: Some(domain.into()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(controller.deploy_principal(secret, false).is_ok());
    }
    controller
        .update_cluster_config(ClusterConfigUpdate {
            unset: BTreeSet::from([ClusterConfigField::GatewayDeployDomain]),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(controller.deploy_principal(secret, false).is_ok());
    {
        let mut inner = controller.inner.lock().await;
        inner.state.services.get_mut("demo.web").unwrap().spec.image =
            "different.example/app:prod".into();
    }
    let operation = controller
        .submit_service_operation(
            secret,
            false,
            "demo.web",
            ServiceOperationKind::PullRedeploy,
            "ci:1",
            RedeployRequest::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        repository.load().await.unwrap().state.service_operations[&operation.operation_id].image,
        "different.example/app:prod"
    );
    controller.revoke_deploy_token(&token.id).await.unwrap();
    assert!(matches!(
        controller.deploy_principal(secret, false),
        Err(ControllerError::Unauthorized)
    ));
    assert!(
        controller.list_deploy_tokens(true).unwrap()[0]
            .token
            .is_none()
    );
    assert!(repository.deploy_tokens().unwrap()[0].secret.is_none());
    assert!(
        repository.load().await.unwrap().state.service_operations[&operation.operation_id]
            .view
            .status
            .active()
    );
}

#[tokio::test]
async fn ordinary_restart_keeps_identity_and_waits_for_a_fresh_health_observation() {
    let (controller, repository, _directory, task) =
        ready_service("restart-in-place", PullPolicy::Always).await;
    let initial_revision = task.revision;
    let op = controller
        .submit_service_operation(
            ADMIN,
            true,
            "demo.web",
            ServiceOperationKind::Restart,
            "restart:1",
            RedeployRequest::default(),
        )
        .await
        .unwrap();
    let duplicate = controller
        .submit_service_operation(
            ADMIN,
            true,
            "demo.web",
            ServiceOperationKind::Restart,
            "restart:1",
            RedeployRequest::default(),
        )
        .await
        .unwrap();
    assert_eq!(op.operation_id, duplicate.operation_id);
    assert!(matches!(
        controller.scale_service("demo.web", 2).await,
        Err(ControllerError::Conflict(_))
    ));
    assert!(matches!(
        controller.force_update_service("demo.web").await,
        Err(ControllerError::Conflict(_))
    ));
    assert!(matches!(
        controller.remove_stack("demo").await,
        Err(ControllerError::Conflict(_))
    ));
    let response = controller
        .heartbeat("node-a", operation_heartbeat(&task, None, None))
        .await
        .unwrap();
    assert!(response.image_assignments.is_empty());
    let assignment = &response.assignments[0];
    assert!(assignment.restart.as_ref().unwrap().execute);
    assert_eq!(
        assignment.restart.as_ref().unwrap().container_id,
        "container-web"
    );
    let generation = assignment.deployment_generation;
    controller
        .heartbeat("node-a", operation_heartbeat(&task, Some(generation), None))
        .await
        .unwrap();
    assert_eq!(
        controller
            .get_service_operation(ADMIN, true, &op.operation_id, None, 0)
            .await
            .unwrap()
            .status,
        ServiceOperationStatus::Running
    );
    controller
        .heartbeat("node-a", operation_heartbeat(&task, Some(generation), None))
        .await
        .unwrap();
    let result = controller
        .get_service_operation(ADMIN, true, &op.operation_id, None, 0)
        .await
        .unwrap();
    assert_eq!(result.status, ServiceOperationStatus::Succeeded);
    let inner = controller.inner.lock().await;
    assert_eq!(inner.state.services["demo.web"].revision, initial_revision);
    assert_eq!(inner.state.tasks.len(), 1);
    assert_eq!(
        inner.state.tasks[&task.id].container_id.as_deref(),
        Some("container-web")
    );
    assert_eq!(
        inner.state.tasks[&task.id].image_id.as_deref(),
        Some("sha256:current")
    );
    drop(inner);
    assert_eq!(
        repository.load().await.unwrap().state.service_operations[&op.operation_id]
            .view
            .status,
        ServiceOperationStatus::Succeeded
    );
}

#[tokio::test]
async fn restart_processes_replicas_serially_without_replacing_transiently_stopped_tasks() {
    let (controller, _repository, _directory, first) =
        ready_service("serial-restart", PullPolicy::Always).await;
    let mut second = first.clone();
    second.id = "zz-second-task".into();
    second.slot = 1;
    second.container_id = Some("container-second".into());
    second.image_id = Some("sha256:current".into());
    {
        let mut inner = controller.inner.lock().await;
        inner
            .state
            .services
            .get_mut("demo.web")
            .unwrap()
            .spec
            .replicas = 2;
        inner.state.tasks.insert(second.id.clone(), second.clone());
        controller.commit_locked(&mut inner).await.unwrap();
    }
    let heartbeat = |result: Option<(&TaskRecord, u64)>| {
        let mut heartbeat = operation_heartbeat(&first, None, None);
        let mut other = operation_heartbeat(&second, None, None);
        other.tasks[0].container_id = second.container_id.clone();
        heartbeat.tasks.extend(other.tasks);
        if let Some((task, generation)) = result {
            heartbeat.task_results = operation_heartbeat(task, Some(generation), None).task_results;
        }
        heartbeat
    };
    let op = controller
        .submit_service_operation(
            ADMIN,
            true,
            "demo.web",
            ServiceOperationKind::Restart,
            "serial:1",
            RedeployRequest::default(),
        )
        .await
        .unwrap();
    let response = controller
        .heartbeat("node-a", heartbeat(None))
        .await
        .unwrap();
    let executing = response
        .assignments
        .iter()
        .filter(|assignment| assignment.restart.as_ref().is_some_and(|r| r.execute))
        .collect::<Vec<_>>();
    assert_eq!(executing.len(), 1);
    assert_eq!(executing[0].id, first.id);
    let generation = executing[0].deployment_generation;
    let mut stopping = heartbeat(None);
    stopping.tasks[0].observed = ObservedTaskState::Failed;
    controller.heartbeat("node-a", stopping).await.unwrap();
    {
        let inner = controller.inner.lock().await;
        assert_eq!(inner.state.tasks.len(), 2);
        assert!(
            inner
                .state
                .tasks
                .values()
                .all(|task| task.desired == DesiredTaskState::Running)
        );
    }
    for (index, task) in [&first, &second].into_iter().enumerate() {
        controller
            .heartbeat("node-a", heartbeat(Some((task, generation))))
            .await
            .unwrap();
        let response = controller
            .heartbeat("node-a", heartbeat(None))
            .await
            .unwrap();
        let result = controller
            .get_service_operation(ADMIN, true, &op.operation_id, None, 0)
            .await
            .unwrap();
        assert_eq!(result.completed_tasks, index + 1);
        if index == 0 {
            assert_eq!(result.status, ServiceOperationStatus::Running);
            assert!(
                response
                    .assignments
                    .iter()
                    .any(|assignment| assignment.id == second.id
                        && assignment.restart.as_ref().is_some_and(|r| r.execute))
            );
        } else {
            assert_eq!(result.status, ServiceOperationStatus::Succeeded);
        }
    }
    let mut inner = controller.inner.lock().await;
    assert_eq!(inner.state.tasks.len(), 2);
    assert_eq!(inner.state.services["demo.web"].revision, first.revision);
    // Even removing the target afterwards must not discard an accepted request.
    inner.state.services.remove("demo.web");
    drop(inner);
    assert_eq!(
        controller
            .submit_service_operation(
                ADMIN,
                true,
                "demo.web",
                ServiceOperationKind::Restart,
                "serial:1",
                RedeployRequest::default(),
            )
            .await
            .unwrap()
            .operation_id,
        op.operation_id,
    );
}

#[tokio::test]
async fn pull_prepares_before_replacement_and_forces_same_image_once_for_all_policies() {
    for policy in [PullPolicy::Always, PullPolicy::Missing, PullPolicy::Never] {
        let (controller, repository, directory, task) = ready_service("pull-prepare", policy).await;
        let op = controller
            .submit_service_operation(
                ADMIN,
                true,
                "demo.web",
                ServiceOperationKind::PullRedeploy,
                "pull:1",
                RedeployRequest::default(),
            )
            .await
            .unwrap();
        let response = controller
            .heartbeat("node-a", operation_heartbeat(&task, None, None))
            .await
            .unwrap();
        assert_eq!(response.image_assignments.len(), 1);
        assert_eq!(response.assignments[0].revision, task.revision);
        let generation = response.image_assignments[0].deployment_generation;
        let report = ImageResolutionReport {
            deployment_generation: generation,
            image: "nginx:latest".into(),
            resolved_image_id: Some("sha256:current".into()),
            services: vec![ImageResolutionServiceReport {
                service_id: "demo.web".into(),
                old_image_ids: BTreeMap::new(),
                changed: false,
            }],
            error: None,
        };
        controller
            .heartbeat("node-a", operation_heartbeat(&task, None, Some(report)))
            .await
            .unwrap();
        // Crash after preparation, before the revision/deployment transaction.
        let recovered = Controller::new(
            test_controller_config(&test_cluster("pull-prepare"), directory.path()),
            ADMIN.into(),
            repository.clone(),
        )
        .await
        .unwrap();
        recovered.tick().await.unwrap();
        let saved = repository.load().await.unwrap();
        let operation = &saved.state.service_operations[&op.operation_id];
        assert_eq!(saved.state.services["demo.web"].revision, task.revision + 1);
        assert_eq!(saved.state.services["demo.web"].spec.pull_policy, policy);
        assert!(operation.view.deployment_generation.is_some());
        let pinned_generation = operation.view.deployment_generation;
        recovered.tick().await.unwrap();
        assert_eq!(
            repository.load().await.unwrap().state.services["demo.web"].revision,
            task.revision + 1
        );
        let response = recovered
            .heartbeat("node-a", operation_heartbeat(&task, None, None))
            .await
            .unwrap();
        assert!(
            response
                .assignments
                .iter()
                .any(|assignment| assignment.revision == task.revision + 1
                    && assignment.prepared_image_id.as_deref() == Some("sha256:current"))
        );
        assert_eq!(
            repository.load().await.unwrap().state.service_operations[&op.operation_id]
                .view
                .deployment_generation,
            pinned_generation
        );
    }
}

#[tokio::test]
async fn pull_failure_leaves_old_tasks_and_revision_untouched() {
    let (controller, _, _directory, task) = ready_service("pull-failure", PullPolicy::Never).await;
    let op = controller
        .submit_service_operation(
            ADMIN,
            true,
            "demo.web",
            ServiceOperationKind::PullRedeploy,
            "pull:fail",
            RedeployRequest::default(),
        )
        .await
        .unwrap();
    let response = controller
        .heartbeat("node-a", operation_heartbeat(&task, None, None))
        .await
        .unwrap();
    let report = ImageResolutionReport {
        deployment_generation: response.image_assignments[0].deployment_generation,
        image: "nginx:latest".into(),
        resolved_image_id: None,
        services: Vec::new(),
        error: Some("sensitive upstream credentials".into()),
    };
    controller
        .heartbeat("node-a", operation_heartbeat(&task, None, Some(report)))
        .await
        .unwrap();
    controller.tick().await.unwrap();
    let result = controller
        .get_service_operation(ADMIN, true, &op.operation_id, None, 0)
        .await
        .unwrap();
    assert_eq!(result.status, ServiceOperationStatus::Failed);
    assert_eq!(result.error.as_deref(), Some("image_pull_failed"));
    let inner = controller.inner.lock().await;
    assert_eq!(inner.state.services["demo.web"].revision, task.revision);
    assert_eq!(
        inner.state.tasks[&task.id].desired,
        DesiredTaskState::Running
    );
}

#[tokio::test]
async fn public_http_boundary_enforces_scope_idempotency_and_secret_visibility() {
    let (controller, _, _directory, _) = ready_service("public-boundary", PullPolicy::Never).await;
    let token = controller
        .create_deploy_token(DeployTokenCreate {
            name: "ci".into(),
            service: "demo.web".into(),
            expires_in: "never".into(),
        })
        .await
        .unwrap();
    let second = controller
        .create_deploy_token(DeployTokenCreate {
            name: "another".into(),
            service: "demo.web".into(),
            expires_in: "1d".into(),
        })
        .await
        .unwrap();
    let secret = token.token.unwrap();
    let controller = Arc::new(controller);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = super::super::deploy_api::public_router(controller.clone());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let base = format!("http://{address}");
    let client = reqwest::Client::new();
    let endpoint = format!("{base}/v1/deploy/services/demo.web/redeploy");
    for path in [
        "/v1/status",
        "/v1/deploy-tokens",
        "/v1/services/demo.web/restart",
        "/v1/kv",
    ] {
        assert_eq!(
            client
                .get(format!("{base}{path}"))
                .bearer_auth(&secret)
                .send()
                .await
                .unwrap()
                .status(),
            404
        );
    }
    assert_eq!(
        client
            .post(&endpoint)
            .bearer_auth(ADMIN)
            .json(&serde_json::json!({}))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    assert_eq!(
        client
            .post(&endpoint)
            .bearer_auth(&secret)
            .json(&serde_json::json!({}))
            .send()
            .await
            .unwrap()
            .status(),
        400
    );
    assert_eq!(
        client
            .post(&endpoint)
            .bearer_auth(&secret)
            .header("Idempotency-Key", "one")
            .json(&serde_json::json!({"image":"evil"}))
            .send()
            .await
            .unwrap()
            .status(),
        400
    );
    assert_eq!(
        client
            .post(format!("{base}/v1/deploy/services/other.web/redeploy"))
            .bearer_auth(&secret)
            .header("Idempotency-Key", "one")
            .json(&serde_json::json!({}))
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    assert_eq!(
        client
            .post(&endpoint)
            .bearer_auth(&secret)
            .header("Idempotency-Key", "oversize")
            .body(" ".repeat(16 * 1024 + 1))
            .send()
            .await
            .unwrap()
            .status(),
        413
    );
    let accepted = client
        .post(&endpoint)
        .bearer_auth(&secret)
        .header("Idempotency-Key", "one")
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(accepted.status(), 202);
    assert_eq!(accepted.headers()["cache-control"], "no-store");
    let location = accepted.headers()["location"].to_str().unwrap().to_owned();
    let op: ServiceOperation = accepted.json().await.unwrap();
    let duplicate: ServiceOperation = client
        .post(&endpoint)
        .bearer_auth(&secret)
        .header("Idempotency-Key", "one")
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(op.operation_id, duplicate.operation_id);
    assert_eq!(
        client
            .post(&endpoint)
            .bearer_auth(&secret)
            .header("Idempotency-Key", "one")
            .json(&serde_json::json!({"source":{"sha":"different"}}))
            .send()
            .await
            .unwrap()
            .status(),
        409
    );
    assert_eq!(
        client
            .post(&endpoint)
            .bearer_auth(&secret)
            .header("Idempotency-Key", "two")
            .json(&serde_json::json!({}))
            .send()
            .await
            .unwrap()
            .status(),
        409
    );
    assert_eq!(
        client
            .get(format!("{base}{location}"))
            .bearer_auth(second.token.unwrap())
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    let observed = client
        .get(format!("{base}{location}"))
        .bearer_auth(&secret)
        .send()
        .await
        .unwrap();
    let body = observed.text().await.unwrap();
    assert!(!body.contains(&secret));
    assert!(!body.contains("nginx"));
    assert!(!body.contains("source"));
    controller.revoke_deploy_token(&token.id).await.unwrap();
    assert_eq!(
        client
            .get(format!("{base}{location}"))
            .bearer_auth(&secret)
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    server.abort();
}
