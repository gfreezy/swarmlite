use std::{collections::HashMap, path::Path, time::Duration};

use anyhow::{Context, Result, bail};
use bollard::{
    API_DEFAULT_VERSION, Docker,
    models::{
        ContainerCreateBody, HealthConfig, HealthStatusEnum, HostConfig,
        PortBinding as DockerPortBinding, RestartPolicy, RestartPolicyNameEnum,
    },
    query_parameters::{
        CreateContainerOptionsBuilder, CreateImageOptionsBuilder, ListContainersOptionsBuilder,
        RemoveContainerOptionsBuilder, StopContainerOptionsBuilder,
    },
};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::{
    config::AgentConfig,
    model::{
        HeartbeatResponse, NodeHeartbeat, NodeRecord, ObservedTaskState, TaskAssignment, TaskReport,
    },
};

const MANAGED_LABEL: &str = "io.swarmlite.managed";
const TASK_LABEL: &str = "io.swarmlite.task_id";
const SERVICE_LABEL: &str = "io.swarmlite.service_id";
const REVISION_LABEL: &str = "io.swarmlite.revision";
const TERM_LABEL: &str = "io.swarmlite.term";
const GENERATION_LABEL: &str = "io.swarmlite.generation";
const STOP_GRACE_LABEL: &str = "io.swarmlite.stop_grace_seconds";

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct FenceState {
    term: u64,
    generation: u64,
}

#[derive(Debug, Clone)]
struct ManagedContainer {
    id: String,
    task_id: String,
    observed: ObservedTaskState,
    labels: HashMap<String, String>,
}

pub async fn run(config: AgentConfig) -> Result<()> {
    let token = config.token()?;
    let docker = Docker::connect_with_socket(&config.docker_socket, 120, API_DEFAULT_VERSION)
        .with_context(|| format!("failed to connect to Docker at {}", config.docker_socket))?;
    docker
        .ping()
        .await
        .context("Docker daemon did not answer ping")?;
    let system = docker
        .info()
        .await
        .context("failed to read Docker system info")?;
    let node = NodeRecord {
        id: config.node_id.clone(),
        address: config.advertise_address.clone(),
        labels: config.labels.clone(),
        cpu_millis: system.ncpu.unwrap_or(0).max(0) as u64 * 1000,
        memory_bytes: system.mem_total.unwrap_or(0).max(0) as u64,
        port_range_start: config.port_range.start,
        port_range_end: config.port_range.end,
    };
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let mut fence = load_fence(Path::new(&config.state_file)).await?;
    let (assignments_tx, assignments_rx) = tokio::sync::watch::channel(None);
    let worker_docker = docker.clone();
    tokio::spawn(async move {
        reconciliation_worker(worker_docker, assignments_rx).await;
    });
    let mut ticker = tokio::time::interval(Duration::from_secs(config.heartbeat_interval_seconds));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    info!(node_id = %config.node_id, "node agent started");

    loop {
        ticker.tick().await;
        let containers = match list_managed(&docker).await {
            Ok(containers) => containers,
            Err(error) => {
                error!(%error, "failed to inspect managed containers");
                continue;
            }
        };
        let heartbeat = NodeHeartbeat {
            node: node.clone(),
            tasks: containers
                .values()
                .map(|container| TaskReport {
                    id: container.task_id.clone(),
                    observed: container.observed.clone(),
                    container_id: Some(container.id.clone()),
                })
                .collect(),
        };
        let response = match send_heartbeat(&client, &config, &token, &heartbeat).await {
            Ok(response) => response,
            Err(error) => {
                warn!(%error, "all controllers are unavailable; leaving current containers unchanged");
                continue;
            }
        };
        if response.leader_term < fence.term
            || (response.leader_term == fence.term && response.generation < fence.generation)
        {
            warn!(
                received_term = response.leader_term,
                current_term = fence.term,
                received_generation = response.generation,
                current_generation = fence.generation,
                "rejected stale controller response"
            );
            continue;
        }
        fence.term = response.leader_term;
        fence.generation = response.generation;
        if let Err(error) = save_fence(Path::new(&config.state_file), &fence).await {
            error!(%error, "failed to persist fencing state; refusing to change containers");
            continue;
        }
        if assignments_tx.send(Some(response)).is_err() {
            bail!("container reconciliation worker stopped unexpectedly");
        }
    }
}

async fn reconciliation_worker(
    docker: Docker,
    mut assignments: tokio::sync::watch::Receiver<Option<HeartbeatResponse>>,
) {
    while assignments.changed().await.is_ok() {
        let Some(response) = assignments.borrow_and_update().clone() else {
            continue;
        };
        let existing = match list_managed(&docker).await {
            Ok(containers) => containers,
            Err(error) => {
                error!(%error, "failed to inspect containers in reconciliation worker");
                continue;
            }
        };
        if let Err(error) = reconcile_containers(&docker, &existing, &response).await {
            error!(error = %format!("{error:#}"), "container reconciliation failed");
        }
    }
}

async fn send_heartbeat(
    client: &reqwest::Client,
    config: &AgentConfig,
    token: &str,
    heartbeat: &NodeHeartbeat,
) -> Result<HeartbeatResponse> {
    let mut errors = Vec::new();
    for controller in &config.controllers {
        let url = format!(
            "{}/v1/nodes/{}/heartbeat",
            controller.trim_end_matches('/'),
            config.node_id
        );
        match post_heartbeat(client, &url, token, heartbeat).await {
            Ok(response) if response.status().is_success() => {
                return response
                    .json()
                    .await
                    .with_context(|| format!("controller {url} returned invalid JSON"));
            }
            Ok(response) => {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                errors.push(format!("{url}: {status} {body}"));
            }
            Err(error) => errors.push(format!("{url}: {error}")),
        }
    }
    bail!(errors.join("; "))
}

async fn post_heartbeat(
    client: &reqwest::Client,
    initial_url: &str,
    token: &str,
    heartbeat: &NodeHeartbeat,
) -> Result<reqwest::Response> {
    let mut url = initial_url.to_owned();
    for _ in 0..3 {
        let response = client
            .post(&url)
            .bearer_auth(token)
            .json(heartbeat)
            .send()
            .await?;
        if response.status() != reqwest::StatusCode::TEMPORARY_REDIRECT {
            return Ok(response);
        }
        url = response
            .headers()
            .get(reqwest::header::LOCATION)
            .and_then(|value| value.to_str().ok())
            .context("controller redirect omitted a valid Location header")?
            .to_owned();
    }
    bail!("too many controller redirects")
}

async fn list_managed(docker: &Docker) -> Result<HashMap<String, ManagedContainer>> {
    let filters = HashMap::from([("label".to_owned(), vec![format!("{MANAGED_LABEL}=true")])]);
    let options = ListContainersOptionsBuilder::default()
        .all(true)
        .filters(&filters)
        .build();
    let summaries = docker.list_containers(Some(options)).await?;
    let mut result = HashMap::new();
    for summary in summaries {
        let Some(id) = summary.id else { continue };
        let labels = summary.labels.unwrap_or_default();
        let Some(task_id) = labels.get(TASK_LABEL).cloned() else {
            continue;
        };
        let inspect = docker.inspect_container(&id, None).await?;
        let observed = inspect
            .state
            .map(observed_state)
            .unwrap_or(ObservedTaskState::Failed);
        result.insert(
            task_id.clone(),
            ManagedContainer {
                id,
                task_id,
                observed,
                labels,
            },
        );
    }
    Ok(result)
}

fn observed_state(state: bollard::models::ContainerState) -> ObservedTaskState {
    if state.running != Some(true) {
        return if state.restarting == Some(true) {
            ObservedTaskState::Starting
        } else {
            ObservedTaskState::Failed
        };
    }
    match state.health.and_then(|health| health.status) {
        Some(HealthStatusEnum::STARTING) => ObservedTaskState::Starting,
        Some(HealthStatusEnum::UNHEALTHY) => ObservedTaskState::Failed,
        Some(HealthStatusEnum::HEALTHY) => ObservedTaskState::Healthy,
        _ => ObservedTaskState::Healthy,
    }
}

async fn reconcile_containers(
    docker: &Docker,
    existing: &HashMap<String, ManagedContainer>,
    response: &HeartbeatResponse,
) -> Result<()> {
    let desired: HashMap<_, _> = response
        .assignments
        .iter()
        .map(|assignment| (assignment.id.as_str(), assignment))
        .collect();

    for (task_id, container) in existing {
        if !desired.contains_key(task_id.as_str()) {
            remove_container(docker, container).await?;
        }
    }

    for assignment in &response.assignments {
        match existing.get(&assignment.id) {
            Some(container)
                if container.observed == ObservedTaskState::Failed
                    || container.labels.get(REVISION_LABEL)
                        != Some(&assignment.revision.to_string()) =>
            {
                remove_container(docker, container).await?;
                create_container(docker, assignment).await?;
            }
            Some(_) => {}
            None => create_container(docker, assignment).await?,
        }
    }
    Ok(())
}

async fn create_container(docker: &Docker, assignment: &TaskAssignment) -> Result<()> {
    info!(
        task_id = %assignment.id,
        image = %assignment.spec.image,
        "creating task container"
    );
    if docker.inspect_image(&assignment.spec.image).await.is_err() {
        let options = CreateImageOptionsBuilder::default()
            .from_image(&assignment.spec.image)
            .build();
        let mut pull = docker.create_image(Some(options), None, None);
        while let Some(item) = pull.next().await {
            item.with_context(|| format!("failed to pull {}", assignment.spec.image))?;
        }
    }

    let mut port_bindings = HashMap::new();
    let exposed_ports = assignment
        .ports
        .iter()
        .map(|port| format!("{}/{}", port.target, port.protocol))
        .collect::<Vec<_>>();
    for port in &assignment.ports {
        port_bindings.insert(
            format!("{}/{}", port.target, port.protocol),
            Some(vec![DockerPortBinding {
                host_ip: Some("0.0.0.0".to_owned()),
                host_port: Some(port.published.to_string()),
            }]),
        );
    }
    let mut labels: HashMap<String, String> = assignment
        .spec
        .container_labels
        .clone()
        .into_iter()
        .collect();
    labels.extend([
        (MANAGED_LABEL.to_owned(), "true".to_owned()),
        (TASK_LABEL.to_owned(), assignment.id.clone()),
        (SERVICE_LABEL.to_owned(), assignment.service_id.clone()),
        (REVISION_LABEL.to_owned(), assignment.revision.to_string()),
        (TERM_LABEL.to_owned(), assignment.leader_term.to_string()),
        (
            GENERATION_LABEL.to_owned(),
            assignment.generation.to_string(),
        ),
        (
            STOP_GRACE_LABEL.to_owned(),
            assignment.spec.stop_grace_period_seconds.to_string(),
        ),
    ]);
    let host_config = HostConfig {
        binds: (!assignment.spec.volumes.is_empty()).then_some(assignment.spec.volumes.clone()),
        port_bindings: (!port_bindings.is_empty()).then_some(port_bindings),
        restart_policy: Some(RestartPolicy {
            name: Some(RestartPolicyNameEnum::UNLESS_STOPPED),
            maximum_retry_count: None,
        }),
        ..Default::default()
    };
    let body = ContainerCreateBody {
        image: Some(assignment.spec.image.clone()),
        cmd: (!assignment.spec.command.is_empty()).then_some(assignment.spec.command.clone()),
        entrypoint: (!assignment.spec.entrypoint.is_empty())
            .then_some(assignment.spec.entrypoint.clone()),
        env: (!assignment.spec.environment.is_empty())
            .then_some(assignment.spec.environment.clone()),
        exposed_ports: (!exposed_ports.is_empty()).then_some(exposed_ports),
        labels: Some(labels),
        healthcheck: assignment
            .spec
            .healthcheck
            .as_ref()
            .map(|healthcheck| HealthConfig {
                test: Some(healthcheck.test.clone()),
                interval: healthcheck.interval_nanos,
                timeout: healthcheck.timeout_nanos,
                retries: healthcheck.retries,
                start_period: healthcheck.start_period_nanos,
                start_interval: healthcheck.start_interval_nanos,
            }),
        stop_timeout: Some(assignment.spec.stop_grace_period_seconds as i64),
        host_config: Some(host_config),
        ..Default::default()
    };
    let short = assignment.id.chars().take(8).collect::<String>();
    let name = format!(
        "swarmlite-{}-{}-{short}",
        sanitize_name(&assignment.service_id),
        assignment.slot
    );
    let create_options = CreateContainerOptionsBuilder::default().name(&name).build();
    let created = docker
        .create_container(Some(create_options), body)
        .await
        .with_context(|| format!("failed to create task {}", assignment.id))?;
    docker
        .start_container(&created.id, None)
        .await
        .with_context(|| format!("failed to start task {}", assignment.id))?;
    Ok(())
}

async fn remove_container(docker: &Docker, container: &ManagedContainer) -> Result<()> {
    let grace = container
        .labels
        .get(STOP_GRACE_LABEL)
        .and_then(|value| value.parse::<i32>().ok())
        .unwrap_or(10);
    info!(task_id = %container.task_id, "removing obsolete task container");
    let stop = StopContainerOptionsBuilder::default().t(grace).build();
    if let Err(error) = docker.stop_container(&container.id, Some(stop)).await {
        warn!(task_id = %container.task_id, %error, "graceful stop failed; forcing removal");
    }
    let remove = RemoveContainerOptionsBuilder::default().force(true).build();
    docker
        .remove_container(&container.id, Some(remove))
        .await
        .with_context(|| format!("failed to remove task {}", container.task_id))?;
    Ok(())
}

fn sanitize_name(value: &str) -> String {
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

async fn load_fence(path: &Path) -> Result<FenceState> {
    match tokio::fs::read(path).await {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .with_context(|| format!("invalid agent state {}", path.display())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(FenceState::default()),
        Err(error) => Err(error).with_context(|| format!("failed to read {}", path.display())),
    }
}

async fn save_fence(path: &Path, state: &FenceState) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let temporary = path.with_extension("tmp");
    tokio::fs::write(&temporary, serde_json::to_vec(state)?).await?;
    tokio::fs::rename(&temporary, path).await?;
    Ok(())
}
