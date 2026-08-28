use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Result, bail};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info, warn};

use crate::{
    client::ControllerClient,
    config::AgentConfig,
    config_files::ConfigCache,
    data_plane::{DATA_STREAM_WRITE_TIMEOUT, DataChannel, DataFrame, MAX_DATA_PAYLOAD_BYTES},
    local_state::{AgentFence, FENCE_KEY, LocalState},
    model::{
        AgentCommand, AgentCommandAck, AgentCommandOperation, AgentCommandPollResponse,
        AgentCommandResult, AgentDataStream, AgentDataStreamOperation,
        CONFIG_GC_GRACE_PERIOD_SECONDS, GatewayReport, HeartbeatResponse, ImageResolutionProgress,
        ImageResolutionReport, ImageResolutionServiceReport, MAX_CONFIG_FILE_BYTES, NodeControl,
        NodeHeartbeat, NodeRecord, ObservedTaskState, TaskReconcilePhase, TaskReconcileProgress,
        TaskReconcileReport, TaskReport,
    },
    registry::RegistryCredentialStore,
    runtime::{
        ContainerRuntime, DockerCompatibleRuntime, ManagedContainer, RuntimeImageProgress,
        RuntimeLogChannel, RuntimeLogChunk, RuntimeTaskProgress,
    },
};

const AGENT_DATA_QUEUE_FRAMES: usize = 64;
const RUNTIME_LOG_QUEUE_FRAMES: usize = 16;

#[derive(Clone)]
struct ReconciliationState {
    task_results: Arc<Mutex<BTreeMap<String, TaskReconcileReport>>>,
    task_progress: Arc<Mutex<BTreeMap<(String, TaskReconcilePhase), TaskReconcileProgress>>>,
    image_results: Arc<Mutex<BTreeMap<(u64, String), ImageResolutionReport>>>,
    image_progress: Arc<Mutex<BTreeMap<(u64, String), ImageResolutionProgress>>>,
    events: tokio::sync::mpsc::UnboundedSender<()>,
}

pub(crate) async fn run_with_token_and_updates(
    config: AgentConfig,
    token: String,
    updates: tokio::sync::watch::Sender<NodeControl>,
    gateway_report: tokio::sync::watch::Receiver<GatewayReport>,
    local_state: LocalState,
    runtime: DockerCompatibleRuntime,
) -> Result<()> {
    run_with_runtime(config, token, updates, gateway_report, local_state, runtime).await
}

async fn run_with_runtime<R: ContainerRuntime>(
    config: AgentConfig,
    token: String,
    updates: tokio::sync::watch::Sender<NodeControl>,
    gateway_report: tokio::sync::watch::Receiver<GatewayReport>,
    local_state: LocalState,
    runtime: R,
) -> Result<()> {
    runtime.ping().await?;
    let system = runtime.system_info().await?;
    let mut fence = local_state
        .get::<AgentFence>(FENCE_KEY)?
        .unwrap_or_default();
    let registry_credentials = RegistryCredentialStore::new(local_state.clone());
    let mut node = NodeRecord {
        id: config.node_id.clone(),
        address: config.advertise_address.clone(),
        labels: config.labels.clone(),
        cpu_millis: system.cpu_millis,
        memory_bytes: system.memory_bytes,
        port_range_start: config.port_range.start,
        port_range_end: config.port_range.end,
        gateway_enabled: config.gateway_enabled,
    };
    let client = ControllerClient::new(&config.controller, token);
    let (assignments_tx, assignments_rx) = tokio::sync::watch::channel(None);
    let reconcile_results = Arc::new(Mutex::new(BTreeMap::<String, TaskReconcileReport>::new()));
    let reconcile_progress = Arc::new(Mutex::new(BTreeMap::<
        (String, TaskReconcilePhase),
        TaskReconcileProgress,
    >::new()));
    let image_results = Arc::new(Mutex::new(
        BTreeMap::<(u64, String), ImageResolutionReport>::new(),
    ));
    let image_progress = Arc::new(Mutex::new(
        BTreeMap::<(u64, String), ImageResolutionProgress>::new(),
    ));
    let (reconcile_events_tx, mut reconcile_events_rx) = tokio::sync::mpsc::unbounded_channel();
    let reconciliation_state = ReconciliationState {
        task_results: Arc::clone(&reconcile_results),
        task_progress: Arc::clone(&reconcile_progress),
        image_results: Arc::clone(&image_results),
        image_progress: Arc::clone(&image_progress),
        events: reconcile_events_tx,
    };
    let runtime = Arc::new(runtime);
    let reconcile_runtime = Arc::clone(&runtime);
    let reconcile_cluster_id = config.cluster_id.clone();
    let reconcile_client = client.clone();
    let config_cache = ConfigCache::new(config.config_dir.clone());
    tokio::spawn(async move {
        reconciliation_loop(
            reconcile_runtime,
            reconcile_client,
            config_cache,
            assignments_rx,
            reconcile_cluster_id,
            reconciliation_state,
        )
        .await;
    });
    let command_runtime = Arc::clone(&runtime);
    let command_client = client.clone();
    let command_node_id = config.node_id.clone();
    let command_cluster_id = config.cluster_id.clone();
    tokio::spawn(async move {
        command_loop(
            command_runtime,
            command_client,
            command_node_id,
            command_cluster_id,
        )
        .await;
    });
    let mut ticker = tokio::time::interval(Duration::from_secs(config.heartbeat_interval_seconds));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    info!(
        node_id = %config.node_id,
        runtime = %runtime.kind(),
        socket = runtime.socket(),
        "node agent started"
    );

    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            event = reconcile_events_rx.recv() => {
                if event.is_none() {
                    bail!("container reconciliation loop stopped unexpectedly");
                }
            }
        }
        let (containers, task_inventory_error) =
            match runtime.list_managed(&config.cluster_id).await {
                Ok(containers) => (containers, None),
                Err(error) => {
                    error!(%error, "failed to inspect managed containers");
                    (HashMap::new(), Some(format!("{error:#}")))
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
                    image_id: container.image_id.clone(),
                    cluster_id: container.cluster_id.clone(),
                    stack: container.stack.clone(),
                    service: container.service.clone(),
                    slot: container.slot,
                    revision: container.revision,
                    spec_hash: container.spec_hash.clone(),
                    ports: container.ports.clone(),
                    config_digests: container.config_digests.clone(),
                })
                .collect(),
            task_inventory_error,
            task_results: reconcile_results
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .values()
                .cloned()
                .collect(),
            task_progress: reconcile_progress
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .values()
                .cloned()
                .collect(),
            image_results: image_results
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .values()
                .cloned()
                .collect(),
            image_progress: image_progress
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .values()
                .cloned()
                .collect(),
            gateway: gateway_report.borrow().clone(),
        };
        let response = match send_heartbeat(&client, &config.node_id, &heartbeat).await {
            Ok(response) => response,
            Err(error) => {
                warn!(%error, "controller is unavailable; leaving current containers unchanged");
                continue;
            }
        };
        if response.generation < fence.generation {
            warn!(
                received_generation = response.generation,
                current_generation = fence.generation,
                "rejected stale controller response"
            );
            continue;
        }
        fence.generation = response.generation;
        if let Err(error) = local_state.put(FENCE_KEY, &fence) {
            error!(%error, "failed to persist fencing state; refusing to change containers");
            continue;
        }
        if let Err(error) = registry_credentials.replace(&response.registry_credentials) {
            error!(%error, "failed to persist registry credentials; refusing to change containers");
            continue;
        }
        let next_control = NodeControl {
            cluster: response.cluster.clone(),
            gateway_enabled: response.gateway_enabled,
            labels: response.labels.clone(),
            gateway_config: response.gateway_config.clone(),
            registry_credentials_hash: response.registry_credentials_hash.clone(),
        };
        let gateway_needs_apply = response.gateway_enabled
            && response.gateway_config.as_ref().is_some_and(|assignment| {
                heartbeat.gateway.error.is_some()
                    || heartbeat.gateway.applied_generation != Some(assignment.generation)
            });
        node.gateway_enabled = response.gateway_enabled;
        node.labels.clone_from(&response.labels);
        if gateway_needs_apply {
            updates.send_replace(next_control);
        } else {
            updates.send_if_modified(|current| {
                if *current == next_control {
                    false
                } else {
                    current.clone_from(&next_control);
                    true
                }
            });
        }
        if assignments_tx.send(Some(response)).is_err() {
            bail!("container reconciliation loop stopped unexpectedly");
        }
    }
}

async fn command_loop<R: ContainerRuntime>(
    runtime: Arc<R>,
    client: ControllerClient,
    node_id: String,
    cluster_id: String,
) {
    loop {
        let path = format!("/v1/nodes/{node_id}/commands?wait_seconds=20");
        let poll = match client.get_json::<AgentCommandPollResponse>(&path).await {
            Ok(poll) => poll,
            Err(error) => {
                warn!(%error, "failed to poll controller command channel");
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
        };
        let Some(command) = poll.command else {
            continue;
        };
        let result = execute_command(
            Arc::clone(&runtime),
            &client,
            &node_id,
            &cluster_id,
            &command,
        )
        .await;
        publish_command_result(&client, &node_id, &command.id, &result).await;
    }
}

async fn execute_command<R: ContainerRuntime>(
    runtime: Arc<R>,
    client: &ControllerClient,
    node_id: &str,
    cluster_id: &str,
    command: &AgentCommand,
) -> AgentCommandResult {
    let result: Result<()> = async {
        match &command.operation {
            AgentCommandOperation::OpenDataSession {
                session_id,
                upload_token,
                streams,
            } => {
                let path = format!(
                    "/v1/data-sessions/{}/nodes/{}",
                    encode_path_segment(session_id),
                    encode_path_segment(node_id)
                );
                let socket = client.connect_data_websocket(&path, upload_token).await?;
                let runtime = Arc::clone(&runtime);
                let cluster_id = cluster_id.to_owned();
                let streams = streams.clone();
                tokio::spawn(async move {
                    if let Err(error) = run_data_session(runtime, cluster_id, streams, socket).await
                    {
                        warn!(%error, "data session stopped with an error");
                    }
                });
                Ok(())
            }
        }
    }
    .await;
    match result {
        Ok(()) => AgentCommandResult { error: None },
        Err(error) => AgentCommandResult {
            error: Some(format!("{error:#}")),
        },
    }
}

async fn run_data_session<R: ContainerRuntime>(
    runtime: Arc<R>,
    cluster_id: String,
    streams: Vec<AgentDataStream>,
    socket: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> Result<()> {
    let containers = runtime.list_managed(&cluster_id).await;
    let (frame_sender, mut frame_receiver) = mpsc::channel::<Vec<u8>>(AGENT_DATA_QUEUE_FRAMES);
    let mut tasks = tokio::task::JoinSet::new();

    match containers {
        Ok(containers) => {
            for stream in streams {
                let Some(container) = containers.get(&stream.task_id).cloned() else {
                    send_data_frame(
                        &frame_sender,
                        DataFrame::error(
                            stream.stream_id,
                            0,
                            format!("task {:?} is not present on this node", stream.task_id),
                        ),
                    )
                    .await?;
                    send_data_frame(&frame_sender, DataFrame::end(stream.stream_id, 1)).await?;
                    continue;
                };
                let stream_runtime = Arc::clone(&runtime);
                let stream_sender = frame_sender.clone();
                tasks.spawn(async move {
                    stream_agent_data(stream_runtime, container, stream, stream_sender).await
                });
            }
        }
        Err(error) => {
            for stream in streams {
                send_data_frame(
                    &frame_sender,
                    DataFrame::error(stream.stream_id, 0, format!("{error:#}")),
                )
                .await?;
                send_data_frame(&frame_sender, DataFrame::end(stream.stream_id, 1)).await?;
            }
        }
    }
    drop(frame_sender);

    let (mut sink, mut source) = socket.split();
    let transfer = async {
        loop {
            tokio::select! {
                frame = frame_receiver.recv() => {
                    let Some(frame) = frame else { break; };
                    tokio::time::timeout(
                        DATA_STREAM_WRITE_TIMEOUT,
                        sink.send(Message::Binary(frame.into())),
                    )
                    .await
                    .map_err(|_| anyhow::anyhow!(
                        "timed out writing to the Controller data stream"
                    ))??;
                }
                incoming = source.next() => {
                    match incoming {
                        Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                        _ => {}
                    }
                }
            }
        }
        anyhow::Ok(())
    }
    .await;
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    let _ = tokio::time::timeout(DATA_STREAM_WRITE_TIMEOUT, sink.close()).await;
    transfer
}

async fn stream_agent_data<R: ContainerRuntime>(
    runtime: Arc<R>,
    container: ManagedContainer,
    stream: AgentDataStream,
    frame_sender: mpsc::Sender<Vec<u8>>,
) -> Result<()> {
    match stream.operation {
        AgentDataStreamOperation::Logs { tail, follow } => {
            let (log_sender, mut log_receiver) =
                mpsc::channel::<RuntimeLogChunk>(RUNTIME_LOG_QUEUE_FRAMES);
            let log_future = runtime.stream_task_logs(&container, tail, follow, log_sender);
            tokio::pin!(log_future);
            let mut result = None;
            let mut sequence = 0_u64;
            loop {
                tokio::select! {
                    chunk = log_receiver.recv() => {
                        let Some(chunk) = chunk else { break; };
                        let channel = match chunk.channel {
                            RuntimeLogChannel::Stdout => DataChannel::Stdout,
                            RuntimeLogChannel::Stderr => DataChannel::Stderr,
                            RuntimeLogChannel::Stdin => DataChannel::Stdin,
                            RuntimeLogChannel::Console => DataChannel::Console,
                        };
                        for payload in chunk.payload.chunks(MAX_DATA_PAYLOAD_BYTES) {
                            send_data_frame(
                                &frame_sender,
                                DataFrame::data(
                                    stream.stream_id,
                                    sequence,
                                    channel,
                                    bytes::Bytes::copy_from_slice(payload),
                                ),
                            )
                            .await?;
                            sequence = sequence.saturating_add(1);
                        }
                    }
                    completed = &mut log_future, if result.is_none() => {
                        result = Some(completed);
                    }
                }
            }
            let result = match result {
                Some(result) => result,
                None => log_future.await,
            };
            if let Err(error) = result {
                send_data_frame(
                    &frame_sender,
                    DataFrame::error(stream.stream_id, sequence, format!("{error:#}")),
                )
                .await?;
                sequence = sequence.saturating_add(1);
            }
            send_data_frame(&frame_sender, DataFrame::end(stream.stream_id, sequence)).await
        }
    }
}

async fn send_data_frame(sender: &mpsc::Sender<Vec<u8>>, frame: DataFrame) -> Result<()> {
    let encoded = frame.encode().map_err(anyhow::Error::msg)?;
    sender
        .send(encoded)
        .await
        .map_err(|_| anyhow::anyhow!("data session closed"))
}

fn encode_path_segment(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}

async fn publish_command_result(
    client: &ControllerClient,
    node_id: &str,
    command_id: &str,
    result: &AgentCommandResult,
) {
    let path = format!("/v1/nodes/{node_id}/commands/{command_id}/result");
    loop {
        match client
            .send_json::<AgentCommandAck, _>(reqwest::Method::POST, &path, Some(result))
            .await
        {
            Ok(_) => return,
            Err(error) if error.is_retryable() => {
                warn!(%error, command_id, "failed to publish agent command result; retrying");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Err(error) => {
                warn!(%error, command_id, "controller rejected agent command result");
                return;
            }
        }
    }
}

async fn reconciliation_loop<R: ContainerRuntime>(
    runtime: Arc<R>,
    client: ControllerClient,
    config_cache: ConfigCache,
    mut assignments: tokio::sync::watch::Receiver<Option<HeartbeatResponse>>,
    cluster_id: String,
    state: ReconciliationState,
) {
    let progress = ReconcileProgressPublisher {
        progress: Arc::clone(&state.task_progress),
        events: state.events.clone(),
    };
    let image_progress = ImageProgressPublisher {
        progress: Arc::clone(&state.image_progress),
        events: state.events.clone(),
    };
    while assignments.changed().await.is_ok() {
        let Some(response) = assignments.borrow_and_update().clone() else {
            continue;
        };
        let existing = match runtime.list_managed(&cluster_id).await {
            Ok(containers) => containers,
            Err(error) => {
                error!(%error, "failed to inspect containers in reconciliation loop");
                let message = format!("{error:#}");
                let reports = response
                    .assignments
                    .iter()
                    .map(|assignment| (&assignment.id, assignment.deployment_generation))
                    .chain(
                        response
                            .remove_tasks
                            .iter()
                            .map(|assignment| (&assignment.id, assignment.deployment_generation)),
                    )
                    .map(|(task_id, desired_generation)| TaskReconcileReport {
                        task_id: task_id.clone(),
                        desired_generation,
                        applied_generation: None,
                        phase: TaskReconcilePhase::Inspect,
                        error: Some(message.clone()),
                    })
                    .collect();
                publish_reconcile_results(&response, reports, &state.task_results, &state.events);
                let image_reports = response
                    .image_assignments
                    .iter()
                    .map(|assignment| ImageResolutionReport {
                        deployment_generation: assignment.deployment_generation,
                        image: assignment.image.clone(),
                        resolved_image_id: None,
                        services: Vec::new(),
                        error: Some(message.clone()),
                    })
                    .collect();
                publish_image_results(
                    &response,
                    image_reports,
                    &state.image_results,
                    &state.events,
                );
                continue;
            }
        };
        progress.retain_for_response(&response);
        image_progress.retain_for_response(&response);
        let image_reports = reconcile_images(
            runtime.as_ref(),
            &existing,
            &response,
            &state.image_results,
            &image_progress,
        )
        .await;
        publish_image_results(
            &response,
            image_reports,
            &state.image_results,
            &state.events,
        );
        let config_errors = prepare_assignment_configs(
            &client,
            &config_cache,
            &existing,
            &response,
            Some(&progress),
        )
        .await;
        let reports = reconcile_containers_with_progress(
            runtime.as_ref(),
            &existing,
            &response,
            Some(&progress),
            &config_errors,
        )
        .await;
        publish_reconcile_results(&response, reports, &state.task_results, &state.events);
        let referenced_paths = referenced_config_cache_paths(&config_cache, &existing, &response);
        let grace_period_ms =
            i64::try_from(CONFIG_GC_GRACE_PERIOD_SECONDS.saturating_mul(1_000)).unwrap_or(i64::MAX);
        match config_cache
            .gc_at(&referenced_paths, unix_ms(), grace_period_ms)
            .await
        {
            Ok(stats) if stats.failures > 0 => warn!(
                referenced = stats.referenced,
                marked = stats.marked,
                retained_for_grace = stats.retained_for_grace,
                deleted = stats.deleted,
                failures = stats.failures,
                "Agent config cache garbage collection had file deletion failures"
            ),
            Ok(stats) if stats.marked > 0 || stats.deleted > 0 => info!(
                referenced = stats.referenced,
                marked = stats.marked,
                retained_for_grace = stats.retained_for_grace,
                deleted = stats.deleted,
                failures = stats.failures,
                "reconciled Agent config cache garbage collection"
            ),
            Ok(stats) if stats.retained_for_grace > 0 => debug!(
                referenced = stats.referenced,
                retained_for_grace = stats.retained_for_grace,
                "Agent config cache garbage collection retained grace-period candidates"
            ),
            Ok(_) => {}
            Err(error) => warn!(%error, "Agent config cache garbage collection failed"),
        }
    }
}

fn referenced_config_cache_paths(
    cache: &ConfigCache,
    existing: &HashMap<String, ManagedContainer>,
    response: &HeartbeatResponse,
) -> BTreeSet<PathBuf> {
    response
        .assignments
        .iter()
        .flat_map(|assignment| assignment.spec.configs.iter())
        .map(|mount| cache.host_path(mount))
        .chain(
            existing
                .values()
                .flat_map(|container| container.config_cache_paths.iter().cloned()),
        )
        .collect()
}

fn unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

#[derive(Clone)]
struct ImageProgressPublisher {
    progress: Arc<Mutex<BTreeMap<(u64, String), ImageResolutionProgress>>>,
    events: tokio::sync::mpsc::UnboundedSender<()>,
}

impl ImageProgressPublisher {
    fn reporter(&self, deployment_generation: u64, image: &str) -> RuntimeImageProgress {
        let image = image.to_owned();
        let progress = Arc::clone(&self.progress);
        let events = self.events.clone();
        RuntimeImageProgress::new(move |status| {
            let next = ImageResolutionProgress {
                deployment_generation,
                image: image.clone(),
                status,
            };
            let key = (deployment_generation, image.clone());
            let mut current = progress
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if current.get(&key) != Some(&next) {
                current.insert(key, next);
                let _ = events.send(());
            }
        })
    }

    fn retain_for_response(&self, response: &HeartbeatResponse) {
        let expected = response
            .image_assignments
            .iter()
            .map(|assignment| (assignment.deployment_generation, assignment.image.as_str()))
            .collect::<std::collections::HashSet<_>>();
        self.progress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain(|(generation, image), _| expected.contains(&(*generation, image.as_str())));
    }
}

async fn reconcile_images<R: ContainerRuntime>(
    runtime: &R,
    existing: &HashMap<String, ManagedContainer>,
    response: &HeartbeatResponse,
    current_results: &Mutex<BTreeMap<(u64, String), ImageResolutionReport>>,
    progress: &ImageProgressPublisher,
) -> Vec<ImageResolutionReport> {
    let completed = current_results
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .keys()
        .cloned()
        .collect::<std::collections::HashSet<_>>();
    let mut reports = Vec::new();
    for assignment in &response.image_assignments {
        let key = (assignment.deployment_generation, assignment.image.clone());
        if completed.contains(&key) {
            continue;
        }
        let reporter = progress.reporter(assignment.deployment_generation, &assignment.image);
        let result = async {
            let resolved_image_id = runtime.resolve_image(&assignment.image, &reporter).await?;
            let mut services = Vec::new();
            for service in &assignment.services {
                let mut old_image_ids = BTreeMap::new();
                for task_id in &service.task_ids {
                    let container = existing.get(task_id).ok_or_else(|| {
                        anyhow::anyhow!("running task {task_id} disappeared during image check")
                    })?;
                    let image_id = container.image_id.clone().ok_or_else(|| {
                        anyhow::anyhow!(
                            "runtime did not report the image ID for running task {task_id}"
                        )
                    })?;
                    old_image_ids.insert(task_id.clone(), image_id);
                }
                let changed = old_image_ids
                    .values()
                    .any(|image_id| image_id != &resolved_image_id);
                services.push(ImageResolutionServiceReport {
                    service_id: service.service_id.clone(),
                    old_image_ids,
                    changed,
                });
            }
            anyhow::Ok((resolved_image_id, services))
        }
        .await;
        reports.push(match result {
            Ok((resolved_image_id, services)) => ImageResolutionReport {
                deployment_generation: assignment.deployment_generation,
                image: assignment.image.clone(),
                resolved_image_id: Some(resolved_image_id),
                services,
                error: None,
            },
            Err(error) => ImageResolutionReport {
                deployment_generation: assignment.deployment_generation,
                image: assignment.image.clone(),
                resolved_image_id: None,
                services: Vec::new(),
                error: Some(format!("{error:#}")),
            },
        });
    }
    reports
}

fn publish_image_results(
    response: &HeartbeatResponse,
    reports: Vec<ImageResolutionReport>,
    results: &Mutex<BTreeMap<(u64, String), ImageResolutionReport>>,
    events: &tokio::sync::mpsc::UnboundedSender<()>,
) {
    let expected = response
        .image_assignments
        .iter()
        .map(|assignment| (assignment.deployment_generation, assignment.image.as_str()))
        .collect::<std::collections::HashSet<_>>();
    let mut current = results
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let previous = current.clone();
    current.retain(|(generation, image), _| expected.contains(&(*generation, image.as_str())));
    for report in reports {
        current.insert((report.deployment_generation, report.image.clone()), report);
    }
    if *current != previous {
        let _ = events.send(());
    }
}

#[derive(Clone)]
struct ReconcileProgressPublisher {
    progress: Arc<Mutex<BTreeMap<(String, TaskReconcilePhase), TaskReconcileProgress>>>,
    events: tokio::sync::mpsc::UnboundedSender<()>,
}

impl ReconcileProgressPublisher {
    fn reporter(&self, task_id: &str, desired_generation: u64) -> RuntimeTaskProgress {
        let task_id = task_id.to_owned();
        let progress = Arc::clone(&self.progress);
        let events = self.events.clone();
        RuntimeTaskProgress::new(move |phase| {
            let next = TaskReconcileProgress {
                task_id: task_id.clone(),
                desired_generation,
                phase,
            };
            let mut current = progress
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let key = (task_id.clone(), phase);
            if current.get(&key) != Some(&next) {
                current.insert(key, next);
                let _ = events.send(());
            }
        })
    }

    fn retain_for_response(&self, response: &HeartbeatResponse) {
        let expected = response
            .assignments
            .iter()
            .map(|assignment| assignment.id.as_str())
            .chain(
                response
                    .remove_tasks
                    .iter()
                    .map(|assignment| assignment.id.as_str()),
            )
            .collect::<std::collections::HashSet<_>>();
        self.progress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain(|(task_id, _), _| expected.contains(task_id.as_str()));
    }
}

fn publish_reconcile_results(
    response: &HeartbeatResponse,
    reports: Vec<TaskReconcileReport>,
    results: &Mutex<BTreeMap<String, TaskReconcileReport>>,
    events: &tokio::sync::mpsc::UnboundedSender<()>,
) {
    let expected = response
        .assignments
        .iter()
        .map(|assignment| assignment.id.as_str())
        .chain(
            response
                .remove_tasks
                .iter()
                .map(|assignment| assignment.id.as_str()),
        )
        .collect::<std::collections::HashSet<_>>();
    let mut current = results
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let previous = current.clone();
    current.retain(|task_id, _| expected.contains(task_id.as_str()));
    for report in reports {
        current.insert(report.task_id.clone(), report);
    }
    if *current != previous {
        let _ = events.send(());
    }
}

async fn send_heartbeat(
    client: &ControllerClient,
    node_id: &str,
    heartbeat: &NodeHeartbeat,
) -> Result<HeartbeatResponse> {
    Ok(client
        .send_json(
            reqwest::Method::POST,
            &format!("/v1/nodes/{node_id}/heartbeat"),
            Some(heartbeat),
        )
        .await?)
}

#[cfg(test)]
async fn reconcile_containers<R: ContainerRuntime>(
    runtime: &R,
    existing: &HashMap<String, ManagedContainer>,
    response: &HeartbeatResponse,
) -> Vec<TaskReconcileReport> {
    reconcile_containers_with_progress(runtime, existing, response, None, &HashMap::new()).await
}

async fn reconcile_containers_with_progress<R: ContainerRuntime>(
    runtime: &R,
    existing: &HashMap<String, ManagedContainer>,
    response: &HeartbeatResponse,
    progress: Option<&ReconcileProgressPublisher>,
    config_errors: &HashMap<String, String>,
) -> Vec<TaskReconcileReport> {
    let mut reports = Vec::new();
    for removal in &response.remove_tasks {
        let reporter = progress.map_or_else(RuntimeTaskProgress::default, |progress| {
            progress.reporter(&removal.id, removal.deployment_generation)
        });
        let result = match existing.get(&removal.id) {
            Some(container) => runtime.remove_task(container, &reporter).await,
            None => {
                reporter.report(TaskReconcilePhase::Remove);
                Ok(())
            }
        };
        reports.push(reconcile_report(
            &removal.id,
            removal.deployment_generation,
            TaskReconcilePhase::Remove,
            result,
        ));
    }

    for assignment in &response.assignments {
        let reporter = progress.map_or_else(RuntimeTaskProgress::default, |progress| {
            progress.reporter(&assignment.id, assignment.deployment_generation)
        });
        if assignment.desired == crate::model::DesiredTaskState::Draining {
            reporter.report(TaskReconcilePhase::Verify);
            reports.push(reconcile_report(
                &assignment.id,
                assignment.deployment_generation,
                TaskReconcilePhase::Verify,
                Ok(()),
            ));
            continue;
        }
        if let Some(message) = config_errors.get(&assignment.id) {
            reporter.report(TaskReconcilePhase::Config);
            reports.push(TaskReconcileReport {
                task_id: assignment.id.clone(),
                desired_generation: assignment.deployment_generation,
                applied_generation: None,
                phase: TaskReconcilePhase::Config,
                error: Some(message.clone()),
            });
            continue;
        }
        let (phase, result) = match existing.get(&assignment.id) {
            Some(container)
                if container.revision.map_or_else(
                    || container.spec_hash.as_deref() != Some(&assignment.spec_hash),
                    |revision| revision != assignment.revision,
                ) =>
            {
                let result = async {
                    runtime.remove_task(container, &reporter).await?;
                    runtime.create_task(assignment, &reporter).await
                }
                .await;
                (TaskReconcilePhase::Replace, result)
            }
            Some(container) if !container.running => {
                let result = match runtime.start_task(container, &reporter).await {
                    Err(error)
                        if crate::runtime::is_host_port_conflict(&error)
                            && assignment_uses_only_dynamic_ports(assignment) =>
                    {
                        async {
                            runtime.remove_task(container, &reporter).await?;
                            runtime.create_task(assignment, &reporter).await
                        }
                        .await
                    }
                    result => result,
                };
                (TaskReconcilePhase::Start, result)
            }
            Some(container) if container.observed == ObservedTaskState::Failed => {
                let result = async {
                    runtime.remove_task(container, &reporter).await?;
                    runtime.create_task(assignment, &reporter).await
                }
                .await;
                (TaskReconcilePhase::Replace, result)
            }
            Some(_) => {
                reporter.report(TaskReconcilePhase::Verify);
                (TaskReconcilePhase::Verify, Ok(()))
            }
            None => (
                TaskReconcilePhase::Create,
                runtime.create_task(assignment, &reporter).await,
            ),
        };
        if result.is_ok() {
            reporter.report(TaskReconcilePhase::Verify);
        }
        if let Err(error) = &result {
            error!(
                task_id = %assignment.id,
                error = %format!("{error:#}"),
                "container reconciliation failed"
            );
        }
        reports.push(reconcile_report(
            &assignment.id,
            assignment.deployment_generation,
            phase,
            result,
        ));
    }
    reports
}

async fn prepare_assignment_configs(
    client: &ControllerClient,
    cache: &ConfigCache,
    existing: &HashMap<String, ManagedContainer>,
    response: &HeartbeatResponse,
    progress: Option<&ReconcileProgressPublisher>,
) -> HashMap<String, String> {
    let mut downloads = HashMap::<String, Result<Vec<u8>, String>>::new();
    let mut errors = HashMap::new();
    for assignment in &response.assignments {
        if assignment.desired != crate::model::DesiredTaskState::Running
            || assignment.spec.configs.is_empty()
            || !assignment_requires_runtime_change(existing.get(&assignment.id), assignment)
        {
            continue;
        }
        let reporter = progress.map_or_else(RuntimeTaskProgress::default, |progress| {
            progress.reporter(&assignment.id, assignment.deployment_generation)
        });
        reporter.report(TaskReconcilePhase::Config);
        for mount in &assignment.spec.configs {
            if cache.is_ready(mount).await {
                continue;
            }
            if !downloads.contains_key(&mount.digest) {
                let result = client
                    .get_bytes(&format!("/v1/configs/{}", mount.digest))
                    .await
                    .map_err(|error| {
                        format!("failed to download config {:?}: {error}", mount.source)
                    })
                    .and_then(|contents| {
                        if contents.len() > MAX_CONFIG_FILE_BYTES {
                            Err(format!(
                                "config {:?} exceeded the {MAX_CONFIG_FILE_BYTES}-byte Agent limit",
                                mount.source
                            ))
                        } else {
                            Ok(contents)
                        }
                    });
                downloads.insert(mount.digest.clone(), result);
            }
            let result = match downloads.get(&mount.digest).expect("inserted above") {
                Ok(contents) => cache
                    .materialize(mount, contents)
                    .await
                    .map(|_| ())
                    .map_err(|error| format!("{error:#}")),
                Err(message) => Err(message.clone()),
            };
            if let Err(message) = result {
                errors.insert(assignment.id.clone(), message);
                break;
            }
        }
    }
    errors
}

fn assignment_requires_runtime_change(
    existing: Option<&ManagedContainer>,
    assignment: &crate::model::TaskAssignment,
) -> bool {
    match existing {
        None => true,
        Some(container) => {
            !container.running
                || container.observed == ObservedTaskState::Failed
                || container.revision.map_or_else(
                    || container.spec_hash.as_deref() != Some(&assignment.spec_hash),
                    |revision| revision != assignment.revision,
                )
        }
    }
}

fn reconcile_report(
    task_id: &str,
    desired_generation: u64,
    phase: TaskReconcilePhase,
    result: Result<()>,
) -> TaskReconcileReport {
    match result {
        Ok(()) => TaskReconcileReport {
            task_id: task_id.to_owned(),
            desired_generation,
            applied_generation: Some(desired_generation),
            phase,
            error: None,
        },
        Err(error) => TaskReconcileReport {
            task_id: task_id.to_owned(),
            desired_generation,
            applied_generation: None,
            phase,
            error: Some(format!("{error:#}")),
        },
    }
}

fn assignment_uses_only_dynamic_ports(assignment: &crate::model::TaskAssignment) -> bool {
    !assignment.ports.is_empty()
        && assignment.ports.iter().all(|binding| {
            assignment
                .spec
                .ports
                .iter()
                .filter(|port| port.target == binding.target && port.protocol == binding.protocol)
                .all(|port| port.published.is_none())
        })
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use crate::{
        config::RuntimeKind,
        model::{ClusterGatewayConfig, ClusterSettings, HeartbeatResponse},
        runtime::RuntimeSystemInfo,
    };

    use super::*;

    #[derive(Clone, Default)]
    struct FakeRuntime {
        created: Arc<Mutex<Vec<String>>>,
        removed: Arc<Mutex<Vec<String>>>,
        started: Arc<Mutex<Vec<String>>>,
        log_output: Arc<Mutex<Vec<u8>>>,
        fail_create: bool,
        start_port_conflicts: Arc<AtomicUsize>,
        resolve_count: Arc<AtomicUsize>,
        resolved_image_id: Option<String>,
        fail_resolve: bool,
    }

    impl ContainerRuntime for FakeRuntime {
        fn kind(&self) -> RuntimeKind {
            RuntimeKind::Docker
        }

        fn socket(&self) -> &str {
            "fake"
        }

        async fn ping(&self) -> Result<()> {
            Ok(())
        }

        async fn system_info(&self) -> Result<RuntimeSystemInfo> {
            Ok(RuntimeSystemInfo {
                cpu_millis: 1,
                memory_bytes: 1,
            })
        }

        async fn list_managed(
            &self,
            _cluster_id: &str,
        ) -> Result<HashMap<String, ManagedContainer>> {
            Ok(HashMap::new())
        }

        async fn resolve_image(
            &self,
            _image: &str,
            progress: &RuntimeImageProgress,
        ) -> Result<String> {
            progress.report(crate::model::ImageResolutionStatus::Checking);
            progress.report(crate::model::ImageResolutionStatus::Pulling);
            self.resolve_count.fetch_add(1, Ordering::Relaxed);
            if self.fail_resolve {
                bail!("image pull denied");
            }
            progress.report(crate::model::ImageResolutionStatus::Comparing);
            Ok(self
                .resolved_image_id
                .clone()
                .unwrap_or_else(|| "sha256:resolved".into()))
        }

        async fn create_task(
            &self,
            assignment: &crate::model::TaskAssignment,
            progress: &RuntimeTaskProgress,
        ) -> Result<()> {
            progress.report(TaskReconcilePhase::Pull);
            if self.fail_create {
                bail!("image pull denied");
            }
            progress.report(TaskReconcilePhase::Create);
            progress.report(TaskReconcilePhase::Start);
            self.created.lock().unwrap().push(assignment.id.clone());
            Ok(())
        }

        async fn start_task(
            &self,
            container: &ManagedContainer,
            progress: &RuntimeTaskProgress,
        ) -> Result<()> {
            progress.report(TaskReconcilePhase::Start);
            if self
                .start_port_conflicts
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                return Err(bollard::errors::Error::DockerResponseServerError {
                    status_code: 500,
                    message: "port is already allocated".into(),
                }
                .into());
            }
            self.started.lock().unwrap().push(container.task_id.clone());
            Ok(())
        }

        async fn remove_task(
            &self,
            container: &ManagedContainer,
            progress: &RuntimeTaskProgress,
        ) -> Result<()> {
            progress.report(TaskReconcilePhase::Stop);
            self.removed.lock().unwrap().push(container.task_id.clone());
            progress.report(TaskReconcilePhase::Remove);
            Ok(())
        }

        async fn stream_task_logs(
            &self,
            container: &ManagedContainer,
            tail: u32,
            _follow: bool,
            output: mpsc::Sender<RuntimeLogChunk>,
        ) -> Result<()> {
            let configured = self.log_output.lock().unwrap().clone();
            let payload = if configured.is_empty() {
                bytes::Bytes::from(format!("{}:{tail}", container.task_id))
            } else {
                bytes::Bytes::from(configured)
            };
            let _ = output
                .send(RuntimeLogChunk {
                    channel: RuntimeLogChannel::Stdout,
                    payload,
                })
                .await;
            Ok(())
        }
    }

    fn managed(task_id: &str) -> ManagedContainer {
        ManagedContainer {
            id: format!("container-{task_id}"),
            image_id: Some("sha256:current".into()),
            task_id: task_id.to_owned(),
            revision: Some(1),
            running: true,
            observed: ObservedTaskState::Healthy,
            stop_grace_seconds: 10,
            cluster_id: Some("cluster-test".into()),
            stack: Some("demo".into()),
            service: Some("web".into()),
            slot: Some(0),
            spec_hash: Some("hash".into()),
            ports: Vec::new(),
            config_digests: Vec::new(),
            config_cache_paths: Vec::new(),
        }
    }

    fn image_test_response(task_ids: &[&str]) -> HeartbeatResponse {
        HeartbeatResponse {
            generation: 2,
            cluster: test_cluster(),
            assignments: Vec::new(),
            image_assignments: vec![crate::model::ImageResolutionAssignment {
                deployment_generation: 2,
                image: "nginx:latest".into(),
                services: task_ids
                    .iter()
                    .enumerate()
                    .map(
                        |(index, task_id)| crate::model::ImageResolutionServiceAssignment {
                            service_id: format!("demo.service-{index}"),
                            task_ids: vec![(*task_id).to_owned()],
                        },
                    )
                    .collect(),
            }],
            gateway_enabled: false,
            labels: Default::default(),
            remove_tasks: Vec::new(),
            gateway_config: None,
            registry_credentials: Default::default(),
            registry_credentials_hash: String::new(),
        }
    }

    #[tokio::test]
    async fn resolves_each_deployment_image_once_and_never_removes_containers() {
        let runtime = FakeRuntime {
            resolved_image_id: Some("sha256:current".into()),
            ..Default::default()
        };
        let existing = HashMap::from([
            ("task-a".into(), managed("task-a")),
            ("task-b".into(), managed("task-b")),
        ]);
        let response = image_test_response(&["task-a", "task-b"]);
        let current = Mutex::new(BTreeMap::new());
        let (events, _receiver) = tokio::sync::mpsc::unbounded_channel();
        let progress = ImageProgressPublisher {
            progress: Arc::new(Mutex::new(BTreeMap::new())),
            events: events.clone(),
        };

        let reports = reconcile_images(&runtime, &existing, &response, &current, &progress).await;
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].services.len(), 2);
        assert!(reports[0].services.iter().all(|service| !service.changed));
        publish_image_results(&response, reports, &current, &events);
        let repeated = reconcile_images(&runtime, &existing, &response, &current, &progress).await;

        assert!(repeated.is_empty());
        assert_eq!(runtime.resolve_count.load(Ordering::Relaxed), 1);
        assert!(runtime.removed.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn image_pull_failure_reports_error_without_removing_the_old_container() {
        let runtime = FakeRuntime {
            fail_resolve: true,
            ..Default::default()
        };
        let existing = HashMap::from([("task-a".into(), managed("task-a"))]);
        let response = image_test_response(&["task-a"]);
        let current = Mutex::new(BTreeMap::new());
        let (events, _receiver) = tokio::sync::mpsc::unbounded_channel();
        let progress = ImageProgressPublisher {
            progress: Arc::new(Mutex::new(BTreeMap::new())),
            events,
        };

        let reports = reconcile_images(&runtime, &existing, &response, &current, &progress).await;

        assert_eq!(reports.len(), 1);
        assert!(reports[0].error.as_deref().unwrap().contains("pull denied"));
        assert!(runtime.removed.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn data_session_frames_preserve_binary_log_bytes() {
        let payload = vec![0, 255, b'\n', 128, 0];
        let runtime = Arc::new(FakeRuntime {
            log_output: Arc::new(Mutex::new(payload.clone())),
            ..FakeRuntime::default()
        });
        let (sender, mut receiver) = mpsc::channel(4);

        stream_agent_data(
            runtime,
            managed("task-a"),
            AgentDataStream {
                stream_id: 7,
                task_id: "task-a".into(),
                operation: AgentDataStreamOperation::Logs {
                    tail: 10,
                    follow: false,
                },
            },
            sender,
        )
        .await
        .unwrap();

        let data = DataFrame::decode(&receiver.recv().await.unwrap()).unwrap();
        assert_eq!(data.kind, crate::data_plane::DataFrameKind::Data);
        assert_eq!(data.stream_id, 7);
        assert_eq!(data.sequence, 0);
        assert_eq!(data.payload.as_ref(), payload);
        let end = DataFrame::decode(&receiver.recv().await.unwrap()).unwrap();
        assert_eq!(end.kind, crate::data_plane::DataFrameKind::End);
        assert_eq!(end.sequence, 1);
    }

    #[tokio::test]
    async fn data_session_splits_large_log_chunks_without_data_loss() {
        let payload = (0..(MAX_DATA_PAYLOAD_BYTES * 3 + 17))
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let runtime = Arc::new(FakeRuntime {
            log_output: Arc::new(Mutex::new(payload.clone())),
            ..FakeRuntime::default()
        });
        let (sender, mut receiver) = mpsc::channel(8);

        stream_agent_data(
            runtime,
            managed("task-a"),
            AgentDataStream {
                stream_id: 7,
                task_id: "task-a".into(),
                operation: AgentDataStreamOperation::Logs {
                    tail: 10,
                    follow: true,
                },
            },
            sender,
        )
        .await
        .unwrap();

        let mut reconstructed = Vec::new();
        for sequence in 0..4 {
            let data = DataFrame::decode(&receiver.recv().await.unwrap()).unwrap();
            assert_eq!(data.kind, crate::data_plane::DataFrameKind::Data);
            assert_eq!(data.sequence, sequence);
            assert!(data.payload.len() <= MAX_DATA_PAYLOAD_BYTES);
            reconstructed.extend_from_slice(&data.payload);
        }
        let end = DataFrame::decode(&receiver.recv().await.unwrap()).unwrap();
        assert_eq!(end.kind, crate::data_plane::DataFrameKind::End);
        assert_eq!(end.sequence, 4);
        assert_eq!(reconstructed, payload);
    }

    fn test_cluster() -> ClusterSettings {
        ClusterSettings {
            schema_version: crate::model::CLUSTER_SCHEMA_VERSION,
            cluster_id: "cluster-test".into(),
            controller_id: "controller-a".into(),
            controller_port: crate::config::DEFAULT_CONTROLLER_PORT,
            gateway: ClusterGatewayConfig::default(),
        }
    }

    #[tokio::test]
    async fn leaves_unknown_containers_untouched_until_explicitly_removed() {
        let runtime = FakeRuntime::default();
        let existing = HashMap::from([("old-task".into(), managed("old-task"))]);
        let mut response = HeartbeatResponse {
            generation: 1,
            cluster: test_cluster(),
            assignments: Vec::new(),
            image_assignments: Vec::new(),
            gateway_enabled: false,
            labels: Default::default(),
            remove_tasks: Vec::new(),
            gateway_config: None,
            registry_credentials: Default::default(),
            registry_credentials_hash: String::new(),
        };

        let reports = reconcile_containers(&runtime, &existing, &response).await;
        assert!(reports.is_empty());
        assert!(runtime.removed.lock().unwrap().is_empty());

        response
            .remove_tasks
            .push(crate::model::TaskRemovalAssignment {
                id: "old-task".into(),
                deployment_generation: 1,
            });
        let reports = reconcile_containers(&runtime, &existing, &response).await;
        assert_eq!(&*runtime.removed.lock().unwrap(), &["old-task"]);
        assert_eq!(reports[0].applied_generation, Some(1));
        assert_eq!(reports[0].phase, TaskReconcilePhase::Remove);
    }

    #[tokio::test]
    async fn config_preparation_failure_preserves_the_existing_container() {
        let runtime = FakeRuntime::default();
        let directory = tempfile::tempdir().unwrap();
        let cache = ConfigCache::new(directory.path().join("configs"));
        let old_cache_path = directory.path().join("configs/mounts/old-config");
        let mut old_container = managed("task-1");
        old_container.config_cache_paths = vec![old_cache_path.clone()];
        let existing = HashMap::from([("task-1".into(), old_container)]);
        let response = HeartbeatResponse {
            generation: 2,
            cluster: test_cluster(),
            assignments: vec![crate::model::TaskAssignment {
                id: "task-1".into(),
                cluster_id: "cluster-test".into(),
                stack: "demo".into(),
                service: "web".into(),
                service_id: "demo.web".into(),
                revision: 2,
                slot: 0,
                desired: crate::model::DesiredTaskState::Running,
                spec: crate::model::ServiceSpec {
                    image: "nginx:alpine".into(),
                    pull_policy: Default::default(),
                    command: Vec::new(),
                    entrypoint: Vec::new(),
                    environment: Vec::new(),
                    expose: Vec::new(),
                    ports: Vec::new(),
                    volumes: Vec::new(),
                    configs: vec![crate::model::ServiceConfigMount {
                        source: "app-config".into(),
                        target: "/etc/app/config.yaml".into(),
                        uid: None,
                        gid: None,
                        mode: 0o444,
                        digest: "a".repeat(64),
                    }],
                    container_labels: Default::default(),
                    service_labels: Default::default(),
                    healthcheck: None,
                    replicas: 1,
                    constraints: Vec::new(),
                    max_surge: 1,
                    stop_grace_period_seconds: 10,
                },
                ports: Vec::new(),
                generation: 2,
                deployment_generation: 2,
                spec_hash: "new-hash".into(),
                image_resolved: false,
            }],
            image_assignments: Vec::new(),
            gateway_enabled: false,
            labels: Default::default(),
            remove_tasks: Vec::new(),
            gateway_config: None,
            registry_credentials: Default::default(),
            registry_credentials_hash: String::new(),
        };
        let config_errors = HashMap::from([("task-1".into(), "download failed".into())]);

        let reports = reconcile_containers_with_progress(
            &runtime,
            &existing,
            &response,
            None,
            &config_errors,
        )
        .await;

        assert!(runtime.removed.lock().unwrap().is_empty());
        assert!(runtime.created.lock().unwrap().is_empty());
        assert!(runtime.started.lock().unwrap().is_empty());
        assert_eq!(reports[0].phase, TaskReconcilePhase::Config);
        assert_eq!(reports[0].error.as_deref(), Some("download failed"));
        let referenced = referenced_config_cache_paths(&cache, &existing, &response);
        assert!(referenced.contains(&old_cache_path));
        assert!(referenced.contains(&cache.host_path(&response.assignments[0].spec.configs[0])));
    }

    #[tokio::test]
    async fn starts_a_matching_stopped_recovered_container_in_place() {
        let runtime = FakeRuntime::default();
        let mut container = managed("task-1");
        container.running = false;
        container.observed = ObservedTaskState::Failed;
        container.ports = vec![crate::model::PortBinding {
            target: 80,
            published: Some(40_000),
            protocol: "tcp".into(),
        }];
        let existing = HashMap::from([("task-1".into(), container)]);
        let response = HeartbeatResponse {
            generation: 1,
            cluster: test_cluster(),
            assignments: vec![crate::model::TaskAssignment {
                id: "task-1".into(),
                cluster_id: "cluster-test".into(),
                stack: "demo".into(),
                service: "web".into(),
                service_id: "demo.web".into(),
                revision: 1,
                slot: 0,
                desired: crate::model::DesiredTaskState::Running,
                spec: crate::model::ServiceSpec {
                    image: "nginx:alpine".into(),
                    pull_policy: Default::default(),
                    command: Vec::new(),
                    entrypoint: Vec::new(),
                    environment: Vec::new(),
                    expose: Vec::new(),
                    ports: vec![crate::model::ServicePort {
                        target: 80,
                        published: None,
                        protocol: "tcp".into(),
                    }],
                    volumes: Vec::new(),
                    configs: Vec::new(),
                    container_labels: Default::default(),
                    service_labels: Default::default(),
                    healthcheck: None,
                    replicas: 1,
                    constraints: Vec::new(),
                    max_surge: 1,
                    stop_grace_period_seconds: 10,
                },
                ports: vec![crate::model::PortBinding {
                    target: 80,
                    published: Some(40_000),
                    protocol: "tcp".into(),
                }],
                generation: 1,
                deployment_generation: 1,
                spec_hash: "hash".into(),
                image_resolved: false,
            }],
            image_assignments: Vec::new(),
            gateway_enabled: false,
            labels: Default::default(),
            remove_tasks: Vec::new(),
            gateway_config: None,
            registry_credentials: Default::default(),
            registry_credentials_hash: String::new(),
        };

        let reports = reconcile_containers(&runtime, &existing, &response).await;
        assert_eq!(&*runtime.started.lock().unwrap(), &["task-1"]);
        assert!(runtime.removed.lock().unwrap().is_empty());
        assert_eq!(reports[0].phase, TaskReconcilePhase::Start);
        assert_eq!(reports[0].applied_generation, Some(1));

        let retry_runtime = FakeRuntime {
            start_port_conflicts: Arc::new(AtomicUsize::new(1)),
            ..FakeRuntime::default()
        };
        let reports = reconcile_containers(&retry_runtime, &existing, &response).await;
        assert_eq!(&*retry_runtime.removed.lock().unwrap(), &["task-1"]);
        assert_eq!(&*retry_runtime.created.lock().unwrap(), &["task-1"]);
        assert_eq!(reports[0].error, None);
    }

    #[tokio::test]
    async fn keeps_a_running_task_when_only_the_service_scale_hash_changes() {
        let runtime = FakeRuntime::default();
        let existing = HashMap::from([("task-1".into(), managed("task-1"))]);
        let response = HeartbeatResponse {
            generation: 2,
            cluster: test_cluster(),
            assignments: vec![crate::model::TaskAssignment {
                id: "task-1".into(),
                cluster_id: "cluster-test".into(),
                stack: "demo".into(),
                service: "web".into(),
                service_id: "demo.web".into(),
                revision: 1,
                slot: 0,
                desired: crate::model::DesiredTaskState::Running,
                spec: crate::model::ServiceSpec {
                    image: "nginx:alpine".into(),
                    pull_policy: Default::default(),
                    command: Vec::new(),
                    entrypoint: Vec::new(),
                    environment: Vec::new(),
                    expose: Vec::new(),
                    ports: Vec::new(),
                    volumes: Vec::new(),
                    configs: Vec::new(),
                    container_labels: Default::default(),
                    service_labels: Default::default(),
                    healthcheck: None,
                    replicas: 3,
                    constraints: Vec::new(),
                    max_surge: 1,
                    stop_grace_period_seconds: 10,
                },
                ports: Vec::new(),
                generation: 2,
                deployment_generation: 2,
                spec_hash: "new-scale-hash".into(),
                image_resolved: false,
            }],
            image_assignments: Vec::new(),
            gateway_enabled: false,
            labels: Default::default(),
            remove_tasks: Vec::new(),
            gateway_config: None,
            registry_credentials: Default::default(),
            registry_credentials_hash: String::new(),
        };

        let reports = reconcile_containers(&runtime, &existing, &response).await;
        assert!(runtime.removed.lock().unwrap().is_empty());
        assert_eq!(reports[0].phase, TaskReconcilePhase::Verify);
        assert_eq!(reports[0].applied_generation, Some(2));
    }

    #[tokio::test]
    async fn reports_create_failures_with_the_assignment_generation() {
        let runtime = FakeRuntime {
            fail_create: true,
            ..Default::default()
        };
        let response = HeartbeatResponse {
            generation: 7,
            cluster: test_cluster(),
            assignments: vec![crate::model::TaskAssignment {
                id: "task-failed".into(),
                cluster_id: "cluster-test".into(),
                stack: "demo".into(),
                service: "web".into(),
                service_id: "demo.web".into(),
                revision: 1,
                slot: 0,
                desired: crate::model::DesiredTaskState::Running,
                spec: crate::model::ServiceSpec {
                    image: "private.example/web:latest".into(),
                    pull_policy: Default::default(),
                    command: Vec::new(),
                    entrypoint: Vec::new(),
                    environment: Vec::new(),
                    expose: Vec::new(),
                    ports: Vec::new(),
                    volumes: Vec::new(),
                    configs: Vec::new(),
                    container_labels: Default::default(),
                    service_labels: Default::default(),
                    healthcheck: None,
                    replicas: 1,
                    constraints: Vec::new(),
                    max_surge: 1,
                    stop_grace_period_seconds: 10,
                },
                ports: Vec::new(),
                generation: 7,
                deployment_generation: 5,
                spec_hash: "hash".into(),
                image_resolved: false,
            }],
            image_assignments: Vec::new(),
            gateway_enabled: false,
            labels: Default::default(),
            remove_tasks: Vec::new(),
            gateway_config: None,
            registry_credentials: Default::default(),
            registry_credentials_hash: String::new(),
        };

        let reports = reconcile_containers(&runtime, &HashMap::new(), &response).await;
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].desired_generation, 5);
        assert_eq!(reports[0].applied_generation, None);
        assert_eq!(reports[0].phase, TaskReconcilePhase::Create);
        assert!(
            reports[0]
                .error
                .as_deref()
                .unwrap()
                .contains("image pull denied")
        );
    }
}
