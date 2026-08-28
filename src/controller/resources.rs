use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    time::Duration,
};

use crate::{
    data_plane::DataFrame,
    model::{AgentCommandOperation, AgentDataStream, AgentDataStreamOperation},
};

use super::*;

const MAX_LOG_TASKS: usize = 64;
const DATA_COMMAND_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Clone)]
struct LogTarget {
    task_id: String,
    node_id: String,
    stack: String,
    service: String,
    slot: u32,
}

impl Controller {
    pub(super) async fn list_stacks(&self) -> StackListResponse {
        let inner = self.inner.lock().await;
        let mut stacks = inner
            .state
            .stacks
            .values()
            .filter(|stack| stack_is_active(&inner.state, &stack.name))
            .map(|stack| {
                (
                    stack.name.clone(),
                    StackSummary {
                        name: stack.name.clone(),
                        services: stack
                            .services
                            .iter()
                            .filter(|id| {
                                inner
                                    .state
                                    .services
                                    .get(*id)
                                    .is_some_and(|service| !service.deleted)
                            })
                            .count() as u32,
                        status: stack
                            .deployment
                            .as_ref()
                            .map(|deployment| deployment.status),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        for stack_key in inner.state.gateway_routes.keys() {
            stacks
                .entry(stack_key.clone())
                .or_insert_with(|| StackSummary {
                    name: stack_key.clone(),
                    services: 0,
                    status: None,
                });
        }
        StackListResponse {
            stacks: stacks.into_values().collect(),
        }
    }

    pub(super) async fn list_services(
        &self,
        stack: Option<&str>,
    ) -> Result<ServiceListResponse, ControllerError> {
        let inner = self.inner.lock().await;
        if let Some(stack) = stack {
            require_stack(&inner.state, stack, "ls")?;
        }
        let services = inner
            .state
            .services
            .values()
            .filter(|service| !service.deleted)
            .filter(|service| stack.is_none_or(|stack| service.stack == stack))
            .map(|service| {
                let running_replicas = inner
                    .state
                    .tasks
                    .values()
                    .filter(|task| {
                        task.service_id == service.id
                            && task.revision == service.revision
                            && task.desired == DesiredTaskState::Running
                            && matches!(
                                task.observed,
                                ObservedTaskState::Running | ObservedTaskState::Healthy
                            )
                    })
                    .count() as u32;
                ServiceSummary {
                    id: service.id.clone(),
                    stack: service.stack.clone(),
                    name: service.name.clone(),
                    image: service.spec.image.clone(),
                    replicas: service.spec.replicas,
                    running_replicas,
                }
            })
            .collect();
        Ok(ServiceListResponse { services })
    }

    pub(super) async fn inspect_service(
        &self,
        target: &str,
    ) -> Result<ServiceInspectResponse, ControllerError> {
        let inner = self.inner.lock().await;
        let service = resolve_service(&inner.state, target, "inspect")?;
        let stack = inner
            .state
            .stacks
            .get(&service.stack)
            .cloned()
            .ok_or_else(|| {
                ControllerError::NotFound(format!("stack {:?} not found", service.stack))
            })?;
        let tasks = inner
            .state
            .tasks
            .values()
            .filter(|task| task.service_id == service.id)
            .cloned()
            .collect();
        Ok(ServiceInspectResponse {
            service,
            stack,
            tasks,
        })
    }

    pub(super) async fn stack_tasks(
        &self,
        stack_name: &str,
    ) -> Result<TaskListResponse, ControllerError> {
        let inner = self.inner.lock().await;
        require_stack(&inner.state, stack_name, "Stack task listing")?;
        Ok(TaskListResponse {
            tasks: summarize_tasks(&inner.state, |service| service.stack == stack_name),
        })
    }

    pub(super) async fn service_tasks(
        &self,
        target: &str,
    ) -> Result<TaskListResponse, ControllerError> {
        let inner = self.inner.lock().await;
        let service = resolve_service(&inner.state, target, "Service task listing")?;
        Ok(TaskListResponse {
            tasks: summarize_tasks(&inner.state, |candidate| candidate.id == service.id),
        })
    }

    pub(super) async fn target_tasks(
        &self,
        target: &str,
    ) -> Result<TaskListResponse, ControllerError> {
        let inner = self.inner.lock().await;
        let matches_stack = stack_is_active(&inner.state, target);
        let service = inner
            .state
            .services
            .get(target)
            .filter(|service| !service.deleted);
        match (matches_stack, service) {
            (true, Some(_)) => Err(ControllerError::Conflict(format!(
                "target {target:?} matches both a Stack and a Service"
            ))),
            (true, None) => Ok(TaskListResponse {
                tasks: summarize_tasks(&inner.state, |service| service.stack == target),
            }),
            (false, Some(service)) => Ok(TaskListResponse {
                tasks: summarize_tasks(&inner.state, |candidate| candidate.id == service.id),
            }),
            (false, None) if target_matches_task(&inner.state, target) => {
                Err(ControllerError::Invalid(format!(
                    "ps expects a Stack or Service, but {target:?} identifies a Task; use the Task's parent Service, or use `swarmlite logs {target}` to read this Task"
                )))
            }
            (false, None) => Err(ControllerError::NotFound(format!(
                "Stack or Service {target:?} not found; ps expects STACK or STACK.SERVICE. Run `swarmlite ls` to list available targets"
            ))),
        }
    }

    pub(super) async fn list_tasks(&self) -> TaskListResponse {
        let inner = self.inner.lock().await;
        TaskListResponse {
            tasks: summarize_tasks(&inner.state, |service| !service.deleted),
        }
    }

    pub(super) async fn create_data_session(
        &self,
        operation: DataSessionOperation,
    ) -> Result<DataSessionCreateResponse, ControllerError> {
        match operation {
            DataSessionOperation::Logs {
                target,
                tail,
                follow,
            } => self.create_log_session(&target, tail, follow).await,
        }
    }

    async fn create_log_session(
        &self,
        target: &str,
        tail: u32,
        follow: bool,
    ) -> Result<DataSessionCreateResponse, ControllerError> {
        if tail > 10_000 {
            return Err(ControllerError::Invalid(
                "--tail may not exceed 10000 lines".into(),
            ));
        }
        let (targets, live_nodes) = {
            let inner = self.inner.lock().await;
            let tasks = resolve_log_tasks(&inner.state, target)?;
            let live_nodes = current_live_nodes(&inner, self.config.node_timeout_seconds);
            let targets = tasks
                .into_iter()
                .map(|task| {
                    let service = inner
                        .state
                        .services
                        .get(&task.service_id)
                        .expect("resolved task service must exist");
                    LogTarget {
                        task_id: task.id.clone(),
                        node_id: task.node_id.clone(),
                        stack: service.stack.clone(),
                        service: service.name.clone(),
                        slot: task.slot,
                    }
                })
                .collect::<Vec<_>>();
            (targets, live_nodes)
        };

        if targets.len() > MAX_LOG_TASKS {
            return Err(ControllerError::Invalid(format!(
                "log target selects {} tasks; at most {MAX_LOG_TASKS} are allowed",
                targets.len()
            )));
        }

        let mut targets = targets;
        targets.sort_by(|left, right| {
            (&left.stack, &left.service, left.slot, &left.task_id).cmp(&(
                &right.stack,
                &right.service,
                right.slot,
                &right.task_id,
            ))
        });

        let streams = targets
            .iter()
            .enumerate()
            .map(|(index, target)| DataSessionStream {
                stream_id: u32::try_from(index + 1).expect("log target limit fits in u32"),
                task_id: target.task_id.clone(),
                node_id: target.node_id.clone(),
                stack: target.stack.clone(),
                service: target.service.clone(),
                slot: target.slot,
            })
            .collect::<Vec<_>>();

        let mut by_node = HashMap::<String, Vec<AgentDataStream>>::new();
        for stream in &streams {
            if live_nodes.contains(&stream.node_id) {
                by_node
                    .entry(stream.node_id.clone())
                    .or_default()
                    .push(AgentDataStream {
                        stream_id: stream.stream_id,
                        task_id: stream.task_id.clone(),
                        operation: AgentDataStreamOperation::Logs { tail, follow },
                    });
            }
        }
        let allowed_streams = by_node
            .iter()
            .map(|(node_id, streams)| {
                (
                    node_id.clone(),
                    streams.iter().map(|stream| stream.stream_id).collect(),
                )
            })
            .collect::<HashMap<String, BTreeSet<u32>>>();
        let registered = self.sessions.register(allowed_streams)?;

        for stream in &streams {
            if !live_nodes.contains(&stream.node_id) {
                let _ = sessions::send_frame(
                    &registered.sender,
                    DataFrame::error(stream.stream_id, 0, "node is offline"),
                )
                .await;
                let _ =
                    sessions::send_frame(&registered.sender, DataFrame::end(stream.stream_id, 1))
                        .await;
            }
        }

        for (node_id, node_streams) in by_node {
            let command = self.commands.enqueue(
                &node_id,
                AgentCommandOperation::OpenDataSession {
                    session_id: registered.id.clone(),
                    upload_token: registered
                        .node_tokens
                        .get(&node_id)
                        .expect("registered node has a token")
                        .clone(),
                    streams: node_streams.clone(),
                },
            );
            let sender = registered.sender.clone();
            tokio::spawn(async move {
                let deadline = tokio::time::Instant::now() + DATA_COMMAND_TIMEOUT;
                let error = match command.wait(deadline).await {
                    Ok(result) => result.error,
                    Err(message) => Some(message.into()),
                };
                if let Some(error) = error {
                    for stream in node_streams {
                        let _ = sessions::send_frame(
                            &sender,
                            DataFrame::error(stream.stream_id, 0, error.clone()),
                        )
                        .await;
                        let _ = sessions::send_frame(&sender, DataFrame::end(stream.stream_id, 1))
                            .await;
                    }
                }
            });
        }

        Ok(DataSessionCreateResponse {
            session_id: registered.id,
            attach_token: registered.client_token,
            streams,
        })
    }
}

fn summarize_tasks(
    state: &ClusterState,
    include: impl Fn(&ServiceRecord) -> bool,
) -> Vec<TaskSummary> {
    let mut tasks = state
        .tasks
        .values()
        .filter_map(|task| {
            let service = state.services.get(&task.service_id)?;
            include(service).then(|| TaskSummary {
                id: task.id.clone(),
                stack: service.stack.clone(),
                service: service.name.clone(),
                slot: task.slot,
                node_id: task.node_id.clone(),
                desired: task.desired.clone(),
                observed: task.observed.clone(),
                image: service.spec.image.clone(),
                ports: task.ports.clone(),
                error: task
                    .reconcile_error
                    .as_ref()
                    .map(|error| format!("{:?}: {}", error.phase, error.message)),
            })
        })
        .collect::<Vec<_>>();
    tasks.sort_by(|left, right| {
        (&left.stack, &left.service, left.slot, &left.id).cmp(&(
            &right.stack,
            &right.service,
            right.slot,
            &right.id,
        ))
    });
    tasks
}

fn resolve_log_tasks(
    state: &ClusterState,
    target: &str,
) -> Result<Vec<TaskRecord>, ControllerError> {
    if let Some(task) = state.tasks.get(target) {
        return Ok(vec![task.clone()]);
    }
    let prefix_matches = state
        .tasks
        .values()
        .filter(|task| task.id.starts_with(target))
        .cloned()
        .collect::<Vec<_>>();
    if prefix_matches.len() == 1 {
        return Ok(prefix_matches);
    }
    if prefix_matches.len() > 1 {
        return Err(ControllerError::Conflict(format!(
            "task ID prefix {target:?} is ambiguous"
        )));
    }

    let named_matches = state
        .tasks
        .values()
        .filter(|task| format!("{}.{}", task.service_id, task.slot.saturating_add(1)) == target)
        .cloned()
        .collect::<Vec<_>>();
    if !named_matches.is_empty()
        && state
            .services
            .get(target)
            .is_some_and(|service| !service.deleted)
    {
        return Err(ControllerError::Conflict(format!(
            "log target {target:?} matches both a Task name and a Service; use a Task ID"
        )));
    }
    if named_matches.len() == 1 {
        return Ok(named_matches);
    }
    if named_matches.len() > 1 {
        return Err(ControllerError::Conflict(format!(
            "task name {target:?} is ambiguous during an update; use a Task ID"
        )));
    }

    if stack_is_active(state, target)
        && !state
            .services
            .get(target)
            .is_some_and(|service| !service.deleted)
    {
        return Err(ControllerError::Invalid(format!(
            "logs expects a Service or Task, but {target:?} is a Stack. Run `swarmlite ps {target}` to list its Tasks.{}",
            stack_service_hint(state, target)
        )));
    }

    let service = resolve_service(state, target, "logs")?;
    let tasks = state
        .tasks
        .values()
        .filter(|task| task.service_id == service.id)
        .cloned()
        .collect::<Vec<_>>();
    if tasks.is_empty() {
        Err(ControllerError::NotFound(format!(
            "service {target:?} has no tasks"
        )))
    } else {
        Ok(tasks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_an_exact_task_before_a_service() {
        let mut state = ClusterState::default();
        state.services.insert(
            "demo.web".into(),
            ServiceRecord {
                id: "demo.web".into(),
                stack: "demo".into(),
                name: "web".into(),
                revision: 1,
                spec: crate::model::ServiceSpec {
                    image: "nginx".into(),
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
                    max_surge: 0,
                    stop_grace_period_seconds: 10,
                },
                deleted: false,
            },
        );
        state.tasks.insert(
            "task-123".into(),
            TaskRecord {
                id: "task-123".into(),
                service_id: "demo.web".into(),
                revision: 1,
                slot: 0,
                node_id: "node-a".into(),
                desired: DesiredTaskState::Running,
                observed: ObservedTaskState::Healthy,
                ports: Vec::new(),
                config_digests: Vec::new(),
                container_id: Some("container-a".into()),
                drain_until_unix_ms: None,
                applied_generation: Some(1),
                reconcile_error: None,
            },
        );
        assert_eq!(
            resolve_log_tasks(&state, "task-1").unwrap()[0].id,
            "task-123"
        );
        assert_eq!(
            resolve_log_tasks(&state, "demo.web.1").unwrap()[0].id,
            "task-123"
        );
        assert_eq!(resolve_log_tasks(&state, "demo.web").unwrap().len(), 1);
    }

    #[test]
    fn rejects_an_ambiguous_task_name_during_an_update() {
        let mut state = ClusterState::default();
        state.services.insert(
            "demo.web".into(),
            ServiceRecord {
                id: "demo.web".into(),
                stack: "demo".into(),
                name: "web".into(),
                revision: 2,
                spec: crate::model::ServiceSpec {
                    image: "nginx".into(),
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
                deleted: false,
            },
        );
        for (id, revision, desired) in [
            ("old-task", 1, DesiredTaskState::Draining),
            ("new-task", 2, DesiredTaskState::Running),
        ] {
            state.tasks.insert(
                id.into(),
                TaskRecord {
                    id: id.into(),
                    service_id: "demo.web".into(),
                    revision,
                    slot: 0,
                    node_id: "node-a".into(),
                    desired,
                    observed: ObservedTaskState::Healthy,
                    ports: Vec::new(),
                    config_digests: Vec::new(),
                    container_id: Some(format!("container-{id}")),
                    drain_until_unix_ms: None,
                    applied_generation: Some(1),
                    reconcile_error: None,
                },
            );
        }

        assert!(matches!(
            resolve_log_tasks(&state, "demo.web.1"),
            Err(ControllerError::Conflict(message))
                if message == "task name \"demo.web.1\" is ambiguous during an update; use a Task ID"
        ));
    }
}
