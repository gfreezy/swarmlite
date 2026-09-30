use super::*;
use clap::CommandFactory;
use swarmlite_core::model::{ClusterConfigResponse, ServiceInspectResponse, TaskRecord};

pub(super) fn routes() -> Router<Arc<UiState>> {
    Router::new()
        .route("/api/inspect/{target}", get(inspect))
        .route("/api/tasks", get(all_tasks))
        .route("/api/tasks/{id}", get(task))
        .route("/api/nodes", get(nodes))
        .route("/api/node-stats", get(node_stats))
        .route("/api/config", get(config))
        .route("/api/cli", get(cli_reference))
}

fn task_info(task: &TaskRecord) -> Value {
    serde_json::to_value(task).expect("task serializes")
}
fn inspect_info(response: &ServiceInspectResponse) -> Value {
    let service = &response.service;
    json!({"service":{"id":service.id,"name":service.name,"stack":service.stack,"revision":service.revision,"deleted":service.deleted,"job_cursor":service.job_cursor,"spec":service.spec},
        "stack":{"name":response.stack.name,"applied_at_unix_ms":response.stack.applied_at_unix_ms,"services":response.stack.services,"gateway":response.stack.gateway,"generation":response.stack.deployment.as_ref().map(|d|d.generation)},
        "tasks":response.tasks.iter().map(task_info).collect::<Vec<_>>()})
}
async fn inspect(
    State(state): State<Arc<UiState>>,
    AxumPath(target): AxumPath<String>,
) -> ApiResult<Value> {
    let response: ServiceInspectResponse = bounded(
        state
            .connection
            .get_json(&format!("/v1/services/{}", encode(&target))),
    )
    .await?;
    Ok(Json(inspect_info(&response)))
}
#[derive(Deserialize)]
struct TargetQuery {
    target: Option<String>,
}
async fn all_tasks(
    State(state): State<Arc<UiState>>,
    Query(query): Query<TargetQuery>,
) -> ApiResult<TaskListResponse> {
    let path = query
        .target
        .filter(|target| !target.is_empty())
        .map_or("/v1/tasks".into(), |target| {
            format!("/v1/tasks?target={}", encode(&target))
        });
    Ok(Json(bounded(state.connection.get_json(&path)).await?))
}
async fn task(
    State(state): State<Arc<UiState>>,
    AxumPath(id): AxumPath<String>,
) -> ApiResult<Value> {
    let status: StatusResponse = bounded(state.connection.get_json("/v1/status")).await?;
    if let Some(task) = status.state.tasks.get(&id) {
        return Ok(Json(json!({"task":task_info(task),"recovery":false})));
    }
    if let Some(task) = status.state.unclaimed_tasks.get(&id) {
        return Ok(Json(json!({"task":task,"recovery":true})));
    }
    Err(ApiError(
        StatusCode::NOT_FOUND,
        "Task is no longer retained. Refresh the task list.".into(),
    ))
}
async fn nodes(State(state): State<Arc<UiState>>) -> ApiResult<Value> {
    let status: StatusResponse = bounded(state.connection.get_json("/v1/status")).await?;
    let ids: BTreeSet<_> = status
        .state
        .members
        .keys()
        .chain(status.state.nodes.keys())
        .collect();
    let nodes:Vec<_>=ids.into_iter().map(|id|{
        let member=status.state.members.get(id);let report=status.state.nodes.get(id);
        json!({"id":id,"member":member,"report":report,
            "tasks":status.state.tasks.values().filter(|task|task.node_id==*id).map(task_info).collect::<Vec<_>>(),
            "unclaimed_tasks":status.state.unclaimed_tasks.values().filter(|task|task.node_id==*id).collect::<Vec<_>>()})
    }).collect();
    Ok(Json(
        json!({"controller_id":status.controller_id,"generation":status.generation,"recovery":status.recovery,"nodes":nodes}),
    ))
}
fn safe_config_value(key: &str, value: Value) -> Value {
    if matches!(key, "proxy.http" | "proxy.https" | "proxy.all")
        && let Some(text) = value.as_str()
    {
        return json!(
            url::Url::parse(text)
                .map(|mut url| {
                    let _ = url.set_username("");
                    let _ = url.set_password(None);
                    url.set_query(None);
                    url.set_fragment(None);
                    url.to_string()
                })
                .unwrap_or_else(|_| "[hidden]".into())
        );
    }
    value
}
async fn config(State(state): State<Arc<UiState>>) -> ApiResult<Value> {
    let response: ClusterConfigResponse = bounded(state.connection.get_json("/v1/config")).await?;
    let fields:Vec<_>=crate::ConfigKey::ALL.iter().map(|key|{
        let meta=key.metadata(); let value=key.current(&response.config).into_json();
        json!({"key":meta.key,"value":safe_config_value(meta.key,value),"type":meta.value_type,"values":meta.values,"constraints":meta.constraints,"default":meta.default_semantics,"description":meta.description,"apply_mode":meta.apply_mode})
    }).collect();
    Ok(Json(
        json!({"generation":response.generation,"fields":fields}),
    ))
}
fn command_destination(path: &str) -> Option<&'static str> {
    match path {
        "status" | "ui" => Some("Overview"),
        "ls" | "inspect" => Some("Workloads"),
        "ps" => Some("Tasks"),
        "logs" => Some("Logs"),
        "job history" => Some("Jobs"),
        "deployment status" | "deployment history" | "deployment attach" => Some("Deployments"),
        "gateway status" => Some("Routes"),
        "node label get" | "node stats" => Some("Nodes"),
        "config get" | "config explain" => Some("Configuration"),
        _ => None,
    }
}
fn commands(command: &clap::Command, prefix: &str, output: &mut Vec<Value>) {
    let children: Vec<_> = command
        .get_subcommands()
        .filter(|c| c.get_name() != "help")
        .collect();
    if children.is_empty() {
        let destination = command_destination(prefix);
        let mut command = command.clone().mut_args(|arg| arg.hide_env_values(true));
        let help = command.render_long_help().to_string();
        output.push(json!({"command":format!("swarmlite {prefix}"),"description":command.get_about().map(ToString::to_string),"page":destination,"help":help,"arguments":super::commands::schema(prefix,&command),"executable":!matches!(prefix,"ui"|"init"|"join"|"serve"|"upgrade"|"deploy"),"local":matches!(prefix,"connection-info"|"join-token"),
            "scope":if destination.is_some(){"Web view + operation"}else if matches!(prefix,"init"|"serve"|"join"|"upgrade"){"Run on the target host"}else if matches!(prefix,"join-token"|"connection-info"){"Shows credentials"}else{"Web operation"}}));
    } else {
        for child in children {
            commands(
                child,
                format!("{prefix} {}", child.get_name()).trim(),
                output,
            );
        }
    }
}
async fn cli_reference() -> Json<Value> {
    let mut command = crate::Cli::command();
    command.build();
    let mut rows = Vec::new();
    commands(&command, "", &mut rows);
    Json(json!({"version":env!("CARGO_PKG_VERSION"),"commands":rows}))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn inspect_retains_complete_configuration() {
        let parsed=swarmlite_stack::parse_stack("services:\n  web:\n    image: nginx\n    environment: {TOKEN: secret-value}\n    command: [echo, secret-value]\n    labels: {token: secret-value}\n    volumes: [data:/data]\n    deploy:\n      replicas: 3\n").unwrap();
        let response: ServiceInspectResponse = serde_json::from_value(json!({
            "service": {"id":"demo.web", "stack":"demo", "name":"web", "revision":1, "spec":parsed.services["web"], "deleted":false},
            "stack": {"name":"demo", "applied_at_unix_ms":0, "services":["demo.web"]}, "tasks":[]
        })).unwrap();
        let info = inspect_info(&response);
        let spec = &info["service"]["spec"];
        assert_eq!(spec["replicas"], 3);
        assert_eq!(spec["volumes"][0], "data:/data");
        assert_eq!(spec["environment"][0], "TOKEN=secret-value");
        assert_eq!(spec["command"], json!(["echo", "secret-value"]));
        assert_eq!(spec["container_labels"]["token"], "secret-value");
        assert_eq!(
            safe_config_value(
                "proxy.http",
                json!("http://user:secret-value@proxy.test:8080/?token=secret-value")
            ),
            "http://proxy.test:8080/"
        );
        assert_eq!(
            safe_config_value("gateway.metrics.enabled", json!(false)),
            false
        );
    }
    #[test]
    fn cli_catalog_covers_leaf_commands_and_maps_read_views() {
        let mut command = crate::Cli::command();
        command.build();
        let mut rows = Vec::new();
        commands(&command, "", &mut rows);
        for name in [
            "init",
            "join",
            "serve",
            "upgrade",
            "registry login",
            "service scale",
            "job cancel",
            "deployment rollback",
            "config unset",
            "node label set",
        ] {
            assert!(
                rows.iter()
                    .any(|row| row["command"] == format!("swarmlite {name}")
                        && row["page"].is_null())
            );
        }
        for name in [
            "ps",
            "inspect",
            "config explain",
            "gateway status",
            "node label get",
        ] {
            assert!(rows.iter().any(
                |row| row["command"] == format!("swarmlite {name}") && row["page"].is_string()
            ));
        }
    }
}

#[derive(Deserialize)]
struct MetricsQuery {
    node: Option<String>,
    #[serde(default)]
    history: bool,
    seconds: Option<u64>,
    from: Option<i64>,
    to: Option<i64>,
}
async fn node_stats(
    State(state): State<Arc<UiState>>,
    Query(query): Query<MetricsQuery>,
) -> ApiResult<swarmlite_core::metrics::NodeStatsResponse> {
    let path = {
        let mut params = url::form_urlencoded::Serializer::new(String::new());
        if let Some(node) = query.node {
            params.append_pair("node", &node);
        }
        params.append_pair("history", if query.history { "true" } else { "false" });
        params.append_pair("seconds", &query.seconds.unwrap_or(900).to_string());
        if let Some(from) = query.from {
            params.append_pair("from", &from.to_string());
        }
        if let Some(to) = query.to {
            params.append_pair("to", &to.to_string());
        }
        format!("/v1/nodes/stats?{}", params.finish())
    };
    Ok(Json(bounded(state.connection.get_json(&path)).await?))
}
