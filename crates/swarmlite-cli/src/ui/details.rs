use super::*;
use swarmlite_core::model::{ServiceInspectResponse, StackDeploymentResponse, TaskRecord};

pub(super) fn routes() -> Router<Arc<UiState>> {
    Router::new()
        .route("/api/stacks/{name}/deployment", get(deployment))
        .route("/api/jobs/{target}", get(job))
        .route("/api/jobs/{target}/history", get(job_history))
}

#[derive(Deserialize)]
struct GenerationQuery {
    generation: Option<u64>,
}
async fn deployment(
    State(state): State<Arc<UiState>>,
    AxumPath(name): AxumPath<String>,
    Query(query): Query<GenerationQuery>,
) -> ApiResult<StackDeploymentResponse> {
    let mut path = format!("/v1/stacks/{}/deployment", encode(&name));
    if let Some(generation) = query.generation {
        path.push_str(&format!("?generation={generation}"));
    }
    Ok(Json(bounded(state.connection.get_json(&path)).await?))
}
async fn job(
    State(state): State<Arc<UiState>>,
    AxumPath(target): AxumPath<String>,
) -> ApiResult<Value> {
    let response: ServiceInspectResponse = bounded(
        state
            .connection
            .get_json(&format!("/v1/services/{}", encode(&target))),
    )
    .await?;
    if response.service.spec.job.is_none() {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "This workload is not a job.".into(),
        ));
    }
    Ok(Json(
        json!({"id":response.service.id, "policy":response.service.spec.job, "next_at_unix_ms":response.service.job_cursor.map(|cursor|cursor.next_at_unix_ms)}),
    ))
}
fn job_summary(task: TaskRecord) -> Value {
    let execution = task.job.as_ref();
    let runtime = execution.and_then(|job| job.runtime.as_ref());
    // Execution specs can contain environment secrets; the UI only needs runtime metadata.
    json!({"id":task.id, "service_id":task.service_id, "node_id":task.node_id, "desired":task.desired, "observed":task.observed,
        "scheduled_at_unix_ms":execution.map(|job|job.scheduled_at_unix_ms), "start_deadline_unix_ms":execution.map(|job|job.start_deadline_unix_ms),
        "started_at_unix_ms":runtime.and_then(|r|r.started_at_unix_ms), "finished_at_unix_ms":runtime.and_then(|r|r.finished_at_unix_ms), "exit_code":runtime.and_then(|r|r.exit_code),
        "stop_reason":runtime.and_then(|r|r.stop_reason.as_ref()), "error":task.reconcile_error.map(|error|error.message)})
}
async fn job_history(
    State(state): State<Arc<UiState>>,
    AxumPath(target): AxumPath<String>,
) -> ApiResult<Vec<Value>> {
    let tasks: Vec<TaskRecord> = bounded(
        state
            .connection
            .get_json(&format!("/v1/jobs/{}/history", encode(&target))),
    )
    .await?;
    Ok(Json(tasks.into_iter().map(job_summary).collect()))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn execution_summary_omits_private_spec_and_preserves_zero_exit_code() {
        let stack = swarmlite_stack::parse_stack("x-swarmlite-jobs:\n  backup:\n    image: busybox\n    environment: {PASSWORD: private-test-value}\n").unwrap();
        let task: TaskRecord = serde_json::from_value(json!({
            "id":"job-123","service_id":"demo.backup","revision":1,"slot":0,"node_id":"node-1","desired":"stopped","observed":"succeeded","ports":[],"container_id":null,"drain_until_unix_ms":null,
            "job":{"job_id":"demo.backup","scheduled_at_unix_ms":10,"not_before_unix_ms":10,"start_deadline_unix_ms":100,"spec":stack.services["backup"],"runtime":{"job_id":"demo.backup","scheduled_at_unix_ms":10,"start_deadline_unix_ms":100,"started_at_unix_ms":20,"exit_code":0,"timeout_seconds":30,"stop_reason":"completed"}}
        })).unwrap();
        let summary = job_summary(task);
        assert_eq!(summary["exit_code"], 0);
        assert_eq!(summary["started_at_unix_ms"], 20);
        assert_eq!(summary["stop_reason"], "completed");
        assert!(!summary.to_string().contains("private-test-value"));
        assert!(summary.get("job").is_none());
    }
}
