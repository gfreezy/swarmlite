use super::*;
use axum::{extract::DefaultBodyLimit, routing::post};
use clap::{CommandFactory, Parser};
use serde::Serialize;
use std::{process::Stdio, sync::Mutex};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};

const OUTPUT_LIMIT: usize = 256 * 1024;
#[derive(Default)]
pub(super) struct Runs(Mutex<BTreeMap<String, Run>>);
struct Run {
    fingerprint: String,
    command: String,
    started: u64,
    status: String,
    exit_code: Option<i32>,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    truncated: bool,
    cancel: watch::Sender<bool>,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RunRequest {
    id: String,
    command: String,
    #[serde(default)]
    values: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    stdin: String,
    confirmed: bool,
}
fn bad(message: impl Into<String>) -> ApiError {
    ApiError(StatusCode::BAD_REQUEST, message.into())
}
fn leaf(path: &str) -> Result<clap::Command, ApiError> {
    let mut command = crate::Cli::command();
    command.build();
    for part in path.split(' ') {
        command = command
            .find_subcommand(part)
            .cloned()
            .ok_or_else(|| bad("Unknown command."))?;
    }
    if matches!(
        path,
        "ui" | "init" | "join" | "serve" | "upgrade" | "deploy"
    ) || command.has_subcommands()
    {
        return Err(bad(
            "Choose a CLI operation. Host lifecycle and YAML deployment commands are not available in the Web UI.",
        ));
    }
    Ok(command)
}
pub(super) fn is_connection_arg(path: &str, id: &str) -> bool {
    matches!(id, "data_dir" | "color" | "help" | "version")
        || (matches!(id, "controller" | "token") && !matches!(path, "init" | "join"))
}
pub(super) fn schema(path: &str, command: &clap::Command) -> Vec<Value> {
    command.get_arguments().filter(|arg|!is_connection_arg(path,arg.get_id().as_str())).map(|arg|{
        let action=arg.get_action();let boolean=matches!(action,clap::ArgAction::SetTrue|clap::ArgAction::SetFalse);
        let multiple=matches!(action,clap::ArgAction::Append)||arg.get_num_args().is_some_and(|n|n.max_values()>1);
        json!({"id":arg.get_id().as_str(),"label":arg.get_long().map(str::to_owned).unwrap_or_else(||arg.get_id().to_string()),"help":arg.get_help().map(ToString::to_string),"required":arg.is_required_set(),"boolean":boolean,"multiple":multiple,
            "secret":arg.get_id().as_str()=="token","defaults":arg.get_default_values().iter().map(|v|v.to_string_lossy()).collect::<Vec<_>>(),
            "choices":arg.get_value_parser().possible_values().map(|values|values.filter(|v|!v.is_hide_set()).map(|v|v.get_name().to_owned()).collect::<Vec<_>>())})
    }).collect()
}
fn prepare(request: &RunRequest) -> Result<Vec<String>, ApiError> {
    if !request.confirmed {
        return Err(bad("Review and confirm the operation before starting it."));
    }
    if uuid::Uuid::parse_str(&request.id).is_err() {
        return Err(bad("A valid request ID is required."));
    }
    let command = leaf(&request.command)?;
    let mut flags = Vec::new();
    let mut positionals = Vec::new();
    for (id, values) in &request.values {
        let arg = command
            .get_arguments()
            .find(|arg| arg.get_id().as_str() == id)
            .ok_or_else(|| bad(format!("Unknown parameter: {id}")))?;
        if is_connection_arg(&request.command, id) {
            return Err(bad(
                "Connection and global settings are fixed by this UI session.",
            ));
        }
        if values.is_empty() {
            continue;
        }
        if arg.is_positional() {
            positionals.push((arg.get_index().unwrap_or_default(), values.clone()));
            continue;
        }
        let long = arg.get_long().ok_or_else(|| bad("Unsupported option."))?;
        if matches!(
            arg.get_action(),
            clap::ArgAction::SetTrue | clap::ArgAction::SetFalse
        ) {
            if values != &["true"] {
                return Err(bad(format!("{long} expects a checkbox value.")));
            }
            flags.push(format!("--{long}"));
        } else if arg.get_num_args().is_some_and(|n| n.max_values() > 1)
            && !matches!(arg.get_action(), clap::ArgAction::Append)
        {
            flags.push(format!("--{long}"));
            flags.extend(values.clone());
        } else {
            for value in values {
                flags.push(format!("--{long}={value}"));
            }
        }
    }
    let mut args: Vec<_> = request.command.split(' ').map(str::to_owned).collect();
    args.extend(flags);
    positionals.sort_by_key(|(index, _)| *index);
    if !positionals.is_empty() {
        args.push("--".into());
        for (_, values) in positionals {
            args.extend(values);
        }
    }
    let argv = std::iter::once("swarmlite".to_owned()).chain(args.iter().cloned());
    crate::Cli::try_parse_from(argv).map_err(|_|bad("Invalid or missing parameters. Check required fields, value types and conflicting options in the command help."))?;
    if request.command != "registry login" && !request.stdin.is_empty() {
        return Err(bad("Only registry login accepts standard input."));
    }
    Ok(args)
}
pub(super) fn routes() -> Router<Arc<UiState>> {
    Router::new()
        .route("/api/commands", post(start).get(list))
        .route("/api/commands/{id}", get(detail))
        .route("/api/commands/{id}/stop", post(stop))
        .layer(DefaultBodyLimit::max(128 * 1024))
}
fn view(id: &str, run: &Run) -> Value {
    json!({"id":id,"command":run.command,"started":run.started,"status":run.status,"exit_code":run.exit_code,
        "stdout":String::from_utf8_lossy(&run.stdout),"stderr":String::from_utf8_lossy(&run.stderr),"truncated":run.truncated})
}
async fn list(State(state): State<Arc<UiState>>) -> Json<Value> {
    let runs = state.runs.0.lock().unwrap();
    Json(
        json!({"runs":runs.iter().map(|(id,run)|json!({"id":id,"command":run.command,"started":run.started,"status":run.status,"exit_code":run.exit_code,"truncated":run.truncated})).collect::<Vec<_>>()}),
    )
}
async fn detail(
    State(state): State<Arc<UiState>>,
    AxumPath(id): AxumPath<String>,
) -> ApiResult<Value> {
    let runs = state.runs.0.lock().unwrap();
    let run = runs
        .get(&id)
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "Operation was not found.".into()))?;
    Ok(Json(view(&id, run)))
}
async fn start(
    State(state): State<Arc<UiState>>,
    Json(request): Json<RunRequest>,
) -> ApiResult<Value> {
    let args = prepare(&request)?;
    let fingerprint =
        swarmlite_stack::config_digest(serde_json::to_string(&request).unwrap().as_bytes());
    let (cancel, receiver) = watch::channel(false);
    {
        let mut runs = state.runs.0.lock().unwrap();
        if let Some(run) = runs.get(&request.id) {
            return if run.fingerprint == fingerprint {
                Ok(Json(view(&request.id, run)))
            } else {
                Err(ApiError(
                    StatusCode::CONFLICT,
                    "This request ID belongs to a different operation.".into(),
                ))
            };
        }
        if runs.values().filter(|run| run.status == "running").count() >= 4 {
            return Err(ApiError(
                StatusCode::TOO_MANY_REQUESTS,
                "Four CLI operations are already running.".into(),
            ));
        }
        // Keep request IDs for the entire session: a lost response must not cause a second write.
        if runs.len() >= 128 {
            return Err(ApiError(
                StatusCode::TOO_MANY_REQUESTS,
                "Session operation limit reached. Start a new UI session.".into(),
            ));
        }
        runs.insert(
            request.id.clone(),
            Run {
                fingerprint,
                command: request.command.clone(),
                started: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64,
                status: "running".into(),
                exit_code: None,
                stdout: Vec::new(),
                stderr: Vec::new(),
                truncated: false,
                cancel,
            },
        );
    }
    let id = request.id.clone();
    let executor = state.clone();
    tokio::spawn(async move {
        execute(executor, request.id, args, request.stdin, receiver).await;
    });
    let runs = state.runs.0.lock().unwrap();
    Ok(Json(view(&id, &runs[&id])))
}
async fn stop(
    State(state): State<Arc<UiState>>,
    AxumPath(id): AxumPath<String>,
    Json(_): Json<Value>,
) -> ApiResult<Value> {
    let runs = state.runs.0.lock().unwrap();
    let run = runs
        .get(&id)
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "Operation was not found.".into()))?;
    let _ = run.cancel.send(true);
    Ok(Json(view(&id, run)))
}
fn append(state: &UiState, id: &str, bytes: &[u8], stderr: bool) {
    let mut runs = state.runs.0.lock().unwrap();
    if let Some(run) = runs.get_mut(id) {
        let output = if stderr {
            &mut run.stderr
        } else {
            &mut run.stdout
        };
        output.extend_from_slice(bytes);
        if output.len() > OUTPUT_LIMIT {
            output.drain(..output.len() - OUTPUT_LIMIT);
            run.truncated = true;
        }
    }
}
async fn pump(mut stream: impl AsyncRead + Unpin, state: Arc<UiState>, id: String, stderr: bool) {
    let mut bytes = [0u8; 4096];
    while let Ok(n) = stream.read(&mut bytes).await {
        if n == 0 {
            break;
        }
        append(&state, &id, &bytes[..n], stderr);
    }
}
async fn execute(
    state: Arc<UiState>,
    id: String,
    args: Vec<String>,
    input: String,
    mut cancel: watch::Receiver<bool>,
) {
    let mut shutdown = state.shutdown.clone();
    let result:std::io::Result<(Option<i32>,bool)>=async {
        let mut command=tokio::process::Command::new(std::env::current_exe()?);
        command.arg("--data-dir").arg(&state.data_dir).arg("--color=never").args(args)
            .env("NO_COLOR","1").env("SWARMLITE_COLOR","never").stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped()).kill_on_drop(true);
        if let Some(controller)=&state.command_controller {command.env("SWARMLITE_CONTROLLER",controller);}
        if let Some(token)=&state.command_token {command.env("SWARMLITE_TOKEN",token);}
        #[cfg(unix)] command.process_group(0);
        let mut child=command.spawn()?;
        let mut stdin=child.stdin.take().unwrap();
        let stdin_task=tokio::spawn(async move {let _=stdin.write_all(input.as_bytes()).await;let _=stdin.shutdown().await;});
        let mut stdout=tokio::spawn(pump(child.stdout.take().unwrap(),state.clone(),id.clone(),false));
        let mut stderr=tokio::spawn(pump(child.stderr.take().unwrap(),state.clone(),id.clone(),true));
        let mut cancelled=false;
        let status=tokio::select! { result=child.wait()=>result?, _=cancel.changed()=>{cancelled=true;terminate(&mut child).await?}, _=shutdown.changed()=>{cancelled=true;terminate(&mut child).await?} };
        stdin_task.abort();
        let readers=async {let _=(&mut stdout).await;let _=(&mut stderr).await;};
        if tokio::time::timeout(Duration::from_secs(2),readers).await.is_err() {stdout.abort();stderr.abort();}
        Ok((status.code(),cancelled))
    }.await;
    let mut runs = state.runs.0.lock().unwrap();
    let run = runs.get_mut(&id).unwrap();
    match result {
        Ok((code, cancelled)) => {
            run.exit_code = code;
            run.status = if cancelled {
                "stopped"
            } else if code == Some(0) {
                "succeeded"
            } else {
                "failed"
            }
            .into();
        }
        Err(error) => {
            run.status = "failed".into();
            run.stderr.extend_from_slice(error.to_string().as_bytes());
        }
    }
}
async fn terminate(child: &mut tokio::process::Child) -> std::io::Result<std::process::ExitStatus> {
    #[cfg(unix)]
    if let Some(pid) = child.id() {
        // Each runner owns a new process group, including SSH and installer children.
        unsafe {
            libc::kill(-(pid as i32), libc::SIGTERM);
        }
        if let Ok(result) = tokio::time::timeout(Duration::from_secs(3), child.wait()).await {
            return result;
        }
        unsafe {
            libc::kill(-(pid as i32), libc::SIGKILL);
        }
    }
    let _ = child.start_kill();
    child.wait().await
}

pub(super) async fn finish(state: &UiState) {
    for run in state.runs.0.lock().unwrap().values() {
        let _ = run.cancel.send(true);
    }
    let _ = tokio::time::timeout(Duration::from_secs(6), async {
        loop {
            if state
                .runs
                .0
                .lock()
                .unwrap()
                .values()
                .all(|run| run.status != "running")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    fn request(command: &str, values: Value) -> RunRequest {
        serde_json::from_value(
            json!({"id":uuid::Uuid::new_v4(),"command":command,"values":values,"confirmed":true}),
        )
        .unwrap()
    }
    #[test]
    fn runner_uses_literal_arguments_and_rejects_overrides() {
        let req = request(
            "node label set",
            json!({"node_id":["node1"],"key":["zone"],"value":["$(touch /tmp/do-not-create); east"]}),
        );
        let args = prepare(&req).unwrap();
        assert_eq!(args.last().unwrap(), "$(touch /tmp/do-not-create); east");
        assert!(prepare(&request("status", json!({"token":["other"]}))).is_err());
        assert!(prepare(&request("ui", json!({}))).is_err());
        assert!(prepare(&request("service scale", json!({}))).is_err());
        assert!(
            prepare(&request(
                "deploy",
                json!({"file":["test.yaml"],"dry_run":["true"],"replace":["true"]})
            ))
            .is_err()
        );
    }
    #[test]
    fn excluded_operations_cannot_be_invoked_through_the_api() {
        for command in ["deploy", "init", "join", "serve", "upgrade", "ui"] {
            assert!(prepare(&request(command, json!({}))).is_err());
        }
        let mut unconfirmed = request("status", json!({}));
        unconfirmed.confirmed = false;
        assert!(prepare(&unconfirmed).is_err());
    }
    #[tokio::test]
    async fn repeated_submission_returns_the_existing_run_and_rejects_changed_payload() {
        let connection = connection::resolve(
            Path::new("/unused"),
            Some("http://127.0.0.1:9".into()),
            Some("test-token".into()),
        )
        .await
        .unwrap();
        let (_sender, shutdown) = watch::channel(false);
        let state = Arc::new(UiState {
            connection,
            data_dir: "/unused".into(),
            command_controller: None,
            command_token: None,
            runs: Runs::default(),
            authority: "localhost".into(),
            session: "test".into(),
            shutdown,
        });
        let request = request("status", json!({"json":["true"]}));
        let id = request.id.clone();
        let fingerprint =
            swarmlite_stack::config_digest(serde_json::to_string(&request).unwrap().as_bytes());
        let (cancel, _) = watch::channel(false);
        state.runs.0.lock().unwrap().insert(
            id.clone(),
            Run {
                fingerprint,
                command: "status".into(),
                started: 0,
                status: "succeeded".into(),
                exit_code: Some(0),
                stdout: b"existing output".to_vec(),
                stderr: vec![],
                truncated: false,
                cancel,
            },
        );
        let Json(result) = start(State(state.clone()), Json(request))
            .await
            .ok()
            .unwrap();
        assert_eq!(result["stdout"], "existing output");
        assert_eq!(state.runs.0.lock().unwrap().len(), 1);
        let mut altered = self::request("status", json!({}));
        altered.id = id;
        assert!(start(State(state), Json(altered)).await.is_err());
    }
}
