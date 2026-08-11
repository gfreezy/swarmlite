use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
};

use anyhow::Result;
use clap::Args;
use futures_util::{SinkExt, StreamExt};
use swarmlite::{
    client::ControllerClient,
    data_plane::{DATA_STREAM_WRITE_TIMEOUT, DataChannel, DataFrame, DataFrameKind},
    model::{
        DataSessionCreateResponse, DataSessionOperation, DataSessionStream, ServiceInspectResponse,
        ServiceListResponse, ServiceScaleRequest, StackDeploymentResponse, TaskListResponse,
    },
    node,
};
use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter};
use tokio_tungstenite::tungstenite::Message;

use super::{ConnectionArgs, deploy, finish_deployment};

const LOG_OUTPUT_BUFFER_BYTES: usize = 256 * 1024;
const LOG_OUTPUT_FLUSH_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);

#[derive(Debug, Args)]
pub(super) struct DeployArgs {
    #[arg(short = 'c', long = "compose-file", visible_alias = "file")]
    file: PathBuf,
    stack: String,
    /// Return after the Controller accepts the desired state.
    #[arg(short = 'd', long)]
    detach: bool,
    #[command(flatten)]
    connection: ConnectionArgs,
}

#[derive(Debug, Args)]
pub(super) struct ListArgs {
    /// Limit the service list to one Stack.
    #[arg(value_name = "STACK")]
    stack: Option<String>,
    #[command(flatten)]
    connection: ConnectionArgs,
}

#[derive(Debug, Args)]
pub(super) struct PsArgs {
    /// Stack or Service whose tasks should be listed.
    #[arg(value_name = "STACK|STACK.SERVICE")]
    target: String,
    #[arg(short = 'q', long)]
    quiet: bool,
    #[arg(long)]
    no_trunc: bool,
    #[command(flatten)]
    connection: ConnectionArgs,
}

#[derive(Debug, Args)]
pub(super) struct InspectArgs {
    #[arg(value_name = "STACK.SERVICE")]
    service: String,
    #[command(flatten)]
    connection: ConnectionArgs,
}

#[derive(Debug, Args)]
pub(super) struct LogsArgs {
    #[arg(value_name = "STACK.SERVICE|TASK")]
    target: String,
    #[arg(short = 'n', long, default_value_t = 100, value_parser = clap::value_parser!(u32).range(0..=10_000))]
    tail: u32,
    /// Continue streaming new output.
    #[arg(short = 'f', long)]
    follow: bool,
    /// Require a single Task and write its bytes without prefixes.
    #[arg(long)]
    raw: bool,
    #[command(flatten)]
    connection: ConnectionArgs,
}

#[derive(Debug, Args)]
pub(super) struct ScaleArgs {
    #[arg(required = true, value_name = "STACK.SERVICE=REPLICAS")]
    services: Vec<String>,
    /// Return after the Controller accepts the desired state.
    #[arg(short = 'd', long)]
    detach: bool,
    #[command(flatten)]
    connection: ConnectionArgs,
}

#[derive(Debug, Args)]
pub(super) struct RestartArgs {
    #[arg(value_name = "STACK.SERVICE")]
    service: String,
    /// Return after the Controller accepts the desired state.
    #[arg(short = 'd', long)]
    detach: bool,
    #[command(flatten)]
    connection: ConnectionArgs,
}

#[derive(Debug, Args)]
pub(super) struct RemoveArgs {
    #[arg(required = true, value_name = "STACK")]
    stacks: Vec<String>,
    /// Return after the Controller accepts the desired state.
    #[arg(short = 'd', long)]
    detach: bool,
    #[command(flatten)]
    connection: ConnectionArgs,
}

pub(super) async fn run_deploy(data_dir: &Path, args: DeployArgs) -> Result<()> {
    let (controller, token) =
        node::resolve_connection(data_dir, args.connection.controller, args.connection.token)
            .await?;
    deploy(controller, args.stack, args.file, token, args.detach).await
}

pub(super) async fn run_list(data_dir: &Path, args: ListArgs) -> Result<()> {
    let client = resolve_client(data_dir, args.connection).await?;
    let path = args.stack.map_or_else(
        || "/v1/services".to_owned(),
        |stack| format!("/v1/services?stack={}", encode(&stack)),
    );
    let response: ServiceListResponse = client.get_json(&path).await?;
    print_service_table(&response);
    Ok(())
}

pub(super) async fn run_ps(data_dir: &Path, args: PsArgs) -> Result<()> {
    let client = resolve_client(data_dir, args.connection).await?;
    let response: TaskListResponse = client
        .get_json(&format!("/v1/tasks?target={}", encode(&args.target)))
        .await?;
    print_task_table(&response, args.quiet, args.no_trunc);
    Ok(())
}

pub(super) async fn run_inspect(data_dir: &Path, args: InspectArgs) -> Result<()> {
    let client = resolve_client(data_dir, args.connection).await?;
    let response: ServiceInspectResponse = client
        .get_json(&format!("/v1/services/{}", encode(&args.service)))
        .await?;
    println!("{}", serde_json::to_string_pretty(&response)?);
    Ok(())
}

pub(super) async fn run_logs(data_dir: &Path, args: LogsArgs) -> Result<()> {
    let client = resolve_client(data_dir, args.connection).await?;
    let response: DataSessionCreateResponse = client
        .send_json(
            reqwest::Method::POST,
            "/v1/data-sessions",
            Some(&DataSessionOperation::Logs {
                target: args.target,
                tail: args.tail,
                follow: args.follow,
            }),
        )
        .await?;
    let path = format!("/v1/data-sessions/{}/client", encode(&response.session_id));
    let mut socket = client
        .connect_data_websocket(&path, &response.attach_token)
        .await?;
    if args.raw && response.streams.len() != 1 {
        let _ = socket.close(None).await;
        anyhow::bail!(
            "--raw requires exactly one Task, but the target selected {}",
            response.streams.len()
        );
    }
    receive_logs(socket, response.streams, args.raw).await
}

pub(super) async fn run_scale(data_dir: &Path, args: ScaleArgs) -> Result<()> {
    let client = resolve_client(data_dir, args.connection).await?;
    let last = args.services.len().saturating_sub(1);
    for (index, value) in args.services.into_iter().enumerate() {
        let (service, replicas) = parse_scale(&value)?;
        let deployment: StackDeploymentResponse = client
            .send_json(
                reqwest::Method::POST,
                &format!("/v1/services/{}/scale", encode(service)),
                Some(&ServiceScaleRequest { replicas }),
            )
            .await?;
        // Serialize intermediate updates so targets in the same Stack cannot conflict.
        finish_deployment(&client, deployment, args.detach && index == last).await?;
        println!("{service} scaled to {replicas}");
    }
    Ok(())
}

pub(super) async fn run_restart(data_dir: &Path, args: RestartArgs) -> Result<()> {
    let client = resolve_client(data_dir, args.connection).await?;
    let deployment: StackDeploymentResponse = client
        .send_json::<_, ()>(
            reqwest::Method::POST,
            &format!("/v1/services/{}/force-update", encode(&args.service)),
            None,
        )
        .await?;
    finish_deployment(&client, deployment, args.detach).await?;
    println!("{}", args.service);
    Ok(())
}

pub(super) async fn run_remove(data_dir: &Path, args: RemoveArgs) -> Result<()> {
    let client = resolve_client(data_dir, args.connection).await?;
    for stack in args.stacks {
        let deployment: StackDeploymentResponse = client
            .send_json::<_, ()>(
                reqwest::Method::DELETE,
                &format!("/v1/stacks/{}", encode(&stack)),
                None,
            )
            .await?;
        finish_deployment(&client, deployment, args.detach).await?;
        println!("{stack}");
    }
    Ok(())
}

async fn resolve_client(data_dir: &Path, connection: ConnectionArgs) -> Result<ControllerClient> {
    let (controller, token) =
        node::resolve_connection(data_dir, connection.controller, connection.token).await?;
    Ok(ControllerClient::new(controller, token))
}

fn encode(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}

fn parse_scale(value: &str) -> Result<(&str, u32)> {
    let (service, replicas) = value.rsplit_once('=').ok_or_else(|| {
        anyhow::anyhow!("scale target {value:?} must use STACK.SERVICE=REPLICAS syntax")
    })?;
    if service.is_empty() || !service.contains('.') {
        anyhow::bail!("scale service must use STACK.SERVICE syntax");
    }
    let replicas = replicas.parse::<u32>().map_err(|_| {
        anyhow::anyhow!("replica count {replicas:?} must be a non-negative integer")
    })?;
    Ok((service, replicas))
}

fn print_service_table(response: &ServiceListResponse) {
    let rows = response
        .services
        .iter()
        .map(|service| {
            vec![
                service.id.clone(),
                format!("{}.{}", service.stack, service.name),
                "replicated".into(),
                format!("{}/{}", service.running_replicas, service.replicas),
                service.image.clone(),
            ]
        })
        .collect();
    print_table(&["ID", "NAME", "MODE", "REPLICAS", "IMAGE"], rows);
}

fn print_task_table(response: &TaskListResponse, quiet: bool, no_trunc: bool) {
    if quiet {
        for task in &response.tasks {
            println!("{}", display_id(&task.id, no_trunc));
        }
        return;
    }
    let rows = response
        .tasks
        .iter()
        .map(|task| {
            let ports = task
                .ports
                .iter()
                .map(|port| format!("{}->{}/{}", port.published, port.target, port.protocol))
                .collect::<Vec<_>>()
                .join(",");
            vec![
                display_id(&task.id, no_trunc),
                format!(
                    "{}.{}.{}",
                    task.stack,
                    task.service,
                    task.slot.saturating_add(1)
                ),
                task.image.clone(),
                task.node_id.clone(),
                format!("{:?}", task.desired),
                format!("{:?}", task.observed),
                task.error.clone().unwrap_or_default(),
                ports,
            ]
        })
        .collect();
    print_table(
        &[
            "ID",
            "NAME",
            "IMAGE",
            "NODE",
            "DESIRED STATE",
            "CURRENT STATE",
            "ERROR",
            "PORTS",
        ],
        rows,
    );
}

async fn receive_logs(
    socket: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    streams: Vec<DataSessionStream>,
    raw: bool,
) -> Result<()> {
    let prefixed = streams.len() > 1 && !raw;
    let prefixes = streams
        .iter()
        .map(|stream| {
            (
                stream.stream_id,
                format!(
                    "{}.{}.{}@{} | ",
                    stream.stack,
                    stream.service,
                    stream.slot.saturating_add(1),
                    stream.node_id
                )
                .into_bytes(),
            )
        })
        .collect::<HashMap<_, _>>();
    let expected_streams = prefixes.keys().copied().collect::<HashSet<_>>();
    let mut ended = HashSet::new();
    let mut failed = HashSet::new();
    let mut sequences = HashMap::<u32, u64>::new();
    let mut line_buffers = HashMap::<u32, Vec<u8>>::new();
    let mut stdout = BufWriter::with_capacity(LOG_OUTPUT_BUFFER_BYTES, tokio::io::stdout());
    let mut stderr = BufWriter::with_capacity(LOG_OUTPUT_BUFFER_BYTES, tokio::io::stderr());
    let mut flush_interval = tokio::time::interval(LOG_OUTPUT_FLUSH_INTERVAL);
    flush_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    flush_interval.reset();
    let (mut sink, mut source) = socket.split();

    while ended.len() < expected_streams.len() {
        let message = tokio::select! {
            message = source.next() => message,
            _ = flush_interval.tick() => {
                flush_log_outputs(&mut stdout, &mut stderr).await?;
                continue;
            }
        }
        .ok_or_else(|| anyhow::anyhow!("data session closed before all streams completed"))??;
        let Message::Binary(encoded) = message else {
            if matches!(message, Message::Close(_)) {
                anyhow::bail!("data session closed before all streams completed");
            }
            continue;
        };
        let frame = DataFrame::decode(&encoded).map_err(anyhow::Error::msg)?;
        if !expected_streams.contains(&frame.stream_id) {
            anyhow::bail!("received unassigned stream ID {}", frame.stream_id);
        }
        let expected_sequence = sequences
            .get(&frame.stream_id)
            .map_or(0, |sequence| sequence.saturating_add(1));
        if frame.sequence != expected_sequence {
            anyhow::bail!(
                "stream {} sequence {} arrived; expected {expected_sequence}",
                frame.stream_id,
                frame.sequence
            );
        }
        sequences.insert(frame.stream_id, frame.sequence);

        match frame.kind {
            DataFrameKind::Data => {
                if prefixed {
                    write_prefixed_log_data(
                        &mut stdout,
                        prefixes.get(&frame.stream_id).expect("known stream"),
                        line_buffers.entry(frame.stream_id).or_default(),
                        &frame.payload,
                    )
                    .await?;
                } else if frame.channel == DataChannel::Stderr {
                    write_log_output(&mut stderr, &frame.payload).await?;
                } else {
                    write_log_output(&mut stdout, &frame.payload).await?;
                }
            }
            DataFrameKind::Error => {
                failed.insert(frame.stream_id);
                let prefix = prefixes.get(&frame.stream_id).expect("known stream");
                write_log_error(&mut stderr, prefix, &frame.payload).await?;
                flush_log_outputs(&mut stdout, &mut stderr).await?;
            }
            DataFrameKind::End => {
                if prefixed
                    && let Some(buffer) = line_buffers.get_mut(&frame.stream_id)
                    && !buffer.is_empty()
                {
                    write_prefixed_log_remainder(
                        &mut stdout,
                        prefixes.get(&frame.stream_id).expect("known stream"),
                        buffer,
                    )
                    .await?;
                }
                ended.insert(frame.stream_id);
            }
            _ => anyhow::bail!("received unsupported {:?} frame", frame.kind),
        }
    }
    flush_log_outputs(&mut stdout, &mut stderr).await?;
    let _ = tokio::time::timeout(DATA_STREAM_WRITE_TIMEOUT, sink.send(Message::Close(None))).await;
    if failed.is_empty() {
        Ok(())
    } else {
        anyhow::bail!("failed to read logs from {} task(s)", failed.len())
    }
}

async fn write_prefixed_log_data<W>(
    output: &mut W,
    prefix: &[u8],
    buffer: &mut Vec<u8>,
    payload: &[u8],
) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    const MAX_PREFIXED_LINE_BYTES: usize = 64 * 1024;

    tokio::time::timeout(DATA_STREAM_WRITE_TIMEOUT, async {
        buffer.extend_from_slice(payload);
        let mut consumed = 0;
        while let Some(newline) = buffer[consumed..].iter().position(|byte| *byte == b'\n') {
            let end = consumed + newline + 1;
            output.write_all(prefix).await?;
            output.write_all(&buffer[consumed..end]).await?;
            consumed = end;
        }
        if consumed > 0 {
            buffer.drain(..consumed);
        }

        let complete_bytes = buffer.len() / MAX_PREFIXED_LINE_BYTES * MAX_PREFIXED_LINE_BYTES;
        for chunk in buffer[..complete_bytes].chunks(MAX_PREFIXED_LINE_BYTES) {
            output.write_all(prefix).await?;
            output.write_all(chunk).await?;
            output.write_all(b"\n").await?;
        }
        if complete_bytes > 0 {
            buffer.drain(..complete_bytes);
        }
        std::io::Result::Ok(())
    })
    .await
    .map_err(|_| anyhow::anyhow!("timed out writing logs to local output"))??;
    Ok(())
}

async fn write_prefixed_log_remainder<W>(
    output: &mut W,
    prefix: &[u8],
    buffer: &mut Vec<u8>,
) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    tokio::time::timeout(DATA_STREAM_WRITE_TIMEOUT, async {
        output.write_all(prefix).await?;
        output.write_all(buffer).await?;
        output.write_all(b"\n").await?;
        std::io::Result::Ok(())
    })
    .await
    .map_err(|_| anyhow::anyhow!("timed out writing logs to local output"))??;
    buffer.clear();
    Ok(())
}

async fn write_log_error<W>(output: &mut W, prefix: &[u8], payload: &[u8]) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    tokio::time::timeout(DATA_STREAM_WRITE_TIMEOUT, async {
        output.write_all(prefix).await?;
        output.write_all(b"ERROR: ").await?;
        output.write_all(payload).await?;
        output.write_all(b"\n").await
    })
    .await
    .map_err(|_| anyhow::anyhow!("timed out writing logs to local output"))??;
    Ok(())
}

async fn write_log_output<W>(output: &mut W, payload: &[u8]) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    tokio::time::timeout(DATA_STREAM_WRITE_TIMEOUT, output.write_all(payload))
        .await
        .map_err(|_| anyhow::anyhow!("timed out writing logs to local output"))??;
    Ok(())
}

async fn flush_log_outputs<Out, Err>(stdout: &mut Out, stderr: &mut Err) -> Result<()>
where
    Out: AsyncWrite + Unpin,
    Err: AsyncWrite + Unpin,
{
    tokio::time::timeout(DATA_STREAM_WRITE_TIMEOUT, async {
        stdout.flush().await?;
        stderr.flush().await
    })
    .await
    .map_err(|_| anyhow::anyhow!("timed out flushing logs to local output"))??;
    Ok(())
}

fn display_id(id: &str, no_trunc: bool) -> String {
    if no_trunc {
        id.to_owned()
    } else {
        id.chars().take(12).collect()
    }
}

fn print_table(headers: &[&str], rows: Vec<Vec<String>>) {
    let mut widths = headers
        .iter()
        .map(|header| header.len())
        .collect::<Vec<_>>();
    for row in &rows {
        for (index, value) in row.iter().enumerate() {
            widths[index] = widths[index].max(value.chars().count());
        }
    }
    print_table_row(
        &headers
            .iter()
            .map(|value| (*value).to_owned())
            .collect::<Vec<_>>(),
        &widths,
    );
    for row in &rows {
        print_table_row(row, &widths);
    }
}

fn print_table_row(values: &[String], widths: &[usize]) {
    for (index, value) in values.iter().enumerate() {
        if index + 1 == values.len() {
            print!("{value}");
        } else {
            let padding = widths[index].saturating_sub(value.chars().count()) + 2;
            print!("{value}{}", " ".repeat(padding));
        }
    }
    println!();
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncReadExt;

    use super::*;

    #[test]
    fn scale_requires_a_qualified_service_name() {
        assert_eq!(parse_scale("demo.web=3").unwrap(), ("demo.web", 3));
        assert!(parse_scale("web=3").is_err());
        assert!(parse_scale("demo.web=-1").is_err());
    }

    #[tokio::test]
    async fn prefixes_fragmented_lines_and_bounds_unterminated_lines() {
        let (mut output, mut input) = tokio::io::duplex(256 * 1024);
        let mut buffer = Vec::new();

        write_prefixed_log_data(&mut output, b"task | ", &mut buffer, b"one\npar")
            .await
            .unwrap();
        write_prefixed_log_data(&mut output, b"task | ", &mut buffer, b"tial\n")
            .await
            .unwrap();
        write_prefixed_log_data(&mut output, b"task | ", &mut buffer, &vec![b'x'; 64 * 1024])
            .await
            .unwrap();
        drop(output);

        let mut rendered = Vec::new();
        input.read_to_end(&mut rendered).await.unwrap();
        let mut expected = b"task | one\ntask | partial\ntask | ".to_vec();
        expected.extend(std::iter::repeat_n(b'x', 64 * 1024));
        expected.push(b'\n');
        assert_eq!(rendered, expected);
        assert!(buffer.is_empty());
    }
}
