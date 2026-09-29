use super::{ConnectionArgs, connection, print_pretty_json, stdout_color};
use crate::swarmlite::model::TaskRecord;
use anyhow::Result;
use clap::Subcommand;
use std::path::Path;

#[derive(Debug, Subcommand)]
pub(super) enum JobCommand {
    /// Start one manual execution (rejects unfinished previous executions).
    Run {
        #[arg(value_name = "STACK.JOB")]
        target: String,
        #[command(flatten)]
        connection: ConnectionArgs,
    },
    /// List retained executions, newest first.
    History {
        #[arg(value_name = "STACK.JOB")]
        target: String,
        #[command(flatten)]
        connection: ConnectionArgs,
        #[arg(long)]
        json: bool,
    },
    /// Request termination of an execution by its full task ID.
    Cancel {
        #[arg(value_name = "TASK_ID")]
        task_id: String,
        #[command(flatten)]
        connection: ConnectionArgs,
    },
}

pub(super) async fn run(data_dir: &Path, command: JobCommand) -> Result<()> {
    let (args, target, operation, json) = match command {
        JobCommand::Run { connection, target } => (connection, target, "run", false),
        JobCommand::History {
            connection,
            target,
            json,
        } => (connection, target, "history", json),
        JobCommand::Cancel {
            connection,
            task_id,
        } => (connection, task_id, "cancel", false),
    };
    let client = connection::resolve(data_dir, args.controller, args.token).await?;
    let target = url::form_urlencoded::byte_serialize(target.as_bytes()).collect::<String>();
    match operation {
        "history" => {
            let tasks: Vec<TaskRecord> = client
                .get_json(&format!("/v1/jobs/{target}/history"))
                .await?;
            if json {
                print_pretty_json(&tasks, stdout_color())?;
            } else {
                println!("TASK ID\tSTATE\tNODE\tSCHEDULED (UNIX MS)\tEXIT");
                for task in tasks {
                    let job = task.job.as_ref().unwrap();
                    println!(
                        "{}\t{:?}\t{}\t{}\t{}",
                        task.id,
                        task.observed,
                        task.node_id,
                        job.scheduled_at_unix_ms,
                        job.runtime
                            .as_ref()
                            .and_then(|r| r.exit_code)
                            .map_or("-".into(), |c| c.to_string())
                    );
                }
            }
        }
        _ => {
            let path = if operation == "run" {
                format!("/v1/jobs/{target}/run")
            } else {
                format!("/v1/job-tasks/{target}/cancel")
            };
            let task: TaskRecord = client
                .send_json::<_, ()>(reqwest::Method::POST, &path, None)
                .await?;
            println!("{}", task.id);
        }
    }
    Ok(())
}
