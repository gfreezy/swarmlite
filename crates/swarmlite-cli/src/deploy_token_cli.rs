use super::{ConnectionArgs, ansi, append_table, connection, stderr_color, stdout_color};
use anyhow::Result;
use clap::Subcommand;
use std::{fmt::Write, path::Path};
use swarmlite_core::service_operations::{DeployToken, DeployTokenCreate, expiry_from_duration};

#[derive(Debug, Subcommand)]
pub(super) enum DeployTokenCommand {
    /// Create a credential scoped to exactly one existing service.
    Create {
        #[arg(long)]
        name: String,
        #[arg(long, value_name = "STACK.SERVICE")]
        service: String,
        /// Positive duration (s/m/h/d), or never for a manually revoked token.
        #[arg(long, default_value = "90d")]
        expires_in: String,
        #[command(flatten)]
        connection: ConnectionArgs,
    },
    /// List token metadata and URL templates; optionally reveal active secrets.
    List {
        #[arg(long)]
        show_secret: bool,
        #[command(flatten)]
        connection: ConnectionArgs,
    },
    /// Revoke a credential and clear its stored secret.
    Revoke {
        id: String,
        #[command(flatten)]
        connection: ConnectionArgs,
    },
}

pub(super) async fn run(data_dir: &Path, command: DeployTokenCommand) -> Result<()> {
    match command {
        DeployTokenCommand::Create {
            name,
            service,
            expires_in,
            connection: args,
        } => {
            expiry_from_duration(&expires_in, 0).map_err(anyhow::Error::msg)?;
            let client = connection::resolve(data_dir, args.controller, args.token).await?;
            let token: DeployToken = client
                .send_json(
                    reqwest::Method::POST,
                    "/v1/deploy-tokens",
                    Some(&DeployTokenCreate {
                        name,
                        service,
                        expires_in,
                    }),
                )
                .await?;
            if token.expires_at_unix_ms.is_none() {
                eprintln!(
                    "{}",
                    ansi(
                        stderr_color(),
                        "33",
                        "永久有效，需手动撤销或轮换；HTTP 不加密凭据。"
                    )
                );
            }
            print!("{}", render(&[token], true, stdout_color()));
        }
        DeployTokenCommand::List {
            show_secret,
            connection: args,
        } => {
            let client = connection::resolve(data_dir, args.controller, args.token).await?;
            let tokens: Vec<DeployToken> = client
                .get_json(&format!("/v1/deploy-tokens?show_secret={show_secret}"))
                .await?;
            print!("{}", render(&tokens, show_secret, stdout_color()));
        }
        DeployTokenCommand::Revoke {
            id,
            connection: args,
        } => {
            let client = connection::resolve(data_dir, args.controller, args.token).await?;
            let id = encode(&id);
            client
                .send_text(
                    reqwest::Method::DELETE,
                    &format!("/v1/deploy-tokens/{id}"),
                    None,
                    None,
                )
                .await?;
            println!("{}", ansi(stdout_color(), "32", "deploy token revoked"));
        }
    }
    Ok(())
}

fn encode(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes())
        .collect::<String>()
        .replace('+', "%20")
}
fn deployment_url(service: &str, color: bool) -> String {
    format!(
        "http://{}/v1/deploy/services/{}/redeploy",
        ansi(color, "33", "<GATEWAY_HOST>"),
        encode(service)
    )
}
fn render(tokens: &[DeployToken], show_secret: bool, color: bool) -> String {
    let now = chrono::Utc::now().timestamp_millis();
    let mut output = String::new();
    let mut rows = Vec::new();
    for token in tokens {
        let status = if token.revoked_at_unix_ms.is_some() {
            "revoked"
        } else if token.active(now) {
            "active"
        } else {
            "expired"
        };
        let status = ansi(color, if status == "active" { "32" } else { "31" }, status);
        let expiry = token
            .expires_at_unix_ms
            .map(|expiry| {
                chrono::DateTime::from_timestamp_millis(expiry)
                    .map(|time| time.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
                    .unwrap_or_else(|| expiry.to_string())
            })
            .unwrap_or_else(|| ansi(color, "33", "never (永久)"));
        if show_secret {
            if !output.is_empty() {
                output.push('\n');
            }
            for (name, value) in [
                ("ID", ansi(color, "2", &token.id)),
                ("NAME", token.name.clone()),
                ("SERVICE", ansi(color, "1;36", &token.service_id)),
                ("STATUS", status),
                ("EXPIRES", expiry),
                ("DEPLOY_URL", deployment_url(&token.service_id, color)),
            ] {
                writeln!(
                    output,
                    "{}  {value}",
                    ansi(color, "1", format!("{name:<10}"))
                )
                .unwrap();
            }
            if let Some(secret) = token.token.as_ref().filter(|_| token.active(now)) {
                writeln!(output, "{}  {secret}", ansi(color, "1", "TOKEN     ")).unwrap();
                let key = uuid::Uuid::new_v4();
                writeln!(output, "{}  curl --fail-with-body --silent --show-error --request POST --header 'Authorization: Bearer {secret}' --header 'Idempotency-Key: manual:{key}' --header 'Content-Type: application/json' --data '{{}}' '{}'", ansi(color, "1", "CURL      "), deployment_url(&token.service_id, false)).unwrap();
            }
        } else {
            rows.push(vec![
                ansi(color, "2", &token.id),
                token.name.clone(),
                ansi(color, "1;36", &token.service_id),
                status,
                expiry,
                deployment_url(&token.service_id, color),
            ]);
        }
    }
    if !show_secret {
        append_table(
            &mut output,
            &["ID", "NAME", "SERVICE", "STATUS", "EXPIRES", "DEPLOY_URL"],
            &rows,
            color,
        );
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn templates_keep_placeholder_and_copyable_secrets_uncolored() {
        let token = DeployToken {
            id: "demo".into(),
            name: "ci".into(),
            service_id: "production.api".into(),
            created_at_unix_ms: 0,
            expires_at_unix_ms: None,
            revoked_at_unix_ms: None,
            token: Some("sldt_demo.secret".into()),
        };
        let plain = render(std::slice::from_ref(&token), true, false);
        assert!(!plain.contains('\x1b'));
        assert!(plain.contains("never (永久)"));
        assert!(plain.contains("http://<GATEWAY_HOST>/v1/deploy/services/production.api/redeploy"));
        let colored = render(std::slice::from_ref(&token), true, true);
        assert!(colored.contains("\x1b[32mactive"));
        for line in colored
            .lines()
            .filter(|line| line.contains("sldt_demo.secret"))
        {
            let value = line.split_once("\x1b[0m").unwrap().1;
            assert!(!value.contains('\x1b'));
        }
        assert!(!render(&[token], false, false).contains("sldt_demo.secret"));
    }
}
