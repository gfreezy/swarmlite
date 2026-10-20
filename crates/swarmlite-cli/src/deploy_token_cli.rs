use super::{
    ConnectionArgs, ansi, append_table, cluster_config, connection, stderr_color, stdout_color,
};
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
            // Fail before creating a credential if its public URL cannot be
            // determined. A failed config request must never imply HTTP mode.
            let domain = cluster_config(&client, None)
                .await?
                .config
                .gateway
                .deploy
                .domain;
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
                        "Token never expires; revoke or rotate it manually."
                    )
                );
            }
            warn_http(domain.as_deref());
            print!(
                "{}",
                render(&[token], true, domain.as_deref(), stdout_color())
            );
        }
        DeployTokenCommand::List {
            show_secret,
            connection: args,
        } => {
            let client = connection::resolve(data_dir, args.controller, args.token).await?;
            let domain = cluster_config(&client, None)
                .await?
                .config
                .gateway
                .deploy
                .domain;
            let tokens: Vec<DeployToken> = client
                .get_json(&format!("/v1/deploy-tokens?show_secret={show_secret}"))
                .await?;
            warn_http(domain.as_deref());
            print!(
                "{}",
                render(&tokens, show_secret, domain.as_deref(), stdout_color())
            );
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
fn warn_http(domain: Option<&str>) {
    if domain.is_none() {
        eprintln!(
            "{}",
            ansi(
                stderr_color(),
                "33",
                "Deployment domain is not configured; HTTP does not encrypt credentials. Replace <GATEWAY_HOST> or set gateway.deploy.domain to use HTTPS."
            )
        );
    }
}

fn deployment_url(service: &str, domain: Option<&str>, color: bool) -> String {
    let base = domain.map_or_else(
        || format!("http://{}", ansi(color, "33", "<GATEWAY_HOST>")),
        |domain| format!("https://{domain}"),
    );
    format!("{base}/v1/deploy/services/{}/redeploy", encode(service))
}
fn render(tokens: &[DeployToken], show_secret: bool, domain: Option<&str>, color: bool) -> String {
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
            .unwrap_or_else(|| ansi(color, "33", "never (permanent)"));
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
                (
                    "DEPLOY_URL",
                    deployment_url(&token.service_id, domain, color),
                ),
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
                writeln!(output, "{}  curl --fail-with-body --silent --show-error --request POST --header 'Authorization: Bearer {secret}' --header 'Idempotency-Key: manual:{key}' --header 'Content-Type: application/json' --data '{{}}' '{}'", ansi(color, "1", "CURL      "), deployment_url(&token.service_id, domain, false)).unwrap();
            }
        } else {
            rows.push(vec![
                ansi(color, "2", &token.id),
                token.name.clone(),
                ansi(color, "1;36", &token.service_id),
                status,
                expiry,
                deployment_url(&token.service_id, domain, color),
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

    #[tokio::test]
    async fn config_failure_does_not_fall_back_to_http_or_create_a_token() {
        use axum::{Router, http::StatusCode, routing::get};
        let app = Router::new()
            .route(
                "/v1/config",
                get(|| async { StatusCode::SERVICE_UNAVAILABLE }),
            )
            .fallback(|| async { StatusCode::INTERNAL_SERVER_ERROR });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let controller = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let directory = tempfile::tempdir().unwrap();
        for command in [
            DeployTokenCommand::Create {
                name: "ci".into(),
                service: "production.api".into(),
                expires_in: "never".into(),
                connection: ConnectionArgs {
                    controller: Some(controller.clone()),
                    token: Some("admin".into()),
                },
            },
            DeployTokenCommand::List {
                show_secret: true,
                connection: ConnectionArgs {
                    controller: Some(controller.clone()),
                    token: Some("admin".into()),
                },
            },
        ] {
            assert!(
                run(directory.path(), command)
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("503")
            );
        }
        server.abort();
    }

    #[test]
    fn configured_domain_is_used_for_urls_and_copyable_curl_without_binding_the_token() {
        let token = DeployToken {
            id: "demo".into(),
            name: "ci".into(),
            service_id: "production.api".into(),
            created_at_unix_ms: 0,
            expires_at_unix_ms: None,
            revoked_at_unix_ms: None,
            token: Some("sldt_demo.secret".into()),
        };
        for domain in ["deploy.example.com", "new.example.com"] {
            for show_secret in [true, false] {
                let output = render(
                    std::slice::from_ref(&token),
                    show_secret,
                    Some(domain),
                    true,
                );
                assert!(output.contains(&format!(
                    "https://{domain}/v1/deploy/services/production.api/redeploy"
                )));
                assert!(!output.contains("<GATEWAY_HOST>"));
                assert!(!output.contains("http://"));
                assert_eq!(output.contains("sldt_demo.secret"), show_secret);
                if show_secret {
                    for line in output.lines().filter(|line| line.contains("https://")) {
                        assert!(!line.split_once("\x1b[0m").unwrap().1.contains('\x1b'));
                    }
                }
            }
        }
        assert_eq!(
            deployment_url("stack.a/b c", Some("deploy.example.com"), false),
            "https://deploy.example.com/v1/deploy/services/stack.a%2Fb%20c/redeploy"
        );
        assert!(render(&[token], true, None, false).contains("http://<GATEWAY_HOST>"));
    }

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
        let plain = render(std::slice::from_ref(&token), true, None, false);
        assert!(!plain.contains('\x1b'));
        assert!(plain.is_ascii());
        assert!(plain.contains("never (permanent)"));
        assert!(plain.contains("http://<GATEWAY_HOST>/v1/deploy/services/production.api/redeploy"));
        let colored = render(std::slice::from_ref(&token), true, None, true);
        assert!(colored.contains("\x1b[32mactive"));
        for line in colored
            .lines()
            .filter(|line| line.contains("sldt_demo.secret"))
        {
            let value = line.split_once("\x1b[0m").unwrap().1;
            assert!(!value.contains('\x1b'));
        }
        assert!(!render(&[token], false, None, false).contains("sldt_demo.secret"));
    }
}
