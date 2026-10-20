//! Real Caddy coverage of generated deployment routes, without a public CA or
//! changes to the machine's trust store. Each Gateway has its own local test CA.
use serde_json::{Value, json};
use std::{
    fs,
    net::TcpListener,
    process::{Command, Output},
    thread,
    time::{Duration, Instant},
};
use swarmlite_core::{gateway, model::ClusterGatewayConfig};

fn success(output: Output) -> String {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

fn config(domain: Option<&str>) -> Value {
    let mut settings = ClusterGatewayConfig::default();
    settings.deploy.domain = domain.map(str::to_owned);
    let mut config = gateway::config_with_deployment(
        &Default::default(),
        &settings,
        "http://127.0.0.1:17080".into(),
        17082,
    );
    // Only the storage/probe plugins are replaced for the stock Caddy image;
    // system deployment routes are exactly those emitted by the Rust renderer.
    config.as_object_mut().unwrap().remove("storage");
    let routes = config["apps"]["http"]["servers"]["swarmlite"]["routes"]
        .as_array_mut()
        .unwrap();
    routes.retain(|route| {
        route["@id"]
            .as_str()
            .is_some_and(|id| id.starts_with("swarmlite-system-deploy"))
    });
    routes.push(json!({
        "match": [{"path": ["/redirect"]}],
        "handle": [{"handler": "static_response", "status_code": 308, "headers": {"Location": ["https://business.example/"]}}],
        "terminal": true
    }));
    routes.push(
        json!({"handle": [{"handler": "static_response", "body": "business"}], "terminal": true}),
    );
    config["apps"]["http"]["servers"]["controller"] = json!({
        "listen": [":17082"], "automatic_https": {"disable": true},
        "routes": [{"handle": [{"handler": "static_response", "status_code": 202,
            "body": "upstream|{http.request.method}|{http.request.uri}|{http.request.header.Authorization}|{http.request.header.Idempotency-Key}"}]}]
    });
    config["apps"]["pki"] = json!({"certificate_authorities": {"local": {"install_trust": false}}});
    config
}

struct Gateway {
    id: String,
    http: u16,
    https: u16,
    directory: tempfile::TempDir,
}

impl Gateway {
    fn start() -> Self {
        let listeners = [
            TcpListener::bind("127.0.0.1:0").unwrap(),
            TcpListener::bind("127.0.0.1:0").unwrap(),
        ];
        let ports = listeners.map(|listener| listener.local_addr().unwrap().port());
        let directory = tempfile::tempdir().unwrap();
        fs::write(
            directory.path().join("caddy.json"),
            serde_json::to_vec(&config(None)).unwrap(),
        )
        .unwrap();
        let id = success(
            Command::new("docker")
                .args([
                    "run",
                    "--rm",
                    "-d",
                    "-p",
                    &format!("127.0.0.1:{}:80", ports[0]),
                    "-p",
                    &format!("127.0.0.1:{}:443", ports[1]),
                    "-v",
                    &format!("{}:/test:ro", directory.path().display()),
                    "caddy:2",
                    "caddy",
                    "run",
                    "--config",
                    "/test/caddy.json",
                ])
                .output()
                .unwrap(),
        )
        .trim()
        .to_owned();
        let gateway = Self {
            id,
            http: ports[0],
            https: ports[1],
            directory,
        };
        let deadline = Instant::now() + Duration::from_secs(20);
        while gateway.request(false, None, "/", None, false).0 != 200 {
            assert!(
                Instant::now() < deadline,
                "Gateway failed to start: {}",
                gateway.logs()
            );
            thread::sleep(Duration::from_millis(100));
        }
        gateway
    }

    fn logs(&self) -> String {
        let output = Command::new("docker")
            .args(["logs", &self.id])
            .output()
            .unwrap();
        format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    }

    fn reload(&self, domain: Option<&str>) {
        fs::write(
            self.directory.path().join("caddy.json"),
            serde_json::to_vec(&config(domain)).unwrap(),
        )
        .unwrap();
        success(
            Command::new("docker")
                .args([
                    "exec",
                    &self.id,
                    "caddy",
                    "reload",
                    "--config",
                    "/test/caddy.json",
                ])
                .output()
                .unwrap(),
        );
        if domain.is_some() {
            let deadline = Instant::now() + Duration::from_secs(20);
            while !Command::new("docker")
                .args([
                    "cp",
                    &format!("{}:/data/caddy/pki/authorities/local/root.crt", self.id),
                    self.ca().to_str().unwrap(),
                ])
                .output()
                .unwrap()
                .status
                .success()
            {
                assert!(
                    Instant::now() < deadline,
                    "test CA missing: {}",
                    self.logs()
                );
                thread::sleep(Duration::from_millis(100));
            }
        }
    }

    fn ca(&self) -> std::path::PathBuf {
        self.directory.path().join("root.crt")
    }

    fn request(
        &self,
        tls: bool,
        domain: Option<&str>,
        path: &str,
        host: Option<&str>,
        post: bool,
    ) -> (u16, String) {
        let port = if tls { self.https } else { self.http };
        let domain = domain.unwrap_or("127.0.0.1");
        let url = format!(
            "{}://{domain}:{port}{path}",
            if tls { "https" } else { "http" }
        );
        let mut command = Command::new("curl");
        command.args([
            "--silent",
            "--show-error",
            "--noproxy",
            "*",
            "--connect-timeout",
            "1",
            "--max-time",
            "3",
            "--dump-header",
            "-",
            "--write-out",
            "\n%{http_code}",
            "--header",
            "Authorization: Bearer sldt_existing.secret",
            // This spoof must not turn an HTTP connection into an HTTPS match.
            "--header",
            "X-Forwarded-Proto: https",
        ]);
        if tls {
            command.args([
                "--resolve",
                &format!("{domain}:{port}:127.0.0.1"),
                "--cacert",
                self.ca().to_str().unwrap(),
            ]);
        }
        if let Some(host) = host {
            command.args(["--header", &format!("Host: {host}")]);
        }
        if post {
            command.args([
                "--header",
                "Idempotency-Key: ci:test:1",
                "--header",
                "Content-Type: application/json",
                "--data",
                "{}",
            ]);
        }
        let output = command.arg(url).output().unwrap();
        if !output.status.success() {
            return (0, String::from_utf8_lossy(&output.stderr).into_owned());
        }
        let output = String::from_utf8(output.stdout).unwrap();
        let (body, status) = output.rsplit_once('\n').unwrap();
        (status.parse().unwrap(), body.to_owned())
    }
}

impl Drop for Gateway {
    fn drop(&mut self) {
        let _ = Command::new("docker").args(["rm", "-f", &self.id]).output();
    }
}

#[test]
#[ignore = "requires Docker, local caddy:2 image, and curl; uses isolated test CAs, never a public CA"]
fn domain_https_and_http_policy_hot_reload_on_multiple_gateways() {
    success(
        Command::new("docker")
            .args(["image", "inspect", "caddy:2"])
            .output()
            .unwrap(),
    );
    let gateways = [Gateway::start(), Gateway::start()];
    let path = "/v1/deploy/services/production.api/redeploy";
    for gateway in &gateways {
        assert_eq!(gateway.request(false, None, path, None, true).0, 202);
    }

    for domain in ["deploy.localhost", "new.localhost"] {
        for gateway in &gateways {
            gateway.reload(Some(domain));
            for host in [
                None,
                Some(domain),
                Some("other.example.com"),
                Some("127.0.0.1"),
            ] {
                for path in [
                    path,
                    "/v1/deploy/operations/op_1",
                    "/v1/deploy",
                    "/v1/deploy/unknown",
                ] {
                    for post in [true, false] {
                        let (status, response) = gateway.request(false, None, path, host, post);
                        assert_eq!(status, 403, "{response}");
                        assert!(!response.to_lowercase().contains("location:"), "{response}");
                        assert!(!response.contains("upstream|"), "{response}");
                    }
                }
            }
            let deadline = Instant::now() + Duration::from_secs(20);
            let response = loop {
                let (status, response) = gateway.request(true, Some(domain), path, None, true);
                if status == 202 {
                    break response;
                }
                assert!(
                    Instant::now() < deadline,
                    "HTTPS failed: {response}\n{}",
                    gateway.logs()
                );
                thread::sleep(Duration::from_millis(100));
            };
            assert!(
                response.contains(&format!(
                    "upstream|POST|{path}|Bearer sldt_existing.secret|ci:test:1"
                )),
                "{response}"
            );
            assert_eq!(
                gateway
                    .request(
                        true,
                        Some(domain),
                        "/v1/deploy/operations/op_1",
                        None,
                        false
                    )
                    .0,
                202
            );
            for host in ["other.example.com", "127.0.0.1", "deploy.localhost"] {
                if host != domain {
                    assert_eq!(
                        gateway
                            .request(true, Some(domain), path, Some(host), true)
                            .0,
                        404
                    );
                }
            }
            let (status, body) = gateway.request(false, None, "/", None, false);
            assert_eq!(status, 200);
            assert!(body.contains("business"));
            assert_eq!(
                gateway.request(false, None, "/redirect", None, false).0,
                308
            );
            assert_eq!(
                gateway
                    .request(false, None, "/v1/deploy-other", None, false)
                    .0,
                200
            );
        }
    }
    for gateway in &gateways {
        gateway.reload(None);
        assert_eq!(gateway.request(false, None, path, None, true).0, 202);
    }
}
