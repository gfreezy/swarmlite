use serde_json::{Value, json};
use std::{
    fs::{self, File},
    net::TcpListener,
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

struct Cluster {
    server: Option<Child>,
    gateway: Option<String>,
    stack: String,
    data: PathBuf,
    log: PathBuf,
    deploy_port: u16,
}
impl Cluster {
    fn cli(&self, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_swarmlite"))
            .args([
                "--data-dir",
                self.data.to_str().unwrap(),
                "--color",
                "never",
            ])
            .args(args)
            .output()
            .unwrap()
    }
    fn start(&mut self) {
        self.server = Some(
            Command::new(env!("CARGO_BIN_EXE_swarmlite"))
                .args([
                    "--data-dir",
                    self.data.to_str().unwrap(),
                    "serve",
                    "--advertise-address",
                    "127.0.0.1",
                    "--runtime",
                    "docker",
                    "--deploy-listen",
                    &format!("0.0.0.0:{}", self.deploy_port),
                ])
                .stdout(Stdio::null())
                .stderr(Stdio::from(
                    File::options()
                        .append(true)
                        .create(true)
                        .open(&self.log)
                        .unwrap(),
                ))
                .spawn()
                .unwrap(),
        );
        wait(
            Duration::from_secs(30),
            || self.cli(&["status", "--json"]).status.success(),
            &self.log,
        );
    }
    fn stop(&mut self) {
        if let Some(mut server) = self.server.take() {
            let _ = server.kill();
            let _ = server.wait();
        }
    }
    fn containers(&self) -> Vec<String> {
        let output = Command::new("docker")
            .args([
                "ps",
                "-aq",
                "--filter",
                &format!("label=io.swarmlite.stack={}", self.stack),
            ])
            .output()
            .unwrap();
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .filter(|line| !line.is_empty())
            .map(str::to_owned)
            .collect()
    }
}
impl Drop for Cluster {
    fn drop(&mut self) {
        self.stop();
        if let Some(id) = self.gateway.take() {
            let _ = Command::new("docker").args(["rm", "-f", &id]).output();
        }
        for id in self.containers() {
            let _ = Command::new("docker").args(["rm", "-f", &id]).output();
        }
    }
}
fn success(output: Output) -> String {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}
fn wait(timeout: Duration, mut predicate: impl FnMut() -> bool, log: &Path) {
    let deadline = Instant::now() + timeout;
    loop {
        if predicate() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out: {}",
            fs::read_to_string(log).unwrap_or_default()
        );
        thread::sleep(Duration::from_millis(150));
    }
}
fn inspect(id: &str) -> Value {
    serde_json::from_str::<Value>(&success(
        Command::new("docker")
            .args(["inspect", id])
            .output()
            .unwrap(),
    ))
    .unwrap()[0]
        .clone()
}
fn request(url: &str, token: &str, key: Option<&str>) -> (u16, Value) {
    let mut command = Command::new("curl");
    command.args([
        "--silent",
        "--show-error",
        "--connect-timeout",
        "2",
        "--max-time",
        "30",
        "--write-out",
        "\n%{http_code}",
        "--header",
        &format!("Authorization: Bearer {token}"),
    ]);
    if let Some(key) = key {
        command.args([
            "--request",
            "POST",
            "--header",
            &format!("Idempotency-Key: {key}"),
            "--header",
            "Content-Type: application/json",
            "--data",
            "{}",
        ]);
    }
    let body = success(command.arg(url).output().unwrap());
    let (body, status) = body.rsplit_once('\n').unwrap();
    (status.parse().unwrap(), serde_json::from_str(body).unwrap())
}

#[test]
#[ignore = "requires Linux Docker, local nginx:alpine and caddy:2 images, curl, and registry access"]
fn http_ci_deployment_and_in_place_restart_with_real_docker_and_gateway() {
    for image in ["nginx:alpine", "caddy:2"] {
        success(
            Command::new("docker")
                .args(["image", "inspect", image])
                .output()
                .unwrap(),
        );
    }
    let directory = tempfile::tempdir().unwrap();
    let listeners = (0..3)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect::<Vec<_>>();
    let ports = listeners
        .iter()
        .map(|listener| listener.local_addr().unwrap().port())
        .collect::<Vec<_>>();
    drop(listeners);
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let mut cluster = Cluster {
        server: None,
        gateway: None,
        stack: format!("ci-e2e-{suffix}"),
        data: directory.path().join("data"),
        log: directory.path().join("serve.log"),
        deploy_port: ports[1],
    };
    success(cluster.cli(&[
        "init",
        "--controller-port",
        &ports[0].to_string(),
        "--advertise-address",
        "127.0.0.1",
        "--runtime",
        "docker",
        "--no-gateway",
    ]));
    cluster.start();
    let file = directory.path().join("stack.yaml");
    fs::write(&file, "services:\n  web:\n    image: nginx:alpine\n    pull_policy: never\n    entrypoint: [sh, -c]\n    command: [\"sleep 600\"]\n    stop_grace_period: 1s\n").unwrap();
    success(cluster.cli(&["deploy", "-c", file.to_str().unwrap(), &cluster.stack]));
    let service = format!("{}.web", cluster.stack);
    let original_id = cluster.containers().into_iter().next().unwrap();
    let before = inspect(&original_id);
    success(
        Command::new("docker")
            .args([
                "exec",
                &original_id,
                "sh",
                "-c",
                "printf preserved > /tmp/restart-proof",
            ])
            .output()
            .unwrap(),
    );
    success(cluster.cli(&["service", "restart", &service]));
    assert_eq!(
        cluster.containers().as_slice(),
        std::slice::from_ref(&original_id)
    );
    let after = inspect(&original_id);
    assert_ne!(before["State"]["StartedAt"], after["State"]["StartedAt"]);
    assert_eq!(before["Image"], after["Image"]);
    assert_eq!(
        before["Config"]["Labels"]["io.swarmlite.task_id"],
        after["Config"]["Labels"]["io.swarmlite.task_id"]
    );
    assert_eq!(
        success(
            Command::new("docker")
                .args(["exec", &original_id, "cat", "/tmp/restart-proof"])
                .output()
                .unwrap()
        ),
        "preserved"
    );

    let output = success(cluster.cli(&[
        "deploy-token",
        "create",
        "--name",
        "e2e",
        "--service",
        &service,
        "--expires-in",
        "never",
    ]));
    assert!(output.contains("never (永久)"));
    assert!(output.contains("http://<GATEWAY_HOST>"));
    let token = output
        .lines()
        .find_map(|line| line.strip_prefix("TOKEN").map(str::trim))
        .unwrap()
        .to_owned();
    let token_id = output
        .lines()
        .find_map(|line| line.strip_prefix("ID").map(str::trim))
        .unwrap()
        .to_owned();
    assert!(!success(cluster.cli(&["deploy-token", "list"])).contains(&token));
    assert!(success(cluster.cli(&["deploy-token", "list", "--show-secret"])).contains(&token));

    // Exercise the actual generated system route through a separate Caddy
    // container, including precedence over an application's HTTP redirect.
    let gateway_config = swarmlite_core::model::ClusterGatewayConfig {
        listen: vec![":80".into()],
        ..Default::default()
    };
    let mut config = swarmlite_core::gateway::config_with_deployment(
        &Default::default(),
        &gateway_config,
        format!("http://host.docker.internal:{}", ports[0]),
        ports[1],
    );
    config.as_object_mut().unwrap().remove("storage");
    config["admin"] = json!({"disabled":true});
    let routes = config["apps"]["http"]["servers"]["swarmlite"]["routes"]
        .as_array_mut()
        .unwrap();
    routes.retain(|route| route["@id"] == "swarmlite-system-deploy");
    routes.push(json!({"handle":[{"handler":"static_response","status_code":308,"headers":{"Location":["https://app.example/"]}}],"terminal":true}));
    let config_file = directory.path().join("caddy.json");
    fs::write(&config_file, serde_json::to_vec(&config).unwrap()).unwrap();
    cluster.gateway = Some(
        success(
            Command::new("docker")
                .args([
                    "run",
                    "-d",
                    "--name",
                    &format!("{}-gateway", cluster.stack),
                    "--add-host",
                    "host.docker.internal:host-gateway",
                    "-p",
                    &format!("127.0.0.1:{}:80", ports[2]),
                    "-v",
                    &format!("{}:/etc/caddy/config.json:ro", config_file.display()),
                    "caddy:2",
                    "caddy",
                    "run",
                    "--config",
                    "/etc/caddy/config.json",
                ])
                .output()
                .unwrap(),
        )
        .trim()
        .into(),
    );
    let base = format!("http://127.0.0.1:{}", ports[2]);
    wait(
        Duration::from_secs(20),
        || {
            Command::new("curl")
                .args([
                    "--silent",
                    "--connect-timeout",
                    "1",
                    "--max-time",
                    "2",
                    &base,
                ])
                .output()
                .is_ok_and(|output| output.status.success())
        },
        &cluster.log,
    );
    assert_eq!(
        request(
            &format!("{base}/v1/deploy/operations/does-not-exist"),
            &token,
            None
        )
        .0,
        404
    );
    let endpoint = format!("{base}/v1/deploy/services/{service}/redeploy");
    let (status, operation) = request(&endpoint, &token, Some("ci:e2e:1"));
    assert_eq!(status, 202);
    let id = operation["operation_id"].as_str().unwrap();
    // Simulate a lost trigger response and controller crash. Retrying the
    // original logical request must retain its operation and generation.
    cluster.stop();
    cluster.start();
    let (status, duplicate) = request(&endpoint, &token, Some("ci:e2e:1"));
    assert_eq!(status, 202);
    assert_eq!(duplicate["operation_id"], operation["operation_id"]);
    let state_url = format!("{base}/v1/deploy/operations/{id}");
    wait(
        Duration::from_secs(120),
        || {
            let (status, state) = request(&state_url, &token, None);
            assert_eq!(status, 200);
            assert_ne!(state["status"], "failed", "{state}");
            state["status"] == "succeeded"
        },
        &cluster.log,
    );
    let current = cluster.containers();
    assert_eq!(current.len(), 1);
    assert_ne!(current[0], original_id);
    let deployed = inspect(&current[0]);
    assert_ne!(
        deployed["Config"]["Labels"]["io.swarmlite.task_id"],
        before["Config"]["Labels"]["io.swarmlite.task_id"]
    );
    assert_eq!(
        deployed["Config"]["Labels"]["io.swarmlite.revision"]
            .as_str()
            .unwrap()
            .parse::<u64>()
            .unwrap(),
        before["Config"]["Labels"]["io.swarmlite.revision"]
            .as_str()
            .unwrap()
            .parse::<u64>()
            .unwrap()
            + 1
    );
    success(cluster.cli(&["deploy-token", "revoke", &token_id]));
    assert_eq!(request(&state_url, &token, None).0, 401);
}
