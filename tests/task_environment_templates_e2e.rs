use std::{
    collections::BTreeMap,
    fs::{self, File},
    net::TcpListener,
    path::Path,
    process::{Child, Command, Output, Stdio},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const TEST_HOSTNAME: &str = "swarmlite-template-e2e-node";

struct TestCluster {
    server: Child,
    stack: String,
}

impl Drop for TestCluster {
    fn drop(&mut self) {
        let _ = self.server.kill();
        let _ = self.server.wait();
        for container in docker_containers(&self.stack) {
            let _ = Command::new("docker")
                .args(["rm", "-f", &container])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
    }
}

#[test]
#[ignore = "requires a running Docker daemon with nginx:alpine available locally"]
fn stack_templates_reach_the_container_environment() {
    require_local_image("nginx:alpine");

    let binary = env!("CARGO_BIN_EXE_swarmlite");
    let directory = tempfile::tempdir().expect("create E2E data directory");
    let data_dir = directory.path().join("data");
    let port = available_port();
    let stack = unique_stack_name();

    let init = Command::new(binary)
        .args([
            "--data-dir",
            path_text(&data_dir),
            "init",
            "--controller-port",
            &port.to_string(),
            "--advertise-address",
            "127.0.0.1",
            "--runtime",
            "docker",
            "--no-gateway",
        ])
        .env("HOSTNAME", TEST_HOSTNAME)
        .output()
        .expect("run swarmlite init");
    assert_success("swarmlite init", &init);

    let stdout_path = directory.path().join("serve.stdout.log");
    let stderr_path = directory.path().join("serve.stderr.log");
    let server = Command::new(binary)
        .args([
            "--data-dir",
            path_text(&data_dir),
            "serve",
            "--advertise-address",
            "127.0.0.1",
            "--runtime",
            "docker",
        ])
        .env("HOSTNAME", TEST_HOSTNAME)
        .stdout(Stdio::from(
            File::create(&stdout_path).expect("create serve stdout log"),
        ))
        .stderr(Stdio::from(
            File::create(&stderr_path).expect("create serve stderr log"),
        ))
        .spawn()
        .expect("start swarmlite serve");
    let mut cluster = TestCluster { server, stack };
    wait_for_controller(binary, &data_dir, &mut cluster.server, &stderr_path);

    let stack_file = directory.path().join("stack.yaml");
    fs::write(
        &stack_file,
        r#"services:
  worker:
    image: nginx:alpine
    pull_policy: never
    entrypoint: ["sh", "-c"]
    command: ["sleep 300"]
    environment:
      CUSTOM_SERVICE: "{{.Service.Name}}"
      CUSTOM_SERVICE_ID: "{{.Service.ID}}"
      CUSTOM_NODE_ID: "{{.Node.ID}}"
      CUSTOM_NODE_HOSTNAME: "{{.Node.Hostname}}"
      CUSTOM_NODE_ARCH: "{{.Node.Platform.Architecture}}"
      CUSTOM_NODE_OS: "{{.Node.Platform.OS}}"
      CUSTOM_TASK_ID: "{{.Task.ID}}"
      CUSTOM_TASK_NAME: "{{.Task.Name}}"
      CUSTOM_TASK_SLOT: "{{.Task.Slot}}"
      CUSTOM_OWNER: '{{index .Service.Labels "com.example.owner"}}'
      CUSTOM_JOINED: '{{join "-" .Service.Name .Task.Slot}}'
    deploy:
      labels:
        com.example.owner: platform
"#,
    )
    .expect("write E2E Stack file");

    let deploy = Command::new(binary)
        .args([
            "--data-dir",
            path_text(&data_dir),
            "deploy",
            "--compose-file",
            path_text(&stack_file),
            "--detach",
            &cluster.stack,
        ])
        .output()
        .expect("deploy E2E Stack");
    assert_success("swarmlite deploy", &deploy);

    let container = wait_for_container(&cluster.stack, Duration::from_secs(30));
    let environment = inspect_environment(&container);
    let task_id = inspect_label(&container, "io.swarmlite.task_id");

    assert_eq!(environment["CUSTOM_SERVICE"], "worker");
    assert_eq!(
        environment["CUSTOM_SERVICE_ID"],
        format!("{}.worker", cluster.stack)
    );
    assert_eq!(environment["CUSTOM_NODE_HOSTNAME"], TEST_HOSTNAME);
    assert!(
        environment["CUSTOM_NODE_ID"].starts_with(&format!("{TEST_HOSTNAME}-")),
        "unexpected node ID: {}",
        environment["CUSTOM_NODE_ID"]
    );
    assert!(!environment["CUSTOM_NODE_ARCH"].is_empty());
    assert!(!environment["CUSTOM_NODE_OS"].is_empty());
    assert_eq!(environment["CUSTOM_TASK_ID"], task_id);
    assert_eq!(
        environment["CUSTOM_TASK_NAME"],
        format!("worker.1.{task_id}")
    );
    assert_eq!(environment["CUSTOM_TASK_SLOT"], "1");
    assert_eq!(environment["CUSTOM_OWNER"], "platform");
    assert_eq!(environment["CUSTOM_JOINED"], "worker-1");

    let remove = Command::new(binary)
        .args([
            "--data-dir",
            path_text(&data_dir),
            "rm",
            "--detach",
            &cluster.stack,
        ])
        .output()
        .expect("remove E2E Stack");
    assert_success("swarmlite rm", &remove);
}

fn require_local_image(image: &str) {
    let output = Command::new("docker")
        .args(["image", "inspect", image])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("run docker image inspect");
    assert!(
        output.success(),
        "E2E test requires {image} locally; run `docker pull {image}` first"
    );
}

fn available_port() -> u16 {
    TcpListener::bind(("127.0.0.1", 0))
        .expect("reserve controller port")
        .local_addr()
        .expect("read controller port")
        .port()
}

fn unique_stack_name() -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_millis();
    format!("template-e2e-{}-{timestamp}", std::process::id())
}

fn wait_for_controller(binary: &str, data_dir: &Path, server: &mut Child, stderr_path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        if let Some(status) = server.try_wait().expect("inspect swarmlite serve") {
            let stderr = fs::read_to_string(stderr_path).unwrap_or_default();
            panic!("swarmlite serve exited with {status}: {stderr}");
        }
        let status = Command::new(binary)
            .args(["--data-dir", path_text(data_dir), "status", "--json"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .expect("probe Swarmlite Controller");
        if status.success() {
            return;
        }
        thread::sleep(Duration::from_millis(200));
    }
    let stderr = fs::read_to_string(stderr_path).unwrap_or_default();
    panic!("Swarmlite Controller did not become ready: {stderr}");
}

fn wait_for_container(stack: &str, timeout: Duration) -> String {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let containers = docker_containers(stack);
        if containers.len() == 1 {
            return containers[0].clone();
        }
        assert!(
            containers.len() <= 1,
            "expected one E2E task container, found {containers:?}"
        );
        thread::sleep(Duration::from_millis(250));
    }
    panic!("timed out waiting for the E2E task container");
}

fn docker_containers(stack: &str) -> Vec<String> {
    let filter = format!("label=io.swarmlite.stack={stack}");
    let output = Command::new("docker")
        .args(["ps", "-aq", "--filter", &filter])
        .output()
        .expect("list E2E task containers");
    assert_success("docker ps", &output);
    String::from_utf8(output.stdout)
        .expect("docker ps emitted non-UTF-8 output")
        .lines()
        .map(str::to_owned)
        .collect()
}

fn inspect_environment(container: &str) -> BTreeMap<String, String> {
    let output = Command::new("docker")
        .args([
            "inspect",
            "--format",
            "{{range .Config.Env}}{{println .}}{{end}}",
            container,
        ])
        .output()
        .expect("inspect task environment");
    assert_success("docker inspect environment", &output);
    String::from_utf8(output.stdout)
        .expect("docker inspect emitted non-UTF-8 environment")
        .lines()
        .filter_map(|entry| entry.split_once('='))
        .map(|(name, value)| (name.to_owned(), value.to_owned()))
        .collect()
}

fn inspect_label(container: &str, label: &str) -> String {
    let template = format!("{{{{index .Config.Labels {label:?}}}}}");
    let output = Command::new("docker")
        .args(["inspect", "--format", &template, container])
        .output()
        .expect("inspect task label");
    assert_success("docker inspect label", &output);
    String::from_utf8(output.stdout)
        .expect("docker inspect emitted a non-UTF-8 label")
        .trim()
        .to_owned()
}

fn assert_success(action: &str, output: &Output) {
    assert!(
        output.status.success(),
        "{action} failed with {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn path_text(path: &Path) -> &str {
    path.to_str().expect("test path is not valid UTF-8")
}
