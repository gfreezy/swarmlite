use std::{
    collections::BTreeMap,
    fs::{self, File},
    net::TcpListener,
    path::Path,
    process::{Child, Command, Output, Stdio},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde_json::Value;

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

#[test]
#[ignore = "requires a running Linux Docker daemon with cgroup v2 and busybox:1.36 available locally"]
fn container_security_and_resource_settings_reach_docker_and_the_kernel() {
    require_local_image("busybox:1.36");

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
  restricted:
    image: busybox:1.36
    pull_policy: never
    entrypoint: ["sh", "-c"]
    command: ["sleep 300"]
    user: "10001:10001"
    read_only: true
    cap_drop: [ALL]
    cap_add: [NET_BIND_SERVICE]
    security_opt: [no-new-privileges:true]
    tmpfs:
      - /tmp:size=8m,mode=1777,noexec,nosuid,nodev
      - /run:size=4m,noexec,nosuid,nodev
    pids_limit: 64
    cpus: "0.5"
    mem_limit: 64m
    memswap_limit: 64m
"#,
    )
    .expect("write runtime settings E2E Stack file");

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
        .expect("deploy runtime settings E2E Stack");
    assert_success("swarmlite deploy", &deploy);

    let container = wait_for_container(&cluster.stack, Duration::from_secs(30));
    let inspect = inspect_container(&container);
    assert_eq!(inspect["State"]["Running"], true);
    assert_eq!(inspect["Config"]["User"], "10001:10001");
    assert_eq!(inspect["HostConfig"]["ReadonlyRootfs"], true);
    assert_eq!(
        inspect["HostConfig"]["CapAdd"],
        serde_json::json!(["NET_BIND_SERVICE"])
    );
    assert_eq!(inspect["HostConfig"]["CapDrop"], serde_json::json!(["ALL"]));
    assert_eq!(
        inspect["HostConfig"]["SecurityOpt"],
        serde_json::json!(["no-new-privileges"])
    );
    assert_eq!(
        inspect["HostConfig"]["Tmpfs"]["/tmp"],
        "size=8m,mode=1777,noexec,nosuid,nodev"
    );
    assert_eq!(inspect["HostConfig"]["PidsLimit"], 64);
    assert_eq!(inspect["HostConfig"]["NanoCpus"], 500_000_000);
    assert_eq!(inspect["HostConfig"]["Memory"], 64 * 1024 * 1024);
    assert_eq!(inspect["HostConfig"]["MemorySwap"], 64 * 1024 * 1024);

    let observations = docker_exec(
        &container,
        r#"set -eu
printf 'uid='; id -u
printf 'gid='; id -g
printf 'no_new_privs='; awk '/^NoNewPrivs:/ {print $2}' /proc/self/status
printf 'cap_eff='; awk '/^CapEff:/ {print $2}' /proc/self/status
printf 'cap_bnd='; awk '/^CapBnd:/ {print $2}' /proc/self/status
printf 'root_options='; awk '$2 == "/" {print $4}' /proc/mounts
printf 'tmpfs_mount='; awk '$2 == "/tmp" {print $3 ":" $4}' /proc/mounts
touch /tmp/swarmlite-e2e-write
printf 'tmpfs_write='; test -f /tmp/swarmlite-e2e-write && echo ok
printf 'pids_max='; cat /sys/fs/cgroup/pids.max
printf 'cpu_max='; cat /sys/fs/cgroup/cpu.max
printf 'memory_max='; cat /sys/fs/cgroup/memory.max
printf 'swap_max='; cat /sys/fs/cgroup/memory.swap.max
"#,
    );
    let observations = observations
        .lines()
        .filter_map(|line| line.split_once('='))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(observations["uid"], "10001");
    assert_eq!(observations["gid"], "10001");
    assert_eq!(observations["no_new_privs"], "1");
    assert_eq!(observations["cap_eff"], "0000000000000000");
    assert_eq!(observations["cap_bnd"], "0000000000000400");
    assert!(
        observations["root_options"]
            .split(',')
            .any(|option| option == "ro")
    );
    let tmpfs_mount = observations["tmpfs_mount"];
    assert!(tmpfs_mount.starts_with("tmpfs:"), "{tmpfs_mount}");
    let tmpfs_options = tmpfs_mount
        .strip_prefix("tmpfs:")
        .expect("tmpfs mount should include its filesystem type");
    for option in ["rw", "nosuid", "nodev", "noexec"] {
        assert!(
            tmpfs_options.split(',').any(|actual| actual == option),
            "missing tmpfs option {option}: {tmpfs_mount}"
        );
    }
    assert_eq!(observations["tmpfs_write"], "ok");
    assert_eq!(observations["pids_max"], "64");
    let (quota, period) = observations["cpu_max"]
        .split_once(' ')
        .expect("cpu.max should contain quota and period");
    assert_eq!(
        quota.parse::<u64>().unwrap() * 2,
        period.parse::<u64>().unwrap()
    );
    assert_eq!(observations["memory_max"], (64 * 1024 * 1024).to_string());
    assert_eq!(observations["swap_max"], "0");

    let remove = Command::new(binary)
        .args([
            "--data-dir",
            path_text(&data_dir),
            "rm",
            "--detach",
            &cluster.stack,
        ])
        .output()
        .expect("remove runtime settings E2E Stack");
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

fn inspect_container(container: &str) -> Value {
    let output = Command::new("docker")
        .args(["inspect", container])
        .output()
        .expect("inspect task container");
    assert_success("docker inspect", &output);
    serde_json::from_slice::<Vec<Value>>(&output.stdout)
        .expect("docker inspect emitted invalid JSON")
        .into_iter()
        .next()
        .expect("docker inspect returned no container")
}

fn docker_exec(container: &str, script: &str) -> String {
    let output = Command::new("docker")
        .args(["exec", container, "sh", "-c", script])
        .output()
        .expect("execute runtime settings probe");
    assert_success("docker exec runtime settings probe", &output);
    String::from_utf8(output.stdout).expect("runtime settings probe emitted non-UTF-8 output")
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
