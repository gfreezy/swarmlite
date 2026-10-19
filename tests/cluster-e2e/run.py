#!/usr/bin/env python3
"""Opt-in, destructive only inside three freshly-created Docker-in-Docker nodes.

No host ports, production sockets, registry writes, or external ACME are used.
Each node has a separate dockerd and network namespace, including its Gateway's
host network. All outer resources are uniquely named and labelled for cleanup.
"""

import argparse
import concurrent.futures
import json
import pathlib
import re
import shlex
import subprocess
import threading
import time
import traceback
import uuid


def run(*args, check=True, timeout=120):
    result = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    if check and result.returncode:
        raise RuntimeError(f"{shlex.join(args)}\n{result.stdout}\n{result.stderr}")
    return result


def wait(label, predicate, timeout=120):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            value = predicate()
            if value:
                return value
        except (RuntimeError, ValueError, KeyError, IndexError, AssertionError) as error:
            last = error
        time.sleep(1)
    raise AssertionError(f"Timed out waiting for {label}; last error: {last}")


class Node:
    def __init__(self, cluster, role):
        self.cluster = cluster
        self.role = role
        self.name = f"{cluster.prefix}-{role}"
        self.image = "baseline"
        self.ip = None
        self.created = False

    def execute(self, *args, **kwargs):
        return run("docker", "exec", self.name, *args, **kwargs)

    def docker(self, *args, **kwargs):
        return self.execute("docker", *args, **kwargs)

    def cli(self, *args, **kwargs):
        return self.execute(f"/opt/swarmlite-{self.image}", "--data-dir", "/state",
                            "--color", "never", *args, **kwargs)

    def start_container(self):
        run("docker", "run", "-d", "--privileged", "--name", self.name,
            "--hostname", self.role, "--network", self.cluster.prefix,
            "--label", f"io.swarmlite.e2e={self.cluster.prefix}",
            "-e", "DOCKER_TLS_CERTDIR=", "--mount",
            f"type=bind,src={self.cluster.output},dst=/artifacts,readonly",
            self.cluster.args.dind_image, "dockerd", "--host=unix:///var/run/docker.sock")
        self.created = True
        self.ip = json.loads(run("docker", "inspect", self.name).stdout)[0][
            "NetworkSettings"]["Networks"][self.cluster.prefix]["IPAddress"]
        wait(f"{self.role} dockerd", lambda: self.docker("info", check=False).returncode == 0)
        for name, source in [("current", self.cluster.args.binary),
                             ("baseline", self.cluster.args.baseline_binary)]:
            run("docker", "cp", str(source), f"{self.name}:/opt/swarmlite-{name}")
        self.docker("load", "-i", "/artifacts/images.tar", timeout=240)
        self.execute("mkdir", "-p", "/state")
        print(f"{self.role}: independent dockerd ready at {self.ip}", flush=True)

    def serve(self):
        command = shlex.join([f"/opt/swarmlite-{self.image}", "--data-dir", "/state",
                              "serve", "--advertise-address", self.ip, "--runtime", "docker"])
        # docker exec -d keeps a genuine long-running supervisor, not a mock agent.
        run("docker", "exec", "-d", self.name, "sh", "-c",
            f"echo $$ > /run/swarmlite.pid; exec {command} >> /state/serve.log 2>&1")

    def signal(self, signal):
        self.execute("sh", "-c", f"kill -{signal} $(cat /run/swarmlite.pid)")

    def stop(self):
        self.signal("TERM")
        wait(f"{self.role} supervisor exit", lambda: self.execute(
            "sh", "-c", "! kill -0 $(cat /run/swarmlite.pid) 2>/dev/null", check=False).returncode == 0,
             timeout=30)

    def gateways(self):
        ids = self.docker("ps", "-q", "--filter", "label=io.swarmlite.component=gateway").stdout.split()
        return [json.loads(self.docker("inspect", value).stdout)[0] for value in ids]

    def get(self, port, host="http.e2e.test", path="/", ok=True):
        # BusyBox wget is available inside dind. No dependency on host routing to
        # VM/container addresses, and both gateways are reached from a third node.
        result = self.cluster.worker.execute("wget", "-qO-", "-T", "2",
                                              "--header", f"Host: {host}",
                                              f"http://{self.ip}:{port}{path}", check=False, timeout=5)
        if not re.fullmatch(r"[0-9a-f]{12}", result.stdout.strip()):
            result.returncode = 1
        if ok and result.returncode:
            raise AssertionError(f"{self.role}:{port}: {result.stdout!r} {result.stderr}")
        return result

    def tcp_status(self):
        return json.loads(self.execute("wget", "-qO-", "http://127.0.0.1:2019/swarmlite/tcp/status").stdout)

    def config(self):
        return json.loads(self.execute("wget", "-qO-", "http://127.0.0.1:2019/config/").stdout)


class Cluster:
    def __init__(self, args):
        self.args = args
        self.output = args.output.resolve()
        if self.output.exists() and any(self.output.iterdir()):
            raise ValueError("Choose an empty output directory to preserve previous artifacts")
        self.output.mkdir(parents=True, exist_ok=True)
        self.prefix = f"swarmlite-e2e-{uuid.uuid4().hex[:10]}"
        self.controller = Node(self, "controller")
        self.gateway = Node(self, "gateway")
        self.worker = Node(self, "worker")
        self.nodes = [self.controller, self.gateway, self.worker]
        self.gateways = self.nodes[:2]
        self.results = []
        self.network_created = False
        self.sampling = False
        self.samples = []
        self.sampler = None
        self.last_sample = 0

    def record(self, name, **details):
        samples = self.samples[self.last_sample:]
        self.last_sample += len(samples)
        item = {"scenario": name, "passed": True, "http_samples": len(samples),
                "http_sample_failures": sum(not s["success"] for s in samples), **details}
        self.results.append(item)
        print(json.dumps(item), flush=True)
        (self.output / f"checkpoint-{len(self.results):02d}.json").write_text(json.dumps({
            "scenario": name, "status": self.status(), "gateways": self.gateway_status()}, indent=2))
        self.save_report()

    def save_report(self, **extra):
        (self.output / "report.json").write_text(json.dumps({
            "prefix": self.prefix, "results": self.results, "http_samples": self.samples,
            **extra}, indent=2) + "\n")

    def status(self):
        return json.loads(self.controller.cli("status", "--json").stdout)

    def gateway_status(self):
        return json.loads(self.controller.cli("gateway", "status", "--json").stdout)

    def ready(self):
        value = self.gateway_status()
        return len([n for n in value["nodes"] if n["enabled"] and n["status"] == "ready"]) == 2

    def tasks(self, stack):
        state = self.status()["state"]
        return [t for t in state["tasks"].values()
                if t["service_id"].startswith(stack + ".") and t["desired"] == "running"]

    def http_ok(self, host="http.e2e.test"):
        return all(g.get(80, host, ok=False).returncode == 0 for g in self.gateways)

    def tcp_ok(self, port=8099):
        return all(g.get(port, ok=False).returncode == 0 for g in self.gateways)

    def tcp_routes(self, node, port):
        return [r for r in node.tcp_status()["routes"] if r["listen"] == f":{port}"]

    def deploy(self, name, tcp=None, hostname=None, detach=False, replicas=2, future=False):
        gateway = {"tls": "disabled", "http": "serve"}
        if tcp:
            gateway["tcp_routes"] = [{"listen": p, "backend": {"service": "web", "port": 8080}}
                                     for p in tcp]
        if hostname:
            gateway["http_routes"] = [{"hostnames": [hostname], "rules": [
                {"backend": {"service": "web", "port": 8080}}]}]
        if future:
            gateway["future_extension"] = {"ignored": True}
            for route in gateway.get("tcp_routes", []):
                route["future_extension"] = True
                route["backend"]["future_extension"] = True
        stack = {"services": {"web": {
            "image": "busybox:1.36", "pull_policy": "never", "expose": ["8080"],
            "entrypoint": ["sh", "-c"], "command": [
                "mkdir -p /www; hostname > /www/index.html; exec httpd -f -p 8080 -h /www"],
            "stop_grace_period": "1s", "healthcheck": {
                "test": ["CMD-SHELL", "test ! -e /tmp/unhealthy && wget -qO- http://127.0.0.1:8080/ >/dev/null"],
                "interval": "1s", "timeout": "1s", "retries": 2},
            "deploy": {"replicas": replicas, "placement": {
                "constraints": ["node.labels.role==worker"]}}}}, "x-swarmlite": gateway}
        filename = f"{name}.json"
        (self.output / filename).write_text(json.dumps(stack, indent=2))
        command = ["deploy", "-c", f"/artifacts/{filename}", name, "--json"]
        if detach:
            command.append("--detach")
        return self.controller.cli(*command, timeout=180)

    def sample_http(self):
        def sample():
            while self.sampling:
                for node in self.gateways:
                    result = node.get(80, ok=False)
                    self.samples.append({"time": time.time(), "node": node.role,
                                         "success": result.returncode == 0})
                time.sleep(0.3)
        self.sampling = True
        self.sampler = threading.Thread(target=sample, daemon=True)
        self.sampler.start()

    def setup(self):
        print(f"Starting isolated cluster {self.prefix}", flush=True)
        run("docker", "network", "create", "--label", f"io.swarmlite.e2e={self.prefix}", self.prefix)
        self.network_created = True
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
            list(pool.map(lambda n: n.start_container(), self.nodes))
        c = self.controller
        c.cli("init", "--token", "cluster-e2e-only-token", "--advertise-address", c.ip,
              "--runtime", "docker", "--gateway-image", self.args.baseline_gateway,
              "--label", "role=controller")
        c.serve()
        wait("Controller", lambda: c.cli("status", "--json", check=False).returncode == 0)
        for node in self.nodes[1:]:
            command = ["join", f"http://{c.ip}:17080", "--token", "cluster-e2e-only-token",
                       "--advertise-address", node.ip, "--runtime", "docker", "--label", f"role={node.role}"]
            if node == self.gateway:
                command.append("--gateway")
            node.cli(*command)
            node.serve()
        wait("three online nodes and two managed Gateways", lambda:
             len(self.status()["state"]["nodes"]) == 3 and self.ready())
        self.deploy("http", hostname="http.e2e.test")
        wait("baseline HTTP", self.http_ok)
        assert all("swarmlite_tcp" not in g.config()["apps"] for g in self.gateways)
        self.record("old HTTP-only cluster", nodes=3, gateways=2,
                    binary_version=c.cli("--version").stdout.strip())
        self.sample_http()

    def upgrade(self):
        original = [t["container_id"] for t in self.tasks("http")]
        for node in [self.worker, self.gateway, self.controller]:
            node.stop()
            node.image = "current"
            node.serve()
            wait(f"{node.role} after binary upgrade", lambda: self.controller.cli(
                "status", "--json", check=False).returncode == 0 and self.http_ok())
        wait("Controller reconciles preserved HTTP containers after restart", lambda:
             len(self.tasks("http")) == 2 and all(t["observed"] == "healthy" and t["container_id"]
                                                 for t in self.tasks("http")))
        assert sorted(original) == sorted(t["container_id"] for t in self.tasks("http"))
        old_ids = {g.role: [c["Id"] for c in g.gateways()] for g in self.gateways}
        self.controller.cli("config", "set", "gateway.image", self.args.gateway)
        wait("blue/green Gateway image upgrade", lambda: self.ready() and all(
            g.gateways() and all(c["Config"]["Image"] == self.args.gateway for c in g.gateways()) and
            not any(c["Id"] in old_ids[g.role] for c in g.gateways()) for g in self.gateways), timeout=180)
        wait("HTTP after Gateway upgrade", self.http_ok)
        assert all("swarmlite_tcp" not in g.config()["apps"] for g in self.gateways)
        self.record("rolling binary and managed Gateway upgrade", http_containers_preserved=True)

    def scenarios(self):
        self.deploy("tcp", tcp=[8099, 8100], future=True)
        wait("two TCP routes on two Gateways", lambda: self.ready() and self.tcp_ok() and self.tcp_ok(8100))
        tasks = self.tasks("tcp")
        assert len(tasks) == 2 and all(t["observed"] == "healthy" for t in tasks)
        state = self.status()["state"]
        assert all(state["nodes"][t["node_id"]]["address"] == self.worker.ip for t in tasks)
        endpoints = {f"{self.worker.ip}:{p['published']}" for t in tasks for p in t["ports"] if p["target"] == 8080}
        for g in self.gateways:
            route = self.tcp_routes(g, 8099)[0]
            assert {b["address"] for b in route["backends"]} == endpoints
            assert all(b["healthy"] for b in route["backends"])
            replies = {g.get(8099).stdout.strip() for _ in range(8)}
            assert len(replies) == 2, replies
        self.record("cross-node TCP, two Gateways, two healthy replicas, round robin",
                    endpoints=sorted(endpoints), unknown_fields_ignored=True)

        # One application's Docker health check fails although TCP still accepts.
        victim = tasks[0]["container_id"]
        self.worker.docker("exec", victim, "touch", "/tmp/unhealthy")
        victim_ports = {f"{self.worker.ip}:{p['published']}" for p in tasks[0]["ports"] if p["target"] == 8080}
        wait("Docker-unhealthy endpoint removed by Controller", lambda: all(
            not (victim_ports & {b["address"] for b in self.tcp_routes(g, 8099)[0]["backends"]})
            for g in self.gateways))
        assert self.tcp_ok() and self.http_ok()
        wait("failed replica automatically replaced", lambda: len(self.tasks("tcp")) == 2 and all(
            t["observed"] == "healthy" and t["container_id"] != victim for t in self.tasks("tcp")))
        self.record("Docker health check excludes live-but-unhealthy backend and replaces it")

        # Occupy a port in only one Gateway's real host network namespace.
        blocker = self.controller.docker("run", "-d", "--network", "host", "busybox:1.36",
                                         "httpd", "-f", "-p", "8101").stdout.strip()
        self.deploy("conflict", tcp=[8101], detach=True)
        wait("isolated bind failure", lambda: self.tcp_routes(self.controller, 8101) and
             not self.tcp_routes(self.controller, 8101)[0]["listening"] and
             self.tcp_routes(self.gateway, 8101)[0]["listening"])
        wait("normal configuration ACK despite TCP bind failure", self.ready)
        assert json.loads(self.controller.cli("deployment", "status", "conflict", "--json").stdout)["status"] != "healthy"
        self.deploy("http-new", hostname="new.e2e.test")
        assert self.http_ok() and self.http_ok("new.e2e.test") and self.tcp_ok()
        self.record("occupied host port isolates TCP route; unrelated HTTP deploy and TCP remain healthy")

        # Restart actual managed Gateway while conflict is still present. --resume
        # must not cause a boot loop or lose existing HTTP/TCP routes.
        for node in self.gateways:
            for container in node.gateways():
                node.docker("restart", container["Id"])
        wait("Gateway --resume with occupied TCP port", lambda: self.http_ok() and self.tcp_ok())
        assert not self.tcp_routes(self.controller, 8101)[0]["listening"]
        self.record("managed Gateway restart and resume under TCP bind conflict")
        self.controller.docker("rm", "-f", blocker)
        wait("released port and owning deployment recover", lambda: self.ready() and self.tcp_ok(8101))
        wait("owning deployment healthy after bind recovery", lambda: json.loads(
            self.controller.cli("deployment", "status", "conflict", "--json").stdout)["status"] == "healthy")
        self.record("release occupied port recovers without redeploy")

        # Freeze only the worker agent so it cannot immediately replace crashed
        # tasks. Active Caddy probes (not Controller endpoint removal) detect death.
        tasks = self.tasks("tcp")
        self.worker.signal("STOP")
        try:
            self.worker.docker("stop", "-t", "0", *[t["container_id"] for t in tasks])
            wait("all dead TCP backends actively detected", lambda: all(
                not any(b["healthy"] for b in self.tcp_routes(g, 8099)[0]["backends"])
                for g in self.gateways), timeout=15)
            for node in self.gateways:
                assert node.get(8099, ok=False).returncode != 0
            assert self.http_ok() and self.tcp_ok(8101)
            self.record("all replicas crash: active TCP health removes them; HTTP and other route survive")
        finally:
            self.worker.signal("CONT")
        wait("replacement endpoints recover after all replicas crash", lambda: self.tcp_ok() and
             len(self.tasks("tcp")) == 2 and all(t["observed"] == "healthy" for t in self.tasks("tcp")))
        new_endpoints = {b["address"] for b in self.tcp_routes(self.controller, 8099)[0]["backends"]}
        assert not (endpoints & new_endpoints)
        self.record("task replacement updates dynamically published ports and restores TCP")

        self.controller.cli("service", "scale", "tcp.web=0", "--detach", timeout=30)
        wait("zero-replica empty upstream configuration", lambda: all(
            self.tcp_routes(g, 8099)[0]["listening"] and not self.tcp_routes(g, 8099)[0]["backends"]
            for g in self.gateways))
        assert all(g.get(8099, ok=False).returncode != 0 for g in self.gateways)
        assert self.http_ok() and self.tcp_ok(8101)
        wait("intentional zero-replica deployment completes", lambda: json.loads(
            self.controller.cli("deployment", "status", "tcp", "--json").stdout)["status"] == "healthy", timeout=30)
        self.record("scale to zero retains listener, closes connections, preserves unrelated forwarding")
        self.controller.cli("service", "scale", "tcp.web=2", timeout=180)
        wait("scale-up TCP recovery", self.tcp_ok)
        self.record("scale back up restores TCP")

        self.controller.stop()
        assert self.http_ok() and self.tcp_ok()
        self.controller.serve()
        wait("Controller restored from persistent DB", lambda: self.ready() and self.tcp_ok() and self.http_ok())
        self.record("Controller restart retains running Gateway forwarding and persisted TCP configuration")

        self.controller.cli("service", "restart", "tcp.web", timeout=180)
        wait("TCP-only service restart completes", self.tcp_ok)
        assert self.http_ok()
        self.record("TCP-only service restart completes without HTTP drain wait")

        # New image ID forces Swarmlite's actual candidate/promote/retire path.
        blocker = self.gateway.docker("run", "-d", "--network", "host", "busybox:1.36",
                                      "httpd", "-f", "-p", "8102").stdout.strip()
        self.deploy("upgrade-conflict", tcp=[8102], detach=True)
        wait("conflict during image replacement", lambda: self.ready() and
             self.tcp_routes(self.gateway, 8102) and not self.tcp_routes(self.gateway, 8102)[0]["listening"])
        old_ids = {g.role: [c["Id"] for c in g.gateways()] for g in self.gateways}
        self.controller.cli("config", "set", "gateway.image", self.args.replacement_gateway)
        wait("TCP-enabled Gateway blue/green", lambda: self.ready() and all(
            g.gateways() and all(c["Config"]["Image"] == self.args.replacement_gateway for c in g.gateways()) and
            not any(c["Id"] in old_ids[g.role] for c in g.gateways()) for g in self.gateways), timeout=180)
        wait("forwarding after TCP-enabled blue/green", lambda: self.http_ok() and self.tcp_ok() and self.tcp_ok(8101))
        assert not self.tcp_routes(self.gateway, 8102)[0]["listening"]
        self.record("real managed Gateway blue/green image replacement despite TCP bind conflict")
        self.gateway.docker("rm", "-f", blocker)
        wait("post-upgrade port recovery", lambda: self.tcp_ok(8102))
        wait("post-upgrade owning deployment healthy", lambda: json.loads(
            self.controller.cli("deployment", "status", "upgrade-conflict", "--json").stdout)["status"] == "healthy")

        self.controller.cli("rm", "tcp", "conflict", "upgrade-conflict", timeout=180)
        wait("TCP app removed", lambda: all("swarmlite_tcp" not in g.config()["apps"] for g in self.gateways))
        for node in self.gateways:
            for port in [8099, 8100, 8101, 8102]:
                proof = node.docker("run", "-d", "--network", "host", "busybox:1.36", "httpd", "-f", "-p", str(port)).stdout.strip()
                try:
                    time.sleep(0.2)
                    assert json.loads(node.docker("inspect", proof).stdout)[0]["State"]["Running"]
                finally:
                    node.docker("rm", "-f", proof)
        assert self.http_ok() and self.http_ok("new.e2e.test")
        self.record("Stack removal releases all TCP ports without leaking listeners or changing HTTP")

    def collect(self):
        self.sampling = False
        if self.sampler:
            self.sampler.join(timeout=10)
        for node in self.nodes:
            if not node.created:
                continue
            destination = self.output / node.role
            destination.mkdir(exist_ok=True)
            (destination / "dockerd.log").write_text(run("docker", "logs", node.name, check=False).stderr)
            run("docker", "cp", f"{node.name}:/state", str(destination), check=False)
            for container in node.gateways():
                log = node.docker("logs", container["Id"], check=False)
                (destination / f"gateway-{container['Id'][:12]}.log").write_text(log.stdout + log.stderr)
            if node.role != "worker":
                for name, operation in [("config", node.config), ("tcp", node.tcp_status)]:
                    try:
                        (destination / f"{name}.json").write_text(json.dumps(operation(), indent=2))
                    except (RuntimeError, ValueError):
                        pass
        for name, operation in [("status", self.status), ("gateways", self.gateway_status)]:
            try:
                (self.output / f"{name}.json").write_text(json.dumps(operation(), indent=2))
            except (RuntimeError, ValueError):
                pass

    def cleanup(self):
        for node in self.nodes:
            if node.created:
                run("docker", "rm", "-fv", node.name, check=False)
        if self.network_created:
            run("docker", "network", "rm", self.prefix, check=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=pathlib.Path)
    parser.add_argument("--baseline-binary", required=True, type=pathlib.Path)
    parser.add_argument("--gateway", default="swarmlite-e2e-gateway:current")
    parser.add_argument("--baseline-gateway", default="swarmlite-e2e-gateway:baseline")
    parser.add_argument("--replacement-gateway", default="swarmlite-e2e-gateway:replacement")
    parser.add_argument("--dind-image", default="docker:29-dind")
    parser.add_argument("--output", required=True, type=pathlib.Path)
    args = parser.parse_args()
    for binary in [args.binary, args.baseline_binary]:
        assert binary.is_file(), binary
    cluster = Cluster(args)
    failure = None
    try:
        # Preloaded images ensure the test never accidentally pulls a release
        # image in place of the local working-tree implementation.
        run("docker", "image", "save", "-o", str(cluster.output / "images.tar"),
            args.gateway, args.baseline_gateway, args.replacement_gateway, "busybox:1.36", timeout=180)
        cluster.setup()
        cluster.upgrade()
        cluster.scenarios()
    except (Exception, KeyboardInterrupt):
        failure = traceback.format_exc()
        print(failure, flush=True)
    finally:
        try:
            cluster.collect()
        except Exception:
            print(traceback.format_exc(), flush=True)
        cluster.cleanup()
        cluster.save_report(passed=failure is None, failure=failure,
                            http_sample_failures=sum(not s["success"] for s in cluster.samples))
    print(f"Report: {cluster.output / 'report.json'}", flush=True)
    return 1 if failure else 0


if __name__ == "__main__":
    raise SystemExit(main())
