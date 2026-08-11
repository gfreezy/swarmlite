# Swarmlite

Swarmlite is a small Rust container orchestrator for one LAN or region. Every machine runs an
Agent through the same `swarmlite serve` command. The initialized node also runs the cluster's
single, immutable Controller. A per-node switch controls whether it also runs a Gateway.

This is an MVP, not a drop-in replacement for Docker Swarm or Kubernetes. It intentionally has
no overlay network or routing mesh.

## Install

### Linux server

The one-command installer detects an existing Docker or Podman installation, installs Docker when
neither is present, downloads and verifies the matching CLI, and installs the systemd service:

```bash
curl -fsSL https://github.com/gfreezy/swarmlite/releases/latest/download/install.sh | sudo sh
```

`auto` reuses the runtime selected by an earlier installation, then prefers an installed Docker,
then an installed Podman. A new machine gets Docker by default. Select rootful Podman explicitly
with:

```bash
curl -fsSL https://github.com/gfreezy/swarmlite/releases/latest/download/install.sh \
  | sudo sh -s -- --runtime podman
```

Podman is often the shorter package installation on Fedora and RHEL. Docker remains the default
because Swarmlite talks to the Docker API directly, while Podman is reached through its
Docker-compatible API and an additional systemd socket.

The installer supports Docker's official repositories on Ubuntu, Debian, Fedora, RHEL, and CentOS.
Podman installation uses the distribution package manager (`apt`, `dnf`, `yum`, `zypper`, or
`pacman`). It enables Docker or the rootful Podman API socket, but it does not guess whether this
node should initialize or join a cluster.

Initialize the first node and start it:

```bash
. /etc/swarmlite/runtime.env
sudo swarmlite --data-dir /var/lib/swarmlite init \
  --runtime "$SWARMLITE_RUNTIME" --runtime-socket "$SWARMLITE_RUNTIME_SOCKET"
sudo systemctl enable --now swarmlite
```

For another node, replace `init` with the generated `join` command and pass the same runtime flags,
then enable the service. Logs and status are available through systemd:

```bash
sudo systemctl status swarmlite
sudo journalctl -u swarmlite -f
```

The installed unit is maintained in
[`packaging/systemd/swarmlite.service`](packaging/systemd/swarmlite.service). It runs the node from
`/usr/local/bin/swarmlite`, stores durable local state in `/var/lib/swarmlite`, and reads the chosen
runtime from `/etc/swarmlite/runtime.env`.

### macOS ARM64 CLI

macOS does not install systemd or a container runtime. Any accessible Docker-compatible Unix socket
is sufficient. If no socket exists, the installer exits and recommends installing OrbStack. Run
the installer without `sudo` so it can access the current user's runtime socket:

```bash
curl -fsSL https://github.com/gfreezy/swarmlite/releases/latest/download/install.sh | sh
```

The installer supports Apple silicon, verifies the CLI archive, and asks for `sudo` only if writing
to `/usr/local/bin` requires it. It probes `$HOME/.orbstack/run/docker.sock`,
`/var/run/docker.sock`, and `$HOME/.docker/run/docker.sock`. Install OrbStack from
[orbstack.dev/download](https://orbstack.dev/download) when no compatible runtime is available.

## Build

Rust 1.97 or newer is required to build Swarmlite:

```bash
cargo build --release --locked
```

GitHub Actions also builds downloadable archives for Linux AMD64, Linux ARM64, and macOS ARM64.
Each workflow run stores one archive and SHA-256 checksum per platform as an Actions artifact;
tag pushes additionally publish them in a GitHub Release.

The project [Dockerfile](Dockerfile) builds Swarmlite. Gateway nodes automatically pull the
prebuilt `ghcr.io/gfreezy/swarmlite-caddy:latest` image, which contains Caddy plus the
Swarmlite storage module. A Docker-compatible runtime is therefore required on deployed nodes;
Go is needed only when developing or publishing the gateway image itself.

## Commands

```text
init                 initialize a single-controller cluster
join                 pull cluster settings and configure another node
serve                run this node's fixed components
config get|set       read or update cluster-wide settings
gateway status|enable|disable
                     read or update one node's gateway switch
node label get|set|remove
                     read or update one node's placement labels
deploy               deploy or update a stack
status               inspect cluster state
```

There are no separate public `controller`, `agent`, or `gateway` runtime commands.

## Quick start

Initialize and serve the first node:

```bash
swarmlite init
swarmlite serve
```

Those commands show foreground operation. On a Linux server installed with `install.sh`, initialize
or join the node under `/var/lib/swarmlite` as shown above, then use
`sudo systemctl enable --now swarmlite` instead of starting `serve` manually.

Every node runs an Agent. The initialized node is permanently the Controller and has its Gateway
enabled by default. Joined nodes are never Controllers and have their Gateway disabled by default.
Use `swarmlite init --no-gateway` when the initial node should not expose a Gateway.

`serve` detects Docker or Podman and automatically selects the address used by the operating
system's default route. Specify an address only when detection cannot choose a reachable one:

```bash
swarmlite serve --advertise-address 10.0.0.21
```

The override is persisted. All durable Swarmlite state on a node is stored in one
`swarmlite.sqlite` database; Swarmlite does not maintain separate local and control-plane database
files or JSON state files. Agent nodes use only the local-state table, while the controller also
uses the control-plane, KV object, and KV lock tables. The default data directory is
`$XDG_STATE_HOME/swarmlite`, or `$HOME/.local/state/swarmlite`.

```bash
swarmlite --data-dir /var/lib/swarmlite serve
```

Deploy and inspect a stack using the saved controller URL and token:

```bash
swarmlite deploy --name demo --file examples/stack.yaml
swarmlite status
```

The controller accepts only one in-progress deployment for a given Stack name. A concurrent
deployment of the same Stack returns `409 Conflict`; deployments using different Stack names may
be submitted independently.

## Join nodes and configure gateways

Print a join command on an initialized node:

```bash
swarmlite join-token
```

Run it on another machine, then start the same runtime command:

```bash
swarmlite join http://10.0.0.21:8080 --token '<generated-token>'
swarmlite serve
```

The initialized node is permanently the only Controller. Enable the Gateway while joining a node
when needed:

```bash
swarmlite join http://10.0.0.21:8080 \
  --token '<generated-token>' \
  --gateway
```

Read or change the Gateway switch after startup:

```bash
swarmlite gateway status node-a
swarmlite gateway enable node-a
swarmlite gateway disable node-a
```

The cluster may temporarily have no Gateway. Deploying a Stack with HTTP routes is rejected until
one is enabled. Moving the Controller requires stopping the entire cluster and initializing a new
cluster on the replacement node.

## Node labels and placement

Set initial labels while creating or joining a node:

```bash
swarmlite init --label region=cn-east --label disk=nvme
swarmlite join http://10.0.0.21:8080 \
  --token '<generated-token>' \
  --label region=cn-east
```

After a node has joined, its labels belong to cluster state. Read or change them through the
controller:

```bash
swarmlite node label get node-a
swarmlite node label set node-a region cn-north
swarmlite node label remove node-a disk
```

`serve` does not accept labels. A heartbeat cannot add or overwrite labels; it receives the
authoritative label set from the controller and caches it in the node's local SQLite state.
Label changes are committed to the controller's SQLite database.

Use the labels with Swarm-style hard placement constraints:

```yaml
services:
  api:
    image: example/api:latest
    deploy:
      replicas: 2
      placement:
        constraints:
          - node.labels.region == cn-north
          - node.labels.disk == nvme
```

When a label change makes a running task violate a hard constraint, Swarmlite stops that task
through the normal drain/remove flow and schedules its replacement on an eligible live node. If
no node matches, the service remains under-replicated until one does; constraints are never
silently ignored.

## Controller lifecycle

The controller identity is immutable for the lifetime of a cluster. Swarmlite has no promotion,
demotion, election, or automatic failover path. Back up the controller's `swarmlite.sqlite` file.
To move the controller, stop every node, initialize a new cluster on the replacement controller,
rejoin the agents with the new token, and redeploy the original Stack files. Existing matching
workload containers can be adopted through the recovery workflow below.

## Gateway and HTTPS

A node with its Gateway enabled makes `serve` create or start a separate `swarmlite-gateway`
container. The
container has `restart=unless-stopped`; stopping, crashing, or upgrading the Swarmlite process
does not stop Caddy. The first node is always a gateway and additional gateways are opt-in. The
controller discovers active gateways and publishes routing configuration automatically. Gateways
listen on `:80` and `:443` by default, so normal public HTTPS needs no listener configuration.
Advanced deployments can override the listeners with repeated `--gateway-listen` options at init.

The default gateway image is `ghcr.io/gfreezy/swarmlite-caddy:latest`. Select another
registry and tag during initialization, or roll the cluster to another image later:

```bash
swarmlite init --gateway-image registry.example.com/swarmlite-caddy:1.0.0
swarmlite config set gateway-image registry.example.com/swarmlite-caddy:1.1.0
```

The image reference is replicated as cluster configuration and returned by
`swarmlite config get`. Every gateway node pulls the new image and recreates its Caddy container
after receiving the update. The pull completes before the existing container is removed, and
the `/data` and `/config` volumes are retained.

Keep `services` compatible with Docker Compose/Swarm and put routing under `x-swarmlite`. Stack
files do not need a top-level `version`:

```yaml
services:
  api:
    image: example/api:latest

x-swarmlite:
  tls: serve
  http: redirect
  http_routes:
    - hostnames: [example.com]
      rules:
        - matches:
            - path: /api
          rewrite:
            strip_prefix: true
          backend:
            service: api
            port: 8080

        - matches:
            - path: /openai
              type: prefix
              ignore_case: true
          rewrite:
            replace_prefix: /
          backend:
            host: api.openai.com
            port: 443
            protocol: https
            preserve_host: false
```

`backend.service` references a service in the same Stack and is checked locally during deployment,
without an external validation service or network request. Swarmlite allocates the required task
port automatically. `backend.host` targets an external DNS name or IP. Both variants support
`preserve_host`, and `protocol` is `http`, `https`, or `h2c`.

Path matches support `exact`, `prefix` (the default), and RE2-compatible `regex`; `ignore_case`
defaults to `false`. Rewrites support exactly one of `strip_prefix`, `replace_prefix`, or
`replace_path`. Multiple match entries in one rule are OR, while a rule without `matches` is the
hostname fallback. Matching precedence is exact, longest prefix, regex, then fallback.

`tls` is `serve|disabled`; `http` is `redirect|serve|disabled`. Each route may override the
top-level defaults. `http: redirect` requires `tls: serve`. See the complete example at
[examples/routing-all.yaml](examples/routing-all.yaml).

VS Code/YAML Language Server completion is available from
[crates/swarmlite-stack/schema/stack.schema.json](crates/swarmlite-stack/schema/stack.schema.json).
The Schema enumerates every supported `services` field and rejects unsupported keys; see
[examples/services-all.yaml](examples/services-all.yaml) for all accepted service forms.
Add this first line to a Stack file if it is not already associated in editor settings:

```yaml
# yaml-language-server: $schema=./crates/swarmlite-stack/schema/stack.schema.json
```

The complete Stack parser, service model, routing validation, precedence, and Caddy JSON renderer
live in the independent `swarmlite-stack` crate. The main orchestrator only persists normalized
service specs and resolves internal service references to healthy task addresses.

Caddy keeps certificates in a cluster-specific Docker volume mounted at `/data`, and the last
accepted runtime configuration in another volume mounted at `/config`. It starts with `--resume`,
so it can restore the last routes even when the controller is unavailable. Total loss of
Swarmlite data does not remove either volume.

The gateway image includes and automatically enables `caddy.storage.swarmlite`. Local
`FileStorage` remains authoritative, while certificate objects are copied to the generic KV API
and certificate issuance uses its distributed locks. This normally lets additional gateway nodes
reuse an existing certificate instead of applying for another one. The controller keeps its fixed
URL current in the heartbeat-delivered Gateway configuration; the cluster token is supplied only
through the container environment and only its SHA-256 fingerprint is stored in a recovery label.

If Swarmlite or its KV state is unavailable, Caddy immediately falls back to its
local certificate data and local lock. Existing HTTPS traffic continues; gateways may apply for
duplicate certificates until coordination returns.

The gateway admin API listens inside the container on `0.0.0.0:2019`, but host port 2019 is
published only on `127.0.0.1`. The local Swarmlite node atomically loads the complete Caddy
configuration received in its heartbeat response and reports the applied generation to the
controller. The generation changes only when the rendered Caddy configuration changes. Gateway
traffic ports are published on all host interfaces.

Disabling the Gateway intentionally stops the container and deletes both its container and
persistent volumes. Enabling it again therefore starts with empty Caddy data. Container
replacement caused by an image, listener, or advertise-address change retains the volumes. Any
configured image must contain `caddy.storage.swarmlite`. See
[caddy-storage/README.md](caddy-storage/README.md).

During rolling updates, old healthy tasks remain routable until replacements are healthy and all
active gateways acknowledge the new routing configuration.

## Generic KV service

The authenticated controller KV API has no Caddy, certificate, or TLS semantics. Integrations
choose their own keys and values. Values are opaque base64 data and mutations use last-write-wins
ordering from the single controller's SQLite transaction sequence.

- `GET`, `PUT`, and `DELETE /v1/kv`
- `GET /v1/kv/keys`
- `GET /v1/kv/stat`
- `POST /v1/kv/locks/{acquire,renew,release}`

Consumers that treat it as an optional cache should continue locally when it is unavailable.
Request and response bodies are documented in [docs/kv-api.md](docs/kv-api.md).

## Persistence and extreme recovery

On the controller, `swarmlite.sqlite` persists local node settings together with cluster settings,
member Gateway switches and labels, stacks, service specifications, desired task assignments, ports, drain
deadlines, and dedicated KV object and lock tables. KV writes do not advance the orchestration
generation. Heartbeat liveness, resources, and observed container state are rebuilt from agent
heartbeats.

Every managed workload container carries the minimal labels needed to collect it after total
control-plane loss: cluster, task, stack, service, slot, revision, normalized spec hash, and
published ports. The labels do not contain the full stack. Keep the original stack file
separately.

The independent Caddy container is also recoverable, but is deliberately not labeled as a task.
It carries these labels:

- `io.swarmlite.managed=true`
- the stable `io.swarmlite.cluster_id`
- `io.swarmlite.system=true` and `io.swarmlite.component=gateway`
- `io.swarmlite.advertise_address`
- `io.swarmlite.gateway_image`, `io.swarmlite.gateway_listen`, and
  `io.swarmlite.gateway_schema`
- `io.swarmlite.gateway_token_sha256`, never the token itself

They let recovery identify the cluster, restore the Gateway switch and listener settings, and keep
using the existing Caddy image without mistaking the container for a stack service.

Stop every old `swarmlite serve`, then rebuild the control plane on a machine that still has local
cluster state or managed containers:

```bash
swarmlite init --recover
swarmlite serve
```

Recovery detects the old cluster ID, including when Caddy is the only remaining managed
container, archives stale SQLite state under `recovery-backup/`, creates a fresh single-controller
control plane, and rotates the join token. Legacy `local.redb` and Raft data are archived too. It
never deletes or changes a container.
On another recovered node, join detects the labeled local Caddy container and restores its Gateway
switch automatically. Rejoin and serve other nodes using the new
token, then deploy the same stack name and file:

```bash
swarmlite deploy --name demo --file stack.yaml
```

Matching containers are adopted by cluster ID, stack, service, slot, and spec hash. Running
containers stay running; matching stopped containers are started in place. Unmatched containers
remain unclaimed. `swarmlite status` reports `recovery.awaiting_adoption` and
`recovery.conflicting_slots`.

## Runtime and networking

Detected runtime sockets include Docker, OrbStack, Podman, and rootless Podman. Override
detection when necessary:

```bash
swarmlite serve --runtime podman --runtime-socket /run/podman/podman.sock
```

Every served node runs the agent, so runtime socket access is required everywhere and is
equivalent to root privileges. There is no cross-node container network. The controller allocates
host ports and gateways connect to `node-advertise-address:allocated-host-port`.

## Current limitations

- Linux Docker and Podman nodes are the intended targets.
- No overlay networking, service VIP, routing mesh, or cross-node DNS.
- Only replicated services are supported; `deploy.mode: global` is rejected.
- No Compose `build`, `configs`, `secrets`, resource reservations, or autoscaling.
- Named volumes and bind mounts remain node-local.
- Gateway routing intentionally supports the documented host/path/rewrite/backend model, not
  arbitrary Caddy handlers.
- The controller API is HTTP; use a trusted private network or terminate TLS in front of it.
- The controller is a single point of control-plane availability and cannot be changed in place.

## Test

```bash
cargo fmt --all --check
cargo test --locked
cargo clippy --all-targets --all-features -- -D warnings
(cd caddy-storage && go test ./...)
```
