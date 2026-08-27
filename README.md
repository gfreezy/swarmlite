# Swarmlite

Swarmlite is a small Rust container orchestrator for machines in one LAN or region. It provides a
single control plane, multi-node scheduling, rolling deployments, service logs, placement rules,
and optional HTTPS routing without the operational weight of Kubernetes.

> [!IMPORTANT]
> Swarmlite is an MVP, not a drop-in replacement for Docker Swarm or Kubernetes. It has one fixed
> Controller and intentionally has no overlay network, routing mesh, or automatic failover.

New here? Follow [Quick start](#quick-start). For an existing cluster, jump to
[node management](#cluster-setup-and-node-management), [Stack operations](#deploying-and-operating-stacks),
[HTTPS routing](#stack-files-and-https-routing), or [recovery](#persistence-backup-and-recovery).

## Quick start

This path creates a single-node Linux cluster and deploys Nginx on host port `8088`.

### 1. Install

Run the installer on a Linux server with systemd. It reuses Docker or Podman when available and
installs Docker when neither is present.

```bash
curl -fsSL https://github.com/gfreezy/swarmlite/releases/latest/download/install.sh | sudo sh
```

### 2. Initialize the first node

```bash
sudo swarmlite init
sudo systemctl start swarmlite
sudo swarmlite status
```

The initialized node is permanently the cluster Controller. It also runs an Agent and has its
Gateway enabled by default.

### 3. Deploy an application

Create `swarmlite.yaml`:

```yaml
services:
  web:
    image: nginx:1.29-alpine
    ports:
      - "8088:80"
    deploy:
      replicas: 1

x-swarmlite:
  name: demo
```

Deploy it and verify the result:

```bash
sudo swarmlite deploy
sudo swarmlite ps demo
curl http://127.0.0.1:8088
```

`deploy` waits for the desired state to become healthy. Add `--detach` when you only want to wait
until the Controller has accepted the deployment.

### 4. Operate the application

Services use the qualified name `STACK.SERVICE`, such as `demo.web`:

```bash
sudo swarmlite ls
sudo swarmlite inspect demo.web
sudo swarmlite logs --tail 200 demo.web
sudo swarmlite logs --tail 200 demo.web.1
sudo swarmlite scale demo.web=3
sudo swarmlite restart demo.web
sudo swarmlite rm demo
```

### 5. Add another node

Install Swarmlite on the new machine. On the Controller, print its generated join command:

```bash
sudo swarmlite join-token
```

Run the printed command with `sudo` on the new machine, then start its service:

```bash
sudo swarmlite join http://10.0.0.21:17080 --token '<generated-token>'
sudo systemctl start swarmlite
```

The new node runs an Agent. It is not a Controller or Gateway unless `--gateway` is supplied while
joining or the Gateway is enabled later.

## Is Swarmlite a good fit?

Swarmlite is designed for small deployments where all machines can reach one another directly.
It is a good fit when you want:

- a compact orchestrator for one trusted LAN or region;
- Compose-style application definitions;
- replicated services, placement constraints, and rolling updates;
- optional Caddy-based HTTP and HTTPS routing;
- Docker or Podman nodes managed through one CLI.

Choose a different orchestrator when you require control-plane high availability, an overlay
network, service VIPs, cross-node DNS, autoscaling, or the broader Kubernetes ecosystem.

## Architecture and core concepts

Every machine runs the same `swarmlite serve` process. Its fixed cluster role determines which
components are active:

| Component | Runs where | Responsibility |
| --- | --- | --- |
| Controller | The node created with `init` | Stores desired state, schedules tasks, and exposes the API |
| Agent | Every node | Reconciles assigned containers with Docker or Podman |
| Gateway | Enabled per node | Runs Caddy and publishes HTTP/HTTPS routes |
| Stack | Cluster-wide | Groups services from one Compose-style YAML file |
| Service | Inside a Stack | Defines an image, replicas, placement, ports, and update behavior |

The Controller identity is immutable for the lifetime of the cluster. Swarmlite has no promotion,
demotion, election, or automatic failover path.

There is no cross-node container network. The Controller allocates host ports, and Gateways reach
tasks through `node-advertise-address:allocated-host-port`. Named volumes and bind mounts remain
local to their node.

## Installation and lifecycle

### Linux server

The one-command installer:

- detects an existing Docker or Podman installation;
- installs Docker when no supported runtime is present;
- downloads and verifies the matching Swarmlite release;
- installs the CLI, systemd unit, and runtime configuration;
- enables the service for future boots without starting an uninitialized node.

```bash
curl -fsSL https://github.com/gfreezy/swarmlite/releases/latest/download/install.sh | sudo sh
```

Runtime selection defaults to `auto`: reuse the previous selection, then prefer installed Docker,
then installed Podman, and finally install Docker. Select rootful Podman explicitly with:

```bash
curl -fsSL https://github.com/gfreezy/swarmlite/releases/latest/download/install.sh \
  | sudo sh -s -- --runtime podman
```

Docker installation uses the official repositories on Ubuntu, Debian, Fedora, RHEL, and CentOS.
Podman installation uses the distribution package manager (`apt`, `dnf`, `yum`, `zypper`, or
`pacman`) and enables the rootful Podman API socket.

The system installation uses these paths:

| Path | Purpose |
| --- | --- |
| `/usr/local/bin/swarmlite` | CLI and node process |
| `/var/lib/swarmlite` | Node identity and durable SQLite state |
| `/etc/swarmlite/runtime.env` | Installed data directory, runtime, and socket |
| `/etc/systemd/system/swarmlite.service` | systemd unit |

Explicit CLI options and process environment variables override the installed configuration.

Common service commands are:

```bash
sudo systemctl start swarmlite
sudo systemctl restart swarmlite
sudo systemctl status swarmlite
sudo journalctl -u swarmlite -f
```

Upgrade to the latest release using the same installer and checksum verification flow. On Linux,
run the command as root so it can update the systemd unit and restart an existing node:

```bash
sudo swarmlite upgrade
```

Install a specific release when needed:

```bash
sudo swarmlite upgrade --version v0.2.0
```

### macOS ARM64 CLI

The macOS installer installs only the CLI. It requires Apple silicon and an accessible
Docker-compatible Unix socket; OrbStack is the recommended local runtime.

Run it without `sudo` so runtime detection uses the current user's environment:

```bash
curl -fsSL https://github.com/gfreezy/swarmlite/releases/latest/download/install.sh | sh
```

The installer probes `$HOME/.orbstack/run/docker.sock`, `/var/run/docker.sock`, and
`$HOME/.docker/run/docker.sock`. It asks for `sudo` only when `/usr/local/bin` is not writable.

### Uninstall

On Linux, remove the service, CLI, and installed configuration while preserving node data:

```bash
curl -fsSL https://github.com/gfreezy/swarmlite/releases/latest/download/install.sh \
  | sudo sh -s -- --uninstall
```

To also delete `/var/lib/swarmlite`, including the node database and recovery state, explicitly
add `--purge`:

```bash
curl -fsSL https://github.com/gfreezy/swarmlite/releases/latest/download/install.sh \
  | sudo sh -s -- --uninstall --purge
```

Neither form removes Docker, Podman, or managed containers. On macOS, remove only the CLI with:

```bash
curl -fsSL https://github.com/gfreezy/swarmlite/releases/latest/download/install.sh \
  | sh -s -- --uninstall
```

## Cluster setup and node management

Commands in this section assume the Linux system installation and therefore use `sudo`. Omit it
when running Swarmlite in a user-owned data directory for local development.

### Initialize a cluster

Initialize the first node and start the installed service:

```bash
sudo swarmlite init
sudo systemctl start swarmlite
```

The Controller API listens on TCP port `17080` by default. Select another port during
initialization with `--controller-port` when necessary.

The first node has its Gateway enabled by default. Initialize without one when another node will
provide ingress:

```bash
sudo swarmlite init --no-gateway
```

When running directly rather than through systemd, use foreground mode:

```bash
swarmlite init
swarmlite serve
```

Swarmlite detects Docker or Podman and selects the address used by the operating system's default
route. When detection cannot choose a reachable address, set it during `init` or `join` so it is
persisted before the service starts:

```bash
sudo swarmlite init --advertise-address 10.0.0.21
```

### Join nodes

Print a reusable join command on the Controller:

```bash
sudo swarmlite join-token
```

Run it on a machine where the installer has already completed:

```bash
sudo swarmlite join http://10.0.0.21:17080 --token '<generated-token>'
sudo systemctl start swarmlite
```

Enable the Gateway while joining when this node should also accept ingress traffic:

```bash
sudo swarmlite join http://10.0.0.21:17080 \
  --token '<generated-token>' \
  --gateway
```

### Manage Gateways

Read or change the Gateway switch after a node has joined:

```bash
sudo swarmlite gateway status node-a
sudo swarmlite gateway enable node-a
sudo swarmlite gateway disable node-a
```

The cluster may temporarily have no Gateway, but deploying a Stack with HTTP routes is rejected
until at least one is enabled.

> [!WARNING]
> Disabling a Gateway deletes its Caddy container and persistent volumes, including its local
> certificate data. Image, listener, and advertise-address replacements retain those volumes.

### Labels and placement

Set initial labels while initializing or joining a node:

```bash
sudo swarmlite init --label region=cn-east --label disk=nvme
sudo swarmlite join http://10.0.0.21:17080 \
  --token '<generated-token>' \
  --label region=cn-east
```

After the node has joined, its labels belong to cluster state and are changed through the
Controller:

```bash
sudo swarmlite node label get node-a
sudo swarmlite node label set node-a region cn-north
sudo swarmlite node label remove node-a disk
```

Use labels with Swarm-style hard placement constraints:

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

When a label change makes a running task violate a constraint, Swarmlite drains that task and
schedules a replacement on an eligible live node. If no node matches, the service remains
under-replicated; constraints are never silently ignored.

`serve` does not accept label arguments. Agent heartbeats receive the authoritative label set from
the Controller and cache it in local SQLite state.

### Private registries

Store registry credentials once on the Controller. The password or token is read only from
standard input, saved in the protected control-plane SQLite state, and synchronized to joined
Agents:

```bash
printf '%s' "$GHCR_TOKEN" | sudo swarmlite registry login ghcr.io \
  --username github-user --password-stdin
```

When running the command away from a configured node, pass the Controller and cluster token:

```bash
printf '%s' "$GHCR_TOKEN" | swarmlite registry login ghcr.io \
  --username github-user --password-stdin \
  --controller http://controller.example:17080 --token "$SWARMLITE_TOKEN"
```

## Deploying and operating Stacks

Deploy or update a Stack from a Compose-style file:

```bash
sudo swarmlite deploy
```

`deploy` reads `swarmlite.yaml` from the current directory by default. Use `--compose-file` (or
`-c`) to select another file. Set the default Stack name in that file; a command-line name remains
available as an override:

```yaml
x-swarmlite:
  name: demo
```

```bash
swarmlite deploy                    # deploy as demo
swarmlite deploy temporary-preview  # deploy the same file under an explicit name
```

The CLI stores the Controller URL and cluster token in node state, so normal workload commands do
not need connection flags. All commands are cluster-scoped and use flat action names:

```bash
sudo swarmlite ls
sudo swarmlite ls demo
sudo swarmlite ps demo
sudo swarmlite ps demo.web
sudo swarmlite inspect demo.web
sudo swarmlite logs --tail 200 demo.web
sudo swarmlite logs --follow demo.web
sudo swarmlite scale demo.web=3
sudo swarmlite restart demo.web
sudo swarmlite rm demo
```

### Operate a cluster over SSH

Management commands accept an SSH Controller URL. The CLI reads the protected connection settings
on the remote node, starts a temporary OpenSSH tunnel, and closes it when the command exits. The
Stack file remains local:

```bash
swarmlite deploy --controller ssh://deploy@server
swarmlite ps --controller ssh://deploy@server demo
swarmlite logs --controller ssh://deploy@server --follow demo.web
```

The SSH URL supports `ssh://[user@]host[:port]`; its port is the SSH port. Host aliases and options
from `~/.ssh/config`, including `ProxyJump`, are handled by the system `ssh` executable. SSH mode is
for management commands only; nodes still use a persistent HTTP Controller URL when joining.

The remote CLI must be the same version and must be able to read `/var/lib/swarmlite`. Root SSH
works directly. For another SSH user, allow only the machine-readable connection command without a
password prompt:

```sudoers
deploy ALL=(root) NOPASSWD: /usr/local/bin/swarmlite connection-info --json
```

The CLI invokes `sudo -n`, transfers the cluster token only through the encrypted SSH stdout, and
keeps it out of process arguments and logs. Inspect the same information locally when needed:

```bash
sudo swarmlite connection-info --json
```

`scale`, `restart`, and `rm` use the same deployment scheduler as `deploy`. They wait for
convergence by default and support `--detach`. `restart` increments the Service revision and
performs the configured rolling replacement.

### Deployment behavior

`deploy` waits until every desired replica has been applied by its Agent and is healthy, and until
obsolete tasks have been removed. Runtime failures include their service, node, task, and execution
phase, and make the command exit non-zero.

Deployments that do not become healthy within five minutes are marked `timed_out`. The Controller
accepts only one active deployment for a given Stack name; a concurrent update to the same Stack
returns `409 Conflict`. Different Stacks may deploy independently.

Control image pulls per Service with the Compose-style `pull_policy` field:

```yaml
services:
  api:
    image: example/api:latest
    pull_policy: always
```

Supported values are `always`, `missing` (the default), `if_not_present` (an alias for `missing`),
and `never`. `missing` uses a cached image when possible, except that an omitted tag or the
`latest` tag is refreshed when a task is created. Deploying an unchanged Stack rolls Services
whose policy requires a refresh (`always`, or `missing` with an effective `latest` tag).

During rolling updates, old healthy tasks remain routable until replacements are healthy and all
active Gateways acknowledge the new routing configuration.

<details>
<summary>How log streaming works</summary>

Agents maintain an authenticated outbound long-poll for data-session control commands, so they do
not need an inbound listener. Bulk data uses a separate WebSocket connection to the Controller.
The Controller issues short-lived, session-scoped tokens and relays bounded binary frames without
converting payloads to UTF-8.

Multiple tasks are multiplexed by stream ID. `logs` supports snapshots and `--follow`, limits
`--tail` to 10,000 lines, and selects at most 64 tasks. Buffers are bounded across the Runtime,
Agent, Controller, and CLI. Backpressure pauses upstream reads instead of silently dropping data;
a connection or local output blocked for 30 seconds terminates that log session.

Pass a Service name to stream all of its tasks, or copy the `STACK.SERVICE.SLOT` name or Task ID
shown by `swarmlite ps` to select one task. During a rolling update, the old and new task can
temporarily share a slot name; use the Task ID in that case.

</details>

## Stack files and HTTPS routing

Stack files keep services compatible with Docker Compose/Swarm and place Swarmlite routing under
`x-swarmlite`. A top-level `version` is not required.

VS Code and YAML Language Server completion is available through
[`stack.schema.json`](https://raw.githubusercontent.com/gfreezy/swarmlite/main/crates/swarmlite-stack/schema/stack.schema.json).
Associate a file with the schema using:

```yaml
# yaml-language-server: $schema=https://raw.githubusercontent.com/gfreezy/swarmlite/main/crates/swarmlite-stack/schema/stack.schema.json
```

See [`examples/services-all.yaml`](examples/services-all.yaml) for accepted Service fields and
[`examples/routing-all.yaml`](examples/routing-all.yaml) for complete routing examples.

### Publish HTTP and HTTPS routes

Gateway nodes listen on `:80` and `:443` by default. A basic internal route looks like this:

```yaml
services:
  api:
    image: example/api:latest
    expose:
      - "8080"

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
```

`backend.service` references a Service in the same Stack. When that Service declares exactly one
TCP target across `expose` and `ports`, `backend.port` is inferred. Multiple targets require an
explicit `backend.port`, and an explicit value must name a declared target. Swarmlite allocates a
host port for each routed task. `backend.host` routes to an external DNS name or IP and always
requires `port`. Both backend forms support `preserve_host`; `protocol` may be `http`, `https`, or
`h2c`.

For replicated routed Services, prefer `expose` and let Swarmlite allocate task ports. An explicit
`ports.published` value is node-local: only one task per protocol can use it on a node, replicas
need enough distinct nodes, and a `start-first` update may need a spare node. Use `stop-first` when
the fixed host port must be released before its replacement starts.

Path matches support `exact`, `prefix` (the default), and RE2-compatible `regex`. Rewrites support
exactly one of `strip_prefix`, `replace_prefix`, or `replace_path`. Multiple matches in one rule
are OR conditions. A rule without matches is the hostname fallback. Precedence is exact, longest
prefix, regex, then fallback.

`tls` is `serve|disabled`, and `http` is `redirect|serve|disabled`. Each route may override the
top-level defaults. `http: redirect` requires `tls: serve`.

### Gateway image and listeners

The default image is `ghcr.io/gfreezy/swarmlite-caddy:latest`. Choose another image during
initialization or roll all Gateways later:

```bash
sudo swarmlite init --gateway-image registry.example.com/swarmlite-caddy:1.0.0
sudo swarmlite config set gateway-image registry.example.com/swarmlite-caddy:1.1.0
sudo swarmlite config get
```

Every Gateway pulls the updated image before replacing its Caddy container. Its `/data` and
`/config` volumes are retained. Advanced installations can override the default listeners with
repeated `--gateway-listen` options during `init`. Custom images must contain
`caddy.storage.swarmlite`; see [`caddy-storage/README.md`](caddy-storage/README.md).

<details>
<summary>Gateway persistence and certificate coordination</summary>

Caddy stores certificates in a cluster-specific volume mounted at `/data` and its last accepted
configuration in a volume mounted at `/config`. It starts with `--resume`, so existing routes can
survive temporary Controller unavailability.

The gateway image enables `caddy.storage.swarmlite`. Local Caddy storage remains authoritative,
while certificate objects are copied to the Controller KV API and certificate issuance uses
distributed locks. Additional Gateways can normally reuse an existing certificate. If the KV API
is unavailable, Caddy falls back immediately to local certificate data and local locks; existing
HTTPS traffic continues, though Gateways may request duplicate certificates until coordination
returns.

The Caddy admin API is reachable on host address `127.0.0.1:2019`; traffic ports are published on
all host interfaces. Each node atomically loads the complete configuration and reports its applied
generation to the Controller.

</details>

## Persistence, backup, and recovery

Each node stores durable state in one `swarmlite.sqlite` database. The Linux installer places it at
`/var/lib/swarmlite/swarmlite.sqlite`; foreground user mode defaults to
`$XDG_STATE_HOME/swarmlite` or `$HOME/.local/state/swarmlite`.

Agent nodes use local-state tables. The Controller database also stores cluster settings, member
Gateway switches and labels, Stacks, deployment outcomes, normalized Service specifications,
desired task assignments, allocated ports, drain deadlines, registry credentials, and KV data.
Heartbeat liveness and observed runtime state are rebuilt from Agents.

### Back up the Controller

Back up the Controller's `swarmlite.sqlite` file and keep the original Stack files separately. The
Controller is a single point of control-plane availability and cannot move in place.

To intentionally replace it, stop every node, initialize a new cluster on the replacement machine,
rejoin Agents with the new token, and redeploy the original Stack files.

### Recover after control-plane loss

Stop every old `swarmlite serve` process. On a machine that still has local cluster state or
managed containers, run:

```bash
sudo systemctl stop swarmlite
sudo swarmlite init --recover
sudo systemctl start swarmlite
```

Recovery detects the previous cluster ID, archives stale state under `recovery-backup/`, rebuilds a
single-controller control plane, and rotates the join token. It never deletes or changes a
container. Legacy `local.redb`, Raft, and split SQLite state are archived as well. Print the new
join command, rejoin the other nodes, and redeploy the same Stack names and files:

```bash
sudo swarmlite join-token
sudo swarmlite deploy --compose-file stack.yaml demo
```

Matching containers are adopted using their cluster, Stack, Service, slot, normalized spec hash,
ports, and Service revision. Matching stopped containers start in place; old or unmatched
containers remain unclaimed. Inspect `recovery.awaiting_adoption` and
`recovery.conflicting_slots` with:

```bash
sudo swarmlite status
```

<details>
<summary>Container identity retained for extreme recovery</summary>

Managed workload containers retain labels for cluster, task, Stack, Service, slot, revision,
normalized spec hash, and published ports. They do not contain the full Stack definition.

The independent Gateway container carries stable cluster and component identity, advertise
address, image, listener, schema, and token-fingerprint labels. It never stores the token itself.
These labels allow recovery to identify a cluster and restore Gateway settings even when Caddy is
the only managed container left.

Recovery first restores the highest valid Service revision, then adopts only containers at that
revision. This allows an interrupted rolling update to complete without immediately replacing a
newer replica with an older one.

</details>

## Runtime, networking, and security

Detected sockets include Docker, OrbStack, rootful Podman, and rootless Podman. Override detection
when necessary:

```bash
swarmlite serve --runtime podman --runtime-socket /run/podman/podman.sock
```

Every Agent requires access to its runtime socket. Treat that access as equivalent to root
privileges. The Controller API uses HTTP, so deploy it on a trusted private network or terminate
TLS in front of it.

The advertised node address must be reachable by other nodes and Gateways. Swarmlite does not
provide firewall rules, NAT traversal, an overlay network, or cross-node DNS.

## Generic KV API

The authenticated Controller KV API stores opaque base64 values with last-write-wins ordering from
the single Controller's SQLite transaction sequence. It has no built-in Caddy, certificate, or TLS
semantics.

Available endpoints are:

- `GET`, `PUT`, and `DELETE /v1/kv`
- `GET /v1/kv/keys`
- `GET /v1/kv/stat`
- `POST /v1/kv/locks/{acquire,renew,release}`

Optional-cache consumers should continue locally when it is unavailable. Request and response
formats are documented in [`docs/kv-api.md`](docs/kv-api.md).

## Command reference

Run `swarmlite COMMAND --help` for complete arguments.

```text
init                 initialize a single-controller cluster
join                 configure another node from cluster settings
join-token           print the generated join command
connection-info      print the stored Controller address and cluster token
upgrade              install the latest or a selected GitHub Release
serve                run this node's fixed components
config get|set       read or update cluster-wide settings
gateway status|enable|disable
                     read or update one node's Gateway switch
node label get|set|remove
                     read or update one node's placement labels
registry login       store private registry credentials
deploy               deploy or update a Stack
ls [STACK]           list Services
ps TARGET            list tasks for a Stack or Service
inspect SERVICE      inspect a Service
logs SERVICE|TASK_NAME|TASK_ID
                     stream container logs
scale SERVICE=N      scale replicated Services
restart SERVICE      roll a Service
rm STACK             remove Stacks
status               inspect cluster state
```

There are no separate public `controller`, `agent`, or `gateway` runtime commands.

## Current limitations

- Linux Docker and Podman nodes are the intended production targets.
- The Controller is a single point of control-plane availability and cannot change in place.
- There is no overlay network, service VIP, routing mesh, or cross-node DNS.
- Only replicated Services are supported; `deploy.mode: global` is rejected.
- Compose `build`, `configs`, `secrets`, resource reservations, and autoscaling are not supported.
- Named volumes and bind mounts remain node-local.
- `stats` and interactive `exec` are not implemented yet.
- Gateway routing supports the documented host/path/rewrite/backend model, not arbitrary Caddy
  handlers.
- The Controller API is HTTP and should remain on a trusted network or behind TLS termination.

## Development

Rust 1.97 or newer is required:

```bash
cargo build --release --locked
```

Run the project checks with:

```bash
cargo fmt --all --check
cargo test --locked
cargo clippy --all-targets --all-features -- -D warnings
(cd caddy-storage && go test ./...)
```

The project [`Dockerfile`](Dockerfile) builds Swarmlite. Gateway nodes pull the prebuilt
`ghcr.io/gfreezy/swarmlite-caddy:latest` image, so Go is required only when developing or
publishing the Gateway image.

GitHub Actions builds release archives and SHA-256 checksums for Linux AMD64, Linux ARM64, and
macOS ARM64. Tag pushes publish those artifacts together with the installer and systemd unit in a
GitHub Release.
