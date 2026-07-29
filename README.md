# Swarmlite

Swarmlite is a small Rust container orchestrator for one LAN or region. Every machine runs the
same `swarmlite serve` command; a node's role set decides which components the node maintains.

Roles are composable:

- `agent` runs containers. It is mandatory on every node and cannot be removed.
- `controller` runs the API and a Raft voter.
- `gateway` maintains an independent Caddy container and publishes service routes to it.

This is an MVP, not a drop-in replacement for Docker Swarm or Kubernetes. It intentionally has
no overlay network or routing mesh.

## Build

Rust 1.97 or newer is required to build Swarmlite:

```bash
cargo build --release --locked
```

The project [Dockerfile](Dockerfile) builds Swarmlite. Gateway nodes automatically pull the
prebuilt `ghcr.io/swarmlite/swarmlite-caddy:latest` image, which contains Caddy plus the
Swarmlite storage module. A Docker-compatible runtime is therefore required on deployed nodes;
Go is needed only when developing or publishing the gateway image itself.

## Commands

```text
init                 initialize a standalone or HA cluster
join                 pull cluster settings and configure another node
serve                run this node's assigned components
config get|set       read or update cluster-wide settings
role get|set|add|remove
                     read or update one node's role set
deploy               deploy or update a stack
status               inspect cluster state
```

There are no separate public `controller`, `agent`, or `gateway` runtime commands.

## Quick start

Initialize and serve the first node:

```bash
swarmlite init --mode standalone
swarmlite serve
```

The first node receives `controller,agent,gateway`. Later standalone joins receive only `agent`
unless roles are requested explicitly.

`serve` detects Docker or Podman and automatically selects the address used by the operating
system's default route. Specify an address only when detection cannot choose a reachable one:

```bash
swarmlite serve --advertise-address 10.0.0.21
```

The override is persisted. Node identity, credentials, roles, CLI defaults, and the agent fence
are stored together in `local.redb`; Swarmlite does not maintain separate JSON state files. The
default data directory is `$XDG_STATE_HOME/swarmlite`, or `$HOME/.local/state/swarmlite`.

```bash
swarmlite --data-dir /var/lib/swarmlite serve
```

Deploy and inspect a stack using the saved controller URL and token:

```bash
swarmlite deploy --name demo --file examples/stack.yaml
swarmlite status
```

## Join nodes and assign roles

Print a join command on an initialized node:

```bash
swarmlite join-token
```

Run it on another machine, then start the same runtime command:

```bash
swarmlite join http://10.0.0.21:8080 --token '<generated-token>'
swarmlite serve
```

The default role allocation is:

| Mode | First node | Later automatic joins |
| --- | --- | --- |
| `standalone` | `controller,agent,gateway` | `agent` |
| `ha` | `controller,agent,gateway` | `controller,agent` until 3 controllers exist, then `agent` |

Gateway is never assigned automatically after `init`. A cluster must retain at least one gateway,
but gateway count has no upper limit. Request an exact role set during join when needed; `agent`
is added automatically:

```bash
swarmlite join http://10.0.0.21:8080 \
  --token '<generated-token>' \
  --roles gateway
```

An explicit join is all-or-nothing. It fails if its controller request would exceed the mode's
limit. Existing nodes keep their reserved roles while offline.

Read or change a joined node after startup:

```bash
swarmlite role get node-a
swarmlite role set node-a agent,gateway
swarmlite role add node-b controller
swarmlite role remove node-c controller
```

`set` replaces the role set except for mandatory `agent`; `add` and `remove` change only the
listed roles. Swarmlite refuses to remove the final controller or final gateway. To move a
controller, remove it from the old node before adding it to the new one. Removing the current
leader first commits the new role set, removes that voter, and lets the remaining voters elect a
new leader. To move a gateway, add the new gateway first, then remove the old one.

## HA

Initialize HA directly:

```console
first$ swarmlite init --mode ha
first$ swarmlite serve
```

The next two default joins receive `controller,agent`; later joins receive only `agent`. Three
controller voters tolerate one controller failure. A joining controller starts as a Raft learner
and is promoted after it starts and reports the assigned role.

A live standalone cluster can be promoted in place:

```bash
swarmlite config set mode ha
```

The controller deterministically assigns existing automatically joined agent nodes until the cluster
has three controllers. Future joins fill any remaining slots. Switching HA back to standalone is
not supported.

```bash
swarmlite config get
```

Cluster configuration and node roles are replicated through Raft. Controller addresses are sent
in heartbeats and persisted locally, so agents do not need a hand-written controller list.

## Gateway and HTTPS

A gateway role makes `serve` create or start a separate `swarmlite-gateway` container. The
container has `restart=unless-stopped`; stopping, crashing, or upgrading the Swarmlite process
does not stop Caddy. The first node is always a gateway and additional gateways are opt-in. The
controller discovers active gateways and publishes routing configuration automatically. Use
`--gateway-listen` during init to change the default `:80`:

```bash
swarmlite init --gateway-listen :80 --gateway-listen :443
```

The default gateway image is `ghcr.io/swarmlite/swarmlite-caddy:latest`. Select another
registry and tag during initialization, or roll the cluster to another image later:

```bash
swarmlite init --gateway-image registry.example.com/swarmlite-caddy:1.0.0
swarmlite config set gateway-image registry.example.com/swarmlite-caddy:1.1.0
```

The image reference is replicated as cluster configuration and returned by
`swarmlite config get`. Every gateway node pulls the new image and recreates its Caddy container
after receiving the update. The pull completes before the existing container is removed, and
the `/data` and `/config` volumes are retained.

Supported service labels under `deploy.labels` are:

- `swarmlite.gateway.enable=true`
- `swarmlite.gateway.host`, containing one host or a comma-separated host list
- `swarmlite.gateway.port`, the container HTTP port
- `swarmlite.gateway.scheme=http|https`, defaulting to `http`

Caddy keeps certificates in a cluster-specific Docker volume mounted at `/data`, and the last
accepted runtime configuration in another volume mounted at `/config`. It starts with `--resume`,
so it can restore the last routes even when no controller is available. Loss of quorum or total
loss of Swarmlite data does not remove either volume.

The gateway image includes and automatically enables `caddy.storage.swarmlite`. Local
`FileStorage` remains authoritative, while certificate objects are copied to the generic KV API
and certificate issuance uses its distributed locks. This normally lets additional gateway nodes
reuse an existing certificate instead of applying for another one. The leader keeps the module's
controller list current through Caddy's admin API; the cluster token is supplied only through the
container environment and only its SHA-256 fingerprint is stored in a recovery label.

If Swarmlite, its KV state, or Raft quorum is unavailable, Caddy immediately falls back to its
local certificate data and local lock. Existing HTTPS traffic continues; gateways may apply for
duplicate certificates until coordination returns.

The gateway admin API listens inside the container on `0.0.0.0:2019`, but host port 2019 is
published only on this node's detected or explicitly configured `advertise-address`. Controllers
push routes to `http://<advertise-address>:2019`; restrict that port to controller nodes on the
trusted cluster network. Gateway traffic ports are published on all host interfaces.

Removing the gateway role intentionally stops the container and deletes both its container and
persistent volumes. Adding the role again therefore starts with empty Caddy data. Container
replacement caused by an image, listener, or advertise-address change retains the volumes. Any
configured image must contain `caddy.storage.swarmlite`. See
[caddy-storage/README.md](caddy-storage/README.md).

During rolling updates, old healthy tasks remain routable until replacements are healthy and all
active gateways acknowledge the new routing configuration.

## Generic KV service

The authenticated controller KV API has no Caddy, certificate, or TLS semantics. Integrations
choose their own keys and values. Values are opaque base64 data and mutations use last-write-wins
ordering by `(physical_unix_ms, logical, replica_id)`.

- `GET`, `PUT`, and `DELETE /v1/kv`
- `GET /v1/kv/keys`
- `GET /v1/kv/stat`
- `POST /v1/kv/locks/{acquire,renew,release}`

Consumers that treat it as an optional cache should continue locally when it is unavailable.
Request and response bodies are documented in [docs/kv-api.md](docs/kv-api.md).

## Persistence and extreme recovery

Raft persists cluster settings, member roles, stacks, service specifications, desired task
assignments, ports, drain deadlines, controller identities, KV state, and Raft metadata.
Heartbeat liveness, resources, and observed container state are rebuilt from agent heartbeats.

Every managed workload container carries the minimal labels needed to collect it after total
control-plane loss: cluster, task, stack, service, slot, revision, normalized spec hash, and
published ports. The labels do not contain the full stack. Keep the original stack file
separately.

The independent Caddy container is also recoverable, but is deliberately not labeled as a task.
It carries these labels:

- `io.swarmlite.managed=true`
- the stable `io.swarmlite.cluster_id`
- `io.swarmlite.system=true` and `io.swarmlite.component=gateway`
- `io.swarmlite.advertise_address` and `io.swarmlite.gateway_bind_address`
- `io.swarmlite.gateway_image`, `io.swarmlite.gateway_listen`, and
  `io.swarmlite.gateway_schema`
- `io.swarmlite.gateway_token_sha256`, never the token itself

They let recovery identify the cluster, restore the gateway role and listener settings, and keep
using the existing Caddy image without mistaking the container for a stack service.

Stop every old `swarmlite serve`, then rebuild the control plane on a machine that still has local
cluster state or managed containers:

```bash
swarmlite init --recover
swarmlite serve
```

Recovery detects the old cluster ID, including when Caddy is the only remaining managed
container, archives stale `local.redb` and Raft data under `recovery-backup/`, creates a fresh
standalone control plane, and rotates the join token. It never deletes or changes a container.
On another recovered node, a join without explicit `--roles` detects the labeled local Caddy
container and restores its gateway role automatically. Rejoin and serve other nodes using the new
token, then deploy the same stack name and file:

```bash
swarmlite deploy --name demo --file stack.yaml
```

Matching containers are adopted by cluster ID, stack, service, slot, and spec hash. Running
containers stay running; matching stopped containers are started in place. Unmatched containers
remain unclaimed. `swarmlite status` reports `recovery.awaiting_adoption` and
`recovery.conflicting_slots`.

## Runtime and networking

Detected runtime sockets include Docker, Docker Desktop, Podman, and rootless Podman. Override
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
- Gateway routing supports host matching and HTTP/HTTPS upstreams, not arbitrary Caddy handlers.
- The controller API is HTTP; use a trusted private network or terminate TLS in front of it.
- Controller membership changes require a live Raft quorum.

## Test

```bash
cargo fmt --all --check
cargo test --locked
cargo clippy --all-targets --all-features -- -D warnings
(cd caddy-storage && go test ./...)
```
