# Swarmlite

Swarmlite is a small Rust container orchestrator for a single LAN or region. Every machine runs the same `swarmlite serve` command. A node initialized or assigned as a manager runs the controller, Raft, and local agent together; a worker runs only the agent.

All clusters use the same embedded OpenRaft control plane and local `redb` storage. One manager is the minimal setup; three managers provide HA and tolerate one manager failure. Caddy ingress is optional.

This is an MVP, not a drop-in replacement for Docker Swarm or Kubernetes. It intentionally has no overlay network or routing mesh.

## Build

Rust 1.97 or newer is required.

```bash
cargo build --release --locked
```

The binary is `target/release/swarmlite`. A multi-stage [Dockerfile](Dockerfile) is also included.

## Commands

The node lifecycle has three commands, plus cluster configuration management:

```text
init   create a Raft cluster
join   pull cluster settings and configure another node
serve  run the components assigned to this node
config get|set  read or update cluster-wide settings
```

There are no separate public `controller` or `agent` commands.

## Quick start

Initialize the first machine once, then serve it:

```bash
swarmlite init
swarmlite serve
```

`init` generates the cluster ID, stable node ID, and authentication token. `serve` detects the Docker or Podman socket and the address selected by the operating system's default route. If no reachable address can be detected, provide it once:

```bash
swarmlite serve --advertise-address 10.0.0.21
```

The override is saved in the node settings. Local node identity, credentials, CLI defaults,
and the agent fencing cursor are stored together in `local.redb`; Swarmlite does not maintain
separate JSON state files. The data directory defaults to `$XDG_STATE_HOME/swarmlite`, or
`$HOME/.local/state/swarmlite` when `XDG_STATE_HOME` is unset. A system service should normally use:

```bash
swarmlite --data-dir /var/lib/swarmlite serve
```

The manager count passed to `init` is one-time bootstrap data. The first successful `serve`
writes it into Raft and clears the bootstrap marker in `local.redb`. Joined nodes never persist
a local copy of the desired manager count; Raft remains the authoritative source.

Deploy and inspect a stack without repeating the controller URL or token:

```bash
swarmlite deploy --name demo --file examples/stack-standalone.yaml
swarmlite status
```

A one-manager cluster can have multiple workers. Node-local state is stored in `local.redb`,
while controller-only Raft state is stored in `raft/raft.redb`.

## Raft persistence boundary

Raft stores only state needed to reconstruct the desired cluster:

- cluster settings, stacks, and service specifications;
- desired task assignments, allocated ports, and drain deadlines;
- manager identities and pending manager reservations;
- Raft's own log, membership, term, vote, and snapshot metadata.

Heartbeat-derived state is intentionally disposable. Node liveness and resources, task observed state and container IDs, the current leader record, and request-deduplication history stay in memory. After a leader change, nodes rebuild that state through heartbeats; existing task assignments receive one node-timeout grace period before failover decisions are made.

## Recover a lost control plane

Every newly created task container carries the minimal recovery identity labels for its
cluster, task, stack, service, replica slot, service-spec hash, published ports, and task
revision. Runtime-management labels also mark the container as managed and retain its
service ID and stop grace period. Raft terms, state generations, cluster epochs, and claim
signatures are not stored as container labels. The labels do not contain the complete stack
file. Keep the original stack file separately.

Extreme recovery deliberately rebuilds the control plane instead of trying to restore an old
Raft membership. Stop every old `swarmlite serve` process, then run this on a machine that
still has local cluster state or managed containers:

```bash
swarmlite init --recover
swarmlite serve
```

`init --recover` detects the single old cluster ID, archives the previous `local.redb` and Raft
directory under `recovery-backup/`, prepares a fresh single-voter Raft,
and rotates the join token. If `local.redb` is unreadable, a single consistent cluster ID on the
managed containers is sufficient. Recovery never deletes or changes a container.

Join and serve the other machines with the new token. `join` may be run on an already
initialized node: when the target has the same cluster ID but a different token, it archives
the stale local control plane, assigns a fresh node and Raft identity, resets the local fence,
and leaves the runtime untouched. Once every machine has reported its containers, deploy the
same stack file and stack name:

```bash
swarmlite deploy --name demo --file stack.yaml
```

The controller adopts containers whose cluster ID, stack name, service name, replica slot,
and normalized service-spec hash match. Adoption preserves a running container and its
published ports; a matching stopped container is started in place. Containers that do not
match remain unclaimed and are never deleted merely because they are absent from the new
control plane. `swarmlite status` reports `recovery.awaiting_adoption` and duplicate logical
slots in `recovery.conflicting_slots`.

Normal `init` refuses to run when the local data directory, local Raft directory, controller
port, or local managed containers indicate an existing cluster. Recovery refuses mixed
cluster IDs and managed containers without the required cluster ID. These checks are local:
before recovery, make sure every old `serve` process is stopped to avoid two control planes or
agents managing the same workload. A per-node `serve.lock` prevents init or join from rewriting
local state while a local service is still running.

Existing containers continue running while the control plane is unavailable, but deploy,
rescheduling, failover, and ingress updates are unavailable until recovery completes.
Containers created by versions that predate the recovery labels cannot be adopted
automatically.

## Join another machine

Print a join command on an initialized node:

```bash
swarmlite join-token
```

Run the printed command on the new machine, then use the same runtime command as every other node:

```bash
swarmlite join http://10.0.0.21:8080 --token '<generated-token>'
swarmlite serve
```

`join` automatically:

- pulls the manager count, Caddy settings, and controller addresses;
- detects and persists the node address; `serve` detects the container runtime;
- receives a manager or worker assignment from the Raft leader;
- saves the node identity, role, token, and controller list locally.

With one configured manager, joined nodes are workers. With three or more, joined nodes receive available manager slots automatically.

## Three-manager HA

Initialize the first manager with an odd manager count:

```console
first$ swarmlite init --controllers 3
first$ swarmlite serve
```

Join and serve two more machines using the command printed by `join-token`:

```console
second$ swarmlite join http://10.0.0.21:8080 --token '<generated-token>'
second$ swarmlite serve

third$ swarmlite join http://10.0.0.21:8080 --token '<generated-token>'
third$ swarmlite serve
```

Each new manager starts as a Raft learner. The leader waits for it to catch up before promoting it to a voter. Raft RPC is authenticated with the cluster token and shares the controller port under `/internal/raft`; that port must be mutually reachable between managers.

Use 1, 3, or 5 managers. Three voters tolerate one failure; five tolerate two. Two managers are rejected because they still lose quorum after one failure.

Controller addresses are included in heartbeat responses and persisted by every node, so workers do not need a hand-written controller list.

## Update cluster configuration

Read the current configuration from the Raft leader:

```bash
swarmlite config get
```

Change the desired manager count after initialization:

```bash
swarmlite config set controllers 3
```

The value must be `1` or an odd number greater than or equal to `3`. The setting is replicated through Raft. Increasing it lets eligible workers fill the new manager slots through their normal heartbeats. Decreasing it makes the leader remove excess non-leader managers one at a time. The command changes the cluster-wide desired count; there are no per-node role commands.

Both commands use the controller address and token saved by `init` or `join`. From an uninitialized machine, pass them explicitly with `--controller` and `--token`.

## Optional Caddy ingress

Caddy is unnecessary when services publish host ports directly. Configure it during initialization when host-based ingress is needed:

```bash
caddy run --config examples/caddy.json

swarmlite init \
  --caddy-admin http://127.0.0.1:2019 \
  --caddy-listen :80
```

Use the same flags together with `--controllers 3` for a three-manager cluster. The settings are cluster-wide and automatically pulled by joined managers.

Supported service labels under `deploy.labels`:

- `swarmlite.ingress.enable=true`
- `swarmlite.ingress.host`, with one host or a comma-separated host list
- `swarmlite.ingress.port`, the container HTTP port
- `swarmlite.ingress.scheme=http|https`, defaulting to `http`

Only the leader publishes Caddy configuration. During rolling updates, old healthy tasks remain routable until replacements are healthy and every Caddy endpoint acknowledges the new routing configuration.

## Runtime and networking

Swarmlite detects these sockets automatically:

```text
Docker          /var/run/docker.sock
Docker Desktop  $HOME/.docker/run/docker.sock
Podman          /run/podman/podman.sock
Rootless Podman $XDG_RUNTIME_DIR/podman/podman.sock
```

Override detection when needed:

```bash
swarmlite serve --runtime podman --runtime-socket /run/podman/podman.sock
```

Runtime socket access is equivalent to root privileges. A manager role does not change this: every served node also runs its local agent.

There is no cross-node container network. The controller assigns host ports from each agent's configured range, and Caddy connects to:

```text
node-advertise-address:allocated-host-port
```

The controller API port and allocated node port range must be reachable inside the LAN.

## Implemented stack fields

- `services.*.image`
- `command`, `entrypoint`, `environment`
- short and long `ports`
- short and long `volumes`
- container `labels`
- `healthcheck`
- `stop_grace_period`
- `deploy.replicas`
- `deploy.placement.constraints`
- `deploy.update_config.parallelism` and `order`
- `deploy.labels`, including Swarmlite ingress labels

Supported constraints are `node.id`, `node.hostname`, and `node.labels.*` with `==` or `!=`.

## Current limitations

- Linux Docker and Podman nodes are the intended targets; Windows containers are not tested.
- No overlay networking, service VIP, routing mesh, or cross-node DNS.
- `deploy.mode: global` is rejected; only replicated services are supported.
- No Compose `build`, `configs`, `secrets`, resource reservations, or autoscaling.
- Named volumes and bind mounts remain node-local. Pin stateful services with placement constraints.
- Automatic task failover is intended for stateless services. Databases need their own replication and leader election.
- Private-registry authentication is not yet forwarded to image pulls.
- Ingress supports host matching and HTTP/HTTPS upstreams, but not arbitrary Caddy handlers.
- Every configured Caddy instance must already contain `apps.http.servers`.
- The API is HTTP. Use a trusted private network or terminate TLS in front of it.
- Adding managers requires an active Raft quorum. If every Raft copy is permanently lost, only containers carrying the recovery labels can be adopted into a new control plane; the original stack files are still required.

## Test

```bash
cargo fmt --all --check
cargo test --locked
cargo clippy --all-targets --all-features -- -D warnings
```
