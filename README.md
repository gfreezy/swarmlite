# Swarmlite

Swarmlite is a small Rust container orchestrator for a single LAN or region. It accepts a useful subset of Docker Swarm stack files, stores its durable control-plane state through an S3-compatible API, supports multiple active/standby controllers, reconciles containers through a node agent, and publishes Traefik HTTP Provider configuration.

This is an MVP, not a drop-in replacement for Docker Swarm or Kubernetes. In particular, it intentionally does not implement an overlay network or a routing mesh.

## Architecture

```text
                         S3 / R2
                    meta.json + snapshots
                       /             \
             Controller A          Controller B
                 Leader              Standby
                    \                 /
                     \               /
                      Agent heartbeat
                     /               \
              Docker node A      Docker node B

Traefik polls GET /v1/traefik and sends traffic to node-address:allocated-port.
```

Every controller points at the same bucket, prefix, and `cluster_id`. Leadership and state commits conditionally replace the same `meta.json` object with `If-Match`. Exactly one candidate succeeds. Every command sent to an agent carries the leader `term` and committed state `generation`; agents persist the highest values and reject older commands.

## Implemented features

- Multiple controller candidates with S3/R2 ETag CAS leases
- Fail-closed behavior when a controller cannot renew its lease
- Immutable JSON state snapshots with an atomic `meta.json` pointer
- Agent heartbeats, node timeout detection, and stateless task rescheduling
- Stable task IDs and idempotent Docker reconciliation
- Start-first and stop-first rolling updates
- Docker health checks as readiness gates for Traefik and rolling updates
- Dynamic host-port allocation per node
- Traefik v3 HTTP Provider JSON
- Bearer-token authentication

Supported stack fields:

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
- `deploy.labels`, including common Traefik HTTP router/service labels

Supported constraints are `node.id`, `node.hostname`, and `node.labels.*` with `==` or `!=`.

## Build

Rust 1.97 or newer is required.

```bash
cargo build --release --locked
```

The binary is `target/release/swarmlite`. A multi-stage [Dockerfile](Dockerfile) is also included.

## Configure S3 or R2

The AWS Rust SDK credential chain is used. For R2:

```bash
export AWS_ACCESS_KEY_ID='...'
export AWS_SECRET_ACCESS_KEY='...'
export SWARMLITE_TOKEN='replace-with-a-long-random-token'
```

Configure the endpoint, bucket, `region: auto`, and `force_path_style: true` as shown in [examples/controller-a.yaml](examples/controller-a.yaml). For AWS S3, omit `endpoint_url`, use the bucket's AWS region, and normally leave `force_path_style` false.

The credentials need `GetObject` and `PutObject` access only to the configured bucket/prefix. Conditional `PutObject` with `If-Match` and `If-None-Match` must be supported by the S3-compatible implementation.

## Run two controllers

Controller IDs and advertised URLs must be unique, while cluster and storage settings must match:

```bash
swarmlite controller --config examples/controller-a.yaml
swarmlite controller --config examples/controller-b.yaml
```

Run controllers under systemd or a separate Docker Compose project with `restart: always`; do not depend on Swarmlite itself to bootstrap the controllers.

Only the leader accepts stack updates and heartbeats. A follower returns HTTP 307 with the current leader URL. The CLI and agent explicitly follow that redirect while preserving the bearer token.

## Run an agent on every Docker node

Set a stable node ID, an address reachable by Traefik, and all controller URLs:

```bash
swarmlite agent --config examples/agent.yaml
```

The agent must be able to access the local Docker socket. Access to that socket is equivalent to root privileges. Only the agent needs Docker access; controllers must not receive a Docker socket.

When every controller is unavailable, agents deliberately leave existing containers unchanged. Once a controller is available again, normal reconciliation resumes.

## Deploy a stack

```bash
export SWARMLITE_TOKEN='replace-with-a-long-random-token'

swarmlite deploy \
  --controller http://10.0.0.10:8080 \
  --name demo \
  --file examples/stack.yaml

swarmlite status \
  --controller http://10.0.0.10:8080
```

Applying the same service definition keeps its revision. Changing the normalized service definition increments the revision and starts a rolling update. Removing a service from the file causes its tasks to be stopped.

## Configure Traefik

Point Traefik's HTTP Provider at any controller:

```yaml
providers:
  http:
    endpoint: http://10.0.0.10:8080/v1/traefik
    pollInterval: 2s
    headers:
      Authorization: Bearer replace-with-a-long-random-token
```

See [examples/traefik.yaml](examples/traefik.yaml). Followers serve the latest committed snapshot, so the endpoint does not have to be the leader. For endpoint-level availability, use a stable internal DNS record or virtual IP for the controllers; if the configured endpoint is temporarily down, Traefik retains its last loaded dynamic configuration.

Swarmlite currently translates these labels:

- `traefik.enable=true`
- `traefik.http.routers.<name>.rule`
- router `service`, `entrypoints`, `middlewares`, `priority`, `tls`, `tls.certresolver`
- `traefik.http.services.<name>.loadbalancer.server.port`
- service `server.scheme` and `passhostheader`

Middleware definitions are not generated yet. A router can still reference a middleware from another Traefik provider, such as `auth@file`.

## Networking model

There is no cross-node container network. For each task, the controller assigns a free host port from the agent's configured range. Traefik receives backends in this form:

```text
http://node-advertise-address:allocated-host-port
```

An explicit stack mapping such as `8080:80` reserves port 8080 on the selected node. Multiple replicas using a fixed published port must be placed on different nodes. When only the Traefik backend port label is present, Swarmlite allocates host ports automatically.

## Current limitations

- Linux Docker nodes are the intended target; Windows containers are not tested.
- No overlay networking, service VIP, routing mesh, or cross-node DNS.
- `deploy.mode: global` is rejected; only replicated services are supported.
- No Compose `build`, `configs`, `secrets`, resource reservations, or autoscaling.
- Named volumes and bind mounts remain node-local. Pin stateful services with placement constraints.
- Automatic node failover is intended for stateless services. Databases need their own replication and leader election.
- Private-registry authentication is not yet forwarded to Docker image pulls.
- One controller process serializes all control-plane writes for a cluster.
- The API is HTTP. Put it on a trusted private network or terminate TLS in front of it.

## Test

```bash
cargo fmt --all --check
cargo test --locked
cargo clippy --all-targets --all-features -- -D warnings
```

