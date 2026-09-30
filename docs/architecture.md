# How Swarmlite works

[← README](../README.md)

- [Design goals and tradeoffs](#design-goals-and-tradeoffs)
- [Components and data flow](#components-and-data-flow)
- [Node monitoring history](#node-monitoring-history)
- [Why there is one fixed Controller](#why-there-is-one-fixed-controller)
- [Why tasks bind ports on the host](#why-tasks-bind-ports-on-the-host)
- [Why Controller-Agent connections use HTTP](#why-controller-agent-connections-use-http)
- [Availability model and non-goals](#availability-model-and-non-goals)

This guide explains the components, data flow and availability tradeoffs. For cluster setup and
maintenance, see the [operations guide](operations.md).

## Design goals and tradeoffs

The central rule is that the control plane may pause while the serving data plane continues.

| Goal | Architectural decision | Accepted tradeoff |
| --- | --- | --- |
| Keep services online while control processes restart | Docker or Podman owns containers and port mappings; Caddy persists its accepted configuration | Deployments, logs, and reconciliation pause when their control dependency is unavailable |
| Keep the control plane small and replaceable | One fixed Controller uses SQLite while running; recovery rebuilds it from the surviving data plane | No election or automatic failover; control-plane-only history is not reconstructed |
| Keep networking direct and observable | Agents use authenticated HTTP/JSON; Gateways route to dynamic host ports | The trusted network must provide reachability, firewalling, and transport protection when needed |
| Preserve availability during partitions | Disconnects freeze the last applied state; deletion requires an explicit desired-state command | A long partition can temporarily leave duplicate or stale containers, so strict singleton workloads need application-level coordination |

## Components and data flow

Every machine runs the same `swarmlite serve` process. Its fixed role determines which components
are active:

| Component | Runs where | Responsibility |
| --- | --- | --- |
| Controller | The node created with `init` | Stores desired state, schedules tasks, and exposes the API |
| Agent | Every node | Reconciles assigned containers with Docker or Podman |
| Gateway | Enabled per node | Runs Caddy and publishes HTTP/HTTPS routes |
| Stack | Cluster-wide | Groups services and jobs from one Stack file |
| Service | Inside a Stack | Defines an image, replicas, placement, ports, and update behavior |
| Job | Inside a Stack | Defines a one-shot workload with manual or scheduled execution |

The Controller and Agents are the control plane. Containers, operating-system port mappings, and
Caddy are the serving data plane. A control process tells the data plane what should run, but it is
not in the request path after that state has been applied.

## Node monitoring history

The Controller stores node monitoring history separately in `metrics.sqlite`. Original five-second
samples are retained for 15 minutes, minute aggregates for 24 hours, hour aggregates for 30 days,
and day aggregates for 365 days. Aggregates preserve sums, counts and peaks, including missing
values, rather than averaging averages.

History writes use a bounded 64-sample queue and batches of at most 1,024 samples, flushed every
30 seconds or when full. Within a transaction, each affected node/time bucket is loaded and
updated once. The persistent writer has a 2 MiB SQLite page-cache budget; each short-lived history
query has a 512 KiB budget. Historical data stays on disk.

The history database uses WAL with `synchronous=NORMAL`, automatic checkpoints after 1,024 WAL
pages (about 4 MiB with 4 KiB pages), and an 8 MiB WAL retention limit after recycling. This limit
does not cap an active WAL file. Buffered samples can be lost if the process exits abruptly, and
recent committed history can be lost after a power failure or hard reset. History is best-effort
telemetry; the separate desired-state and Agent databases keep their existing durability settings.

## Why there is one fixed Controller

One Controller serializes scheduling, deployment, and routing decisions against one authoritative
SQLite state. Avoiding consensus, quorums, leader election, replicated logs, and split-brain
recovery keeps a small cluster understandable and makes causal history easy for operators and AI
tools to inspect.

This is viable because the Controller is not a traffic proxy. Losing it pauses new decisions but
does not stop containers or Caddy. Swarmlite therefore chooses a replaceable control plane over a
highly available control plane: there is no promotion, demotion, election, or automatic failover.
Recovery rebuilds desired state from the surviving data plane and the original Stack files.

## Why tasks bind ports on the host

Swarmlite does not create a cross-node container network. Docker or Podman allocates a host port,
the Agent reports it to the Controller, and Gateways route directly to
`node-advertise-address:allocated-host-port`.

The mapping belongs to the operating system and container runtime, so restarting the Agent does
not interrupt packets. Dynamic ports let replicas and `start-first` replacements coexist on the
same node. The cost is that nodes and Gateways must be mutually reachable, volumes remain
node-local, and Swarmlite provides no service VIP, routing mesh, NAT traversal, overlay network, or
cross-node DNS.

## Why Controller-Agent connections use HTTP

Control coordination deliberately uses authenticated HTTP with JSON payloads. Bulk logs use an
authenticated WebSocket session. The intended environment is a small trusted network, and a plain
protocol is easy to inspect, reproduce with `curl`, record in logs, and reason about during
AI-assisted debugging.

TLS inside Swarmlite would add certificate bootstrap, trust distribution, renewal, hostname
validation, and another recovery dependency. Swarmlite leaves transport protection to the trusted
network, its firewall or VPN, or an external TLS endpoint. This is an explicit security tradeoff,
not an accidental omission: the bearer token authenticates requests but cannot hide them from, or
protect them against modification by, a machine on the same network path.

## Availability model and non-goals

Managed Service and Gateway containers use the runtime's `unless-stopped` restart policy. They are
not child processes of `swarmlite serve`. An active Caddy starts with `--resume` and keeps its last
accepted configuration in its slot-local config volume. Replacement Gateways always start with
fresh slot volumes and rebuild Controller-owned state before they accept traffic; retired volumes
are deleted after a successful handoff. Agents persist task identity, Stack, Service, slot, revision,
specification hash, ports, and config digests as container labels, then adopt matching containers
after restarting. Job containers use restart policy `no`; see [job execution semantics](applications.md#scheduled-jobs).

Replaying the same desired state is idempotent. Disconnection freezes the last applied state;
deletion happens only from an explicit desired-state change. This favors availability of existing
traffic over strict singleton execution and immediate reconciliation.

Swarmlite is a good fit for a trusted LAN or region that needs Compose-style definitions,
replicated services, placement constraints, rolling updates, logs, and optional HTTPS routing.
Choose another orchestrator when you require control-plane high availability, an overlay network,
service VIPs, cross-node DNS, autoscaling, global services, or the broader Kubernetes ecosystem.
