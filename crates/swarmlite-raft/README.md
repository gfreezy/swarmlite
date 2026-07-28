# swarmlite-raft

Embedded OpenRaft control-plane storage for Swarmlite.

The crate owns consensus, persistent Raft logs, snapshots, peer RPC and manager
membership. It intentionally does not depend on Swarmlite's scheduler models.
The application serializes its complete durable `ClusterState` into bytes and
submits a generation compare-and-swap replacement:

- `redb` stores votes, committed indexes, Raft logs, the state machine and its
  latest snapshot in `<data_dir>/raft.redb`.
- Axum and Reqwest provide authenticated peer RPC.
- `request_id` deduplicates retries; `expected_generation` rejects stale writes.

```rust,no_run
use swarmlite_raft::{CommandOutcome, ManagerNode, NodeConfig, RaftNode};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let node = RaftNode::open(NodeConfig::new(
    1,
    ManagerNode {
        raft_url: "http://10.0.0.10:8080/internal/raft".into(),
        api_url: "http://10.0.0.10:8080".into(),
    },
    "/var/lib/swarmlite/raft",
    "production",
    "replace-with-a-long-random-token",
))
.await?;

node.initialize().await?;
let response = node
    .replace("unique-request-id", 0, serde_json::to_vec(&"state")?)
    .await?;
assert_eq!(response.outcome, CommandOutcome::Applied);
# Ok(())
# }
```

Mount `node.rpc_router()` at the path encoded in `ManagerNode::raft_url`. The
router requires the internal bearer token and disables Axum's default body
limit so snapshots can be transferred. Use TLS or a trusted private network;
the shared token is not a substitute for transport encryption.

To add a manager safely:

1. Start its `RaftNode` and RPC router with an empty data directory.
2. Call `leader.add_learner(id, node).await`.
3. After it has caught up, call `leader.promote(id).await`.

Workers do not run this crate. Production HA should use three voting managers;
two voters do not tolerate a failure.

The host application still owns process lifecycle, stable node-ID and token
provisioning, leader redirects, serialization of `ClusterState`, and the
user-facing join API. Call `initialize()` only for the first node of a new
cluster; subsequent managers join through `add_learner()` and `promote()`.
