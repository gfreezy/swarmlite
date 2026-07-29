use std::collections::BTreeSet;
use std::time::Duration;

use axum::Router;
use swarmlite_raft::{CommandOutcome, ControllerNode, NodeConfig, RaftNode};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

struct RunningServer {
    shutdown: oneshot::Sender<()>,
    task: JoinHandle<()>,
}

async fn start_server(node: &RaftNode, listener: TcpListener) -> RunningServer {
    let app = Router::new().nest("/internal/raft", node.rpc_router());
    let (shutdown, receiver) = oneshot::channel();
    let task = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = receiver.await;
            })
            .await
            .unwrap();
    });
    RunningServer { shutdown, task }
}

async fn wait_for_value(node: &RaftNode, expected: &[u8]) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if node.local_state().await.value == expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("replicated value did not reach the follower");
}

#[tokio::test]
async fn replicates_over_http_after_learners_are_promoted() {
    let token = "0123456789abcdef0123456789abcdef";
    let directories = [
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
    ];
    let listeners = [
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
    ];
    let addresses = [
        listeners[0].local_addr().unwrap(),
        listeners[1].local_addr().unwrap(),
        listeners[2].local_addr().unwrap(),
    ];

    let mut nodes = Vec::new();
    for (index, directory) in directories.iter().enumerate() {
        let base_url = format!("http://{}", addresses[index]);
        let controller = ControllerNode {
            raft_url: format!("{base_url}/internal/raft"),
            api_url: base_url,
        };
        nodes.push(
            RaftNode::open(NodeConfig::new(
                index as u64 + 1,
                controller,
                directory.path(),
                "three-node-test",
                token,
            ))
            .await
            .unwrap(),
        );
    }

    let mut servers = Vec::new();
    for (node, listener) in nodes.iter().zip(listeners) {
        servers.push(start_server(node, listener).await);
    }

    nodes[0].initialize().await.unwrap();
    nodes[0]
        .raft()
        .wait(Some(Duration::from_secs(10)))
        .current_leader(1, "first controller becomes leader")
        .await
        .unwrap();

    nodes[0]
        .add_learner(2, nodes[1].local_node().clone())
        .await
        .unwrap();
    nodes[0].promote(2).await.unwrap();
    nodes[0]
        .add_learner(3, nodes[2].local_node().clone())
        .await
        .unwrap();
    nodes[0].promote(3).await.unwrap();
    assert_eq!(nodes[0].voter_ids(), BTreeSet::from([1, 2, 3]));

    let response = nodes[0]
        .replace("three-node-write", 0, b"replicated-state".to_vec())
        .await
        .unwrap();
    assert_eq!(response.outcome, CommandOutcome::Applied);
    wait_for_value(&nodes[1], b"replicated-state").await;
    wait_for_value(&nodes[2], b"replicated-state").await;

    let (first, second, third) = tokio::join!(
        nodes[0].shutdown(),
        nodes[1].shutdown(),
        nodes[2].shutdown()
    );
    first.unwrap();
    second.unwrap();
    third.unwrap();
    for server in servers {
        let _ = server.shutdown.send(());
        server.task.await.unwrap();
    }
}
