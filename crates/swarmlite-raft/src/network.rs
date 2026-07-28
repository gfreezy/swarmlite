use std::io;
use std::sync::Arc;

use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::routing::post;
use axum::{Json, Router};
use openraft::error::{InstallSnapshotError, NetworkError, RemoteError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use reqwest::Client;
use serde::{Serialize, de::DeserializeOwned};

use crate::types::{ManagerNode, NodeId, Raft, RpcError, TypeConfig};

#[derive(Debug, Clone)]
pub struct HttpNetwork {
    client: Client,
    token: Arc<str>,
}

impl HttpNetwork {
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            client: Client::new(),
            token: Arc::from(token.into()),
        }
    }

    async fn send_rpc<Req, Resp, Err>(
        &self,
        target: NodeId,
        target_node: &ManagerNode,
        route: &str,
        request: &Req,
    ) -> Result<Resp, openraft::error::RPCError<NodeId, ManagerNode, Err>>
    where
        Req: Serialize + ?Sized,
        Resp: DeserializeOwned,
        Err: std::error::Error + DeserializeOwned,
    {
        let url = format!("{}/{}", target_node.raft_url.trim_end_matches('/'), route);
        let response = self
            .client
            .post(&url)
            .bearer_auth(self.token.as_ref())
            .json(request)
            .send()
            .await
            .map_err(|error| {
                if error.is_connect() || error.is_timeout() {
                    openraft::error::RPCError::Unreachable(Unreachable::new(&error))
                } else {
                    openraft::error::RPCError::Network(NetworkError::new(&error))
                }
            })?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            let error = io::Error::other(format!(
                "Raft peer {target} returned {status}: {}",
                body.chars().take(512).collect::<String>()
            ));
            return Err(openraft::error::RPCError::Network(NetworkError::new(
                &error,
            )));
        }
        let result: Result<Resp, Err> = response
            .json()
            .await
            .map_err(|error| openraft::error::RPCError::Network(NetworkError::new(&error)))?;
        result.map_err(|error| {
            openraft::error::RPCError::RemoteError(RemoteError::new(target, error))
        })
    }
}

impl RaftNetworkFactory<TypeConfig> for HttpNetwork {
    type Network = HttpConnection;

    async fn new_client(&mut self, target: NodeId, node: &ManagerNode) -> Self::Network {
        HttpConnection {
            network: self.clone(),
            target,
            node: node.clone(),
        }
    }
}

pub struct HttpConnection {
    network: HttpNetwork,
    target: NodeId,
    node: ManagerNode,
}

impl RaftNetwork<TypeConfig> for HttpConnection {
    async fn append_entries(
        &mut self,
        request: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RpcError> {
        self.network
            .send_rpc(self.target, &self.node, "append", &request)
            .await
    }

    async fn install_snapshot(
        &mut self,
        request: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<InstallSnapshotResponse<NodeId>, RpcError<InstallSnapshotError>> {
        self.network
            .send_rpc(self.target, &self.node, "snapshot", &request)
            .await
    }

    async fn vote(
        &mut self,
        request: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RpcError> {
        self.network
            .send_rpc(self.target, &self.node, "vote", &request)
            .await
    }
}

#[derive(Clone)]
struct RpcState {
    raft: Raft,
    token: Arc<str>,
}

/// Returns the authenticated internal router. Mount this router at the URL
/// stored in [`ManagerNode::raft_url`], for example `/internal/raft`.
pub fn rpc_router(raft: Raft, token: impl Into<String>) -> Router {
    Router::new()
        .route("/vote", post(vote))
        .route("/append", post(append))
        .route("/snapshot", post(snapshot))
        .layer(DefaultBodyLimit::disable())
        .with_state(RpcState {
            raft,
            token: Arc::from(token.into()),
        })
}

async fn vote(
    State(state): State<RpcState>,
    headers: HeaderMap,
    Json(request): Json<VoteRequest<NodeId>>,
) -> Result<Json<Result<VoteResponse<NodeId>, openraft::error::RaftError<NodeId>>>, StatusCode> {
    authorize(&headers, &state.token)?;
    Ok(Json(state.raft.vote(request).await))
}

async fn append(
    State(state): State<RpcState>,
    headers: HeaderMap,
    Json(request): Json<AppendEntriesRequest<TypeConfig>>,
) -> Result<
    Json<Result<AppendEntriesResponse<NodeId>, openraft::error::RaftError<NodeId>>>,
    StatusCode,
> {
    authorize(&headers, &state.token)?;
    Ok(Json(state.raft.append_entries(request).await))
}

async fn snapshot(
    State(state): State<RpcState>,
    headers: HeaderMap,
    Json(request): Json<InstallSnapshotRequest<TypeConfig>>,
) -> Result<
    Json<
        Result<
            InstallSnapshotResponse<NodeId>,
            openraft::error::RaftError<NodeId, InstallSnapshotError>,
        >,
    >,
    StatusCode,
> {
    authorize(&headers, &state.token)?;
    Ok(Json(state.raft.install_snapshot(request).await))
}

fn authorize(headers: &HeaderMap, expected: &str) -> Result<(), StatusCode> {
    let provided = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .unwrap_or_default();
    if constant_time_eq(provided.as_bytes(), expected.as_bytes()) {
        Ok(())
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}
