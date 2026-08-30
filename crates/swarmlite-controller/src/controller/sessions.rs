use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::extract::ws::{Message, WebSocket};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::{mpsc, watch};
use tracing::warn;
use uuid::Uuid;

use crate::data_plane::{DATA_STREAM_WRITE_TIMEOUT, DataFrame, DataFrameKind};

use super::ControllerError;

const SESSION_QUEUE_FRAMES: usize = 64;
const SESSION_ATTACH_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_ACTIVE_DATA_SESSIONS: usize = 128;

struct DataSession {
    client_token: String,
    node_tokens: HashMap<String, String>,
    allowed_streams: HashMap<String, BTreeSet<u32>>,
    client_attached: bool,
    attached_nodes: HashSet<String>,
    sender: mpsc::Sender<Vec<u8>>,
    receiver: Option<mpsc::Receiver<Vec<u8>>>,
    cancel: watch::Sender<bool>,
}

#[derive(Default)]
struct SessionState {
    sessions: HashMap<String, DataSession>,
}

#[derive(Clone, Default)]
pub(super) struct DataSessionBroker {
    state: Arc<Mutex<SessionState>>,
}

pub(super) struct RegisteredDataSession {
    pub(super) id: String,
    pub(super) client_token: String,
    pub(super) node_tokens: HashMap<String, String>,
    pub(super) sender: mpsc::Sender<Vec<u8>>,
}

pub(super) struct ClientDataAttachment {
    broker: DataSessionBroker,
    session_id: String,
    receiver: mpsc::Receiver<Vec<u8>>,
    cancel: watch::Receiver<bool>,
}

pub(super) struct AgentDataAttachment {
    sender: mpsc::Sender<Vec<u8>>,
    allowed_streams: BTreeSet<u32>,
    cancel: watch::Receiver<bool>,
}

impl DataSessionBroker {
    pub(super) fn new() -> Self {
        Self::default()
    }

    pub(super) fn register(
        &self,
        allowed_streams: HashMap<String, BTreeSet<u32>>,
    ) -> Result<RegisteredDataSession, ControllerError> {
        let id = Uuid::new_v4().to_string();
        let client_token = Uuid::new_v4().to_string();
        let node_tokens = allowed_streams
            .keys()
            .map(|node_id| (node_id.clone(), Uuid::new_v4().to_string()))
            .collect::<HashMap<_, _>>();
        let (sender, receiver) = mpsc::channel(SESSION_QUEUE_FRAMES);
        let (cancel, _) = watch::channel(false);
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.sessions.len() >= MAX_ACTIVE_DATA_SESSIONS {
            return Err(ControllerError::Conflict(format!(
                "at most {MAX_ACTIVE_DATA_SESSIONS} data sessions may be active"
            )));
        }
        state.sessions.insert(
            id.clone(),
            DataSession {
                client_token: client_token.clone(),
                node_tokens: node_tokens.clone(),
                allowed_streams,
                client_attached: false,
                attached_nodes: HashSet::new(),
                sender: sender.clone(),
                receiver: Some(receiver),
                cancel,
            },
        );
        drop(state);

        let expiry_broker = self.clone();
        let expiry_id = id.clone();
        tokio::spawn(async move {
            tokio::time::sleep(SESSION_ATTACH_TIMEOUT).await;
            expiry_broker.expire_unattached(&expiry_id);
        });

        Ok(RegisteredDataSession {
            id,
            client_token,
            node_tokens,
            sender,
        })
    }

    pub(super) fn attach_client(
        &self,
        session_id: &str,
        token: &str,
    ) -> Result<ClientDataAttachment, ControllerError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let session = state.sessions.get_mut(session_id).ok_or_else(|| {
            ControllerError::NotFound(format!("data session {session_id:?} not found"))
        })?;
        if !constant_time_eq(token.as_bytes(), session.client_token.as_bytes()) {
            return Err(ControllerError::Unauthorized);
        }
        if session.client_attached {
            return Err(ControllerError::Conflict(format!(
                "data session {session_id:?} already has a client"
            )));
        }
        let receiver = session.receiver.take().ok_or_else(|| {
            ControllerError::Conflict(format!("data session {session_id:?} already has a client"))
        })?;
        session.client_attached = true;
        Ok(ClientDataAttachment {
            broker: self.clone(),
            session_id: session_id.to_owned(),
            receiver,
            cancel: session.cancel.subscribe(),
        })
    }

    pub(super) fn attach_agent(
        &self,
        session_id: &str,
        node_id: &str,
        token: &str,
    ) -> Result<AgentDataAttachment, ControllerError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let session = state.sessions.get_mut(session_id).ok_or_else(|| {
            ControllerError::NotFound(format!("data session {session_id:?} not found"))
        })?;
        let expected_token = session
            .node_tokens
            .get(node_id)
            .ok_or(ControllerError::Unauthorized)?;
        if !constant_time_eq(token.as_bytes(), expected_token.as_bytes()) {
            return Err(ControllerError::Unauthorized);
        }
        if !session.attached_nodes.insert(node_id.to_owned()) {
            return Err(ControllerError::Conflict(format!(
                "node {node_id:?} is already attached to data session {session_id:?}"
            )));
        }
        Ok(AgentDataAttachment {
            sender: session.sender.clone(),
            allowed_streams: session
                .allowed_streams
                .get(node_id)
                .cloned()
                .unwrap_or_default(),
            cancel: session.cancel.subscribe(),
        })
    }

    fn expire_unattached(&self, session_id: &str) {
        let should_cancel = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .sessions
            .get(session_id)
            .is_some_and(|session| !session.client_attached);
        if should_cancel {
            self.cancel(session_id);
        }
    }

    fn cancel(&self, session_id: &str) {
        let session = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .sessions
            .remove(session_id);
        if let Some(session) = session {
            session.cancel.send_replace(true);
        }
    }
}

impl ClientDataAttachment {
    pub(super) async fn serve(mut self, socket: WebSocket) {
        let (mut sink, mut source) = socket.split();
        loop {
            tokio::select! {
                frame = self.receiver.recv() => {
                    let Some(frame) = frame else { break; };
                    match tokio::time::timeout(
                        DATA_STREAM_WRITE_TIMEOUT,
                        sink.send(Message::Binary(frame.into())),
                    ).await {
                        Ok(Ok(())) => {}
                        Ok(Err(error)) => {
                            warn!(session_id = %self.session_id, %error, "client data stream failed");
                            break;
                        }
                        Err(_) => {
                            warn!(session_id = %self.session_id, "client data stream write timed out");
                            break;
                        }
                    }
                }
                incoming = source.next() => {
                    match incoming {
                        Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                        _ => {}
                    }
                }
                changed = self.cancel.changed() => {
                    if changed.is_err() || *self.cancel.borrow() {
                        break;
                    }
                }
            }
        }
        self.broker.cancel(&self.session_id);
        let _ = tokio::time::timeout(DATA_STREAM_WRITE_TIMEOUT, sink.close()).await;
    }
}

impl AgentDataAttachment {
    pub(super) async fn serve(mut self, socket: WebSocket) {
        let (_sink, mut source) = socket.split();
        let mut ended = BTreeSet::new();
        let mut sequences = HashMap::<u32, u64>::new();
        let mut disconnect_error = "node data stream disconnected".to_owned();

        loop {
            tokio::select! {
                incoming = source.next() => {
                    let encoded = match incoming {
                        Some(Ok(Message::Binary(encoded))) => encoded,
                        Some(Ok(Message::Close(_))) | None => break,
                        Some(Ok(_)) => continue,
                        Some(Err(error)) => {
                            disconnect_error = format!("node data stream failed: {error}");
                            break;
                        }
                    };
                    let frame = match DataFrame::decode(&encoded) {
                        Ok(frame) => frame,
                        Err(error) => {
                            disconnect_error = format!("node sent an invalid data frame: {error}");
                            break;
                        }
                    };
                    if !self.allowed_streams.contains(&frame.stream_id) {
                        disconnect_error = format!(
                            "node sent unassigned stream ID {}",
                            frame.stream_id
                        );
                        break;
                    }
                    if !matches!(frame.kind, DataFrameKind::Data | DataFrameKind::End | DataFrameKind::Error) {
                        disconnect_error = format!("node sent disallowed {:?} frame", frame.kind);
                        break;
                    }
                    let expected = sequences
                        .get(&frame.stream_id)
                        .map_or(0, |sequence| sequence.saturating_add(1));
                    if frame.sequence != expected {
                        disconnect_error = format!(
                            "stream {} sequence {} arrived; expected {expected}",
                            frame.stream_id, frame.sequence
                        );
                        break;
                    }
                    sequences.insert(frame.stream_id, frame.sequence);
                    if frame.kind == DataFrameKind::End {
                        ended.insert(frame.stream_id);
                    }
                    if self.sender.send(encoded.to_vec()).await.is_err() {
                        return;
                    }
                    if ended.len() == self.allowed_streams.len() {
                        return;
                    }
                }
                changed = self.cancel.changed() => {
                    if changed.is_err() || *self.cancel.borrow() {
                        return;
                    }
                }
            }
        }

        for stream_id in self.allowed_streams.difference(&ended).copied() {
            let sequence = sequences
                .get(&stream_id)
                .map_or(0, |sequence| sequence.saturating_add(1));
            if send_frame(
                &self.sender,
                DataFrame::error(stream_id, sequence, &disconnect_error),
            )
            .await
            .is_err()
            {
                return;
            }
            let _ = send_frame(&self.sender, DataFrame::end(stream_id, sequence + 1)).await;
        }
    }
}

pub(super) async fn send_frame(sender: &mpsc::Sender<Vec<u8>>, frame: DataFrame) -> Result<(), ()> {
    let encoded = frame.encode().map_err(|_| ())?;
    sender.send(encoded).await.map_err(|_| ())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn session_tokens_are_scoped_and_single_use() {
        let broker = DataSessionBroker::new();
        let registered = broker
            .register(HashMap::from([("node-a".into(), BTreeSet::from([1]))]))
            .unwrap();

        assert!(
            broker
                .attach_client(&registered.id, "not-the-token")
                .is_err()
        );
        assert!(
            broker
                .attach_client(&registered.id, &registered.client_token)
                .is_ok()
        );
        assert!(
            broker
                .attach_client(&registered.id, &registered.client_token)
                .is_err()
        );

        let node_token = registered.node_tokens.get("node-a").unwrap();
        assert!(
            broker
                .attach_agent(&registered.id, "node-b", node_token)
                .is_err()
        );
        assert!(
            broker
                .attach_agent(&registered.id, "node-a", node_token)
                .is_ok()
        );
        assert!(
            broker
                .attach_agent(&registered.id, "node-a", node_token)
                .is_err()
        );
    }
}
