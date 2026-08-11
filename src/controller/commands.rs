use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use tokio::sync::{oneshot, watch};
use uuid::Uuid;

use crate::model::{AgentCommand, AgentCommandOperation, AgentCommandResult};

const DELIVERY_LEASE: Duration = Duration::from_secs(10);

struct QueuedCommand {
    node_id: String,
    command: AgentCommand,
    dispatched_at: Option<Instant>,
}

#[derive(Default)]
struct BrokerState {
    queues: HashMap<String, VecDeque<String>>,
    commands: HashMap<String, QueuedCommand>,
    waiters: HashMap<String, oneshot::Sender<AgentCommandResult>>,
}

impl BrokerState {
    fn remove_from_queue(&mut self, node_id: &str, command_id: &str) {
        let empty = self.queues.get_mut(node_id).is_some_and(|queue| {
            queue.retain(|queued| queued != command_id);
            queue.is_empty()
        });
        if empty {
            self.queues.remove(node_id);
        }
    }
}

#[derive(Clone)]
pub(super) struct AgentCommandBroker {
    state: Arc<Mutex<BrokerState>>,
    changes: watch::Sender<u64>,
}

pub(super) struct PendingAgentCommand {
    broker: AgentCommandBroker,
    node_id: String,
    command_id: String,
    receiver: Option<oneshot::Receiver<AgentCommandResult>>,
    finished: bool,
}

impl PendingAgentCommand {
    pub(super) async fn wait(
        mut self,
        deadline: tokio::time::Instant,
    ) -> Result<AgentCommandResult, &'static str> {
        let receiver = self.receiver.take().expect("command receiver is present");
        let result = match tokio::time::timeout_at(deadline, receiver).await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(_)) => Err("agent command was cancelled"),
            Err(_) => Err("timed out waiting for the node agent"),
        };
        if result.is_err() {
            self.broker.cancel(&self.node_id, &self.command_id);
        }
        self.finished = true;
        result
    }
}

impl Drop for PendingAgentCommand {
    fn drop(&mut self) {
        if !self.finished {
            self.broker.cancel(&self.node_id, &self.command_id);
        }
    }
}

impl AgentCommandBroker {
    pub(super) fn new() -> Self {
        let (changes, _) = watch::channel(0);
        Self {
            state: Arc::new(Mutex::new(BrokerState::default())),
            changes,
        }
    }

    pub(super) fn enqueue(
        &self,
        node_id: &str,
        operation: AgentCommandOperation,
    ) -> PendingAgentCommand {
        let id = Uuid::new_v4().to_string();
        let command = AgentCommand {
            id: id.clone(),
            operation,
        };
        let (sender, receiver) = oneshot::channel();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state
            .queues
            .entry(node_id.to_owned())
            .or_default()
            .push_back(id.clone());
        state.commands.insert(
            id.clone(),
            QueuedCommand {
                node_id: node_id.to_owned(),
                command,
                dispatched_at: None,
            },
        );
        state.waiters.insert(id.clone(), sender);
        drop(state);
        self.changes
            .send_modify(|revision| *revision = revision.saturating_add(1));
        PendingAgentCommand {
            broker: self.clone(),
            node_id: node_id.to_owned(),
            command_id: id,
            receiver: Some(receiver),
            finished: false,
        }
    }

    pub(super) async fn next(&self, node_id: &str, wait: Duration) -> Option<AgentCommand> {
        let mut changes = self.changes.subscribe();
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            if let Some(command) = self.take_ready(node_id) {
                return Some(command);
            }
            if tokio::time::timeout_at(deadline, changes.changed())
                .await
                .is_err()
            {
                return self.take_ready(node_id);
            }
        }
    }

    pub(super) fn complete(
        &self,
        node_id: &str,
        command_id: &str,
        result: AgentCommandResult,
    ) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let matches_node = state
            .commands
            .get(command_id)
            .is_some_and(|queued| queued.node_id == node_id);
        if !matches_node {
            return false;
        }
        state.remove_from_queue(node_id, command_id);
        state.commands.remove(command_id);
        let Some(waiter) = state.waiters.remove(command_id) else {
            return false;
        };
        waiter.send(result).is_ok()
    }

    pub(super) fn cancel(&self, node_id: &str, command_id: &str) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state
            .commands
            .get(command_id)
            .is_some_and(|queued| queued.node_id == node_id)
        {
            state.remove_from_queue(node_id, command_id);
            state.commands.remove(command_id);
            state.waiters.remove(command_id);
        }
    }

    fn take_ready(&self, node_id: &str) -> Option<AgentCommand> {
        let now = Instant::now();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let expired = state
            .commands
            .values()
            .filter(|queued| {
                queued.node_id == node_id
                    && queued
                        .dispatched_at
                        .is_some_and(|at| now.duration_since(at) >= DELIVERY_LEASE)
            })
            .map(|queued| queued.command.id.clone())
            .collect::<Vec<_>>();
        if !expired.is_empty() {
            let queue = state.queues.entry(node_id.to_owned()).or_default();
            for id in expired {
                if !queue.contains(&id) {
                    queue.push_back(id);
                }
            }
        }

        loop {
            let id = state.queues.get_mut(node_id)?.pop_front()?;
            let Some(queued) = state.commands.get_mut(&id) else {
                continue;
            };
            if queued
                .dispatched_at
                .is_some_and(|at| now.duration_since(at) < DELIVERY_LEASE)
            {
                continue;
            }
            queued.dispatched_at = Some(now);
            return Some(queued.command.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn dispatches_and_completes_a_node_command() {
        let broker = AgentCommandBroker::new();
        let pending = broker.enqueue(
            "node-a",
            AgentCommandOperation::OpenDataSession {
                session_id: "session-a".into(),
                upload_token: "token-a".into(),
                streams: Vec::new(),
            },
        );
        let command = broker
            .next("node-a", Duration::from_millis(1))
            .await
            .unwrap();
        assert_eq!(command.id, pending.command_id);
        assert!(broker.complete("node-a", &command.id, AgentCommandResult { error: None },));
        assert!(
            pending
                .wait(tokio::time::Instant::now() + Duration::from_secs(1))
                .await
                .unwrap()
                .error
                .is_none()
        );
    }

    #[test]
    fn dropping_a_pending_command_removes_it_from_the_node_queue() {
        let broker = AgentCommandBroker::new();
        let pending = broker.enqueue(
            "node-a",
            AgentCommandOperation::OpenDataSession {
                session_id: "session-a".into(),
                upload_token: "token-a".into(),
                streams: Vec::new(),
            },
        );
        let id = pending.command_id.clone();
        drop(pending);
        let state = broker
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(!state.commands.contains_key(&id));
        assert!(!state.queues.contains_key("node-a"));
    }
}
