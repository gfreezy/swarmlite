use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeployTokenCreate {
    pub name: String,
    pub service: String,
    #[serde(default = "default_expiry")]
    pub expires_in: String,
}

fn default_expiry() -> String {
    "90d".into()
}

#[derive(Clone, Serialize, Deserialize)]
pub struct DeployToken {
    pub id: String,
    pub name: String,
    pub service_id: String,
    pub created_at_unix_ms: i64,
    pub expires_at_unix_ms: Option<i64>,
    pub revoked_at_unix_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
}

impl std::fmt::Debug for DeployToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeployToken")
            .field("id", &self.id)
            .field("service_id", &self.service_id)
            .finish_non_exhaustive()
    }
}

impl DeployToken {
    pub fn active(&self, now: i64) -> bool {
        self.revoked_at_unix_ms.is_none()
            && self.expires_at_unix_ms.is_none_or(|expiry| now < expiry)
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedeployRequest {
    #[serde(default)]
    pub source: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceOperationKind {
    Restart,
    PullRedeploy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServiceOperationStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
}

impl ServiceOperationStatus {
    pub fn active(self) -> bool {
        matches!(self, Self::Queued | Self::Running)
    }
}

/// Only this bounded, redacted projection is returned by the public API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceOperation {
    pub operation_id: String,
    pub service: String,
    pub kind: ServiceOperationKind,
    pub status: ServiceOperationStatus,
    pub phase: String,
    pub revision: u64,
    pub deployment_generation: Option<u64>,
    pub completed_tasks: usize,
    pub total_tasks: usize,
    pub stalled: bool,
    pub blocked: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RestartAssignment {
    pub operation_id: String,
    pub container_id: String,
    pub image_id: String,
    pub deadline_unix_ms: i64,
    pub execute: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestartTarget {
    pub task_id: String,
    pub node_id: String,
    pub container_id: String,
    pub image_id: String,
    pub restarted: bool,
    pub completed: bool,
    pub deadline_unix_ms: Option<i64>,
}

/// Internal durable operation, deliberately excluded from cluster status JSON.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceOperationRecord {
    pub view: ServiceOperation,
    pub principal_id: String,
    pub idempotency_key: String,
    pub request_hash: String,
    pub stack: String,
    pub generation: u64,
    pub target_revision: u64,
    pub image: String,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
    pub finished_at_unix_ms: Option<i64>,
    pub source: BTreeMap<String, String>,
    pub tasks: Vec<RestartTarget>,
    pub preparing_nodes: BTreeSet<String>,
    pub prepared_images: BTreeMap<String, String>,
}

impl ServiceOperationRecord {
    pub fn pins_images(&self, state: &crate::model::ClusterState) -> bool {
        self.view.kind == ServiceOperationKind::PullRedeploy
            && (self.view.status.active()
                || (self.view.deployment_generation.is_some()
                    && state
                        .services
                        .get(&self.view.service)
                        .is_some_and(|service| {
                            !service.deleted && service.revision == self.target_revision
                        })))
    }

    pub fn restarting_task(&self, task_id: &str) -> bool {
        self.view.status.active()
            && self.view.kind == ServiceOperationKind::Restart
            && self
                .tasks
                .iter()
                .any(|task| task.task_id == task_id && !task.completed)
    }
}

pub fn expiry_from_duration(value: &str, now: i64) -> Result<Option<i64>, &'static str> {
    if value == "never" {
        return Ok(None);
    }
    let (number, multiplier) = if let Some(value) = value.strip_suffix('d') {
        (value, 86_400_000_i64)
    } else if let Some(value) = value.strip_suffix('h') {
        (value, 3_600_000)
    } else if let Some(value) = value.strip_suffix('m') {
        (value, 60_000)
    } else if let Some(value) = value.strip_suffix('s') {
        (value, 1_000)
    } else {
        return Err("expires_in must be a positive duration (s, m, h, d) or never");
    };
    let invalid = "expires_in must be a positive duration within the timestamp range";
    if number.is_empty() || !number.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid);
    }
    let count: i64 = number.parse().map_err(|_| invalid)?;
    if count == 0 {
        return Err(invalid);
    }
    count
        .checked_mul(multiplier)
        .and_then(|duration| now.checked_add(duration))
        .map(Some)
        .ok_or(invalid)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn token_expires_at_the_exact_deadline_and_permanent_tokens_remain_revocable() {
        let mut token = DeployToken {
            id: "test".into(),
            name: "ci".into(),
            service_id: "demo.web".into(),
            created_at_unix_ms: 0,
            expires_at_unix_ms: Some(100),
            revoked_at_unix_ms: None,
            token: Some("secret-must-not-appear-in-debug".into()),
        };
        assert!(token.active(99));
        assert!(!token.active(100));
        token.expires_at_unix_ms = None;
        assert!(token.active(i64::MAX));
        token.revoked_at_unix_ms = Some(101);
        assert!(!token.active(102));
        assert!(!format!("{token:?}").contains("secret-must-not-appear-in-debug"));
    }

    #[test]
    fn expiry_supports_permanent_and_rejects_invalid_durations() {
        assert_eq!(expiry_from_duration("never", 100).unwrap(), None);
        assert_eq!(expiry_from_duration("24h", 100).unwrap(), Some(86_400_100));
        for input in ["0d", "-1d", "+1h", "1", "1.5d", "", "999999999999999999d"] {
            assert!(expiry_from_duration(input, 100).is_err(), "{input}");
        }
        assert!(expiry_from_duration("1d", i64::MAX).is_err());
        assert!(
            serde_json::from_str::<DeployTokenCreate>(
                r#"{"name":"ci","service":"demo.web","expires_in":null}"#
            )
            .is_err()
        );
    }
}
