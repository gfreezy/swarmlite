use super::*;
use swarmlite_core::service_operations::{DeployToken, ServiceOperationRecord};

const SERVICE_SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS deploy_tokens (
        cluster_id TEXT NOT NULL, id TEXT NOT NULL, name TEXT NOT NULL,
        service_id TEXT NOT NULL, secret TEXT, digest BLOB NOT NULL,
        created_at_unix_ms INTEGER NOT NULL, expires_at_unix_ms INTEGER,
        revoked_at_unix_ms INTEGER, PRIMARY KEY(cluster_id, id)
    ) STRICT;
    CREATE TABLE IF NOT EXISTS service_operations (
        cluster_id TEXT NOT NULL, id TEXT NOT NULL, principal_id TEXT NOT NULL,
        request_key TEXT NOT NULL, active_stack TEXT, document BLOB NOT NULL,
        PRIMARY KEY(cluster_id, id), UNIQUE(cluster_id, principal_id, request_key),
        UNIQUE(cluster_id, active_stack)
    ) STRICT;";

#[derive(Clone)]
pub(crate) struct StoredDeployToken {
    pub metadata: DeployToken,
    pub secret: Option<String>,
    pub digest: Vec<u8>,
}

impl StateRepository {
    pub(super) fn initialize_service_storage(&self, data_dir: &Path) -> StorageResult<()> {
        self.with_connection(|connection| {
            let version: Option<(u32, String)> = connection.query_row(
                "SELECT schema_version, cluster_id FROM control_plane WHERE singleton = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            ).optional().map_err(backend)?;
            if let Some((12, cluster_id)) = version {
                if cluster_id != self.cluster.cluster_id {
                    return Err(invalid("cluster identity mismatch"));
                }
                // VACUUM INTO takes a consistent SQLite snapshot, including committed WAL pages.
                let backup = data_dir.join(format!("swarmlite.schema12.{}.sqlite", uuid::Uuid::new_v4()));
                #[cfg(unix)]
                {
                    use std::os::unix::fs::OpenOptionsExt;
                    std::fs::OpenOptions::new()
                        .write(true).create_new(true).mode(0o600)
                        .open(&backup).map_err(backend)?;
                }
                connection.execute("VACUUM INTO ?1", [backup.to_string_lossy().as_ref()])
                    .map_err(backend)?;
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    std::fs::set_permissions(&backup, std::fs::Permissions::from_mode(0o600))
                        .map_err(backend)?;
                }
                let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(backend)?;
                let document: Vec<u8> = tx.query_row(
                    "SELECT document FROM control_plane WHERE singleton = 1 AND schema_version = 12",
                    [], |row| row.get(0),
                ).map_err(backend)?;
                let persisted: PersistedControlPlane = serde_json::from_slice(&document).map_err(invalid)?;
                if persisted.schema_version != 12
                    || persisted.cluster_id != self.cluster.cluster_id
                    || !same_cluster_identity(&persisted.cluster, &self.cluster)
                {
                    return Err(invalid("migration cluster or schema mismatch"));
                }
                let mut value: serde_json::Value = serde_json::from_slice(&document).map_err(invalid)?;
                value["schema_version"] = serde_json::json!(PERSISTED_SCHEMA_VERSION);
                tx.execute_batch(SERVICE_SCHEMA).map_err(backend)?;
                tx.execute(
                    "UPDATE control_plane SET schema_version = ?1, document = ?2,
                         generation = generation + 1
                     WHERE singleton = 1 AND schema_version = 12",
                    params![PERSISTED_SCHEMA_VERSION, serde_json::to_vec(&value).map_err(invalid)?],
                ).map_err(backend)?;
                tx.commit().map_err(backend)?;
            } else {
                connection.execute_batch(SERVICE_SCHEMA).map_err(backend)?;
            }
            Ok(())
        })
    }

    pub(crate) fn create_deploy_token(&self, token: &StoredDeployToken) -> StorageResult<()> {
        self.with_connection(|connection| {
            let t = &token.metadata;
            connection
                .execute(
                    "INSERT INTO deploy_tokens VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
                    params![
                        self.cluster.cluster_id,
                        t.id,
                        t.name,
                        t.service_id,
                        token.secret,
                        token.digest,
                        t.created_at_unix_ms,
                        t.expires_at_unix_ms,
                        t.revoked_at_unix_ms
                    ],
                )
                .map_err(backend)?;
            Ok(())
        })
    }

    pub(crate) fn deploy_tokens(&self) -> StorageResult<Vec<StoredDeployToken>> {
        self.with_connection(|connection| {
            let mut statement = connection.prepare("SELECT id,name,service_id,secret,digest,created_at_unix_ms,expires_at_unix_ms,revoked_at_unix_ms FROM deploy_tokens WHERE cluster_id = ?1 ORDER BY created_at_unix_ms,id").map_err(backend)?;
            statement.query_map([&self.cluster.cluster_id], |row| Ok(StoredDeployToken {
                metadata: DeployToken { id: row.get(0)?, name: row.get(1)?, service_id: row.get(2)?, token: None, created_at_unix_ms: row.get(5)?, expires_at_unix_ms: row.get(6)?, revoked_at_unix_ms: row.get(7)? }, secret: row.get(3)?, digest: row.get(4)?,
            })).map_err(backend)?.collect::<Result<Vec<_>,_>>().map_err(backend)
        })
    }

    pub(crate) fn revoke_deploy_token(&self, id: &str, now: i64) -> StorageResult<bool> {
        self.with_connection(|connection| Ok(connection.execute("UPDATE deploy_tokens SET secret = NULL, revoked_at_unix_ms = COALESCE(revoked_at_unix_ms, ?3) WHERE cluster_id = ?1 AND id = ?2", params![self.cluster.cluster_id, id, now]).map_err(backend)? == 1))
    }
}

pub(super) fn write_operations(
    connection: &Connection,
    cluster: &str,
    operations: &BTreeMap<String, ServiceOperationRecord>,
) -> StorageResult<()> {
    // This runs inside the desired-state CAS transaction: acceptance, stack slot and
    // deployment association cannot become visible independently.
    for (id, operation) in operations {
        let exists = connection
            .query_row(
                "SELECT 1 FROM service_operations WHERE cluster_id = ?1 AND id = ?2",
                params![cluster, id],
                |_| Ok(()),
            )
            .optional()
            .map_err(backend)?
            .is_some();
        if !exists && operation.principal_id != "admin" {
            let valid = connection.query_row("SELECT 1 FROM deploy_tokens WHERE cluster_id = ?1 AND id = ?2 AND revoked_at_unix_ms IS NULL AND (expires_at_unix_ms IS NULL OR expires_at_unix_ms > ?3)", params![cluster, operation.principal_id, crate::controller::unix_ms()], |_| Ok(())).optional().map_err(backend)?.is_some();
            if !valid {
                return Err(StorageError::CredentialInactive);
            }
        }
    }
    connection
        .execute(
            "DELETE FROM service_operations WHERE cluster_id = ?1",
            [cluster],
        )
        .map_err(backend)?;
    for (id, operation) in operations {
        connection
            .execute(
                "INSERT INTO service_operations VALUES (?1,?2,?3,?4,?5,?6)",
                params![
                    cluster,
                    id,
                    operation.principal_id,
                    operation.idempotency_key,
                    operation.view.status.active().then_some(&operation.stack),
                    serde_json::to_vec(operation).map_err(invalid)?
                ],
            )
            .map_err(backend)?;
    }
    Ok(())
}

pub(super) fn read_operations(
    connection: &Connection,
    cluster: &str,
) -> StorageResult<BTreeMap<String, ServiceOperationRecord>> {
    let mut statement = connection
        .prepare("SELECT id,document FROM service_operations WHERE cluster_id = ?1")
        .map_err(backend)?;
    let rows = statement
        .query_map([cluster], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
        })
        .map_err(backend)?;
    let mut operations = BTreeMap::new();
    for row in rows {
        let (id, document) = row.map_err(backend)?;
        operations.insert(id, serde_json::from_slice(&document).map_err(invalid)?);
    }
    Ok(operations)
}
