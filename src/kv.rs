use std::collections::BTreeSet;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use rusqlite::{OptionalExtension, TransactionBehavior, params};

use crate::{
    database::Database,
    model::{
        KvListResponse, KvLockAcquireResponse, KvLockStatus, KvObjectResponse, KvStatResponse,
    },
    storage::{StorageError, StorageResult},
};

const MAX_KEY_BYTES: usize = 1_024;
const MAX_OBJECT_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug)]
pub(crate) struct LegacyKvObject {
    pub key: String,
    pub value_base64: String,
    pub modified_at_unix_ms: i64,
}

#[derive(Debug)]
pub(crate) struct LegacyKvLock {
    pub name: String,
    pub owner_id: String,
    pub fencing_token: u64,
    pub lease_until_unix_ms: i64,
}

#[derive(Debug, Default)]
pub(crate) struct LegacyKvImport {
    pub objects: Vec<LegacyKvObject>,
    pub locks: Vec<LegacyKvLock>,
    pub next_fencing_token: u64,
}

#[derive(Clone)]
pub(crate) struct KvRepository {
    database: Database,
}

impl KvRepository {
    pub(crate) fn open(database: Database) -> StorageResult<Self> {
        let repository = Self { database };
        let connection = repository.database.connect().map_err(backend)?;
        connection
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS kv_objects (
                    key TEXT PRIMARY KEY,
                    value BLOB NOT NULL,
                    modified_at_unix_ms INTEGER NOT NULL CHECK (modified_at_unix_ms >= 0)
                 ) STRICT;
                 CREATE TABLE IF NOT EXISTS kv_locks (
                    name TEXT PRIMARY KEY,
                    owner_id TEXT NOT NULL,
                    fencing_token INTEGER NOT NULL CHECK (fencing_token > 0),
                    lease_until_unix_ms INTEGER NOT NULL CHECK (lease_until_unix_ms >= 0)
                 ) STRICT;
                 CREATE TABLE IF NOT EXISTS kv_meta (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    next_fencing_token INTEGER NOT NULL CHECK (next_fencing_token >= 0)
                 ) STRICT;
                 INSERT OR IGNORE INTO kv_meta(singleton, next_fencing_token) VALUES (1, 0);",
            )
            .map_err(backend)?;
        Ok(repository)
    }

    pub(crate) fn put(
        &self,
        key: &str,
        value: &[u8],
        modified_at_unix_ms: i64,
    ) -> StorageResult<()> {
        let connection = self.database.connect().map_err(backend)?;
        connection
            .execute(
                "INSERT INTO kv_objects(key, value, modified_at_unix_ms) VALUES (?1, ?2, ?3)
                 ON CONFLICT(key) DO UPDATE SET
                    value = excluded.value,
                    modified_at_unix_ms = excluded.modified_at_unix_ms",
                params![key, value, modified_at_unix_ms],
            )
            .map_err(backend)?;
        Ok(())
    }

    pub(crate) fn delete(&self, key: &str, recursive: bool) -> StorageResult<()> {
        let connection = self.database.connect().map_err(backend)?;
        if recursive {
            connection
                .execute(
                    "DELETE FROM kv_objects
                     WHERE key = ?1 OR substr(key, 1, length(?1) + 1) = ?1 || '/'",
                    [key],
                )
                .map_err(backend)?;
        } else {
            connection
                .execute("DELETE FROM kv_objects WHERE key = ?1", [key])
                .map_err(backend)?;
        }
        Ok(())
    }

    pub(crate) fn get(&self, key: &str) -> StorageResult<Option<KvObjectResponse>> {
        let connection = self.database.connect().map_err(backend)?;
        connection
            .query_row(
                "SELECT value, modified_at_unix_ms FROM kv_objects WHERE key = ?1",
                [key],
                |row| {
                    let value: Vec<u8> = row.get(0)?;
                    Ok(KvObjectResponse {
                        key: key.to_owned(),
                        value_base64: STANDARD.encode(&value),
                        modified_at_unix_ms: row.get(1)?,
                        size: value.len() as u64,
                    })
                },
            )
            .optional()
            .map_err(backend)
    }

    pub(crate) fn list(
        &self,
        path: &str,
        recursive: bool,
    ) -> StorageResult<Option<KvListResponse>> {
        let keys = self.keys()?;
        if !keys
            .iter()
            .any(|key| key == path || is_component_descendant(key, path))
        {
            return Ok(None);
        }
        let mut result = BTreeSet::new();
        for key in keys.iter().filter(|key| is_component_descendant(key, path)) {
            let relative = if path.is_empty() {
                key.as_str()
            } else {
                &key[path.len() + 1..]
            };
            if recursive {
                let mut current = String::new();
                for component in relative.split('/') {
                    if !current.is_empty() {
                        current.push('/');
                    }
                    current.push_str(component);
                    result.insert(join_path(path, &current));
                }
            } else if let Some((first, _)) = relative.split_once('/') {
                result.insert(join_path(path, first));
            } else {
                result.insert(join_path(path, relative));
            }
        }
        Ok(Some(KvListResponse {
            keys: result.into_iter().collect(),
        }))
    }

    pub(crate) fn stat(&self, key: &str) -> StorageResult<Option<KvStatResponse>> {
        if !key.is_empty()
            && let Some(object) = self.get(key)?
        {
            return Ok(Some(KvStatResponse {
                key: key.to_owned(),
                modified_at_unix_ms: object.modified_at_unix_ms,
                size: object.size,
                is_value: true,
            }));
        }
        let connection = self.database.connect().map_err(backend)?;
        let modified_at_unix_ms = connection
            .query_row(
                "SELECT MAX(modified_at_unix_ms) FROM kv_objects
                 WHERE ?1 = '' OR substr(key, 1, length(?1) + 1) = ?1 || '/'",
                [key],
                |row| row.get::<_, Option<i64>>(0),
            )
            .map_err(backend)?;
        Ok(
            modified_at_unix_ms.map(|modified_at_unix_ms| KvStatResponse {
                key: key.to_owned(),
                modified_at_unix_ms,
                size: 0,
                is_value: false,
            }),
        )
    }

    pub(crate) fn acquire_lock(
        &self,
        name: &str,
        owner_id: &str,
        now: i64,
        lease_until_unix_ms: i64,
    ) -> StorageResult<KvLockAcquireResponse> {
        let mut connection = self.database.connect().map_err(backend)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(backend)?;
        let existing = transaction
            .query_row(
                "SELECT owner_id, fencing_token, lease_until_unix_ms
                 FROM kv_locks WHERE name = ?1",
                [name],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(backend)?;
        if let Some((existing_owner, _, existing_deadline)) = &existing
            && *existing_deadline > now
            && existing_owner != owner_id
        {
            return Ok(KvLockAcquireResponse {
                status: KvLockStatus::Busy,
                fencing_token: None,
                lease_until_unix_ms: Some(*existing_deadline),
                retry_after_millis: Some(
                    u64::try_from(*existing_deadline - now)
                        .unwrap_or(1_000)
                        .clamp(100, 1_000),
                ),
            });
        }

        let fencing_token = if let Some((existing_owner, token, existing_deadline)) = existing
            && existing_deadline > now
            && existing_owner == owner_id
        {
            transaction
                .execute(
                    "UPDATE kv_locks SET lease_until_unix_ms = ?1 WHERE name = ?2",
                    params![lease_until_unix_ms, name],
                )
                .map_err(backend)?;
            u64::try_from(token).map_err(|_| invalid("negative KV fencing token"))?
        } else {
            let token = transaction
                .query_row(
                    "UPDATE kv_meta SET next_fencing_token = next_fencing_token + 1
                     WHERE singleton = 1 RETURNING next_fencing_token",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .map_err(backend)?;
            transaction
                .execute(
                    "INSERT INTO kv_locks(name, owner_id, fencing_token, lease_until_unix_ms)
                     VALUES (?1, ?2, ?3, ?4)
                     ON CONFLICT(name) DO UPDATE SET
                        owner_id = excluded.owner_id,
                        fencing_token = excluded.fencing_token,
                        lease_until_unix_ms = excluded.lease_until_unix_ms",
                    params![name, owner_id, token, lease_until_unix_ms],
                )
                .map_err(backend)?;
            u64::try_from(token).map_err(|_| invalid("negative KV fencing token"))?
        };
        transaction.commit().map_err(backend)?;
        Ok(KvLockAcquireResponse {
            status: KvLockStatus::Acquired,
            fencing_token: Some(fencing_token),
            lease_until_unix_ms: Some(lease_until_unix_ms),
            retry_after_millis: None,
        })
    }

    pub(crate) fn renew_lock(
        &self,
        name: &str,
        owner_id: &str,
        fencing_token: u64,
        now: i64,
        lease_until_unix_ms: i64,
    ) -> StorageResult<bool> {
        let token = i64::try_from(fencing_token)
            .map_err(|_| invalid("KV fencing token exceeds SQLite integer range"))?;
        let connection = self.database.connect().map_err(backend)?;
        let changed = connection
            .execute(
                "UPDATE kv_locks SET lease_until_unix_ms = ?1
                 WHERE name = ?2 AND owner_id = ?3 AND fencing_token = ?4
                   AND lease_until_unix_ms > ?5",
                params![lease_until_unix_ms, name, owner_id, token, now],
            )
            .map_err(backend)?;
        Ok(changed == 1)
    }

    pub(crate) fn release_lock(
        &self,
        name: &str,
        owner_id: &str,
        fencing_token: u64,
    ) -> StorageResult<bool> {
        let token = i64::try_from(fencing_token)
            .map_err(|_| invalid("KV fencing token exceeds SQLite integer range"))?;
        let mut connection = self.database.connect().map_err(backend)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(backend)?;
        let existing = transaction
            .query_row(
                "SELECT owner_id, fencing_token FROM kv_locks WHERE name = ?1",
                [name],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
            )
            .optional()
            .map_err(backend)?;
        let matched = existing
            .as_ref()
            .is_none_or(|(existing_owner, existing_token)| {
                existing_owner == owner_id && *existing_token == token
            });
        if matched && existing.is_some() {
            transaction
                .execute("DELETE FROM kv_locks WHERE name = ?1", [name])
                .map_err(backend)?;
        }
        transaction.commit().map_err(backend)?;
        Ok(matched)
    }

    pub(crate) fn import_legacy(&self, import: LegacyKvImport) -> StorageResult<()> {
        if import.objects.is_empty() && import.locks.is_empty() && import.next_fencing_token == 0 {
            return Ok(());
        }
        let mut connection = self.database.connect().map_err(backend)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(backend)?;
        for object in import.objects {
            let value = STANDARD.decode(&object.value_base64).map_err(invalid)?;
            transaction
                .execute(
                    "INSERT OR IGNORE INTO kv_objects(key, value, modified_at_unix_ms)
                     VALUES (?1, ?2, ?3)",
                    params![object.key, value, object.modified_at_unix_ms],
                )
                .map_err(backend)?;
        }
        for lock in import.locks {
            let token = i64::try_from(lock.fencing_token)
                .map_err(|_| invalid("legacy KV fencing token exceeds SQLite integer range"))?;
            transaction
                .execute(
                    "INSERT OR IGNORE INTO kv_locks(name, owner_id, fencing_token, lease_until_unix_ms)
                     VALUES (?1, ?2, ?3, ?4)",
                    params![lock.name, lock.owner_id, token, lock.lease_until_unix_ms],
                )
                .map_err(backend)?;
        }
        let next = i64::try_from(import.next_fencing_token)
            .map_err(|_| invalid("legacy KV fencing token exceeds SQLite integer range"))?;
        transaction
            .execute(
                "UPDATE kv_meta SET next_fencing_token = MAX(next_fencing_token, ?1)
                 WHERE singleton = 1",
                [next],
            )
            .map_err(backend)?;
        transaction.commit().map_err(backend)
    }

    fn keys(&self) -> StorageResult<Vec<String>> {
        let connection = self.database.connect().map_err(backend)?;
        let mut statement = connection
            .prepare("SELECT key FROM kv_objects ORDER BY key")
            .map_err(backend)?;
        statement
            .query_map([], |row| row.get(0))
            .map_err(backend)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(backend)
    }
}

pub(crate) fn decode_put(key: &str, value_base64: &str) -> Result<Vec<u8>, String> {
    validate_key(key)?;
    let value = STANDARD
        .decode(value_base64)
        .map_err(|_| "value_base64 must contain valid base64".to_owned())?;
    if value.len() > MAX_OBJECT_BYTES {
        return Err(format!(
            "KV objects may not exceed {MAX_OBJECT_BYTES} bytes"
        ));
    }
    Ok(value)
}

pub(crate) fn validate_key(key: &str) -> Result<(), String> {
    validate_query_path(key)?;
    if key.is_empty() {
        return Err("KV keys must not be empty".to_owned());
    }
    Ok(())
}

pub(crate) fn validate_query_path(path: &str) -> Result<(), String> {
    if path.len() > MAX_KEY_BYTES {
        return Err(format!("KV keys may not exceed {MAX_KEY_BYTES} bytes"));
    }
    if path.is_empty() {
        return Ok(());
    }
    if path.starts_with('/') || path.ends_with('/') || path.split('/').any(str::is_empty) {
        return Err(
            "KV keys must use non-empty '/'-separated components without a leading or trailing slash"
                .to_owned(),
        );
    }
    Ok(())
}

fn is_component_descendant(key: &str, prefix: &str) -> bool {
    prefix.is_empty()
        || key
            .strip_prefix(prefix)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn join_path(path: &str, relative: &str) -> String {
    if path.is_empty() {
        relative.to_owned()
    } else {
        format!("{path}/{relative}")
    }
}

fn backend(error: impl std::fmt::Display) -> StorageError {
    StorageError::Backend(error.to_string())
}

fn invalid(error: impl std::fmt::Display) -> StorageError {
    StorageError::InvalidData(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn repository() -> (KvRepository, tempfile::TempDir) {
        let directory = tempfile::tempdir().unwrap();
        let database = Database::open(directory.path()).unwrap();
        (KvRepository::open(database).unwrap(), directory)
    }

    #[test]
    fn stores_overwrites_and_recursively_deletes_objects() {
        let (repository, _directory) = repository();
        repository.put("a/cert", b"one", 10).unwrap();
        repository.put("ab/cert", b"two", 11).unwrap();
        repository.put("a/cert", b"new", 20).unwrap();
        assert_eq!(
            STANDARD
                .decode(repository.get("a/cert").unwrap().unwrap().value_base64)
                .unwrap(),
            b"new"
        );
        repository.delete("a", true).unwrap();
        assert!(repository.get("a/cert").unwrap().is_none());
        assert!(repository.get("ab/cert").unwrap().is_some());
    }

    #[test]
    fn lists_direct_and_recursive_prefixes() {
        let (repository, _directory) = repository();
        repository.put("apps/a/config", b"a", 10).unwrap();
        repository.put("apps/b", b"b", 11).unwrap();
        assert_eq!(
            repository.list("apps", false).unwrap().unwrap().keys,
            ["apps/a", "apps/b"]
        );
        assert_eq!(
            repository.list("apps", true).unwrap().unwrap().keys,
            ["apps/a", "apps/a/config", "apps/b"]
        );
    }
}
