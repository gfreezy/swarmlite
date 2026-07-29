use std::collections::BTreeSet;

use base64::{Engine as _, engine::general_purpose::STANDARD};

use crate::model::{
    KvDeleteRequest, KvListResponse, KvObject, KvObjectResponse, KvPutRequest, KvPutResponse,
    KvStatResponse, KvState, KvVersion,
};

const MAX_KEY_BYTES: usize = 1_024;
const MAX_OBJECT_BYTES: usize = 4 * 1024 * 1024;
const MAX_REPLICA_ID_BYTES: usize = 256;

pub(crate) fn apply_put(
    state: &mut KvState,
    request: KvPutRequest,
) -> Result<KvPutResponse, String> {
    validate_key(&request.key)?;
    validate_version(&request.version)?;
    validate_modified_at(request.modified_at_unix_ms)?;
    let size = STANDARD
        .decode(&request.value_base64)
        .map_err(|_| "value_base64 must contain valid base64".to_owned())?
        .len();
    if size > MAX_OBJECT_BYTES {
        return Err(format!(
            "KV objects may not exceed {MAX_OBJECT_BYTES} bytes"
        ));
    }

    let existing = state.objects.get(&request.key);
    let applied = existing.is_none_or(|object| request.version > object.version);
    let version = if applied {
        state.objects.insert(
            request.key,
            KvObject {
                value_base64: request.value_base64,
                version: request.version.clone(),
                modified_at_unix_ms: request.modified_at_unix_ms,
                tombstone: false,
            },
        );
        request.version
    } else {
        existing.expect("checked above").version.clone()
    };
    Ok(KvPutResponse { applied, version })
}

pub(crate) fn apply_delete(
    state: &mut KvState,
    request: KvDeleteRequest,
) -> Result<KvPutResponse, String> {
    validate_key(&request.key)?;
    validate_version(&request.version)?;
    validate_modified_at(request.modified_at_unix_ms)?;
    if request.recursive {
        let existing = state.prefix_tombstones.get(&request.key);
        let applied = existing.is_none_or(|version| request.version > *version);
        let version = if applied {
            state
                .prefix_tombstones
                .insert(request.key, request.version.clone());
            request.version
        } else {
            existing.expect("checked above").clone()
        };
        return Ok(KvPutResponse { applied, version });
    }

    let existing = state.objects.get(&request.key);
    let applied = existing.is_none_or(|object| request.version > object.version);
    let version = if applied {
        state.objects.insert(
            request.key,
            KvObject {
                value_base64: String::new(),
                version: request.version.clone(),
                modified_at_unix_ms: request.modified_at_unix_ms,
                tombstone: true,
            },
        );
        request.version
    } else {
        existing.expect("checked above").version.clone()
    };
    Ok(KvPutResponse { applied, version })
}

pub(crate) fn get(state: &KvState, key: &str) -> Result<Option<KvObjectResponse>, String> {
    validate_key(key)?;
    let Some(object) = visible_object(state, key) else {
        return Ok(None);
    };
    let size = STANDARD
        .decode(&object.value_base64)
        .map_err(|_| "persisted KV value contains invalid base64".to_owned())?
        .len() as u64;
    Ok(Some(KvObjectResponse {
        key: key.to_owned(),
        value_base64: object.value_base64.clone(),
        version: object.version.clone(),
        modified_at_unix_ms: object.modified_at_unix_ms,
        size,
    }))
}

pub(crate) fn exists(state: &KvState, key: &str) -> Result<bool, String> {
    validate_query_path(key)?;
    Ok(visible_keys(state)
        .any(|candidate| candidate == key || is_component_descendant(candidate, key)))
}

pub(crate) fn list(
    state: &KvState,
    path: &str,
    recursive: bool,
) -> Result<Option<KvListResponse>, String> {
    validate_query_path(path)?;
    if !exists(state, path)? {
        return Ok(None);
    }

    let mut keys = BTreeSet::new();
    for key in visible_keys(state) {
        if !is_component_descendant(key, path) {
            continue;
        }
        let relative = if path.is_empty() {
            key
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
                keys.insert(join_path(path, &current));
            }
        } else if let Some((first, _)) = relative.split_once('/') {
            keys.insert(join_path(path, first));
        } else {
            keys.insert(join_path(path, relative));
        }
    }
    Ok(Some(KvListResponse {
        keys: keys.into_iter().collect(),
    }))
}

pub(crate) fn stat(state: &KvState, key: &str) -> Result<Option<KvStatResponse>, String> {
    validate_query_path(key)?;
    if !key.is_empty()
        && let Some(object) = get(state, key)?
    {
        return Ok(Some(KvStatResponse {
            key: key.to_owned(),
            modified_at_unix_ms: object.modified_at_unix_ms,
            size: object.size,
            is_value: true,
        }));
    }

    let modified_at_unix_ms = visible_keys(state)
        .filter(|candidate| is_component_descendant(candidate, key))
        .filter_map(|candidate| visible_object(state, candidate))
        .map(|object| object.modified_at_unix_ms)
        .max();
    Ok(
        modified_at_unix_ms.map(|modified_at_unix_ms| KvStatResponse {
            key: key.to_owned(),
            modified_at_unix_ms,
            size: 0,
            is_value: false,
        }),
    )
}

fn visible_keys(state: &KvState) -> impl Iterator<Item = &str> {
    state.objects.iter().filter_map(|(key, object)| {
        (!object.tombstone && !hidden_by_prefix_tombstone(state, key, &object.version))
            .then_some(key.as_str())
    })
}

fn visible_object<'a>(state: &'a KvState, key: &str) -> Option<&'a KvObject> {
    let object = state.objects.get(key)?;
    (!object.tombstone && !hidden_by_prefix_tombstone(state, key, &object.version))
        .then_some(object)
}

fn hidden_by_prefix_tombstone(state: &KvState, key: &str, version: &KvVersion) -> bool {
    state.prefix_tombstones.iter().any(|(prefix, tombstone)| {
        (key == prefix || is_component_descendant(key, prefix)) && tombstone >= version
    })
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

fn validate_key(key: &str) -> Result<(), String> {
    validate_query_path(key)?;
    if key.is_empty() {
        return Err("KV keys must not be empty".to_owned());
    }
    Ok(())
}

fn validate_query_path(path: &str) -> Result<(), String> {
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

fn validate_version(version: &KvVersion) -> Result<(), String> {
    if version.physical_unix_ms < 0 {
        return Err("KV version time must not be negative".to_owned());
    }
    if version.replica_id.trim().is_empty() || version.replica_id.len() > MAX_REPLICA_ID_BYTES {
        return Err(format!(
            "KV replica_id must contain 1 to {MAX_REPLICA_ID_BYTES} bytes"
        ));
    }
    Ok(())
}

fn validate_modified_at(modified_at_unix_ms: i64) -> Result<(), String> {
    if modified_at_unix_ms < 0 {
        return Err("KV modified_at_unix_ms must not be negative".to_owned());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn version(time: i64, replica: &str) -> KvVersion {
        KvVersion {
            physical_unix_ms: time,
            logical: 0,
            replica_id: replica.into(),
        }
    }

    fn put(state: &mut KvState, key: &str, value: &str, time: i64) -> bool {
        apply_put(
            state,
            KvPutRequest {
                key: key.into(),
                value_base64: STANDARD.encode(value),
                version: version(time, "node-a"),
                modified_at_unix_ms: time,
            },
        )
        .unwrap()
        .applied
    }

    #[test]
    fn last_write_wins_and_stale_updates_are_ignored() {
        let mut state = KvState::default();
        assert!(put(&mut state, "apps/demo/config", "new", 20));
        assert!(!put(&mut state, "apps/demo/config", "old", 10));
        assert_eq!(
            STANDARD
                .decode(
                    get(&state, "apps/demo/config")
                        .unwrap()
                        .unwrap()
                        .value_base64
                )
                .unwrap(),
            b"new"
        );
    }

    #[test]
    fn recursive_tombstone_does_not_match_similar_component() {
        let mut state = KvState::default();
        put(&mut state, "a/cert", "one", 10);
        put(&mut state, "ab/cert", "two", 10);
        apply_delete(
            &mut state,
            KvDeleteRequest {
                key: "a".into(),
                version: version(20, "node-a"),
                modified_at_unix_ms: 20,
                recursive: true,
            },
        )
        .unwrap();
        assert!(get(&state, "a/cert").unwrap().is_none());
        assert!(get(&state, "ab/cert").unwrap().is_some());
        assert!(put(&mut state, "a/cert", "three", 30));
        assert!(get(&state, "a/cert").unwrap().is_some());
    }

    #[test]
    fn exact_delete_is_lww_and_allows_a_newer_write() {
        let mut state = KvState::default();
        put(&mut state, "apps/demo/config", "one", 10);
        apply_delete(
            &mut state,
            KvDeleteRequest {
                key: "apps/demo/config".into(),
                version: version(20, "node-a"),
                modified_at_unix_ms: 20,
                recursive: false,
            },
        )
        .unwrap();
        assert!(get(&state, "apps/demo/config").unwrap().is_none());
        assert!(!put(&mut state, "apps/demo/config", "stale", 15));
        assert!(put(&mut state, "apps/demo/config", "two", 30));
        assert!(get(&state, "apps/demo/config").unwrap().is_some());
    }

    #[test]
    fn list_supports_direct_and_recursive_prefixes() {
        let mut state = KvState::default();
        put(&mut state, "apps/a/config", "a", 10);
        put(&mut state, "apps/b", "b", 10);
        assert_eq!(
            list(&state, "apps", false).unwrap().unwrap().keys,
            ["apps/a", "apps/b"]
        );
        assert_eq!(
            list(&state, "apps", true).unwrap().unwrap().keys,
            ["apps/a", "apps/a/config", "apps/b"]
        );
    }
}
