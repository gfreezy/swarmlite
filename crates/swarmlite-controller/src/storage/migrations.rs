//! COMPAT(11 -> 12): the only supported historical storage format.
//! Remove this entire module and its startup call in the next release.
//! Run inside the initialization transaction, before normal storage reads.
use super::*;

pub(super) fn upgrade_11_to_12(
    connection: &Connection,
    cluster: &ClusterSettings,
) -> StorageResult<()> {
    let row = connection
        .query_row(
            "SELECT schema_version, cluster_id, document FROM control_plane WHERE singleton = 1",
            [],
            |row| {
                Ok((
                    row.get::<_, u32>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                ))
            },
        )
        .optional()
        .map_err(backend)?;
    let Some((version, cluster_id, bytes)) = row else {
        return Ok(());
    };
    if version != 11 {
        return Ok(());
    }
    let mut document: serde_json::Value = serde_json::from_slice(&bytes).map_err(invalid)?;
    if cluster_id != cluster.cluster_id
        || document["cluster_id"] != cluster_id
        || document["schema_version"] != 11
    {
        return Err(StorageError::InvalidData(
            "invalid schema 11 cluster identity or document version".into(),
        ));
    }
    // Schema 11 could serialize this no-op cache option. Convert stored snapshots
    // once; current Stack configuration and schema 12 no longer accept it.
    remove_cache_hash(&mut document["state"]);
    document["schema_version"] = serde_json::json!(PERSISTED_SCHEMA_VERSION);
    // Deserialize before writing to validate the complete document. Serde defaults
    // populate the new job fields; serialization writes the current representation.
    let current: PersistedControlPlane = serde_json::from_value(document).map_err(invalid)?;
    if !same_cluster_identity(&current.cluster, cluster) {
        return Err(StorageError::InvalidData(
            "schema 11 belongs to a different cluster".into(),
        ));
    }
    connection.execute(
        "UPDATE control_plane SET schema_version = ?1, document = ?2, generation = generation + 1 WHERE singleton = 1 AND schema_version = 11",
        params![PERSISTED_SCHEMA_VERSION, serde_json::to_vec(&current).map_err(invalid)?],
    ).map_err(backend)?;
    Ok(())
}

fn remove_cache_hash(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(object) => {
            if let Some(key) = object
                .get_mut("cache")
                .and_then(|cache| cache.get_mut("key"))
                .and_then(serde_json::Value::as_object_mut)
            {
                key.remove("hash");
            }
            for value in object.values_mut() {
                remove_cache_hash(value);
            }
        }
        serde_json::Value::Array(array) => {
            for value in array {
                remove_cache_hash(value);
            }
        }
        _ => {}
    }
}
