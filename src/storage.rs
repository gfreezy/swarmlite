use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::{Client, primitives::ByteStream};
use aws_smithy_types::error::metadata::ProvideErrorMetadata;
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{
    config::S3Config,
    model::{ClusterMeta, ClusterState},
};

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("object was modified concurrently")]
    Conflict,
    #[error("object storage error: {0}")]
    Backend(String),
    #[error("invalid persisted data: {0}")]
    InvalidData(String),
}

pub type StorageResult<T> = Result<T, StorageError>;

#[derive(Debug, Clone)]
pub struct StoredObject {
    pub body: Vec<u8>,
    pub etag: String,
}

#[derive(Debug, Clone)]
pub enum PutCondition {
    Unconditional,
    IfAbsent,
    IfMatch(String),
}

#[async_trait]
pub trait ObjectStore: Send + Sync {
    async fn get(&self, key: &str) -> StorageResult<Option<StoredObject>>;
    async fn put(&self, key: &str, body: Vec<u8>, condition: PutCondition)
    -> StorageResult<String>;
}

pub struct S3ObjectStore {
    client: Client,
    bucket: String,
}

impl S3ObjectStore {
    pub async fn new(config: &S3Config) -> StorageResult<Self> {
        let shared = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(config.region.clone()))
            .load()
            .await;
        let mut builder =
            aws_sdk_s3::config::Builder::from(&shared).force_path_style(config.force_path_style);
        if let Some(endpoint) = &config.endpoint_url {
            builder = builder.endpoint_url(endpoint);
        }
        Ok(Self {
            client: Client::from_conf(builder.build()),
            bucket: config.bucket.clone(),
        })
    }
}

#[async_trait]
impl ObjectStore for S3ObjectStore {
    async fn get(&self, key: &str) -> StorageResult<Option<StoredObject>> {
        let output = match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(output) => output,
            Err(error) => {
                if error
                    .as_service_error()
                    .and_then(|value| value.code())
                    .is_some_and(|code| code == "NoSuchKey" || code == "NotFound")
                {
                    return Ok(None);
                }
                return Err(StorageError::Backend(error.to_string()));
            }
        };
        let etag = output
            .e_tag()
            .ok_or_else(|| StorageError::Backend(format!("GET {key} returned no ETag")))?
            .to_owned();
        let body = output
            .body
            .collect()
            .await
            .map_err(|error| StorageError::Backend(error.to_string()))?
            .into_bytes()
            .to_vec();
        Ok(Some(StoredObject { body, etag }))
    }

    async fn put(
        &self,
        key: &str,
        body: Vec<u8>,
        condition: PutCondition,
    ) -> StorageResult<String> {
        let mut request = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .content_type("application/json")
            .body(ByteStream::from(body));
        request = match condition {
            PutCondition::Unconditional => request,
            PutCondition::IfAbsent => request.if_none_match("*"),
            PutCondition::IfMatch(etag) => request.if_match(etag),
        };
        match request.send().await {
            Ok(output) => output
                .e_tag()
                .map(ToOwned::to_owned)
                .ok_or_else(|| StorageError::Backend(format!("PUT {key} returned no ETag"))),
            Err(error) => {
                let code = error.as_service_error().and_then(|value| value.code());
                if code.is_some_and(|value| {
                    value == "PreconditionFailed" || value == "ConditionalRequestConflict"
                }) {
                    Err(StorageError::Conflict)
                } else {
                    Err(StorageError::Backend(error.to_string()))
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct StateRepository {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    cluster_id: String,
}

#[derive(Debug, Clone)]
pub struct VersionedMeta {
    pub value: ClusterMeta,
    pub etag: String,
}

impl StateRepository {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: String, cluster_id: String) -> Self {
        Self {
            store,
            prefix: prefix.trim_matches('/').to_owned(),
            cluster_id,
        }
    }

    pub fn from_s3(store: Arc<dyn ObjectStore>, config: &S3Config, cluster_id: String) -> Self {
        Self::new(store, config.prefix.clone(), cluster_id)
    }

    pub async fn initialize(&self) -> StorageResult<VersionedMeta> {
        if let Some(meta) = self.load_meta().await? {
            self.verify_cluster(&meta.value)?;
            return Ok(meta);
        }

        let snapshot_key = self.snapshot_key(0);
        let snapshot = encode(&ClusterState::default())?;
        match self
            .store
            .put(&snapshot_key, snapshot, PutCondition::IfAbsent)
            .await
        {
            Ok(_) | Err(StorageError::Conflict) => {}
            Err(error) => return Err(error),
        }

        let meta = ClusterMeta {
            schema_version: 1,
            cluster_id: self.cluster_id.clone(),
            leader: None,
            generation: 0,
            snapshot_key,
        };
        match self
            .store
            .put(&self.meta_key(), encode(&meta)?, PutCondition::IfAbsent)
            .await
        {
            Ok(etag) => Ok(VersionedMeta { value: meta, etag }),
            Err(StorageError::Conflict) => self.load_meta().await?.ok_or_else(|| {
                StorageError::Backend("meta object disappeared after concurrent create".to_owned())
            }),
            Err(error) => Err(error),
        }
    }

    pub async fn load_meta(&self) -> StorageResult<Option<VersionedMeta>> {
        self.store
            .get(&self.meta_key())
            .await?
            .map(|object| {
                let value: ClusterMeta = decode(&object.body)?;
                self.verify_cluster(&value)?;
                Ok(VersionedMeta {
                    value,
                    etag: object.etag,
                })
            })
            .transpose()
    }

    pub async fn load_state(&self, meta: &ClusterMeta) -> StorageResult<ClusterState> {
        let object = self.store.get(&meta.snapshot_key).await?.ok_or_else(|| {
            StorageError::Backend(format!("snapshot {} does not exist", meta.snapshot_key))
        })?;
        decode(&object.body)
    }

    pub async fn put_snapshot(
        &self,
        generation: u64,
        state: &ClusterState,
    ) -> StorageResult<String> {
        let key = self.snapshot_key(generation);
        self.store
            .put(&key, encode(state)?, PutCondition::IfAbsent)
            .await?;
        Ok(key)
    }

    pub async fn cas_meta(&self, meta: &ClusterMeta, expected_etag: &str) -> StorageResult<String> {
        self.store
            .put(
                &self.meta_key(),
                encode(meta)?,
                PutCondition::IfMatch(expected_etag.to_owned()),
            )
            .await
    }

    fn verify_cluster(&self, meta: &ClusterMeta) -> StorageResult<()> {
        if meta.cluster_id == self.cluster_id {
            Ok(())
        } else {
            Err(StorageError::InvalidData(format!(
                "expected cluster {}, storage contains {}",
                self.cluster_id, meta.cluster_id
            )))
        }
    }

    fn meta_key(&self) -> String {
        self.key("meta.json")
    }

    fn snapshot_key(&self, generation: u64) -> String {
        self.key(&format!(
            "snapshots/{generation:020}-{}.json",
            Uuid::new_v4()
        ))
    }

    fn key(&self, suffix: &str) -> String {
        if self.prefix.is_empty() {
            suffix.to_owned()
        } else {
            format!("{}/{}", self.prefix, suffix.trim_start_matches('/'))
        }
    }
}

fn encode<T: Serialize>(value: &T) -> StorageResult<Vec<u8>> {
    serde_json::to_vec(value).map_err(|error| StorageError::InvalidData(error.to_string()))
}

fn decode<T: DeserializeOwned>(value: &[u8]) -> StorageResult<T> {
    serde_json::from_slice(value).map_err(|error| StorageError::InvalidData(error.to_string()))
}

#[derive(Default)]
pub struct MemoryObjectStore {
    objects: Mutex<HashMap<String, StoredObject>>,
    sequence: Mutex<u64>,
}

#[async_trait]
impl ObjectStore for MemoryObjectStore {
    async fn get(&self, key: &str) -> StorageResult<Option<StoredObject>> {
        Ok(self.objects.lock().await.get(key).cloned())
    }

    async fn put(
        &self,
        key: &str,
        body: Vec<u8>,
        condition: PutCondition,
    ) -> StorageResult<String> {
        let mut objects = self.objects.lock().await;
        let allowed = match &condition {
            PutCondition::Unconditional => true,
            PutCondition::IfAbsent => !objects.contains_key(key),
            PutCondition::IfMatch(expected) => objects
                .get(key)
                .is_some_and(|object| object.etag == *expected),
        };
        if !allowed {
            return Err(StorageError::Conflict);
        }
        let mut sequence = self.sequence.lock().await;
        *sequence += 1;
        let etag = format!("\"{}\"", *sequence);
        objects.insert(
            key.to_owned(),
            StoredObject {
                body,
                etag: etag.clone(),
            },
        );
        Ok(etag)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn initializes_once_and_enforces_cas() {
        let store = Arc::new(MemoryObjectStore::default());
        let repo = StateRepository::new(store, "clusters/test".into(), "test".into());
        let first = repo.initialize().await.unwrap();
        let second = repo.initialize().await.unwrap();
        assert_eq!(first.value.snapshot_key, second.value.snapshot_key);

        let mut changed = first.value.clone();
        changed.generation = 1;
        let new_etag = repo.cas_meta(&changed, &first.etag).await.unwrap();
        assert_ne!(new_etag, first.etag);
        assert!(matches!(
            repo.cas_meta(&changed, &first.etag).await,
            Err(StorageError::Conflict)
        ));
    }
}
