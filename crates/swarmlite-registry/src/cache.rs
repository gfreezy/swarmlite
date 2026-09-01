use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex as StdMutex},
    time::{Duration, SystemTime},
};

use anyhow::{Context, Result, bail};
use filetime::FileTime;
use tokio::{fs, io::AsyncWriteExt, sync::Mutex};
use tracing::warn;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct RegistryCacheConfig {
    pub root: PathBuf,
    pub ttl: Duration,
    pub gc_interval: Duration,
    pub partial_ttl: Duration,
}

impl RegistryCacheConfig {
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            ttl: Duration::from_secs(30 * 60),
            gc_interval: Duration::from_secs(5 * 60),
            partial_ttl: Duration::from_secs(60 * 60),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RegistryCacheStats {
    pub objects: u64,
    pub bytes: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct CachedObject {
    pub path: PathBuf,
    pub size: u64,
}

#[derive(Clone)]
pub(crate) struct RegistryCache {
    config: Arc<RegistryCacheConfig>,
    active: Arc<StdMutex<HashMap<PathBuf, usize>>>,
    maintenance: Arc<Mutex<()>>,
}

impl RegistryCache {
    pub async fn open(config: RegistryCacheConfig) -> Result<Self> {
        fs::create_dir_all(config.root.join("objects"))
            .await
            .with_context(|| {
                format!(
                    "failed to create registry cache at {}",
                    config.root.display()
                )
            })?;
        fs::create_dir_all(config.root.join("tmp")).await?;
        let cache = Self {
            config: Arc::new(config),
            active: Arc::new(StdMutex::new(HashMap::new())),
            maintenance: Arc::new(Mutex::new(())),
        };
        cache.gc().await?;
        cache.spawn_gc();
        Ok(cache)
    }

    pub async fn get(&self, digest: &str) -> Result<Option<CachedObject>> {
        let path = self.object_path(digest)?;
        let metadata = match fs::metadata(&path).await {
            Ok(metadata) if metadata.is_file() => metadata,
            Ok(_) => return Ok(None),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let touched = path.clone();
        tokio::task::spawn_blocking(move || filetime::set_file_mtime(touched, FileTime::now()))
            .await
            .context("registry cache touch task failed")??;
        Ok(Some(CachedObject {
            path,
            size: metadata.len(),
        }))
    }

    pub async fn store_bytes(&self, digest: &str, bytes: &[u8]) -> Result<Option<CachedObject>> {
        if let Some(object) = self.get(digest).await? {
            return Ok(Some(object));
        }
        let temp = self.temp_path();
        fs::write(&temp, bytes).await?;
        self.commit_temp(digest, temp).await.map(Some)
    }

    pub async fn begin_write(
        &self,
        digest: &str,
        expected_size: Option<u64>,
    ) -> Result<CacheWriter> {
        self.object_path(digest)?;
        let temp = self.temp_path();
        let file = fs::File::create(&temp).await?;
        Ok(CacheWriter {
            cache: self.clone(),
            digest: digest.to_owned(),
            temp,
            file: Some(file),
            expected_size,
            written: 0,
        })
    }

    pub fn lease(&self, object: &CachedObject) -> CacheLease {
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *active.entry(object.path.clone()).or_default() += 1;
        CacheLease {
            path: object.path.clone(),
            active: self.active.clone(),
        }
    }

    pub async fn stats(&self) -> Result<RegistryCacheStats> {
        let root = self.config.root.join("objects");
        tokio::task::spawn_blocking(move || {
            scan_objects(&root).map(|objects| RegistryCacheStats {
                objects: u64::try_from(objects.len()).unwrap_or(u64::MAX),
                bytes: objects.iter().map(|object| object.size).sum(),
            })
        })
        .await
        .context("registry cache scan task failed")?
    }

    async fn commit_temp(&self, digest: &str, temp: PathBuf) -> Result<CachedObject> {
        let path = self.object_path(digest)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        match fs::rename(&temp, &path).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let _ = fs::remove_file(&temp).await;
            }
            Err(error) => return Err(error.into()),
        }
        let size = fs::metadata(&path).await?.len();
        Ok(CachedObject { path, size })
    }

    fn object_path(&self, digest: &str) -> Result<PathBuf> {
        let (algorithm, encoded) = digest
            .split_once(':')
            .context("invalid registry object digest")?;
        if algorithm != "sha256"
            || encoded.len() != 64
            || !encoded.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            bail!("unsupported registry object digest {digest:?}");
        }
        Ok(self
            .config
            .root
            .join("objects/sha256")
            .join(&encoded[..2])
            .join(encoded))
    }

    fn temp_path(&self) -> PathBuf {
        self.config
            .root
            .join("tmp")
            .join(format!("{}.part", Uuid::new_v4()))
    }

    fn spawn_gc(&self) {
        let cache = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(cache.config.gc_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            interval.tick().await;
            loop {
                interval.tick().await;
                if let Err(error) = cache.gc().await {
                    warn!(%error, "registry cache GC failed");
                }
            }
        });
    }

    pub async fn gc(&self) -> Result<RegistryCacheStats> {
        let _guard = self.maintenance.lock().await;
        self.gc_locked().await
    }

    async fn gc_locked(&self) -> Result<RegistryCacheStats> {
        let config = self.config.clone();
        let active = self
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        tokio::task::spawn_blocking(move || gc_sync(&config, &active))
            .await
            .context("registry cache GC task failed")?
    }
}

pub(crate) struct CacheWriter {
    cache: RegistryCache,
    digest: String,
    temp: PathBuf,
    file: Option<fs::File>,
    expected_size: Option<u64>,
    written: u64,
}

impl CacheWriter {
    pub async fn write(&mut self, bytes: &[u8]) -> Result<()> {
        self.file
            .as_mut()
            .context("registry cache writer is already closed")?
            .write_all(bytes)
            .await?;
        self.written = self
            .written
            .saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        Ok(())
    }

    pub fn has_expected_size(&self) -> bool {
        self.expected_size == Some(self.written)
    }

    pub async fn commit(mut self) -> Result<CachedObject> {
        let file = self
            .file
            .take()
            .context("registry cache writer is already closed")?;
        file.sync_all().await?;
        drop(file);
        if let Some(expected) = self.expected_size
            && self.written != expected
        {
            let _ = fs::remove_file(&self.temp).await;
            bail!(
                "registry object {} expected {expected} bytes but received {}",
                self.digest,
                self.written
            );
        }
        self.cache
            .commit_temp(&self.digest, self.temp.clone())
            .await
    }

    pub async fn abort(mut self) {
        self.file.take();
        let _ = fs::remove_file(&self.temp).await;
    }
}

pub(crate) struct CacheLease {
    path: PathBuf,
    active: Arc<StdMutex<HashMap<PathBuf, usize>>>,
}

impl Drop for CacheLease {
    fn drop(&mut self) {
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(count) = active.get_mut(&self.path) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                active.remove(&self.path);
            }
        }
    }
}

#[derive(Debug)]
struct DiskObject {
    path: PathBuf,
    size: u64,
    modified: SystemTime,
}

fn scan_objects(root: &Path) -> Result<Vec<DiskObject>> {
    let mut result = Vec::new();
    if !root.exists() {
        return Ok(result);
    }
    let mut directories = vec![root.to_owned()];
    while let Some(directory) = directories.pop() {
        for entry in std::fs::read_dir(directory)? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            if metadata.is_dir() {
                directories.push(entry.path());
            } else if metadata.is_file() {
                result.push(DiskObject {
                    path: entry.path(),
                    size: metadata.len(),
                    modified: metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
                });
            }
        }
    }
    Ok(result)
}

fn gc_sync(
    config: &RegistryCacheConfig,
    active: &HashMap<PathBuf, usize>,
) -> Result<RegistryCacheStats> {
    let now = SystemTime::now();
    let tmp = config.root.join("tmp");
    if tmp.exists() {
        for entry in std::fs::read_dir(&tmp)? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            if metadata.is_file()
                && now
                    .duration_since(metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH))
                    .unwrap_or_default()
                    >= config.partial_ttl
            {
                let _ = std::fs::remove_file(entry.path());
            }
        }
    }

    let objects = scan_objects(&config.root.join("objects"))?;
    for object in &objects {
        if !active.contains_key(&object.path)
            && now.duration_since(object.modified).unwrap_or_default() >= config.ttl
        {
            let _ = std::fs::remove_file(&object.path);
        }
    }
    let objects = scan_objects(&config.root.join("objects"))?;
    Ok(RegistryCacheStats {
        objects: u64::try_from(objects.len()).unwrap_or(u64::MAX),
        bytes: objects.iter().map(|object| object.size).sum(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest(byte: char) -> String {
        format!("sha256:{}", byte.to_string().repeat(64))
    }

    #[tokio::test]
    async fn stores_objects_and_expires_them_by_time() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = RegistryCacheConfig::new(directory.path().to_owned());
        config.ttl = Duration::ZERO;
        let cache = RegistryCache::open(config).await.unwrap();
        cache
            .store_bytes(&digest('a'), b"hello")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cache.stats().await.unwrap().objects, 1);
        cache.gc().await.unwrap();
        assert_eq!(cache.stats().await.unwrap().objects, 0);
    }

    #[tokio::test]
    async fn streaming_writer_commits_only_after_the_expected_length() {
        let directory = tempfile::tempdir().unwrap();
        let cache = RegistryCache::open(RegistryCacheConfig::new(directory.path().to_owned()))
            .await
            .unwrap();
        let object_digest = digest('b');
        let mut writer = cache.begin_write(&object_digest, Some(5)).await.unwrap();
        writer.write(b"he").await.unwrap();
        assert!(cache.get(&object_digest).await.unwrap().is_none());
        writer.write(b"llo").await.unwrap();
        writer.commit().await.unwrap();
        assert_eq!(cache.get(&object_digest).await.unwrap().unwrap().size, 5);

        let incomplete_digest = digest('c');
        let mut writer = cache
            .begin_write(&incomplete_digest, Some(5))
            .await
            .unwrap();
        writer.write(b"no").await.unwrap();
        assert!(writer.commit().await.is_err());
        assert!(cache.get(&incomplete_digest).await.unwrap().is_none());
    }
}
