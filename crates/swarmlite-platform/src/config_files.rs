use std::{
    collections::{BTreeMap, BTreeSet},
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

use crate::model::{ServiceConfigMount, config_digest};

const GC_STATE_FILE: &str = "gc-state.json";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ConfigCacheGcStats {
    pub referenced: usize,
    pub marked: usize,
    pub retained_for_grace: usize,
    pub deleted: usize,
    pub failures: usize,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigCacheGcState {
    candidates: BTreeMap<String, i64>,
}

#[derive(Debug, Clone)]
pub struct ConfigCache {
    root: PathBuf,
}

impl ConfigCache {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    pub fn host_path(&self, mount: &ServiceConfigMount) -> PathBuf {
        config_mount_host_path(&self.root, mount)
    }

    pub async fn is_ready(&self, mount: &ServiceConfigMount) -> bool {
        let path = config_mount_host_path(&self.root, mount);
        let metadata = match tokio::fs::symlink_metadata(&path).await {
            Ok(metadata) if metadata.file_type().is_file() => metadata,
            _ => return false,
        };
        if metadata.mode() & 0o7777 != effective_mode(mount)
            || mount.uid.is_some_and(|uid| metadata.uid() != uid)
            || mount.gid.is_some_and(|gid| metadata.gid() != gid)
        {
            return false;
        }
        tokio::fs::read(path)
            .await
            .is_ok_and(|contents| config_digest(&contents) == mount.digest)
    }

    pub async fn materialize(
        &self,
        mount: &ServiceConfigMount,
        contents: &[u8],
    ) -> Result<PathBuf> {
        if config_digest(contents) != mount.digest {
            bail!(
                "downloaded config {:?} does not match digest {}",
                mount.source,
                mount.digest
            );
        }
        let directory = self.root.join("mounts");
        tokio::fs::create_dir_all(&directory)
            .await
            .with_context(|| format!("failed to create config cache {}", directory.display()))?;
        tokio::fs::set_permissions(&self.root, std::fs::Permissions::from_mode(0o700))
            .await
            .with_context(|| format!("failed to protect config cache {}", self.root.display()))?;
        tokio::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700))
            .await
            .with_context(|| format!("failed to protect config cache {}", directory.display()))?;

        let destination = config_mount_host_path(&self.root, mount);
        let temporary = directory.join(format!(".tmp-{}", Uuid::new_v4()));
        let result = async {
            let mut file = tokio::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)
                .await
                .with_context(|| {
                    format!("failed to create temporary config {}", temporary.display())
                })?;
            file.write_all(contents).await.with_context(|| {
                format!("failed to write temporary config {}", temporary.display())
            })?;
            file.sync_all().await.with_context(|| {
                format!("failed to sync temporary config {}", temporary.display())
            })?;
            drop(file);
            if mount.uid.is_some() || mount.gid.is_some() {
                std::os::unix::fs::chown(&temporary, mount.uid, mount.gid).with_context(|| {
                    format!("failed to set config ownership on {}", temporary.display())
                })?;
            }
            tokio::fs::set_permissions(
                &temporary,
                std::fs::Permissions::from_mode(effective_mode(mount)),
            )
            .await
            .with_context(|| format!("failed to set config mode on {}", temporary.display()))?;
            tokio::fs::rename(&temporary, &destination)
                .await
                .with_context(|| {
                    format!(
                        "failed to publish config {} to {}",
                        temporary.display(),
                        destination.display()
                    )
                })?;
            anyhow::Ok(())
        }
        .await;
        if result.is_err() {
            let _ = tokio::fs::remove_file(&temporary).await;
        }
        result?;
        Ok(destination)
    }

    pub async fn gc_at(
        &self,
        referenced_paths: &BTreeSet<PathBuf>,
        now_unix_ms: i64,
        grace_period_ms: i64,
    ) -> Result<ConfigCacheGcStats> {
        if grace_period_ms < 0 {
            bail!("config cache GC grace period must not be negative");
        }
        let mounts = self.root.join("mounts");
        let mut directory = match tokio::fs::read_dir(&mounts).await {
            Ok(directory) => directory,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(ConfigCacheGcStats::default());
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("failed to inspect config cache {}", mounts.display())
                });
            }
        };
        let mut entries = BTreeSet::new();
        while let Some(entry) = directory
            .next_entry()
            .await
            .with_context(|| format!("failed to enumerate config cache {}", mounts.display()))?
        {
            let metadata = tokio::fs::symlink_metadata(entry.path())
                .await
                .with_context(|| {
                    format!("failed to inspect cached config {}", entry.path().display())
                })?;
            if metadata.is_file() || metadata.file_type().is_symlink() {
                entries.insert(entry.file_name().to_string_lossy().into_owned());
            }
        }

        let referenced_names = referenced_paths
            .iter()
            .filter_map(|path| {
                path.strip_prefix(&mounts)
                    .ok()
                    .filter(|relative| relative.components().count() == 1)
                    .and_then(|relative| relative.to_str().map(str::to_owned))
            })
            .collect::<BTreeSet<_>>();
        let mut state = self.read_gc_state().await?;
        let original_candidates = state.candidates.clone();
        state.candidates.retain(|name, _| entries.contains(name));
        let mut stats = ConfigCacheGcStats::default();
        for name in entries {
            if referenced_names.contains(&name) {
                stats.referenced += 1;
                state.candidates.remove(&name);
                continue;
            }
            let Some(since) = state.candidates.get(&name).copied() else {
                state.candidates.insert(name, now_unix_ms);
                stats.marked += 1;
                continue;
            };
            if now_unix_ms.saturating_sub(since) < grace_period_ms {
                stats.retained_for_grace += 1;
                continue;
            }
            let path = mounts.join(&name);
            match tokio::fs::remove_file(&path).await {
                Ok(()) => {
                    state.candidates.remove(&name);
                    stats.deleted += 1;
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    state.candidates.remove(&name);
                }
                Err(_) => stats.failures += 1,
            }
        }
        if state.candidates != original_candidates {
            self.write_gc_state(&state).await?;
        }
        Ok(stats)
    }

    async fn read_gc_state(&self) -> Result<ConfigCacheGcState> {
        let path = self.root.join(GC_STATE_FILE);
        match tokio::fs::read(&path).await {
            Ok(contents) => serde_json::from_slice(&contents).with_context(|| {
                format!("failed to parse config cache GC state {}", path.display())
            }),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Ok(ConfigCacheGcState::default())
            }
            Err(error) => Err(error).with_context(|| {
                format!("failed to read config cache GC state {}", path.display())
            }),
        }
    }

    async fn write_gc_state(&self, state: &ConfigCacheGcState) -> Result<()> {
        let path = self.root.join(GC_STATE_FILE);
        let temporary = self
            .root
            .join(format!(".{GC_STATE_FILE}-{}", Uuid::new_v4()));
        let contents =
            serde_json::to_vec(state).context("failed to serialize config cache GC state")?;
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .await
            .with_context(|| {
                format!(
                    "failed to create config cache GC state {}",
                    temporary.display()
                )
            })?;
        let result = async {
            file.write_all(&contents).await?;
            file.sync_all().await?;
            drop(file);
            tokio::fs::set_permissions(&temporary, std::fs::Permissions::from_mode(0o600)).await?;
            tokio::fs::rename(&temporary, &path).await?;
            anyhow::Ok(())
        }
        .await;
        if result.is_err() {
            let _ = tokio::fs::remove_file(&temporary).await;
        }
        result
            .with_context(|| format!("failed to persist config cache GC state {}", path.display()))
    }
}

pub fn config_mount_host_path(root: &Path, mount: &ServiceConfigMount) -> PathBuf {
    let uid = mount
        .uid
        .map_or_else(|| "default".into(), |uid| uid.to_string());
    let gid = mount
        .gid
        .map_or_else(|| "default".into(), |gid| gid.to_string());
    root.join("mounts").join(format!(
        "{}-u{uid}-g{gid}-m{:04o}",
        mount.digest,
        effective_mode(mount)
    ))
}

fn effective_mode(mount: &ServiceConfigMount) -> u32 {
    mount.mode & 0o7555
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn materializes_verified_immutable_config_variants() {
        let directory = tempfile::tempdir().unwrap();
        let contents = b"server { listen :80 }\n";
        let mount = ServiceConfigMount {
            source: "caddy".into(),
            target: "/etc/caddy/Caddyfile".into(),
            uid: None,
            gid: None,
            mode: 0o666,
            digest: config_digest(contents),
        };
        let cache = ConfigCache::new(directory.path().join("configs"));
        let path = cache.materialize(&mount, contents).await.unwrap();

        assert_eq!(tokio::fs::read(&path).await.unwrap(), contents);
        assert_eq!(
            tokio::fs::metadata(&path).await.unwrap().mode() & 0o7777,
            0o444
        );
        assert!(cache.is_ready(&mount).await);

        tokio::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644))
            .await
            .unwrap();
        tokio::fs::write(&path, b"corrupt").await.unwrap();
        assert!(!cache.is_ready(&mount).await);
        cache.materialize(&mount, contents).await.unwrap();
        assert!(cache.is_ready(&mount).await);
    }

    #[tokio::test]
    async fn cache_gc_persists_grace_and_never_deletes_referenced_mounts() {
        let directory = tempfile::tempdir().unwrap();
        let cache = ConfigCache::new(directory.path().join("configs"));
        let make_mount = |source: &str, contents: &[u8]| ServiceConfigMount {
            source: source.into(),
            target: format!("/etc/{source}"),
            uid: None,
            gid: None,
            mode: 0o444,
            digest: config_digest(contents),
        };
        let current = make_mount("current", b"current");
        let old = make_mount("old", b"old");
        let current_path = cache.materialize(&current, b"current").await.unwrap();
        let old_path = cache.materialize(&old, b"old").await.unwrap();
        let referenced = BTreeSet::from([current_path.clone()]);

        let stats = cache.gc_at(&referenced, 1_000, 100).await.unwrap();
        assert_eq!(stats.referenced, 1);
        assert_eq!(stats.marked, 1);
        assert!(old_path.exists());
        let stats = cache.gc_at(&referenced, 1_099, 100).await.unwrap();
        assert_eq!(stats.retained_for_grace, 1);
        assert!(old_path.exists());

        let restarted = ConfigCache::new(directory.path().join("configs"));
        let stats = restarted.gc_at(&referenced, 1_100, 100).await.unwrap();
        assert_eq!(stats.deleted, 1);
        assert!(!old_path.exists());
        assert!(current_path.exists());

        restarted.gc_at(&BTreeSet::new(), 1_200, 100).await.unwrap();
        restarted.gc_at(&referenced, 1_300, 100).await.unwrap();
        assert!(current_path.exists());
        let stats = restarted.gc_at(&BTreeSet::new(), 2_000, 100).await.unwrap();
        assert_eq!(stats.marked, 1);
        assert!(current_path.exists());
    }
}
