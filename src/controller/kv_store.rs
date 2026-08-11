use super::*;

impl Controller {
    pub(super) async fn put_kv(
        &self,
        request: KvPutRequest,
    ) -> Result<KvPutResponse, ControllerError> {
        let mut inner = self.inner.lock().await;
        let previous = inner.kv.clone();
        let response = kv::apply_put(&mut inner.kv, request).map_err(ControllerError::Invalid)?;
        if response.applied
            && let Err(error) = self.commit_kv_locked(&mut inner).await
        {
            inner.kv = previous;
            return Err(error.into());
        }
        Ok(response)
    }

    pub(super) async fn delete_kv(
        &self,
        request: KvDeleteRequest,
    ) -> Result<KvPutResponse, ControllerError> {
        let mut inner = self.inner.lock().await;
        let previous = inner.kv.clone();
        let response =
            kv::apply_delete(&mut inner.kv, request).map_err(ControllerError::Invalid)?;
        if response.applied
            && let Err(error) = self.commit_kv_locked(&mut inner).await
        {
            inner.kv = previous;
            return Err(error.into());
        }
        Ok(response)
    }

    pub(super) async fn kv_object(&self, key: &str) -> Result<KvObjectResponse, ControllerError> {
        let inner = self.inner.lock().await;
        kv::get(&inner.kv, key)
            .map_err(ControllerError::Invalid)?
            .ok_or_else(|| ControllerError::NotFound(format!("KV key {key} was not found")))
    }

    pub(super) async fn list_kv(
        &self,
        path: &str,
        recursive: bool,
    ) -> Result<KvListResponse, ControllerError> {
        let inner = self.inner.lock().await;
        kv::list(&inner.kv, path, recursive)
            .map_err(ControllerError::Invalid)?
            .ok_or_else(|| ControllerError::NotFound(format!("KV path {path} was not found")))
    }

    pub(super) async fn stat_kv(&self, key: &str) -> Result<KvStatResponse, ControllerError> {
        let inner = self.inner.lock().await;
        kv::stat(&inner.kv, key)
            .map_err(ControllerError::Invalid)?
            .ok_or_else(|| ControllerError::NotFound(format!("KV key {key} was not found")))
    }

    pub(super) async fn acquire_kv_lock(
        &self,
        request: KvLockAcquireRequest,
    ) -> Result<KvLockAcquireResponse, ControllerError> {
        validate_kv_lock_identity(&request.name, &request.owner_id)?;
        validate_kv_lock_lease(request.lease_millis)?;
        let mut inner = self.inner.lock().await;

        let now = unix_ms();
        if let Some(lock) = inner.kv.locks.get(&request.name)
            && lock.lease_until_unix_ms > now
            && lock.owner_id != request.owner_id
        {
            return Ok(KvLockAcquireResponse {
                status: KvLockStatus::Busy,
                fencing_token: None,
                lease_until_unix_ms: Some(lock.lease_until_unix_ms),
                retry_after_millis: Some(
                    u64::try_from(lock.lease_until_unix_ms - now)
                        .unwrap_or(1_000)
                        .clamp(100, 1_000),
                ),
            });
        }

        let previous = inner.kv.clone();
        let lease_until_unix_ms = lease_deadline(now, request.lease_millis)?;
        let fencing_token = if let Some(lock) = inner.kv.locks.get_mut(&request.name)
            && lock.lease_until_unix_ms > now
            && lock.owner_id == request.owner_id
        {
            lock.lease_until_unix_ms = lease_until_unix_ms;
            lock.fencing_token
        } else {
            inner.kv.next_fencing_token = inner
                .kv
                .next_fencing_token
                .checked_add(1)
                .ok_or_else(|| ControllerError::Invalid("KV lock token overflow".to_owned()))?;
            let token = inner.kv.next_fencing_token;
            inner.kv.locks.insert(
                request.name,
                KvLock {
                    owner_id: request.owner_id,
                    fencing_token: token,
                    lease_until_unix_ms,
                },
            );
            token
        };
        if let Err(error) = self.commit_kv_locked(&mut inner).await {
            inner.kv = previous;
            return Err(error.into());
        }
        Ok(KvLockAcquireResponse {
            status: KvLockStatus::Acquired,
            fencing_token: Some(fencing_token),
            lease_until_unix_ms: Some(lease_until_unix_ms),
            retry_after_millis: None,
        })
    }

    pub(super) async fn renew_kv_lock(
        &self,
        request: KvLockMutationRequest,
    ) -> Result<(), ControllerError> {
        validate_kv_lock_identity(&request.name, &request.owner_id)?;
        let lease_millis = request
            .lease_millis
            .ok_or_else(|| ControllerError::Invalid("lease_millis is required".to_owned()))?;
        validate_kv_lock_lease(lease_millis)?;
        let mut inner = self.inner.lock().await;
        let now = unix_ms();
        let previous = inner.kv.clone();
        let lock = inner
            .kv
            .locks
            .get_mut(&request.name)
            .filter(|lock| {
                lock.owner_id == request.owner_id
                    && lock.fencing_token == request.fencing_token
                    && lock.lease_until_unix_ms > now
            })
            .ok_or_else(|| {
                ControllerError::Conflict("the KV lock is no longer owned".to_owned())
            })?;
        lock.lease_until_unix_ms = lease_deadline(now, lease_millis)?;
        if let Err(error) = self.commit_kv_locked(&mut inner).await {
            inner.kv = previous;
            return Err(error.into());
        }
        Ok(())
    }

    pub(super) async fn release_kv_lock(
        &self,
        request: KvLockMutationRequest,
    ) -> Result<(), ControllerError> {
        validate_kv_lock_identity(&request.name, &request.owner_id)?;
        let mut inner = self.inner.lock().await;
        let Some(lock) = inner.kv.locks.get(&request.name) else {
            return Ok(());
        };
        if lock.owner_id != request.owner_id || lock.fencing_token != request.fencing_token {
            return Err(ControllerError::Conflict(
                "the KV lock is owned by another writer".to_owned(),
            ));
        }
        let previous = inner.kv.clone();
        inner.kv.locks.remove(&request.name);
        if let Err(error) = self.commit_kv_locked(&mut inner).await {
            inner.kv = previous;
            return Err(error.into());
        }
        Ok(())
    }
}

fn validate_kv_lock_identity(name: &str, owner_id: &str) -> Result<(), ControllerError> {
    if name.trim().is_empty() || name.len() > 1_024 {
        return Err(ControllerError::Invalid(
            "KV lock name must contain 1 to 1024 bytes".to_owned(),
        ));
    }
    if owner_id.trim().is_empty() || owner_id.len() > 512 {
        return Err(ControllerError::Invalid(
            "KV lock owner_id must contain 1 to 512 bytes".to_owned(),
        ));
    }
    Ok(())
}

fn validate_kv_lock_lease(lease_millis: u64) -> Result<(), ControllerError> {
    if !(MIN_KV_LOCK_LEASE_MS..=MAX_KV_LOCK_LEASE_MS).contains(&lease_millis) {
        return Err(ControllerError::Invalid(format!(
            "KV lock lease_millis must be between {MIN_KV_LOCK_LEASE_MS} and {MAX_KV_LOCK_LEASE_MS}"
        )));
    }
    Ok(())
}

fn lease_deadline(now: i64, lease_millis: u64) -> Result<i64, ControllerError> {
    let lease_millis = i64::try_from(lease_millis)
        .map_err(|_| ControllerError::Invalid("KV lock lease is too large".to_owned()))?;
    now.checked_add(lease_millis)
        .ok_or_else(|| ControllerError::Invalid("KV lock lease overflow".to_owned()))
}
