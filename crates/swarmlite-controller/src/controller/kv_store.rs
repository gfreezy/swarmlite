use super::*;

impl Controller {
    pub(super) async fn put_kv(&self, request: KvPutRequest) -> Result<(), ControllerError> {
        let value = kv::decode_put(&request.key, &request.value_base64)
            .map_err(ControllerError::Invalid)?;
        self.kv_repository
            .put(&request.key, &value, unix_ms())
            .map_err(Into::into)
    }

    pub(super) async fn delete_kv(&self, request: KvDeleteRequest) -> Result<(), ControllerError> {
        kv::validate_key(&request.key).map_err(ControllerError::Invalid)?;
        self.kv_repository
            .delete(&request.key, request.recursive)
            .map_err(Into::into)
    }

    pub(super) async fn kv_object(&self, key: &str) -> Result<KvObjectResponse, ControllerError> {
        kv::validate_key(key).map_err(ControllerError::Invalid)?;
        self.kv_repository
            .get(key)?
            .ok_or_else(|| ControllerError::NotFound(format!("KV key {key} was not found")))
    }

    pub(super) async fn list_kv(
        &self,
        path: &str,
        recursive: bool,
    ) -> Result<KvListResponse, ControllerError> {
        kv::validate_query_path(path).map_err(ControllerError::Invalid)?;
        self.kv_repository
            .list(path, recursive)?
            .ok_or_else(|| ControllerError::NotFound(format!("KV path {path} was not found")))
    }

    pub(super) async fn stat_kv(&self, key: &str) -> Result<KvStatResponse, ControllerError> {
        kv::validate_query_path(key).map_err(ControllerError::Invalid)?;
        self.kv_repository
            .stat(key)?
            .ok_or_else(|| ControllerError::NotFound(format!("KV key {key} was not found")))
    }

    pub(super) async fn acquire_kv_lock(
        &self,
        request: KvLockAcquireRequest,
    ) -> Result<KvLockAcquireResponse, ControllerError> {
        validate_kv_lock_identity(&request.name, &request.owner_id)?;
        validate_kv_lock_lease(request.lease_millis)?;
        let now = unix_ms();
        let deadline = lease_deadline(now, request.lease_millis)?;
        self.kv_repository
            .acquire_lock(&request.name, &request.owner_id, now, deadline)
            .map_err(Into::into)
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
        let now = unix_ms();
        let deadline = lease_deadline(now, lease_millis)?;
        if self.kv_repository.renew_lock(
            &request.name,
            &request.owner_id,
            request.fencing_token,
            now,
            deadline,
        )? {
            Ok(())
        } else {
            Err(ControllerError::Conflict(
                "the KV lock is no longer owned".to_owned(),
            ))
        }
    }

    pub(super) async fn release_kv_lock(
        &self,
        request: KvLockMutationRequest,
    ) -> Result<(), ControllerError> {
        validate_kv_lock_identity(&request.name, &request.owner_id)?;
        if self.kv_repository.release_lock(
            &request.name,
            &request.owner_id,
            request.fencing_token,
        )? {
            Ok(())
        } else {
            Err(ControllerError::Conflict(
                "the KV lock is owned by another writer".to_owned(),
            ))
        }
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
