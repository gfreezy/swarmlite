use super::*;

impl Controller {
    pub(super) async fn gateway(&self) -> Result<gateway::HttpServer, ControllerError> {
        let inner = self.inner.lock().await;
        Ok(gateway::generate(&inner.state, &self.config.gateway.listen))
    }

    pub(super) async fn gateway_sync_loop(self: Arc<Self>) {
        let mut ticker = tokio::time::interval(Duration::from_secs(
            self.config.gateway.resync_interval_seconds,
        ));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                _ = self.gateway_notify.notified() => {}
            }
            loop {
                match self.sync_gateway_once().await {
                    Ok(()) => break,
                    Err(error) => {
                        warn!(%error, "gateway configuration sync failed");
                        tokio::select! {
                            _ = tokio::time::sleep(Duration::from_secs(
                                self.config.gateway.retry_interval_seconds,
                            )) => {}
                            _ = self.gateway_notify.notified() => {}
                        }
                    }
                }
            }
        }
    }

    pub(super) async fn sync_gateway_once(&self) -> Result<(), String> {
        let (generation, server, storage, endpoints) = {
            let inner = self.inner.lock().await;
            (
                inner.generation,
                gateway::generate(&inner.state, &self.config.gateway.listen),
                gateway::storage(self.config.advertise_url.clone()),
                gateway_endpoints(&inner.state, self.config.gateway.admin_port),
            )
        };

        if endpoints.is_empty() {
            let mut sync = self.gateway_sync.lock().await;
            sync.applied_generation = None;
            sync.endpoint_errors.clear();
            return Ok(());
        }

        let results = join_all(endpoints.iter().map(|endpoint| async {
            match self.push_gateway_storage(endpoint, &storage).await {
                Ok(()) => (true, self.push_gateway_server(endpoint, &server).await),
                Err(error) => (false, Err(error)),
            }
        }))
        .await;
        let mut endpoint_errors = BTreeMap::new();
        for (endpoint, (_, result)) in endpoints.iter().cloned().zip(results) {
            if let Err(error) = result {
                endpoint_errors.insert(endpoint, error);
            }
        }
        {
            let mut sync = self.gateway_sync.lock().await;
            sync.endpoint_errors = endpoint_errors.clone();
            if endpoint_errors.is_empty() {
                sync.applied_generation = Some(generation);
            }
        }
        if !endpoint_errors.is_empty() {
            return Err(endpoint_errors
                .into_iter()
                .map(|(endpoint, error)| format!("{endpoint}: {error}"))
                .collect::<Vec<_>>()
                .join("; "));
        }

        info!(generation, "gateway configuration applied");
        let mut inner = self.inner.lock().await;
        if inner.generation != generation {
            self.gateway_notify.notify_one();
            return Ok(());
        }
        let deadline = unix_ms() + self.config.gateway.drain_timeout_seconds as i64 * 1000;
        let previous = inner.state.clone();
        let mut changed = false;
        for task in inner.state.tasks.values_mut() {
            if task.desired == DesiredTaskState::Draining && task.drain_until_unix_ms.is_none() {
                task.drain_until_unix_ms = Some(deadline);
                changed = true;
            }
        }
        if changed && let Err(error) = self.commit_locked(&mut inner).await {
            inner.state = previous;
            return Err(error.to_string());
        }
        Ok(())
    }

    pub(super) async fn push_gateway_server(
        &self,
        endpoint: &str,
        server: &gateway::HttpServer,
    ) -> Result<(), String> {
        let url = format!(
            "{}/config/apps/http/servers/{}",
            endpoint.trim_end_matches('/'),
            self.config.gateway.server_name
        );
        let response = self
            .gateway_client
            .post(&url)
            .json(server)
            .send()
            .await
            .map_err(|error| error.to_string())?;
        if response.status().is_success() {
            return Ok(());
        }
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        let body = body.chars().take(512).collect::<String>();
        Err(format!("{status} {body}"))
    }

    pub(super) async fn push_gateway_storage(
        &self,
        endpoint: &str,
        storage: &gateway::StorageConfig,
    ) -> Result<(), String> {
        let url = format!("{}/config/storage", endpoint.trim_end_matches('/'));
        let response = self
            .gateway_client
            .post(&url)
            .json(storage)
            .send()
            .await
            .map_err(|error| error.to_string())?;
        if response.status().is_success() {
            return Ok(());
        }
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        let body = body.chars().take(512).collect::<String>();
        Err(format!("{status} {body}"))
    }
}
