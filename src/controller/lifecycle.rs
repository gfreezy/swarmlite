use super::*;

impl Controller {
    pub(super) async fn new(
        config: ControllerConfig,
        token: String,
        repository: StateRepository,
    ) -> Result<Self, StorageError> {
        let mut versioned = repository.initialize_with_cluster(&config.cluster).await?;
        let controller_id = config.cluster.controller_id.clone();
        let gateway_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.gateway.request_timeout_seconds))
            .build()
            .map_err(|error| StorageError::Backend(error.to_string()))?;
        let live_nodes = versioned
            .state
            .tasks
            .values()
            .map(|task| (task.node_id.clone(), Instant::now()))
            .collect();

        let mut changed = false;
        for task in versioned.state.tasks.values_mut() {
            if task.desired == DesiredTaskState::Draining
                && task.drain_until_unix_ms.take().is_some()
            {
                changed = true;
            }
        }
        let now = unix_ms();
        let address = reqwest::Url::parse(&config.advertise_url)
            .ok()
            .and_then(|url| url.host_str().map(ToOwned::to_owned))
            .unwrap_or_default();
        let (gateway_enabled, labels, joined_at_unix_ms) = versioned
            .state
            .members
            .get(&controller_id)
            .map(|member| {
                (
                    member.gateway_enabled,
                    member.labels.clone(),
                    member.joined_at_unix_ms,
                )
            })
            .unwrap_or_else(|| (config.gateway_enabled, config.labels.clone(), now));
        let controller_member = NodeMember {
            id: controller_id.clone(),
            address,
            gateway_enabled,
            labels,
            joined_at_unix_ms,
        };
        if versioned
            .state
            .members
            .get(&controller_id)
            .is_none_or(|member| member != &controller_member)
        {
            versioned
                .state
                .members
                .insert(controller_id.clone(), controller_member);
            changed = true;
        }
        if changed {
            versioned.generation = repository
                .replace(
                    versioned.generation,
                    &versioned.cluster,
                    &versioned.state,
                    &versioned.kv,
                )
                .await?;
        }

        info!(%controller_id, "single controller started");
        Ok(Self {
            config,
            token,
            repository,
            inner: Mutex::new(Inner {
                generation: versioned.generation,
                cluster: versioned.cluster,
                state: versioned.state,
                kv: versioned.kv,
                live_nodes,
            }),
            gateway_client,
            gateway_notify: Notify::new(),
            gateway_sync: Mutex::new(GatewaySyncState::default()),
        })
    }

    pub(super) async fn control_loop(self: Arc<Self>) {
        let mut ticker =
            tokio::time::interval(Duration::from_secs(self.config.reconcile_interval_seconds));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            if let Err(error) = self.tick().await {
                warn!(%error, "controller reconciliation tick failed");
            }
        }
    }

    pub(super) async fn tick(&self) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().await;
        let live = current_live_nodes(&inner, self.config.node_timeout_seconds);
        let previous = inner.state.clone();
        let mut changed = scheduler::finish_drains(&mut inner.state, unix_ms());
        changed |= scheduler::reconcile(&mut inner.state, &live);
        if changed && let Err(error) = self.commit_locked(&mut inner).await {
            inner.state = previous;
            return Err(error);
        }
        Ok(())
    }

    pub(super) async fn commit_locked(&self, inner: &mut Inner) -> Result<(), StorageError> {
        let generation = self
            .repository
            .replace(inner.generation, &inner.cluster, &inner.state, &inner.kv)
            .await?;
        inner.generation = generation;
        self.gateway_notify.notify_one();
        Ok(())
    }

    pub(super) async fn commit_kv_locked(&self, inner: &mut Inner) -> Result<(), StorageError> {
        let generation = self
            .repository
            .replace(inner.generation, &inner.cluster, &inner.state, &inner.kv)
            .await?;
        inner.generation = generation;
        Ok(())
    }

    pub(super) fn cluster_settings(&self, inner: &Inner) -> ClusterSettings {
        inner.cluster.clone()
    }
}
