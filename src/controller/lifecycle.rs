use super::*;

impl Controller {
    pub(super) async fn new(
        config: ControllerConfig,
        token: String,
        repository: StateRepository,
    ) -> Result<Self, StorageError> {
        let mut versioned = repository.initialize_with_cluster(&config.cluster).await?;
        let kv_repository = repository.kv_repository();
        let controller_id = config.cluster.controller_id.clone();
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
                .replace(versioned.generation, &versioned.cluster, &versioned.state)
                .await?;
        }

        let gateway_config = gateway::config(
            &versioned.state,
            &versioned.cluster.gateway.listen,
            config.advertise_url.clone(),
        );

        info!(%controller_id, "single controller started");
        Ok(Self {
            config,
            token,
            repository,
            kv_repository,
            deploying_stacks: std::sync::Mutex::new(BTreeSet::new()),
            inner: Mutex::new(Inner {
                generation: versioned.generation,
                cluster: versioned.cluster,
                state: versioned.state,
                live_nodes,
                gateway_generation: versioned.generation,
                gateway_config,
                gateway_reports: HashMap::new(),
            }),
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
        let (gateway_generation, gateway_config) = self.next_gateway_snapshot(inner)?;
        let generation = self
            .repository
            .replace(inner.generation, &inner.cluster, &inner.state)
            .await?;
        inner.generation = generation;
        inner.gateway_generation = gateway_generation;
        inner.gateway_config = gateway_config;
        Ok(())
    }

    pub(super) async fn commit_gateway_ack_locked(
        &self,
        inner: &mut Inner,
    ) -> Result<(), StorageError> {
        let generation = self
            .repository
            .replace(inner.generation, &inner.cluster, &inner.state)
            .await?;
        inner.generation = generation;
        Ok(())
    }
}
