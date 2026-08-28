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
        let gateway_generation = if versioned.state.gateway_generation == 0 {
            changed = true;
            versioned.generation.max(1)
        } else {
            versioned.state.gateway_generation
        };
        versioned.state.gateway_generation = gateway_generation;
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
        let gateway_snapshot = GatewayRecoverySnapshot::new(
            versioned.cluster.cluster_id.clone(),
            gateway_generation,
            versioned.state.gateway_routes.clone(),
        );
        let (status_changes, _) = tokio::sync::watch::channel(versioned.generation);

        info!(%controller_id, "single controller started");
        Ok(Self {
            config,
            token,
            repository,
            kv_repository,
            commands: commands::AgentCommandBroker::new(),
            sessions: sessions::DataSessionBroker::new(),
            deploying_stacks: std::sync::Mutex::new(BTreeSet::new()),
            status_changes,
            inner: Mutex::new(Inner {
                generation: versioned.generation,
                status_revision: versioned.generation,
                cluster: versioned.cluster,
                state: versioned.state,
                live_nodes,
                gateway_generation,
                gateway_config,
                gateway_snapshot,
                gateway_reports: HashMap::new(),
                task_progress: HashMap::new(),
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
        let now_unix_ms = unix_ms();
        let mut changed = scheduler::finish_drains(&mut inner.state, now_unix_ms);
        changed |= scheduler::reconcile(&mut inner.state, &live);
        changed |= gateway::refresh_ready_stack_routes(&mut inner.state);
        changed |= self.refresh_stack_deployments_locked(&mut inner, now_unix_ms)?;
        if changed && let Err(error) = self.commit_locked(&mut inner).await {
            inner.state = previous;
            return Err(error);
        }
        let config_digests = referenced_config_digests(&inner.state);
        drop(inner);
        let grace_period_ms =
            i64::try_from(CONFIG_GC_GRACE_PERIOD_SECONDS.saturating_mul(1_000)).unwrap_or(i64::MAX);
        match self
            .repository
            .gc_config_blobs(&config_digests, now_unix_ms, grace_period_ms)
        {
            Ok(stats) if stats.marked > 0 || stats.deleted > 0 => info!(
                referenced = stats.referenced,
                marked = stats.marked,
                retained_for_grace = stats.retained_for_grace,
                deleted = stats.deleted,
                "reconciled Controller config blob garbage collection"
            ),
            Ok(stats) if stats.retained_for_grace > 0 => debug!(
                referenced = stats.referenced,
                retained_for_grace = stats.retained_for_grace,
                "Controller config blob garbage collection retained grace-period candidates"
            ),
            Ok(_) => {}
            Err(error) => warn!(%error, "Controller config blob garbage collection failed"),
        }
        Ok(())
    }

    pub(super) async fn commit_locked(&self, inner: &mut Inner) -> Result<(), StorageError> {
        let (gateway_generation, gateway_config, gateway_snapshot) =
            self.next_gateway_snapshot(inner)?;
        inner.state.gateway_generation = gateway_generation;
        let generation = self
            .repository
            .replace(inner.generation, &inner.cluster, &inner.state)
            .await?;
        inner.generation = generation;
        inner.gateway_generation = gateway_generation;
        inner.gateway_config = gateway_config;
        inner.gateway_snapshot = gateway_snapshot;
        self.notify_status_locked(inner);
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
        self.notify_status_locked(inner);
        Ok(())
    }

    pub(super) fn notify_status_locked(&self, inner: &mut Inner) {
        inner.status_revision = inner.status_revision.saturating_add(1);
        self.status_changes.send_replace(inner.status_revision);
    }
}

pub(super) fn referenced_config_digests(state: &ClusterState) -> BTreeSet<String> {
    state
        .services
        .values()
        .flat_map(|service| {
            service
                .spec
                .configs
                .iter()
                .map(|config| config.digest.clone())
        })
        .chain(
            state
                .tasks
                .values()
                .flat_map(|task| task.config_digests.iter().cloned()),
        )
        .chain(
            state
                .unclaimed_tasks
                .values()
                .flat_map(|task| task.config_digests.iter().cloned()),
        )
        .collect()
}
