use super::*;

impl Controller {
    pub(super) async fn new(
        config: ControllerConfig,
        token: String,
        repository: StateRepository,
    ) -> Result<Self, StorageError> {
        let versioned = repository.initialize_with_cluster(&config.cluster).await?;
        let gateway_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.gateway.request_timeout_seconds))
            .build()
            .map_err(|error| StorageError::Backend(error.to_string()))?;
        // Preserve existing assignments during the first node timeout after a takeover.
        let live_nodes = versioned
            .state
            .tasks
            .values()
            .map(|task| (task.node_id.clone(), Instant::now()))
            .collect();
        Ok(Self {
            config,
            token,
            repository,
            inner: Mutex::new(Inner {
                generation: versioned.generation,
                cluster: versioned.cluster,
                state: versioned.state,
                kv: versioned.kv,
                is_leader: false,
                live_nodes,
                controller_ack_candidates: HashMap::new(),
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
        if !self.repository.is_leader() {
            inner.is_leader = false;
            self.refresh_locked(&mut inner).await?;
            return Ok(());
        }
        if !inner.is_leader {
            self.refresh_locked(&mut inner).await?;
            self.try_acquire_locked(&mut inner).await?;
            return Ok(());
        }

        if inner.is_leader {
            let timeout = Duration::from_secs(self.config.node_timeout_seconds);
            let now = Instant::now();
            let live: BTreeSet<String> = inner
                .live_nodes
                .iter()
                .filter(|(_, seen)| now.duration_since(**seen) <= timeout)
                .map(|(id, _)| id.clone())
                .collect();
            let previous = inner.state.clone();
            let now_unix_ms = unix_ms();
            let voters = self.repository.voter_ids();
            let mut changed = prune_controllers(&mut inner.state, now_unix_ms, &voters);
            changed |= scheduler::finish_drains(&mut inner.state, now_unix_ms);
            changed |= scheduler::reconcile(&mut inner.state, &live);
            if changed && let Err(error) = self.commit_locked(&mut inner).await {
                inner.state = previous;
                return Err(error);
            }
            return Ok(());
        }
        Ok(())
    }

    pub(super) fn expire_local_lease(&self, inner: &mut Inner) {
        if inner.is_leader && !self.repository.is_leader() {
            warn!("Raft leadership changed; entering standby mode");
            inner.is_leader = false;
        }
    }

    pub(super) async fn refresh_locked(&self, inner: &mut Inner) -> Result<(), StorageError> {
        let latest = if self.repository.is_leader() {
            self.repository.load_consistent().await?
        } else {
            self.repository.load_local().await?
        };
        if latest.generation != inner.generation {
            inner.generation = latest.generation;
            inner.cluster = latest.cluster;
            inner.state = latest.state;
            inner.kv = latest.kv;
        }
        Ok(())
    }

    pub(super) async fn try_acquire_locked(&self, inner: &mut Inner) -> Result<(), StorageError> {
        self.repository
            .ensure_voter(
                self.repository.raft().node_id(),
                self.repository.raft().local_node().clone(),
            )
            .await?;
        let term = self.repository.current_term();
        info!(term, "acquired controller leadership");
        inner.is_leader = true;
        inner.live_nodes.clear();
        inner.controller_ack_candidates.clear();
        inner.state.nodes.clear();
        let takeover_time = Instant::now();
        for node_id in inner.state.members.keys() {
            inner
                .controller_ack_candidates
                .insert(node_id.clone(), takeover_time);
        }
        for node_id in inner.state.tasks.values().map(|task| task.node_id.clone()) {
            inner.live_nodes.insert(node_id, takeover_time);
        }
        let mut drains_reset = false;
        for task in inner.state.tasks.values_mut() {
            if task.desired == DesiredTaskState::Draining
                && task.drain_until_unix_ms.take().is_some()
            {
                drains_reset = true;
            }
        }
        let now = unix_ms();
        let self_record = ControllerRecord {
            node_id: self.config.controller_id.clone(),
            advertise_url: self.config.advertise_url.trim_end_matches('/').to_owned(),
            raft_id: self.repository.raft().node_id(),
            raft_url: self.repository.raft().local_node().raft_url.clone(),
            reserved_at_unix_ms: now,
        };
        let controller_changed = inner
            .state
            .controllers
            .get(&self.config.controller_id)
            .is_none_or(|record| {
                record.advertise_url != self_record.advertise_url
                    || record.raft_id != self_record.raft_id
                    || record.raft_url != self_record.raft_url
            });
        inner
            .state
            .controllers
            .insert(self.config.controller_id.clone(), self_record);
        let address = reqwest::Url::parse(&self.config.advertise_url)
            .ok()
            .and_then(|url| url.host_str().map(ToOwned::to_owned))
            .unwrap_or_default();
        let (roles, labels, automatic_roles, joined_at_unix_ms) = inner
            .state
            .members
            .get(&self.config.controller_id)
            .map(|member| {
                (
                    member.roles.clone(),
                    member.labels.clone(),
                    member.automatic_roles,
                    member.joined_at_unix_ms,
                )
            })
            .unwrap_or_else(|| {
                (
                    self.config.roles.clone(),
                    self.config.labels.clone(),
                    true,
                    now,
                )
            });
        let self_member = NodeMember {
            id: self.config.controller_id.clone(),
            address,
            roles,
            labels,
            automatic_roles,
            controller_url: self.config.advertise_url.trim_end_matches('/').to_owned(),
            raft_id: self.repository.raft().node_id(),
            raft_url: self.repository.raft().local_node().raft_url.clone(),
            joined_at_unix_ms,
        };
        let member_changed = inner
            .state
            .members
            .get(&self.config.controller_id)
            .is_none_or(|member| member != &self_member);
        if member_changed {
            inner
                .state
                .members
                .insert(self.config.controller_id.clone(), self_member);
        }
        if drains_reset || controller_changed || member_changed {
            self.commit_locked(inner).await?;
        } else {
            self.gateway_notify.notify_one();
        }
        Ok(())
    }

    pub(super) async fn commit_locked(&self, inner: &mut Inner) -> Result<(), StorageError> {
        self.expire_local_lease(inner);
        if !inner.is_leader {
            return Err(StorageError::Conflict);
        }
        match self
            .repository
            .replace(inner.generation, &inner.cluster, &inner.state, &inner.kv)
            .await
        {
            Ok(generation) => {
                inner.generation = generation;
                self.gateway_notify.notify_one();
                Ok(())
            }
            Err(error) => {
                inner.is_leader = false;
                Err(error)
            }
        }
    }

    pub(super) async fn commit_kv_locked(&self, inner: &mut Inner) -> Result<(), StorageError> {
        self.expire_local_lease(inner);
        if !inner.is_leader {
            return Err(StorageError::Conflict);
        }
        match self
            .repository
            .replace(inner.generation, &inner.cluster, &inner.state, &inner.kv)
            .await
        {
            Ok(generation) => {
                inner.generation = generation;
                Ok(())
            }
            Err(error) => {
                inner.is_leader = false;
                Err(error)
            }
        }
    }

    pub(super) fn leader_redirect(&self, path: &str) -> ControllerError {
        let location = self
            .repository
            .leader_url()
            .map(|leader| format!("{}{}", leader.trim_end_matches('/'), path));
        ControllerError::NotLeader(location)
    }

    pub(super) fn leader_redirect_with_query(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> ControllerError {
        let location = self.repository.leader_url().and_then(|leader| {
            let target = format!("{}{}", leader.trim_end_matches('/'), path);
            let mut target = reqwest::Url::parse(&target).ok()?;
            target.query_pairs_mut().extend_pairs(query.iter().cloned());
            Some(target.into())
        });
        ControllerError::NotLeader(location)
    }

    pub(super) fn cluster_settings(&self, inner: &Inner) -> Result<ClusterSettings, StorageError> {
        Ok(inner.cluster.clone())
    }
}
