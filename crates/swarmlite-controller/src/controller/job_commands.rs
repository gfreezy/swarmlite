use super::*;
use crate::model::JobExecution;

impl Controller {
    pub(super) async fn list_jobs(&self) -> Vec<ServiceRecord> {
        self.inner
            .lock()
            .await
            .state
            .services
            .values()
            .filter(|s| !s.deleted && s.spec.job.is_some())
            .cloned()
            .collect()
    }

    pub(super) async fn job_history(
        &self,
        target: &str,
    ) -> Result<Vec<TaskRecord>, ControllerError> {
        let inner = self.inner.lock().await;
        let service = resolve_service(&inner.state, target, "job history")?;
        require_job(&service)?;
        let mut tasks = inner
            .state
            .tasks
            .values()
            .filter(|t| t.service_id == service.id && t.job.is_some())
            .cloned()
            .collect::<Vec<_>>();
        tasks.sort_by_key(|t| std::cmp::Reverse(t.job.as_ref().unwrap().scheduled_at_unix_ms));
        Ok(tasks)
    }

    pub(super) async fn run_job(&self, target: &str) -> Result<TaskRecord, ControllerError> {
        let mut inner = self.inner.lock().await;
        let service = resolve_service(&inner.state, target, "job run")?;
        require_job(&service)?;
        if inner
            .state
            .tasks
            .values()
            .any(|t| t.service_id == service.id && !t.observed.is_job_terminal())
        {
            return Err(ControllerError::Conflict("job still has an unfinished execution; cancel it and wait for termination before running manually".into()));
        }
        let live = current_live_nodes(&inner, self.config.node_timeout_seconds);
        let eligible = live
            .into_iter()
            .filter(|id| inner.state.nodes.get(id).is_some_and(|n| n.supports_jobs))
            .collect();
        let mut template = service.clone();
        template.spec.replicas = 1;
        let mut task = scheduler::schedule_task(&inner.state, &template, &eligible)
            .ok_or_else(|| ControllerError::Conflict("no eligible live Agent for job".into()))?;
        let now = unix_ms();
        task.job = Some(JobExecution {
            runtime: None,
            job_id: service.id.clone(),
            scheduled_at_unix_ms: now,
            not_before_unix_ms: now,
            // Manual invocations have a bounded startup window even without a cron.
            start_deadline_unix_ms: service
                .spec
                .job
                .as_ref()
                .unwrap()
                .next_after(now)
                .unwrap_or(now + 300_000)
                .min(now + 300_000),
            spec: Box::new(service.spec),
        });
        let previous = inner.state.clone();
        inner.state.tasks.insert(task.id.clone(), task.clone());
        if let Err(error) = self.commit_locked(&mut inner).await {
            inner.state = previous;
            return Err(error.into());
        }
        Ok(task)
    }

    pub(super) async fn cancel_job_task(&self, id: &str) -> Result<TaskRecord, ControllerError> {
        let mut inner = self.inner.lock().await;
        let previous = inner.state.clone();
        let task = inner
            .state
            .tasks
            .get_mut(id)
            .filter(|t| t.job.is_some())
            .ok_or_else(|| ControllerError::NotFound(format!("job task {id:?} not found")))?;
        task.desired = DesiredTaskState::Stopped;
        let result = task.clone();
        if let Err(error) = self.commit_locked(&mut inner).await {
            inner.state = previous;
            return Err(error.into());
        }
        Ok(result)
    }
}

fn require_job(service: &ServiceRecord) -> Result<(), ControllerError> {
    if service.spec.job.is_none() {
        return Err(ControllerError::Invalid(format!(
            "{} is not a job",
            service.id
        )));
    }
    Ok(())
}
