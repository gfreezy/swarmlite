use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use croner::Cron;
use serde::{Deserialize, Serialize};

/// Schedule policy. Container settings remain identical to service settings.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JobSpec {
    pub schedule: Option<String>,
    #[serde(rename = "timezone")]
    pub time_zone: String,
    pub suspend: bool,
    pub timeout_seconds: Option<u64>,
}

impl JobSpec {
    pub fn validate(&self) -> Result<()> {
        if let Some(schedule) = &self.schedule {
            if schedule.split_whitespace().count() != 5 {
                bail!("schedule must contain five cron fields (minute hour day month weekday)");
            }
            schedule.parse::<Cron>().context("invalid schedule")?;
        }
        self.time_zone.parse::<Tz>().context("invalid timezone")?;
        Ok(())
    }

    pub fn next_after(&self, unix_ms: i64) -> Result<i64> {
        let cron: Cron = self
            .schedule
            .as_deref()
            .context("manual job has no schedule")?
            .parse()?;
        let zone: Tz = self.time_zone.parse()?;
        let time = DateTime::<Utc>::from_timestamp_millis(unix_ms)
            .context("schedule timestamp out of range")?
            .with_timezone(&zone);
        Ok(cron.find_next_occurrence(&time, false)?.timestamp_millis())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn job_parser_reuses_container_fields_and_requires_five_field_cron() {
        let stack = crate::parse_stack("x-swarmlite-jobs:\n  backup:\n    image: busybox:1.37\n    schedule: '0 2 * * *'\n    timezone: Asia/Shanghai\n    timeout: 30m\n    stop_signal: SIGINT\n    stop_grace_period: 20s\n    environment: {MODE: backup}\n").unwrap();
        let spec = &stack.services["backup"];
        assert_eq!(spec.stop_signal.as_deref(), Some("SIGINT"));
        assert_eq!(spec.stop_grace_period_seconds, 20);
        let job = spec.job.as_ref().unwrap();
        assert_eq!(job.timeout_seconds, Some(1800));
        assert_eq!(job.next_after(0).unwrap(), 64_800_000); // Jan 2, 02:00 UTC+8
        for extra in [
            "ports: [80]",
            "healthcheck: {test: [CMD, true]}",
            "deploy: {replicas: 1}",
            "extends: app",
            "timeout: 0s",
            "timezone: nowhere",
        ] {
            assert!(crate::parse_stack(&format!("x-swarmlite-jobs:\n  j:\n    image: busybox\n    schedule: '* * * * *'\n    {extra}\n")).is_err(), "{extra}");
        }
        let mut invalid = job.clone();
        invalid.schedule = Some("* * * * * *".into());
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn manual_job_has_defaults_without_a_schedule() {
        let stack =
            crate::parse_stack("x-swarmlite-jobs:\n  manual:\n    image: busybox\n").unwrap();
        let job = stack.services["manual"].job.as_ref().unwrap();
        assert_eq!(job.schedule, None);
        assert_eq!(job.time_zone, "UTC");
        assert_eq!(job.timeout_seconds, Some(1800));
    }

    #[test]
    fn job_timezone_fall_back_produces_distinct_utc_occurrences() {
        let job = JobSpec {
            schedule: Some("30 1 * * *".into()),
            time_zone: "America/New_York".into(),
            suspend: false,
            timeout_seconds: None,
        };
        let start = DateTime::parse_from_rfc3339("2026-11-01T00:00:00Z")
            .unwrap()
            .timestamp_millis();
        let first = job.next_after(start).unwrap();
        let second = job.next_after(first).unwrap();
        assert!(first > start && second > first);
        assert_eq!(job.next_after(start).unwrap(), first);
    }
}
