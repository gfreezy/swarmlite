use super::*;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, btree_map::Entry},
    path::PathBuf,
    sync::{Arc as StdArc, Mutex as StdMutex, mpsc},
};
use swarmlite_core::metrics::{
    MetricPoint, MetricSample, NodeMetrics, NodeStats, NodeStatsResponse,
};

const DAY: u64 = 86400;
const RETENTION_SECONDS: u64 = 365 * DAY;
const FLUSH_SECONDS: u64 = 30;
const QUEUE_CAPACITY: usize = 64;
const BATCH_POINTS: usize = 1024;
// Incremental rollups keep their own sums/counts; never average averages.
const TIERS: [(u64, u64); 3] = [(60, DAY), (3600, 30 * DAY), (DAY, RETENTION_SECONDS)];

pub(super) struct Latest {
    sample: MetricSample,
    received: Instant,
}
struct Pending {
    node: String,
    source: i64,
    point: MetricPoint,
}
pub(super) struct Store {
    sender: mpsc::SyncSender<Pending>,
    path: PathBuf,
    cluster: String,
    error: StdArc<StdMutex<Option<String>>>,
}
#[derive(Default, Serialize, Deserialize)]
struct Aggregate {
    sums: [f64; 6],
    counts: [u64; 6],
    peaks: [Option<f64>; 6],
    samples: u64,
}
impl Aggregate {
    fn add(&mut self, point: &MetricPoint) {
        self.samples += point.sample_count;
        for (i, value) in point.values().into_iter().enumerate() {
            if let Some(v) = value {
                self.sums[i] += v;
                self.counts[i] += 1;
                self.peaks[i] = Some(self.peaks[i].map_or(v, |old| old.max(v)));
            }
        }
    }
    fn point(&self, time: i64) -> MetricPoint {
        let v: [Option<f64>; 6] = std::array::from_fn(|i| {
            (self.counts[i] > 0).then(|| self.sums[i] / self.counts[i] as f64)
        });
        MetricPoint {
            received_at_unix_ms: time,
            cpu_percent: v[0],
            memory_percent: v[1],
            disk_read: v[2],
            disk_write: v[3],
            net_receive: v[4],
            net_transmit: v[5],
            sample_count: self.samples,
            peaks: self.peaks,
        }
    }
}
fn connect(path: &std::path::Path) -> Result<Connection, rusqlite::Error> {
    let c = Connection::open(path)?;
    c.busy_timeout(Duration::from_secs(2))?;
    c.execute_batch(
        "PRAGMA cache_size=-512;
         PRAGMA synchronous=NORMAL;
         PRAGMA wal_autocheckpoint=1024;
         PRAGMA journal_size_limit=8388608;",
    )?;
    Ok(c)
}
fn initialize(path: &std::path::Path) -> anyhow::Result<Connection> {
    let c = connect(path)?;
    // Only the persistent writer gets a larger cache; short-lived readers stay small.
    c.execute_batch("PRAGMA cache_size=-2048;
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS metric_samples (
            cluster TEXT NOT NULL,node TEXT NOT NULL,source INTEGER NOT NULL,received INTEGER NOT NULL,point TEXT NOT NULL,
            PRIMARY KEY(cluster,node,source)
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS sample_time ON metric_samples(cluster,node,received);
        CREATE INDEX IF NOT EXISTS sample_expiry ON metric_samples(received);
        CREATE TABLE IF NOT EXISTS metric_rollups (
            cluster TEXT NOT NULL,node TEXT NOT NULL,resolution INTEGER NOT NULL,bucket INTEGER NOT NULL,aggregate TEXT NOT NULL,
            PRIMARY KEY(cluster,node,resolution,bucket)
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS rollup_expiry ON metric_rollups(resolution,bucket);")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(c)
}
fn flush(c: &mut Connection, cluster: &str, batch: &[Pending], now: i64) -> anyhow::Result<()> {
    let tx = c.transaction()?;
    // Bounded by the batch, with keys borrowed from it. Load and write each bucket once.
    let mut aggregates = BTreeMap::new();
    {
        let mut insert = tx.prepare(
            "INSERT OR IGNORE INTO metric_samples(cluster,node,source,received,point)
             VALUES (?1,?2,?3,?4,?5)",
        )?;
        let mut select = tx.prepare(
            "SELECT aggregate FROM metric_rollups
             WHERE cluster=?1 AND node=?2 AND resolution=?3 AND bucket=?4",
        )?;
        for p in batch {
            let inserted = insert.execute(params![
                cluster,
                p.node,
                p.source,
                p.point.received_at_unix_ms,
                serde_json::to_string(&p.point)?
            ])?;
            if inserted == 0 {
                continue;
            }
            for (resolution, _) in TIERS {
                let bucket = p
                    .point
                    .received_at_unix_ms
                    .div_euclid(resolution as i64 * 1000)
                    * resolution as i64
                    * 1000;
                let aggregate = match aggregates.entry((p.node.as_str(), resolution, bucket)) {
                    Entry::Occupied(entry) => entry.into_mut(),
                    Entry::Vacant(entry) => {
                        let old: Option<String> = select
                            .query_row(params![cluster, p.node, resolution as i64, bucket], |r| {
                                r.get(0)
                            })
                            .optional()?;
                        entry.insert(
                            old.map(|s| serde_json::from_str::<Aggregate>(&s))
                                .transpose()?
                                .unwrap_or_default(),
                        )
                    }
                };
                aggregate.add(&p.point);
            }
        }
    }
    {
        let mut upsert = tx.prepare(
            "INSERT INTO metric_rollups(cluster,node,resolution,bucket,aggregate)
             VALUES (?1,?2,?3,?4,?5)
             ON CONFLICT(cluster,node,resolution,bucket)
             DO UPDATE SET aggregate=excluded.aggregate",
        )?;
        // Primary-key order also keeps writes to nearby database pages together.
        for ((node, resolution, bucket), aggregate) in aggregates {
            upsert.execute(params![
                cluster,
                node,
                resolution as i64,
                bucket,
                serde_json::to_string(&aggregate)?
            ])?;
        }
    }
    tx.execute(
        "DELETE FROM metric_samples WHERE received < ?1 OR received > ?2",
        params![now - 900_000, now + 60_000],
    )?;
    for (resolution, retention) in TIERS {
        // Drop entire expired buckets, keeping at most the specified retention.
        tx.execute(
            "DELETE FROM metric_rollups WHERE resolution=?1 AND (bucket < ?2 OR bucket > ?3)",
            params![
                resolution as i64,
                now - retention as i64 * 1000,
                now + 60_000
            ],
        )?;
    }
    tx.commit()?;
    Ok(())
}
fn query(
    c: &Connection,
    cluster: &str,
    node: &str,
    range: HistoryRange,
) -> anyhow::Result<Vec<MetricPoint>> {
    let HistoryRange {
        start,
        end: now,
        resolution,
    } = range;
    if resolution == 5 {
        let mut q=c.prepare("SELECT point FROM metric_samples WHERE cluster=?1 AND node=?2 AND received>=?3 AND received<=?4 ORDER BY received DESC LIMIT 181")?;
        let rows = q.query_map(params![cluster, node, start, now], |r| {
            r.get::<_, String>(0)
        })?;
        let mut points = rows
            .map(|r| Ok(serde_json::from_str(&r?)?))
            .collect::<anyhow::Result<Vec<_>>>()?;
        points.reverse();
        Ok(points)
    } else {
        let mut q=c.prepare("SELECT bucket,aggregate FROM metric_rollups WHERE cluster=?1 AND node=?2 AND resolution=?3 AND bucket>=?4 AND bucket<=?5 ORDER BY bucket LIMIT 1441")?;
        let rows = q.query_map(
            params![
                cluster,
                node,
                resolution as i64,
                start.div_euclid(resolution as i64 * 1000) * resolution as i64 * 1000,
                now
            ],
            |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)),
        )?;
        rows.map(|r| {
            let (t, s) = r?;
            Ok(serde_json::from_str::<Aggregate>(&s)?.point(t))
        })
        .collect()
    }
}
fn resolution(seconds: u64) -> u64 {
    if seconds <= 900 {
        5
    } else if seconds <= DAY {
        60
    } else if seconds <= 30 * DAY {
        3600
    } else {
        DAY
    }
}
#[derive(Clone, Copy)]
struct HistoryRange {
    start: i64,
    end: i64,
    resolution: u64,
}
fn history_range(
    seconds: u64,
    from: Option<i64>,
    to: Option<i64>,
    now: i64,
) -> Result<HistoryRange, ControllerError> {
    let (start, end) = match (from, to) {
        (Some(start), Some(end)) => (start, end),
        (None, None) if seconds > 0 && seconds <= RETENTION_SECONDS => {
            (now - seconds as i64 * 1000, now)
        }
        _ => {
            return Err(ControllerError::Invalid(
                "choose a valid duration, or provide both from and to timestamps".into(),
            ));
        }
    };
    if start >= end || start < now - RETENTION_SECONDS as i64 * 1000 || end > now {
        return Err(ControllerError::Invalid(
            "time range must be ordered, in the past, and within the last 365 days".into(),
        ));
    }
    // Resolution depends on the oldest requested timestamp, not just the width
    // of the window: a short slice of last year only has daily data retained.
    Ok(HistoryRange {
        start,
        end,
        resolution: resolution(((now - start + 999) / 1000) as u64),
    })
}
impl Store {
    pub(super) fn new(path: PathBuf, cluster: String) -> Self {
        let (sender, receiver) = mpsc::sync_channel::<Pending>(QUEUE_CAPACITY);
        let error = StdArc::new(StdMutex::new(None));
        let worker_path = path.clone();
        let worker_cluster = cluster.clone();
        let worker_error = StdArc::clone(&error);
        std::thread::Builder::new()
            .name("node-metrics".into())
            .spawn(move || {
                let mut connection = None;
                let mut batch = Vec::with_capacity(BATCH_POINTS);
                let mut deadline = Instant::now() + Duration::from_secs(FLUSH_SECONDS);
                loop {
                    let event =
                        receiver.recv_timeout(deadline.saturating_duration_since(Instant::now()));
                    let disconnected = matches!(event, Err(mpsc::RecvTimeoutError::Disconnected));
                    if let Ok(p) = event {
                        batch.push(p);
                    }
                    if !disconnected && batch.len() < BATCH_POINTS && Instant::now() < deadline {
                        continue;
                    }
                    let result = (|| -> anyhow::Result<()> {
                        if connection.is_none() {
                            connection = Some(initialize(&worker_path)?);
                        }
                        flush(
                            connection.as_mut().unwrap(),
                            &worker_cluster,
                            &batch,
                            unix_ms(),
                        )
                    })();
                    let mut error = worker_error
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    match result {
                        Ok(()) => *error = None,
                        Err(e) => {
                            warn!(error=%e,"node metrics history write failed");
                            *error = Some(format!("History write failed: {e}"));
                            connection = None;
                        }
                    }
                    // A failed database must not grow RAM or block heartbeats; dropped samples remain gaps.
                    batch.clear();
                    deadline = Instant::now() + Duration::from_secs(FLUSH_SECONDS);
                    if disconnected {
                        break;
                    }
                }
            })
            .expect("start metrics writer");
        Self {
            sender,
            path,
            cluster,
            error,
        }
    }
    fn enqueue(&self, id: &str, sample: &MetricSample) {
        if self
            .sender
            .try_send(Pending {
                node: id.into(),
                source: sample.metrics.sampled_at_unix_ms,
                point: MetricPoint::from_sample(sample),
            })
            .is_err()
        {
            *self
                .error
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) =
                Some("History queue full or writer unavailable; some samples were dropped".into());
        }
    }
    async fn history(&self, node: &str, range: HistoryRange) -> anyhow::Result<Vec<MetricPoint>> {
        let path = self.path.clone();
        let cluster = self.cluster.clone();
        let node = node.to_owned();
        tokio::task::spawn_blocking(move || -> anyhow::Result<Vec<MetricPoint>> {
            if !path.exists() {
                return Ok(vec![]);
            }
            query(&connect(&path)?, &cluster, &node, range)
        })
        .await?
    }
    fn error(&self) -> Option<String> {
        self.error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}
pub(super) fn record(inner: &mut Inner, store: &Store, id: &str, metrics: NodeMetrics) {
    if inner
        .metrics
        .get(id)
        .is_some_and(|s| s.sample.metrics.sampled_at_unix_ms == metrics.sampled_at_unix_ms)
    {
        return;
    }
    let sample = MetricSample {
        received_at_unix_ms: unix_ms(),
        metrics,
    };
    store.enqueue(id, &sample);
    inner.metrics.insert(
        id.into(),
        Latest {
            sample,
            received: Instant::now(),
        },
    );
}
impl Controller {
    pub(super) async fn node_stats(
        &self,
        node: Option<&str>,
        history: bool,
        seconds: u64,
        from: Option<i64>,
        to: Option<i64>,
    ) -> Result<NodeStatsResponse, ControllerError> {
        let range = history_range(seconds, from, to, unix_ms())?;
        if history && node.is_none() {
            return Err(ControllerError::Invalid(
                "history requires a node filter".into(),
            ));
        }
        let inner = self.inner.lock().await;
        if let Some(id) = node
            && !inner.state.members.contains_key(id)
        {
            return Err(ControllerError::NotFound(format!("node not found: {id}")));
        }
        let now = Instant::now();
        let stale_after = self.config.node_timeout_seconds.max(30);
        let nodes = inner
            .state
            .members
            .values()
            .filter(|n| node.is_none_or(|id| id == n.id))
            .map(|n| {
                let latest = inner.metrics.get(&n.id);
                let age = latest.map(|s| now.duration_since(s.received).as_secs_f64());
                NodeStats {
                    id: n.id.clone(),
                    address: n.address.clone(),
                    stale: age.is_none_or(|a| a > stale_after as f64),
                    age_seconds: age,
                    latest: latest.map(|s| s.sample.clone()),
                    history: vec![],
                }
            })
            .collect();
        drop(inner);
        let mut response = NodeStatsResponse {
            nodes,
            retention_seconds: RETENTION_SECONDS,
            resolution_seconds: range.resolution,
            history_range_seconds: ((range.end - range.start) / 1000) as u64,
            range_start_unix_ms: range.start,
            range_end_unix_ms: range.end,
            stale_after_seconds: stale_after,
            storage_error: self.metrics_store.error(),
        };
        if history && let Some(n) = response.nodes.first_mut() {
            match self.metrics_store.history(&n.id, range).await {
                Ok(points) => n.history = points,
                Err(e) => response.storage_error = Some(format!("History query failed: {e}")),
            }
            if response.resolution_seconds == 5
                && let Some(latest) = &n.latest
                && latest.received_at_unix_ms >= range.start
                && latest.received_at_unix_ms <= range.end
                && n.history
                    .last()
                    .is_none_or(|p| p.received_at_unix_ms < latest.received_at_unix_ms)
            {
                n.history.push(MetricPoint::from_sample(latest));
            }
        }
        Ok(response)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn custom_range_uses_oldest_data_tier_and_validates_bounds() {
        let now = 400 * DAY as i64 * 1000;
        let range = history_range(
            900,
            Some(now - 60 * DAY as i64 * 1000),
            Some(now - 60 * DAY as i64 * 1000 + 300_000),
            now,
        )
        .unwrap();
        assert_eq!(range.resolution, DAY);
        assert_eq!(range.end - range.start, 300_000);
        assert!(history_range(900, Some(now), None, now).is_err());
        assert!(history_range(900, Some(now), Some(now - 1), now).is_err());
        assert!(history_range(900, Some(now - 1000), Some(now + 1), now).is_err());
        assert!(history_range(900, Some(0), Some(now - 1), now).is_err());
    }
    fn pending(node: &str, time: i64, value: Option<f64>) -> Pending {
        Pending {
            node: node.into(),
            source: time,
            point: MetricPoint::from_sample(&MetricSample {
                received_at_unix_ms: time,
                metrics: NodeMetrics {
                    cpu_percent: value,
                    ..Default::default()
                },
            }),
        }
    }
    #[test]
    fn aggregation_is_incremental_idempotent_and_null_aware() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("metrics.sqlite");
        let mut c = initialize(&path).unwrap();
        let t = 2 * DAY as i64 * 1000;
        flush(
            &mut c,
            "cluster",
            &[
                pending("a", t + 1000, Some(10.)),
                pending("a", t + 2000, Some(30.)),
            ],
            t + 2000,
        )
        .unwrap();
        drop(c);
        let mut c = connect(&path).unwrap();
        flush(
            &mut c,
            "cluster",
            &[
                pending("a", t + 2000, Some(30.)),
                pending("a", t + 3000, Some(80.)),
                pending("a", t + 4000, None),
                pending("b", t + 4000, Some(99.)),
            ],
            t + 4000,
        )
        .unwrap();
        let points = query(
            &c,
            "cluster",
            "a",
            history_range(3600, None, None, t + 5000).unwrap(),
        )
        .unwrap();
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].cpu_percent, Some(40.));
        assert_eq!(points[0].peaks[0], Some(80.));
        assert_eq!(points[0].sample_count, 4);
        assert_eq!(points[0].memory_percent, None);
        assert!(
            query(
                &c,
                "other",
                "a",
                history_range(3600, None, None, t + 5000).unwrap()
            )
            .unwrap()
            .is_empty()
        );
    }
    #[test]
    fn grouped_aggregation_preserves_nodes_tiers_and_deduplication_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("metrics.sqlite");
        let mut c = initialize(&path).unwrap();
        // Cross a minute, hour and day boundary in the same batch.
        let t = 2 * DAY as i64 * 1000 - 1000;
        let sample = |node, time, cpu, memory| {
            let mut p = pending(node, time, cpu);
            p.point.memory_percent = memory;
            p.point.peaks[1] = memory;
            p
        };
        flush(
            &mut c,
            "cluster",
            &[
                sample("a", t - 1000, Some(10.), Some(20.)),
                sample("b", t, Some(50.), None),
                sample("a", t, Some(30.), None),
                sample("a", t, Some(99.), Some(99.)),
                sample("a", t + 1000, Some(70.), Some(40.)),
                sample("a", t + 2000, None, Some(80.)),
                sample("b", t + 2000, Some(90.), None),
            ],
            t + 2000,
        )
        .unwrap();
        drop(c);
        let mut c = initialize(&path).unwrap();
        let mut duplicate = sample("a", t + 2000, Some(99.), Some(99.));
        duplicate.source = t;
        flush(
            &mut c,
            "cluster",
            &[duplicate, sample("a", t + 3000, Some(110.), None)],
            t + 3000,
        )
        .unwrap();
        for seconds in [3600, 7 * DAY, 365 * DAY] {
            let range = history_range(seconds, None, None, t + 4000).unwrap();
            let a = query(&c, "cluster", "a", range).unwrap();
            assert_eq!(a.len(), 2);
            assert_eq!(a[0].cpu_percent, Some(20.));
            assert_eq!(a[0].memory_percent, Some(20.));
            assert_eq!(a[0].peaks[0], Some(30.));
            assert_eq!(a[0].sample_count, 2);
            assert_eq!(a[1].cpu_percent, Some(90.));
            assert_eq!(a[1].memory_percent, Some(60.));
            assert_eq!(a[1].peaks[0], Some(110.));
            assert_eq!(a[1].peaks[1], Some(80.));
            assert_eq!(a[1].sample_count, 3);
            let b = query(&c, "cluster", "b", range).unwrap();
            assert_eq!(b.len(), 2);
            assert_eq!(b[0].cpu_percent, Some(50.));
            assert_eq!(b[1].cpu_percent, Some(90.));
            assert_eq!(b[0].sample_count, 1);
            assert_eq!(b[1].sample_count, 1);
        }
    }
    #[test]
    fn failed_aggregation_rolls_back_raw_samples_and_other_buckets() {
        let dir = tempfile::tempdir().unwrap();
        let mut c = initialize(&dir.path().join("metrics.sqlite")).unwrap();
        let t = 2 * DAY as i64 * 1000;
        c.execute(
            "INSERT INTO metric_rollups VALUES ('cluster','b',60,?1,'invalid-json')",
            [t],
        )
        .unwrap();
        assert!(
            flush(
                &mut c,
                "cluster",
                &[pending("a", t, Some(10.)), pending("b", t, Some(20.))],
                t,
            )
            .is_err()
        );
        assert_eq!(
            c.query_row("SELECT count(*) FROM metric_samples", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert_eq!(
            c.query_row("SELECT count(*) FROM metric_rollups", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            1
        );
    }
    #[test]
    fn retention_removes_raw_and_old_rollups() {
        let dir = tempfile::tempdir().unwrap();
        let mut c = initialize(&dir.path().join("metrics.sqlite")).unwrap();
        let t = DAY as i64 * 1000;
        flush(&mut c, "c", &[pending("a", t, Some(10.))], t).unwrap();
        flush(&mut c, "c", &[], t + 2 * DAY as i64 * 1000).unwrap();
        assert_eq!(
            c.query_row("SELECT count(*) FROM metric_samples", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert_eq!(
            c.query_row("SELECT count(*) FROM metric_rollups", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            2
        );
        flush(&mut c, "c", &[], t + 366 * DAY as i64 * 1000).unwrap();
        assert_eq!(
            c.query_row("SELECT count(*) FROM metric_rollups", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert_eq!(
            [
                resolution(300),
                resolution(3600),
                resolution(7 * DAY),
                resolution(365 * DAY)
            ],
            [5, 60, 3600, DAY]
        );
    }
}
