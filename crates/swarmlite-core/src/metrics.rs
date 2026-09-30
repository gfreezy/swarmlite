//! Node-level host metrics. Rates are absent until two valid samples exist.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NodeMetrics {
    pub sampled_at_unix_ms: i64,
    pub interval_seconds: Option<f64>,
    pub uptime_seconds: Option<f64>,
    pub cpu_count: u32,
    pub cpu_percent: Option<f64>,
    pub io_wait_percent: Option<f64>,
    pub load_average: Option<[f64; 3]>,
    pub memory: Option<MemoryMetrics>,
    pub filesystems: Vec<FilesystemMetrics>,
    pub disks: Vec<DiskMetrics>,
    pub networks: Vec<NetworkMetrics>,
    #[serde(default)]
    pub errors: Vec<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryMetrics {
    pub total_bytes: u64,
    pub available_bytes: u64,
    pub swap_total_bytes: u64,
    pub swap_free_bytes: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilesystemMetrics {
    pub device: String,
    pub mount: String,
    pub filesystem: String,
    pub total_bytes: u64,
    pub available_bytes: u64,
    pub used_bytes: u64,
    pub inodes: u64,
    pub free_inodes: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiskMetrics {
    pub device: String,
    /// Only non-partition leaf devices contribute to the host aggregate.
    pub aggregate: bool,
    pub read_bytes_per_second: Option<f64>,
    pub write_bytes_per_second: Option<f64>,
    pub read_iops: Option<f64>,
    pub write_iops: Option<f64>,
    pub busy_percent: Option<f64>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkMetrics {
    pub interface: String,
    pub aggregate: bool,
    pub receive_bytes_per_second: Option<f64>,
    pub transmit_bytes_per_second: Option<f64>,
    pub receive_errors: u64,
    pub transmit_errors: u64,
    pub receive_dropped: u64,
    pub transmit_dropped: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricSample {
    pub received_at_unix_ms: i64,
    pub metrics: NodeMetrics,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeStats {
    pub id: String,
    pub address: String,
    pub stale: bool,
    pub age_seconds: Option<f64>,
    pub latest: Option<MetricSample>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub history: Vec<MetricPoint>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeStatsResponse {
    pub nodes: Vec<NodeStats>,
    pub retention_seconds: u64,
    pub resolution_seconds: u64,
    pub history_range_seconds: u64,
    pub range_start_unix_ms: i64,
    pub range_end_unix_ms: i64,
    pub stale_after_seconds: u64,
    pub storage_error: Option<String>,
}

/// Compact history: device inventories live only in the latest sample.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricPoint {
    pub received_at_unix_ms: i64,
    pub cpu_percent: Option<f64>,
    pub memory_percent: Option<f64>,
    pub disk_read: Option<f64>,
    pub disk_write: Option<f64>,
    pub net_receive: Option<f64>,
    pub net_transmit: Option<f64>,
    pub sample_count: u64,
    /// Peaks in CPU, memory, disk read/write, network receive/transmit order.
    pub peaks: [Option<f64>; 6],
}
impl MetricPoint {
    pub fn values(&self) -> [Option<f64>; 6] {
        [
            self.cpu_percent,
            self.memory_percent,
            self.disk_read,
            self.disk_write,
            self.net_receive,
            self.net_transmit,
        ]
    }
    pub fn from_sample(sample: &MetricSample) -> Self {
        fn sum(values: impl Iterator<Item = Option<f64>>) -> Option<f64> {
            let mut values = values.peekable();
            values.peek()?;
            values.sum()
        }
        let m = &sample.metrics;
        let mut point = Self {
            received_at_unix_ms: sample.received_at_unix_ms,
            cpu_percent: m.cpu_percent,
            memory_percent: m.memory.as_ref().filter(|m| m.total_bytes > 0).map(|m| {
                m.total_bytes.saturating_sub(m.available_bytes) as f64 * 100. / m.total_bytes as f64
            }),
            disk_read: sum(m
                .disks
                .iter()
                .filter(|d| d.aggregate)
                .map(|d| d.read_bytes_per_second)),
            disk_write: sum(m
                .disks
                .iter()
                .filter(|d| d.aggregate)
                .map(|d| d.write_bytes_per_second)),
            net_receive: sum(m
                .networks
                .iter()
                .filter(|n| n.aggregate)
                .map(|n| n.receive_bytes_per_second)),
            net_transmit: sum(m
                .networks
                .iter()
                .filter(|n| n.aggregate)
                .map(|n| n.transmit_bytes_per_second)),
            sample_count: 1,
            peaks: [None; 6],
        };
        point.peaks = point.values();
        point
    }
}
