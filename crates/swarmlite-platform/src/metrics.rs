//! Lightweight Linux host sampler, independent of Docker and the reconciliation loop.
use swarmlite_core::metrics::NodeMetrics;
use tokio::sync::watch;

pub fn start() -> watch::Receiver<Option<NodeMetrics>> {
    let (tx, rx) = watch::channel(None);
    #[cfg(target_os = "linux")]
    tokio::spawn(async move {
        let mut collector = linux::Collector::default();
        let mut timer = tokio::time::interval(std::time::Duration::from_secs(5));
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = tx.closed() => break,
                _ = timer.tick() => {}
            }
            let result = tokio::task::spawn_blocking(move || {
                let sample = collector.sample();
                (collector, sample)
            })
            .await;
            match result {
                Ok((next, sample)) => {
                    collector = next;
                    if tx.send(Some(sample)).is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });
    #[cfg(not(target_os = "linux"))]
    drop(tx);
    rx
}

#[cfg(any(target_os = "linux", all(test, unix)))]
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
mod linux {
    use std::{
        collections::{BTreeMap, BTreeSet},
        ffi::CString,
        fs,
        path::Path,
        time::{Instant, SystemTime, UNIX_EPOCH},
    };
    use swarmlite_core::metrics::*;

    #[derive(Default)]
    pub(super) struct Collector {
        previous: Option<Instant>,
        cpu: Option<[u64; 3]>,
        disks: BTreeMap<String, Vec<u64>>,
        networks: BTreeMap<String, Vec<u64>>,
    }
    fn read(path: &str, errors: &mut Vec<String>) -> Option<String> {
        fs::read_to_string(path)
            .map_err(|e| errors.push(format!("{path}: {e}")))
            .ok()
    }
    fn numbers(s: &str) -> Option<Vec<u64>> {
        s.split_whitespace().map(|v| v.parse().ok()).collect()
    }
    fn rate(now: u64, before: u64, seconds: f64) -> Option<f64> {
        (seconds > 0.)
            .then(|| now.checked_sub(before).map(|n| n as f64 / seconds))
            .flatten()
    }
    fn cpu_counters(s: &str) -> Option<[u64; 3]> {
        let v = numbers(
            s.lines()
                .find(|l| l.starts_with("cpu "))?
                .strip_prefix("cpu ")?,
        )?;
        if v.len() < 4 {
            return None;
        }
        // guest and guest_nice are already included in user and nice.
        Some([v.iter().take(8).sum(), v[3], *v.get(4).unwrap_or(&0)])
    }
    fn cpu_usage(now: [u64; 3], before: [u64; 3]) -> Option<(f64, f64)> {
        let total = now[0].checked_sub(before[0])?;
        let idle = now[1].checked_sub(before[1])?;
        let wait = now[2].checked_sub(before[2])?;
        if total == 0 || idle + wait > total {
            return None;
        }
        Some((
            (total - idle - wait) as f64 * 100. / total as f64,
            wait as f64 * 100. / total as f64,
        ))
    }
    fn memory(s: &str) -> Option<MemoryMetrics> {
        let values: BTreeMap<_, _> = s
            .lines()
            .filter_map(|l| {
                let (key, value) = l.split_once(':')?;
                Some((
                    key,
                    value
                        .split_whitespace()
                        .next()?
                        .parse::<u64>()
                        .ok()?
                        .checked_mul(1024)?,
                ))
            })
            .collect();
        Some(MemoryMetrics {
            total_bytes: *values.get("MemTotal")?,
            available_bytes: *values.get("MemAvailable")?,
            swap_total_bytes: *values.get("SwapTotal")?,
            swap_free_bytes: *values.get("SwapFree")?,
        })
    }
    fn disk_counters(s: &str) -> BTreeMap<String, Vec<u64>> {
        s.lines()
            .filter_map(|l| {
                let mut v = l.split_whitespace();
                v.next()?;
                v.next()?;
                let name = v.next()?;
                if name.starts_with("loop") || name.starts_with("ram") {
                    return None;
                }
                let fields = numbers(&v.collect::<Vec<_>>().join(" "))?;
                (fields.len() >= 11).then(|| (name.to_owned(), fields))
            })
            .collect()
    }
    fn network_counters(s: &str) -> BTreeMap<String, Vec<u64>> {
        s.lines()
            .filter_map(|l| {
                let (name, values) = l.split_once(':')?;
                let fields = numbers(values)?;
                (name.trim() != "lo" && fields.len() >= 16)
                    .then(|| (name.trim().to_owned(), fields))
            })
            .collect()
    }
    fn unescape(s: &str) -> String {
        // Decode each mountinfo octal escape once (a literal backslash must not be decoded twice).
        let bytes = s.as_bytes();
        let mut out = Vec::new();
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b'\\'
                && i + 3 < bytes.len()
                && bytes[i + 1..i + 4]
                    .iter()
                    .all(|c| (b'0'..=b'7').contains(c))
            {
                let value = (bytes[i + 1] - b'0') as u16 * 64
                    + (bytes[i + 2] - b'0') as u16 * 8
                    + (bytes[i + 3] - b'0') as u16;
                if value <= 255 {
                    out.push(value as u8);
                    i += 4;
                    continue;
                }
            }
            out.push(bytes[i]);
            i += 1;
        }
        String::from_utf8_lossy(&out).into_owned()
    }
    // libc counter widths differ between supported Unix targets.
    #[allow(clippy::unnecessary_cast)]
    fn filesystems(s: &str, errors: &mut Vec<String>) -> Vec<FilesystemMetrics> {
        let mut seen = BTreeSet::new();
        s.lines()
            .filter_map(|l| {
                let (left, right) = l.split_once(" - ")?;
                let l: Vec<_> = left.split_whitespace().collect();
                let r: Vec<_> = right.split_whitespace().collect();
                if l.len() < 6 || r.len() < 2 {
                    return None;
                }
                // Local block filesystems only. Overlay root supports containerized agents;
                // remote filesystems are excluded to avoid blocking on unavailable servers.
                if !r[1].starts_with("/dev/") && !(r[0] == "overlay" && l[4] == "/") {
                    return None;
                }
                if !seen.insert((l[2].to_owned(), r[0].to_owned())) {
                    return None;
                }
                let mount = unescape(l[4]);
                let path = CString::new(mount.as_bytes()).ok()?;
                let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
                // SAFETY: path is NUL terminated; stat points to writable storage of the required size.
                if unsafe { libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) } != 0 {
                    errors.push(format!("{mount}: {}", std::io::Error::last_os_error()));
                    return None;
                }
                // SAFETY: statvfs initialized the struct on success.
                let stat = unsafe { stat.assume_init() };
                let block = stat.f_frsize as u64;
                Some(FilesystemMetrics {
                    device: unescape(r[1]),
                    mount,
                    filesystem: r[0].into(),
                    total_bytes: (stat.f_blocks as u64).saturating_mul(block),
                    available_bytes: (stat.f_bavail as u64).saturating_mul(block),
                    used_bytes: (stat.f_blocks as u64)
                        .saturating_sub(stat.f_bfree as u64)
                        .saturating_mul(block),
                    inodes: stat.f_files as u64,
                    free_inodes: stat.f_ffree as u64,
                })
            })
            .take(256)
            .collect()
    }
    impl Collector {
        pub(super) fn sample(&mut self) -> NodeMetrics {
            let now = Instant::now();
            let seconds = self.previous.map(|p| now.duration_since(p).as_secs_f64());
            let mut m = NodeMetrics {
                sampled_at_unix_ms: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64,
                interval_seconds: seconds,
                ..Default::default()
            };
            let stat = read("/proc/stat", &mut m.errors);
            let cpu = stat.as_deref().and_then(cpu_counters);
            m.cpu_count = stat
                .as_deref()
                .map(|s| {
                    s.lines()
                        .filter(|l| {
                            l.starts_with("cpu")
                                && l.as_bytes().get(3).is_some_and(u8::is_ascii_digit)
                        })
                        .count() as u32
                })
                .unwrap_or(0);
            if let Some((busy, wait)) = cpu.zip(self.cpu).and_then(|(a, b)| cpu_usage(a, b)) {
                m.cpu_percent = Some(busy);
                m.io_wait_percent = Some(wait);
            }
            m.memory = read("/proc/meminfo", &mut m.errors)
                .as_deref()
                .and_then(memory);
            m.uptime_seconds = read("/proc/uptime", &mut m.errors)
                .and_then(|s| s.split_whitespace().next()?.parse().ok());
            m.load_average = read("/proc/loadavg", &mut m.errors).and_then(|s| {
                let mut v = s.split_whitespace();
                Some([
                    v.next()?.parse().ok()?,
                    v.next()?.parse().ok()?,
                    v.next()?.parse().ok()?,
                ])
            });
            let disks = read("/proc/diskstats", &mut m.errors)
                .as_deref()
                .map(disk_counters)
                .unwrap_or_default();
            for (name, v) in disks.iter().take(256) {
                let sys = Path::new("/sys/class/block").join(name);
                if sys.join("partition").exists() {
                    continue;
                }
                let aggregate = fs::read_dir(sys.join("slaves"))
                    .ok()
                    .is_some_and(|mut entries| entries.next().is_none());
                let rates = |index: usize, factor: f64| {
                    self.disks
                        .get(name)
                        .zip(seconds)
                        .and_then(|(old, dt)| rate(v[index], old[index], dt))
                        .map(|r| r * factor)
                };
                m.disks.push(DiskMetrics {
                    device: name.clone(),
                    aggregate,
                    read_bytes_per_second: rates(2, 512.),
                    write_bytes_per_second: rates(6, 512.),
                    read_iops: rates(0, 1.),
                    write_iops: rates(4, 1.),
                    busy_percent: rates(9, 0.1).map(|v| v.min(100.)),
                });
            }
            let networks = read("/proc/net/dev", &mut m.errors)
                .as_deref()
                .map(network_counters)
                .unwrap_or_default();
            for (name, v) in networks.iter().take(256) {
                let rates = |index: usize| {
                    self.networks
                        .get(name)
                        .zip(seconds)
                        .and_then(|(old, dt)| rate(v[index], old[index], dt))
                };
                m.networks.push(NetworkMetrics {
                    interface: name.clone(),
                    aggregate: !Path::new("/sys/devices/virtual/net").join(name).exists(),
                    receive_bytes_per_second: rates(0),
                    transmit_bytes_per_second: rates(8),
                    receive_errors: v[2],
                    receive_dropped: v[3],
                    transmit_errors: v[10],
                    transmit_dropped: v[11],
                });
            }
            if let Some(mounts) = read("/proc/self/mountinfo", &mut m.errors) {
                m.filesystems = filesystems(&mounts, &mut m.errors);
            }
            self.previous = Some(now);
            self.cpu = cpu;
            self.disks = disks;
            self.networks = networks;
            m
        }
    }
    #[cfg(test)]
    mod tests {
        use super::*;
        #[test]
        fn counters_exclude_guest_and_handle_resets() {
            assert_eq!(
                cpu_counters("cpu  10 5 5 60 10 5 5 0 8 2\n"),
                Some([100, 60, 10])
            );
            assert_eq!(cpu_usage([200, 120, 20], [100, 60, 10]), Some((30., 10.)));
            assert_eq!(cpu_usage([100, 60, 10], [100, 60, 10]), None);
            assert_eq!(cpu_usage([10, 6, 1], [100, 60, 10]), None);
            assert_eq!(rate(2048, 1024, 2.), Some(512.));
            assert_eq!(rate(10, 1024, 2.), None);
            assert_eq!(rate(10, 1, 0.), None);
        }
        #[test]
        fn available_memory_and_proc_parsers() {
            let m=memory("MemTotal: 1000 kB\nMemFree: 10 kB\nMemAvailable: 400 kB\nSwapTotal: 100 kB\nSwapFree: 25 kB").unwrap();
            assert_eq!(m.available_bytes, 409600);
            assert!(memory("MemTotal: 1000 kB").is_none());
            assert_eq!(unescape(r"/data\040files\134040"), r"/data files\040");
            let disks =
                disk_counters("8 0 sda 1 0 2 0 3 0 4 0 0 5 0\n7 0 loop0 1 0 2 0 3 0 4 0 0 5 0");
            assert_eq!(disks.len(), 1);
            assert_eq!(disks["sda"][6], 4);
            let net = network_counters("eth0: 100 1 2 3 0 0 0 0 200 1 4 5 0 0 0 0");
            assert_eq!(net["eth0"][8], 200);
        }
        #[test]
        #[cfg(target_os = "linux")]
        fn collects_real_linux_host() {
            let mut collector = Collector::default();
            let sample = collector.sample();
            assert!(sample.cpu_count > 0);
            assert!(sample.memory.unwrap().total_bytes > 0);
            assert!(sample.cpu_percent.is_none());
        }
    }
}
