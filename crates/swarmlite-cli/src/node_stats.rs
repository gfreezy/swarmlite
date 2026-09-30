use crate::{ConnectionArgs, ansi, connection, stdout_color};
use anyhow::Result;
use clap::Args;
use std::{
    fmt::Write as _,
    io::{IsTerminal, Write},
    path::Path,
    time::Duration,
};
use swarmlite_core::metrics::{NodeMetrics, NodeStatsResponse};

#[derive(Debug, Args)]
pub(crate) struct StatsArgs {
    /// Limit output to one node and include filesystem/device/interface details.
    node: Option<String>,
    /// Refresh every five seconds until Ctrl-C. JSON watch emits one object per line.
    #[arg(long)]
    watch: bool,
    /// Print machine-readable JSON without terminal formatting.
    #[arg(long)]
    json: bool,
    /// Show automatically aggregated history for one node (5m, 15m, 1h, 24h, 7d, 30d, 365d).
    #[arg(long, value_parser = ["5m", "15m", "1h", "24h", "7d", "30d", "365d"], requires = "node")]
    history: Option<String>,
    #[command(flatten)]
    connection: ConnectionArgs,
}
pub(crate) async fn run(data_dir: &Path, options: StatsArgs) -> Result<()> {
    let client = connection::resolve(
        data_dir,
        options.connection.controller,
        options.connection.token,
    )
    .await?;
    let mut path = options.node.as_ref().map_or_else(
        || "/v1/nodes/stats".into(),
        |node| {
            format!(
                "/v1/nodes/stats?{}",
                url::form_urlencoded::Serializer::new(String::new())
                    .append_pair("node", node)
                    .finish()
            )
        },
    );
    if let Some(range) = options.history.as_deref() {
        let seconds = match range {
            "5m" => 300,
            "15m" => 900,
            "1h" => 3600,
            "24h" => 86400,
            "7d" => 604800,
            "30d" => 2592000,
            _ => 31536000,
        };
        path.push_str(&format!("&history=true&seconds={seconds}"));
    }
    let tty =
        std::io::stdout().is_terminal() && std::env::var("TERM").ok().as_deref() != Some("dumb");
    let color = tty && stdout_color() && !options.json;
    loop {
        let response = tokio::select! {
            result=client.get_json::<NodeStatsResponse>(&path)=>result,
            _=tokio::signal::ctrl_c(), if options.watch=>return Ok(()),
        };
        match response {
            Ok(response) => {
                if options.json {
                    println!("{}", serde_json::to_string(&response)?);
                } else {
                    if options.watch && tty {
                        print!("\x1b[2J\x1b[H");
                    }
                    print!("{}", format_stats(&response, options.node.is_some(), color));
                    if options.history.is_some()
                        && response.nodes.iter().all(|node| node.history.is_empty())
                    {
                        println!(
                            "No retained history in this range yet; batches flush every 30 seconds."
                        );
                    }
                    std::io::stdout().flush()?;
                }
            }
            Err(error) if options.watch => eprintln!(
                "{} {error}",
                ansi(color, "33", "Metrics unavailable; retrying:")
            ),
            Err(error) => return Err(error.into()),
        }
        if !options.watch {
            return Ok(());
        }
        tokio::select! {
            _=tokio::signal::ctrl_c()=>return Ok(()),
            _=tokio::time::sleep(Duration::from_secs(5))=>{}
        }
    }
}
fn percent(value: Option<f64>, color: bool) -> String {
    match value {
        Some(v) => ansi(
            color,
            if v >= 90. {
                "31"
            } else if v >= 75. {
                "33"
            } else {
                "32"
            },
            format!("{:>6.1}%", v),
        ),
        None => ansi(color, "90", format!("{:>7}", "—")),
    }
}
fn ratio(used: u64, total: u64) -> Option<f64> {
    (total > 0).then(|| used as f64 * 100. / total as f64)
}
fn bytes(v: f64) -> String {
    let units = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut n = v;
    let mut i = 0;
    while n >= 1024. && i < units.len() - 1 {
        n /= 1024.;
        i += 1;
    }
    format!("{n:.1} {}", units[i])
}
fn speed(v: Option<f64>) -> String {
    v.map(|v| format!("{}/s", bytes(v)))
        .unwrap_or_else(|| "—".into())
}
fn sum(values: impl Iterator<Item = Option<f64>>) -> Option<f64> {
    let v: Vec<_> = values.collect();
    if v.is_empty() {
        None
    } else {
        v.into_iter().sum()
    }
}
fn clean(text: &str) -> String {
    text.chars()
        .map(|c| if c.is_control() { '�' } else { c })
        .collect()
}
fn detail(out: &mut String, m: &NodeMetrics, color: bool) {
    let load = m
        .load_average
        .map(|v| format!("{:.2} / {:.2} / {:.2}", v[0], v[1], v[2]))
        .unwrap_or_else(|| "—".into());
    let _ = writeln!(
        out,
        "\n{}\n  CPU: {} logical cores · I/O wait {} · Load 1/5/15m: {}",
        ansi(color, "1", "Host"),
        m.cpu_count,
        percent(m.io_wait_percent, color),
        load
    );
    let _ = writeln!(
        out,
        "  Uptime: {} · Sample interval: {}",
        m.uptime_seconds
            .map(|s| format!("{:.1} days", s / 86400.))
            .unwrap_or_else(|| "—".into()),
        m.interval_seconds
            .map(|s| format!("{s:.1}s"))
            .unwrap_or_else(|| "warming up".into())
    );
    if let Some(mem) = &m.memory {
        let _ = writeln!(
            out,
            "  Memory: {} used / {} total · {} available\n  Swap: {} used / {} total",
            bytes(mem.total_bytes.saturating_sub(mem.available_bytes) as f64),
            bytes(mem.total_bytes as f64),
            bytes(mem.available_bytes as f64),
            bytes(mem.swap_total_bytes.saturating_sub(mem.swap_free_bytes) as f64),
            bytes(mem.swap_total_bytes as f64)
        );
    }
    let _ = writeln!(
        out,
        "\n{}\n  {:<24} {:<16} {:>12} {:>12} {:>7} {:>7}",
        ansi(color, "1", "Filesystems"),
        "MOUNT",
        "DEVICE",
        "USED",
        "AVAILABLE",
        "USE",
        "INODES"
    );
    for f in &m.filesystems {
        let _ = writeln!(
            out,
            "  {:<24} {:<16} {:>12} {:>12} {} {} ({})",
            clean(&f.mount),
            clean(&f.device),
            bytes(f.used_bytes as f64),
            bytes(f.available_bytes as f64),
            percent(
                ratio(f.used_bytes, f.used_bytes.saturating_add(f.available_bytes)),
                color
            ),
            percent(
                ratio(f.inodes.saturating_sub(f.free_inodes), f.inodes),
                color
            ),
            clean(&f.filesystem)
        );
    }
    let _ = writeln!(
        out,
        "\n{}\n  {:<16} {:>14} {:>14} {:>10} {:>10} {:>7}",
        ansi(color, "1", "Disk I/O"),
        "DEVICE",
        "READ",
        "WRITE",
        "READ IOPS",
        "WRITE IOPS",
        "BUSY"
    );
    for d in &m.disks {
        let _ = writeln!(
            out,
            "  {:<16} {:>14} {:>14} {:>10} {:>10} {}{}",
            clean(&d.device),
            speed(d.read_bytes_per_second),
            speed(d.write_bytes_per_second),
            d.read_iops
                .map(|v| format!("{v:.1}"))
                .unwrap_or_else(|| "—".into()),
            d.write_iops
                .map(|v| format!("{v:.1}"))
                .unwrap_or_else(|| "—".into()),
            percent(d.busy_percent, color),
            if d.aggregate {
                ""
            } else {
                " (stacked; excluded from total)"
            }
        );
    }
    let _ = writeln!(
        out,
        "\n{}\n  {:<16} {:>14} {:>14} {:>14} {:>14}",
        ansi(color, "1", "Network I/O"),
        "INTERFACE",
        "RECEIVE",
        "TRANSMIT",
        "ERRORS RX/TX",
        "DROPS RX/TX"
    );
    for n in &m.networks {
        let _ = writeln!(
            out,
            "  {:<16} {:>14} {:>14} {:>14} {:>14}{}",
            clean(&n.interface),
            speed(n.receive_bytes_per_second),
            speed(n.transmit_bytes_per_second),
            format!("{}/{}", n.receive_errors, n.transmit_errors),
            format!("{}/{}", n.receive_dropped, n.transmit_dropped),
            if n.aggregate {
                ""
            } else {
                " (virtual; excluded from total)"
            }
        );
    }
    for error in &m.errors {
        let _ = writeln!(out, "  {}", ansi(color, "33", clean(error)));
    }
}
pub(crate) fn format_stats(response: &NodeStatsResponse, details: bool, color: bool) -> String {
    let mut out = String::new();
    let width = response
        .nodes
        .iter()
        .map(|n| clean(&n.id).chars().count())
        .max()
        .unwrap_or(4)
        .max(4);
    let _ = writeln!(
        out,
        "{}",
        ansi(
            color,
            "1",
            format!(
                "{:<width$}  {:<9} {:>7} {:>7} {:>7} {:>12} {:>12} {:>12} {:>12} {:>7}",
                "NODE",
                "STATUS",
                "CPU",
                "MEM",
                "DISK*",
                "DISK READ",
                "DISK WRITE",
                "NET RX",
                "NET TX",
                "AGE"
            )
        )
    );
    for node in &response.nodes {
        let m = node.latest.as_ref().map(|s| &s.metrics);
        let state = if m.is_none() {
            "NO DATA"
        } else if node.stale {
            "STALE"
        } else if m.is_some_and(|m| !m.errors.is_empty()) {
            "PARTIAL"
        } else {
            "OK"
        };
        let code = match state {
            "STALE" => "31",
            "PARTIAL" => "33",
            "NO DATA" => "90",
            _ => "32",
        };
        let mem = m.and_then(|m| m.memory.as_ref()).and_then(|m| {
            ratio(
                m.total_bytes.saturating_sub(m.available_bytes),
                m.total_bytes,
            )
        });
        let disk = m.and_then(|m| {
            m.filesystems
                .iter()
                .filter_map(|f| ratio(f.used_bytes, f.used_bytes.saturating_add(f.available_bytes)))
                .reduce(f64::max)
        });
        let dr = m.and_then(|m| {
            sum(m
                .disks
                .iter()
                .filter(|d| d.aggregate)
                .map(|d| d.read_bytes_per_second))
        });
        let dw = m.and_then(|m| {
            sum(m
                .disks
                .iter()
                .filter(|d| d.aggregate)
                .map(|d| d.write_bytes_per_second))
        });
        let rx = m.and_then(|m| {
            sum(m
                .networks
                .iter()
                .filter(|d| d.aggregate)
                .map(|d| d.receive_bytes_per_second))
        });
        let tx = m.and_then(|m| {
            sum(m
                .networks
                .iter()
                .filter(|d| d.aggregate)
                .map(|d| d.transmit_bytes_per_second))
        });
        let _ = writeln!(
            out,
            "{:<width$}  {} {} {} {} {:>12} {:>12} {:>12} {:>12} {:>7}",
            clean(&node.id),
            ansi(color, code, format!("{state:<9}")),
            percent(m.and_then(|m| m.cpu_percent), color),
            percent(mem, color),
            percent(disk, color),
            speed(dr),
            speed(dw),
            speed(rx),
            speed(tx),
            node.age_seconds
                .map(|s| format!("{s:.0}s"))
                .unwrap_or_else(|| "—".into())
        );
        if details && let Some(m) = m {
            detail(&mut out, m, color);
        }
    }
    if let Some(error) = &response.storage_error {
        let _ = writeln!(out, "History storage: {}", ansi(color, "33", clean(error)));
    }
    for node in &response.nodes {
        if !node.history.is_empty() {
            let _ = writeln!(
                out,
                "\n{} · {}s buckets · avg / peak · UTC",
                ansi(color, "1", format!("{} history", clean(&node.id))),
                response.resolution_seconds
            );
            let _ = writeln!(
                out,
                "{:<22} {:>17} {:>17} {:>15} {:>15} {:>15} {:>15} {:>7}",
                "BUCKET",
                "CPU AVG / PEAK",
                "MEM AVG / PEAK",
                "DISK READ AVG",
                "DISK WRITE AVG",
                "NET RX AVG",
                "NET TX AVG",
                "SAMPLES"
            );
            for p in &node.history {
                let time = chrono::DateTime::from_timestamp_millis(p.received_at_unix_ms)
                    .map(|t| t.format("%Y-%m-%d %H:%M:%S").to_string())
                    .unwrap_or_else(|| "—".into());
                let _ = writeln!(
                    out,
                    "{time:<22} {} / {} {} / {} {:>15} {:>15} {:>15} {:>15} {:>7}",
                    percent(p.cpu_percent, color),
                    percent(p.peaks[0], color),
                    percent(p.memory_percent, color),
                    percent(p.peaks[1], color),
                    speed(p.disk_read),
                    speed(p.disk_write),
                    speed(p.net_receive),
                    speed(p.net_transmit),
                    p.sample_count
                );
            }
        }
    }
    if response.nodes.is_empty() {
        out.push_str("No joined nodes.\n");
    }
    out.push_str("\nDISK*: fullest filesystem (used / used + available). Totals exclude stacked disks and virtual NICs.\n");
    out.push_str("— = unavailable or warming up. STALE values are last reported, not current. NO DATA: wait for a Linux Agent with monitoring support.\n");
    out
}
#[cfg(test)]
mod tests {
    use super::*;
    use swarmlite_core::metrics::{MetricSample, NodeStats};
    #[test]
    fn plain_output_missing_and_stale_are_unambiguous() {
        let response = NodeStatsResponse {
            retention_seconds: 900,
            stale_after_seconds: 30,
            resolution_seconds: 5,
            history_range_seconds: 900,
            range_start_unix_ms: 0,
            range_end_unix_ms: 900_000,
            storage_error: None,
            nodes: vec![NodeStats {
                id: "node-1".into(),
                address: "127.0.0.1".into(),
                stale: true,
                age_seconds: Some(60.),
                latest: Some(MetricSample {
                    received_at_unix_ms: 0,
                    metrics: NodeMetrics {
                        cpu_percent: Some(95.),
                        ..Default::default()
                    },
                }),
                history: vec![],
            }],
        };
        let plain = format_stats(&response, true, false);
        assert!(!plain.contains('\x1b'));
        assert!(plain.contains("STALE"));
        assert!(plain.contains("95.0%"));
        assert!(plain.contains('—'));
        let colored = format_stats(&response, false, true);
        assert!(colored.contains("\x1b[31m"));
        assert_eq!(sum([Some(1.), None].into_iter()), None);
        assert_eq!(sum([].into_iter()), None);
        assert!(!clean("node\x1b[2J").contains('\x1b'));
    }
}
