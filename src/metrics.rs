use serde::{Deserialize, Serialize};

/// All measurements collected by the receiver at the end of a test run.
/// Fields are `Option` so the same struct covers both test types without waste.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct MetricsReport {
    pub scenario: String,      // "redis" | "quic" | "tcp"
    pub test: String,          // "throughput" | "latency"
    pub timestamp_utc: String,

    // ── Throughput ────────────────────────────────────────────────────────
    pub total_bytes: Option<u64>,
    pub duration_ms: Option<f64>,
    pub throughput_mbps: Option<f64>,
    pub throughput_gbps: Option<f64>,
    /// Wall-clock time spent (de)serialising on the receiver side.
    pub deserialise_ms: Option<f64>,

    // ── Latency ───────────────────────────────────────────────────────────
    /// All per-message RTT samples in microseconds (sender → receiver → sender).
    pub latency_samples_us: Option<Vec<u64>>,
    pub p50_us: Option<u64>,
    pub p95_us: Option<u64>,
    pub p99_us: Option<u64>,
    pub p999_us: Option<u64>,
    pub min_us: Option<u64>,
    pub max_us: Option<u64>,
    pub mean_us: Option<f64>,
    pub stddev_us: Option<f64>,
    pub messages_per_sec: Option<f64>,
}

impl MetricsReport {
    pub fn new(scenario: &str, test: &str) -> Self {
        Self {
            scenario: scenario.to_string(),
            test: test.to_string(),
            timestamp_utc: chrono::Utc::now().to_rfc3339(),
            ..Default::default()
        }
    }

    /// Populate throughput fields from raw measurements.
    pub fn set_throughput(&mut self, bytes: u64, duration_ms: f64, deserialise_ms: f64) {
        let secs = duration_ms / 1_000.0;
        let mbps = (bytes as f64 / (1024.0 * 1024.0)) / secs;
        self.total_bytes = Some(bytes);
        self.duration_ms = Some(duration_ms);
        self.throughput_mbps = Some(mbps);
        self.throughput_gbps = Some(mbps / 1024.0);
        self.deserialise_ms = Some(deserialise_ms);
    }

    /// Populate latency fields from a slice of RTT samples (microseconds).
    pub fn set_latency(&mut self, mut samples: Vec<u64>, total_ms: f64) {
        let n = samples.len() as f64;
        samples.sort_unstable();
        let p = |pct: f64| samples[((pct / 100.0) * (samples.len() as f64 - 1.0)) as usize];

        let mean = samples.iter().sum::<u64>() as f64 / n;
        let variance = samples.iter().map(|&x| (x as f64 - mean).powi(2)).sum::<f64>() / n;

        self.p50_us = Some(p(50.0));
        self.p95_us = Some(p(95.0));
        self.p99_us = Some(p(99.0));
        self.p999_us = Some(p(99.9));
        self.min_us = Some(*samples.first().unwrap());
        self.max_us = Some(*samples.last().unwrap());
        self.mean_us = Some(mean);
        self.stddev_us = Some(variance.sqrt());
        self.messages_per_sec = Some((samples.len() as f64 / total_ms) * 1_000.0);
        self.latency_samples_us = Some(samples);
    }

    /// Pretty-print a summary to stdout.
    pub fn print_summary(&self) {
        println!("\n══════════════════════════════════════════");
        println!("  {} / {}  —  {}", self.scenario.to_uppercase(), self.test, self.timestamp_utc);
        println!("══════════════════════════════════════════");
        if let (Some(bytes), Some(dur), Some(mbps)) =
            (self.total_bytes, self.duration_ms, self.throughput_mbps)
        {
            println!(
                "  Bytes transferred : {} MiB",
                bytes / (1024 * 1024)
            );
            println!("  Duration          : {:.1} ms", dur);
            println!("  Throughput        : {:.2} MB/s  ({:.4} Gbps)", mbps, self.throughput_gbps.unwrap_or(0.0));
            println!("  Deserialise time  : {:.2} ms", self.deserialise_ms.unwrap_or(0.0));
        }
        if let (Some(p50), Some(p95), Some(p99)) = (self.p50_us, self.p95_us, self.p99_us) {
            let total = self.latency_samples_us.as_ref().map(|v| v.len()).unwrap_or(0);
            println!("  Messages          : {}", total);
            println!("  p50  latency      : {} µs", p50);
            println!("  p95  latency      : {} µs", p95);
            println!("  p99  latency      : {} µs", p99);
            println!("  p99.9 latency     : {} µs", self.p999_us.unwrap_or(0));
            println!("  Mean latency      : {:.1} µs", self.mean_us.unwrap_or(0.0));
            println!("  Std-dev           : {:.1} µs", self.stddev_us.unwrap_or(0.0));
            println!("  Throughput        : {:.0} msg/s", self.messages_per_sec.unwrap_or(0.0));
        }
        println!("══════════════════════════════════════════\n");
    }

    /// Write the report as pretty-printed JSON to `path`.
    pub fn save(&self, path: &str) -> anyhow::Result<()> {
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(path, json)?;
        println!("Results written to {path}");
        Ok(())
    }
}
