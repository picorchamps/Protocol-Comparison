use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// The shared test structure used in latency tests across all three scenarios.
/// The same logical fields are present regardless of serialisation format.
/// bincode 1.x uses the serde traits for encoding, so no extra derives are needed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestPayload {
    pub id: u64,
    /// Microseconds since UNIX epoch — stamped by the sender at send time.
    pub timestamp_us: u64,
    pub sequence: u32,
    pub label: String,
    /// Eight f64 values (≈ 64 bytes of numeric data).
    pub values: [f64; 8],
    /// Key/value metadata pairs (stored as a vec for bincode compatibility).
    pub tags: Vec<(String, String)>,
}

impl TestPayload {
    pub fn new_small(sequence: u32) -> Self {
        Self {
            id: sequence as u64,
            timestamp_us: now_us(),
            sequence,
            label: format!("payload-{:032}", sequence),
            values: [1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8],
            tags: vec![
                ("env".into(), "benchmark".into()),
                ("version".into(), "1.0".into()),
                ("host".into(), "sender".into()),
            ],
        }
    }

    /// Re-stamp the send timestamp immediately before putting the payload on the wire.
    pub fn stamp(&mut self) {
        self.timestamp_us = now_us();
    }

    /// Round-trip time in microseconds since `stamp()` was called.
    pub fn rtt_us(&self) -> u64 {
        now_us().saturating_sub(self.timestamp_us)
    }

    // ── Protobuf conversion (used by the TCP scenario) ──────────────────────

    pub fn to_proto(&self) -> crate::proto::TestPayload {
        crate::proto::TestPayload {
            id: self.id,
            timestamp_us: self.timestamp_us,
            sequence: self.sequence,
            label: self.label.clone(),
            values: self.values.to_vec(),
            tags: self.tags.iter().cloned().collect::<HashMap<_, _>>(),
        }
    }

    #[allow(dead_code)]
    pub fn from_proto(p: crate::proto::TestPayload) -> Self {
        let mut values = [0f64; 8];
        for (i, v) in p.values.iter().take(8).enumerate() {
            values[i] = *v;
        }
        Self {
            id: p.id,
            timestamp_us: p.timestamp_us,
            sequence: p.sequence,
            label: p.label,
            values,
            tags: p.tags.into_iter().collect(),
        }
    }
}

// ── Throughput data ──────────────────────────────────────────────────────────

pub const THROUGHPUT_SIZE: usize = 1 << 30; // 1 GiB

/// Generate 1 GiB of deterministic pseudo-random bytes as the throughput payload.
/// Using a fast LCG so generation overhead is negligible.
pub fn generate_throughput_data() -> Vec<u8> {
    let mut data = vec![0u8; THROUGHPUT_SIZE];
    let mut state: u64 = 0xdeadbeef_cafebabe;
    for chunk in data.chunks_mut(8) {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let bytes = state.to_le_bytes();
        let len = chunk.len();
        chunk.copy_from_slice(&bytes[..len]);
    }
    data
}

// ── Timing helper ─────────────────────────────────────────────────────────────

pub fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before UNIX epoch")
        .as_micros() as u64
}
