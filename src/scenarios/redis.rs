use std::time::Instant;

use anyhow::Result;
use base64::{engine::general_purpose::STANDARD as B64, Engine};
use futures_util::StreamExt;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};

use crate::cli::{ReceiverArgs, SenderArgs, TestType};
use crate::metrics::MetricsReport;
use crate::payload::{generate_throughput_data, now_us, TestPayload, THROUGHPUT_SIZE};

const DEFAULT_REDIS_PORT: u16 = 6379;

// Pub/sub channel names
const CH_DATA: &str = "pico:throughput";
const CH_PING: &str = "pico:ping";
const CH_PONG: &str = "pico:pong";
const CH_CTRL: &str = "pico:ctrl";
const KEY_READY: &str = "pico:ready";

#[derive(Serialize, Deserialize)]
struct RedisChunk {
    transfer_id: u64,
    chunk_index: u32,
    total_chunks: u32,
    /// Binary data encoded as base64 (JSON is text-only).
    data: String,
}

fn redis_url(host: &str, port: u16) -> String {
    format!("redis://{host}:{port}")
}

// ── Receiver ─────────────────────────────────────────────────────────────────

pub async fn run_receiver(args: ReceiverArgs) -> Result<MetricsReport> {
    let port = args.port.unwrap_or(DEFAULT_REDIS_PORT);
    let host = "redis"; // always connect to the redis service
    let url = redis_url(host, port);
    let client = redis::Client::open(url.clone())?;

    let mut report = MetricsReport::new("redis", &format!("{:?}", args.test).to_lowercase());

    // NOTE: each scenario function sets pico:ready AFTER subscribing to its
    // data channel, so the sender can never publish before we are listening.
    match args.test {
        TestType::Throughput => redis_receiver_throughput(client, &mut report).await?,
        TestType::Latency => redis_receiver_latency(client, &mut report).await?,
    }

    report.print_summary();
    report.save(&args.output)?;
    Ok(report)
}

async fn redis_receiver_throughput(client: redis::Client, report: &mut MetricsReport) -> Result<()> {
    let mut pubsub = client.get_async_pubsub().await?;
    pubsub.subscribe(CH_DATA).await?;
    println!("[redis/receiver] Subscribed to {CH_DATA}");

    // Signal ready AFTER subscription is established — the sender polls this key
    // and must not start publishing before we are actually listening.
    let mut pub_conn = client.get_multiplexed_async_connection().await?;
    let _: () = pub_conn.set_ex(KEY_READY, "1", 60u64).await?;
    println!("[redis/receiver] Set {KEY_READY} — ready");

    let mut stream = pubsub.into_on_message();
    let mut total_bytes = 0u64;
    let mut deser_ms = 0.0f64;
    let mut t0_opt: Option<Instant> = None;
    let mut expected_chunks: Option<u32> = None;
    let mut received_chunks = 0u32;

    while let Some(msg) = stream.next().await {
        let json_str: String = msg.get_payload()?;
        let t_deser = Instant::now();
        let chunk: RedisChunk = serde_json::from_str(&json_str)?;
        let data = B64.decode(&chunk.data)?;
        deser_ms += t_deser.elapsed().as_secs_f64() * 1_000.0;

        if t0_opt.is_none() {
            t0_opt = Some(Instant::now());
            expected_chunks = Some(chunk.total_chunks);
        }

        total_bytes += data.len() as u64;
        received_chunks += 1;

        if received_chunks >= expected_chunks.unwrap_or(u32::MAX) {
            break;
        }
    }

    let duration_ms = t0_opt.map(|t| t.elapsed().as_secs_f64() * 1_000.0).unwrap_or(0.0);
    println!("[redis/receiver] Received {total_bytes} bytes in {duration_ms:.1} ms");

    // Publish done signal
    let _: i64 = pub_conn
        .publish(CH_CTRL, serde_json::json!({"status":"done","bytes":total_bytes}).to_string())
        .await?;

    report.set_throughput(total_bytes, duration_ms, deser_ms);
    Ok(())
}

const KEY_LATENCY_RESULTS: &str = "pico:latency_results";

async fn redis_receiver_latency(client: redis::Client, report: &mut MetricsReport) -> Result<()> {
    let mut pubsub = client.get_async_pubsub().await?;
    pubsub.subscribe(CH_PING).await?;
    let mut pub_conn = client.get_multiplexed_async_connection().await?;
    // Signal ready only after subscription is live.
    let _: () = pub_conn.set_ex(KEY_READY, "1", 60u64).await?;
    println!("[redis/receiver] Subscribed to {CH_PING}, set {KEY_READY} — ready");

    let mut stream = pubsub.into_on_message();
    let mut count = 0usize;

    while let Some(msg) = stream.next().await {
        let json_str: String = msg.get_payload()?;
        let _: i64 = pub_conn.publish(CH_PONG, &json_str).await?;
        count += 1;
        if count >= 1000 {
            break;
        }
    }
    println!("[redis/receiver] Echoed {count} messages — waiting for RTT results key…");

    // Sender writes RTT data to a Redis key after all exchanges.
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(10);
    let results_json: Option<String> = loop {
        let val: Option<String> = pub_conn.get(KEY_LATENCY_RESULTS).await?;
        if val.is_some() {
            break val;
        }
        if tokio::time::Instant::now() >= deadline {
            break None;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    };

    if let Some(json) = results_json {
        let results: serde_json::Value = serde_json::from_str(&json)?;
        let rtts: Vec<u64> = serde_json::from_value(results["rtts_us"].clone())?;
        let total_ms: f64 = results["total_ms"].as_f64().unwrap_or(0.0);
        report.set_latency(rtts, total_ms);
    }
    Ok(())
}

// ── Sender ───────────────────────────────────────────────────────────────────

pub async fn run_sender(args: SenderArgs) -> Result<()> {
    let port = args.port.unwrap_or(DEFAULT_REDIS_PORT);
    // For Redis, --host is the Redis server (not a direct peer).
    let url = redis_url(&args.host, port);
    let client = redis::Client::open(url)?;

    wait_for_receiver_ready(&client).await?;

    match args.test {
        TestType::Throughput => redis_sender_throughput(&client, &args).await?,
        TestType::Latency => redis_sender_latency(&client).await?,
    }
    Ok(())
}

async fn redis_sender_throughput(client: &redis::Client, args: &SenderArgs) -> Result<()> {
    let mut pub_conn = client.get_multiplexed_async_connection().await?;

    // Subscribe to ctrl channel to wait for the done signal
    let mut ctrl_sub = client.get_async_pubsub().await?;
    ctrl_sub.subscribe(CH_CTRL).await?;
    let mut ctrl_stream = ctrl_sub.into_on_message();

    println!("[redis/sender] Generating 1 GiB payload…");
    let data = generate_throughput_data();
    let chunk_size = args.chunk_size;
    let total_chunks = data.len().div_ceil(chunk_size) as u32;
    let transfer_id: u64 = now_us();

    println!("[redis/sender] Publishing {total_chunks} chunks…");
    let t0 = Instant::now();

    for (i, window) in data.chunks(chunk_size).enumerate() {
        let chunk = RedisChunk {
            transfer_id,
            chunk_index: i as u32,
            total_chunks,
            data: B64.encode(window),
        };
        let json = serde_json::to_string(&chunk)?;
        let _: i64 = pub_conn.publish(CH_DATA, json).await?;
    }

    // Wait for receiver done signal
    let _msg = ctrl_stream.next().await;
    let elapsed = t0.elapsed().as_secs_f64() * 1_000.0;
    let mbps = (THROUGHPUT_SIZE as f64 / (1024.0 * 1024.0)) / (elapsed / 1_000.0);
    println!("[redis/sender] Done: {elapsed:.1} ms  ({mbps:.2} MB/s)");
    Ok(())
}

async fn redis_sender_latency(client: &redis::Client) -> Result<()> {
    let mut pub_conn = client.get_multiplexed_async_connection().await?;
    let mut pong_sub = client.get_async_pubsub().await?;
    pong_sub.subscribe(CH_PONG).await?;
    let mut pong_stream = pong_sub.into_on_message();

    println!("[redis/sender] Starting 1000-message latency test…");
    let mut rtts: Vec<u64> = Vec::with_capacity(1000);
    let t_total = Instant::now();

    for seq in 0u32..1000 {
        let mut payload = TestPayload::new_small(seq);
        payload.stamp();
        let json = serde_json::to_string(&payload)?;

        let _: i64 = pub_conn.publish(CH_PING, &json).await?;

        // Wait for pong
        if let Some(msg) = pong_stream.next().await {
            let rtt = payload.rtt_us();
            rtts.push(rtt);
            let _: String = msg.get_payload()?; // consume the message
        }
    }

    let total_ms = t_total.elapsed().as_secs_f64() * 1_000.0;
    print_latency_summary(&rtts, total_ms);

    // Write RTT data to Redis key so the receiver can include it in the report.
    let results = serde_json::json!({"rtts_us": rtts, "total_ms": total_ms});
    let mut store_conn = client.get_multiplexed_async_connection().await?;
    let _: () = store_conn
        .set_ex(KEY_LATENCY_RESULTS, results.to_string(), 60u64)
        .await?;
    Ok(())
}

// ── Helpers ───────────────────────────────────────────────────────────────────

async fn wait_for_receiver_ready(client: &redis::Client) -> Result<()> {
    println!("[redis/sender] Waiting for receiver to set {KEY_READY}…");
    let mut conn = client.get_multiplexed_async_connection().await?;
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(30);
    loop {
        let val: Option<String> = conn.get(KEY_READY).await?;
        if val.as_deref() == Some("1") {
            println!("[redis/sender] Receiver is ready");
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!("Receiver did not become ready within 30 s");
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
}

fn print_latency_summary(rtts: &[u64], total_ms: f64) {
    let mut sorted = rtts.to_vec();
    sorted.sort_unstable();
    let p = |pct: f64| sorted[((pct / 100.0) * (sorted.len() as f64 - 1.0)) as usize];
    let mean: f64 = sorted.iter().sum::<u64>() as f64 / sorted.len() as f64;
    println!(
        "[redis/sender] p50={} p95={} p99={} mean={:.1} msg/s={:.0}",
        p(50.0),
        p(95.0),
        p(99.0),
        mean,
        (sorted.len() as f64 / total_ms) * 1_000.0,
    );
}
