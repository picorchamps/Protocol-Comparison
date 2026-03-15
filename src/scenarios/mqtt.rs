use std::time::Instant;

use anyhow::{Context, Result};
use base64::{engine::general_purpose::STANDARD as B64, Engine};
use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, QoS};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::cli::{ReceiverArgs, SenderArgs, TestType};
use crate::metrics::MetricsReport;
use crate::payload::{generate_throughput_data, now_us, TestPayload, THROUGHPUT_SIZE};

const DEFAULT_PORT: u16 = 1883;

const TOPIC_DATA: &str = "pico/throughput";
const TOPIC_PING: &str = "pico/ping";
const TOPIC_PONG: &str = "pico/pong";
const TOPIC_CTRL: &str = "pico/ctrl";
const TOPIC_READY: &str = "pico/sync/ready";
const TOPIC_LATENCY_RESULTS: &str = "pico/latency_results";

#[derive(Serialize, Deserialize)]
struct MqttChunk {
    transfer_id: u64,
    chunk_index: u32,
    total_chunks: u32,
    data: String,
}

// ── Shared helpers ────────────────────────────────────────────────────────────

/// Connects to the broker, subscribes to `auto_subscribe` topics on every
/// (re)connect, and returns once the initial connection + subscriptions are
/// established.  Publish events are forwarded on the returned channel.
async fn make_client(
    id: &str,
    host: &str,
    port: u16,
    auto_subscribe: Vec<(String, QoS)>,
) -> Result<(AsyncClient, mpsc::UnboundedReceiver<(String, bytes::Bytes)>)> {
    let mut opts = MqttOptions::new(id, host, port);
    opts.set_keep_alive(std::time::Duration::from_secs(30));
    opts.set_max_packet_size(100 * 1024 * 1024, 100 * 1024 * 1024);

    let (client, mut eventloop) = AsyncClient::new(opts, 1024);
    let (tx, rx) = mpsc::unbounded_channel::<(String, bytes::Bytes)>();

    let resub_client = client.clone();
    let id_str = id.to_string();

    // Fired once when the first ConnAck arrives so make_client can return.
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<()>();
    let mut ready_tx_opt = Some(ready_tx);

    tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Incoming::ConnAck(_))) => {
                    eprintln!("[mqtt/{}] Connected to broker", id_str);
                    // Re-subscribe on every connect (needed after clean-session reconnects).
                    for (topic, qos) in &auto_subscribe {
                        if let Err(e) = resub_client.subscribe(topic.as_str(), *qos).await {
                            eprintln!("[mqtt/{}] subscribe {topic} failed: {e}", id_str);
                        }
                    }
                    if let Some(tx) = ready_tx_opt.take() {
                        let _ = tx.send(());
                    }
                }
                Ok(Event::Incoming(Incoming::Publish(p))) => {
                    if tx.send((p.topic.clone(), p.payload.clone())).is_err() {
                        // Receiver dropped — test is done.
                        break;
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    eprintln!("[mqtt/{}] error: {e} — retrying in 1 s", id_str);
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        }
    });

    tokio::time::timeout(std::time::Duration::from_secs(30), ready_rx)
        .await
        .context("Timed out waiting for MQTT broker connection")?
        .context("Event loop task ended before connecting")?;

    Ok((client, rx))
}

// ── Receiver ─────────────────────────────────────────────────────────────────

pub async fn run_receiver(args: ReceiverArgs) -> Result<MetricsReport> {
    let port = args.port.unwrap_or(DEFAULT_PORT);
    let topics = match args.test {
        TestType::Throughput => vec![(TOPIC_DATA.to_string(), QoS::AtMostOnce)],
        TestType::Latency => vec![
            (TOPIC_PING.to_string(), QoS::AtLeastOnce),
            (TOPIC_LATENCY_RESULTS.to_string(), QoS::AtLeastOnce),
        ],
    };
    let (client, rx) = make_client("pico-receiver", "mosquitto", port, topics).await?;

    let mut report = MetricsReport::new("mqtt", &format!("{:?}", args.test).to_lowercase());
    match args.test {
        TestType::Throughput => mqtt_receiver_throughput(client, rx, &mut report).await?,
        TestType::Latency => mqtt_receiver_latency(client, rx, &mut report).await?,
    }

    report.print_summary();
    report.save(&args.output)?;
    Ok(report)
}

async fn mqtt_receiver_throughput(
    client: AsyncClient,
    mut rx: mpsc::UnboundedReceiver<(String, bytes::Bytes)>,
    report: &mut MetricsReport,
) -> Result<()> {
    // Clear any stale retained ready message, then publish a fresh timestamped one.
    client.publish(TOPIC_READY, QoS::AtLeastOnce, true, b"".to_vec()).await?;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    client.publish(TOPIC_READY, QoS::AtLeastOnce, true, ready_timestamp().into_bytes()).await?;
    println!("[mqtt/receiver] Subscribed to {TOPIC_DATA}, signalled ready");

    let mut total_bytes = 0u64;
    let mut deser_ms = 0.0f64;
    let mut t0_opt: Option<Instant> = None;
    let mut expected_chunks: Option<u32> = None;
    let mut received_chunks = 0u32;

    while let Some((_topic, payload)) = rx.recv().await {
        let t_deser = Instant::now();
        let chunk: MqttChunk = serde_json::from_slice(&payload)?;
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
    println!("[mqtt/receiver] Received {total_bytes} bytes in {duration_ms:.1} ms");

    client.publish(TOPIC_CTRL, QoS::AtLeastOnce, false, b"done".to_vec()).await?;
    // Clear the retained ready so the next run starts clean.
    client.publish(TOPIC_READY, QoS::AtLeastOnce, true, b"".to_vec()).await?;

    report.set_throughput(total_bytes, duration_ms, deser_ms);
    Ok(())
}

async fn mqtt_receiver_latency(
    client: AsyncClient,
    mut rx: mpsc::UnboundedReceiver<(String, bytes::Bytes)>,
    report: &mut MetricsReport,
) -> Result<()> {
    // Clear stale retained messages from prior runs, then signal readiness.
    client.publish(TOPIC_LATENCY_RESULTS, QoS::AtLeastOnce, true, b"".to_vec()).await?;
    client.publish(TOPIC_READY, QoS::AtLeastOnce, true, b"".to_vec()).await?;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    client.publish(TOPIC_READY, QoS::AtLeastOnce, true, ready_timestamp().into_bytes()).await?;
    println!("[mqtt/receiver] Subscribed to {TOPIC_PING}, signalled ready");

    let mut count = 0usize;
    while let Some((topic, payload)) = rx.recv().await {
        if topic != TOPIC_PING {
            continue;
        }
        client.publish(TOPIC_PONG, QoS::AtLeastOnce, false, payload.to_vec()).await?;
        count += 1;
        if count >= 1000 {
            break;
        }
    }
    println!("[mqtt/receiver] Echoed {count} pings — waiting for RTT results…");

    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(30);
    loop {
        tokio::select! {
            msg = rx.recv() => {
                match msg {
                    Some((topic, payload)) if topic == TOPIC_LATENCY_RESULTS && !payload.is_empty() => {
                        let results: serde_json::Value = serde_json::from_slice(&payload)?;
                        let rtts: Vec<u64> = serde_json::from_value(results["rtts_us"].clone())?;
                        let total_ms = results["total_ms"].as_f64().unwrap_or(0.0);
                        report.set_latency(rtts, total_ms);
                        // Clear the retained result so the next run starts clean.
                        client.publish(TOPIC_LATENCY_RESULTS, QoS::AtLeastOnce, true, b"".to_vec()).await?;
                        break;
                    }
                    Some(_) => {}
                    None => break,
                }
            }
            _ = tokio::time::sleep_until(deadline) => {
                eprintln!("[mqtt/receiver] Timed out waiting for latency results");
                break;
            }
        }
    }

    client.publish(TOPIC_READY, QoS::AtLeastOnce, true, b"".to_vec()).await?;
    Ok(())
}

// ── Sender ───────────────────────────────────────────────────────────────────

pub async fn run_sender(args: SenderArgs) -> Result<()> {
    let port = args.port.unwrap_or(DEFAULT_PORT);
    let topics = match args.test {
        TestType::Throughput => vec![
            (TOPIC_CTRL.to_string(), QoS::AtLeastOnce),
            (TOPIC_READY.to_string(), QoS::AtLeastOnce),
        ],
        TestType::Latency => vec![
            (TOPIC_PONG.to_string(), QoS::AtLeastOnce),
            (TOPIC_READY.to_string(), QoS::AtLeastOnce),
        ],
    };
    let (client, mut rx) = make_client("pico-sender", &args.host, port, topics).await?;

    match args.test {
        TestType::Throughput => mqtt_sender_throughput(client, &mut rx, &args).await?,
        TestType::Latency => mqtt_sender_latency(client, &mut rx).await?,
    }
    Ok(())
}

async fn mqtt_sender_throughput(
    client: AsyncClient,
    rx: &mut mpsc::UnboundedReceiver<(String, bytes::Bytes)>,
    args: &SenderArgs,
) -> Result<()> {
    println!("[mqtt/sender] Waiting for receiver ready signal…");
    wait_for_ready(rx).await?;

    println!("[mqtt/sender] Generating 1 GiB payload…");
    let data = generate_throughput_data();
    let chunk_size = args.chunk_size;
    let total_chunks = data.len().div_ceil(chunk_size) as u32;
    let transfer_id = now_us();

    println!("[mqtt/sender] Publishing {total_chunks} chunks of {chunk_size} bytes…");
    let t0 = Instant::now();

    for (i, window) in data.chunks(chunk_size).enumerate() {
        let chunk = MqttChunk {
            transfer_id,
            chunk_index: i as u32,
            total_chunks,
            data: B64.encode(window),
        };
        client
            .publish(TOPIC_DATA, QoS::AtMostOnce, false, serde_json::to_vec(&chunk)?)
            .await?;
    }

    loop {
        match rx.recv().await {
            Some((topic, _)) if topic == TOPIC_CTRL => break,
            Some(_) => {}
            None => anyhow::bail!("MQTT connection closed before receiving done signal"),
        }
    }

    let elapsed = t0.elapsed().as_secs_f64() * 1_000.0;
    let mbps = (THROUGHPUT_SIZE as f64 / (1024.0 * 1024.0)) / (elapsed / 1_000.0);
    println!("[mqtt/sender] Done: {elapsed:.1} ms  ({mbps:.2} MB/s)");
    Ok(())
}

async fn mqtt_sender_latency(
    client: AsyncClient,
    rx: &mut mpsc::UnboundedReceiver<(String, bytes::Bytes)>,
) -> Result<()> {
    println!("[mqtt/sender] Waiting for receiver ready signal…");
    wait_for_ready(rx).await?;

    println!("[mqtt/sender] Starting 1000-message latency test…");
    let mut rtts: Vec<u64> = Vec::with_capacity(1000);
    let t_total = Instant::now();

    for seq in 0u32..1000 {
        let mut payload = TestPayload::new_small(seq);
        payload.stamp();
        client
            .publish(TOPIC_PING, QoS::AtLeastOnce, false, serde_json::to_vec(&payload)?)
            .await?;

        loop {
            match rx.recv().await {
                Some((topic, _)) if topic == TOPIC_PONG => break,
                Some(_) => {}
                None => anyhow::bail!("MQTT connection closed during latency test"),
            }
        }
        rtts.push(payload.rtt_us());
    }

    let total_ms = t_total.elapsed().as_secs_f64() * 1_000.0;
    print_latency_summary(&rtts, total_ms);

    // Use retain=true so the broker re-delivers to the receiver even if it briefly
    // reconnects between the ping phase ending and the results being read.
    let results = serde_json::json!({"rtts_us": rtts, "total_ms": total_ms});
    client
        .publish(TOPIC_LATENCY_RESULTS, QoS::AtLeastOnce, true, results.to_string().into_bytes())
        .await?;

    // Give the receiver time to read and save the results before we exit.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    Ok(())
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn ready_timestamp() -> String {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .to_string()
}

/// Wait for a retained TOPIC_READY whose timestamp is less than 30 s old.
async fn wait_for_ready(rx: &mut mpsc::UnboundedReceiver<(String, bytes::Bytes)>) -> Result<()> {
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(60);
    loop {
        tokio::select! {
            msg = rx.recv() => {
                match msg {
                    Some((topic, payload)) if topic == TOPIC_READY && !payload.is_empty() => {
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs();
                        let ts = std::str::from_utf8(&payload)
                            .ok()
                            .and_then(|s| s.parse::<u64>().ok())
                            .unwrap_or(0);
                        let age = now.saturating_sub(ts);
                        if age < 30 {
                            println!("[mqtt/sender] Receiver is ready (message age {age}s)");
                            return Ok(());
                        }
                        println!("[mqtt/sender] Ignoring stale ready message (age {age}s), waiting…");
                    }
                    Some(_) => {}
                    None => anyhow::bail!("MQTT connection closed while waiting for ready"),
                }
            }
            _ = tokio::time::sleep_until(deadline) => {
                anyhow::bail!("Receiver did not become ready within 60 s");
            }
        }
    }
}

fn print_latency_summary(rtts: &[u64], total_ms: f64) {
    let mut sorted = rtts.to_vec();
    sorted.sort_unstable();
    let p = |pct: f64| sorted[((pct / 100.0) * (sorted.len() as f64 - 1.0)) as usize];
    let mean: f64 = sorted.iter().sum::<u64>() as f64 / sorted.len() as f64;
    println!(
        "[mqtt/sender] p50={} p95={} p99={} mean={:.1} msg/s={:.0}",
        p(50.0),
        p(95.0),
        p(99.0),
        mean,
        (sorted.len() as f64 / total_ms) * 1_000.0,
    );
}
