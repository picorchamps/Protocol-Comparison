use std::time::Instant;

use anyhow::Result;
use prost::Message;
use tokio::net::{TcpListener, TcpStream};

use crate::cli::{ReceiverArgs, SenderArgs, TestType};
use crate::framing::{read_framed, recv_sync, send_sync, write_framed};
use crate::metrics::MetricsReport;
use crate::payload::{generate_throughput_data, now_us, TestPayload, THROUGHPUT_SIZE};
use crate::proto;

const DEFAULT_PORT: u16 = 4434;

// ── Receiver ─────────────────────────────────────────────────────────────────

pub async fn run_receiver(args: ReceiverArgs) -> Result<MetricsReport> {
    let port = args.port.unwrap_or(DEFAULT_PORT);
    let listener = TcpListener::bind(format!("0.0.0.0:{port}")).await?;
    println!("[tcp/receiver] Listening on 0.0.0.0:{port}");

    let (stream, peer) = listener.accept().await?;
    println!("[tcp/receiver] Connection from {peer}");
    stream.set_nodelay(true)?;

    let mut report = MetricsReport::new("tcp", &format!("{:?}", args.test).to_lowercase());

    match args.test {
        TestType::Throughput => tcp_receiver_throughput(stream, &mut report, &args).await?,
        TestType::Latency => tcp_receiver_latency(stream, &mut report).await?,
    }

    report.print_summary();
    report.save(&args.output)?;
    Ok(report)
}

async fn tcp_receiver_throughput(
    mut stream: TcpStream,
    report: &mut MetricsReport,
    _args: &ReceiverArgs,
) -> Result<()> {
    // Sync handshake
    recv_sync(&mut stream).await?;
    send_sync(&mut stream).await?;
    println!("[tcp/receiver] Sync complete, waiting for data…");

    let t0 = Instant::now();
    let mut deser_ms = 0.0f64;
    let mut total_bytes = 0u64;

    // Read chunks until we get all of them
    loop {
        let raw = read_framed(&mut stream).await?;
        let t_deser = Instant::now();
        let chunk = proto::ThroughputChunk::decode(raw.as_slice())?;
        deser_ms += t_deser.elapsed().as_secs_f64() * 1_000.0;
        total_bytes += chunk.data.len() as u64;

        if chunk.chunk_index + 1 >= chunk.total_chunks {
            break;
        }
    }

    let duration_ms = t0.elapsed().as_secs_f64() * 1_000.0;
    println!("[tcp/receiver] Received {total_bytes} bytes in {duration_ms:.1} ms");

    // Send ACK
    use tokio::io::AsyncWriteExt;
    stream.write_all(b"ACK!").await?;

    report.set_throughput(total_bytes, duration_ms, deser_ms);
    Ok(())
}

async fn tcp_receiver_latency(mut stream: TcpStream, report: &mut MetricsReport) -> Result<()> {
    recv_sync(&mut stream).await?;
    send_sync(&mut stream).await?;
    println!("[tcp/receiver] Sync complete, starting latency echo…");

    let mut count = 0usize;
    loop {
        let raw = match read_framed(&mut stream).await {
            Ok(b) => b,
            Err(_) => break,
        };
        if raw.is_empty() {
            break;
        }
        // Echo back the exact bytes unchanged (sender re-decodes to get timestamp)
        write_framed(&mut stream, &raw).await?;
        count += 1;
        if count >= 1000 {
            break;
        }
    }
    println!("[tcp/receiver] Echoed {count} messages — waiting for RTT results…");

    // Sender sends one final framed message with all RTT samples and total time.
    let results_raw = read_framed(&mut stream).await?;
    let results: serde_json::Value = serde_json::from_slice(&results_raw)?;
    let rtts: Vec<u64> = serde_json::from_value(results["rtts_us"].clone())?;
    let total_ms: f64 = results["total_ms"].as_f64().unwrap_or(0.0);
    report.set_latency(rtts, total_ms);
    Ok(())
}

// ── Sender ───────────────────────────────────────────────────────────────────

pub async fn run_sender(args: SenderArgs) -> Result<()> {
    let port = args.port.unwrap_or(DEFAULT_PORT);
    let addr = format!("{}:{}", args.host, port);
    println!("[tcp/sender] Connecting to {addr}…");

    let stream = connect_with_retry(&addr).await?;
    stream.set_nodelay(true)?;
    println!("[tcp/sender] Connected");

    match args.test {
        TestType::Throughput => tcp_sender_throughput(stream, &args).await?,
        TestType::Latency => tcp_sender_latency(stream).await?,
    }
    Ok(())
}

async fn tcp_sender_throughput(mut stream: TcpStream, args: &SenderArgs) -> Result<()> {
    // Sync
    send_sync(&mut stream).await?;
    recv_sync(&mut stream).await?;
    println!("[tcp/sender] Sync OK — generating 1 GiB payload…");

    let data = generate_throughput_data();
    let chunk_size = args.chunk_size;
    let total_chunks = data.len().div_ceil(chunk_size) as u32;
    let transfer_id: u64 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    println!("[tcp/sender] Sending {total_chunks} chunks of {chunk_size} bytes…");
    let t0 = Instant::now();

    for (i, window) in data.chunks(chunk_size).enumerate() {
        let chunk = proto::ThroughputChunk {
            transfer_id,
            chunk_index: i as u32,
            total_chunks,
            data: window.to_vec(),
        };
        let mut buf = Vec::with_capacity(chunk.encoded_len());
        chunk.encode(&mut buf)?;
        write_framed(&mut stream, &buf).await?;
    }

    // Wait for ACK
    use tokio::io::AsyncReadExt;
    let mut ack = [0u8; 4];
    stream.read_exact(&mut ack).await?;

    let elapsed = t0.elapsed().as_secs_f64() * 1_000.0;
    let mbps = (THROUGHPUT_SIZE as f64 / (1024.0 * 1024.0)) / (elapsed / 1_000.0);
    println!("[tcp/sender] Done: {elapsed:.1} ms  ({mbps:.2} MB/s)");
    Ok(())
}

async fn tcp_sender_latency(mut stream: TcpStream) -> Result<()> {
    send_sync(&mut stream).await?;
    recv_sync(&mut stream).await?;
    println!("[tcp/sender] Sync OK — starting 1000-message latency test…");

    let mut rtts: Vec<u64> = Vec::with_capacity(1000);
    let t_total = Instant::now();

    for seq in 0u32..1000 {
        let mut payload = TestPayload::new_small(seq);
        payload.stamp();
        let proto_msg = payload.to_proto();
        let mut buf = Vec::with_capacity(proto_msg.encoded_len());
        proto_msg.encode(&mut buf)?;

        write_framed(&mut stream, &buf).await?;

        let raw = read_framed(&mut stream).await?;
        let rtt = now_us().saturating_sub(payload.timestamp_us);
        rtts.push(rtt);

        let _ = proto::TestPayload::decode(raw.as_slice())?;
    }

    let total_ms = t_total.elapsed().as_secs_f64() * 1_000.0;
    print_latency_summary(&rtts, total_ms);

    // Send RTT results to receiver so it can write a complete JSON report.
    let results = serde_json::json!({"rtts_us": rtts, "total_ms": total_ms});
    write_framed(&mut stream, results.to_string().as_bytes()).await?;
    Ok(())
}

// ── Helpers ───────────────────────────────────────────────────────────────────

async fn connect_with_retry(addr: &str) -> Result<TcpStream> {
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(30);
    loop {
        match TcpStream::connect(addr).await {
            Ok(s) => return Ok(s),
            Err(e) => {
                if tokio::time::Instant::now() >= deadline {
                    anyhow::bail!("Could not connect to {addr} within 30 s: {e}");
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
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
        "[tcp/sender] p50={} p95={} p99={} mean={:.1} msg/s={:.0}",
        p(50.0),
        p(95.0),
        p(99.0),
        mean,
        (sorted.len() as f64 / total_ms) * 1_000.0,
    );
}
