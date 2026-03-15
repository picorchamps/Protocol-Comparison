use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use quinn::{ClientConfig, Endpoint, ServerConfig};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

use crate::cli::{ReceiverArgs, SenderArgs, TestType};
use tokio::io::AsyncWriteExt as _;

use crate::framing::{read_framed, read_large, recv_sync, send_sync, write_framed, write_large};
use crate::metrics::MetricsReport;
use crate::payload::{generate_throughput_data, TestPayload, THROUGHPUT_SIZE};

const DEFAULT_PORT: u16 = 4433;
// ── TLS helpers ───────────────────────────────────────────────────────────────
//
// quinn::crypto::rustls::QuicServerConfig / QuicClientConfig set the required
// "h3" / internal QUIC ALPN themselves.  Do NOT set alpn_protocols on the
// rustls config — doing so overrides quinn's built-in ALPN and produces the
// "peer doesn't support any known protocol" (error 120) handshake failure.

fn make_server_config(certs_dir: &str) -> Result<ServerConfig> {
    let cert_path = format!("{certs_dir}/server.crt");
    let key_path = format!("{certs_dir}/server.key");

    let (certs, key) = if std::path::Path::new(&cert_path).exists() {
        load_certs_from_files(&cert_path, &key_path)?
    } else {
        generate_self_signed(certs_dir)?
    };

    let tls = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;
    // No alpn_protocols — quinn sets its own ALPN token internally.

    Ok(ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(tls)?,
    )))
}

fn make_client_config(certs_dir: &str) -> Result<ClientConfig> {
    let ca_path = format!("{certs_dir}/ca.crt");
    let ca_pem = std::fs::read(&ca_path)
        .map_err(|e| anyhow::anyhow!("Cannot read {ca_path}: {e}. Run scripts/gen_certs.sh first."))?;

    let mut roots = rustls::RootCertStore::empty();
    for cert in rustls_pemfile::certs(&mut ca_pem.as_slice()) {
        roots.add(cert?)?;
    }

    let tls = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    // No alpn_protocols — quinn sets its own ALPN token internally.

    Ok(ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(tls)?,
    )))
}

fn load_certs_from_files(
    cert_path: &str,
    key_path: &str,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let cert_pem = std::fs::read(cert_path)?;
    let key_pem = std::fs::read(key_path)?;
    let certs: Vec<CertificateDer<'static>> =
        rustls_pemfile::certs(&mut cert_pem.as_slice()).collect::<Result<_, _>>()?;
    let key = rustls_pemfile::private_key(&mut key_pem.as_slice())?
        .ok_or_else(|| anyhow::anyhow!("No private key found in {key_path}"))?;
    Ok((certs, key))
}

fn generate_self_signed(
    certs_dir: &str,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    println!("[quic] Generating self-signed cert (no cert files found in {certs_dir})");
    let key_pair = rcgen::KeyPair::generate()?;
    let cert = rcgen::CertificateParams::new(vec![
        "receiver".to_string(),
        "localhost".to_string(),
        "127.0.0.1".to_string(),
    ])?
    .self_signed(&key_pair)?;

    let cert_der: CertificateDer<'static> = cert.der().clone().into_owned().into();
    let key_der: PrivateKeyDer<'static> =
        PrivatePkcs8KeyDer::from(key_pair.serialize_der()).into();

    // Save so the sender container can read the CA cert
    let dir = std::path::Path::new(certs_dir);
    if dir.exists() {
        std::fs::write(dir.join("server.crt"), cert.pem())?;
        std::fs::write(dir.join("server.key"), key_pair.serialize_pem())?;
        std::fs::write(dir.join("ca.crt"), cert.pem())?;
        println!("[quic] Saved certs to {certs_dir}");
    }

    Ok((vec![cert_der], key_der))
}

// ── Receiver ─────────────────────────────────────────────────────────────────

pub async fn run_receiver(args: ReceiverArgs) -> Result<MetricsReport> {
    let port = args.port.unwrap_or(DEFAULT_PORT);
    let server_config = make_server_config(&args.certs_dir)?;

    let addr: SocketAddr = format!("0.0.0.0:{port}").parse()?;
    let endpoint = Endpoint::server(server_config, addr)?;
    println!("[quic/receiver] Listening on {addr}");

    let incoming = endpoint.accept().await.ok_or_else(|| anyhow::anyhow!("No incoming connection"))?;
    let conn = incoming.await?;
    println!("[quic/receiver] Connection from {}", conn.remote_address());

    let mut report = MetricsReport::new("quic", &format!("{:?}", args.test).to_lowercase());

    match args.test {
        TestType::Throughput => quic_receiver_throughput(conn, &mut report).await?,
        TestType::Latency => quic_receiver_latency(conn, &mut report).await?,
    }

    report.print_summary();
    report.save(&args.output)?;
    Ok(report)
}

async fn quic_receiver_throughput(conn: quinn::Connection, report: &mut MetricsReport) -> Result<()> {
    let (mut send, mut recv) = conn.accept_bi().await?;

    // Sync
    recv_sync(&mut recv).await?;
    send_sync(&mut send).await?;
    println!("[quic/receiver] Sync OK — waiting for 1 GiB…");

    let t0 = Instant::now();
    let data = read_large(&mut recv).await?;
    let duration_ms = t0.elapsed().as_secs_f64() * 1_000.0;

    println!("[quic/receiver] Received {} bytes in {duration_ms:.1} ms", data.len());
    // ACK
    send.write_all(b"ACK!").await?;
    send.finish()?;

    report.set_throughput(data.len() as u64, duration_ms, 0.0);
    Ok(())
}

async fn quic_receiver_latency(conn: quinn::Connection, report: &mut MetricsReport) -> Result<()> {
    let (mut send, mut recv) = conn.accept_bi().await?;

    recv_sync(&mut recv).await?;
    send_sync(&mut send).await?;
    println!("[quic/receiver] Sync OK — starting latency echo…");

    let mut count = 0usize;
    loop {
        let raw = match read_framed(&mut recv).await {
            Ok(b) => b,
            Err(_) => break,
        };
        if raw.is_empty() {
            break;
        }
        write_framed(&mut send, &raw).await?;
        send.flush().await?;
        count += 1;
        if count >= 1000 {
            break;
        }
    }
    println!("[quic/receiver] Echoed {count} messages — waiting for RTT results…");

    // Read RTT summary sent by the sender after all exchanges.
    let results_raw = read_framed(&mut recv).await?;
    let results: serde_json::Value = serde_json::from_slice(&results_raw)?;
    let rtts: Vec<u64> = serde_json::from_value(results["rtts_us"].clone())?;
    let total_ms: f64 = results["total_ms"].as_f64().unwrap_or(0.0);
    report.set_latency(rtts, total_ms);
    Ok(())
}

// ── Sender ───────────────────────────────────────────────────────────────────

pub async fn run_sender(args: SenderArgs) -> Result<()> {
    let port = args.port.unwrap_or(DEFAULT_PORT);
    let client_config = make_client_config(&args.certs_dir)?;

    let mut endpoint = Endpoint::client("0.0.0.0:0".parse()?)?;
    endpoint.set_default_client_config(client_config);

    let addr = tokio::net::lookup_host(format!("{}:{}", args.host, port))
        .await?
        .next()
        .ok_or_else(|| anyhow::anyhow!("DNS lookup returned no addresses for {}", args.host))?;
    println!("[quic/sender] Connecting to {} ({addr})…", args.host);

    // Retry until the receiver is up
    let conn = connect_with_retry(&endpoint, addr, &args.host).await?;
    println!("[quic/sender] Connected");

    match args.test {
        TestType::Throughput => quic_sender_throughput(conn).await?,
        TestType::Latency => quic_sender_latency(conn).await?,
    }
    Ok(())
}

async fn quic_sender_throughput(conn: quinn::Connection) -> Result<()> {
    let (mut send, mut recv) = conn.open_bi().await?;

    send_sync(&mut send).await?;
    recv_sync(&mut recv).await?;
    println!("[quic/sender] Sync OK — generating and sending 1 GiB…");

    let data = generate_throughput_data();
    let t0 = Instant::now();
    write_large(&mut send, &data).await?;
    send.flush().await?;

    // Wait for ACK
    let mut ack = [0u8; 4];
    recv.read_exact(&mut ack).await?;

    let elapsed = t0.elapsed().as_secs_f64() * 1_000.0;
    let mbps = (THROUGHPUT_SIZE as f64 / (1024.0 * 1024.0)) / (elapsed / 1_000.0);
    println!("[quic/sender] Done: {elapsed:.1} ms  ({mbps:.2} MB/s)");
    Ok(())
}

async fn quic_sender_latency(conn: quinn::Connection) -> Result<()> {
    let (mut send, mut recv) = conn.open_bi().await?;

    send_sync(&mut send).await?;
    recv_sync(&mut recv).await?;
    println!("[quic/sender] Sync OK — starting 1000-message latency test…");

    let mut rtts: Vec<u64> = Vec::with_capacity(1000);
    let t_total = Instant::now();

    for seq in 0u32..1000 {
        let mut payload = TestPayload::new_small(seq);
        payload.stamp();
        let encoded = bincode::serialize(&payload)?;

        write_framed(&mut send, &encoded).await?;
        send.flush().await?;

        let raw = read_framed(&mut recv).await?;
        let rtt = payload.rtt_us();
        rtts.push(rtt);

        // Verify decode
        let _: TestPayload = bincode::deserialize(&raw)?;
    }

    let total_ms = t_total.elapsed().as_secs_f64() * 1_000.0;
    print_latency_summary(&rtts, total_ms);

    // Forward RTT data to receiver for the JSON report.
    let results = serde_json::json!({"rtts_us": rtts, "total_ms": total_ms});
    write_framed(&mut send, results.to_string().as_bytes()).await?;
    send.flush().await?;
    Ok(())
}

// ── Helpers ───────────────────────────────────────────────────────────────────

async fn connect_with_retry(
    endpoint: &Endpoint,
    addr: SocketAddr,
    server_name: &str,
) -> Result<quinn::Connection> {
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(30);
    loop {
        match endpoint.connect(addr, server_name)?.await {
            Ok(conn) => return Ok(conn),
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
        "[quic/sender] p50={} p95={} p99={} mean={:.1} msg/s={:.0}",
        p(50.0),
        p(95.0),
        p(99.0),
        mean,
        (sorted.len() as f64 / total_ms) * 1_000.0,
    );
}
