use clap::{Args, Parser, Subcommand, ValueEnum};

#[derive(Parser)]
#[command(name = "pico-launcher", about = "IPC benchmark: Redis/QUIC/TCP/MQTT comparison")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Redis pub/sub with JSON serialisation
    Redis(ScenarioArgs),
    /// QUIC streams with bincode serialisation
    Quic(ScenarioArgs),
    /// TCP with protobuf serialisation
    Tcp(ScenarioArgs),
    /// MQTT pub/sub via Mosquitto broker with JSON serialisation
    Mqtt(ScenarioArgs),
    /// Generate HTML dashboard from one or more JSON result files
    Report(ReportArgs),
}

#[derive(Args)]
pub struct ScenarioArgs {
    #[command(subcommand)]
    pub role: Role,
}

#[derive(Subcommand)]
pub enum Role {
    /// Send data to the receiver
    Sender(SenderArgs),
    /// Receive data from the sender and record metrics
    Receiver(ReceiverArgs),
}

#[derive(Args, Clone, Debug)]
pub struct SenderArgs {
    /// Test to run
    #[arg(long, default_value = "throughput")]
    pub test: TestType,

    /// Hostname or IP of the receiver (QUIC/TCP) or Redis server (Redis)
    #[arg(long, default_value = "receiver")]
    pub host: String,

    /// Override the default port for the chosen scenario
    #[arg(long)]
    pub port: Option<u16>,

    /// Chunk size in bytes when chunking the 1 GB payload (Redis / TCP throughput).
    /// Keep this well below Redis's client-output-buffer hard limit (default 32 MB).
    /// Base64 encoding adds ~33% overhead, so 4 MB binary → ~5.4 MB on the wire.
    #[arg(long, default_value = "4194304")]
    pub chunk_size: usize,

    /// Path to the directory containing ca.crt for QUIC TLS
    #[arg(long, default_value = "/certs")]
    pub certs_dir: String,
}

#[derive(Args, Clone, Debug)]
pub struct ReceiverArgs {
    /// Test to run
    #[arg(long, default_value = "throughput")]
    pub test: TestType,

    /// JSON file to write results into
    #[arg(long, default_value = "results.json")]
    pub output: String,

    /// Override the default listen port for the chosen scenario
    #[arg(long)]
    pub port: Option<u16>,

    /// Path to the directory containing server.crt / server.key for QUIC TLS
    #[arg(long, default_value = "/certs")]
    pub certs_dir: String,
}

#[derive(ValueEnum, Clone, Debug, PartialEq)]
pub enum TestType {
    Throughput,
    Latency,
}

#[derive(Args)]
pub struct ReportArgs {
    /// One or more JSON result files produced by a receiver
    pub files: Vec<String>,

    /// Output HTML file
    #[arg(long, default_value = "dashboard.html")]
    pub output: String,
}
