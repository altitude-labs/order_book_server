#![allow(unused_crate_dependencies)]

use std::net::Ipv4Addr;
use std::path::PathBuf;

use clap::{Parser, ValueEnum};
#[cfg(feature = "grpc")]
use grpc::{run_grpc_server, run_grpc_transport};
#[cfg(feature = "grpc")]
use server::run_websocket_transport;
use server::{Result, ServerConfig, SnapshotMode, run_websocket_server};

// The fan-out path allocates heavily (per-level price/size strings, per-coin
// payload groupings, one JSON buffer per send); mimalloc handles that
// multi-threaded churn with far less contention than glibc malloc.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Markets to include in the orderbook
#[derive(Debug, Clone, Copy, ValueEnum, Default)]
pub enum Markets {
    /// Perpetual futures only
    Perps,
    /// Spot markets only (including @ coins)
    Spot,
    /// HIP-3 markets only
    Hip3,
    /// All markets (perps + spot + hip3)
    #[default]
    All,
}

/// Transport server to run
#[derive(Debug, Clone, Copy, ValueEnum, Default)]
pub enum Transport {
    /// WebSocket server using the existing JSON API
    #[default]
    Websocket,
    /// gRPC server using typed protobuf payloads
    Grpc,
    /// WebSocket and gRPC servers sharing one order-book runtime
    Both,
}

#[derive(Debug, Parser)]
#[command(author, version, about = "Real-time Orderbook Server for Hyperliquid")]
struct Args {
    /// Server address (e.g., 0.0.0.0)
    #[arg(long, default_value = "0.0.0.0")]
    address: Ipv4Addr,

    /// Server port (e.g., 8000)
    #[arg(long, default_value = "8000")]
    port: u16,

    /// WebSocket port. Defaults to --port.
    #[arg(long)]
    ws_port: Option<u16>,

    /// gRPC port. Defaults to --port for grpc mode and --port + 1 for both mode.
    #[arg(long)]
    grpc_port: Option<u16>,

    /// Transport server to run: websocket, grpc, or both
    #[arg(long, value_enum, default_value = "websocket")]
    transport: Transport,

    /// Compression level for WebSocket connections (0-9).
    /// 0 = disabled, 1 = fastest (default), 9 = best ratio
    #[arg(long, default_value = "1")]
    compression_level: u32,

    /// Base directory for hlnode data files.
    /// For Docker: the directory containing .hyperliquid_rpc_hlnode_mainnet/
    /// For Direct: the directory containing hl/hyperliquid_data/
    #[arg(long)]
    data_dir: Option<PathBuf>,

    /// Which markets to include: perps, spot, hip3, all
    #[arg(long, value_enum, default_value = "all")]
    markets: Markets,

    // ========== Snapshot Configuration ==========
    /// Snapshot fetching mode: docker or direct
    /// - docker: Use 'docker exec <container> hl-node ...' (for Docker users)
    /// - direct: Call 'hl-node ...' directly (for systemctl/bare metal users)
    #[arg(long, value_enum, default_value = "docker")]
    snapshot_mode: SnapshotMode,

    /// Docker container name (only used in docker mode)
    #[arg(long, default_value = "hyperliquid_hlnode")]
    docker_container: String,

    /// Path to hl-node binary (only used in direct mode).
    /// Default: 'hl-node' (assumes in PATH)
    #[arg(long, default_value = "hl-node")]
    hlnode_binary: String,

    /// Path to abci_state.rmp file (only used in direct mode).
    /// Default: <data_dir>/hl/hyperliquid_data/abci_state.rmp
    #[arg(long)]
    abci_state_path: Option<PathBuf>,

    /// Path where snapshot JSON will be written.
    /// Default: process-specific orderbook_snapshot_<pid>.json in the node data
    /// hyperliquid_data directory (docker mode) or system temp dir (direct mode).
    #[arg(long)]
    snapshot_output_path: Option<PathBuf>,

    /// Path to visor_abci_state.json (optional, for height info).
    /// Default: <data_dir>/.hyperliquid_rpc_hlnode_mainnet/volumes/hl/hyperliquid_data/visor_abci_state.json
    #[arg(long)]
    visor_state_path: Option<PathBuf>,

    /// Port for Prometheus metrics endpoint (0 to disable)
    #[arg(long, default_value = "9090")]
    metrics_port: u16,

    /// BBO-only mode: lightweight mode that only tracks best bid/ask per coin.
    /// Reduces RAM from 2-3GB to ~100MB. Disables L2/L4/Trades subscriptions.
    #[arg(long, default_value = "false")]
    bbo_only: bool,

    /// Resend the last l2Book snapshot for each active subscription every N ms
    /// when nothing has changed. Off by default (0 = disabled). Matches the
    /// official Hyperliquid API behavior of pushing a heartbeat snapshot per block
    /// so downstream clients with stall timers don't disconnect on quiet coins.
    #[arg(long, default_value = "0")]
    l2book_heartbeat_ms: u64,

    /// Resend the last bbo payload for each active subscription every N ms
    /// when nothing has changed. Off by default (0 = disabled).
    #[arg(long, default_value = "0")]
    bbo_heartbeat_ms: u64,

    /// Log level: error, warn, info, debug, trace
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Tolerate drift instead of re-syncing. When set, data-loss events are still
    /// counted in metrics (orderbook_desyncs_total) but never trigger a snapshot
    /// re-fetch. The book keeps serving live events through drift and does NOT
    /// self-heal until restarted. Use only when a non-converging re-sync loop is
    /// worse than a knowingly-incomplete book.
    #[arg(long, default_value = "false")]
    no_resync: bool,
}

/// Start the Prometheus metrics HTTP server
async fn start_metrics_server(port: u16) {
    use axum::{Router, response::IntoResponse, routing::get};

    async fn metrics_handler() -> impl IntoResponse {
        server::metrics::gather_metrics()
    }

    let app = Router::new().route("/metrics", get(metrics_handler));
    let addr = format!("0.0.0.0:{}", port);

    log::info!("Metrics server listening on http://{}/metrics", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await.expect("failed to bind metrics port");
    axum::serve(listener, app).await.expect("metrics server failed");
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Initialize logger with specified level
    // SAFETY: We're setting this before any threads are spawned
    #[allow(unsafe_code)]
    unsafe {
        std::env::set_var("RUST_LOG", &args.log_level);
    }
    env_logger::init();

    // Register Prometheus metrics
    server::metrics::register_metrics();

    let full_address = format!("{}:{}", args.address, selected_primary_port(&args)?);

    // Determine market flags from Markets enum
    let (include_perps, include_spot, include_hip3) = match args.markets {
        Markets::Perps => (true, false, false),
        Markets::Spot => (false, true, false),
        Markets::Hip3 => (false, false, true),
        Markets::All => (true, true, true),
    };

    // Build config
    let config = ServerConfig {
        address: full_address.clone(),
        compression_level: args.compression_level,
        data_dir: args.data_dir.clone(),
        include_perps,
        include_spot,
        include_hip3,
        snapshot_mode: args.snapshot_mode,
        docker_container: args.docker_container.clone(),
        hlnode_binary: args.hlnode_binary.clone(),
        abci_state_path: args.abci_state_path.clone(),
        snapshot_output_path: args.snapshot_output_path.clone(),
        visor_state_path: args.visor_state_path.clone(),
        metrics_port: args.metrics_port,
        bbo_only: args.bbo_only,
        l2book_heartbeat_ms: args.l2book_heartbeat_ms,
        bbo_heartbeat_ms: args.bbo_heartbeat_ms,
        no_resync: args.no_resync,
    };

    println!("Orderbook Server v{}", env!("CARGO_PKG_VERSION"));
    println!("  Transport: {:?}", args.transport);
    match args.transport {
        Transport::Websocket => println!("  WebSocket: ws://{}:{}/ws", args.address, selected_ws_port(&args)),
        Transport::Grpc => println!("  gRPC: http://{}:{}", args.address, selected_grpc_port(&args)?),
        Transport::Both => {
            println!("  WebSocket: ws://{}:{}/ws", args.address, selected_ws_port(&args));
            println!("  gRPC: http://{}:{}", args.address, selected_grpc_port(&args)?);
        }
    }
    println!("  Markets: {:?}", args.markets);
    if config.bbo_only {
        println!("  Mode: BBO-ONLY (lightweight, ~100MB RAM)");
        println!("  Note: L2/L4/Trades subscriptions disabled");
    }
    println!("  Snapshot mode: {:?}", config.snapshot_mode);
    match config.snapshot_mode {
        SnapshotMode::Docker => {
            println!("  Container: {}", config.docker_container);
        }
        SnapshotMode::Direct => {
            println!("  hl-node binary: {}", config.hlnode_binary);
            if let Some(ref path) = config.abci_state_path {
                println!("  abci_state: {}", path.display());
            }
            if let Some(ref path) = config.snapshot_output_path {
                println!("  snapshot output: {}", path.display());
            }
        }
    }
    if let Some(ref dir) = config.data_dir {
        println!("  Data dir: {}", dir.display());
    }
    if config.metrics_port > 0 {
        println!("  Metrics: http://0.0.0.0:{}/metrics", config.metrics_port);
    }
    println!("  Log level: {}", args.log_level);
    if config.no_resync {
        println!("  Re-sync: DISABLED (--no-resync) — drift tolerated, book will NOT self-heal");
    }
    println!();

    // Spawn uptime counter
    tokio::spawn(async {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        loop {
            interval.tick().await;
            server::metrics::UPTIME_SECONDS.inc();
        }
    });

    // Start metrics server if port > 0
    if config.metrics_port > 0 {
        let metrics_port = config.metrics_port;
        tokio::spawn(async move {
            start_metrics_server(metrics_port).await;
        });
    }

    tokio::select! {
        result = run_selected_transport(args.transport, config, args.address, args.port, args.ws_port, args.grpc_port) => {
            if let Err(e) = result {
                log::error!("Server error: {e}");
            }
        }
        _ = tokio::signal::ctrl_c() => {
            log::info!("Shutdown signal received, exiting gracefully");
        }
    }

    Ok(())
}

async fn run_selected_transport(
    transport: Transport,
    config: ServerConfig,
    address: Ipv4Addr,
    port: u16,
    ws_port: Option<u16>,
    grpc_port: Option<u16>,
) -> Result<()> {
    match transport {
        Transport::Websocket => {
            let config = config.with_address(format!("{}:{}", address, ws_port.unwrap_or(port)));
            run_websocket_server(config).await
        }
        Transport::Grpc => {
            let config = config.with_address(format!("{}:{}", address, grpc_port.unwrap_or(port)));
            run_grpc_transport_standalone(config).await
        }
        Transport::Both => {
            let ws_config = config.clone().with_address(format!("{}:{}", address, ws_port.unwrap_or(port)));
            let grpc_port = match grpc_port {
                Some(port) => port,
                None => port.checked_add(1).ok_or_else(|| "--transport both needs --grpc-port when --port is 65535")?,
            };
            if grpc_port == ws_port.unwrap_or(port) {
                return Err("WebSocket and gRPC ports must differ in --transport both mode".into());
            }
            let grpc_config = config.with_address(format!("{}:{}", address, grpc_port));
            run_both_transports(ws_config, grpc_config).await
        }
    }
}

#[cfg(feature = "grpc")]
async fn run_grpc_transport_standalone(config: ServerConfig) -> Result<()> {
    run_grpc_server(config).await
}

#[cfg(not(feature = "grpc"))]
async fn run_grpc_transport_standalone(_config: ServerConfig) -> Result<()> {
    Err("gRPC transport requested, but this binary was built without the `grpc` feature".into())
}

#[cfg(feature = "grpc")]
async fn run_both_transports(ws_config: ServerConfig, grpc_config: ServerConfig) -> Result<()> {
    let runtime = server::transport::OrderBookRuntime::spawn(&ws_config);
    tokio::select! {
        result = run_websocket_transport(ws_config, runtime.clone()) => result,
        result = run_grpc_transport(grpc_config, runtime) => result,
    }
}

#[cfg(not(feature = "grpc"))]
async fn run_both_transports(_ws_config: ServerConfig, _grpc_config: ServerConfig) -> Result<()> {
    Err("both transport requested, but this binary was built without the `grpc` feature".into())
}

fn selected_primary_port(args: &Args) -> Result<u16> {
    match args.transport {
        Transport::Websocket => Ok(selected_ws_port(args)),
        Transport::Grpc => selected_grpc_port(args),
        Transport::Both => Ok(selected_ws_port(args)),
    }
}

const fn selected_ws_port(args: &Args) -> u16 {
    match args.ws_port {
        Some(port) => port,
        None => args.port,
    }
}

fn selected_grpc_port(args: &Args) -> Result<u16> {
    if let Some(port) = args.grpc_port {
        return Ok(port);
    }
    if matches!(args.transport, Transport::Both) {
        return args
            .port
            .checked_add(1)
            .ok_or_else(|| "--transport both needs --grpc-port when --port is 65535".into());
    }
    Ok(args.port)
}

trait WithAddress {
    fn with_address(self, address: String) -> Self;
}

impl WithAddress for ServerConfig {
    fn with_address(mut self, address: String) -> Self {
        self.address = address;
        self
    }
}
