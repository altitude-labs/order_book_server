#![allow(unused_crate_dependencies)]

use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use clap::{Parser, ValueEnum};
use futures_util::{SinkExt, StreamExt};
use grpc::orderbook as pb;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_tungstenite::{connect_async, tungstenite::Message};

type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Channel {
    Bbo,
    L2Book,
    Trades,
    BookDiffs,
    OrderUpdates,
}

impl Channel {
    const fn ws_name(self) -> &'static str {
        match self {
            Self::Bbo => "bbo",
            Self::L2Book => "l2Book",
            Self::Trades => "trades",
            Self::BookDiffs => "bookDiffs",
            Self::OrderUpdates => "orderUpdates",
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Transport {
    Websocket,
    Grpc,
}

impl Transport {
    const fn label(self) -> &'static str {
        match self {
            Self::Websocket => "websocket",
            Self::Grpc => "grpc",
        }
    }
}

#[derive(Debug, Parser, Clone)]
#[command(about = "Compare client-observed latency for matching orderbook messages across two endpoints")]
struct Args {
    /// Endpoint A URL. WebSocket example: ws://host:port/ws. gRPC example: http://host:port
    #[arg(long)]
    endpoint_a: Option<String>,

    /// Endpoint A transport
    #[arg(long, value_enum, default_value = "websocket")]
    transport_a: Transport,

    /// Label for endpoint A in output
    #[arg(long, default_value = "a")]
    label_a: String,

    /// Endpoint B URL. WebSocket example: ws://host:port/ws. gRPC example: http://host:port
    #[arg(long)]
    endpoint_b: Option<String>,

    /// Endpoint B transport
    #[arg(long, value_enum, default_value = "grpc")]
    transport_b: Transport,

    /// Label for endpoint B in output
    #[arg(long, default_value = "b")]
    label_b: String,

    /// Legacy WebSocket endpoint, usually ws://host:port/ws. Equivalent to --endpoint-a with --transport-a websocket.
    #[arg(long)]
    ws: Option<String>,

    /// Legacy gRPC endpoint, usually http://host:port. Equivalent to --endpoint-b with --transport-b grpc.
    #[arg(long)]
    grpc: Option<String>,

    /// Channel to subscribe to on both transports
    #[arg(long, value_enum, default_value = "bbo")]
    channel: Channel,

    /// Coin for bbo/l2Book/trades/bookDiffs subscriptions
    #[arg(long)]
    coin: Option<String>,

    /// User address for orderUpdates subscriptions
    #[arg(long)]
    user: Option<String>,

    /// Optional l2Book nSigFigs
    #[arg(long)]
    n_sig_figs: Option<u32>,

    /// Optional l2Book mantissa
    #[arg(long)]
    mantissa: Option<u64>,

    /// Optional l2Book nLevels
    #[arg(long)]
    n_levels: Option<u64>,

    /// Report a message as missing on the other transport after this delay
    #[arg(long, default_value = "5000")]
    match_timeout_ms: u64,

    /// Stop after this many matched messages. 0 runs forever.
    #[arg(long, default_value = "100")]
    max_matches: usize,

    /// Emit aggregate latency metrics every N seconds
    #[arg(long, default_value = "10")]
    summary_interval_sec: u64,

    /// Only print summary and missing-match lines, not every matched message
    #[arg(long, default_value = "false")]
    quiet_matches: bool,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum EndpointSide {
    A,
    B,
}

impl EndpointSide {
    const fn other(self) -> Self {
        match self {
            Self::A => Self::B,
            Self::B => Self::A,
        }
    }
}

#[derive(Debug, Clone)]
struct EndpointConfig {
    side: EndpointSide,
    label: String,
    transport: Transport,
    url: String,
}

#[derive(Debug)]
struct Observed {
    source: EndpointSide,
    key: String,
    channel: &'static str,
    recv_us: u128,
    sample: Value,
}

#[derive(Debug)]
struct Pending {
    channel: &'static str,
    first_source: EndpointSide,
    first_recv_us: u128,
    first_seen_at: Instant,
    sample: Value,
}

#[derive(Debug)]
struct Stats {
    started_at: Instant,
    total_matches: u64,
    total_missing_a: u64,
    total_missing_b: u64,
    interval_matches: u64,
    interval_missing_a: u64,
    interval_missing_b: u64,
    interval_deltas_us: Vec<i64>,
    label_a: String,
    label_b: String,
}

const OUTLIER_THRESHOLD_US: i64 = 10_000;

impl Stats {
    fn new(started_at: Instant, label_a: String, label_b: String) -> Self {
        Self {
            started_at,
            total_matches: 0,
            total_missing_a: 0,
            total_missing_b: 0,
            interval_matches: 0,
            interval_missing_a: 0,
            interval_missing_b: 0,
            interval_deltas_us: Vec::new(),
            label_a,
            label_b,
        }
    }

    fn record_match(&mut self, b_minus_a_us: i64) {
        self.total_matches += 1;
        self.interval_matches += 1;
        self.interval_deltas_us.push(b_minus_a_us);
    }

    const fn record_missing(&mut self, missing: EndpointSide) {
        match missing {
            EndpointSide::A => {
                self.total_missing_a += 1;
                self.interval_missing_a += 1;
            }
            EndpointSide::B => {
                self.total_missing_b += 1;
                self.interval_missing_b += 1;
            }
        }
    }

    fn summary(&mut self) -> Value {
        self.interval_deltas_us.sort_unstable();
        let delta_count = self.interval_deltas_us.len();
        let avg = if delta_count == 0 {
            None
        } else {
            Some(self.interval_deltas_us.iter().sum::<i64>() as f64 / delta_count as f64)
        };
        let min = self.interval_deltas_us.first().copied();
        let p50 = percentile(&self.interval_deltas_us, 50);
        let p95 = percentile(&self.interval_deltas_us, 95);
        let p99 = percentile(&self.interval_deltas_us, 99);
        let max = self.interval_deltas_us.last().copied();
        let b_ahead_over_10ms = self.interval_deltas_us.iter().filter(|delta| **delta <= -OUTLIER_THRESHOLD_US).count();
        let a_ahead_over_10ms = self.interval_deltas_us.iter().filter(|delta| **delta >= OUTLIER_THRESHOLD_US).count();
        let largest_b_ahead = self.interval_deltas_us.first().copied().filter(|delta| *delta < 0).map(i64::abs);
        let largest_a_ahead = self.interval_deltas_us.last().copied().filter(|delta| *delta > 0);

        let summary = json!({
            "type": "summary",
            "uptime": format!("{:?}", self.started_at.elapsed()),
            "labels": {
                "a": &self.label_a,
                "b": &self.label_b,
            },
            "interval": {
                "matches": self.interval_matches,
                "missingA": self.interval_missing_a,
                "missingB": self.interval_missing_b,
                "bFaster": self.interval_deltas_us.iter().filter(|delta| **delta < 0).count(),
                "aFaster": self.interval_deltas_us.iter().filter(|delta| **delta > 0).count(),
                "ties": self.interval_deltas_us.iter().filter(|delta| **delta == 0).count(),
                "outliers": {
                    "threshold": format_signed_duration_us(OUTLIER_THRESHOLD_US),
                    "thresholdUs": OUTLIER_THRESHOLD_US,
                    "bAheadOverThreshold": b_ahead_over_10ms,
                    "aAheadOverThreshold": a_ahead_over_10ms,
                    "largestBAhead": largest_b_ahead.map(format_duration_us),
                    "largestBAheadUs": largest_b_ahead,
                    "largestAAhead": largest_a_ahead.map(format_duration_us),
                    "largestAAheadUs": largest_a_ahead,
                },
                "bMinusAUs": {
                    "avg": avg,
                    "min": min,
                    "p50": p50,
                    "p95": p95,
                    "p99": p99,
                    "max": max,
                },
                "bMinusA": {
                    "avg": avg.map(format_signed_duration_us_f64),
                    "min": min.map(format_signed_duration_us),
                    "p50": p50.map(format_signed_duration_us),
                    "p95": p95.map(format_signed_duration_us),
                    "p99": p99.map(format_signed_duration_us),
                    "max": max.map(format_signed_duration_us),
                },
            },
            "total": {
                "matches": self.total_matches,
                "missingA": self.total_missing_a,
                "missingB": self.total_missing_b,
            },
        });

        self.interval_matches = 0;
        self.interval_missing_a = 0;
        self.interval_missing_b = 0;
        self.interval_deltas_us.clear();

        summary
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    validate_args(&args)?;
    let [endpoint_a, endpoint_b] = endpoint_configs(&args)?;

    let (observed_tx, mut observed_rx) = mpsc::channel::<Observed>(8192);
    let start = Instant::now();

    spawn_listener(args.clone(), endpoint_a.clone(), observed_tx.clone(), start);
    spawn_listener(args.clone(), endpoint_b.clone(), observed_tx, start);

    let timeout = Duration::from_millis(args.match_timeout_ms);
    let mut pending: HashMap<String, Pending> = HashMap::new();
    let mut matched = 0usize;
    let mut cleanup = tokio::time::interval(Duration::from_millis((args.match_timeout_ms / 2).max(100)));
    let mut summary = tokio::time::interval(Duration::from_secs(args.summary_interval_sec.max(1)));
    let mut stats = Stats::new(start, endpoint_a.label.clone(), endpoint_b.label.clone());

    loop {
        tokio::select! {
            Some(observed) = observed_rx.recv() => {
                if let Some(previous) = pending.remove(&observed.key) {
                    if previous.first_source == observed.source {
                        pending.insert(observed.key, previous);
                        continue;
                    }

                    matched += 1;
                    let delta_us = observed.recv_us as i128 - previous.first_recv_us as i128;
                    let b_minus_a_us = clamp_i64(if observed.source == EndpointSide::B { delta_us } else { -delta_us });
                    stats.record_match(b_minus_a_us);
                    if !args.quiet_matches {
                        println!("{}", json!({
                            "type": "match",
                            "channel": observed.channel,
                            "key": observed.key,
                            "first": endpoint_label(previous.first_source, &endpoint_a, &endpoint_b),
                            "second": endpoint_label(observed.source, &endpoint_a, &endpoint_b),
                            "aRecvUs": if observed.source == EndpointSide::A { observed.recv_us } else { previous.first_recv_us },
                            "bRecvUs": if observed.source == EndpointSide::B { observed.recv_us } else { previous.first_recv_us },
                            "bMinusAUs": b_minus_a_us,
                            "bMinusA": format_signed_duration_us(b_minus_a_us),
                        }));
                    }

                    if args.max_matches > 0 && matched >= args.max_matches {
                        println!("{}", stats.summary());
                        return Ok(());
                    }
                } else {
                    pending.insert(observed.key, Pending {
                        channel: observed.channel,
                        first_source: observed.source,
                        first_recv_us: observed.recv_us,
                        first_seen_at: Instant::now(),
                        sample: observed.sample,
                    });
                }
            }
            _ = cleanup.tick() => {
                let now = Instant::now();
                let mut expired = Vec::new();
                pending.retain(|key, entry| {
                    if now.duration_since(entry.first_seen_at) >= timeout {
                        expired.push((key.clone(), entry.channel, entry.first_source, entry.first_recv_us, entry.sample.clone()));
                        false
                    } else {
                        true
                    }
                });
                for (key, channel, first_source, first_recv_us, sample) in expired {
                    let missing = first_source.other();
                    stats.record_missing(missing);
                    println!("{}", json!({
                        "type": "missing_match",
                        "channel": channel,
                        "key": key,
                        "seenOn": endpoint_label(first_source, &endpoint_a, &endpoint_b),
                        "missingOn": endpoint_label(missing, &endpoint_a, &endpoint_b),
                        "waitedMs": args.match_timeout_ms,
                        "firstRecvUs": first_recv_us,
                        "sample": sample,
                    }));
                }
            }
            _ = summary.tick() => {
                println!("{}", stats.summary());
            }
        }
    }
}

fn spawn_listener(args: Args, endpoint: EndpointConfig, observed_tx: mpsc::Sender<Observed>, start: Instant) {
    tokio::spawn(async move {
        let result = match endpoint.transport {
            Transport::Websocket => listen_websocket(&args, endpoint.clone(), observed_tx, start).await,
            Transport::Grpc => listen_grpc(&args, endpoint.clone(), observed_tx, start).await,
        };
        if let Err(err) = result {
            eprintln!("{} listener exited ({} {}): {err}", endpoint.label, endpoint.transport.label(), endpoint.url);
        }
    });
}

fn endpoint_label<'a>(side: EndpointSide, endpoint_a: &'a EndpointConfig, endpoint_b: &'a EndpointConfig) -> &'a str {
    match side {
        EndpointSide::A => &endpoint_a.label,
        EndpointSide::B => &endpoint_b.label,
    }
}

fn endpoint_configs(args: &Args) -> Result<[EndpointConfig; 2]> {
    match (&args.endpoint_a, &args.endpoint_b, &args.ws, &args.grpc) {
        (Some(endpoint_a), Some(endpoint_b), None, None) => Ok([
            EndpointConfig {
                side: EndpointSide::A,
                label: args.label_a.clone(),
                transport: args.transport_a,
                url: endpoint_a.clone(),
            },
            EndpointConfig {
                side: EndpointSide::B,
                label: args.label_b.clone(),
                transport: args.transport_b,
                url: endpoint_b.clone(),
            },
        ]),
        (None, None, Some(ws), Some(grpc)) => Ok([
            EndpointConfig {
                side: EndpointSide::A,
                label: "websocket".to_string(),
                transport: Transport::Websocket,
                url: ws.clone(),
            },
            EndpointConfig {
                side: EndpointSide::B,
                label: "grpc".to_string(),
                transport: Transport::Grpc,
                url: grpc.clone(),
            },
        ]),
        _ => Err("use either --endpoint-a/--endpoint-b or legacy --ws/--grpc, but do not mix them".into()),
    }
}

fn validate_args(args: &Args) -> Result<()> {
    let using_new = args.endpoint_a.is_some() || args.endpoint_b.is_some();
    let using_legacy = args.ws.is_some() || args.grpc.is_some();
    if using_new && using_legacy {
        return Err("use either --endpoint-a/--endpoint-b or legacy --ws/--grpc, not both".into());
    }
    if using_new && (args.endpoint_a.is_none() || args.endpoint_b.is_none()) {
        return Err("--endpoint-a and --endpoint-b are both required".into());
    }
    if using_legacy && (args.ws.is_none() || args.grpc.is_none()) {
        return Err("--ws and --grpc are both required in legacy mode".into());
    }
    if !using_new && !using_legacy {
        return Err("--endpoint-a and --endpoint-b are required".into());
    }

    match args.channel {
        Channel::Bbo | Channel::L2Book | Channel::Trades | Channel::BookDiffs => {
            if args.coin.is_none() {
                return Err(format!("--coin is required for {}", args.channel.ws_name()).into());
            }
        }
        Channel::OrderUpdates => {
            if args.user.is_none() {
                return Err("--user is required for orderUpdates".into());
            }
        }
    }
    Ok(())
}

async fn listen_websocket(
    args: &Args,
    endpoint: EndpointConfig,
    observed_tx: mpsc::Sender<Observed>,
    start: Instant,
) -> Result<()> {
    let (mut socket, _) = connect_async(&endpoint.url).await?;
    socket.send(Message::Text(ws_subscribe_message(args)?.to_string().into())).await?;
    eprintln!("{} connected: {} {}", endpoint.label, endpoint.transport.label(), endpoint.url);

    while let Some(message) = socket.next().await {
        let message = message?;
        let text = match message {
            Message::Text(text) => text.to_string(),
            Message::Binary(bytes) => String::from_utf8(bytes.to_vec())?,
            Message::Ping(_) | Message::Pong(_) => continue,
            Message::Close(frame) => return Err(format!("websocket closed: {frame:?}").into()),
            Message::Frame(_) => continue,
        };

        let value: Value = serde_json::from_str(&text)?;
        for observed in observations_from_ws(endpoint.side, &value, start.elapsed().as_micros()) {
            if observed_tx.send(observed).await.is_err() {
                return Ok(());
            }
        }
    }

    Ok(())
}

async fn listen_grpc(
    args: &Args,
    endpoint: EndpointConfig,
    observed_tx: mpsc::Sender<Observed>,
    start: Instant,
) -> Result<()> {
    let mut client = pb::orderbook_client::OrderbookClient::connect(endpoint.url.clone()).await?;
    let (request_tx, request_rx) = mpsc::channel(8);
    request_tx.send(grpc_subscribe_message(args)?).await?;

    let mut stream = client.stream(ReceiverStream::new(request_rx)).await?.into_inner();
    eprintln!("{} connected: {} {}", endpoint.label, endpoint.transport.label(), endpoint.url);

    while let Some(message) = stream.message().await? {
        for observed in observations_from_grpc(endpoint.side, &message, start.elapsed().as_micros()) {
            if observed_tx.send(observed).await.is_err() {
                return Ok(());
            }
        }
    }

    Ok(())
}

fn ws_subscribe_message(args: &Args) -> Result<Value> {
    let subscription = match args.channel {
        Channel::Bbo | Channel::Trades | Channel::BookDiffs => {
            json!({"type": args.channel.ws_name(), "coin": required_coin(args)?})
        }
        Channel::L2Book => json!({
            "type": "l2Book",
            "coin": required_coin(args)?,
            "nSigFigs": args.n_sig_figs,
            "mantissa": args.mantissa,
            "nLevels": args.n_levels,
        }),
        Channel::OrderUpdates => json!({"type": "orderUpdates", "user": required_user(args)?}),
    };

    Ok(json!({
        "method": "subscribe",
        "subscription": subscription,
    }))
}

fn grpc_subscribe_message(args: &Args) -> Result<pb::ClientMessage> {
    let subscription = match args.channel {
        Channel::Bbo => pb::subscription::Subscription::Bbo(pb::CoinSubscription { coin: required_coin(args)? }),
        Channel::Trades => pb::subscription::Subscription::Trades(pb::CoinSubscription { coin: required_coin(args)? }),
        Channel::BookDiffs => {
            pb::subscription::Subscription::BookDiffs(pb::CoinSubscription { coin: required_coin(args)? })
        }
        Channel::L2Book => pb::subscription::Subscription::L2Book(pb::L2BookSubscription {
            coin: required_coin(args)?,
            n_sig_figs: args.n_sig_figs,
            mantissa: args.mantissa,
            n_levels: args.n_levels,
        }),
        Channel::OrderUpdates => {
            pb::subscription::Subscription::OrderUpdates(pb::UserSubscription { user: required_user(args)? })
        }
    };

    Ok(pb::ClientMessage {
        message: Some(pb::client_message::Message::Subscribe(pb::Subscription { subscription: Some(subscription) })),
    })
}

fn required_coin(args: &Args) -> Result<String> {
    args.coin.clone().ok_or_else(|| "--coin is required".into())
}

fn required_user(args: &Args) -> Result<String> {
    args.user.clone().ok_or_else(|| "--user is required".into())
}

fn observations_from_ws(source: EndpointSide, message: &Value, recv_us: u128) -> Vec<Observed> {
    let Some(channel) = message.get("channel").and_then(Value::as_str) else {
        return Vec::new();
    };
    let data = &message["data"];

    match channel {
        "bbo" => observed_one(source, "bbo", bbo_key_json(data), recv_us, data.clone()),
        "l2Book" => observed_one(source, "l2Book", l2_key_json(data), recv_us, data.clone()),
        "trades" => observed_many_json(source, "trades", data, trade_key_json, recv_us),
        "bookDiffs" => observed_many_json(source, "bookDiffs", data, book_diff_key_json, recv_us),
        "orderUpdates" => observed_many_json(source, "orderUpdates", data, order_update_key_json, recv_us),
        _ => Vec::new(),
    }
}

fn observations_from_grpc(source: EndpointSide, message: &pb::ServerMessage, recv_us: u128) -> Vec<Observed> {
    let Some(message) = &message.message else {
        return Vec::new();
    };

    match message {
        pb::server_message::Message::Bbo(bbo) => {
            observed_one(source, "bbo", bbo_key_proto(bbo), recv_us, bbo_sample_proto(bbo))
        }
        pb::server_message::Message::L2Book(book) => {
            observed_one(source, "l2Book", l2_key_proto(book), recv_us, l2_sample_proto(book))
        }
        pb::server_message::Message::Trades(trades) => trades
            .trades
            .iter()
            .filter_map(|trade| {
                Some(Observed {
                    source,
                    key: trade_key_proto(trade)?,
                    channel: "trades",
                    recv_us,
                    sample: trade_sample_proto(trade),
                })
            })
            .collect(),
        pb::server_message::Message::BookDiffs(diffs) => diffs
            .book_diffs
            .iter()
            .filter_map(|diff| {
                Some(Observed {
                    source,
                    key: book_diff_key_proto(diff)?,
                    channel: "bookDiffs",
                    recv_us,
                    sample: book_diff_sample_proto(diff),
                })
            })
            .collect(),
        pb::server_message::Message::OrderUpdates(updates) => updates
            .order_updates
            .iter()
            .filter_map(|update| {
                Some(Observed {
                    source,
                    key: order_update_key_proto(update)?,
                    channel: "orderUpdates",
                    recv_us,
                    sample: order_update_sample_proto(update),
                })
            })
            .collect(),
        pb::server_message::Message::Error(err) => {
            eprintln!("grpc server error: {}", err.message);
            Vec::new()
        }
        _ => Vec::new(),
    }
}

fn observed_one(
    source: EndpointSide,
    channel: &'static str,
    key: Option<String>,
    recv_us: u128,
    sample: Value,
) -> Vec<Observed> {
    key.map_or_else(Vec::new, |key| vec![Observed { source, key, channel, recv_us, sample }])
}

fn observed_many_json(
    source: EndpointSide,
    channel: &'static str,
    data: &Value,
    key_fn: fn(&Value) -> Option<String>,
    recv_us: u128,
) -> Vec<Observed> {
    let Some(items) = data.as_array() else {
        return Vec::new();
    };

    items
        .iter()
        .filter_map(|item| Some(Observed { source, key: key_fn(item)?, channel, recv_us, sample: item.clone() }))
        .collect()
}

fn bbo_key_json(value: &Value) -> Option<String> {
    Some(format!(
        "bbo|{}|{}|{}|{}",
        str_field(value, "coin")?,
        scalar(value.get("time")?),
        level_key_json(value.get("bbo").and_then(|bbo| bbo.get(0)).or_else(|| value.get("bid"))),
        level_key_json(value.get("bbo").and_then(|bbo| bbo.get(1)).or_else(|| value.get("ask"))),
    ))
}

fn bbo_key_proto(bbo: &pb::Bbo) -> Option<String> {
    Some(format!(
        "bbo|{}|{}|{}|{}",
        bbo.coin,
        bbo.time,
        level_key_proto(bbo.bid.as_ref()),
        level_key_proto(bbo.ask.as_ref()),
    ))
}

fn l2_key_json(value: &Value) -> Option<String> {
    Some(format!(
        "l2Book|{}|{}|{}|{}",
        str_field(value, "coin")?,
        scalar(value.get("time")?),
        levels_edge_key_json(value.get("levels").and_then(|levels| levels.get(0))),
        levels_edge_key_json(value.get("levels").and_then(|levels| levels.get(1))),
    ))
}

fn l2_key_proto(book: &pb::L2Book) -> Option<String> {
    Some(format!(
        "l2Book|{}|{}|{}|{}",
        book.coin,
        book.time,
        levels_edge_key_proto(&book.bids),
        levels_edge_key_proto(&book.asks),
    ))
}

fn trade_key_json(value: &Value) -> Option<String> {
    Some(format!(
        "trade|{}|{}|{}|{}|{}|{}|{}",
        str_field(value, "coin")?,
        scalar(value.get("tid")?),
        str_field(value, "hash")?,
        scalar(value.get("time")?),
        str_field(value, "side")?,
        str_field(value, "px")?,
        str_field(value, "sz")?,
    ))
}

fn trade_key_proto(trade: &pb::Trade) -> Option<String> {
    Some(format!(
        "trade|{}|{}|{}|{}|{}|{}|{}",
        trade.coin,
        trade.tid,
        trade.hash,
        trade.time,
        side_label(trade.side),
        trade.px,
        trade.sz,
    ))
}

fn book_diff_key_json(value: &Value) -> Option<String> {
    Some(format!(
        "bookDiff|{}|{}|{}|{}|{}",
        lower_str(value, "user")?,
        scalar(value.get("oid")?),
        str_field(value, "coin")?,
        str_field(value, "px")?,
        canonical(&value["rawBookDiff"]),
    ))
}

fn book_diff_key_proto(diff: &pb::OrderDiffEvent) -> Option<String> {
    Some(format!(
        "bookDiff|{}|{}|{}|{}|{}",
        diff.user.to_lowercase(),
        diff.oid,
        diff.coin,
        diff.px,
        order_diff_key_proto(diff.raw_book_diff.as_ref()?),
    ))
}

fn order_update_key_json(value: &Value) -> Option<String> {
    let status = &value["orderStatus"];
    let order = &status["order"];
    let hash_part = status.get("hash").and_then(Value::as_str).map_or_else(
        || format!("time:{}", status.get("time").map_or_else(String::new, scalar)),
        |hash| format!("hash:{hash}"),
    );

    Some(format!(
        "orderUpdate|{}|{}|{}|{}|{}|{}|{}|{}|{}",
        lower_str(status, "status")?,
        lower_str(status, "user").or_else(|| lower_str(value, "user"))?,
        str_field(order, "coin")?,
        scalar(order.get("oid")?),
        hash_part,
        str_field(order, "side")?,
        str_field(order, "limitPx")?,
        str_field(order, "sz")?,
        scalar(order.get("timestamp")?),
    ))
}

fn order_update_key_proto(update: &pb::OrderUpdate) -> Option<String> {
    let status = update.order_status.as_ref()?;
    let order = status.order.as_ref()?;
    let hash_part = status.hash.as_ref().map_or_else(|| format!("time:{}", status.time), |hash| format!("hash:{hash}"));

    Some(format!(
        "orderUpdate|{}|{}|{}|{}|{}|{}|{}|{}|{}",
        status.status.to_lowercase(),
        status.user.to_lowercase(),
        order.coin,
        order.oid,
        hash_part,
        side_label(order.side),
        order.limit_px,
        order.sz,
        order.timestamp,
    ))
}

fn level_key_json(level: Option<&Value>) -> String {
    let Some(level) = level else {
        return "none".to_string();
    };
    format!(
        "{}:{}:{}",
        str_field(level, "px").unwrap_or_default(),
        str_field(level, "sz").unwrap_or_default(),
        level.get("n").map_or_else(String::new, scalar),
    )
}

fn level_key_proto(level: Option<&pb::Level>) -> String {
    level.map_or_else(|| "none".to_string(), |level| format!("{}:{}:{}", level.px, level.sz, level.n))
}

fn levels_edge_key_json(levels: Option<&Value>) -> String {
    let Some(levels) = levels.and_then(Value::as_array) else {
        return "none".to_string();
    };
    format!("len={}|first={}|last={}", levels.len(), level_key_json(levels.first()), level_key_json(levels.last()),)
}

fn levels_edge_key_proto(levels: &[pb::Level]) -> String {
    format!("len={}|first={}|last={}", levels.len(), level_key_proto(levels.first()), level_key_proto(levels.last()),)
}

fn order_diff_key_proto(diff: &pb::OrderDiff) -> String {
    match &diff.diff {
        Some(pb::order_diff::Diff::New(new)) => format!("new:{}", new.sz),
        Some(pb::order_diff::Diff::Update(update)) => format!("update:{}:{}", update.orig_sz, update.new_sz),
        Some(pb::order_diff::Diff::Remove(_)) => "remove".to_string(),
        None => "none".to_string(),
    }
}

fn str_field<'a>(value: &'a Value, field: &str) -> Option<&'a str> {
    value.get(field).and_then(Value::as_str)
}

fn lower_str(value: &Value, field: &str) -> Option<String> {
    Some(str_field(value, field)?.to_lowercase())
}

fn scalar(value: &Value) -> String {
    value.as_str().map_or_else(|| value.to_string(), ToString::to_string)
}

fn canonical(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "null".to_string())
}

fn side_label(side: i32) -> &'static str {
    match pb::Side::try_from(side).unwrap_or(pb::Side::Unspecified) {
        pb::Side::Ask => "A",
        pb::Side::Bid => "B",
        pb::Side::Unspecified => "",
    }
}

fn percentile(sorted: &[i64], percentile: usize) -> Option<i64> {
    if sorted.is_empty() {
        return None;
    }
    let index = ((sorted.len() - 1) * percentile) / 100;
    sorted.get(index).copied()
}

fn clamp_i64(value: i128) -> i64 {
    value.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
}

fn format_signed_duration_us(value: i64) -> String {
    format_signed_duration_us_f64(value as f64)
}

fn format_duration_us(value: i64) -> String {
    format_duration_us_f64(value as f64)
}

fn format_signed_duration_us_f64(value: f64) -> String {
    let sign = if value < 0.0 { "-" } else { "+" };
    format!("{sign}{}", format_duration_us_f64(value.abs()))
}

fn format_duration_us_f64(value: f64) -> String {
    let abs = value.abs();
    if abs < 1_000.0 {
        format!("{abs:.0}us")
    } else if abs < 1_000_000.0 {
        format!("{:.3}ms", abs / 1_000.0)
    } else {
        format!("{:.3}s", abs / 1_000_000.0)
    }
}

fn bbo_sample_proto(bbo: &pb::Bbo) -> Value {
    json!({
        "coin": bbo.coin,
        "time": bbo.time,
        "bid": bbo.bid.as_ref().map(level_sample_proto),
        "ask": bbo.ask.as_ref().map(level_sample_proto),
    })
}

fn l2_sample_proto(book: &pb::L2Book) -> Value {
    json!({
        "coin": book.coin,
        "time": book.time,
        "bids": book.bids.iter().take(3).map(level_sample_proto).collect::<Vec<_>>(),
        "asks": book.asks.iter().take(3).map(level_sample_proto).collect::<Vec<_>>(),
    })
}

fn trade_sample_proto(trade: &pb::Trade) -> Value {
    json!({
        "coin": trade.coin,
        "side": side_label(trade.side),
        "px": trade.px,
        "sz": trade.sz,
        "hash": trade.hash,
        "time": trade.time,
        "tid": trade.tid,
    })
}

fn book_diff_sample_proto(diff: &pb::OrderDiffEvent) -> Value {
    json!({
        "user": diff.user,
        "oid": diff.oid,
        "px": diff.px,
        "coin": diff.coin,
        "rawBookDiff": diff.raw_book_diff.as_ref().map(order_diff_key_proto),
    })
}

fn order_update_sample_proto(update: &pb::OrderUpdate) -> Value {
    let status = update.order_status.as_ref();
    json!({
        "user": update.user,
        "time": update.time,
        "height": update.height,
        "status": status.map(|status| status.status.as_str()),
        "oid": status.and_then(|status| status.order.as_ref()).map(|order| order.oid),
        "coin": status.and_then(|status| status.order.as_ref()).map(|order| order.coin.as_str()),
    })
}

fn level_sample_proto(level: &pb::Level) -> Value {
    json!({
        "px": level.px,
        "sz": level.sz,
        "n": level.n,
    })
}
