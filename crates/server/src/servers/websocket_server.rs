use crate::{
    listeners::order_book::{
        CoinBbo, InternalMessage, L2FrameCache, L2ParamGuard, L2SnapshotParams, OrderBookListener,
    },
    metrics::{
        BBO_CHANGES_TOTAL, BROADCAST_RECEIVERS, BROADCASTS_TOTAL, CHANNEL_DROPS_TOTAL, CHANNEL_LAG,
        LISTENER_LOCK_HOLD_LATENCY, LISTENER_LOCK_WAIT_LATENCY, MESSAGES_SENT_TOTAL, ORDERBOOK_HEIGHT,
        PayloadTimestampKind, TRANSPORT_CHANNEL_MESSAGES_SENT_TOTAL, TRANSPORT_CONNECTIONS_ACTIVE,
        TRANSPORT_CONNECTIONS_TOTAL, TRANSPORT_FANOUT_ACTIVE_SUBSCRIPTIONS, TRANSPORT_FANOUT_LATENCY,
        TRANSPORT_FANOUT_SUBSCRIPTIONS, TRANSPORT_MESSAGES_SENT_TOTAL, TRANSPORT_MESSAGES_SKIPPED_TOTAL,
        TRANSPORT_SEND_ERRORS_TOTAL, TRANSPORT_SEND_LATENCY, WS_CONNECTIONS_ACTIVE, WS_CONNECTIONS_TOTAL,
        WS_SEND_ERRORS_TOTAL, observe_transport_event_egress_age, observe_transport_payload_egress_age,
    },
    order_book::{Coin, Snapshot},
    prelude::*,
    transport::{
        ActiveL2Params, ActiveSubscriptionInterests, OrderBookRuntime, SubscriptionInterest, SubscriptionInterestGuard,
    },
    types::{
        Bbo, L2Book, L4Book, L4BookUpdates, L4Order, Trade,
        inner::InnerLevel,
        subscription::{
            ClientMessage, DEFAULT_LEVELS, OrderUpdate, ServerResponse, Subscription, SubscriptionKind,
            SubscriptionManager, TransportKind,
        },
    },
};
use axum::{Router, routing::get};
use futures_util::{SinkExt, StreamExt};
use log::{error, info};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::select;
use tokio::{net::TcpListener, sync::Mutex, sync::broadcast::Sender};
use yawc::{FrameView, OpCode, WebSocket};

use crate::ServerConfig;

/// Per-(coin, params) cached L2 broadcast. `hash` is used for change-based dedup;
/// `payload` is resent verbatim (with refreshed `time`) when the heartbeat fires,
/// and is only stored when the L2 heartbeat is enabled (default off) - the
/// change-driven sends use the broadcast's shared frames instead.
struct L2Entry {
    hash: u64,
    last_sent: Instant,
    payload: Option<L2Book>,
}

/// Raw fixed-point (px, sz) pairs for the best bid and ask. Comparing these
/// for dedup avoids the four String allocations the old tuple cost per BBO
/// per connection per change-check.
type BboKey = (Option<(u64, u64)>, Option<(u64, u64)>);

/// Per-coin cached BBO broadcast. `tuple` is used for change-based dedup;
/// `payload` is resent verbatim (with refreshed `time`) when the heartbeat fires,
/// and is only stored when the BBO heartbeat is enabled (default off) - the
/// change-driven sends use the broadcast's shared frames instead.
struct BboEntry {
    tuple: BboKey,
    last_sent: Instant,
    payload: Option<Bbo>,
}

impl BboEntry {
    fn upsert(cache: &mut HashMap<String, Self>, coin: &str, tuple: BboKey, payload: Option<Bbo>) {
        let entry = Self { tuple, last_sent: Instant::now(), payload };
        if let Some(slot) = cache.get_mut(coin) {
            *slot = entry;
        } else {
            cache.insert(coin.to_string(), entry);
        }
    }
}

/// Build a tokio interval that fires often enough to drive both heartbeats with
/// at most half the configured period of drift. Returns None when both heartbeats are disabled.
fn build_heartbeat_ticker(l2book_heartbeat_ms: u64, bbo_heartbeat_ms: u64) -> Option<tokio::time::Interval> {
    let enabled = [l2book_heartbeat_ms, bbo_heartbeat_ms].into_iter().filter(|&ms| ms > 0).min()?;
    let tick_ms = (enabled / 2).max(50).min(500);
    let mut interval = tokio::time::interval(Duration::from_millis(tick_ms));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    Some(interval)
}

/// Await the next heartbeat tick, or pend forever when no heartbeat is configured.
async fn heartbeat_tick(ticker: &mut Option<tokio::time::Interval>) {
    match ticker {
        Some(t) => {
            t.tick().await;
        }
        None => std::future::pending::<()>().await,
    }
}

pub async fn run_websocket_server(config: ServerConfig) -> Result<()> {
    let runtime = OrderBookRuntime::spawn(&config);
    run_websocket_transport(config, runtime).await
}

pub async fn run_websocket_transport(config: ServerConfig, runtime: OrderBookRuntime) -> Result<()> {
    let internal_message_tx = runtime.internal_message_tx();
    let listener = runtime.listener();
    let compression_level = config.compression_level;

    let websocket_opts =
        yawc::Options::default().with_compression_level(yawc::CompressionLevel::new(compression_level));

    let start_time = runtime.start_time();
    let listener_for_health = listener.clone();

    let app: Router = Router::new()
        .route(
            "/ws",
            get({
                let internal_message_tx = internal_message_tx.clone();
                let bbo_only = runtime.bbo_only();
                let l2book_heartbeat_ms = runtime.l2book_heartbeat_ms();
                let bbo_heartbeat_ms = runtime.bbo_heartbeat_ms();
                let listener = listener.clone();
                move |ws_upgrade| async move {
                    ws_handler(
                        ws_upgrade,
                        internal_message_tx.clone(),
                        listener.clone(),
                        bbo_only,
                        l2book_heartbeat_ms,
                        bbo_heartbeat_ms,
                        websocket_opts,
                    )
                }
            }),
        )
        .route(
            "/health",
            get(move || {
                let listener = listener_for_health.clone();
                async move {
                    let wait_start = Instant::now();
                    let guard = listener.lock().await;
                    LISTENER_LOCK_WAIT_LATENCY
                        .with_label_values(&["websocket_health"])
                        .observe(wait_start.elapsed().as_secs_f64());
                    let hold_start = Instant::now();
                    let is_ready = guard.is_ready();
                    LISTENER_LOCK_HOLD_LATENCY
                        .with_label_values(&["websocket_health"])
                        .observe(hold_start.elapsed().as_secs_f64());
                    let uptime_secs = start_time.elapsed().as_secs();
                    let height = ORDERBOOK_HEIGHT.get();
                    let connections = WS_CONNECTIONS_ACTIVE.get();
                    let body = format!(
                        r#"{{"status":"{}","uptime_seconds":{},"height":{},"connections":{}}}"#,
                        if is_ready { "ready" } else { "initializing" },
                        uptime_secs,
                        height,
                        connections,
                    );
                    axum::response::Response::builder().header("content-type", "application/json").body(body).unwrap()
                }
            }),
        );

    let tcp_listener = TcpListener::bind(&config.address).await?;
    info!("WebSocket server running at ws://{}", config.address);

    axum::serve(NoDelayListener(tcp_listener), app).await?;
    Ok(())
}

/// `TcpListener` wrapper that sets `TCP_NODELAY` on every accepted socket.
/// Without it, Nagle's algorithm can delay small frames (BBO updates are a few
/// hundred bytes) by up to an RTT while an unacked segment is outstanding.
struct NoDelayListener(TcpListener);

impl axum::serve::Listener for NoDelayListener {
    type Io = tokio::net::TcpStream;
    type Addr = std::net::SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        // Delegate to TcpListener's impl (it retries transient accept errors).
        let (stream, addr) = axum::serve::Listener::accept(&mut self.0).await;
        if let Err(err) = stream.set_nodelay(true) {
            log::warn!("failed to set TCP_NODELAY on {addr}: {err}");
        }
        (stream, addr)
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        self.0.local_addr()
    }
}

#[allow(clippy::too_many_arguments)]
fn ws_handler(
    incoming: yawc::IncomingUpgrade,
    internal_message_tx: Sender<Arc<InternalMessage>>,
    listener: Arc<Mutex<OrderBookListener>>,
    bbo_only: bool,
    l2book_heartbeat_ms: u64,
    bbo_heartbeat_ms: u64,
    websocket_opts: yawc::Options,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    // Reject malformed WS handshakes cleanly. The previous `.unwrap()` would panic
    // inside the axum handler task and dump a backtrace per request.
    let (resp, fut) = match incoming.upgrade(websocket_opts) {
        Ok(pair) => pair,
        Err(err) => {
            log::warn!("rejecting malformed websocket upgrade: {err}");
            return (axum::http::StatusCode::BAD_REQUEST, "invalid websocket upgrade").into_response();
        }
    };
    tokio::spawn(async move {
        let ws = match fut.await {
            Ok(ok) => ok,
            Err(err) => {
                log::error!("failed to upgrade websocket connection: {err}");
                return;
            }
        };

        handle_socket(ws, internal_message_tx, listener, bbo_only, l2book_heartbeat_ms, bbo_heartbeat_ms).await;
    });

    resp.into_response()
}

#[allow(clippy::too_many_arguments)]
async fn handle_socket(
    mut socket: WebSocket,
    internal_message_tx: Sender<Arc<InternalMessage>>,
    listener: Arc<Mutex<OrderBookListener>>,
    bbo_only: bool,
    l2book_heartbeat_ms: u64,
    bbo_heartbeat_ms: u64,
) {
    // Track connection metrics
    WS_CONNECTIONS_ACTIVE.inc();
    WS_CONNECTIONS_TOTAL.inc();
    TRANSPORT_CONNECTIONS_ACTIVE.with_label_values(&[TransportKind::Websocket.label()]).inc();
    TRANSPORT_CONNECTIONS_TOTAL.with_label_values(&[TransportKind::Websocket.label()]).inc();

    // Use a guard to decrement active connections when this function exits
    struct ConnectionGuard;
    impl Drop for ConnectionGuard {
        fn drop(&mut self) {
            WS_CONNECTIONS_ACTIVE.dec();
            TRANSPORT_CONNECTIONS_ACTIVE.with_label_values(&[TransportKind::Websocket.label()]).dec();
            BROADCAST_RECEIVERS.dec();
        }
    }
    let _connection_guard = ConnectionGuard;

    let mut internal_message_rx = internal_message_tx.subscribe();
    BROADCAST_RECEIVERS.set(internal_message_tx.receiver_count() as i64);
    let mut manager = SubscriptionManager::new(TransportKind::Websocket);
    // Market-filtered universe for subscription validation. Refreshed from
    // Snapshot broadcasts (Arc-shared, built once in the listener) whenever the
    // coin set changes - the old code rebuilt the full String set per connection
    // on every broadcast.
    let wait_start = Instant::now();
    let guard = listener.lock().await;
    LISTENER_LOCK_WAIT_LATENCY.with_label_values(&["websocket_setup"]).observe(wait_start.elapsed().as_secs_f64());
    let hold_start = Instant::now();
    let is_ready = guard.is_ready();
    let mut universe = guard.universe();
    let active_l2_params = guard.active_l2_params();
    let active_subscription_interests = guard.active_subscription_interests();
    LISTENER_LOCK_HOLD_LATENCY.with_label_values(&["websocket_setup"]).observe(hold_start.elapsed().as_secs_f64());
    drop(guard);
    // Per-subscription cache for L2 dedup + heartbeat resend. The whole
    // subscription is the key so nLevels is included without allocating a
    // formatted string on every broadcast/heartbeat lookup.
    let mut last_l2: HashMap<Subscription, L2Entry> = HashMap::new();
    // Subscriptions waiting for their first L2 payload. Normal L2 fanout uses
    // dirty-coin keyed lookup; this set preserves first-send behavior for quiet
    // coins without scanning every L2 subscription after the connection is warm.
    let mut uncached_l2: HashSet<Subscription> = HashSet::new();
    // Per-coin cache for BBO dedup + heartbeat resend
    let mut last_bbo: HashMap<String, BboEntry> = HashMap::new();
    // Shared L2 variant registry + this connection's refcount guards (one per variant
    // shape it subscribes to). Dropping the map on disconnect releases every guard,
    // so cleanup is robust to abnormal disconnects.
    let mut l2_param_guards: HashMap<L2SnapshotParams, L2ParamGuard> = HashMap::new();
    let mut subscription_interest_guards: HashMap<Subscription, SubscriptionInterestGuard> = HashMap::new();
    if !is_ready {
        let msg = ServerResponse::Error("Order book not ready for streaming (waiting for snapshot)".to_string());
        let _ = send_socket_message(&mut socket, msg).await;
        return;
    }

    // Optional heartbeat ticker. We tick at min(enabled_heartbeats)/2 (clamped to [50, 500] ms)
    // so each subscription's last-sent timestamp can drift at most half a heartbeat from the configured value.
    let mut heartbeat_ticker = build_heartbeat_ticker(l2book_heartbeat_ms, bbo_heartbeat_ms);
    let l2_hb = if l2book_heartbeat_ms > 0 { Some(Duration::from_millis(l2book_heartbeat_ms)) } else { None };
    let bbo_hb = if bbo_heartbeat_ms > 0 { Some(Duration::from_millis(bbo_heartbeat_ms)) } else { None };

    // `alive` flips to false the moment any `send_socket_message` returns false
    // (network error or send timeout). The outer loop checks it at every iteration
    // boundary so a wedged client is dropped instead of looping forever.
    let mut alive = true;
    // Set after a broadcast-channel lag: a dropped Snapshot message may have
    // carried dirty coins this connection never saw, so the next Snapshot must
    // re-evaluate every subscription instead of trusting the dirty-set skip.
    let mut force_full_l2 = false;
    while alive {
        select! {
            recv_result = internal_message_rx.recv() => {
                match recv_result {
                    Ok(msg) => {
                        let fanout_start = Instant::now();
                        let channel = msg.fanout_channel();
                        let channel_label = channel.label();
                        let active_fanout_subscriptions = manager.active_subscriptions_for_fanout(channel);
                        let mut fanout_subscriptions = 0usize;
                        match msg.as_ref() {
                            InternalMessage::Snapshot{ l2_snapshots, time, dirty, universe: new_universe, l2_frames } => {
                                if let Some(u) = new_universe {
                                    universe = Arc::clone(u);
                                }
                                if force_full_l2 {
                                    fanout_subscriptions = manager.subscription_count_for_type(SubscriptionKind::L2Book);
                                    for sub in manager.subscriptions_for_type(SubscriptionKind::L2Book) {
                                        if !alive { break; }
                                        alive &= send_ws_data_from_snapshot(&mut socket, sub, l2_snapshots.as_ref(), *time, &mut last_l2, dirty, force_full_l2, l2_frames, l2_hb.is_some()).await;
                                        if last_l2.contains_key(sub) {
                                            uncached_l2.remove(sub);
                                        }
                                    }
                                } else {
                                    for coin in dirty {
                                        for sub in manager.subscriptions_for_coin(SubscriptionKind::L2Book, coin.as_str()) {
                                            fanout_subscriptions += 1;
                                            if !alive { break; }
                                            alive &= send_ws_data_from_snapshot(&mut socket, sub, l2_snapshots.as_ref(), *time, &mut last_l2, dirty, force_full_l2, l2_frames, l2_hb.is_some()).await;
                                            if last_l2.contains_key(sub) {
                                                uncached_l2.remove(sub);
                                            }
                                        }
                                    }
                                    let pending: Vec<Subscription> = uncached_l2
                                        .iter()
                                        .filter(|sub| match sub.coin_key() {
                                            Some(coin) => !dirty.contains(coin),
                                            None => true,
                                        })
                                        .cloned()
                                        .collect();
                                    for sub in pending {
                                        fanout_subscriptions += 1;
                                        if !alive { break; }
                                        alive &= send_ws_data_from_snapshot(&mut socket, &sub, l2_snapshots.as_ref(), *time, &mut last_l2, dirty, force_full_l2, l2_frames, l2_hb.is_some()).await;
                                        if last_l2.contains_key(&sub) {
                                            uncached_l2.remove(&sub);
                                        }
                                    }
                                }
                                force_full_l2 = false;
                            },
                            InternalMessage::BboUpdate{ bbos, time } => {
                                // Fast path for changed BBO coins only. Iterating
                                // changed payloads through the keyed subscription
                                // index avoids scanning every BBO subscription on
                                // every book change.
                                for (coin, bbo) in bbos.iter() {
                                    let coin = coin.as_str();
                                    for _sub in manager.subscriptions_for_coin(SubscriptionKind::Bbo, coin) {
                                        fanout_subscriptions += 1;
                                        if !alive { break; }
                                        alive &= send_ws_data_from_bbo(&mut socket, coin, bbo, *time, &mut last_bbo, bbo_hb.is_some()).await;
                                    }
                                }
                            },
                            InternalMessage::Fills{ trades_by_coin } => {
                                // Per-coin payloads were grouped once in the listener; the
                                // wire frame is serialized once by the first subscribed
                                // connection and shared (refcounted bytes) by every other.
                                for (coin, ct) in trades_by_coin.iter() {
                                    for _sub in manager.subscriptions_for_coin(SubscriptionKind::Trades, coin) {
                                        fanout_subscriptions += 1;
                                        if !alive { break; }
                                        BROADCASTS_TOTAL.with_label_values(&["trades"]).inc();
                                        let frame = ct.frame.get_or_serialize(SubscriptionKind::Trades.label(), || ServerResponse::Trades(Arc::clone(&ct.trades)));
                                        if let Some(event_time_ms) = Trade::latest_time(ct.trades.as_ref()) {
                                            observe_transport_event_egress_age(TransportKind::Websocket, SubscriptionKind::Trades, event_time_ms);
                                            observe_transport_payload_egress_age(TransportKind::Websocket, SubscriptionKind::Trades, PayloadTimestampKind::TradeTime,
                                                event_time_ms,
                                            );
                                        }
                                        alive &= send_socket_frame(&mut socket, SubscriptionKind::Trades, frame).await;
                                    }
                                }
                            },
                            InternalMessage::L4OrderDiffs{ time, height, diffs_by_coin } => {
                                for (coin, cd) in diffs_by_coin.iter() {
                                    for _sub in manager.subscriptions_for_coin(SubscriptionKind::BookDiffs, coin) {
                                        fanout_subscriptions += 1;
                                        if !alive { break; }
                                        BROADCASTS_TOTAL.with_label_values(&["bookDiffs"]).inc();
                                        let frame = cd.book_diffs_frame.get_or_serialize(SubscriptionKind::BookDiffs.label(), || ServerResponse::BookDiffs(Arc::clone(&cd.diffs)));
                                        observe_transport_event_egress_age(TransportKind::Websocket, SubscriptionKind::BookDiffs, *time);
                                        alive &= send_socket_frame(&mut socket, SubscriptionKind::BookDiffs, frame).await;
                                    }
                                    for _sub in manager.subscriptions_for_coin(SubscriptionKind::L4Book, coin) {
                                        fanout_subscriptions += 1;
                                        if !alive { break; }
                                        BROADCASTS_TOTAL.with_label_values(&["l4"]).inc();
                                        let frame = cd.l4_frame.get_or_serialize(SubscriptionKind::L4Book.label(), || {
                                            ServerResponse::L4Book(L4Book::Updates(L4BookUpdates {
                                                time: *time,
                                                height: *height,
                                                order_statuses: Arc::new(Vec::new()),
                                                book_diffs: Arc::clone(&cd.diffs),
                                            }))
                                        });
                                        observe_transport_event_egress_age(TransportKind::Websocket, SubscriptionKind::L4Book, *time);
                                        alive &= send_socket_frame(&mut socket, SubscriptionKind::L4Book, frame).await;
                                    }
                                }
                            },
                            InternalMessage::L4OrderStatuses{ time, height, statuses_by_coin, statuses_by_user } => {
                                for (coin, cs) in statuses_by_coin.iter() {
                                    for _sub in manager.subscriptions_for_coin(SubscriptionKind::L4Book, coin) {
                                        fanout_subscriptions += 1;
                                        if !alive { break; }
                                        BROADCASTS_TOTAL.with_label_values(&["l4"]).inc();
                                        let frame = cs.l4_frame.get_or_serialize(SubscriptionKind::L4Book.label(), || {
                                            ServerResponse::L4Book(L4Book::Updates(L4BookUpdates {
                                                time: *time,
                                                height: *height,
                                                order_statuses: Arc::clone(&cs.statuses),
                                                book_diffs: Arc::new(Vec::new()),
                                            }))
                                        });
                                        observe_transport_event_egress_age(TransportKind::Websocket, SubscriptionKind::L4Book, *time);
                                        cs.timestamps
                                            .observe_transport_egress_age(TransportKind::Websocket, SubscriptionKind::L4Book);
                                        alive &= send_socket_frame(&mut socket, SubscriptionKind::L4Book, frame).await;
                                    }
                                }
                                for (user, statuses) in statuses_by_user.iter() {
                                    if !alive { break; }
                                    for _sub in manager.order_update_subscriptions_for_user(*user) {
                                        fanout_subscriptions += 1;
                                        if !alive { break; }
                                        alive &= send_ws_order_updates(&mut socket, statuses, *time, *height).await;
                                    }
                                }
                            },
                        }
                        TRANSPORT_FANOUT_SUBSCRIPTIONS
                            .with_label_values(&[TransportKind::Websocket.label(), channel_label])
                            .observe(fanout_subscriptions as f64);
                        TRANSPORT_FANOUT_ACTIVE_SUBSCRIPTIONS
                            .with_label_values(&[TransportKind::Websocket.label(), channel_label])
                            .observe(active_fanout_subscriptions as f64);
                        TRANSPORT_FANOUT_LATENCY
                            .with_label_values(&[TransportKind::Websocket.label(), channel_label])
                            .observe(fanout_start.elapsed().as_secs_f64());

                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        CHANNEL_LAG.with_label_values(&[TransportKind::Websocket.label()]).set(n as i64);
                        CHANNEL_DROPS_TOTAL.with_label_values(&[TransportKind::Websocket.label()]).inc();
                        // A dropped Snapshot may have carried dirty coins we never
                        // saw - process the next one in full (hash dedup still
                        // suppresses sends whose payload didn't actually change).
                        force_full_l2 = true;
                        log::debug!("Receiver lagged: {n} messages");
                    }
                    Err(err) => {
                        error!("Receiver error: {err}");
                        return;
                    }
                }
            }

            _ = heartbeat_tick(&mut heartbeat_ticker) => {
                let now = Instant::now();
                let now_ms = chrono::Utc::now().timestamp_millis().max(0) as u64;
                for sub in manager.subscriptions_for_type(SubscriptionKind::L2Book) {
                    if !alive { break; }
                    if let Subscription::L2Book { .. } = sub {
                        let Some(hb) = l2_hb else { continue };
                        if let Some(entry) = last_l2.get_mut(sub) {
                            // payload is always Some when the heartbeat is enabled
                            // (the change-driven send stores it for exactly this).
                            if now.duration_since(entry.last_sent) >= hb
                                && let Some(payload) = entry.payload.as_mut()
                            {
                                payload.set_time(now_ms);
                                entry.last_sent = now;
                                BROADCASTS_TOTAL.with_label_values(&["l2_heartbeat"]).inc();
                                let payload = payload.clone();
                                alive &= send_socket_message(&mut socket, ServerResponse::L2Book(payload)).await;
                            }
                        }
                    }
                }
                for sub in manager.subscriptions_for_type(SubscriptionKind::Bbo) {
                    if !alive { break; }
                    if let Subscription::Bbo { coin } = sub {
                        let Some(hb) = bbo_hb else { continue };
                        if let Some(entry) = last_bbo.get_mut(coin) {
                            if now.duration_since(entry.last_sent) >= hb
                                && let Some(payload) = entry.payload.as_mut()
                            {
                                payload.time = now_ms;
                                entry.last_sent = now;
                                BROADCASTS_TOTAL.with_label_values(&["bbo_heartbeat"]).inc();
                                let payload = payload.clone();
                                alive &= send_socket_message(&mut socket, ServerResponse::Bbo(payload)).await;
                            }
                        }
                    }
                }
            }

            msg = socket.next() => {
                if let Some(frame) = msg {
                    match frame.opcode {
                        OpCode::Text => {
                            let text = match std::str::from_utf8(&frame.payload) {
                                Ok(text) => text,
                                Err(err) => {
                                    log::warn!("unable to parse websocket content: {err}: {:?}", frame.payload.as_ref());
                                    // deserves to close the connection because the payload is not a valid utf8 string.
                                    return;
                                }
                            };

                            log::debug!("Client message: {text}");

                            if let Ok(value) = serde_json::from_str::<ClientMessage>(text) {
                                match value {
                                    ClientMessage::Ping => {
                                        alive &= send_socket_message(&mut socket, ServerResponse::Pong).await;
                                    }
                                    _ => {
                                        alive &= receive_client_message(
                                            &mut socket,
                                            &mut manager,
                                            value,
                                            &universe,
                                            listener.clone(),
                                            bbo_only,
                                            &mut last_l2,
                                            &mut uncached_l2,
                                            &mut last_bbo,
                                            &active_l2_params,
                                            &mut l2_param_guards,
                                            &active_subscription_interests,
                                            &mut subscription_interest_guards,
                                        ).await;
                                    }
                                }
                            }
                            else {
                                let msg = ServerResponse::Error(format!("Error parsing JSON into valid websocket request: {text}"));
                                alive &= send_socket_message(&mut socket, msg).await;
                            }
                        }
                        OpCode::Close => {
                            info!("Client disconnected");
                            return;
                        }
                        _ => {}
                    }
                } else {
                    info!("Client connection closed");
                    return;
                }
            }
        }
    }
    info!("Dropping connection: socket write failed or timed out");
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_arguments)]
async fn receive_client_message(
    socket: &mut WebSocket,
    manager: &mut SubscriptionManager,
    client_message: ClientMessage,
    universe: &HashSet<String>,
    listener: Arc<Mutex<OrderBookListener>>,
    bbo_only: bool,
    last_l2: &mut HashMap<Subscription, L2Entry>,
    uncached_l2: &mut HashSet<Subscription>,
    last_bbo: &mut HashMap<String, BboEntry>,
    active_l2_params: &ActiveL2Params,
    l2_param_guards: &mut HashMap<L2SnapshotParams, L2ParamGuard>,
    active_subscription_interests: &ActiveSubscriptionInterests,
    subscription_interest_guards: &mut HashMap<Subscription, SubscriptionInterestGuard>,
) -> bool {
    let subscription = match &client_message {
        ClientMessage::Unsubscribe { subscription } | ClientMessage::Subscribe { subscription } => subscription.clone(),
        ClientMessage::Ping => unreachable!("Ping is handled before receive_client_message"),
    };
    // BBO-only mode rejects non-BBO subs up-front, before validation, so the
    // operator sees a single clear "denied" message in the log instead of "valid
    // subscription" then a rejection.
    if bbo_only && !matches!(&subscription, Subscription::Bbo { .. }) {
        return send_socket_message(
            socket,
            ServerResponse::Error(
                "BBO-only mode: L2/L4/Trades subscriptions disabled. Only BBO subscriptions allowed.".to_string(),
            ),
        )
        .await;
    }
    // this is used for display purposes only, hence unwrap_or_default. It also shouldn't fail
    let sub = serde_json::to_string(&subscription).unwrap_or_default();
    if !subscription.validate(universe) {
        return send_socket_message(socket, ServerResponse::Error(format!("Invalid subscription: {sub}"))).await;
    }

    let (word, success) = match &client_message {
        ClientMessage::Subscribe { .. } => match manager.subscribe(subscription.clone()) {
            Ok(inserted) => {
                // Register the variant shape so the listener computes it. One guard
                // per shape per connection (n_levels is a send-time truncation, not
                // part of the cached shape); the entry API dedups shared shapes.
                if inserted && let Subscription::L2Book { n_sig_figs, mantissa, .. } = &subscription {
                    let params = L2SnapshotParams::new(*n_sig_figs, *mantissa);
                    l2_param_guards.entry(params).or_insert_with(|| active_l2_params.acquire(params));
                    uncached_l2.insert(subscription.clone());
                }
                if inserted && let Some(interest) = subscription_interest(&subscription) {
                    subscription_interest_guards
                        .entry(subscription.clone())
                        .or_insert_with(|| active_subscription_interests.acquire(interest));
                }
                ("", inserted)
            }
            Err(err) => {
                return send_socket_message(socket, ServerResponse::Error(format!("Rejected subscription: {err}")))
                    .await;
            }
        },
        ClientMessage::Unsubscribe { .. } => {
            let removed = manager.unsubscribe(subscription.clone());
            // Drop the per-connection dedup/heartbeat cache entry for the just-unsubscribed
            // stream. Without this, a client that sub/unsub-cycles distinct L2 variants on
            // the same coin (or BBO across coins) leaks one entry per cycle until disconnect.
            if removed {
                match &subscription {
                    Subscription::L2Book { n_sig_figs, mantissa, .. } => {
                        last_l2.remove(&subscription);
                        uncached_l2.remove(&subscription);
                        // Release this connection's guard for the shape only if no
                        // remaining L2 subscription on this connection still uses it
                        // (e.g. same shape on another coin / different n_levels).
                        let params = L2SnapshotParams::new(*n_sig_figs, *mantissa);
                        let still_used = manager.subscriptions().iter().any(|s| {
                            matches!(s, Subscription::L2Book { n_sig_figs: nsf, mantissa: m, .. }
                                if L2SnapshotParams::new(*nsf, *m) == params)
                        });
                        if !still_used {
                            l2_param_guards.remove(&params);
                        }
                    }
                    Subscription::Bbo { coin } => {
                        last_bbo.remove(coin);
                    }
                    _ => {}
                }
                subscription_interest_guards.remove(&subscription);
            }
            ("un", removed)
        }
        ClientMessage::Ping => unreachable!(),
    };
    if success {
        let snapshot_msg = if let ClientMessage::Subscribe { subscription } = &client_message {
            let msg = subscription.handle_immediate_snapshot(listener).await;
            match msg {
                Ok(msg) => msg,
                Err(err) => {
                    manager.unsubscribe(subscription.clone());
                    subscription_interest_guards.remove(subscription);
                    return send_socket_message(
                        socket,
                        ServerResponse::Error(format!("Unable to grab order book snapshot: {err}")),
                    )
                    .await;
                }
            }
        } else {
            None
        };
        if !send_socket_message(socket, ServerResponse::SubscriptionResponse(client_message)).await {
            return false;
        }
        if let Some(snapshot_msg) = snapshot_msg {
            return send_socket_message(socket, snapshot_msg).await;
        }
        true
    } else {
        send_socket_message(socket, ServerResponse::Error(format!("Already {word}subscribed: {sub}"))).await
    }
}

const fn subscription_interest(subscription: &Subscription) -> Option<SubscriptionInterest> {
    match subscription {
        Subscription::Bbo { .. } => Some(SubscriptionInterest::Bbo),
        Subscription::Trades { .. } => Some(SubscriptionInterest::Trades),
        Subscription::L4Book { .. } => Some(SubscriptionInterest::L4Book),
        Subscription::BookDiffs { .. } => Some(SubscriptionInterest::BookDiffs),
        Subscription::OrderUpdates { .. } => Some(SubscriptionInterest::OrderUpdates),
        Subscription::L2Book { .. } => None,
    }
}

/// Fast BBO broadcast - directly from BBO HashMap without L2 snapshot computation.
/// Returns false if the socket send failed/timed out (caller must drop the connection).
async fn send_ws_data_from_bbo(
    socket: &mut WebSocket,
    coin: &str,
    cb: &CoinBbo,
    time: u64,
    last_bbo: &mut HashMap<String, BboEntry>,
    store_payload: bool,
) -> bool {
    let (best_bid, best_ask) = (&cb.raw.0, &cb.raw.1);
    // Dedup on the raw fixed-point values BEFORE rendering anything: the
    // strings are only built when the BBO actually changed.
    let current: BboKey = (
        best_bid.as_ref().map(|(px, sz, _)| (px.value(), sz.value())),
        best_ask.as_ref().map(|(px, sz, _)| (px.value(), sz.value())),
    );

    if last_bbo.get(coin).map(|e| e.tuple) != Some(current) {
        // Canonical wire format (Px/Sz::to_str) - matches what the L2 path
        // emits. Rendered inside the shared-frame builder, so it runs once
        // per coin per broadcast (plus once per heartbeat-enabled
        // connection for the resend payload) instead of per connection.
        let render = || {
            let bid =
                best_bid.as_ref().map(|(px, sz, n)| crate::types::Level::new(px.to_str(), sz.to_str(), *n as usize));
            let ask =
                best_ask.as_ref().map(|(px, sz, n)| crate::types::Level::new(px.to_str(), sz.to_str(), *n as usize));
            Bbo { coin: coin.to_string(), time, bid, ask }
        };

        BBO_CHANGES_TOTAL.with_label_values(&[coin]).inc();
        BROADCASTS_TOTAL.with_label_values(&["bbo"]).inc();
        let frame = cb.frame.get_or_serialize(SubscriptionKind::Bbo.label(), || ServerResponse::Bbo(render()));
        let payload = store_payload.then(render);
        BboEntry::upsert(last_bbo, coin, current, payload);
        observe_transport_event_egress_age(TransportKind::Websocket, SubscriptionKind::Bbo, time);
        return send_socket_frame(socket, SubscriptionKind::Bbo, frame).await;
    }
    TRANSPORT_MESSAGES_SKIPPED_TOTAL
        .with_label_values(&[TransportKind::Websocket.label(), SubscriptionKind::Bbo.label(), "unchanged"])
        .inc();
    true
}

/// Per-send timeout. A slow or hostile client whose TCP receive window stays full
/// would otherwise block `socket.send(...).await` indefinitely, freezing this
/// connection's whole `select!` loop and accumulating broadcast lag.
const WS_SEND_TIMEOUT: Duration = Duration::from_secs(5);

/// Send a `ServerResponse` to the client. Returns `false` when the underlying
/// socket failed to write (network error or `WS_SEND_TIMEOUT` elapsed). Callers
/// in the `select!` loop must bail out on `false` so we drop the doomed
/// connection instead of looping forever on a wedged write.
async fn send_socket_message(socket: &mut WebSocket, msg: ServerResponse) -> bool {
    let channel = msg.channel_label();
    let payload = match serde_json::to_string(&msg) {
        Ok(p) => p,
        Err(err) => {
            error!("Server response serialization error: {err}");
            // Serialization failure is our bug, not the client's; keep the connection.
            return true;
        }
    };
    send_socket_payload(socket, channel, bytes::Bytes::from(payload)).await
}

/// Send a pre-serialized wire frame (built once in/for the listener broadcast
/// and shared by every subscribed connection). An empty frame means its
/// serialization failed when it was first built (already logged there) - skip
/// it and keep the connection, mirroring `send_socket_message`.
async fn send_socket_frame(socket: &mut WebSocket, channel: SubscriptionKind, frame: bytes::Bytes) -> bool {
    if frame.is_empty() {
        return true;
    }
    send_socket_payload(socket, channel.label(), frame).await
}

async fn send_socket_payload(socket: &mut WebSocket, channel: &'static str, payload: bytes::Bytes) -> bool {
    let send_start = Instant::now();
    match tokio::time::timeout(WS_SEND_TIMEOUT, socket.send(FrameView::text(payload))).await {
        Ok(Ok(())) => {
            TRANSPORT_SEND_LATENCY
                .with_label_values(&[TransportKind::Websocket.label()])
                .observe(send_start.elapsed().as_secs_f64());
            MESSAGES_SENT_TOTAL.inc();
            TRANSPORT_MESSAGES_SENT_TOTAL.with_label_values(&[TransportKind::Websocket.label()]).inc();
            TRANSPORT_CHANNEL_MESSAGES_SENT_TOTAL.with_label_values(&[TransportKind::Websocket.label(), channel]).inc();
            true
        }
        Ok(Err(err)) => {
            TRANSPORT_SEND_LATENCY
                .with_label_values(&[TransportKind::Websocket.label()])
                .observe(send_start.elapsed().as_secs_f64());
            error!("Failed to send: {err}");
            WS_SEND_ERRORS_TOTAL.inc();
            TRANSPORT_SEND_ERRORS_TOTAL.with_label_values(&[TransportKind::Websocket.label(), "error"]).inc();
            false
        }
        Err(_) => {
            TRANSPORT_SEND_LATENCY
                .with_label_values(&[TransportKind::Websocket.label()])
                .observe(send_start.elapsed().as_secs_f64());
            error!("Send timeout (>{:?}); dropping slow client", WS_SEND_TIMEOUT);
            WS_SEND_ERRORS_TOTAL.inc();
            TRANSPORT_SEND_ERRORS_TOTAL.with_label_values(&[TransportKind::Websocket.label(), "timeout"]).inc();
            // Best-effort close handshake. If the close itself times out we just drop.
            let _unused = tokio::time::timeout(Duration::from_secs(1), socket.close()).await;
            false
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn send_ws_data_from_snapshot(
    socket: &mut WebSocket,
    subscription: &Subscription,
    snapshot: &HashMap<Coin, Arc<HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>>,
    time: u64,
    last_l2: &mut HashMap<Subscription, L2Entry>,
    dirty: &HashSet<Coin>,
    force_full: bool,
    l2_frames: &L2FrameCache,
    store_payload: bool,
) -> bool {
    // BBO subscriptions are filtered out by the caller (they are served by the
    // BboUpdate fast path), so only L2Book needs handling here.
    if let Subscription::L2Book { coin, n_sig_figs, n_levels, mantissa } = subscription {
        // Skip coins that were not rebuilt in this flush: the payload we already
        // sent is still current, so the truncate/export/hash work below would be
        // pure waste. Runs for every subscription on every broadcast, which is
        // why it compares with `&str` (no allocation). `force_full` overrides
        // after a broadcast lag; a missing cache entry means we never sent
        // anything for this subscription (it is brand new) - always process.
        if !force_full && !dirty.contains(coin.as_str()) && last_l2.contains_key(subscription) {
            TRANSPORT_MESSAGES_SKIPPED_TOTAL
                .with_label_values(&[TransportKind::Websocket.label(), SubscriptionKind::L2Book.label(), "not_dirty"])
                .inc();
            return true;
        }

        let n_levels = n_levels.unwrap_or(DEFAULT_LEVELS);
        // Resolve the data source BEFORE consulting the shared frame cache, so
        // the raced-variant early-return below doesn't poison the cache.
        let variant = match snapshot.get(coin.as_str()) {
            Some(per_coin) => {
                let Some(variant) = per_coin.get(&L2SnapshotParams::new(*n_sig_figs, *mantissa)) else {
                    // Coin present but this variant shape hasn't been built yet
                    // (subscriber raced the flush); the next flush covers it.
                    error!("Variant for coin {coin} not found");
                    return true;
                };
                Some(variant)
            }
            // The coin's book emptied and the multi-book evicted it. Send an
            // empty snapshot so subscribers learn the book is gone instead of
            // keeping the last non-empty payload on screen forever.
            None => None,
        };

        // Truncate/export/hash once per (coin, shape, nLevels) per broadcast
        // via the shared L2 cache. WebSocket JSON is also serialized lazily once
        // from the rendered payload, so neither render nor serde cost scales
        // with subscribed connection count.
        let built = l2_frames.get_or_build(coin, *n_sig_figs, *mantissa, n_levels, time, variant);
        let current_hash = built.hash();

        if last_l2.get(subscription).map(|e| e.hash) != Some(current_hash) {
            BROADCASTS_TOTAL.with_label_values(&["l2"]).inc();
            let payload = store_payload.then(|| built.payload_clone());
            last_l2.insert(subscription.clone(), L2Entry { hash: current_hash, last_sent: Instant::now(), payload });
            observe_transport_event_egress_age(TransportKind::Websocket, SubscriptionKind::L2Book, time);
            return send_socket_frame(socket, SubscriptionKind::L2Book, built.websocket_frame()).await;
        }
        TRANSPORT_MESSAGES_SKIPPED_TOTAL
            .with_label_values(&[TransportKind::Websocket.label(), SubscriptionKind::L2Book.label(), "unchanged"])
            .inc();
    }
    true
}

impl Subscription {
    // snapshots that begin a stream
    async fn handle_immediate_snapshot(
        &self,
        listener: Arc<Mutex<OrderBookListener>>,
    ) -> Result<Option<ServerResponse>> {
        if let Self::L4Book { coin } = self {
            // Snapshot ONLY the requested coin. The old path cloned the entire
            // multi-book (every coin, every order) under the listener lock,
            // stalling event processing for hundreds of milliseconds per
            // l4Book subscribe.
            let wait_start = Instant::now();
            let guard = listener.lock().await;
            LISTENER_LOCK_WAIT_LATENCY
                .with_label_values(&["websocket_l4_snapshot"])
                .observe(wait_start.elapsed().as_secs_f64());
            let hold_start = Instant::now();
            let snapshot = guard.compute_snapshot_for_coin(&Coin::new(coin));
            LISTENER_LOCK_HOLD_LATENCY
                .with_label_values(&["websocket_l4_snapshot"])
                .observe(hold_start.elapsed().as_secs_f64());
            if let Some((time, height, coin_snapshot)) = snapshot {
                let levels =
                    coin_snapshot.as_ref().clone().map(|orders| orders.into_iter().map(L4Order::from).collect());
                return Ok(Some(ServerResponse::L4Book(L4Book::Snapshot { coin: coin.clone(), time, height, levels })));
            }
            return Err("Snapshot Failed".into());
        }
        Ok(None)
    }
}

/// Send order updates for one grouped user payload. The caller has already
/// matched subscriptions by parsed user address, so this path does no hex
/// parsing or map lookup per subscribed client.
async fn send_ws_order_updates(
    socket: &mut WebSocket,
    statuses: &crate::listeners::order_book::UserStatuses,
    time: u64,
    height: u64,
) -> bool {
    if !statuses.statuses.is_empty() {
        observe_transport_event_egress_age(TransportKind::Websocket, SubscriptionKind::OrderUpdates, time);
        statuses.timestamps.observe_transport_egress_age(TransportKind::Websocket, SubscriptionKind::OrderUpdates);
        let frame = statuses.frame.get_or_serialize(SubscriptionKind::OrderUpdates.label(), || {
            let user_updates: Vec<OrderUpdate> = statuses
                .statuses
                .iter()
                .map(|status| OrderUpdate::new(status.user, time, height, status.clone()))
                .collect();
            ServerResponse::OrderUpdates(user_updates)
        });
        return send_socket_frame(socket, SubscriptionKind::OrderUpdates, frame).await;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_l2_cache_uses_full_subscription_key() {
        // Two subscriptions differing only in nLevels MUST have distinct cache keys:
        // a shared entry made their dedup hashes ping-pong (both resent every
        // broadcast) and unsubscribing one dropped the other's cache.
        let a = Subscription::L2Book { coin: "BTC".to_string(), n_sig_figs: Some(5), mantissa: None, n_levels: None };
        let b =
            Subscription::L2Book { coin: "BTC".to_string(), n_sig_figs: Some(5), mantissa: None, n_levels: Some(50) };
        let mut cache = HashMap::new();
        cache.insert(a.clone(), 1_u8);
        cache.insert(b.clone(), 2_u8);
        assert_eq!(cache.len(), 2);
        // Validation rejects an explicit nLevels == DEFAULT_LEVELS, so the
        // None default cannot collide with a permitted explicit value.
        assert!(
            !Subscription::L2Book {
                coin: "BTC".to_string(),
                n_sig_figs: Some(5),
                mantissa: None,
                n_levels: Some(DEFAULT_LEVELS),
            }
            .validate(&HashSet::from(["BTC".to_string()]))
        );
        assert_ne!(
            a,
            Subscription::L2Book { coin: "ETH".to_string(), n_sig_figs: Some(5), mantissa: None, n_levels: None }
        );
        assert_ne!(
            a,
            Subscription::L2Book { coin: "BTC".to_string(), n_sig_figs: Some(5), mantissa: Some(2), n_levels: None }
        );
    }
}
