use crate::orderbook as pb;
use log::{error, info};
use server::{
    Result, ServerConfig,
    metrics::{BBO_CHANGES_TOTAL, BROADCASTS_TOTAL, MESSAGES_SENT_TOTAL, ORDERBOOK_HEIGHT, WS_SEND_ERRORS_TOTAL},
    transport::{
        ActiveL2Params, ClientMessage, Coin, CoinBbo, CoinStatuses, DEFAULT_LEVELS, InnerLevel, InternalMessage,
        L2ParamGuard, L2SnapshotParams, L4Order, Level, MAX_LEVELS, NodeDataOrderDiff, NodeDataOrderStatus,
        OrderBookListener, OrderDiff, Side, Snapshot, Subscription, SubscriptionManager, Trade, hl_listen_hft,
    },
};
use std::{
    collections::{HashMap, HashSet},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    select,
    sync::{
        Mutex,
        broadcast::{Sender, channel},
        mpsc,
    },
};
use tokio_stream::{Stream, wrappers::ReceiverStream};
use tonic::{Request, Response, Status, transport::Server};

struct L2Entry {
    hash: u64,
    last_sent: Instant,
    payload: Option<pb::L2Book>,
}

type BboKey = (Option<(u64, u64)>, Option<(u64, u64)>);

struct BboEntry {
    tuple: BboKey,
    last_sent: Instant,
    payload: Option<pb::Bbo>,
}

fn l2_cache_key(coin: &str, n_sig_figs: Option<u32>, mantissa: Option<u64>, n_levels: Option<usize>) -> String {
    format!("{}:{}:{}:{}", coin, n_sig_figs.unwrap_or(0), mantissa.unwrap_or(0), n_levels.unwrap_or(DEFAULT_LEVELS))
}

fn build_heartbeat_ticker(l2book_heartbeat_ms: u64, bbo_heartbeat_ms: u64) -> Option<tokio::time::Interval> {
    let enabled = [l2book_heartbeat_ms, bbo_heartbeat_ms].into_iter().filter(|&ms| ms > 0).min()?;
    let tick_ms = (enabled / 2).max(50).min(500);
    let mut interval = tokio::time::interval(Duration::from_millis(tick_ms));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    Some(interval)
}

async fn heartbeat_tick(ticker: &mut Option<tokio::time::Interval>) {
    match ticker {
        Some(t) => {
            t.tick().await;
        }
        None => std::future::pending::<()>().await,
    }
}

pub async fn run_grpc_server(config: ServerConfig) -> Result<()> {
    let (internal_message_tx, _) = channel::<Arc<InternalMessage>>(16384);

    let market_filter = (config.include_perps, config.include_spot, config.include_hip3);
    let ignore_spot = !config.include_spot;
    let active_l2_params = ActiveL2Params::new();

    let listener = {
        let internal_message_tx = internal_message_tx.clone();
        let mut listener =
            OrderBookListener::new(Some(internal_message_tx), ignore_spot, active_l2_params, market_filter);
        listener.set_tolerate_drift(config.no_resync);
        listener
    };
    let listener = Arc::new(Mutex::new(listener));
    let listener_task = {
        let listener = listener.clone();
        let config = config.clone();
        tokio::spawn(async move {
            info!("Starting HFT-optimized listener");
            let result = hl_listen_hft(listener, config).await;
            if let Err(err) = result {
                error!("Listener fatal error: {err}");
                std::process::exit(1);
            }
        })
    };

    let addr = config.address.parse()?;
    let service = GrpcOrderbookService {
        internal_message_tx,
        listener,
        bbo_only: config.bbo_only,
        l2book_heartbeat_ms: config.l2book_heartbeat_ms,
        bbo_heartbeat_ms: config.bbo_heartbeat_ms,
        start_time: Instant::now(),
        active_connections: Arc::new(AtomicU64::new(0)),
    };

    info!("gRPC server running at http://{}", config.address);

    let orderbook_service = pb::orderbook_server::OrderbookServer::new(service);
    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter.set_serving::<pb::orderbook_server::OrderbookServer<GrpcOrderbookService>>().await;

    tokio::select! {
        result = Server::builder().tcp_nodelay(true).add_service(health_service).add_service(orderbook_service).serve(addr) => {
            if let Err(err) = result {
                error!("gRPC server fatal error: {err}");
                std::process::exit(2);
            }
        }
        join = listener_task => {
            error!("Listener task exited unexpectedly: {join:?}");
            std::process::exit(1);
        }
    }

    Ok(())
}

#[derive(Clone)]
struct GrpcOrderbookService {
    internal_message_tx: Sender<Arc<InternalMessage>>,
    listener: Arc<Mutex<OrderBookListener>>,
    bbo_only: bool,
    l2book_heartbeat_ms: u64,
    bbo_heartbeat_ms: u64,
    start_time: Instant,
    active_connections: Arc<AtomicU64>,
}

#[tonic::async_trait]
impl pb::orderbook_server::Orderbook for GrpcOrderbookService {
    type StreamStream = Pin<Box<dyn Stream<Item = std::result::Result<pb::ServerMessage, Status>> + Send + 'static>>;

    async fn stream(
        &self,
        request: Request<tonic::Streaming<pb::ClientMessage>>,
    ) -> std::result::Result<Response<Self::StreamStream>, Status> {
        let (tx, rx) = mpsc::channel(1024);
        self.active_connections.fetch_add(1, Ordering::Relaxed);

        let state = GrpcConnectionState {
            incoming: request.into_inner(),
            outgoing: tx,
            internal_message_tx: self.internal_message_tx.clone(),
            listener: self.listener.clone(),
            bbo_only: self.bbo_only,
            l2book_heartbeat_ms: self.l2book_heartbeat_ms,
            bbo_heartbeat_ms: self.bbo_heartbeat_ms,
            active_connections: self.active_connections.clone(),
        };
        tokio::spawn(async move {
            state.run().await;
        });

        let stream: Self::StreamStream = Box::pin(ReceiverStream::new(rx));
        Ok(Response::new(stream))
    }

    async fn ping(&self, _request: Request<pb::Ping>) -> std::result::Result<Response<pb::Pong>, Status> {
        Ok(Response::new(pb::Pong {}))
    }

    async fn health(
        &self,
        _request: Request<pb::HealthRequest>,
    ) -> std::result::Result<Response<pb::HealthResponse>, Status> {
        let is_ready = self.listener.lock().await.is_ready();
        Ok(Response::new(pb::HealthResponse {
            status: if is_ready { "ready" } else { "initializing" }.to_string(),
            uptime_seconds: self.start_time.elapsed().as_secs(),
            height: ORDERBOOK_HEIGHT.get().max(0) as u64,
            connections: self.active_connections.load(Ordering::Relaxed),
        }))
    }
}

struct GrpcConnectionState {
    incoming: tonic::Streaming<pb::ClientMessage>,
    outgoing: mpsc::Sender<std::result::Result<pb::ServerMessage, Status>>,
    internal_message_tx: Sender<Arc<InternalMessage>>,
    listener: Arc<Mutex<OrderBookListener>>,
    bbo_only: bool,
    l2book_heartbeat_ms: u64,
    bbo_heartbeat_ms: u64,
    active_connections: Arc<AtomicU64>,
}

impl GrpcConnectionState {
    async fn run(mut self) {
        let _guard = GrpcConnectionGuard { active_connections: self.active_connections.clone() };
        let is_ready = self.listener.lock().await.is_ready();
        if !is_ready {
            let _ = send_grpc_message(
                &self.outgoing,
                error_message("Order book not ready for streaming (waiting for snapshot)"),
            )
            .await;
            return;
        }

        let mut internal_message_rx = self.internal_message_tx.subscribe();
        let mut manager = SubscriptionManager::default();
        let mut universe = self.listener.lock().await.universe();
        let mut last_l2: HashMap<String, L2Entry> = HashMap::new();
        let mut last_bbo: HashMap<String, BboEntry> = HashMap::new();
        let mut user_addrs: HashMap<String, alloy::primitives::Address> = HashMap::new();
        let active_l2_params = self.listener.lock().await.active_l2_params();
        let mut l2_param_guards: HashMap<L2SnapshotParams, L2ParamGuard> = HashMap::new();
        let mut heartbeat_ticker = build_heartbeat_ticker(self.l2book_heartbeat_ms, self.bbo_heartbeat_ms);
        let l2_hb =
            if self.l2book_heartbeat_ms > 0 { Some(Duration::from_millis(self.l2book_heartbeat_ms)) } else { None };
        let bbo_hb = if self.bbo_heartbeat_ms > 0 { Some(Duration::from_millis(self.bbo_heartbeat_ms)) } else { None };
        let mut force_full_l2 = false;

        loop {
            select! {
                recv_result = internal_message_rx.recv() => {
                    let Ok(msg) = recv_result else {
                        return;
                    };
                    match msg.as_ref() {
                        InternalMessage::Snapshot{ l2_snapshots, time, dirty, universe: new_universe, .. } => {
                            if let Some(u) = new_universe {
                                universe = Arc::clone(u);
                            }
                            for sub in manager.subscriptions() {
                                if !matches!(sub, Subscription::Bbo { .. })
                                    && !send_grpc_data_from_snapshot(&self.outgoing, sub, l2_snapshots.as_ref(), *time, &mut last_l2, dirty, force_full_l2, l2_hb.is_some()).await {
                                    return;
                                }
                            }
                            force_full_l2 = false;
                        }
                        InternalMessage::BboUpdate{ bbos, time } => {
                            for sub in manager.subscriptions() {
                                if let Subscription::Bbo { coin } = sub
                                    && !send_grpc_data_from_bbo(&self.outgoing, coin, bbos, *time, &mut last_bbo, bbo_hb.is_some()).await {
                                    return;
                                }
                            }
                        }
                        InternalMessage::Fills{ trades_by_coin } => {
                            for sub in manager.subscriptions() {
                                if let Subscription::Trades { coin } = sub
                                    && let Some(ct) = trades_by_coin.get(coin.as_str()) {
                                    BROADCASTS_TOTAL.with_label_values(&["trades"]).inc();
                                    if !send_grpc_message(&self.outgoing, trades_message(ct.trades.as_ref())).await {
                                        return;
                                    }
                                }
                            }
                        }
                        InternalMessage::L4OrderDiffs{ time, height, diffs_by_coin } => {
                            for sub in manager.subscriptions() {
                                match sub {
                                    Subscription::BookDiffs { coin } => {
                                        if let Some(cd) = diffs_by_coin.get(coin.as_str()) {
                                            BROADCASTS_TOTAL.with_label_values(&["bookDiffs"]).inc();
                                            if !send_grpc_message(&self.outgoing, book_diffs_message(cd.diffs.as_ref())).await {
                                                return;
                                            }
                                        }
                                    }
                                    Subscription::L4Book { coin } => {
                                        if let Some(cd) = diffs_by_coin.get(coin.as_str()) {
                                            BROADCASTS_TOTAL.with_label_values(&["l4"]).inc();
                                            if !send_grpc_message(&self.outgoing, l4_updates_message(*time, *height, &[], cd.diffs.as_ref())).await {
                                                return;
                                            }
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                        InternalMessage::L4OrderStatuses{ time, height, statuses_by_coin } => {
                            for sub in manager.subscriptions() {
                                match sub {
                                    Subscription::L4Book { coin } => {
                                        if let Some(cs) = statuses_by_coin.get(coin.as_str()) {
                                            BROADCASTS_TOTAL.with_label_values(&["l4"]).inc();
                                            if !send_grpc_message(&self.outgoing, l4_updates_message(*time, *height, cs.statuses.as_ref(), &[])).await {
                                                return;
                                            }
                                        }
                                    }
                                    Subscription::OrderUpdates { user } => {
                                        if !send_grpc_order_updates(&self.outgoing, user, *time, *height, statuses_by_coin, &mut user_addrs).await {
                                            return;
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
                _ = heartbeat_tick(&mut heartbeat_ticker) => {
                    let now = Instant::now();
                    let now_ms = chrono::Utc::now().timestamp_millis().max(0) as u64;
                    for sub in manager.subscriptions() {
                        match sub {
                            Subscription::L2Book { coin, n_sig_figs, mantissa, n_levels } => {
                                let Some(hb) = l2_hb else { continue };
                                let key = l2_cache_key(coin, *n_sig_figs, *mantissa, *n_levels);
                                if let Some(entry) = last_l2.get_mut(&key)
                                    && now.duration_since(entry.last_sent) >= hb
                                    && let Some(payload) = entry.payload.as_mut() {
                                    payload.time = now_ms;
                                    entry.last_sent = now;
                                    BROADCASTS_TOTAL.with_label_values(&["l2_heartbeat"]).inc();
                                    if !send_grpc_message(&self.outgoing, l2_book_message(payload.clone())).await {
                                        return;
                                    }
                                }
                            }
                            Subscription::Bbo { coin } => {
                                let Some(hb) = bbo_hb else { continue };
                                if let Some(entry) = last_bbo.get_mut(coin)
                                    && now.duration_since(entry.last_sent) >= hb
                                    && let Some(payload) = entry.payload.as_mut() {
                                    payload.time = now_ms;
                                    entry.last_sent = now;
                                    BROADCASTS_TOTAL.with_label_values(&["bbo_heartbeat"]).inc();
                                    if !send_grpc_message(&self.outgoing, bbo_message(payload.clone())).await {
                                        return;
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                }
                message = self.incoming.message() => {
                    let message = match message {
                        Ok(Some(message)) => message,
                        Ok(None) => return,
                        Err(err) => {
                            error!("gRPC stream receive error: {err}");
                            return;
                        }
                    };
                    let client_message = match client_message_from_proto(message) {
                        Ok(message) => message,
                        Err(err) => {
                            if !send_grpc_message(&self.outgoing, error_message(&err)).await {
                                return;
                            }
                            continue;
                        }
                    };
                    match client_message {
                        ClientMessage::Ping => {
                            if !send_grpc_message(&self.outgoing, pong_message()).await {
                                return;
                            }
                        }
                        other => {
                            if !receive_grpc_client_message(&self.outgoing, &mut manager, other, &universe, self.listener.clone(), self.bbo_only, &mut last_l2, &mut last_bbo, &active_l2_params, &mut l2_param_guards).await {
                                return;
                            }
                        }
                    }
                }
            }
        }
    }
}

struct GrpcConnectionGuard {
    active_connections: Arc<AtomicU64>,
}

impl Drop for GrpcConnectionGuard {
    fn drop(&mut self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }
}

#[allow(clippy::too_many_arguments)]
async fn receive_grpc_client_message(
    outgoing: &mpsc::Sender<std::result::Result<pb::ServerMessage, Status>>,
    manager: &mut SubscriptionManager,
    client_message: ClientMessage,
    universe: &HashSet<String>,
    listener: Arc<Mutex<OrderBookListener>>,
    bbo_only: bool,
    last_l2: &mut HashMap<String, L2Entry>,
    last_bbo: &mut HashMap<String, BboEntry>,
    active_l2_params: &ActiveL2Params,
    l2_param_guards: &mut HashMap<L2SnapshotParams, L2ParamGuard>,
) -> bool {
    let subscription = match &client_message {
        ClientMessage::Unsubscribe { subscription } | ClientMessage::Subscribe { subscription } => subscription.clone(),
        ClientMessage::Ping => unreachable!("Ping is handled before receive_grpc_client_message"),
    };
    if bbo_only && !matches!(&subscription, Subscription::Bbo { .. }) {
        return send_grpc_message(
            outgoing,
            error_message("BBO-only mode: L2/L4/Trades subscriptions disabled. Only BBO subscriptions allowed."),
        )
        .await;
    }
    let sub = format!("{subscription:?}");
    if !subscription.validate(universe) {
        return send_grpc_message(outgoing, error_message(&format!("Invalid subscription: {sub}"))).await;
    }

    let (word, success) = match &client_message {
        ClientMessage::Subscribe { .. } => match manager.subscribe(subscription.clone()) {
            Ok(inserted) => {
                if inserted && let Subscription::L2Book { n_sig_figs, mantissa, .. } = &subscription {
                    let params = L2SnapshotParams::new(*n_sig_figs, *mantissa);
                    l2_param_guards.entry(params).or_insert_with(|| active_l2_params.acquire(params));
                }
                ("", inserted)
            }
            Err(err) => {
                return send_grpc_message(outgoing, error_message(&format!("Rejected subscription: {err}"))).await;
            }
        },
        ClientMessage::Unsubscribe { .. } => {
            let removed = manager.unsubscribe(subscription.clone());
            if removed {
                match &subscription {
                    Subscription::L2Book { coin, n_sig_figs, mantissa, n_levels } => {
                        last_l2.remove(&l2_cache_key(coin, *n_sig_figs, *mantissa, *n_levels));
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
            }
            ("un", removed)
        }
        ClientMessage::Ping => unreachable!(),
    };
    if success {
        let snapshot_msg = if let ClientMessage::Subscribe { subscription } = &client_message {
            let msg = immediate_snapshot(&subscription, listener).await;
            match msg {
                Ok(msg) => msg,
                Err(err) => {
                    manager.unsubscribe(subscription.clone());
                    return send_grpc_message(
                        outgoing,
                        error_message(&format!("Unable to grab order book snapshot: {err}")),
                    )
                    .await;
                }
            }
        } else {
            None
        };
        if !send_grpc_message(outgoing, subscription_response_message(&client_message)).await {
            return false;
        }
        if let Some(snapshot_msg) = snapshot_msg {
            return send_grpc_message(outgoing, snapshot_msg).await;
        }
        true
    } else {
        send_grpc_message(outgoing, error_message(&format!("Already {word}subscribed: {sub}"))).await
    }
}

async fn immediate_snapshot(
    subscription: &Subscription,
    listener: Arc<Mutex<OrderBookListener>>,
) -> Result<Option<pb::ServerMessage>> {
    if let Subscription::L4Book { coin } = subscription {
        let snapshot = listener.lock().await.compute_snapshot_for_coin(&Coin::new(coin));
        if let Some((time, height, coin_snapshot)) = snapshot {
            let [bids, asks] = coin_snapshot
                .as_ref()
                .clone()
                .map(|orders| orders.into_iter().map(|order| l4_order_to_proto(&L4Order::from(order))).collect());
            return Ok(Some(l4_book_message(pb::L4Book {
                payload: Some(pb::l4_book::Payload::Snapshot(pb::L4BookSnapshot {
                    coin: coin.clone(),
                    time,
                    height,
                    bids,
                    asks,
                })),
            })));
        }
        return Err("Snapshot Failed".into());
    }
    Ok(None)
}

async fn send_grpc_data_from_bbo(
    outgoing: &mpsc::Sender<std::result::Result<pb::ServerMessage, Status>>,
    coin: &str,
    bbos: &HashMap<Coin, CoinBbo>,
    time: u64,
    last_bbo: &mut HashMap<String, BboEntry>,
    store_payload: bool,
) -> bool {
    if let Some(cb) = bbos.get(coin) {
        let (best_bid, best_ask) = (&cb.raw.0, &cb.raw.1);
        let current: BboKey = (
            best_bid.as_ref().map(|(px, sz, _)| (px.value(), sz.value())),
            best_ask.as_ref().map(|(px, sz, _)| (px.value(), sz.value())),
        );

        if last_bbo.get(coin).map(|e| e.tuple) != Some(current) {
            let payload = pb::Bbo {
                coin: coin.to_string(),
                time,
                bid: best_bid.as_ref().map(|(px, sz, n)| pb::Level {
                    px: px.to_str(),
                    sz: sz.to_str(),
                    n: u64::from(*n),
                }),
                ask: best_ask.as_ref().map(|(px, sz, n)| pb::Level {
                    px: px.to_str(),
                    sz: sz.to_str(),
                    n: u64::from(*n),
                }),
            };

            BBO_CHANGES_TOTAL.with_label_values(&[coin]).inc();
            BROADCASTS_TOTAL.with_label_values(&["bbo"]).inc();
            last_bbo.insert(
                coin.to_string(),
                BboEntry { tuple: current, last_sent: Instant::now(), payload: store_payload.then(|| payload.clone()) },
            );
            return send_grpc_message(outgoing, bbo_message(payload)).await;
        }
    }
    true
}

#[allow(clippy::too_many_arguments)]
async fn send_grpc_data_from_snapshot(
    outgoing: &mpsc::Sender<std::result::Result<pb::ServerMessage, Status>>,
    subscription: &Subscription,
    snapshot: &HashMap<Coin, Arc<HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>>,
    time: u64,
    last_l2: &mut HashMap<String, L2Entry>,
    dirty: &HashSet<Coin>,
    force_full: bool,
    store_payload: bool,
) -> bool {
    if let Subscription::L2Book { coin, n_sig_figs, n_levels, mantissa } = subscription {
        let key = l2_cache_key(coin, *n_sig_figs, *mantissa, *n_levels);
        if !force_full && !dirty.contains(coin.as_str()) && last_l2.contains_key(&key) {
            return true;
        }

        let n_levels = n_levels.unwrap_or(DEFAULT_LEVELS);
        let variant = match snapshot.get(coin.as_str()) {
            Some(per_coin) => {
                let Some(variant) = per_coin.get(&L2SnapshotParams::new(*n_sig_figs, *mantissa)) else {
                    error!("Variant for coin {coin} not found");
                    return true;
                };
                Some(variant)
            }
            None => None,
        };
        let exported: [Vec<Level>; 2] =
            variant.map_or_else(|| [Vec::new(), Vec::new()], |v| v.truncate(n_levels).export_inner_snapshot());

        use std::hash::{Hash, Hasher};
        let mut hasher = rustc_hash::FxHasher::default();
        exported.hash(&mut hasher);
        let current_hash = hasher.finish();

        if last_l2.get(&key).map(|e| e.hash) != Some(current_hash) {
            let payload = pb::L2Book {
                coin: coin.clone(),
                time,
                n_sig_figs: *n_sig_figs,
                mantissa: *mantissa,
                n_levels: Some(n_levels as u64),
                bids: exported[0].iter().map(level_to_proto).collect(),
                asks: exported[1].iter().map(level_to_proto).collect(),
            };
            BROADCASTS_TOTAL.with_label_values(&["l2"]).inc();
            last_l2.insert(
                key,
                L2Entry {
                    hash: current_hash,
                    last_sent: Instant::now(),
                    payload: store_payload.then(|| payload.clone()),
                },
            );
            return send_grpc_message(outgoing, l2_book_message(payload)).await;
        }
    }
    true
}

async fn send_grpc_order_updates(
    outgoing: &mpsc::Sender<std::result::Result<pb::ServerMessage, Status>>,
    user: &str,
    time: u64,
    height: u64,
    statuses_by_coin: &HashMap<String, CoinStatuses>,
    user_addrs: &mut HashMap<String, alloy::primitives::Address>,
) -> bool {
    let user_addr = match user_addrs.get(user) {
        Some(addr) => *addr,
        None => match user.parse::<alloy::primitives::Address>() {
            Ok(addr) => {
                user_addrs.insert(user.to_string(), addr);
                addr
            }
            Err(_) => return true,
        },
    };

    let updates: Vec<pb::OrderUpdate> = statuses_by_coin
        .values()
        .flat_map(|cs| cs.statuses.iter())
        .filter(|status| status.user == user_addr)
        .map(|status| pb::OrderUpdate {
            user: status.user.to_string(),
            time,
            height,
            order_status: Some(order_status_to_proto(status)),
        })
        .collect();

    if !updates.is_empty() {
        return send_grpc_message(outgoing, order_updates_message(updates)).await;
    }
    true
}

async fn send_grpc_message(
    outgoing: &mpsc::Sender<std::result::Result<pb::ServerMessage, Status>>,
    message: pb::ServerMessage,
) -> bool {
    match outgoing.send(Ok(message)).await {
        Ok(()) => {
            MESSAGES_SENT_TOTAL.inc();
            true
        }
        Err(err) => {
            error!("Failed to send gRPC stream message: {err}");
            WS_SEND_ERRORS_TOTAL.inc();
            false
        }
    }
}

fn client_message_from_proto(message: pb::ClientMessage) -> std::result::Result<ClientMessage, String> {
    match message.message.ok_or("missing client message oneof")? {
        pb::client_message::Message::Subscribe(sub) => {
            Ok(ClientMessage::Subscribe { subscription: subscription_from_proto(sub)? })
        }
        pb::client_message::Message::Unsubscribe(sub) => {
            Ok(ClientMessage::Unsubscribe { subscription: subscription_from_proto(sub)? })
        }
        pb::client_message::Message::Ping(_) => Ok(ClientMessage::Ping),
    }
}

fn client_message_to_proto(message: &ClientMessage) -> pb::ClientMessage {
    match message {
        ClientMessage::Subscribe { subscription } => pb::ClientMessage {
            message: Some(pb::client_message::Message::Subscribe(subscription_to_proto(subscription))),
        },
        ClientMessage::Unsubscribe { subscription } => pb::ClientMessage {
            message: Some(pb::client_message::Message::Unsubscribe(subscription_to_proto(subscription))),
        },
        ClientMessage::Ping => pb::ClientMessage { message: Some(pb::client_message::Message::Ping(pb::Ping {})) },
    }
}

fn subscription_from_proto(subscription: pb::Subscription) -> std::result::Result<Subscription, String> {
    match subscription.subscription.ok_or("missing subscription oneof")? {
        pb::subscription::Subscription::Trades(s) => Ok(Subscription::Trades { coin: s.coin }),
        pb::subscription::Subscription::L2Book(s) => {
            let n_levels = match s.n_levels {
                Some(value) if value > MAX_LEVELS as u64 => {
                    return Err("n_levels exceeds usize/max level range".to_string());
                }
                Some(value) => Some(value as usize),
                None => None,
            };
            Ok(Subscription::L2Book { coin: s.coin, n_sig_figs: s.n_sig_figs, n_levels, mantissa: s.mantissa })
        }
        pb::subscription::Subscription::L4Book(s) => Ok(Subscription::L4Book { coin: s.coin }),
        pb::subscription::Subscription::Bbo(s) => Ok(Subscription::Bbo { coin: s.coin }),
        pb::subscription::Subscription::OrderUpdates(s) => Ok(Subscription::OrderUpdates { user: s.user }),
        pb::subscription::Subscription::BookDiffs(s) => Ok(Subscription::BookDiffs { coin: s.coin }),
    }
}

fn subscription_to_proto(subscription: &Subscription) -> pb::Subscription {
    let subscription = match subscription {
        Subscription::Trades { coin } => {
            pb::subscription::Subscription::Trades(pb::CoinSubscription { coin: coin.clone() })
        }
        Subscription::L2Book { coin, n_sig_figs, n_levels, mantissa } => {
            pb::subscription::Subscription::L2Book(pb::L2BookSubscription {
                coin: coin.clone(),
                n_sig_figs: *n_sig_figs,
                mantissa: *mantissa,
                n_levels: n_levels.map(|v| v as u64),
            })
        }
        Subscription::L4Book { coin } => {
            pb::subscription::Subscription::L4Book(pb::CoinSubscription { coin: coin.clone() })
        }
        Subscription::Bbo { coin } => pb::subscription::Subscription::Bbo(pb::CoinSubscription { coin: coin.clone() }),
        Subscription::OrderUpdates { user } => {
            pb::subscription::Subscription::OrderUpdates(pb::UserSubscription { user: user.clone() })
        }
        Subscription::BookDiffs { coin } => {
            pb::subscription::Subscription::BookDiffs(pb::CoinSubscription { coin: coin.clone() })
        }
    };
    pb::Subscription { subscription: Some(subscription) }
}

fn side_to_proto(side: Side) -> i32 {
    match side {
        Side::Ask => pb::Side::Ask as i32,
        Side::Bid => pb::Side::Bid as i32,
    }
}

fn level_to_proto(level: &Level) -> pb::Level {
    pb::Level { px: level.px().to_string(), sz: level.sz().to_string(), n: level.n() as u64 }
}

fn l4_order_to_proto(order: &L4Order) -> pb::L4Order {
    pb::L4Order {
        user: order.user.map(|user| user.to_string()),
        coin: order.coin.clone(),
        side: side_to_proto(order.side),
        limit_px: order.limit_px.clone(),
        sz: order.sz.clone(),
        oid: order.oid,
        timestamp: order.timestamp,
        trigger_condition: order.trigger_condition.clone(),
        is_trigger: order.is_trigger,
        trigger_px: order.trigger_px.clone(),
        children_json: order.children.iter().map(serde_json::Value::to_string).collect(),
        is_position_tpsl: order.is_position_tpsl,
        reduce_only: order.reduce_only,
        order_type: order.order_type.clone(),
        orig_sz: order.orig_sz.clone(),
        tif: order.tif.clone(),
        cloid: order.cloid.clone(),
    }
}

fn order_status_to_proto(status: &NodeDataOrderStatus) -> pb::OrderStatus {
    pb::OrderStatus {
        time: status.time.to_string(),
        user: status.user.to_string(),
        hash: status.hash.clone(),
        builder_json: status.builder.as_ref().map(serde_json::Value::to_string),
        status: status.status.clone(),
        order: Some(l4_order_to_proto(&status.order)),
    }
}

fn order_diff_event_to_proto(diff: &NodeDataOrderDiff) -> pb::OrderDiffEvent {
    pb::OrderDiffEvent {
        user: diff.user().to_string(),
        oid: diff.oid_value(),
        px: diff.px().to_string(),
        coin: diff.coin_str().to_string(),
        raw_book_diff: Some(order_diff_to_proto(diff.diff())),
    }
}

fn order_diff_to_proto(diff: &OrderDiff) -> pb::OrderDiff {
    let diff = match diff {
        OrderDiff::New { sz } => pb::order_diff::Diff::New(pb::NewOrderDiff { sz: sz.clone() }),
        OrderDiff::Update { orig_sz, new_sz } => {
            pb::order_diff::Diff::Update(pb::UpdateOrderDiff { orig_sz: orig_sz.clone(), new_sz: new_sz.clone() })
        }
        OrderDiff::Remove => pb::order_diff::Diff::Remove(pb::RemoveOrderDiff {}),
    };
    pb::OrderDiff { diff: Some(diff) }
}

fn trade_to_proto(trade: &Trade) -> pb::Trade {
    pb::Trade {
        coin: trade.coin().to_string(),
        side: side_to_proto(trade.side()),
        px: trade.px().to_string(),
        sz: trade.sz().to_string(),
        hash: trade.hash().to_string(),
        time: trade.time(),
        tid: trade.tid(),
        users: trade.users().into_iter().map(|user| user.to_string()).collect(),
    }
}

fn subscription_response_message(message: &ClientMessage) -> pb::ServerMessage {
    pb::ServerMessage {
        message: Some(pb::server_message::Message::SubscriptionResponse(client_message_to_proto(message))),
    }
}

fn l2_book_message(book: pb::L2Book) -> pb::ServerMessage {
    pb::ServerMessage { message: Some(pb::server_message::Message::L2Book(book)) }
}

fn l4_book_message(book: pb::L4Book) -> pb::ServerMessage {
    pb::ServerMessage { message: Some(pb::server_message::Message::L4Book(book)) }
}

fn l4_updates_message(
    time: u64,
    height: u64,
    statuses: &[NodeDataOrderStatus],
    diffs: &[NodeDataOrderDiff],
) -> pb::ServerMessage {
    l4_book_message(pb::L4Book {
        payload: Some(pb::l4_book::Payload::Updates(pb::L4BookUpdates {
            time,
            height,
            order_statuses: statuses.iter().map(order_status_to_proto).collect(),
            book_diffs: diffs.iter().map(order_diff_event_to_proto).collect(),
        })),
    })
}

fn trades_message(trades: &[Trade]) -> pb::ServerMessage {
    pb::ServerMessage {
        message: Some(pb::server_message::Message::Trades(pb::Trades {
            trades: trades.iter().map(trade_to_proto).collect(),
        })),
    }
}

fn bbo_message(bbo: pb::Bbo) -> pb::ServerMessage {
    pb::ServerMessage { message: Some(pb::server_message::Message::Bbo(bbo)) }
}

fn book_diffs_message(diffs: &[NodeDataOrderDiff]) -> pb::ServerMessage {
    pb::ServerMessage {
        message: Some(pb::server_message::Message::BookDiffs(pb::BookDiffs {
            book_diffs: diffs.iter().map(order_diff_event_to_proto).collect(),
        })),
    }
}

fn order_updates_message(updates: Vec<pb::OrderUpdate>) -> pb::ServerMessage {
    pb::ServerMessage {
        message: Some(pb::server_message::Message::OrderUpdates(pb::OrderUpdates { order_updates: updates })),
    }
}

fn pong_message() -> pb::ServerMessage {
    pb::ServerMessage { message: Some(pb::server_message::Message::Pong(pb::Pong {})) }
}

fn error_message(message: &str) -> pb::ServerMessage {
    pb::ServerMessage { message: Some(pb::server_message::Message::Error(pb::Error { message: message.to_string() })) }
}
