use crate::orderbook as pb;
use log::{error, info};
use prost::Message as _;
use server::{
    Result, ServerConfig,
    metrics::{
        BBO_CHANGES_TOTAL, BROADCASTS_TOTAL, CHANNEL_DROPS_TOTAL, CHANNEL_LAG, LISTENER_LOCK_HOLD_LATENCY,
        LISTENER_LOCK_WAIT_LATENCY, MESSAGES_SENT_TOTAL, ORDERBOOK_HEIGHT, PayloadTimestampKind,
        TRANSPORT_CHANNEL_MESSAGES_SENT_TOTAL, TRANSPORT_CONNECTIONS_ACTIVE, TRANSPORT_CONNECTIONS_TOTAL,
        TRANSPORT_FANOUT_ACTIVE_SUBSCRIPTIONS, TRANSPORT_FANOUT_LATENCY, TRANSPORT_FANOUT_SUBSCRIPTIONS,
        TRANSPORT_MESSAGES_SENT_TOTAL, TRANSPORT_MESSAGES_SKIPPED_TOTAL, TRANSPORT_OUTGOING_QUEUE_DEPTH,
        TRANSPORT_PAYLOAD_BUILD_LATENCY, TRANSPORT_PAYLOAD_BYTES, TRANSPORT_PAYLOAD_CACHE_TOTAL,
        TRANSPORT_SEND_ERRORS_TOTAL, TRANSPORT_SEND_LATENCY, WS_SEND_ERRORS_TOTAL, observe_transport_event_egress_age,
        observe_transport_payload_egress_age,
    },
    transport::{
        ActiveL2Params, ActiveSubscriptionInterests, ClientMessage, Coin, CoinBbo, DEFAULT_LEVELS, InnerLevel,
        InternalMessage, L2BuiltFrame, L2FrameCache, L2ParamGuard, L2SnapshotParams, L4Order, Level, MAX_LEVELS,
        NodeDataOrderDiff, NodeDataOrderStatus, OrderBookListener, OrderBookRuntime, OrderDiff, Side, Snapshot,
        Subscription, SubscriptionInterest, SubscriptionInterestGuard, SubscriptionKind, SubscriptionManager, Trade,
        TransportKind, UserStatuses,
    },
};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    hash::{Hash, Hasher},
    pin::Pin,
    sync::{
        Arc, LazyLock, Mutex as StdMutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    select,
    sync::{Mutex, broadcast::Sender, mpsc},
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

impl BboEntry {
    fn upsert(cache: &mut HashMap<String, Self>, coin: &str, tuple: BboKey, payload: Option<pb::Bbo>) {
        let entry = Self { tuple, last_sent: Instant::now(), payload };
        if let Some(slot) = cache.get_mut(coin) {
            *slot = entry;
        } else {
            cache.insert(coin.to_string(), entry);
        }
    }
}

const GRPC_PAYLOAD_CACHE_CAP: usize = 512;
static GRPC_PAYLOAD_CACHE: LazyLock<StdMutex<GrpcPayloadCache>> =
    LazyLock::new(|| StdMutex::new(GrpcPayloadCache::with_capacity(GRPC_PAYLOAD_CACHE_CAP)));

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct GrpcPayloadCacheKey {
    source: usize,
    channel: SubscriptionKind,
    a: u64,
    b: u64,
}

struct GrpcPayloadCache {
    entries: HashMap<GrpcPayloadCacheKey, pb::ServerMessage>,
    insertion_order: VecDeque<GrpcPayloadCacheKey>,
    cap: usize,
}

impl GrpcPayloadCache {
    fn with_capacity(cap: usize) -> Self {
        Self { entries: HashMap::with_capacity(cap), insertion_order: VecDeque::with_capacity(cap), cap }
    }

    fn get(&self, key: &GrpcPayloadCacheKey) -> Option<pb::ServerMessage> {
        self.entries.get(key).cloned()
    }

    fn insert(&mut self, key: GrpcPayloadCacheKey, message: pb::ServerMessage) {
        if self.entries.contains_key(&key) {
            return;
        }
        self.entries.insert(key, message);
        self.insertion_order.push_back(key);
        while self.entries.len() > self.cap {
            if let Some(old_key) = self.insertion_order.pop_front() {
                self.entries.remove(&old_key);
            } else {
                break;
            }
        }
    }
}

fn grpc_payload_key<T>(source: *const T, channel: SubscriptionKind, a: u64, b: u64) -> GrpcPayloadCacheKey {
    GrpcPayloadCacheKey { source: source.cast::<()>() as usize, channel, a, b }
}

fn hash_value<T: Hash>(value: &T) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
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
    let runtime = OrderBookRuntime::spawn(&config);
    run_grpc_transport(config, runtime).await
}

pub async fn run_grpc_transport(config: ServerConfig, runtime: OrderBookRuntime) -> Result<()> {
    let addr = config.address.parse()?;
    let service = GrpcOrderbookService {
        internal_message_tx: runtime.internal_message_tx(),
        listener: runtime.listener(),
        bbo_only: runtime.bbo_only(),
        l2book_heartbeat_ms: runtime.l2book_heartbeat_ms(),
        bbo_heartbeat_ms: runtime.bbo_heartbeat_ms(),
        start_time: runtime.start_time(),
        active_connections: Arc::new(AtomicU64::new(0)),
    };

    info!("gRPC server running at http://{}", config.address);

    let orderbook_service = pb::orderbook_server::OrderbookServer::new(service);
    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter.set_serving::<pb::orderbook_server::OrderbookServer<GrpcOrderbookService>>().await;

    Server::builder().tcp_nodelay(true).add_service(health_service).add_service(orderbook_service).serve(addr).await?;
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
        TRANSPORT_CONNECTIONS_ACTIVE.with_label_values(&["grpc"]).inc();
        TRANSPORT_CONNECTIONS_TOTAL.with_label_values(&["grpc"]).inc();

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
        let wait_start = Instant::now();
        let guard = self.listener.lock().await;
        LISTENER_LOCK_WAIT_LATENCY.with_label_values(&["grpc_health"]).observe(wait_start.elapsed().as_secs_f64());
        let hold_start = Instant::now();
        let is_ready = guard.is_ready();
        LISTENER_LOCK_HOLD_LATENCY.with_label_values(&["grpc_health"]).observe(hold_start.elapsed().as_secs_f64());
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
        let wait_start = Instant::now();
        let listener_guard = self.listener.lock().await;
        LISTENER_LOCK_WAIT_LATENCY.with_label_values(&["grpc_setup"]).observe(wait_start.elapsed().as_secs_f64());
        let hold_start = Instant::now();
        let is_ready = listener_guard.is_ready();
        let mut universe = listener_guard.universe();
        let active_l2_params = listener_guard.active_l2_params();
        let active_subscription_interests = listener_guard.active_subscription_interests();
        LISTENER_LOCK_HOLD_LATENCY.with_label_values(&["grpc_setup"]).observe(hold_start.elapsed().as_secs_f64());
        drop(listener_guard);
        if !is_ready {
            let _ = send_grpc_message(
                &self.outgoing,
                error_message("Order book not ready for streaming (waiting for snapshot)"),
            )
            .await;
            return;
        }

        let mut internal_message_rx = self.internal_message_tx.subscribe();
        let mut manager = SubscriptionManager::new(TransportKind::Grpc);
        let mut last_l2: HashMap<Subscription, L2Entry> = HashMap::new();
        let mut uncached_l2: HashSet<Subscription> = HashSet::new();
        let mut last_bbo: HashMap<String, BboEntry> = HashMap::new();
        let mut l2_param_guards: HashMap<L2SnapshotParams, L2ParamGuard> = HashMap::new();
        let mut subscription_interest_guards: HashMap<Subscription, SubscriptionInterestGuard> = HashMap::new();
        let mut heartbeat_ticker = build_heartbeat_ticker(self.l2book_heartbeat_ms, self.bbo_heartbeat_ms);
        let l2_hb =
            if self.l2book_heartbeat_ms > 0 { Some(Duration::from_millis(self.l2book_heartbeat_ms)) } else { None };
        let bbo_hb = if self.bbo_heartbeat_ms > 0 { Some(Duration::from_millis(self.bbo_heartbeat_ms)) } else { None };
        let mut force_full_l2 = false;

        loop {
            select! {
                recv_result = internal_message_rx.recv() => {
                    let msg = match recv_result {
                        Ok(msg) => msg,
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            CHANNEL_LAG.with_label_values(&["grpc"]).set(n as i64);
                            CHANNEL_DROPS_TOTAL.with_label_values(&["grpc"]).inc();
                            // A dropped Snapshot may have carried dirty coins
                            // this connection never saw; process the next
                            // snapshot in full and let hash dedup suppress
                            // unchanged sends.
                            force_full_l2 = true;
                            log::debug!("gRPC receiver lagged: {n} messages");
                            continue;
                        }
                        Err(err) => {
                            error!("gRPC internal receiver error: {err}");
                            return;
                        }
                    };
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
                                    if !send_grpc_data_from_snapshot(&self.outgoing, sub, l2_snapshots.as_ref(), *time, &mut last_l2, dirty, force_full_l2, l2_hb.is_some(), l2_frames).await {
                                        return;
                                    }
                                    if last_l2.contains_key(sub) {
                                        uncached_l2.remove(sub);
                                    }
                                }
                            } else {
                                for coin in dirty {
                                    for sub in manager.subscriptions_for_coin(SubscriptionKind::L2Book, coin.as_str()) {
                                        fanout_subscriptions += 1;
                                        if !send_grpc_data_from_snapshot(&self.outgoing, sub, l2_snapshots.as_ref(), *time, &mut last_l2, dirty, force_full_l2, l2_hb.is_some(), l2_frames).await {
                                            return;
                                        }
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
                                    if !send_grpc_data_from_snapshot(&self.outgoing, &sub, l2_snapshots.as_ref(), *time, &mut last_l2, dirty, force_full_l2, l2_hb.is_some(), l2_frames).await {
                                        return;
                                    }
                                    if last_l2.contains_key(&sub) {
                                        uncached_l2.remove(&sub);
                                    }
                                }
                            }
                            force_full_l2 = false;
                        }
                        InternalMessage::BboUpdate{ bbos, time } => {
                            for (coin, bbo) in bbos.iter() {
                                let coin = coin.as_str();
                                for _sub in manager.subscriptions_for_coin(SubscriptionKind::Bbo, coin) {
                                    fanout_subscriptions += 1;
                                    if !send_grpc_data_from_bbo(&self.outgoing, coin, bbo, *time, &mut last_bbo, bbo_hb.is_some()).await {
                                        return;
                                    }
                                }
                            }
                        }
                        InternalMessage::Fills{ trades_by_coin } => {
                            for (coin, ct) in trades_by_coin.iter() {
                                for _sub in manager.subscriptions_for_coin(SubscriptionKind::Trades, coin) {
                                    fanout_subscriptions += 1;
                                    BROADCASTS_TOTAL.with_label_values(&["trades"]).inc();
                                    if let Some(event_time_ms) = Trade::latest_time(ct.trades.as_ref()) {
                                        observe_transport_event_egress_age(TransportKind::Grpc, SubscriptionKind::Trades, event_time_ms);
                                        observe_transport_payload_egress_age(TransportKind::Grpc, SubscriptionKind::Trades, PayloadTimestampKind::TradeTime,
                                            event_time_ms,
                                        );
                                    }
                                    let cache_key = grpc_payload_key(
                                        Arc::as_ptr(&ct.trades),
                                        SubscriptionKind::Trades,
                                        ct.trades.len() as u64,
                                        Trade::latest_time(ct.trades.as_ref()).unwrap_or(0),
                                    );
                                    if !send_grpc_message(&self.outgoing, cached_grpc_message(cache_key, SubscriptionKind::Trades, || trades_message(ct.trades.as_ref()))).await {
                                        return;
                                    }
                                }
                            }
                        }
                        InternalMessage::L4OrderDiffs{ time, height, diffs_by_coin } => {
                            for (coin, cd) in diffs_by_coin.iter() {
                                for _sub in manager.subscriptions_for_coin(SubscriptionKind::BookDiffs, coin) {
                                    fanout_subscriptions += 1;
                                    BROADCASTS_TOTAL.with_label_values(&["bookDiffs"]).inc();
                                    observe_transport_event_egress_age(TransportKind::Grpc, SubscriptionKind::BookDiffs, *time);
                                    let cache_key = grpc_payload_key(
                                        Arc::as_ptr(&cd.diffs),
                                        SubscriptionKind::BookDiffs,
                                        *time,
                                        *height ^ cd.diffs.len() as u64,
                                    );
                                    if !send_grpc_message(&self.outgoing, cached_grpc_message(cache_key, SubscriptionKind::BookDiffs, || book_diffs_message(cd.diffs.as_ref()))).await {
                                        return;
                                    }
                                }
                                for _sub in manager.subscriptions_for_coin(SubscriptionKind::L4Book, coin) {
                                    fanout_subscriptions += 1;
                                    BROADCASTS_TOTAL.with_label_values(&["l4"]).inc();
                                    observe_transport_event_egress_age(TransportKind::Grpc, SubscriptionKind::L4Book, *time);
                                    let cache_key = grpc_payload_key(
                                        Arc::as_ptr(&cd.diffs),
                                        SubscriptionKind::L4Book,
                                        *time,
                                        *height ^ cd.diffs.len() as u64,
                                    );
                                    if !send_grpc_message(&self.outgoing, cached_grpc_message(cache_key, SubscriptionKind::L4Book, || l4_updates_message(*time, *height, &[], cd.diffs.as_ref()))).await {
                                        return;
                                    }
                                }
                            }
                        }
                        InternalMessage::L4OrderStatuses{ time, height, statuses_by_coin, statuses_by_user } => {
                            for (coin, cs) in statuses_by_coin.iter() {
                                for _sub in manager.subscriptions_for_coin(SubscriptionKind::L4Book, coin) {
                                    fanout_subscriptions += 1;
                                    BROADCASTS_TOTAL.with_label_values(&["l4"]).inc();
                                    observe_transport_event_egress_age(TransportKind::Grpc, SubscriptionKind::L4Book, *time);
                                    let cache_key = grpc_payload_key(
                                        Arc::as_ptr(&cs.statuses),
                                        SubscriptionKind::L4Book,
                                        *time,
                                        *height ^ cs.statuses.len() as u64,
                                    );
                                    cs.timestamps.observe_transport_egress_age(TransportKind::Grpc, SubscriptionKind::L4Book);
                                    if !send_grpc_message(&self.outgoing, cached_grpc_message(cache_key, SubscriptionKind::L4Book, || l4_updates_message(*time, *height, cs.statuses.as_ref(), &[]))).await {
                                        return;
                                    }
                                }
                            }
                            for (user, statuses) in statuses_by_user.iter() {
                                for _sub in manager.order_update_subscriptions_for_user(*user) {
                                    fanout_subscriptions += 1;
                                    if !send_grpc_order_updates(&self.outgoing, statuses, *time, *height).await {
                                        return;
                                    }
                                }
                            }
                        }
                    }
                    TRANSPORT_FANOUT_SUBSCRIPTIONS
                        .with_label_values(&["grpc", channel_label])
                        .observe(fanout_subscriptions as f64);
                    TRANSPORT_FANOUT_ACTIVE_SUBSCRIPTIONS
                        .with_label_values(&["grpc", channel_label])
                        .observe(active_fanout_subscriptions as f64);
                    TRANSPORT_FANOUT_LATENCY
                        .with_label_values(&["grpc", channel_label])
                        .observe(fanout_start.elapsed().as_secs_f64());
                }
                _ = heartbeat_tick(&mut heartbeat_ticker) => {
                    let now = Instant::now();
                    let now_ms = chrono::Utc::now().timestamp_millis().max(0) as u64;
                    for sub in manager.subscriptions_for_type(SubscriptionKind::L2Book) {
                        if let Subscription::L2Book { .. } = sub {
                            let Some(hb) = l2_hb else { continue };
                            if let Some(entry) = last_l2.get_mut(sub)
                                && now.duration_since(entry.last_sent) >= hb
                                && let Some(payload) = entry.payload.as_mut() {
                                payload.time = now_ms;
                                entry.last_sent = now;
                                BROADCASTS_TOTAL.with_label_values(&["l2_heartbeat"]).inc();
                                if !send_grpc_message(&self.outgoing, build_grpc_message(SubscriptionKind::L2Book, || l2_book_message(payload.clone()))).await {
                                    return;
                                }
                            }
                        }
                    }
                    for sub in manager.subscriptions_for_type(SubscriptionKind::Bbo) {
                        if let Subscription::Bbo { coin } = sub {
                            let Some(hb) = bbo_hb else { continue };
                            if let Some(entry) = last_bbo.get_mut(coin)
                                && now.duration_since(entry.last_sent) >= hb
                                && let Some(payload) = entry.payload.as_mut() {
                                payload.time = now_ms;
                                entry.last_sent = now;
                                BROADCASTS_TOTAL.with_label_values(&["bbo_heartbeat"]).inc();
                                if !send_grpc_message(&self.outgoing, build_grpc_message(SubscriptionKind::Bbo, || bbo_message(payload.clone()))).await {
                                    return;
                                }
                            }
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
                            if !receive_grpc_client_message(
                                &self.outgoing,
                                &mut manager,
                                other,
                                &universe,
                                self.listener.clone(),
                                self.bbo_only,
                                &mut last_l2,
                                &mut uncached_l2,
                                &mut last_bbo,
                                &active_l2_params,
                                &mut l2_param_guards,
                                &active_subscription_interests,
                                &mut subscription_interest_guards,
                            )
                            .await
                            {
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
        TRANSPORT_CONNECTIONS_ACTIVE.with_label_values(&["grpc"]).dec();
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
                return send_grpc_message(outgoing, error_message(&format!("Rejected subscription: {err}"))).await;
            }
        },
        ClientMessage::Unsubscribe { .. } => {
            let removed = manager.unsubscribe(subscription.clone());
            if removed {
                match &subscription {
                    Subscription::L2Book { n_sig_figs, mantissa, .. } => {
                        last_l2.remove(&subscription);
                        uncached_l2.remove(&subscription);
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
            let msg = immediate_snapshot(&subscription, listener).await;
            match msg {
                Ok(msg) => msg,
                Err(err) => {
                    manager.unsubscribe(subscription.clone());
                    subscription_interest_guards.remove(subscription);
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

async fn immediate_snapshot(
    subscription: &Subscription,
    listener: Arc<Mutex<OrderBookListener>>,
) -> Result<Option<pb::ServerMessage>> {
    if let Subscription::L4Book { coin } = subscription {
        let wait_start = Instant::now();
        let guard = listener.lock().await;
        LISTENER_LOCK_WAIT_LATENCY.with_label_values(&["grpc_l4_snapshot"]).observe(wait_start.elapsed().as_secs_f64());
        let hold_start = Instant::now();
        let snapshot = guard.compute_snapshot_for_coin(&Coin::new(coin));
        LISTENER_LOCK_HOLD_LATENCY.with_label_values(&["grpc_l4_snapshot"]).observe(hold_start.elapsed().as_secs_f64());
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
    cb: &CoinBbo,
    time: u64,
    last_bbo: &mut HashMap<String, BboEntry>,
    store_payload: bool,
) -> bool {
    let (best_bid, best_ask) = (&cb.raw.0, &cb.raw.1);
    let current: BboKey = (
        best_bid.as_ref().map(|(px, sz, _)| (px.value(), sz.value())),
        best_ask.as_ref().map(|(px, sz, _)| (px.value(), sz.value())),
    );

    if last_bbo.get(coin).map(|e| e.tuple) != Some(current) {
        BBO_CHANGES_TOTAL.with_label_values(&[coin]).inc();
        BROADCASTS_TOTAL.with_label_values(&["bbo"]).inc();
        observe_transport_event_egress_age(TransportKind::Grpc, SubscriptionKind::Bbo, time);
        let cache_key = grpc_payload_key(cb, SubscriptionKind::Bbo, time, hash_value(&current));
        let message = cached_grpc_message(cache_key, SubscriptionKind::Bbo, || {
            bbo_message(pb::Bbo {
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
            })
        });
        let payload = store_payload.then(|| bbo_payload_from_message(&message)).flatten();
        BboEntry::upsert(last_bbo, coin, current, payload);
        return send_grpc_message(outgoing, message).await;
    }
    TRANSPORT_MESSAGES_SKIPPED_TOTAL.with_label_values(&["grpc", "bbo", "unchanged"]).inc();
    true
}

#[allow(clippy::too_many_arguments)]
async fn send_grpc_data_from_snapshot(
    outgoing: &mpsc::Sender<std::result::Result<pb::ServerMessage, Status>>,
    subscription: &Subscription,
    snapshot: &HashMap<Coin, Arc<HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>>,
    time: u64,
    last_l2: &mut HashMap<Subscription, L2Entry>,
    dirty: &HashSet<Coin>,
    force_full: bool,
    store_payload: bool,
    l2_frames: &L2FrameCache,
) -> bool {
    if let Subscription::L2Book { coin, n_sig_figs, n_levels, mantissa } = subscription {
        if !force_full && !dirty.contains(coin.as_str()) && last_l2.contains_key(subscription) {
            TRANSPORT_MESSAGES_SKIPPED_TOTAL.with_label_values(&["grpc", "l2Book", "not_dirty"]).inc();
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
        let built = l2_frames.get_or_build(coin, *n_sig_figs, *mantissa, n_levels, time, variant);
        let current_hash = built.hash();

        if last_l2.get(subscription).map(|e| e.hash) != Some(current_hash) {
            BROADCASTS_TOTAL.with_label_values(&["l2"]).inc();
            observe_transport_event_egress_age(TransportKind::Grpc, SubscriptionKind::L2Book, time);
            let cache_key = grpc_payload_key(Arc::as_ptr(&built), SubscriptionKind::L2Book, time, current_hash);
            let message = cached_grpc_message(cache_key, SubscriptionKind::L2Book, || {
                l2_book_message(l2_book_from_rendered(&built))
            });
            let payload = store_payload.then(|| l2_payload_from_message(&message)).flatten();
            last_l2.insert(subscription.clone(), L2Entry { hash: current_hash, last_sent: Instant::now(), payload });
            return send_grpc_message(outgoing, message).await;
        }
        TRANSPORT_MESSAGES_SKIPPED_TOTAL.with_label_values(&["grpc", "l2Book", "unchanged"]).inc();
    }
    true
}

async fn send_grpc_order_updates(
    outgoing: &mpsc::Sender<std::result::Result<pb::ServerMessage, Status>>,
    statuses: &UserStatuses,
    time: u64,
    height: u64,
) -> bool {
    if !statuses.statuses.is_empty() {
        observe_transport_event_egress_age(TransportKind::Grpc, SubscriptionKind::OrderUpdates, time);
        statuses.timestamps.observe_transport_egress_age(TransportKind::Grpc, SubscriptionKind::OrderUpdates);
        let cache_key = grpc_payload_key(
            Arc::as_ptr(&statuses.statuses),
            SubscriptionKind::OrderUpdates,
            time,
            height ^ statuses.statuses.len() as u64,
        );
        let message = cached_grpc_message(cache_key, SubscriptionKind::OrderUpdates, || {
            let updates: Vec<pb::OrderUpdate> = statuses
                .statuses
                .iter()
                .map(|status| pb::OrderUpdate {
                    user: status.user.to_string(),
                    time,
                    height,
                    order_status: Some(order_status_to_proto(status)),
                })
                .collect();
            order_updates_message(updates)
        });
        return send_grpc_message(outgoing, message).await;
    }
    true
}

async fn send_grpc_message(
    outgoing: &mpsc::Sender<std::result::Result<pb::ServerMessage, Status>>,
    message: pb::ServerMessage,
) -> bool {
    let channel = server_message_channel(&message);
    let send_start = Instant::now();
    let queued = outgoing.max_capacity().saturating_sub(outgoing.capacity());
    TRANSPORT_OUTGOING_QUEUE_DEPTH.with_label_values(&["grpc"]).observe(queued as f64);
    match outgoing.send(Ok(message)).await {
        Ok(()) => {
            TRANSPORT_SEND_LATENCY.with_label_values(&["grpc"]).observe(send_start.elapsed().as_secs_f64());
            MESSAGES_SENT_TOTAL.inc();
            TRANSPORT_MESSAGES_SENT_TOTAL.with_label_values(&["grpc"]).inc();
            TRANSPORT_CHANNEL_MESSAGES_SENT_TOTAL.with_label_values(&["grpc", channel]).inc();
            true
        }
        Err(err) => {
            TRANSPORT_SEND_LATENCY.with_label_values(&["grpc"]).observe(send_start.elapsed().as_secs_f64());
            error!("Failed to send gRPC stream message: {err}");
            WS_SEND_ERRORS_TOTAL.inc();
            TRANSPORT_SEND_ERRORS_TOTAL.with_label_values(&["grpc", "closed"]).inc();
            false
        }
    }
}

fn server_message_channel(message: &pb::ServerMessage) -> &'static str {
    match message.message.as_ref() {
        Some(pb::server_message::Message::SubscriptionResponse(_)) => "subscriptionResponse",
        Some(pb::server_message::Message::L2Book(_)) => "l2Book",
        Some(pb::server_message::Message::L4Book(_)) => "l4Book",
        Some(pb::server_message::Message::Trades(_)) => "trades",
        Some(pb::server_message::Message::Bbo(_)) => "bbo",
        Some(pb::server_message::Message::BookDiffs(_)) => "bookDiffs",
        Some(pb::server_message::Message::OrderUpdates(_)) => "orderUpdates",
        Some(pb::server_message::Message::Pong(_)) => "pong",
        Some(pb::server_message::Message::Error(_)) => "error",
        None => "unknown",
    }
}

fn build_grpc_message(channel: SubscriptionKind, build: impl FnOnce() -> pb::ServerMessage) -> pb::ServerMessage {
    let build_start = Instant::now();
    finish_grpc_message(channel, build_start, build())
}

fn cached_grpc_message(
    key: GrpcPayloadCacheKey,
    channel: SubscriptionKind,
    build: impl FnOnce() -> pb::ServerMessage,
) -> pb::ServerMessage {
    if let Ok(cache) = GRPC_PAYLOAD_CACHE.lock()
        && let Some(message) = cache.get(&key)
    {
        TRANSPORT_PAYLOAD_CACHE_TOTAL.with_label_values(&[TransportKind::Grpc.label(), channel.label(), "hit"]).inc();
        TRANSPORT_PAYLOAD_BYTES
            .with_label_values(&[TransportKind::Grpc.label(), channel.label()])
            .observe(message.encoded_len() as f64);
        return message;
    }

    TRANSPORT_PAYLOAD_CACHE_TOTAL.with_label_values(&[TransportKind::Grpc.label(), channel.label(), "miss"]).inc();
    let message = build_grpc_message(channel, build);
    if let Ok(mut cache) = GRPC_PAYLOAD_CACHE.lock() {
        cache.insert(key, message.clone());
    }
    message
}

fn finish_grpc_message(
    channel: SubscriptionKind,
    build_start: Instant,
    message: pb::ServerMessage,
) -> pb::ServerMessage {
    TRANSPORT_PAYLOAD_BUILD_LATENCY
        .with_label_values(&[TransportKind::Grpc.label(), channel.label()])
        .observe(build_start.elapsed().as_secs_f64());
    TRANSPORT_PAYLOAD_BYTES
        .with_label_values(&[TransportKind::Grpc.label(), channel.label()])
        .observe(message.encoded_len() as f64);
    message
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

fn l2_book_from_rendered(rendered: &L2BuiltFrame) -> pb::L2Book {
    pb::L2Book {
        coin: rendered.coin().to_string(),
        time: rendered.time(),
        n_sig_figs: rendered.n_sig_figs(),
        mantissa: rendered.mantissa(),
        n_levels: rendered.n_levels().map(|n| n as u64),
        bids: rendered.levels()[0].iter().map(level_to_proto).collect(),
        asks: rendered.levels()[1].iter().map(level_to_proto).collect(),
    }
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

fn bbo_payload_from_message(message: &pb::ServerMessage) -> Option<pb::Bbo> {
    match message.message.as_ref()? {
        pb::server_message::Message::Bbo(payload) => Some(payload.clone()),
        _ => None,
    }
}

fn l2_payload_from_message(message: &pb::ServerMessage) -> Option<pb::L2Book> {
    match message.message.as_ref()? {
        pb::server_message::Message::L2Book(payload) => Some(payload.clone()),
        _ => None,
    }
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
