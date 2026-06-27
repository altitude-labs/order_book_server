//! Prometheus metrics for orderbook server monitoring
//!
//! Provides comprehensive metrics for:
//! - WebSocket connections and subscriptions
//! - Event processing latency and throughput
//! - Orderbook health and state
//! - Errors and anomalies

use lazy_static::lazy_static;
use prometheus::{
    Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry,
};

use crate::transport::{SubscriptionKind, TransportKind};

lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();

    // ==================== CONNECTION METRICS ====================

    /// Current number of active WebSocket connections
    pub static ref WS_CONNECTIONS_ACTIVE: IntGauge = IntGauge::new(
        "hl_orderbook_server_ws_connections_active",
        "Number of active WebSocket connections"
    ).expect("metric can be created");

    /// Total WebSocket connections since startup
    pub static ref WS_CONNECTIONS_TOTAL: IntCounter = IntCounter::new(
        "hl_orderbook_server_ws_connections_total",
        "Total WebSocket connections since startup"
    ).expect("metric can be created");

    /// Active subscriptions by type (bbo, l2Book, l4Book, trades)
    pub static ref WS_SUBSCRIPTIONS_ACTIVE: IntGaugeVec = IntGaugeVec::new(
        Opts::new("hl_orderbook_server_ws_subscriptions_active", "Active subscriptions by type"),
        &["type"]
    ).expect("metric can be created");

    /// Active subscriptions by transport and type.
    pub static ref TRANSPORT_SUBSCRIPTIONS_ACTIVE: IntGaugeVec = IntGaugeVec::new(
        Opts::new("hl_orderbook_server_transport_subscriptions_active", "Active subscriptions by transport and type"),
        &["transport", "type"]
    ).expect("metric can be created");

    /// Active client connections by transport (websocket, grpc)
    pub static ref TRANSPORT_CONNECTIONS_ACTIVE: IntGaugeVec = IntGaugeVec::new(
        Opts::new("hl_orderbook_server_transport_connections_active", "Active client connections by transport"),
        &["transport"]
    ).expect("metric can be created");

    /// Total client connections by transport since startup
    pub static ref TRANSPORT_CONNECTIONS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("hl_orderbook_server_transport_connections_total", "Total client connections by transport since startup"),
        &["transport"]
    ).expect("metric can be created");

    // ==================== LATENCY METRICS ====================

    /// BBO broadcast latency in seconds
    pub static ref BBO_BROADCAST_LATENCY: Histogram = Histogram::with_opts(
        HistogramOpts::new("hl_orderbook_server_bbo_broadcast_latency_seconds", "BBO broadcast latency")
            .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0])
    ).expect("metric can be created");

    /// L2 broadcast latency in seconds
    pub static ref L2_BROADCAST_LATENCY: Histogram = Histogram::with_opts(
        HistogramOpts::new("hl_orderbook_server_l2_broadcast_latency_seconds", "L2 broadcast latency")
            .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0])
    ).expect("metric can be created");

    /// Number of coins rebuilt per L2 broadcast (conflation batch size). Tracks how
    /// many coins changed within each 50ms throttle window; spikes toward the
    /// universe size indicate full-universe windows (lock-duration pressure).
    pub static ref L2_CONFLATION_BATCH_SIZE: Histogram = Histogram::with_opts(
        HistogramOpts::new("hl_orderbook_server_l2_conflation_batch_size", "Coins rebuilt per L2 broadcast")
            .buckets(vec![1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 150.0])
    ).expect("metric can be created");

    /// L2 flush phase latency. Splits the total L2 broadcast latency into
    /// snapshot computation, optional universe rebuild, and broadcast send.
    pub static ref L2_FLUSH_STAGE_LATENCY: HistogramVec = HistogramVec::new(
        HistogramOpts::new("hl_orderbook_server_l2_flush_stage_latency_seconds", "L2 flush phase latency")
            .buckets(vec![0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5]),
        &["stage"]
    ).expect("metric can be created");

    /// Number of active L2 aggregation variant shapes at flush time.
    pub static ref L2_ACTIVE_VARIANTS: Histogram = Histogram::with_opts(
        HistogramOpts::new("hl_orderbook_server_l2_active_variants", "Active L2 aggregation variants per flush")
            .buckets(vec![0.0, 1.0, 2.0, 3.0, 5.0, 8.0, 13.0, 21.0])
    ).expect("metric can be created");

    /// Number of coins whose L2 snapshots were actually recomputed in a flush.
    pub static ref L2_RECOMPUTED_COINS: Histogram = Histogram::with_opts(
        HistogramOpts::new("hl_orderbook_server_l2_recomputed_coins", "Coins recomputed per L2 flush")
            .buckets(vec![0.0, 1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 150.0])
    ).expect("metric can be created");

    /// Number of coins currently held in the listener-side L2 snapshot cache.
    pub static ref L2_CACHE_COINS: IntGauge = IntGauge::new(
        "hl_orderbook_server_l2_cache_coins",
        "Coins currently held in the L2 snapshot cache"
    ).expect("metric can be created");

    /// Event processing latency by type
    pub static ref EVENT_PROCESSING_LATENCY: HistogramVec = HistogramVec::new(
        HistogramOpts::new("hl_orderbook_server_event_processing_latency_seconds", "Event processing latency")
            .buckets(vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1]),
        &["event_type"]
    ).expect("metric can be created");

    /// Number of node events in each parsed batch. This lets operators interpret
    /// listener latency as per-batch vs per-event cost.
    pub static ref EVENT_BATCH_SIZE: HistogramVec = HistogramVec::new(
        HistogramOpts::new("hl_orderbook_server_event_batch_size", "Node events per parsed batch")
            .buckets(vec![0.0, 1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0]),
        &["event_type"]
    ).expect("metric can be created");

    /// Number of grouped payload buckets emitted by listener broadcast prep.
    /// This explains fan-out cost when one input batch expands into many
    /// per-coin or per-user payloads.
    pub static ref GROUPED_PAYLOAD_GROUPS: HistogramVec = HistogramVec::new(
        HistogramOpts::new("hl_orderbook_server_grouped_payload_groups", "Grouped payload buckets emitted by listener broadcast prep")
            .buckets(vec![0.0, 1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0]),
        &["channel", "group_by"]
    ).expect("metric can be created");

    /// Number of events retained inside grouped payloads after filtering/pairing.
    /// Compare this with `event_batch_size` to see whether work is dominated by
    /// input batch size or output fan-out shape.
    pub static ref GROUPED_PAYLOAD_EVENTS: HistogramVec = HistogramVec::new(
        HistogramOpts::new("hl_orderbook_server_grouped_payload_events", "Events retained in grouped payloads after filtering or pairing")
            .buckets(vec![0.0, 1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0, 50000.0, 100000.0]),
        &["channel", "group_by"]
    ).expect("metric can be created");

    /// Distinct coins changed by one state-apply batch. High values explain BBO
    /// and L2 prep spikes even when raw event count is moderate.
    pub static ref STATE_APPLY_CHANGED_COINS: HistogramVec = HistogramVec::new(
        HistogramOpts::new("hl_orderbook_server_state_apply_changed_coins", "Distinct coins changed by one state apply batch")
            .buckets(vec![0.0, 1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0]),
        &["event_type"]
    ).expect("metric can be created");

    /// Maintenance-stage latency while the listener lock is held. This covers
    /// pending-cache eviction and slab compaction, which run on the periodic
    /// state-progress cadence.
    pub static ref MAINTENANCE_STAGE_LATENCY: HistogramVec = HistogramVec::new(
        HistogramOpts::new("hl_orderbook_server_maintenance_stage_latency_seconds", "Listener maintenance stage latency")
            .buckets(vec![0.000001, 0.000005, 0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5]),
        &["stage"]
    ).expect("metric can be created");

    /// Snapshot/re-sync stage latency. Covers external snapshot computation,
    /// JSON load, listener init, and replay work.
    pub static ref SNAPSHOT_STAGE_LATENCY: HistogramVec = HistogramVec::new(
        HistogramOpts::new("hl_orderbook_server_snapshot_stage_latency_seconds", "Snapshot and resync stage latency")
            .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0]),
        &["stage"]
    ).expect("metric can be created");

    /// Cached replay events applied after a snapshot fetch.
    pub static ref SNAPSHOT_REPLAY_EVENTS: Histogram = Histogram::with_opts(
        HistogramOpts::new("hl_orderbook_server_snapshot_replay_events", "Cached events replayed after snapshot load")
            .buckets(vec![0.0, 10.0, 100.0, 1000.0, 10000.0, 50000.0, 100000.0, 500000.0, 1000000.0])
    ).expect("metric can be created");

    /// Replay-cache size while a snapshot fetch is pending.
    pub static ref SNAPSHOT_REPLAY_CACHE_EVENTS: Histogram = Histogram::with_opts(
        HistogramOpts::new("hl_orderbook_server_snapshot_replay_cache_events", "Replay-cache event count while snapshot fetch is pending")
            .buckets(vec![0.0, 10.0, 100.0, 1000.0, 10000.0, 50000.0, 100000.0, 500000.0, 1000000.0])
    ).expect("metric can be created");

    /// Listener-stage latency inside one parsed event batch. `event_processing`
    /// is the total; this splits it into broadcast preparation, state apply, and
    /// BBO preparation so bottlenecks can be localized.
    pub static ref LISTENER_STAGE_LATENCY: HistogramVec = HistogramVec::new(
        HistogramOpts::new("hl_orderbook_server_listener_stage_latency_seconds", "Listener stage latency by event type and stage")
            .buckets(vec![0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5]),
        &["event_type", "stage"]
    ).expect("metric can be created");

    /// Time spent waiting to acquire the listener mutex. This exposes contention
    /// between event ingestion, snapshot serving, health checks, and connection
    /// setup.
    pub static ref LISTENER_LOCK_WAIT_LATENCY: HistogramVec = HistogramVec::new(
        HistogramOpts::new("hl_orderbook_server_listener_lock_wait_latency_seconds", "Listener mutex wait latency by caller")
            .buckets(vec![0.000001, 0.000005, 0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5]),
        &["caller"]
    ).expect("metric can be created");

    /// Time the listener mutex is held after acquisition. Long holds usually
    /// point at expensive work that should move outside the lock.
    pub static ref LISTENER_LOCK_HOLD_LATENCY: HistogramVec = HistogramVec::new(
        HistogramOpts::new("hl_orderbook_server_listener_lock_hold_latency_seconds", "Listener mutex hold latency by caller")
            .buckets(vec![0.000001, 0.000005, 0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5]),
        &["caller"]
    ).expect("metric can be created");

    /// JSON parse latency for one watcher line, before the listener lock is held.
    pub static ref FILE_PARSE_LATENCY: HistogramVec = HistogramVec::new(
        HistogramOpts::new("hl_orderbook_server_file_parse_latency_seconds", "JSON parse latency for watcher lines")
            .buckets(vec![0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1]),
        &["source"]
    ).expect("metric can be created");

    /// Time a file watcher blocks while enqueueing one line into the bounded
    /// listener channel. Non-trivial values mean downstream backpressure.
    pub static ref FILE_WATCHER_ENQUEUE_LATENCY: HistogramVec = HistogramVec::new(
        HistogramOpts::new("hl_orderbook_server_file_watcher_enqueue_latency_seconds", "File watcher bounded-channel enqueue latency")
            .buckets(vec![0.000001, 0.000005, 0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1]),
        &["source", "kind"]
    ).expect("metric can be created");

    /// Age of one watcher event when the listener receives it. This starts when
    /// the watcher attempts to enqueue the event, so it includes bounded-channel
    /// backpressure plus time spent waiting in the listener queue.
    pub static ref FILE_WATCHER_HANDOFF_AGE: HistogramVec = HistogramVec::new(
        HistogramOpts::new("hl_orderbook_server_file_watcher_handoff_age_seconds", "File watcher event age at listener receive")
            .buckets(vec![0.000001, 0.000005, 0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]),
        &["source", "kind"]
    ).expect("metric can be created");

    /// Number of watcher events drained by one listener recv_many call.
    pub static ref FILE_WATCHER_RECV_BATCH_SIZE: Histogram = Histogram::with_opts(
        HistogramOpts::new("hl_orderbook_server_file_watcher_recv_batch_size", "Watcher events drained per listener receive batch")
            .buckets(vec![1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0])
    ).expect("metric can be created");

    /// Time spent by one client connection handling one broadcast message before
    /// returning to the receive loop. This includes subscription filtering,
    /// serialization/cache lookup, and socket/queue send time.
    pub static ref TRANSPORT_FANOUT_LATENCY: HistogramVec = HistogramVec::new(
        HistogramOpts::new("hl_orderbook_server_transport_fanout_latency_seconds", "Per-connection fan-out latency by transport and channel")
            .buckets(vec![0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5]),
        &["transport", "channel"]
    ).expect("metric can be created");

    /// Number of subscriptions a connection has to consider for one internal
    /// broadcast message. High values explain fan-out latency spikes even when
    /// send latency is low.
    pub static ref TRANSPORT_FANOUT_SUBSCRIPTIONS: HistogramVec = HistogramVec::new(
        HistogramOpts::new("hl_orderbook_server_transport_fanout_subscriptions", "Subscriptions considered per fan-out message")
            .buckets(vec![0.0, 1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 150.0, 200.0, 256.0]),
        &["transport", "channel"]
    ).expect("metric can be created");

    /// Number of active subscriptions on the connection that could receive one
    /// internal broadcast message before keyed fan-out filtering. Compare this
    /// to `transport_fanout_subscriptions` to see scan work avoided by coin/user
    /// indexes.
    pub static ref TRANSPORT_FANOUT_ACTIVE_SUBSCRIPTIONS: HistogramVec = HistogramVec::new(
        HistogramOpts::new("hl_orderbook_server_transport_fanout_active_subscriptions", "Active subscriptions eligible for one fan-out message before keyed filtering")
            .buckets(vec![0.0, 1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 150.0, 200.0, 256.0]),
        &["transport", "channel"]
    ).expect("metric can be created");

    /// Time spent sending or queueing one outbound message to a client.
    pub static ref TRANSPORT_SEND_LATENCY: HistogramVec = HistogramVec::new(
        HistogramOpts::new("hl_orderbook_server_transport_send_latency_seconds", "Client send latency by transport")
            .buckets(vec![0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5]),
        &["transport"]
    ).expect("metric can be created");

    /// Outbound per-client queue occupancy sampled before send. For gRPC this
    /// is the bounded mpsc queue feeding tonic's response stream; high values
    /// mean the client transport cannot drain messages as fast as fan-out
    /// produces them.
    pub static ref TRANSPORT_OUTGOING_QUEUE_DEPTH: HistogramVec = HistogramVec::new(
        HistogramOpts::new("hl_orderbook_server_transport_outgoing_queue_depth", "Outbound transport queue depth before send")
            .buckets(vec![0.0, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 768.0, 1024.0]),
        &["transport"]
    ).expect("metric can be created");

    /// Time spent constructing a transport-native payload before send. For gRPC
    /// this captures protobuf struct conversion work; WebSocket JSON frame
    /// build cost is covered by `wire_frame_build_latency_seconds`.
    pub static ref TRANSPORT_PAYLOAD_BUILD_LATENCY: HistogramVec = HistogramVec::new(
        HistogramOpts::new("hl_orderbook_server_transport_payload_build_latency_seconds", "Transport-native payload build latency")
            .buckets(vec![0.000001, 0.000005, 0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1]),
        &["transport", "channel"]
    ).expect("metric can be created");

    /// Encoded transport-native payload size. For gRPC this uses protobuf
    /// `encoded_len()` without allocating the encoded buffer.
    pub static ref TRANSPORT_PAYLOAD_BYTES: HistogramVec = HistogramVec::new(
        HistogramOpts::new("hl_orderbook_server_transport_payload_bytes", "Transport-native payload encoded bytes")
            .buckets(vec![64.0, 256.0, 1024.0, 4096.0, 16384.0, 65536.0, 262144.0, 1048576.0, 4194304.0]),
        &["transport", "channel"]
    ).expect("metric can be created");

    /// Transport-native payload cache hits/misses. For gRPC this shows whether
    /// protobuf conversion is being reused across clients receiving the same
    /// internal broadcast payload.
    pub static ref TRANSPORT_PAYLOAD_CACHE_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("hl_orderbook_server_transport_payload_cache_total", "Transport-native payload cache hits and misses by transport/channel"),
        &["transport", "channel", "outcome"]
    ).expect("metric can be created");

    /// Shared wire-frame cache hits/misses by channel. Misses indicate JSON
    /// serialization and payload rendering work happened on a transport task.
    pub static ref WIRE_FRAME_CACHE_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("hl_orderbook_server_wire_frame_cache_total", "Wire frame cache hits and misses by channel"),
        &["channel", "outcome"]
    ).expect("metric can be created");

    /// Time spent rendering/serializing a shared wire frame on cache misses.
    pub static ref WIRE_FRAME_BUILD_LATENCY: HistogramVec = HistogramVec::new(
        HistogramOpts::new("hl_orderbook_server_wire_frame_build_latency_seconds", "Wire frame render and serialization latency by channel")
            .buckets(vec![0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5]),
        &["channel"]
    ).expect("metric can be created");

    /// Serialized wire-frame byte size by channel.
    pub static ref WIRE_FRAME_BYTES: HistogramVec = HistogramVec::new(
        HistogramOpts::new("hl_orderbook_server_wire_frame_bytes", "Serialized wire frame bytes by channel")
            .buckets(vec![64.0, 256.0, 1024.0, 4096.0, 16384.0, 65536.0, 262144.0, 1048576.0, 4194304.0]),
        &["channel"]
    ).expect("metric can be created");

    /// Wire-frame serialization failures by channel.
    pub static ref WIRE_FRAME_BUILD_ERRORS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("hl_orderbook_server_wire_frame_build_errors_total", "Wire frame serialization errors by channel"),
        &["channel"]
    ).expect("metric can be created");

    /// Pending pairing-cache entries evicted during maintenance.
    pub static ref PENDING_CACHE_EVICTIONS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("hl_orderbook_server_pending_cache_evictions_total", "Pending order pairing cache evictions"),
        &["cache", "reason"]
    ).expect("metric can be created");

    /// Price-level slabs compacted during maintenance.
    pub static ref ORDERBOOK_PRICE_LEVEL_SLABS_COMPACTED_TOTAL: IntCounter = IntCounter::new(
        "hl_orderbook_server_orderbook_price_level_slabs_compacted_total",
        "Price-level slabs compacted during maintenance"
    ).expect("metric can be created");

    /// Aggregate linked-list slab live/capacity across all price levels.
    pub static ref ORDERBOOK_PRICE_LEVEL_SLAB_NODES: IntGaugeVec = IntGaugeVec::new(
        Opts::new("hl_orderbook_server_orderbook_price_level_slab_nodes", "Aggregate orderbook price-level slab nodes"),
        &["kind"]
    ).expect("metric can be created");

    /// Snapshot fetch attempts by result.
    pub static ref SNAPSHOT_FETCH_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("hl_orderbook_server_snapshot_fetch_total", "Snapshot fetch attempts by result"),
        &["result"]
    ).expect("metric can be created");

    /// Age of an event at transport egress, derived from source timestamps
    /// embedded in payloads. This estimates end-to-end server-side delay from
    /// node event time to the moment a transport queues/sends the update.
    pub static ref TRANSPORT_EVENT_EGRESS_AGE: HistogramVec = HistogramVec::new(
        HistogramOpts::new("hl_orderbook_server_transport_event_egress_age_seconds", "Event age at transport egress by transport and channel")
            .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]),
        &["transport", "channel"]
    ).expect("metric can be created");

    /// Age of per-payload timestamps at transport egress. Unlike
    /// `transport_event_egress_age_seconds`, which usually uses the enclosing
    /// broadcast/block time, this uses timestamps embedded in user-facing
    /// payload records such as trade time or order timestamp.
    pub static ref TRANSPORT_PAYLOAD_EGRESS_AGE: HistogramVec = HistogramVec::new(
        HistogramOpts::new("hl_orderbook_server_transport_payload_egress_age_seconds", "Payload timestamp age at transport egress")
            .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0]),
        &["transport", "channel", "timestamp"]
    ).expect("metric can be created");

    /// Age of node timestamps at internal pipeline stages. Recorded for both
    /// `block_time` and `local_time` when the source batch has them, so operators
    /// can separate node/file lag from listener processing lag.
    pub static ref SOURCE_EVENT_AGE: HistogramVec = HistogramVec::new(
        HistogramOpts::new("hl_orderbook_server_source_event_age_seconds", "Source event age by source timestamp and pipeline stage")
            .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0]),
        &["source", "timestamp", "stage"]
    ).expect("metric can be created");
}

lazy_static! {
    // ==================== THROUGHPUT METRICS ====================

    /// Events processed by type (orders, diffs, fills)
    pub static ref EVENTS_PROCESSED_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("hl_orderbook_server_events_processed_total", "Total events processed"),
        &["type"]
    ).expect("metric can be created");

    /// Broadcasts sent by channel type
    pub static ref BROADCASTS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("hl_orderbook_server_broadcasts_total", "Total broadcasts sent"),
        &["channel"]
    ).expect("metric can be created");

    /// Fill legs dropped because no matching counterpart followed (trades pairing).
    /// ~0 in steady state; growth means the node stopped emitting the two legs
    /// of a match adjacently and trade pairing should be revisited.
    pub static ref TRADES_UNPAIRED_FILLS_TOTAL: IntCounter = IntCounter::new(
        "hl_orderbook_server_trades_unpaired_fills_total", "Fill legs dropped without a matching counterpart"
    ).expect("metric can be created");

    /// WebSocket messages sent
    pub static ref MESSAGES_SENT_TOTAL: IntCounter = IntCounter::new(
        "hl_orderbook_server_messages_sent_total",
        "Total WebSocket messages sent"
    ).expect("metric can be created");

    /// Messages sent by transport. Unlike the legacy messages_sent_total, this
    /// separates WebSocket and gRPC so both-mode deployments can compare cost.
    pub static ref TRANSPORT_MESSAGES_SENT_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("hl_orderbook_server_transport_messages_sent_total", "Messages sent by transport"),
        &["transport"]
    ).expect("metric can be created");

    /// Messages sent by transport and channel. Use this next to fan-out latency
    /// to separate "many candidate subscriptions" from "many actual writes".
    pub static ref TRANSPORT_CHANNEL_MESSAGES_SENT_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("hl_orderbook_server_transport_channel_messages_sent_total", "Messages sent by transport and channel"),
        &["transport", "channel"]
    ).expect("metric can be created");

    /// Messages intentionally skipped by a transport after fan-out. This tracks
    /// subscription-level dedup and dirty-set suppression, which reduces writes
    /// even when the listener has already broadcast a candidate update.
    pub static ref TRANSPORT_MESSAGES_SKIPPED_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("hl_orderbook_server_transport_messages_skipped_total", "Transport messages skipped by transport, channel, and reason"),
        &["transport", "channel", "reason"]
    ).expect("metric can be created");

    /// Listener-side broadcast preparation skipped because no live subscription
    /// currently needs that channel.
    pub static ref BROADCAST_PREP_SKIPPED_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("hl_orderbook_server_broadcast_prep_skipped_total", "Broadcast preparation skipped by channel and reason"),
        &["channel", "reason"]
    ).expect("metric can be created");

    /// Listener-side internal broadcast send latency by channel. This is the
    /// tokio broadcast channel handoff from the listener to transport tasks.
    pub static ref INTERNAL_BROADCAST_SEND_LATENCY: HistogramVec = HistogramVec::new(
        HistogramOpts::new("hl_orderbook_server_internal_broadcast_send_latency_seconds", "Listener internal broadcast send latency by channel")
            .buckets(vec![0.000001, 0.000005, 0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01]),
        &["channel"]
    ).expect("metric can be created");

    /// Number of broadcast receivers present when the listener sent a message.
    pub static ref INTERNAL_BROADCAST_RECEIVERS: HistogramVec = HistogramVec::new(
        HistogramOpts::new("hl_orderbook_server_internal_broadcast_receivers", "Internal broadcast receivers per listener send")
            .buckets(vec![0.0, 1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 5000.0]),
        &["channel"]
    ).expect("metric can be created");

    /// Number of grouped payload buckets in one internal broadcast message.
    /// Examples: coins in a BBO/L2/trades/bookDiffs message, or users in an
    /// orderUpdates message. This makes listener -> transport fanout shape
    /// visible before each client applies its subscription filter.
    pub static ref INTERNAL_BROADCAST_PAYLOAD_GROUPS: HistogramVec = HistogramVec::new(
        HistogramOpts::new("hl_orderbook_server_internal_broadcast_payload_groups", "Grouped payload buckets per internal broadcast")
            .buckets(vec![0.0, 1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0]),
        &["channel"]
    ).expect("metric can be created");

    /// Number of user-facing records in one internal broadcast message. For L2,
    /// this records dirty/recomputed coins because that is the downstream unit
    /// each transport connection filters and renders.
    pub static ref INTERNAL_BROADCAST_PAYLOAD_EVENTS: HistogramVec = HistogramVec::new(
        HistogramOpts::new("hl_orderbook_server_internal_broadcast_payload_events", "User-facing records per internal broadcast")
            .buckets(vec![0.0, 1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0, 50000.0, 100000.0]),
        &["channel"]
    ).expect("metric can be created");

    /// Listener-side internal broadcast send errors by channel.
    pub static ref INTERNAL_BROADCAST_SEND_ERRORS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("hl_orderbook_server_internal_broadcast_send_errors_total", "Internal broadcast send errors by channel and reason"),
        &["channel", "reason"]
    ).expect("metric can be created");

    // ==================== HEALTH METRICS ====================

    /// Current orderbook block height
    pub static ref ORDERBOOK_HEIGHT: IntGauge = IntGauge::new(
        "hl_orderbook_server_orderbook_height",
        "Current orderbook block height"
    ).expect("metric can be created");

    /// Orderbook timestamp in milliseconds
    pub static ref ORDERBOOK_TIME_MS: IntGauge = IntGauge::new(
        "hl_orderbook_server_orderbook_time_ms",
        "Orderbook timestamp in milliseconds"
    ).expect("metric can be created");

    /// Pending order statuses in HFT cache
    pub static ref PENDING_ORDERS_CACHE: IntGauge = IntGauge::new(
        "hl_orderbook_server_pending_orders_cache_size",
        "Pending order statuses in HFT cache"
    ).expect("metric can be created");

    /// Pending order diffs in HFT cache
    pub static ref PENDING_DIFFS_CACHE: IntGauge = IntGauge::new(
        "hl_orderbook_server_pending_diffs_cache_size",
        "Pending order diffs in HFT cache"
    ).expect("metric can be created");

    /// Broadcast channel lag by transport (receivers behind).
    pub static ref CHANNEL_LAG: IntGaugeVec = IntGaugeVec::new(
        Opts::new("hl_orderbook_server_broadcast_channel_lag", "Broadcast channel lag"),
        &["transport"]
    ).expect("metric can be created");

    // ==================== ERROR METRICS ====================

    /// Parse errors by type
    pub static ref PARSE_ERRORS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("hl_orderbook_server_parse_errors_total", "Total parse errors"),
        &["type"]
    ).expect("metric can be created");

    /// WebSocket send errors
    pub static ref WS_SEND_ERRORS_TOTAL: IntCounter = IntCounter::new(
        "hl_orderbook_server_ws_send_errors_total",
        "Total WebSocket send errors"
    ).expect("metric can be created");

    /// Send failures by transport and reason (error, timeout, closed).
    pub static ref TRANSPORT_SEND_ERRORS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("hl_orderbook_server_transport_send_errors_total", "Send failures by transport and reason"),
        &["transport", "reason"]
    ).expect("metric can be created");

    /// Times the order book was marked out-of-sync (by reason). Every increment
    /// triggers a background snapshot re-fetch that rebuilds the book, so a
    /// non-zero rate here means events were lost but the book self-healed.
    pub static ref ORDERBOOK_DESYNCS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("hl_orderbook_server_orderbook_desyncs_total", "Times the order book was marked out-of-sync"),
        &["reason"]
    ).expect("metric can be created");

    /// Messages dropped due to channel lag by transport.
    pub static ref CHANNEL_DROPS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("hl_orderbook_server_channel_drops_total", "Messages dropped due to channel lag"),
        &["transport"]
    ).expect("metric can be created");

    // ==================== FILE WATCHER METRICS ====================

    /// File events received per source (orders, diffs, fills)
    pub static ref FILE_EVENTS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("hl_orderbook_server_file_events_total", "File events received by source"),
        &["source"]
    ).expect("metric can be created");

    /// Lines parsed from files by source
    pub static ref FILE_LINES_PARSED_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("hl_orderbook_server_file_lines_parsed_total", "Lines parsed from files by source"),
        &["source"]
    ).expect("metric can be created");

    // ==================== ORDERBOOK STATS ====================

    /// Total orders currently in the orderbook
    pub static ref ORDERBOOK_ORDERS_TOTAL: IntGauge = IntGauge::new(
        "hl_orderbook_server_orderbook_orders_total",
        "Total orders currently in orderbook"
    ).expect("metric can be created");

    /// Number of coins tracked in orderbook
    pub static ref ORDERBOOK_COINS_COUNT: IntGauge = IntGauge::new(
        "hl_orderbook_server_orderbook_coins_count",
        "Number of coins tracked in orderbook"
    ).expect("metric can be created");

    /// BBO changes per coin (top 5 tracked individually)
    pub static ref BBO_CHANGES_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("hl_orderbook_server_bbo_changes_total", "BBO changes by coin"),
        &["coin"]
    ).expect("metric can be created");

    // ==================== UPTIME & SYSTEM ====================

    /// Server uptime in seconds
    pub static ref UPTIME_SECONDS: IntCounter = IntCounter::new(
        "hl_orderbook_server_uptime_seconds",
        "Server uptime in seconds"
    ).expect("metric can be created");

    /// Server start timestamp (unix seconds)
    pub static ref SERVER_START_TIME: IntGauge = IntGauge::new(
        "hl_orderbook_server_server_start_time_seconds",
        "Server start timestamp (unix seconds)"
    ).expect("metric can be created");

    /// Broadcast channel receiver count
    pub static ref BROADCAST_RECEIVERS: IntGauge = IntGauge::new(
        "hl_orderbook_server_broadcast_receivers",
        "Number of broadcast channel receivers"
    ).expect("metric can be created");

    /// Whether a snapshot fetch task is currently pending.
    pub static ref SNAPSHOT_FETCH_PENDING: IntGauge = IntGauge::new(
        "hl_orderbook_server_snapshot_fetch_pending",
        "Whether a snapshot fetch task is currently pending"
    ).expect("metric can be created");

}

/// Register all metrics with the registry
pub fn register_metrics() {
    // Connection metrics
    REGISTRY.register(Box::new(WS_CONNECTIONS_ACTIVE.clone())).ok();
    REGISTRY.register(Box::new(WS_CONNECTIONS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(WS_SUBSCRIPTIONS_ACTIVE.clone())).ok();
    REGISTRY.register(Box::new(TRANSPORT_SUBSCRIPTIONS_ACTIVE.clone())).ok();
    REGISTRY.register(Box::new(TRANSPORT_CONNECTIONS_ACTIVE.clone())).ok();
    REGISTRY.register(Box::new(TRANSPORT_CONNECTIONS_TOTAL.clone())).ok();

    // Latency metrics
    REGISTRY.register(Box::new(BBO_BROADCAST_LATENCY.clone())).ok();
    REGISTRY.register(Box::new(L2_BROADCAST_LATENCY.clone())).ok();
    REGISTRY.register(Box::new(L2_CONFLATION_BATCH_SIZE.clone())).ok();
    REGISTRY.register(Box::new(L2_FLUSH_STAGE_LATENCY.clone())).ok();
    REGISTRY.register(Box::new(L2_ACTIVE_VARIANTS.clone())).ok();
    REGISTRY.register(Box::new(L2_RECOMPUTED_COINS.clone())).ok();
    REGISTRY.register(Box::new(L2_CACHE_COINS.clone())).ok();
    REGISTRY.register(Box::new(EVENT_PROCESSING_LATENCY.clone())).ok();
    REGISTRY.register(Box::new(EVENT_BATCH_SIZE.clone())).ok();
    REGISTRY.register(Box::new(GROUPED_PAYLOAD_GROUPS.clone())).ok();
    REGISTRY.register(Box::new(GROUPED_PAYLOAD_EVENTS.clone())).ok();
    REGISTRY.register(Box::new(STATE_APPLY_CHANGED_COINS.clone())).ok();
    REGISTRY.register(Box::new(MAINTENANCE_STAGE_LATENCY.clone())).ok();
    REGISTRY.register(Box::new(SNAPSHOT_STAGE_LATENCY.clone())).ok();
    REGISTRY.register(Box::new(SNAPSHOT_REPLAY_EVENTS.clone())).ok();
    REGISTRY.register(Box::new(SNAPSHOT_REPLAY_CACHE_EVENTS.clone())).ok();
    REGISTRY.register(Box::new(LISTENER_STAGE_LATENCY.clone())).ok();
    REGISTRY.register(Box::new(LISTENER_LOCK_WAIT_LATENCY.clone())).ok();
    REGISTRY.register(Box::new(LISTENER_LOCK_HOLD_LATENCY.clone())).ok();
    REGISTRY.register(Box::new(FILE_PARSE_LATENCY.clone())).ok();
    REGISTRY.register(Box::new(FILE_WATCHER_ENQUEUE_LATENCY.clone())).ok();
    REGISTRY.register(Box::new(FILE_WATCHER_HANDOFF_AGE.clone())).ok();
    REGISTRY.register(Box::new(FILE_WATCHER_RECV_BATCH_SIZE.clone())).ok();
    REGISTRY.register(Box::new(TRANSPORT_FANOUT_LATENCY.clone())).ok();
    REGISTRY.register(Box::new(TRANSPORT_FANOUT_SUBSCRIPTIONS.clone())).ok();
    REGISTRY.register(Box::new(TRANSPORT_FANOUT_ACTIVE_SUBSCRIPTIONS.clone())).ok();
    REGISTRY.register(Box::new(TRANSPORT_SEND_LATENCY.clone())).ok();
    REGISTRY.register(Box::new(TRANSPORT_OUTGOING_QUEUE_DEPTH.clone())).ok();
    REGISTRY.register(Box::new(TRANSPORT_PAYLOAD_BUILD_LATENCY.clone())).ok();
    REGISTRY.register(Box::new(TRANSPORT_PAYLOAD_BYTES.clone())).ok();
    REGISTRY.register(Box::new(TRANSPORT_PAYLOAD_CACHE_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(WIRE_FRAME_CACHE_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(WIRE_FRAME_BUILD_LATENCY.clone())).ok();
    REGISTRY.register(Box::new(WIRE_FRAME_BYTES.clone())).ok();
    REGISTRY.register(Box::new(TRANSPORT_EVENT_EGRESS_AGE.clone())).ok();
    REGISTRY.register(Box::new(TRANSPORT_PAYLOAD_EGRESS_AGE.clone())).ok();
    REGISTRY.register(Box::new(SOURCE_EVENT_AGE.clone())).ok();

    // Throughput metrics
    REGISTRY.register(Box::new(EVENTS_PROCESSED_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(BROADCASTS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(TRADES_UNPAIRED_FILLS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(MESSAGES_SENT_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(TRANSPORT_MESSAGES_SENT_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(TRANSPORT_CHANNEL_MESSAGES_SENT_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(TRANSPORT_MESSAGES_SKIPPED_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(BROADCAST_PREP_SKIPPED_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(INTERNAL_BROADCAST_SEND_LATENCY.clone())).ok();
    REGISTRY.register(Box::new(INTERNAL_BROADCAST_RECEIVERS.clone())).ok();
    REGISTRY.register(Box::new(INTERNAL_BROADCAST_PAYLOAD_GROUPS.clone())).ok();
    REGISTRY.register(Box::new(INTERNAL_BROADCAST_PAYLOAD_EVENTS.clone())).ok();

    // Health metrics
    REGISTRY.register(Box::new(ORDERBOOK_HEIGHT.clone())).ok();
    REGISTRY.register(Box::new(ORDERBOOK_TIME_MS.clone())).ok();
    REGISTRY.register(Box::new(PENDING_ORDERS_CACHE.clone())).ok();
    REGISTRY.register(Box::new(PENDING_DIFFS_CACHE.clone())).ok();
    REGISTRY.register(Box::new(CHANNEL_LAG.clone())).ok();

    // Error metrics
    REGISTRY.register(Box::new(PARSE_ERRORS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(WS_SEND_ERRORS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(TRANSPORT_SEND_ERRORS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(INTERNAL_BROADCAST_SEND_ERRORS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(WIRE_FRAME_BUILD_ERRORS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(PENDING_CACHE_EVICTIONS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(SNAPSHOT_FETCH_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(CHANNEL_DROPS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(ORDERBOOK_DESYNCS_TOTAL.clone())).ok();

    // File watcher metrics
    REGISTRY.register(Box::new(FILE_EVENTS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(FILE_LINES_PARSED_TOTAL.clone())).ok();

    // Orderbook stats
    REGISTRY.register(Box::new(ORDERBOOK_ORDERS_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(ORDERBOOK_COINS_COUNT.clone())).ok();
    REGISTRY.register(Box::new(BBO_CHANGES_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(ORDERBOOK_PRICE_LEVEL_SLABS_COMPACTED_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(ORDERBOOK_PRICE_LEVEL_SLAB_NODES.clone())).ok();

    // Uptime & system
    REGISTRY.register(Box::new(UPTIME_SECONDS.clone())).ok();
    REGISTRY.register(Box::new(SERVER_START_TIME.clone())).ok();
    REGISTRY.register(Box::new(BROADCAST_RECEIVERS.clone())).ok();
    REGISTRY.register(Box::new(SNAPSHOT_FETCH_PENDING.clone())).ok();

    // Set server start time
    let start_time =
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0);
    SERVER_START_TIME.set(start_time);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadTimestampKind {
    StatusTime,
    OrderTimestamp,
    TradeTime,
}

impl PayloadTimestampKind {
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::StatusTime => "status_time",
            Self::OrderTimestamp => "order_timestamp",
            Self::TradeTime => "trade_time",
        }
    }
}

/// Observe how old an event is when a transport is about to send it. `event_time_ms`
/// is expected to be a unix timestamp in milliseconds from node/book data.
pub fn observe_transport_event_egress_age(transport: TransportKind, channel: SubscriptionKind, event_time_ms: u64) {
    if event_time_ms == 0 {
        return;
    }
    let now_ms = chrono::Utc::now().timestamp_millis();
    let Ok(event_time_ms) = i64::try_from(event_time_ms) else {
        return;
    };
    let age_ms = now_ms.saturating_sub(event_time_ms);
    if age_ms >= 0 {
        TRANSPORT_EVENT_EGRESS_AGE
            .with_label_values(&[transport.label(), channel.label()])
            .observe(age_ms as f64 / 1000.0);
    }
}

/// Observe how old a timestamp embedded in a user-facing payload is when a
/// transport is about to send it.
pub fn observe_transport_payload_egress_age(
    transport: TransportKind,
    channel: SubscriptionKind,
    timestamp: PayloadTimestampKind,
    event_time_ms: u64,
) {
    if event_time_ms == 0 {
        return;
    }
    let now_ms = chrono::Utc::now().timestamp_millis();
    let Ok(event_time_ms) = i64::try_from(event_time_ms) else {
        return;
    };
    let age_ms = now_ms.saturating_sub(event_time_ms);
    if age_ms >= 0 {
        TRANSPORT_PAYLOAD_EGRESS_AGE
            .with_label_values(&[transport.label(), channel.label(), timestamp.label()])
            .observe(age_ms as f64 / 1000.0);
    }
}

/// Observe how old a source event timestamp is at an internal pipeline stage.
/// `timestamp` should be a fixed label such as `block_time` or `local_time`.
pub fn observe_source_event_age(
    source: &'static str,
    timestamp: &'static str,
    stage: &'static str,
    event_time_ms: u64,
) {
    if event_time_ms == 0 {
        return;
    }
    let now_ms = chrono::Utc::now().timestamp_millis();
    let Ok(event_time_ms) = i64::try_from(event_time_ms) else {
        return;
    };
    let age_ms = now_ms.saturating_sub(event_time_ms);
    if age_ms >= 0 {
        SOURCE_EVENT_AGE.with_label_values(&[source, timestamp, stage]).observe(age_ms as f64 / 1000.0);
    }
}

/// Get metrics as Prometheus text format
pub fn gather_metrics() -> String {
    use prometheus::Encoder;
    let encoder = prometheus::TextEncoder::new();
    let metric_families = REGISTRY.gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}
