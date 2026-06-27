//! Public transport integration surface for non-WebSocket frontends.

use std::{sync::Arc, time::Instant};

use log::{error, info};
use tokio::sync::{Mutex, broadcast::Sender};

use crate::ServerConfig;
pub use crate::listeners::order_book::{
    ActiveL2Params, ActiveSubscriptionInterests, CoinBbo, CoinDiffs, CoinStatuses, CoinTrades, InternalMessage,
    L2BuiltFrame, L2FrameCache, L2ParamGuard, L2SnapshotParams, OrderBookListener, OrderStatusPayloadTimestamps,
    RawBbo, SubscriptionInterest, SubscriptionInterestGuard, UserStatuses, hl_listen_hft,
};
pub use crate::order_book::{
    Snapshot,
    types::{Coin, Side},
};
pub use crate::types::{
    L4Order, Level, OrderDiff, Trade,
    inner::InnerLevel,
    node_data::{NodeDataOrderDiff, NodeDataOrderStatus},
    subscription::{
        ClientMessage, DEFAULT_LEVELS, FanoutChannel, MAX_LEVELS, Subscription, SubscriptionKind, SubscriptionManager,
        TransportKind,
    },
};

/// Shared order-book runtime consumed by one or more frontend transports.
#[derive(Clone)]
pub struct OrderBookRuntime {
    internal_message_tx: Sender<Arc<InternalMessage>>,
    listener: Arc<Mutex<OrderBookListener>>,
    start_time: Instant,
    bbo_only: bool,
    l2book_heartbeat_ms: u64,
    bbo_heartbeat_ms: u64,
}

impl OrderBookRuntime {
    /// Start the file-watcher/snapshot/order-book pipeline once.
    #[must_use]
    pub fn spawn(config: &ServerConfig) -> Self {
        // One channel multiplexes every event (L2/BBO/L4/fills) to all transport
        // connections. Depth 16384 gives seconds of burst headroom while each
        // slot remains just one Arc pointer.
        let (internal_message_tx, _) = tokio::sync::broadcast::channel::<Arc<InternalMessage>>(16384);

        let market_filter = (config.include_perps, config.include_spot, config.include_hip3);
        let ignore_spot = !config.include_spot;
        let active_l2_params = ActiveL2Params::new();
        let active_subscription_interests = ActiveSubscriptionInterests::new();

        let listener = {
            let internal_message_tx = internal_message_tx.clone();
            let mut listener = OrderBookListener::new(
                Some(internal_message_tx),
                ignore_spot,
                active_l2_params,
                active_subscription_interests,
                market_filter,
            );
            listener.set_tolerate_drift(config.no_resync);
            listener
        };
        let listener = Arc::new(Mutex::new(listener));

        let listener_task = {
            let listener = listener.clone();
            let config = config.clone();
            tokio::spawn(async move {
                info!("Starting HFT-optimized listener");
                hl_listen_hft(listener, config).await
            })
        };

        tokio::spawn(async move {
            match listener_task.await {
                Ok(Ok(())) => error!("Listener task exited unexpectedly"),
                Ok(Err(err)) => error!("Listener fatal error: {err}"),
                Err(err) => error!("Listener task panicked or was aborted: {err}"),
            }
            std::process::exit(1);
        });

        Self {
            internal_message_tx,
            listener,
            start_time: Instant::now(),
            bbo_only: config.bbo_only,
            l2book_heartbeat_ms: config.l2book_heartbeat_ms,
            bbo_heartbeat_ms: config.bbo_heartbeat_ms,
        }
    }

    #[must_use]
    pub fn internal_message_tx(&self) -> Sender<Arc<InternalMessage>> {
        self.internal_message_tx.clone()
    }

    #[must_use]
    pub fn listener(&self) -> Arc<Mutex<OrderBookListener>> {
        self.listener.clone()
    }

    #[must_use]
    pub const fn start_time(&self) -> Instant {
        self.start_time
    }

    #[must_use]
    pub const fn bbo_only(&self) -> bool {
        self.bbo_only
    }

    #[must_use]
    pub const fn l2book_heartbeat_ms(&self) -> u64 {
        self.l2book_heartbeat_ms
    }

    #[must_use]
    pub const fn bbo_heartbeat_ms(&self) -> u64 {
        self.bbo_heartbeat_ms
    }
}
