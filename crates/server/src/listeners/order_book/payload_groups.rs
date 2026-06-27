use std::{collections::HashMap, sync::Arc};

use alloy::primitives::Address;

use super::{OrderStatusPayloadTimestamps, OrderStatusTimestampSlice, SharedFrame};
use crate::{
    metrics::{GROUPED_PAYLOAD_EVENTS, GROUPED_PAYLOAD_GROUPS, TRADES_UNPAIRED_FILLS_TOTAL},
    order_book::Side,
    types::{
        Trade,
        node_data::{Batch, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
    },
};

/// One coin's trades plus the shared `trades` wire frame.
pub struct CoinTrades {
    pub trades: Arc<Vec<Trade>>,
    pub(crate) frame: SharedFrame,
}

/// One coin's order diffs plus the shared wire frames derived from them
/// (`bookDiffs` for `BookDiffs` subscribers, `l4Book` updates for `L4Book` ones).
pub struct CoinDiffs {
    pub diffs: Arc<Vec<NodeDataOrderDiff>>,
    pub(crate) book_diffs_frame: SharedFrame,
    pub(crate) l4_frame: SharedFrame,
}

impl CoinDiffs {
    /// Group order diffs per coin (one clone per event - the batch itself is
    /// consumed by the state apply). `skip_spot` folds the `--markets` filtering
    /// in, replacing the old whole-batch `filter_events` clone.
    pub(super) fn group_by_coin(events: &[NodeDataOrderDiff], skip_spot: bool) -> HashMap<String, Self> {
        let mut by_coin: HashMap<String, Vec<NodeDataOrderDiff>> = HashMap::with_capacity(events.len().min(512));
        let mut retained = 0usize;
        for diff in events {
            let coin = diff.coin();
            if skip_spot && coin.is_spot() {
                continue;
            }
            retained += 1;
            by_coin.entry(coin.value()).or_default().push(diff.clone());
        }
        GroupedPayloadShape::new(by_coin.len(), retained).observe("bookDiffs", "coin");
        by_coin
            .into_iter()
            .map(|(coin, diffs)| {
                (
                    coin,
                    Self { diffs: Arc::new(diffs), book_diffs_frame: SharedFrame::new(), l4_frame: SharedFrame::new() },
                )
            })
            .collect()
    }
}

/// One coin's order statuses plus the shared `l4Book` updates wire frame.
pub struct CoinStatuses {
    pub statuses: Arc<Vec<NodeDataOrderStatus>>,
    pub timestamps: OrderStatusPayloadTimestamps,
    pub(crate) l4_frame: SharedFrame,
}

impl CoinStatuses {
    /// Group order statuses per coin (one clone per event).
    pub(super) fn group_by_coin(events: &[NodeDataOrderStatus]) -> HashMap<String, Self> {
        let mut by_coin: HashMap<String, Vec<NodeDataOrderStatus>> = HashMap::with_capacity(events.len().min(512));
        for status in events {
            by_coin.entry(status.order.coin.clone()).or_default().push(status.clone());
        }
        GroupedPayloadShape::new(by_coin.len(), events.len()).observe("orderStatuses", "coin");
        by_coin
            .into_iter()
            .map(|(coin, statuses)| {
                let timestamps = statuses.payload_timestamps();
                (coin, Self { statuses: Arc::new(statuses), timestamps, l4_frame: SharedFrame::new() })
            })
            .collect()
    }
}

/// One user's order statuses plus the shared `orderUpdates` wire frame.
pub struct UserStatuses {
    pub statuses: Arc<Vec<NodeDataOrderStatus>>,
    pub timestamps: OrderStatusPayloadTimestamps,
    pub(crate) frame: SharedFrame,
}

impl UserStatuses {
    /// Group order statuses per user so `orderUpdates` subscribers do not scan
    /// all coins/statuses on every broadcast.
    pub(super) fn group_by_user(events: &[NodeDataOrderStatus]) -> HashMap<Address, Self> {
        let mut by_user: HashMap<Address, Vec<NodeDataOrderStatus>> = HashMap::with_capacity(events.len().min(512));
        for status in events {
            by_user.entry(status.user).or_default().push(status.clone());
        }
        GroupedPayloadShape::new(by_user.len(), events.len()).observe("orderUpdates", "user");
        by_user
            .into_iter()
            .map(|(user, statuses)| {
                let timestamps = statuses.payload_timestamps();
                (user, Self { statuses: Arc::new(statuses), timestamps, frame: SharedFrame::new() })
            })
            .collect()
    }
}

/// Pairs fill legs into public-schema trades; holds at most the one leg awaiting
/// its counterpart across Fills batches (single-event batches in
/// `--stream-with-block-info` mode).
///
/// A match produces two fill records (buyer + seller) sharing a `tid`. The node
/// emits them as immediate neighbours in the fills stream. Pairing only needs to
/// remember the single previous leg: when the next leg shares its `tid` they
/// form a trade; otherwise the previous leg was unpairable and is dropped.
#[derive(Default)]
pub(super) struct TradePairer {
    prev: Option<NodeDataFill>,
}

impl TradePairer {
    pub(super) fn group_by_coin(&mut self, batch: Batch<NodeDataFill>) -> HashMap<String, CoinTrades> {
        let events_len = batch.events_len();
        let mut by_coin: HashMap<String, Vec<Trade>> = HashMap::with_capacity((events_len / 2).clamp(1, 512));
        let mut retained = 0usize;
        for fill in batch.events() {
            match self.prev.take() {
                Some(prev) if prev.1.tid == fill.1.tid => {
                    let (bid, ask) = if fill.1.side == Side::Bid { (fill, prev) } else { (prev, fill) };
                    if let Some(trade) = Trade::from_fills(bid, ask) {
                        retained += 1;
                        by_coin.entry(trade.coin.clone()).or_default().push(trade);
                    } else {
                        // Same tid but mismatched coin/sides: both legs are
                        // unpairable, count them so this stays observable.
                        TRADES_UNPAIRED_FILLS_TOTAL.inc_by(2);
                    }
                }
                Some(_) => {
                    // Previous leg never met its counterpart: unpairable, drop it.
                    TRADES_UNPAIRED_FILLS_TOTAL.inc();
                    self.prev = Some(fill);
                }
                None => self.prev = Some(fill),
            }
        }
        GroupedPayloadShape::new(by_coin.len(), retained).observe("trades", "coin");
        by_coin
            .into_iter()
            .map(|(coin, trades)| (coin, CoinTrades { trades: Arc::new(trades), frame: SharedFrame::new() }))
            .collect()
    }
}

#[derive(Clone, Copy)]
struct GroupedPayloadShape {
    groups: usize,
    events: usize,
}

impl GroupedPayloadShape {
    const fn new(groups: usize, events: usize) -> Self {
        Self { groups, events }
    }

    fn observe(self, channel: &'static str, group_by: &'static str) {
        GROUPED_PAYLOAD_GROUPS.with_label_values(&[channel, group_by]).observe(self.groups as f64);
        GROUPED_PAYLOAD_EVENTS.with_label_values(&[channel, group_by]).observe(self.events as f64);
    }
}
