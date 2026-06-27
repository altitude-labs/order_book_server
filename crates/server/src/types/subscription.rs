use crate::metrics::{TRANSPORT_SUBSCRIPTIONS_ACTIVE, WS_SUBSCRIPTIONS_ACTIVE};
use crate::types::node_data::{NodeDataOrderDiff, NodeDataOrderStatus};
use crate::types::{Bbo, L2Book, L4Book, Trade};
use alloy::primitives::Address;
use log::debug;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

pub const MAX_LEVELS: usize = 100;
pub const DEFAULT_LEVELS: usize = 20;
/// Hard cap on subscriptions per WS connection. The broadcast hot paths iterate
/// every subscription on every event, and L4Book subscribes also trigger a
/// listener-lock-held snapshot computation - one client with thousands of subs
/// can stall every other client. 256 is comfortably above any legitimate use
/// (every market × every channel × every L2 param-tuple) while bounding the worst case.
pub(crate) const MAX_SUBSCRIPTIONS_PER_CONNECTION: usize = 256;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "method")]
#[serde(rename_all = "camelCase")]
pub enum ClientMessage {
    Subscribe { subscription: Subscription },
    Unsubscribe { subscription: Subscription },
    Ping,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "camelCase")]
pub enum Subscription {
    #[serde(rename_all = "camelCase")]
    Trades { coin: String },
    #[serde(rename_all = "camelCase")]
    L2Book { coin: String, n_sig_figs: Option<u32>, n_levels: Option<usize>, mantissa: Option<u64> },
    #[serde(rename_all = "camelCase")]
    L4Book { coin: String },
    #[serde(rename_all = "camelCase")]
    Bbo { coin: String },
    #[serde(rename_all = "camelCase")]
    OrderUpdates { user: String },
    #[serde(rename_all = "camelCase")]
    BookDiffs { coin: String },
}

impl Subscription {
    pub fn validate(&self, universe: &HashSet<String>) -> bool {
        match self {
            Self::Trades { coin } => universe.contains(coin),
            Self::L2Book { coin, n_sig_figs, n_levels, mantissa } => {
                if !universe.contains(coin) {
                    debug!("Invalid subscription: coin not found");
                    return false;
                }
                if *n_levels == Some(DEFAULT_LEVELS) {
                    debug!("Invalid subscription: set n_levels to this by using null");
                    return false;
                }
                let n_levels = n_levels.unwrap_or(DEFAULT_LEVELS);
                if n_levels > MAX_LEVELS {
                    debug!("Invalid subscription: n_levels too high");
                    return false;
                }
                if let Some(n_sig_figs) = *n_sig_figs {
                    if !(2..=5).contains(&n_sig_figs) {
                        debug!("Invalid subscription: sig figs aren't set correctly");
                        return false;
                    }
                    if let Some(m) = *mantissa {
                        if n_sig_figs < 5 || (m != 5 && m != 2) {
                            return false;
                        }
                    }
                } else if mantissa.is_some() {
                    debug!("Invalid subscription: mantissa can not be some if sig figs are not set");
                    return false;
                }
                debug!("Valid subscription");
                true
            }
            Self::L4Book { coin } | Self::Bbo { coin } | Self::BookDiffs { coin } => {
                if !universe.contains(coin) {
                    debug!("Invalid subscription: coin not found");
                    return false;
                }
                debug!("Valid subscription");
                true
            }
            Self::OrderUpdates { user } => {
                // Validate the user address format (must be valid hex address)
                if user.len() != 42 || !user.starts_with("0x") {
                    debug!("Invalid subscription: user address must be 42 characters starting with 0x");
                    return false;
                }
                if user[2..].chars().any(|c| !c.is_ascii_hexdigit()) {
                    debug!("Invalid subscription: user address contains invalid hex characters");
                    return false;
                }
                debug!("Valid orderUpdates subscription for user: {}", user);
                true
            }
        }
    }
}

impl Subscription {
    pub const fn kind(&self) -> SubscriptionKind {
        match self {
            Self::Bbo { .. } => SubscriptionKind::Bbo,
            Self::L2Book { .. } => SubscriptionKind::L2Book,
            Self::L4Book { .. } => SubscriptionKind::L4Book,
            Self::Trades { .. } => SubscriptionKind::Trades,
            Self::OrderUpdates { .. } => SubscriptionKind::OrderUpdates,
            Self::BookDiffs { .. } => SubscriptionKind::BookDiffs,
        }
    }

    pub const fn type_label(&self) -> &'static str {
        self.kind().label()
    }

    pub fn coin_key(&self) -> Option<&str> {
        match self {
            Self::Bbo { coin }
            | Self::L2Book { coin, .. }
            | Self::L4Book { coin }
            | Self::Trades { coin }
            | Self::BookDiffs { coin } => Some(coin),
            Self::OrderUpdates { .. } => None,
        }
    }

    fn user_key(&self) -> Option<Address> {
        match self {
            Self::OrderUpdates { user } => user.parse().ok(),
            _ => None,
        }
    }
}

/// Order update for a specific user - streams raw order status data
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OrderUpdate {
    pub user: Address,
    pub time: u64,
    pub height: u64,
    pub order_status: NodeDataOrderStatus,
}

impl OrderUpdate {
    pub(crate) fn new(user: Address, time: u64, height: u64, order_status: NodeDataOrderStatus) -> Self {
        Self { user, time, height, order_status }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "channel", content = "data")]
#[serde(rename_all = "camelCase")]
pub(crate) enum ServerResponse {
    SubscriptionResponse(ClientMessage),
    L2Book(L2Book),
    L4Book(L4Book),
    Trades(std::sync::Arc<Vec<Trade>>),
    Bbo(Bbo),
    BookDiffs(std::sync::Arc<Vec<NodeDataOrderDiff>>),
    OrderUpdates(Vec<OrderUpdate>),
    Pong,
    Error(String),
}

impl ServerResponse {
    pub(crate) const fn channel_label(&self) -> &'static str {
        match self {
            Self::SubscriptionResponse(_) => "subscriptionResponse",
            Self::L2Book(_) => "l2Book",
            Self::L4Book(_) => "l4Book",
            Self::Trades(_) => "trades",
            Self::Bbo(_) => "bbo",
            Self::BookDiffs(_) => "bookDiffs",
            Self::OrderUpdates(_) => "orderUpdates",
            Self::Pong => "pong",
            Self::Error(_) => "error",
        }
    }
}

pub struct SubscriptionManager {
    transport: TransportKind,
    subscriptions: HashSet<Subscription>,
    subscriptions_by_type: HashMap<SubscriptionKind, Vec<Subscription>>,
    subscriptions_by_coin: HashMap<SubscriptionKind, HashMap<String, Vec<Subscription>>>,
    order_updates_by_user: HashMap<Address, Vec<Subscription>>,
}

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
pub enum SubscriptionKind {
    Bbo,
    L2Book,
    L4Book,
    Trades,
    OrderUpdates,
    BookDiffs,
}

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
pub enum FanoutChannel {
    L2Book,
    Bbo,
    Trades,
    BookDiffs,
    OrderStatuses,
}

impl FanoutChannel {
    pub const fn label(self) -> &'static str {
        match self {
            Self::L2Book => "l2Book",
            Self::Bbo => "bbo",
            Self::Trades => "trades",
            Self::BookDiffs => "bookDiffs",
            Self::OrderStatuses => "orderStatuses",
        }
    }
}

impl SubscriptionKind {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Bbo => "bbo",
            Self::L2Book => "l2Book",
            Self::L4Book => "l4Book",
            Self::Trades => "trades",
            Self::OrderUpdates => "orderUpdates",
            Self::BookDiffs => "bookDiffs",
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum TransportKind {
    Websocket,
    Grpc,
    Unknown,
}

impl TransportKind {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Websocket => "websocket",
            Self::Grpc => "grpc",
            Self::Unknown => "unknown",
        }
    }

    pub const fn uses_legacy_websocket_metrics(self) -> bool {
        matches!(self, Self::Websocket)
    }
}

impl Default for SubscriptionManager {
    fn default() -> Self {
        Self::new(TransportKind::Unknown)
    }
}

impl SubscriptionManager {
    pub fn new(transport: TransportKind) -> Self {
        Self {
            transport,
            subscriptions: HashSet::new(),
            subscriptions_by_type: HashMap::new(),
            subscriptions_by_coin: HashMap::new(),
            order_updates_by_user: HashMap::new(),
        }
    }

    /// Tries to add the subscription. Returns `Err` once the per-connection cap
    /// is reached, distinguishing "already subscribed" (Ok(false)) from "limit hit".
    pub fn subscribe(&mut self, sub: Subscription) -> Result<bool, &'static str> {
        if self.subscriptions.len() >= MAX_SUBSCRIPTIONS_PER_CONNECTION && !self.subscriptions.contains(&sub) {
            return Err("subscription limit reached for this connection");
        }
        let kind = sub.kind();
        let coin_key = sub.coin_key().map(str::to_string);
        let user_key = sub.user_key();
        let inserted = self.subscriptions.insert(sub.clone());
        if inserted {
            // Store a second copy by type so fan-out paths can skip unrelated
            // subscription classes. Type buckets are Vec-backed because they are
            // read on every broadcast; the main HashSet above still handles
            // duplicate detection, and the per-connection cap bounds removal cost.
            self.subscriptions_by_type.entry(kind).or_default().push(sub.clone());
            if let Some(coin_key) = coin_key {
                self.subscriptions_by_coin.entry(kind).or_default().entry(coin_key).or_default().push(sub);
            } else if let Some(user_key) = user_key {
                self.order_updates_by_user.entry(user_key).or_default().push(sub);
            }
            TRANSPORT_SUBSCRIPTIONS_ACTIVE.with_label_values(&[self.transport.label(), kind.label()]).inc();
            if self.transport.uses_legacy_websocket_metrics() {
                WS_SUBSCRIPTIONS_ACTIVE.with_label_values(&[kind.label()]).inc();
            }
        }
        Ok(inserted)
    }

    pub fn unsubscribe(&mut self, sub: Subscription) -> bool {
        let kind = sub.kind();
        let removed = self.subscriptions.remove(&sub);
        if removed {
            if let Some(by_type) = self.subscriptions_by_type.get_mut(&kind) {
                by_type.retain(|candidate| candidate != &sub);
                if by_type.is_empty() {
                    self.subscriptions_by_type.remove(&kind);
                }
            }
            if let Some(coin_key) = sub.coin_key()
                && let Some(by_coin) = self.subscriptions_by_coin.get_mut(&kind)
            {
                if let Some(subscriptions) = by_coin.get_mut(coin_key) {
                    subscriptions.retain(|candidate| candidate != &sub);
                    if subscriptions.is_empty() {
                        by_coin.remove(coin_key);
                    }
                }
                if by_coin.is_empty() {
                    self.subscriptions_by_coin.remove(&kind);
                }
            } else if let Some(user_key) = sub.user_key() {
                if let Some(subscriptions) = self.order_updates_by_user.get_mut(&user_key) {
                    subscriptions.retain(|candidate| candidate != &sub);
                    if subscriptions.is_empty() {
                        self.order_updates_by_user.remove(&user_key);
                    }
                }
            }
            TRANSPORT_SUBSCRIPTIONS_ACTIVE.with_label_values(&[self.transport.label(), kind.label()]).dec();
            if self.transport.uses_legacy_websocket_metrics() {
                WS_SUBSCRIPTIONS_ACTIVE.with_label_values(&[kind.label()]).dec();
            }
        }
        removed
    }

    pub const fn subscriptions(&self) -> &HashSet<Subscription> {
        &self.subscriptions
    }

    pub fn subscriptions_for_type(&self, kind: SubscriptionKind) -> impl Iterator<Item = &Subscription> {
        self.subscriptions_by_type.get(&kind).into_iter().flat_map(|subscriptions| subscriptions.iter())
    }

    pub fn subscriptions_for_coin(&self, kind: SubscriptionKind, coin: &str) -> impl Iterator<Item = &Subscription> {
        self.subscriptions_by_coin
            .get(&kind)
            .and_then(|subscriptions| subscriptions.get(coin))
            .into_iter()
            .flat_map(|subscriptions| subscriptions.iter())
    }

    pub fn order_update_subscriptions_for_user(&self, user: Address) -> impl Iterator<Item = &Subscription> {
        self.order_updates_by_user.get(&user).into_iter().flat_map(|subscriptions| subscriptions.iter())
    }

    pub fn subscription_count_for_type(&self, kind: SubscriptionKind) -> usize {
        self.subscriptions_by_type.get(&kind).map_or(0, Vec::len)
    }

    pub fn active_subscriptions_for_fanout(&self, channel: FanoutChannel) -> usize {
        match channel {
            FanoutChannel::L2Book => self.subscription_count_for_type(SubscriptionKind::L2Book),
            FanoutChannel::Bbo => self.subscription_count_for_type(SubscriptionKind::Bbo),
            FanoutChannel::Trades => self.subscription_count_for_type(SubscriptionKind::Trades),
            FanoutChannel::BookDiffs => self
                .subscription_count_for_type(SubscriptionKind::BookDiffs)
                .saturating_add(self.subscription_count_for_type(SubscriptionKind::L4Book)),
            FanoutChannel::OrderStatuses => self
                .subscription_count_for_type(SubscriptionKind::L4Book)
                .saturating_add(self.subscription_count_for_type(SubscriptionKind::OrderUpdates)),
        }
    }
}

impl Drop for SubscriptionManager {
    fn drop(&mut self) {
        for sub in &self.subscriptions {
            let kind = sub.kind();
            TRANSPORT_SUBSCRIPTIONS_ACTIVE.with_label_values(&[self.transport.label(), kind.label()]).dec();
            if self.transport.uses_legacy_websocket_metrics() {
                WS_SUBSCRIPTIONS_ACTIVE.with_label_values(&[kind.label()]).dec();
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::types::node_data::NodeDataOrderDiff;
    use crate::types::subscription::Subscription;

    use super::{ClientMessage, ServerResponse};

    #[test]
    fn test_message_deserialization_subscription_response() {
        let message = r#"
            {"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC","nSigFigs":null,"mantissa":null}}}
        "#;
        let msg = serde_json::from_str(message).unwrap();
        assert!(matches!(msg, ServerResponse::SubscriptionResponse(_)));
    }

    #[test]
    fn test_message_deserialization_l2book() {
        let message = r#"
            {"channel":"l2Book","data":{"coin":"BTC","time":1751427259657,"levels":[[{"px":"106217.0","sz":"0.001","n":1},{"px":"106215.0","sz":"0.001","n":1},{"px":"106213.0","sz":"0.27739","n":1},{"px":"106193.0","sz":"0.49943","n":1},{"px":"106190.0","sz":"0.52899","n":1},{"px":"106162.0","sz":"0.55931","n":1},{"px":"106160.0","sz":"0.55023","n":1},{"px":"106140.0","sz":"0.001","n":1},{"px":"106137.0","sz":"0.001","n":1},{"px":"106131.0","sz":"0.001","n":1},{"px":"106111.0","sz":"0.01094","n":1},{"px":"106085.0","sz":"1.02207","n":2},{"px":"105916.0","sz":"0.001","n":1},{"px":"105913.0","sz":"1.01927","n":2},{"px":"105822.0","sz":"0.00474","n":1},{"px":"105698.0","sz":"0.51012","n":1},{"px":"105696.0","sz":"0.001","n":1},{"px":"105604.0","sz":"0.55072","n":1},{"px":"105579.0","sz":"0.00217","n":1},{"px":"105543.0","sz":"0.0197","n":1}],[{"px":"106233.0","sz":"0.26739","n":3},{"px":"106258.0","sz":"0.001","n":1},{"px":"106270.0","sz":"0.49128","n":2},{"px":"106306.0","sz":"0.27263","n":1},{"px":"106311.0","sz":"0.23837","n":1},{"px":"106350.0","sz":"0.001","n":1},{"px":"106396.0","sz":"0.24733","n":1},{"px":"106414.0","sz":"0.27088","n":1},{"px":"106560.0","sz":"0.0001","n":1},{"px":"106597.0","sz":"0.56981","n":1},{"px":"106637.0","sz":"0.57002","n":1},{"px":"106932.0","sz":"0.001","n":1},{"px":"107012.0","sz":"1.06873","n":2},{"px":"107094.0","sz":"0.0041","n":1},{"px":"107360.0","sz":"0.001","n":1},{"px":"107535.0","sz":"0.002","n":1},{"px":"107638.0","sz":"0.001","n":1},{"px":"107639.0","sz":"0.0007","n":1},{"px":"107650.0","sz":"0.00074","n":1},{"px":"107675.0","sz":"0.00083","n":1}]]}}
        "#;
        let msg: ServerResponse = serde_json::from_str(message).unwrap();
        assert!(matches!(msg, ServerResponse::L2Book(_)));
    }

    #[test]
    fn test_message_deserialization_trade() {
        let message = r#"
            {"channel":"trades","data":[{"coin":"BTC","side":"A","px":"106296.0","sz":"0.00017","time":1751430933565,"hash":"0xde93a8a0729ade63d8840417805ba9010b008818422ddedb1285744426b73503","tid":293353986402527,"users":["0xcc0a3b6e3267c84361e91d8230868eea53431e4b","0x010461c14e146ac35fe42271bdc1134ee31c703a"]}]}
        "#;
        let msg: ServerResponse = serde_json::from_str(message).unwrap();
        assert!(matches!(msg, ServerResponse::Trades(_)));
    }

    #[test]
    fn test_arc_payloads_serialize_identically_to_vec() {
        // The fan-out payloads are Arc-shared across connections; serde's "rc"
        // feature must keep Arc<Vec<T>> byte-identical to Vec<T> on the wire -
        // the JSON format is part of the public API.
        let trades_json = r#"[{"coin":"BTC","side":"A","px":"106296.0","sz":"0.00017","time":1751430933565,"hash":"0xde93a8a0729ade63d8840417805ba9010b008818422ddedb1285744426b73503","tid":293353986402527,"users":["0xcc0a3b6e3267c84361e91d8230868eea53431e4b","0x010461c14e146ac35fe42271bdc1134ee31c703a"]}]"#;
        let plain: Vec<crate::types::Trade> = serde_json::from_str(trades_json).unwrap();
        let arced: std::sync::Arc<Vec<crate::types::Trade>> = serde_json::from_str(trades_json).unwrap();
        assert_eq!(serde_json::to_string(&plain).unwrap(), serde_json::to_string(&arced).unwrap());

        let diffs_json = r#"[{"user":"0x0000000000000000000000000000000000000001","oid":123,"px":"50000.0","coin":"BTC","raw_book_diff":{"new":{"sz":"1.5"}}}]"#;
        let plain: Vec<NodeDataOrderDiff> = serde_json::from_str(diffs_json).unwrap();
        let arced: std::sync::Arc<Vec<NodeDataOrderDiff>> = serde_json::from_str(diffs_json).unwrap();
        assert_eq!(serde_json::to_string(&plain).unwrap(), serde_json::to_string(&arced).unwrap());
    }

    #[test]
    fn test_client_message_deserialization() {
        let message = r#"
            { "method": "subscribe", "subscription":{ "type": "l2Book", "coin": "BTC" }}
        "#;
        let msg: ClientMessage = serde_json::from_str(message).unwrap();
        assert!(matches!(
            msg,
            ClientMessage::Subscribe {
                subscription: Subscription::L2Book { n_sig_figs: None, n_levels: None, mantissa: None, .. },
            }
        ));
    }

    #[test]
    fn test_order_updates_subscription_deserialization() {
        let message = r#"
            { "method": "subscribe", "subscription":{ "type": "orderUpdates", "user": "0xABc1234567890abcDEF1234567890AbCdEf12345" }}
        "#;
        let msg: ClientMessage = serde_json::from_str(message).unwrap();
        assert!(matches!(msg, ClientMessage::Subscribe { subscription: Subscription::OrderUpdates { .. } }));
        if let ClientMessage::Subscribe { subscription: Subscription::OrderUpdates { user } } = msg {
            assert_eq!(user, "0xABc1234567890abcDEF1234567890AbCdEf12345");
        }
    }

    #[test]
    fn test_book_diffs_subscription_deserialization() {
        let message = r#"
            { "method": "subscribe", "subscription":{ "type": "bookDiffs", "coin": "BTC" }}
        "#;
        let msg: ClientMessage = serde_json::from_str(message).unwrap();
        assert!(matches!(msg, ClientMessage::Subscribe { subscription: Subscription::BookDiffs { .. } }));
        if let ClientMessage::Subscribe { subscription: Subscription::BookDiffs { coin } } = msg {
            assert_eq!(coin, "BTC");
        }
    }

    // ==================== Subscription Validation Tests ====================

    fn universe() -> std::collections::HashSet<String> {
        ["BTC", "ETH", "SOL", "@1", "PURR/USDC", "flx:COIN"].iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn test_validate_trades_valid_coin() {
        assert!(Subscription::Trades { coin: "BTC".to_string() }.validate(&universe()));
    }

    #[test]
    fn test_validate_trades_invalid_coin() {
        assert!(!Subscription::Trades { coin: "FAKE".to_string() }.validate(&universe()));
    }

    #[test]
    fn test_validate_bbo_valid() {
        assert!(Subscription::Bbo { coin: "ETH".to_string() }.validate(&universe()));
    }

    #[test]
    fn test_validate_bbo_invalid_coin() {
        assert!(!Subscription::Bbo { coin: "NOPE".to_string() }.validate(&universe()));
    }

    #[test]
    fn test_validate_book_diffs_valid() {
        assert!(Subscription::BookDiffs { coin: "BTC".to_string() }.validate(&universe()));
    }

    #[test]
    fn test_validate_book_diffs_invalid_coin() {
        assert!(!Subscription::BookDiffs { coin: "FAKE".to_string() }.validate(&universe()));
    }

    #[test]
    fn test_validate_l4book_valid() {
        assert!(Subscription::L4Book { coin: "SOL".to_string() }.validate(&universe()));
    }

    #[test]
    fn test_validate_l2book_defaults_valid() {
        let sub = Subscription::L2Book { coin: "BTC".to_string(), n_sig_figs: None, n_levels: None, mantissa: None };
        assert!(sub.validate(&universe()));
    }

    #[test]
    fn test_validate_l2book_n_levels_at_default_rejected() {
        // Setting n_levels to DEFAULT_LEVELS (20) explicitly is rejected
        let sub =
            Subscription::L2Book { coin: "BTC".to_string(), n_sig_figs: None, n_levels: Some(20), mantissa: None };
        assert!(!sub.validate(&universe()));
    }

    #[test]
    fn test_validate_l2book_n_levels_over_max() {
        let sub =
            Subscription::L2Book { coin: "BTC".to_string(), n_sig_figs: None, n_levels: Some(101), mantissa: None };
        assert!(!sub.validate(&universe()));
    }

    #[test]
    fn test_validate_l2book_n_levels_at_max() {
        let sub =
            Subscription::L2Book { coin: "BTC".to_string(), n_sig_figs: None, n_levels: Some(100), mantissa: None };
        assert!(sub.validate(&universe()));
    }

    #[test]
    fn test_validate_l2book_sig_figs_valid_range() {
        for sf in 2..=5 {
            let sub =
                Subscription::L2Book { coin: "BTC".to_string(), n_sig_figs: Some(sf), n_levels: None, mantissa: None };
            assert!(sub.validate(&universe()), "sig_figs={sf} should be valid");
        }
    }

    #[test]
    fn test_validate_l2book_sig_figs_out_of_range() {
        for sf in [0, 1, 6, 10] {
            let sub =
                Subscription::L2Book { coin: "BTC".to_string(), n_sig_figs: Some(sf), n_levels: None, mantissa: None };
            assert!(!sub.validate(&universe()), "sig_figs={sf} should be invalid");
        }
    }

    #[test]
    fn test_validate_l2book_mantissa_without_sig_figs() {
        let sub = Subscription::L2Book { coin: "BTC".to_string(), n_sig_figs: None, n_levels: None, mantissa: Some(5) };
        assert!(!sub.validate(&universe()));
    }

    #[test]
    fn test_validate_l2book_mantissa_valid_with_sig_figs_5() {
        for m in [2, 5] {
            let sub = Subscription::L2Book {
                coin: "BTC".to_string(),
                n_sig_figs: Some(5),
                n_levels: None,
                mantissa: Some(m),
            };
            assert!(sub.validate(&universe()), "mantissa={m} with sig_figs=5 should be valid");
        }
    }

    #[test]
    fn test_validate_l2book_mantissa_invalid_value() {
        let sub =
            Subscription::L2Book { coin: "BTC".to_string(), n_sig_figs: Some(5), n_levels: None, mantissa: Some(3) };
        assert!(!sub.validate(&universe()));
    }

    #[test]
    fn test_validate_l2book_mantissa_invalid_with_low_sig_figs() {
        let sub =
            Subscription::L2Book { coin: "BTC".to_string(), n_sig_figs: Some(3), n_levels: None, mantissa: Some(5) };
        assert!(!sub.validate(&universe()));
    }

    #[test]
    fn test_validate_order_updates_valid_address() {
        let sub = Subscription::OrderUpdates { user: "0xABcDEF1234567890abcdef1234567890AbCdEf12".to_string() };
        assert!(sub.validate(&universe()));
    }

    #[test]
    fn test_validate_order_updates_too_short() {
        let sub = Subscription::OrderUpdates { user: "0xABC".to_string() };
        assert!(!sub.validate(&universe()));
    }

    #[test]
    fn test_validate_order_updates_no_prefix() {
        let sub = Subscription::OrderUpdates { user: "ABcDEF1234567890abcdef1234567890AbCdEf1234".to_string() };
        assert!(!sub.validate(&universe()));
    }

    #[test]
    fn test_validate_order_updates_invalid_hex() {
        let sub = Subscription::OrderUpdates { user: "0xGGGGGG1234567890abcdef1234567890AbCdEf12".to_string() };
        assert!(!sub.validate(&universe()));
    }

    // ==================== type_label Tests ====================

    #[test]
    fn test_type_labels() {
        assert_eq!(Subscription::Bbo { coin: "".to_string() }.type_label(), "bbo");
        assert_eq!(
            Subscription::L2Book { coin: "".to_string(), n_sig_figs: None, n_levels: None, mantissa: None }
                .type_label(),
            "l2Book"
        );
        assert_eq!(Subscription::L4Book { coin: "".to_string() }.type_label(), "l4Book");
        assert_eq!(Subscription::Trades { coin: "".to_string() }.type_label(), "trades");
        assert_eq!(Subscription::OrderUpdates { user: "".to_string() }.type_label(), "orderUpdates");
        assert_eq!(Subscription::BookDiffs { coin: "".to_string() }.type_label(), "bookDiffs");
    }

    // ==================== SubscriptionManager Tests ====================

    #[test]
    fn test_subscribe_returns_true_on_new() {
        let mut mgr = super::SubscriptionManager::default();
        assert_eq!(mgr.subscribe(Subscription::Bbo { coin: "BTC".to_string() }), Ok(true));
    }

    #[test]
    fn test_subscribe_returns_false_on_duplicate() {
        let mut mgr = super::SubscriptionManager::default();
        let _ = mgr.subscribe(Subscription::Bbo { coin: "BTC".to_string() });
        assert_eq!(mgr.subscribe(Subscription::Bbo { coin: "BTC".to_string() }), Ok(false));
    }

    #[test]
    fn test_unsubscribe_returns_true_when_exists() {
        let mut mgr = super::SubscriptionManager::default();
        let _ = mgr.subscribe(Subscription::Trades { coin: "ETH".to_string() });
        assert!(mgr.unsubscribe(Subscription::Trades { coin: "ETH".to_string() }));
    }

    #[test]
    fn test_unsubscribe_returns_false_when_not_exists() {
        let mut mgr = super::SubscriptionManager::default();
        assert!(!mgr.unsubscribe(Subscription::Trades { coin: "ETH".to_string() }));
    }

    #[test]
    fn test_subscriptions_list() {
        let mut mgr = super::SubscriptionManager::default();
        let _ = mgr.subscribe(Subscription::Bbo { coin: "BTC".to_string() });
        let _ = mgr.subscribe(Subscription::Trades { coin: "ETH".to_string() });
        assert_eq!(mgr.subscriptions().len(), 2);
    }

    #[test]
    fn test_keyed_subscriptions_filter_by_kind_coin_and_user() {
        let mut mgr = super::SubscriptionManager::default();
        let _ = mgr.subscribe(Subscription::Trades { coin: "BTC".to_string() });
        let _ = mgr.subscribe(Subscription::Trades { coin: "ETH".to_string() });
        let _ = mgr.subscribe(Subscription::BookDiffs { coin: "BTC".to_string() });
        let _ = mgr.subscribe(Subscription::L4Book { coin: "BTC".to_string() });
        let _ = mgr.subscribe(Subscription::Bbo { coin: "BTC".to_string() });
        let user = "0xABcDEF1234567890abcdef1234567890AbCdEf12".parse().unwrap();
        let _ = mgr
            .subscribe(Subscription::OrderUpdates { user: "0xABcDEF1234567890abcdef1234567890AbCdEf12".to_string() });

        assert_eq!(mgr.active_subscriptions_for_fanout(super::FanoutChannel::Bbo), 1);
        assert_eq!(mgr.active_subscriptions_for_fanout(super::FanoutChannel::Trades), 2);
        assert_eq!(mgr.active_subscriptions_for_fanout(super::FanoutChannel::BookDiffs), 2);
        assert_eq!(mgr.active_subscriptions_for_fanout(super::FanoutChannel::OrderStatuses), 2);

        let btc_trades: Vec<_> = mgr.subscriptions_for_coin(super::SubscriptionKind::Trades, "BTC").collect();
        assert_eq!(btc_trades.len(), 1);
        assert!(matches!(btc_trades[0], Subscription::Trades { coin } if coin == "BTC"));

        let eth_book_diffs: Vec<_> = mgr.subscriptions_for_coin(super::SubscriptionKind::BookDiffs, "ETH").collect();
        assert!(eth_book_diffs.is_empty());

        assert_eq!(mgr.order_update_subscriptions_for_user(user).count(), 1);

        assert!(mgr.unsubscribe(Subscription::Trades { coin: "BTC".to_string() }));
        assert_eq!(mgr.subscriptions_for_coin(super::SubscriptionKind::Trades, "BTC").count(), 0);
        assert_eq!(mgr.subscriptions_for_coin(super::SubscriptionKind::Trades, "ETH").count(), 1);
    }

    #[test]
    fn test_subscribe_enforces_per_connection_cap() {
        // C4 regression test: fill the manager up to the cap and verify the next
        // distinct subscription is rejected, while re-subscribing to something already
        // present succeeds (idempotent, would otherwise lock people out of resubs).
        let mut mgr = super::SubscriptionManager::default();
        for i in 0..super::MAX_SUBSCRIPTIONS_PER_CONNECTION {
            let res = mgr.subscribe(Subscription::Trades { coin: format!("COIN{i}") });
            assert_eq!(res, Ok(true), "subscribe #{i} should succeed");
        }
        // One more distinct sub - rejected.
        let res = mgr.subscribe(Subscription::Trades { coin: "OVERFLOW".to_string() });
        assert!(res.is_err());
        // Re-subscribing to one we already have is OK (returns Ok(false) = no-op).
        let res = mgr.subscribe(Subscription::Trades { coin: "COIN0".to_string() });
        assert_eq!(res, Ok(false));
    }

    // ==================== ServerResponse Serde Tests ====================

    #[test]
    fn test_server_response_pong_serialization() {
        let json = serde_json::to_string(&super::ServerResponse::Pong).unwrap();
        assert_eq!(json, r#"{"channel":"pong"}"#);
    }

    #[test]
    fn test_server_response_error_serialization() {
        let json = serde_json::to_string(&super::ServerResponse::Error("test error".to_string())).unwrap();
        assert!(json.contains("test error"));
        assert!(json.contains("error"));
    }

    #[test]
    fn test_server_response_bbo_serialization() {
        let bbo = crate::types::Bbo {
            coin: "BTC".to_string(),
            time: 1000,
            bid: Some(crate::types::Level::new("100".to_string(), "1.5".to_string(), 2)),
            ask: None,
        };
        let json = serde_json::to_string(&super::ServerResponse::Bbo(bbo)).unwrap();
        assert!(json.contains("\"channel\":\"bbo\""));
        assert!(json.contains("BTC"));
    }

    // ==================== ClientMessage Serde Tests ====================

    #[test]
    fn test_all_subscription_types_deserialize() {
        let cases = [
            (r#"{"method":"subscribe","subscription":{"type":"trades","coin":"BTC"}}"#, "trades"),
            (r#"{"method":"subscribe","subscription":{"type":"bbo","coin":"BTC"}}"#, "bbo"),
            (r#"{"method":"subscribe","subscription":{"type":"l4Book","coin":"BTC"}}"#, "l4Book"),
            (r#"{"method":"subscribe","subscription":{"type":"bookDiffs","coin":"BTC"}}"#, "bookDiffs"),
            (r#"{"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC"}}"#, "l2Book"),
            (
                r#"{"method":"subscribe","subscription":{"type":"orderUpdates","user":"0xABcDEF1234567890abcdef1234567890AbCdEf12"}}"#,
                "orderUpdates",
            ),
        ];
        for (json, label) in cases {
            let msg: ClientMessage = serde_json::from_str(json).expect(&format!("failed to parse {label}"));
            assert!(matches!(msg, ClientMessage::Subscribe { .. }), "expected Subscribe for {label}");
        }
    }

    #[test]
    fn test_unsubscribe_deserialization() {
        let json = r#"{"method":"unsubscribe","subscription":{"type":"trades","coin":"BTC"}}"#;
        let msg: ClientMessage = serde_json::from_str(json).unwrap();
        assert!(matches!(msg, ClientMessage::Unsubscribe { .. }));
    }

    #[test]
    fn test_ping_pong() {
        let message = r#"{ "method": "ping" }"#;
        let msg: ClientMessage = serde_json::from_str(message).unwrap();
        assert!(matches!(msg, ClientMessage::Ping));

        let response = serde_json::to_string(&ServerResponse::Pong).unwrap();
        assert_eq!(response, r#"{"channel":"pong"}"#);
    }
}
