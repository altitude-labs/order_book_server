use crate::{
    metrics::{PayloadTimestampKind, observe_transport_payload_egress_age},
    transport::{SubscriptionKind, TransportKind},
    types::node_data::NodeDataOrderStatus,
};

/// Precomputed max timestamps embedded in grouped order-status payloads.
/// Transports use this for egress-age metrics without scanning the same status
/// vector once per subscribed connection.
#[derive(Clone, Copy, Debug, Default)]
pub struct OrderStatusPayloadTimestamps {
    status_time_ms: Option<u64>,
    order_timestamp_ms: Option<u64>,
}

impl OrderStatusPayloadTimestamps {
    #[must_use]
    pub const fn status_time_ms(self) -> Option<u64> {
        self.status_time_ms
    }

    #[must_use]
    pub const fn order_timestamp_ms(self) -> Option<u64> {
        self.order_timestamp_ms
    }

    pub fn observe_transport_egress_age(self, transport: TransportKind, channel: SubscriptionKind) {
        if let Some(event_time_ms) = self.status_time_ms {
            observe_transport_payload_egress_age(transport, channel, PayloadTimestampKind::StatusTime, event_time_ms);
        }
        if let Some(event_time_ms) = self.order_timestamp_ms {
            observe_transport_payload_egress_age(
                transport,
                channel,
                PayloadTimestampKind::OrderTimestamp,
                event_time_ms,
            );
        }
    }
}

pub(crate) trait OrderStatusTimestampSlice {
    fn payload_timestamps(&self) -> OrderStatusPayloadTimestamps;
}

impl OrderStatusTimestampSlice for [NodeDataOrderStatus] {
    fn payload_timestamps(&self) -> OrderStatusPayloadTimestamps {
        OrderStatusPayloadTimestamps {
            status_time_ms: self.iter().map(NodeDataOrderStatus::time_ms).max(),
            order_timestamp_ms: self.iter().map(NodeDataOrderStatus::order_timestamp_ms).max(),
        }
    }
}
