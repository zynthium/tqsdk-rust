use std::str::FromStr;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderLifecycle {
    #[default]
    Unknown,
    Submitting,
    Sent,
    Accepted,
    PartiallyFilled,
    Filled,
    Rejected,
    Cancelling,
    Cancelled,
    Failed,
}

impl OrderLifecycle {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Submitting => "submitting",
            Self::Sent => "sent",
            Self::Accepted => "accepted",
            Self::PartiallyFilled => "partially_filled",
            Self::Filled => "filled",
            Self::Rejected => "rejected",
            Self::Cancelling => "cancelling",
            Self::Cancelled => "cancelled",
            Self::Failed => "failed",
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Filled | Self::Rejected | Self::Cancelled | Self::Failed
        )
    }

    pub fn can_transition_to(self, next: Self) -> bool {
        if self == next {
            return true;
        }

        match self {
            Self::Unknown => true,
            Self::Submitting => matches!(
                next,
                Self::Sent
                    | Self::Accepted
                    | Self::PartiallyFilled
                    | Self::Filled
                    | Self::Rejected
                    | Self::Cancelling
                    | Self::Cancelled
                    | Self::Failed
            ),
            Self::Sent => matches!(
                next,
                Self::Accepted
                    | Self::PartiallyFilled
                    | Self::Filled
                    | Self::Rejected
                    | Self::Cancelling
                    | Self::Cancelled
                    | Self::Failed
            ),
            Self::Accepted => matches!(
                next,
                Self::PartiallyFilled
                    | Self::Filled
                    | Self::Rejected
                    | Self::Cancelling
                    | Self::Cancelled
                    | Self::Failed
            ),
            Self::PartiallyFilled => matches!(
                next,
                Self::Filled | Self::Rejected | Self::Cancelling | Self::Cancelled | Self::Failed
            ),
            Self::Cancelling => {
                matches!(
                    next,
                    Self::Filled | Self::Rejected | Self::Cancelled | Self::Failed
                )
            }
            Self::Filled | Self::Rejected | Self::Cancelled | Self::Failed => false,
        }
    }

    pub fn infer_from_order_value(order: &Value) -> Option<Self> {
        let order = order.as_object()?;

        if let Some(lifecycle) = order
            .get("lifecycle")
            .and_then(Value::as_str)
            .and_then(|value| value.parse().ok())
        {
            return Some(lifecycle);
        }

        Self::infer_from_order_object(order)
    }

    pub(crate) fn infer_from_order_value_ignoring_lifecycle(order: &Value) -> Option<Self> {
        let order = order.as_object()?;
        Self::infer_from_order_object(order)
    }

    fn infer_from_order_object(order: &Map<String, Value>) -> Option<Self> {
        let status = order.get("status").and_then(Value::as_str);
        let is_dead = order.get("is_dead").and_then(Value::as_bool);
        let is_error = order.get("is_error").and_then(Value::as_bool);
        let volume_orign = order.get("volume_orign").and_then(Value::as_i64);
        let volume_left = order.get("volume_left").and_then(Value::as_i64);
        let last_msg = order.get("last_msg").and_then(Value::as_str);

        Self::infer(
            status,
            is_dead,
            is_error,
            volume_orign,
            volume_left,
            last_msg,
        )
    }

    pub fn infer(
        status: Option<&str>,
        is_dead: Option<bool>,
        is_error: Option<bool>,
        volume_orign: Option<i64>,
        volume_left: Option<i64>,
        last_msg: Option<&str>,
    ) -> Option<Self> {
        if is_error == Some(true) {
            return Some(Self::Rejected);
        }

        let normalized_status = status.map(normalize_status);
        let status = normalized_status.as_deref();

        if let Some(status) = status {
            if status.contains("REJECT") {
                return Some(Self::Rejected);
            }
            if status.contains("FAIL") || status.contains("ERROR") {
                return Some(Self::Failed);
            }
            if status.contains("CANCELLING") || status.contains("CANCELING") {
                return Some(Self::Cancelling);
            }
            if status.contains("CANCEL") {
                return Some(Self::Cancelled);
            }
            if status.contains("SUBMIT") {
                return Some(Self::Submitting);
            }
            if status.contains("SENT") {
                return Some(Self::Sent);
            }
            if status.contains("PART") {
                return Some(Self::PartiallyFilled);
            }
            if status.contains("FILL") || status.contains("ALLTRADED") {
                return Some(Self::Filled);
            }
            if is_dead == Some(true) {
                return Some(finished_state(volume_orign, volume_left, last_msg));
            }
            if status.contains("ACCEPT") || status == "ALIVE" {
                return Some(fill_aware_open_state(volume_orign, volume_left));
            }
            if status.contains("FINISH") {
                return Some(finished_state(volume_orign, volume_left, last_msg));
            }

            return Some(Self::Unknown);
        }

        if is_dead == Some(true) {
            return Some(finished_state(volume_orign, volume_left, last_msg));
        }

        volume_aware_state(volume_orign, volume_left)
    }
}

impl FromStr for OrderLifecycle {
    type Err = ();

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "unknown" => Ok(Self::Unknown),
            "submitting" => Ok(Self::Submitting),
            "sent" => Ok(Self::Sent),
            "accepted" => Ok(Self::Accepted),
            "partially_filled" => Ok(Self::PartiallyFilled),
            "filled" => Ok(Self::Filled),
            "rejected" => Ok(Self::Rejected),
            "cancelling" => Ok(Self::Cancelling),
            "cancelled" => Ok(Self::Cancelled),
            "canceled" => Ok(Self::Cancelled),
            "failed" => Ok(Self::Failed),
            _ => Err(()),
        }
    }
}

fn normalize_status(status: &str) -> String {
    status.trim().replace('-', "_").to_ascii_uppercase()
}

fn fill_aware_open_state(volume_orign: Option<i64>, volume_left: Option<i64>) -> OrderLifecycle {
    if let Some(state) = volume_aware_state(volume_orign, volume_left) {
        return state;
    }
    OrderLifecycle::Accepted
}

fn finished_state(
    volume_orign: Option<i64>,
    volume_left: Option<i64>,
    last_msg: Option<&str>,
) -> OrderLifecycle {
    if last_msg.is_some_and(message_suggests_rejection) {
        return OrderLifecycle::Rejected;
    }

    match (volume_orign, volume_left) {
        (Some(original), Some(0)) if original > 0 => OrderLifecycle::Filled,
        (Some(original), Some(left)) if original > 0 && left > 0 => OrderLifecycle::Cancelled,
        (_, Some(0)) => OrderLifecycle::Filled,
        _ => OrderLifecycle::Filled,
    }
}

fn volume_aware_state(
    volume_orign: Option<i64>,
    volume_left: Option<i64>,
) -> Option<OrderLifecycle> {
    match (volume_orign, volume_left) {
        (Some(original), Some(0)) if original > 0 => Some(OrderLifecycle::Filled),
        (Some(original), Some(left)) if original > 0 && left < original => {
            Some(OrderLifecycle::PartiallyFilled)
        }
        _ => None,
    }
}

fn message_suggests_rejection(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase();
    ["reject", "error", "invalid", "insufficient", "not enough"]
        .iter()
        .any(|needle| normalized.contains(needle))
}
