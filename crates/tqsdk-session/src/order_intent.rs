#![cfg_attr(not(test), forbid(unsafe_code))]

use tqsdk_core::{CommandId, TradeDirection, TradeOffset};

/// Session-local lifecycle for a registered client order intent.
///
/// This tracks submission ownership only. Runtime command and order state stay
/// authoritative for exchange-visible lifecycle transitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderIntentLifecycle {
    Prepared,
    Submitting,
    Submitted,
}

/// User-provided shape of an order intent before it is submitted.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderIntentSpec {
    pub account_id: String,
    pub client_order_id: String,
    pub order_id: String,
    pub symbol: String,
    pub direction: TradeDirection,
    pub offset: Option<TradeOffset>,
    pub volume: i64,
    pub limit_price: f64,
}

/// Session-scoped record for a user order intent.
///
/// This is a reconciliation substrate shared by higher facades. It is not a
/// durable store and does not replace runtime command/order lifecycle state.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderIntentRecord {
    account_id: String,
    client_order_id: String,
    order_id: String,
    symbol: String,
    direction: TradeDirection,
    offset: Option<TradeOffset>,
    volume: i64,
    limit_price: f64,
    command_id: Option<CommandId>,
    lifecycle: OrderIntentLifecycle,
}

impl OrderIntentRecord {
    #[must_use]
    pub fn new(spec: OrderIntentSpec) -> Self {
        Self {
            account_id: spec.account_id,
            client_order_id: spec.client_order_id,
            order_id: spec.order_id,
            symbol: spec.symbol,
            direction: spec.direction,
            offset: spec.offset,
            volume: spec.volume,
            limit_price: spec.limit_price,
            command_id: None,
            lifecycle: OrderIntentLifecycle::Prepared,
        }
    }

    #[must_use]
    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    #[must_use]
    pub fn client_order_id(&self) -> &str {
        &self.client_order_id
    }

    #[must_use]
    pub fn order_id(&self) -> &str {
        &self.order_id
    }

    #[must_use]
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    #[must_use]
    pub fn direction(&self) -> TradeDirection {
        self.direction
    }

    #[must_use]
    pub fn offset(&self) -> Option<TradeOffset> {
        self.offset
    }

    #[must_use]
    pub fn volume(&self) -> i64 {
        self.volume
    }

    #[must_use]
    pub fn limit_price(&self) -> f64 {
        self.limit_price
    }

    #[must_use]
    pub fn command_id(&self) -> Option<CommandId> {
        self.command_id
    }

    #[must_use]
    pub fn lifecycle(&self) -> OrderIntentLifecycle {
        self.lifecycle
    }

    #[must_use]
    pub fn request_matches(&self, other: &Self) -> bool {
        self.account_id == other.account_id
            && self.client_order_id == other.client_order_id
            && self.order_id == other.order_id
            && self.symbol == other.symbol
            && self.direction == other.direction
            && self.offset == other.offset
            && self.volume == other.volume
            && self.limit_price == other.limit_price
    }

    pub(crate) fn begin_submission(&mut self) -> bool {
        if self.lifecycle != OrderIntentLifecycle::Prepared {
            return false;
        }
        self.lifecycle = OrderIntentLifecycle::Submitting;
        true
    }

    pub(crate) fn mark_submitted(&mut self, command_id: CommandId) -> bool {
        // Keep the pre-existing update helper compatible for callers that
        // record a command synchronously, while still traversing the same
        // Prepared -> Submitting -> Submitted state machine under one lock.
        if self.lifecycle == OrderIntentLifecycle::Prepared {
            self.lifecycle = OrderIntentLifecycle::Submitting;
        }
        if self.lifecycle != OrderIntentLifecycle::Submitting {
            return false;
        }
        self.command_id = Some(command_id);
        self.lifecycle = OrderIntentLifecycle::Submitted;
        true
    }

    pub(crate) fn key(&self) -> (String, String) {
        (self.account_id.clone(), self.client_order_id.clone())
    }
}

/// Outcome of registering a session-scoped order intent.
#[derive(Debug, Clone, PartialEq)]
pub enum OrderIntentRegistration {
    Registered(OrderIntentRecord),
    Existing(OrderIntentRecord),
}
