#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::hash_map::Entry;

use serde_json::{Number, Value};
use tqsdk_core::{CommandId, Order, OrderId, TradeDirection, TradeOffset};

use crate::{
    OrderRef, TqApi, WaitFacadeError, api::WaitInsertOrderRequest, driver::SubmittedOrderIntent,
};

/// User-supplied idempotency key for an order intent.
///
/// The wait facade maps this id to the runtime `order_id`, so reconnect/retry
/// code can look up the same order instead of blindly submitting a new one.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ClientOrderId(String);

impl ClientOrderId {
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for ClientOrderId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for ClientOrderId {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

/// Builder for a reconnect-friendly limit order intent.
///
/// This is still a thin wait-facade helper: it submits a runtime trade command
/// and returns an [`OrderRef`]-backed ticket. It does not create local order
/// state or bypass runtime order lifecycle checks.
pub struct LimitOrderIntent<'a> {
    api: &'a mut TqApi,
    account_id: String,
    symbol: String,
    client_order_id: Option<ClientOrderId>,
    direction: Option<TradeDirection>,
    offset: Option<TradeOffset>,
    volume: Option<i64>,
    limit_price: Option<f64>,
}

impl<'a> LimitOrderIntent<'a> {
    #[must_use]
    pub(crate) fn new(
        api: &'a mut TqApi,
        account_id: impl Into<String>,
        symbol: impl Into<String>,
    ) -> Self {
        Self {
            api,
            account_id: account_id.into(),
            symbol: symbol.into(),
            client_order_id: None,
            direction: None,
            offset: None,
            volume: None,
            limit_price: None,
        }
    }

    #[must_use]
    pub fn client_intent(mut self, id: impl Into<ClientOrderId>) -> Self {
        self.client_order_id = Some(id.into());
        self
    }

    #[must_use]
    pub fn client_order_id(self, id: impl Into<ClientOrderId>) -> Self {
        self.client_intent(id)
    }

    #[must_use]
    pub fn side(mut self, direction: TradeDirection, offset: TradeOffset, volume: i64) -> Self {
        self.direction = Some(direction);
        self.offset = Some(offset);
        self.volume = Some(volume);
        self
    }

    #[must_use]
    pub fn buy_open(self, volume: i64) -> Self {
        self.side(TradeDirection::Buy, TradeOffset::Open, volume)
    }

    #[must_use]
    pub fn sell_open(self, volume: i64) -> Self {
        self.side(TradeDirection::Sell, TradeOffset::Open, volume)
    }

    #[must_use]
    pub fn buy_close(self, volume: i64) -> Self {
        self.side(TradeDirection::Buy, TradeOffset::Close, volume)
    }

    #[must_use]
    pub fn sell_close(self, volume: i64) -> Self {
        self.side(TradeDirection::Sell, TradeOffset::Close, volume)
    }

    #[must_use]
    pub fn at(mut self, limit_price: f64) -> Self {
        self.limit_price = Some(limit_price);
        self
    }

    pub async fn send_once(self) -> crate::error::Result<OrderTicket> {
        let client_order_id = self.client_order_id.ok_or(WaitFacadeError::InvalidState(
            "client intent id is required",
        ))?;
        if client_order_id.as_str().trim().is_empty() {
            return Err(WaitFacadeError::InvalidState(
                "client intent id must not be empty",
            ));
        }

        let direction = self
            .direction
            .ok_or(WaitFacadeError::InvalidState("order direction is required"))?;
        let offset = self.offset;
        let volume = self
            .volume
            .ok_or(WaitFacadeError::InvalidState("order volume is required"))?;
        if volume <= 0 {
            return Err(WaitFacadeError::InvalidState(
                "order volume must be positive",
            ));
        }

        let price = self
            .limit_price
            .ok_or(WaitFacadeError::InvalidState("limit price is required"))?;
        let limit_price = Number::from_f64(price)
            .ok_or(WaitFacadeError::InvalidState("limit price must be finite"))?;

        let order_id = client_order_id.as_str().to_owned();
        let order = self.api.get_order(&self.account_id, &order_id);
        if order.snapshot(self.api)?.is_some() {
            return Ok(OrderTicket::new(client_order_id, order, None, false));
        }

        let submitted_intent = SubmittedOrderIntent {
            symbol: self.symbol.clone(),
            direction,
            offset,
            volume,
            limit_price: price,
        };
        let intent_key = (self.account_id.clone(), order_id.clone());
        match self
            .api
            .driver
            .submitted_order_intents
            .entry(intent_key.clone())
        {
            Entry::Occupied(entry) => {
                if entry.get() != &submitted_intent {
                    return Err(WaitFacadeError::InvalidState(
                        "client intent id was already submitted with different order fields",
                    ));
                }
                return Ok(OrderTicket::new(client_order_id, order, None, false));
            }
            Entry::Vacant(entry) => {
                entry.insert(submitted_intent);
            }
        }

        let command = self
            .api
            .submit_insert_order(WaitInsertOrderRequest {
                account_id: self.account_id.clone(),
                symbol: self.symbol.clone(),
                order_id: OrderId::new(order_id),
                direction,
                offset,
                volume,
                limit_price: Some(Value::Number(limit_price)),
            })
            .await;

        match command {
            Ok(command_id) => Ok(OrderTicket::new(
                client_order_id,
                order,
                Some(command_id),
                true,
            )),
            Err(error) => {
                self.api.driver.submitted_order_intents.remove(&intent_key);
                Err(error)
            }
        }
    }
}

/// Ticket returned after submitting or recovering an order intent.
#[derive(Debug, Clone)]
pub struct OrderTicket {
    client_order_id: ClientOrderId,
    order: OrderRef,
    command_id: Option<CommandId>,
    submitted: bool,
}

impl OrderTicket {
    #[must_use]
    fn new(
        client_order_id: ClientOrderId,
        order: OrderRef,
        command_id: Option<CommandId>,
        submitted: bool,
    ) -> Self {
        Self {
            client_order_id,
            order,
            command_id,
            submitted,
        }
    }

    #[must_use]
    pub fn client_order_id(&self) -> &str {
        self.client_order_id.as_str()
    }

    #[must_use]
    pub fn order(&self) -> &OrderRef {
        &self.order
    }

    #[must_use]
    pub fn into_order(self) -> OrderRef {
        self.order
    }

    #[must_use]
    pub fn command_id(&self) -> Option<CommandId> {
        self.command_id
    }

    #[must_use]
    pub fn was_submitted(&self) -> bool {
        self.submitted
    }

    pub async fn wait_terminal(&self, api: &mut TqApi) -> crate::error::Result<Order> {
        self.order.wait_terminal(api).await
    }

    pub async fn wait_terminal_until(
        &self,
        api: &mut TqApi,
        deadline: tokio::time::Instant,
    ) -> crate::error::Result<Order> {
        self.order.wait_terminal_until(api, deadline).await
    }
}
