#![cfg_attr(not(test), forbid(unsafe_code))]

use serde_json::{Number, Value};
use tqsdk_core::{
    CommandId, CommandStatus, Order, OrderId, OrderLifecycle, TradeDirection, TradeOffset,
};
use tqsdk_session::{
    OrderIntentRecord, OrderIntentRegistration, OrderIntentSpec, SessionFacadeError,
};

use crate::{OrderRef, TqApi, WaitFacadeError, api::WaitInsertOrderRequest};

/// User-supplied idempotency key for an order intent.
///
/// The wait facade maps this id to the runtime `order_id` and stores the intent
/// in the shared session ledger, so retry code can look up the same order
/// instead of blindly submitting a new one.
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

        let intent_record = OrderIntentRecord::new(OrderIntentSpec {
            account_id: self.account_id.clone(),
            client_order_id: client_order_id.as_str().to_owned(),
            order_id: client_order_id.as_str().to_owned(),
            symbol: self.symbol.clone(),
            direction,
            offset,
            volume,
            limit_price: price,
        });
        match self
            .api
            .session()
            .remember_order_intent(intent_record)
            .map_err(map_session_intent_error)?
        {
            OrderIntentRegistration::Existing(existing) => {
                return Ok(OrderTicket::new(
                    client_order_id,
                    order,
                    existing.command_id(),
                    false,
                ));
            }
            OrderIntentRegistration::Registered(_) => {}
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
            Ok(command_id) => {
                self.api
                    .session()
                    .update_order_intent_command(
                        &self.account_id,
                        client_order_id.as_str(),
                        command_id,
                    )
                    .map_err(map_session_intent_error)?;
                Ok(OrderTicket::new(
                    client_order_id,
                    order,
                    Some(command_id),
                    true,
                ))
            }
            Err(error) => {
                self.api
                    .session()
                    .forget_order_intent(&self.account_id, client_order_id.as_str())
                    .map_err(map_session_intent_error)?;
                Err(error)
            }
        }
    }
}

fn map_session_intent_error(error: SessionFacadeError) -> WaitFacadeError {
    match error {
        SessionFacadeError::InvalidState(message) => WaitFacadeError::InvalidState(message),
        other => WaitFacadeError::Session(other),
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

/// Typed status for an [`OrderTicket`].
///
/// This intentionally combines the session command ledger and the runtime order
/// object without creating a separate order state tree.
#[derive(Debug, Clone)]
pub enum OrderTicketState {
    Unknown {
        command_id: Option<CommandId>,
    },
    CommandPending {
        command_id: CommandId,
        status: CommandStatus,
    },
    Live {
        command_id: Option<CommandId>,
        order: Order,
    },
    Filled {
        command_id: Option<CommandId>,
        order: Order,
    },
    Cancelled {
        command_id: Option<CommandId>,
        order: Option<Order>,
    },
    Rejected {
        command_id: Option<CommandId>,
        order: Option<Order>,
    },
    Failed {
        command_id: Option<CommandId>,
        order: Option<Order>,
    },
}

impl OrderTicketState {
    #[must_use]
    pub fn command_id(&self) -> Option<CommandId> {
        match self {
            Self::Unknown { command_id }
            | Self::Live { command_id, .. }
            | Self::Filled { command_id, .. }
            | Self::Cancelled { command_id, .. }
            | Self::Rejected { command_id, .. }
            | Self::Failed { command_id, .. } => *command_id,
            Self::CommandPending { command_id, .. } => Some(*command_id),
        }
    }

    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Filled { .. }
                | Self::Cancelled { .. }
                | Self::Rejected { .. }
                | Self::Failed { .. }
        )
    }
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

    pub fn status(&self, api: &TqApi) -> crate::error::Result<OrderTicketState> {
        let order = self.order.snapshot(api)?;
        let command_status = self.command_status(api)?;

        match order {
            Some(order) => Ok(state_from_order(self.command_id, order)),
            None => Ok(state_from_command(self.command_id, command_status)),
        }
    }

    pub async fn cancel_remaining(&self, api: &mut TqApi) -> crate::error::Result<()> {
        self.order.cancel_remaining(api).await
    }

    pub async fn wait_partially_filled(&self, api: &mut TqApi) -> crate::error::Result<Order> {
        self.order.wait_partially_filled(api).await
    }

    pub async fn wait_partially_filled_until(
        &self,
        api: &mut TqApi,
        deadline: tokio::time::Instant,
    ) -> crate::error::Result<Order> {
        self.order.wait_partially_filled_until(api, deadline).await
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

    pub async fn wait_reconnect_safe_terminal(
        &self,
        api: &mut TqApi,
    ) -> crate::error::Result<OrderTicketState> {
        self.wait_reconnect_safe_terminal_with_deadline(api, None)
            .await
    }

    pub async fn wait_reconnect_safe_terminal_until(
        &self,
        api: &mut TqApi,
        deadline: tokio::time::Instant,
    ) -> crate::error::Result<OrderTicketState> {
        self.wait_reconnect_safe_terminal_with_deadline(api, Some(deadline))
            .await
    }

    async fn wait_reconnect_safe_terminal_with_deadline(
        &self,
        api: &mut TqApi,
        deadline: Option<tokio::time::Instant>,
    ) -> crate::error::Result<OrderTicketState> {
        loop {
            let state = self.status(api)?;
            if state.is_terminal() {
                return Ok(state);
            }

            if !api.wait_update(deadline).await? {
                return Ok(OrderTicketState::Unknown {
                    command_id: state.command_id(),
                });
            }
        }
    }

    fn command_status(&self, api: &TqApi) -> crate::error::Result<Option<CommandStatus>> {
        let Some(command_id) = self.command_id else {
            return Ok(None);
        };
        let Some(status) = api
            .session()
            .command_status(command_id)
            .map_err(WaitFacadeError::Session)?
        else {
            return Ok(None);
        };

        status
            .parse()
            .map(Some)
            .map_err(|()| WaitFacadeError::InvalidState("unknown command status"))
    }
}

fn state_from_order(command_id: Option<CommandId>, order: Order) -> OrderTicketState {
    match order.lifecycle {
        OrderLifecycle::Filled => OrderTicketState::Filled { command_id, order },
        OrderLifecycle::Cancelled => OrderTicketState::Cancelled {
            command_id,
            order: Some(order),
        },
        OrderLifecycle::Rejected => OrderTicketState::Rejected {
            command_id,
            order: Some(order),
        },
        OrderLifecycle::Failed => OrderTicketState::Failed {
            command_id,
            order: Some(order),
        },
        OrderLifecycle::Unknown
        | OrderLifecycle::Submitting
        | OrderLifecycle::Sent
        | OrderLifecycle::Accepted
        | OrderLifecycle::PartiallyFilled
        | OrderLifecycle::Cancelling => OrderTicketState::Live { command_id, order },
    }
}

fn state_from_command(
    command_id: Option<CommandId>,
    command_status: Option<CommandStatus>,
) -> OrderTicketState {
    match (command_id, command_status) {
        (Some(command_id), Some(CommandStatus::Rejected)) => OrderTicketState::Rejected {
            command_id: Some(command_id),
            order: None,
        },
        (Some(command_id), Some(CommandStatus::Cancelled)) => OrderTicketState::Cancelled {
            command_id: Some(command_id),
            order: None,
        },
        (Some(command_id), Some(CommandStatus::Failed)) => OrderTicketState::Failed {
            command_id: Some(command_id),
            order: None,
        },
        (Some(command_id), Some(CommandStatus::Completed)) => OrderTicketState::Unknown {
            command_id: Some(command_id),
        },
        (Some(command_id), Some(status)) => OrderTicketState::CommandPending { command_id, status },
        (None, Some(_)) => OrderTicketState::Unknown { command_id: None },
        (command_id, None) => OrderTicketState::Unknown { command_id },
    }
}
