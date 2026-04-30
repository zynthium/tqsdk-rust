use tqsdk_core::{Order, Trade, TradeDirection, TradeOffset};

#[derive(Debug, Clone, PartialEq)]
pub enum TargetPosTaskExecutionEvent {
    InsertOrder {
        request_seq: u64,
        order_id: String,
        direction: TradeDirection,
        offset: TradeOffset,
        volume: i64,
        limit_price: f64,
    },
    CancelOrder {
        order_id: String,
    },
    OrderFinished {
        order_id: String,
        status: String,
        filled_volume: i64,
        remaining_volume: i64,
        last_msg: String,
    },
    Trade {
        trade_id: String,
        order_id: String,
        direction: String,
        offset: String,
        volume: i64,
        price: f64,
        trade_date_time: i64,
    },
    TargetReached {
        request_seq: u64,
        target_volume: i64,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct TargetPosTaskTradeFill {
    pub trade_id: String,
    pub order_id: String,
    pub direction: String,
    pub offset: String,
    pub volume: i64,
    pub price: f64,
    pub trade_date_time: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TargetPosTaskReachedTarget {
    pub request_seq: u64,
    pub target_volume: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TargetPosTaskOrderReport {
    pub request_seq: u64,
    pub order_id: String,
    pub direction: TradeDirection,
    pub offset: TradeOffset,
    pub requested_volume: i64,
    pub limit_price: f64,
    pub cancel_requested: bool,
    pub status: Option<String>,
    pub filled_volume: i64,
    pub remaining_volume: i64,
    pub last_msg: Option<String>,
    pub trade_count: usize,
    pub filled_turnover: f64,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct TargetPosTaskExecutionReport {
    pub events: Vec<TargetPosTaskExecutionEvent>,
    pub trades: Vec<TargetPosTaskTradeFill>,
    pub orders: Vec<TargetPosTaskOrderReport>,
    pub submitted_order_count: usize,
    pub cancel_request_count: usize,
    pub finished_order_count: usize,
    pub filled_volume: i64,
    pub filled_turnover: f64,
    pub last_reached_target: Option<TargetPosTaskReachedTarget>,
}

impl From<&Trade> for TargetPosTaskTradeFill {
    fn from(trade: &Trade) -> Self {
        Self {
            trade_id: trade.trade_id.clone(),
            order_id: trade.order_id.clone(),
            direction: trade.direction.clone(),
            offset: trade.offset.clone(),
            volume: trade.volume,
            price: trade.price,
            trade_date_time: trade.trade_date_time,
        }
    }
}

impl TargetPosTaskExecutionReport {
    fn order_report_mut(&mut self, order_id: &str) -> Option<&mut TargetPosTaskOrderReport> {
        self.orders
            .iter_mut()
            .find(|report| report.order_id == order_id)
    }

    pub(super) fn record_insert_order(
        &mut self,
        request_seq: u64,
        order_id: &str,
        direction: TradeDirection,
        offset: TradeOffset,
        volume: i64,
        limit_price: f64,
    ) {
        self.submitted_order_count += 1;
        self.orders.push(TargetPosTaskOrderReport {
            request_seq,
            order_id: order_id.to_string(),
            direction,
            offset,
            requested_volume: volume,
            limit_price,
            cancel_requested: false,
            status: None,
            filled_volume: 0,
            remaining_volume: volume,
            last_msg: None,
            trade_count: 0,
            filled_turnover: 0.0,
        });
        self.events.push(TargetPosTaskExecutionEvent::InsertOrder {
            request_seq,
            order_id: order_id.to_string(),
            direction,
            offset,
            volume,
            limit_price,
        });
    }

    pub(super) fn record_cancel_order(&mut self, order_id: &str) {
        self.cancel_request_count += 1;
        if let Some(order_report) = self.order_report_mut(order_id) {
            order_report.cancel_requested = true;
        }
        self.events.push(TargetPosTaskExecutionEvent::CancelOrder {
            order_id: order_id.to_string(),
        });
    }

    pub(super) fn record_order_finished(&mut self, order: &Order) {
        self.finished_order_count += 1;
        if let Some(order_report) = self.order_report_mut(&order.order_id) {
            order_report.status = Some(order.status.clone());
            order_report.filled_volume = order.volume_origin - order.volume_left;
            order_report.remaining_volume = order.volume_left;
            order_report.last_msg = Some(order.last_msg.clone());
        }
        self.events
            .push(TargetPosTaskExecutionEvent::OrderFinished {
                order_id: order.order_id.clone(),
                status: order.status.clone(),
                filled_volume: order.volume_origin - order.volume_left,
                remaining_volume: order.volume_left,
                last_msg: order.last_msg.clone(),
            });
    }

    pub(super) fn record_target_reached(&mut self, request_seq: u64, target_volume: i64) {
        self.last_reached_target = Some(TargetPosTaskReachedTarget {
            request_seq,
            target_volume,
        });
        self.events
            .push(TargetPosTaskExecutionEvent::TargetReached {
                request_seq,
                target_volume,
            });
    }

    pub(super) fn record_trade(&mut self, trade: &Trade) {
        let fill = TargetPosTaskTradeFill::from(trade);
        self.filled_volume += fill.volume;
        self.filled_turnover += fill.price * fill.volume as f64;
        if let Some(order_report) = self.order_report_mut(&fill.order_id) {
            order_report.trade_count += 1;
            order_report.filled_volume += fill.volume;
            order_report.remaining_volume =
                (order_report.requested_volume - order_report.filled_volume).max(0);
            order_report.filled_turnover += fill.price * fill.volume as f64;
        }
        self.events.push(TargetPosTaskExecutionEvent::Trade {
            trade_id: fill.trade_id.clone(),
            order_id: fill.order_id.clone(),
            direction: fill.direction.clone(),
            offset: fill.offset.clone(),
            volume: fill.volume,
            price: fill.price,
            trade_date_time: fill.trade_date_time,
        });
        self.trades.push(fill);
    }
}
