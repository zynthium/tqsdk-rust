use serde_json::json;
use tqsdk_wait::{OrderRef, TqApi};

use crate::{Result, TaskError};

use super::state::DesiredOrder;

pub(super) async fn insert_desired_order(
    api: &mut TqApi,
    account_id: &str,
    symbol: &str,
    desired_order: &DesiredOrder,
) -> Result<OrderRef> {
    api.insert_order(
        account_id,
        symbol,
        desired_order.direction,
        Some(desired_order.offset),
        desired_order.volume,
        Some(json!(desired_order.limit_price)),
    )
    .await
    .map_err(TaskError::from)
}

pub(super) async fn cancel_order(api: &mut TqApi, account_id: &str, order_id: &str) -> Result<()> {
    api.cancel_order(account_id, order_id)
        .await
        .map_err(TaskError::from)
}
