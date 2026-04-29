use serde_json::{Map, Value, json};

use crate::{
    CommandId, CommandStatus,
    state::{CommitResult, StateReadView},
};

use super::{command_detail_from_seed, commit_touches_path, commit_touches_path_prefix};

pub(super) fn query_completed_status(
    snapshot: StateReadView<'_>,
    route_label: &str,
    commit: &CommitResult,
    command_id: CommandId,
    detail: &Map<String, Value>,
) -> Option<(CommandStatus, Option<Value>)> {
    let query_id = detail.get("query_id").and_then(Value::as_str)?;
    if !commit_touches_path_prefix(commit, ["query", query_id]) {
        return None;
    }
    snapshot.get(["query", query_id])?;

    let mut extra_detail = Map::new();
    if let Some(has_more) = snapshot.get(["query", query_id, "has_more"]).cloned() {
        extra_detail.insert("has_more".to_string(), has_more);
    }

    completed_from_seed(route_label, command_id, detail, extra_detail)
}

pub(super) fn trade_login_completed_status(
    snapshot: StateReadView<'_>,
    route_label: &str,
    commit: &CommitResult,
    command_id: CommandId,
    detail: &Map<String, Value>,
) -> Option<(CommandStatus, Option<Value>)> {
    let account_id = detail.get("account_id").and_then(Value::as_str)?;
    if !commit_touches_path(commit, ["trade", account_id, "trade_more_data"]) {
        return None;
    }
    let trade_more_data = snapshot
        .get(["trade", account_id, "trade_more_data", "value"])?
        .as_bool()?;
    if trade_more_data {
        return None;
    }

    let mut extra_detail = Map::new();
    extra_detail.insert("trade_more_data".to_string(), json!(false));
    completed_from_seed(route_label, command_id, detail, extra_detail)
}

pub(super) fn path_completed_status<const N: usize>(
    snapshot: StateReadView<'_>,
    route_label: &str,
    commit: &CommitResult,
    command_id: CommandId,
    detail: &Map<String, Value>,
    path: [&str; N],
    extra_detail: Map<String, Value>,
) -> Option<(CommandStatus, Option<Value>)> {
    if !commit_touches_path(commit, path) {
        return None;
    }
    snapshot.get(path)?;

    completed_from_seed(route_label, command_id, detail, extra_detail)
}

pub(super) fn pre_insert_order_completed_status(
    snapshot: StateReadView<'_>,
    route_label: &str,
    commit: &CommitResult,
    command_id: CommandId,
    detail: &Map<String, Value>,
) -> Option<(CommandStatus, Option<Value>)> {
    let account_id = detail.get("account_id").and_then(Value::as_str)?;
    let order_id = detail.get("order_id").and_then(Value::as_str)?;
    if !commit_touches_path(commit, ["trade", account_id, "pre_insert_orders", order_id]) {
        return None;
    }
    snapshot.get(["trade", account_id, "pre_insert_orders", order_id])?;

    let mut extra_detail = Map::new();
    if let Some(pre_margin) = snapshot
        .get([
            "trade",
            account_id,
            "pre_insert_orders",
            order_id,
            "pre_margin",
        ])
        .cloned()
    {
        extra_detail.insert("pre_margin".to_string(), pre_margin);
    }

    completed_from_seed(route_label, command_id, detail, extra_detail)
}

pub(super) fn trade_order_status(
    snapshot: StateReadView<'_>,
    route_label: &str,
    commit: &CommitResult,
    command_id: CommandId,
    detail: &Map<String, Value>,
) -> Option<(CommandStatus, Option<Value>)> {
    let account_id = detail.get("account_id").and_then(Value::as_str)?;
    let order_id = detail.get("order_id").and_then(Value::as_str)?;
    if !commit_touches_path(commit, ["trade", account_id, "orders", order_id]) {
        return None;
    }
    let order_status = snapshot
        .get(["trade", account_id, "orders", order_id, "status"])?
        .as_str()?;
    let exchange_order_id = snapshot
        .get(["trade", account_id, "orders", order_id, "exchange_order_id"])
        .and_then(Value::as_str)
        .unwrap_or("");
    let last_msg = snapshot
        .get(["trade", account_id, "orders", order_id, "last_msg"])
        .cloned();
    let volume_left = snapshot
        .get(["trade", account_id, "orders", order_id, "volume_left"])
        .cloned();

    let status = match order_status {
        "ALIVE" => CommandStatus::Acked,
        "FINISHED" if exchange_order_id.is_empty() => CommandStatus::Rejected,
        "FINISHED" => CommandStatus::Completed,
        _ => return None,
    };

    let mut extra_detail = Map::new();
    extra_detail.insert("order_status".to_string(), json!(order_status));
    if !exchange_order_id.is_empty() {
        extra_detail.insert("exchange_order_id".to_string(), json!(exchange_order_id));
    }
    if let Some(last_msg) = last_msg {
        extra_detail.insert("last_msg".to_string(), last_msg);
    }
    if let Some(volume_left) = volume_left {
        extra_detail.insert("volume_left".to_string(), volume_left);
    }

    Some((
        status,
        command_detail_from_seed(
            detail.clone(),
            command_id,
            Some(route_label),
            None,
            extra_detail,
        ),
    ))
}

fn completed_from_seed(
    route_label: &str,
    command_id: CommandId,
    detail: &Map<String, Value>,
    extra_detail: Map<String, Value>,
) -> Option<(CommandStatus, Option<Value>)> {
    Some((
        CommandStatus::Completed,
        command_detail_from_seed(
            detail.clone(),
            command_id,
            Some(route_label),
            None,
            extra_detail,
        ),
    ))
}
