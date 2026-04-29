use serde_json::{Map, Value, json};

use crate::{
    commands::{OutboundDispatch, OutboundFrame, OutboundRequest},
    ids::{CommandId, ProtocolDomain},
    state::{CommitResult, StatePath, StateReadView},
};

pub(super) fn command_detail_map_from_snapshot(
    snapshot: StateReadView<'_>,
    command_id: CommandId,
) -> Option<Map<String, Value>> {
    let command_segment = command_id.get().to_string();
    snapshot
        .get(["runtime", "commands", command_segment.as_str(), "detail"])
        .and_then(Value::as_object)
        .cloned()
}

pub(super) fn command_detail_from_seed(
    mut seed: Map<String, Value>,
    _command_id: CommandId,
    route_label: Option<&str>,
    dispatch: Option<&OutboundDispatch>,
    extra: Map<String, Value>,
) -> Option<Value> {
    if let Some(route_label) = route_label {
        seed.insert("route".to_string(), json!(route_label));
    }

    if let Some(dispatch) = dispatch {
        for (key, value) in command_detail_fields_from_dispatch(dispatch) {
            seed.entry(key).or_insert(value);
        }
    }

    seed.extend(extra);

    if seed.is_empty() {
        None
    } else {
        Some(Value::Object(seed))
    }
}

pub(super) fn command_detail_fields_from_dispatch(
    dispatch: &OutboundDispatch,
) -> Map<String, Value> {
    let mut detail = Map::new();

    match &dispatch.request {
        OutboundRequest::Transport(OutboundFrame::Text(text)) => {
            if let Ok(Value::Object(request)) = serde_json::from_str::<Value>(text) {
                if let Some(aid) = request.get("aid").and_then(Value::as_str) {
                    detail.insert("aid".to_string(), json!(aid));
                }

                if dispatch.domain == ProtocolDomain::Trade {
                    if let Some(account_id) = request
                        .get("user_id")
                        .or_else(|| request.get("user_name"))
                        .and_then(Value::as_str)
                    {
                        detail.insert("account_id".to_string(), json!(account_id));
                    }
                    if let Some(order_id) = request.get("order_id").and_then(Value::as_str) {
                        detail.insert("order_id".to_string(), json!(order_id));
                    }
                    if let Some(trading_day) = request.get("trading_day").and_then(Value::as_str) {
                        detail.insert("trading_day".to_string(), json!(trading_day));
                    }
                    if let Some(exchange_id) = request.get("exchange_id").and_then(Value::as_str) {
                        detail.insert("exchange_id".to_string(), json!(exchange_id));
                    }
                }
            }
        }
        OutboundRequest::Transport(OutboundFrame::Binary(_)) => {
            detail.insert("frame".to_string(), json!("binary"));
        }
        OutboundRequest::Transport(OutboundFrame::Ping) => {
            detail.insert("frame".to_string(), json!("ping"));
        }
        OutboundRequest::Transport(OutboundFrame::Close) => {
            detail.insert("frame".to_string(), json!("close"));
        }
        OutboundRequest::Http(request) => {
            detail.insert("method".to_string(), json!(request.method.as_str()));
            if let Some(path) = &request.path {
                detail.insert("path".to_string(), json!(path));
            }
            if let Some(Value::Object(body)) = &request.body {
                if let Some(aid) = body.get("aid").and_then(Value::as_str) {
                    detail.insert("aid".to_string(), json!(aid));
                }
                if let Some(query_id) = body.get("query_id").and_then(Value::as_str) {
                    detail.insert("query_id".to_string(), json!(query_id));
                }
            }
        }
        OutboundRequest::Query(request) => {
            detail.insert("aid".to_string(), json!("ins_query"));
            detail.insert("query_id".to_string(), json!(request.query_id.as_str()));
        }
        OutboundRequest::Replay(request) => {
            detail.insert("action".to_string(), json!(request.action));
        }
        OutboundRequest::Internal(request) => {
            detail.insert("label".to_string(), json!(request.label));
        }
    }

    detail
}

pub(super) fn is_terminal_command_status(status: Option<&str>) -> bool {
    matches!(
        status,
        Some("completed" | "rejected" | "failed" | "cancelled")
    )
}

pub(super) fn commit_touches_path<I, S>(commit: &CommitResult, path: I) -> bool
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    commit.changes.path_hits.contains(&StatePath::new(path))
}

pub(super) fn commit_touches_path_prefix<I, S>(commit: &CommitResult, path: I) -> bool
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let prefix = path.into_iter().map(Into::into).collect::<Vec<_>>();
    commit.changes.path_hits.iter().any(|hit| {
        let segments = hit.segments();
        segments.len() >= prefix.len()
            && segments
                .iter()
                .zip(prefix.iter())
                .all(|(left, right)| left == right)
    })
}
