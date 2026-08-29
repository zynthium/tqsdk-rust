use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use serde_json::{Map, Number, Value, json};
use tqsdk_core::{
    ChartId, CommitScope, FieldMutation, InputPayload, IoEvent, Kline, MutationSource,
    NormalizedMutation, ObjectKey, ProtocolDomain, Quote, RuntimeInput, SeriesKey, StatePath,
    Symbol, Tick,
};

use crate::replay::{ReplayMarketEvent, ReplayMarketPayload};
use crate::{Result, TaskHost};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReplayKlineSpec {
    pub(crate) symbol: String,
    pub(crate) duration_ns: i64,
    pub(crate) view_width: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReplayTickSpec {
    pub(crate) symbol: String,
    pub(crate) view_width: usize,
}

#[derive(Debug, Default)]
pub(crate) struct BacktestTickWindowState {
    row_ids: VecDeque<i64>,
    retained_ids: HashSet<i64>,
}

#[derive(Debug, Default)]
pub(crate) struct BacktestTickSubscription {
    charts: Vec<BacktestTickChart>,
    max_view_width: usize,
    window: BacktestTickWindowState,
}

#[derive(Debug)]
struct BacktestTickChart {
    view_width: usize,
    chart_id: String,
}

impl BacktestTickWindowState {
    fn push(&mut self, id: i64, capacity: usize) -> Vec<i64> {
        let capacity = capacity.max(1);
        if self.retained_ids.insert(id) {
            self.row_ids.push_back(id);
        }
        let mut evicted = Vec::new();
        while self.row_ids.len() > capacity {
            if let Some(id) = self.row_ids.pop_front() {
                self.retained_ids.remove(&id);
                evicted.push(id);
            }
        }
        evicted
    }

    fn right_id(&self) -> i64 {
        *self
            .row_ids
            .back()
            .expect("a tick window contains the current tick")
    }

    fn left_id(&self, view_width: usize) -> i64 {
        let index = self.row_ids.len().saturating_sub(view_width.max(1));
        self.row_ids[index]
    }
}

pub(crate) fn backtest_tick_subscriptions(
    ticks: &[ReplayTickSpec],
) -> HashMap<String, BacktestTickSubscription> {
    let mut subscriptions = HashMap::<String, BacktestTickSubscription>::with_capacity(ticks.len());
    for spec in ticks {
        let subscription = subscriptions.entry(spec.symbol.clone()).or_default();
        subscription.max_view_width = subscription.max_view_width.max(spec.view_width.max(1));
        subscription.charts.push(BacktestTickChart {
            view_width: spec.view_width,
            chart_id: tick_chart_id(&spec.symbol, spec.view_width),
        });
    }
    subscriptions
}

pub(crate) fn ingest_replay_market_event(
    host: &TaskHost,
    event: &ReplayMarketEvent,
    klines: &[ReplayKlineSpec],
    ticks: &[ReplayTickSpec],
) -> Result<()> {
    ingest_replay_market_batch(
        host,
        vec![replay_market_update(event, klines, ticks)],
        Vec::new(),
    )
}

pub(crate) fn replay_market_update(
    event: &ReplayMarketEvent,
    klines: &[ReplayKlineSpec],
    ticks: &[ReplayTickSpec],
) -> Value {
    match event.payload() {
        ReplayMarketPayload::Quote(quote) => {
            quote_update(event.symbol(), quote, event.underlying_symbol())
        }
        ReplayMarketPayload::Kline { duration_ns, row } => kline_update(
            event.symbol(),
            *duration_ns,
            row,
            klines,
            event.underlying_symbol(),
        ),
        ReplayMarketPayload::Tick(tick) => {
            tick_update(event.symbol(), tick, ticks, event.underlying_symbol())
        }
    }
}

/// Build already-normalized local-backtest mutations for one replayed market event.
///
/// A tick serial is only part of the facade state when the caller declared a
/// corresponding subscription. Quote and simulator projection happen outside
/// this helper, so quote-only backtests do not retain every historical tick.
pub(crate) fn backtest_market_mutations(
    event: &ReplayMarketEvent,
    klines: &[ReplayKlineSpec],
    tick_subscriptions: &mut HashMap<String, BacktestTickSubscription>,
) -> Option<Vec<NormalizedMutation>> {
    match event.payload() {
        ReplayMarketPayload::Quote(quote) => Some(quote_mutations(
            event.symbol(),
            quote,
            event.underlying_symbol(),
        )),
        ReplayMarketPayload::Kline { duration_ns, row } => Some(kline_mutations(
            event.symbol(),
            *duration_ns,
            row,
            klines,
            event.underlying_symbol(),
        )),
        ReplayMarketPayload::Tick(tick) => backtest_tick_mutations(
            event.symbol(),
            tick,
            event.underlying_symbol(),
            tick_subscriptions,
        ),
    }
}

pub(crate) fn ingest_presorted_replay_market_mutations(
    host: &TaskHost,
    mut market_mutations: Vec<NormalizedMutation>,
    mut replay_mutations: Vec<NormalizedMutation>,
    quote_fields: Vec<(String, Vec<FieldMutation>)>,
) -> Result<()> {
    market_mutations.extend(quote_field_mutations(quote_fields));
    if market_mutations.is_empty() && replay_mutations.is_empty() {
        return Ok(());
    }

    let handle = host.api().session().handle();
    if replay_mutations.is_empty() {
        handle.ingest_presorted_market_mutations(
            market_mutations,
            vec![],
            CommitScope::ReplayStep,
        )?;
    } else {
        replay_mutations.extend(market_mutations);
        handle.ingest_presorted_replay_step_mutations(
            replay_mutations,
            vec![],
            CommitScope::ReplayStep,
        )?;
    }
    Ok(())
}

pub(crate) fn ingest_replay_market_batch(
    host: &TaskHost,
    mut updates: Vec<Value>,
    quote_fields: Vec<(String, Vec<FieldMutation>)>,
) -> Result<()> {
    if let Some(quote_update) = quote_fields_update(quote_fields) {
        updates.push(quote_update);
    }
    if updates.is_empty() {
        return Ok(());
    }

    host.api().session().handle().ingest(
        RuntimeInput::Io(IoEvent {
            route: "market-replay".to_string(),
            domains: vec![ProtocolDomain::Market],
            payload: InputPayload::Json(json!({
                "aid": "rtn_data",
                "data": updates
            })),
        }),
        vec![],
        CommitScope::ReplayStep,
    )?;
    Ok(())
}

fn quote_fields_update(quote_fields: Vec<(String, Vec<FieldMutation>)>) -> Option<Value> {
    let mut quotes = Map::new();
    for (symbol, fields) in quote_fields {
        let fields = fields
            .into_iter()
            .map(|field| (field.field, field.value))
            .collect::<Map<_, _>>();
        if !fields.is_empty() {
            quotes.insert(symbol, Value::Object(fields));
        }
    }
    if quotes.is_empty() {
        None
    } else {
        Some(json!({"quotes": Value::Object(quotes)}))
    }
}

pub(crate) fn seed_replay_serials(
    host: &TaskHost,
    klines: &[ReplayKlineSpec],
    ticks: &[ReplayTickSpec],
) -> Result<()> {
    for spec in klines {
        let chart_id = kline_chart_id(&spec.symbol, spec.duration_ns, spec.view_width);
        host.api().session().handle().ingest(
            RuntimeInput::Io(IoEvent {
                route: "market-replay-seed".to_string(),
                domains: vec![ProtocolDomain::Market],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "charts": {
                            chart_id: {
                                "state": {
                                    "ins_list": spec.symbol,
                                    "duration": spec.duration_ns
                                },
                                "left_id": -1,
                                "right_id": -1,
                                "more_data": false,
                                "ready": true
                            }
                        },
                        "klines": {
                            spec.symbol.clone(): {
                                spec.duration_ns.to_string(): {
                                    "data": {
                                        "-1": {
                                            "id": -1,
                                            "datetime": -1
                                        }
                                    }
                                }
                            }
                        }
                    }]
                })),
            }),
            vec![],
            CommitScope::ReplayStep,
        )?;
    }

    for spec in ticks {
        let chart_id = tick_chart_id(&spec.symbol, spec.view_width);
        host.api().session().handle().ingest(
            RuntimeInput::Io(IoEvent {
                route: "market-replay-seed".to_string(),
                domains: vec![ProtocolDomain::Market],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "charts": {
                            chart_id: {
                                "state": {
                                    "ins_list": spec.symbol,
                                    "duration": 0
                                },
                                "left_id": -1,
                                "right_id": -1,
                                "more_data": false,
                                "ready": true
                            }
                        },
                        "ticks": {
                            spec.symbol.clone(): {
                                "data": {
                                    "-1": {
                                        "id": -1,
                                        "datetime": -1
                                    }
                                }
                            }
                        }
                    }]
                })),
            }),
            vec![],
            CommitScope::ReplayStep,
        )?;
    }

    Ok(())
}

fn quote_update(symbol: &str, quote: &Quote, underlying_symbol: Option<&str>) -> Value {
    let mut quote_value = Map::new();
    insert_string_if_present(&mut quote_value, "datetime", &quote.datetime);
    insert_f64_if_finite(&mut quote_value, "last_price", quote.last_price);
    insert_f64_if_finite(&mut quote_value, "ask_price1", quote.ask_price1);
    insert_i64_if_nonzero(&mut quote_value, "ask_volume1", quote.ask_volume1);
    insert_f64_if_finite(&mut quote_value, "bid_price1", quote.bid_price1);
    insert_i64_if_nonzero(&mut quote_value, "bid_volume1", quote.bid_volume1);
    insert_string_if_present(
        &mut quote_value,
        "underlying_symbol",
        effective_underlying_symbol(quote, underlying_symbol),
    );

    json!({
        "quotes": {
            symbol: Value::Object(quote_value)
        }
    })
}

fn kline_update(
    symbol: &str,
    duration_ns: i64,
    row: &Kline,
    klines: &[ReplayKlineSpec],
    underlying_symbol: Option<&str>,
) -> Value {
    let row_id = row.id.to_string();
    let mut charts = Map::new();
    for spec in klines
        .iter()
        .filter(|spec| spec.symbol == symbol && spec.duration_ns == duration_ns)
    {
        let chart_id = kline_chart_id(symbol, duration_ns, spec.view_width);
        charts.insert(
            chart_id,
            json!({
                "state": {
                    "ins_list": symbol,
                    "duration": duration_ns
                },
                "left_id": row.id,
                "right_id": row.id,
                "more_data": false,
                "ready": true
            }),
        );
    }

    let mut body = json!({
        "charts": Value::Object(charts),
        "klines": {
            symbol: {
                duration_ns.to_string(): {
                    "data": {
                        row_id: kline_value(row)
                    }
                }
            }
        }
    });
    insert_quote_underlying_update(&mut body, symbol, underlying_symbol);
    body
}

fn tick_update(
    symbol: &str,
    tick: &Tick,
    ticks: &[ReplayTickSpec],
    underlying_symbol: Option<&str>,
) -> Value {
    let row_id = tick.id.to_string();
    let mut charts = Map::new();
    for spec in ticks.iter().filter(|spec| spec.symbol == symbol) {
        let chart_id = tick_chart_id(symbol, spec.view_width);
        charts.insert(
            chart_id,
            json!({
                "state": {
                    "ins_list": symbol,
                    "duration": 0
                },
                "left_id": tick.id,
                "right_id": tick.id,
                "more_data": false,
                "ready": true
            }),
        );
    }

    let mut body = json!({
        "charts": Value::Object(charts),
        "ticks": {
            symbol: {
                "data": {
                    row_id: tick_value(tick)
                }
            }
        }
    });
    insert_quote_underlying_update(&mut body, symbol, underlying_symbol);
    body
}

fn quote_mutations(
    symbol: &str,
    quote: &Quote,
    underlying_symbol: Option<&str>,
) -> Vec<NormalizedMutation> {
    let mut fields = Vec::with_capacity(6);
    push_string_if_present(&mut fields, "datetime", &quote.datetime);
    push_f64_if_finite(&mut fields, "last_price", quote.last_price);
    push_f64_if_finite(&mut fields, "ask_price1", quote.ask_price1);
    push_i64_if_nonzero(&mut fields, "ask_volume1", quote.ask_volume1);
    push_f64_if_finite(&mut fields, "bid_price1", quote.bid_price1);
    push_i64_if_nonzero(&mut fields, "bid_volume1", quote.bid_volume1);
    push_string_if_present(
        &mut fields,
        "underlying_symbol",
        effective_underlying_symbol(quote, underlying_symbol),
    );
    market_mutation(
        StatePath::new(["quotes", symbol]),
        Some(ObjectKey::Quote {
            symbol: Symbol::new(symbol),
        }),
        fields,
    )
    .into_iter()
    .collect()
}

fn kline_mutations(
    symbol: &str,
    duration_ns: i64,
    row: &Kline,
    klines: &[ReplayKlineSpec],
    underlying_symbol: Option<&str>,
) -> Vec<NormalizedMutation> {
    let mut mutations = Vec::with_capacity(klines.len().saturating_mul(2).saturating_add(2));
    for spec in klines
        .iter()
        .filter(|spec| spec.symbol == symbol && spec.duration_ns == duration_ns)
    {
        push_chart_bounds_mutation(
            &mut mutations,
            &kline_chart_id(symbol, duration_ns, spec.view_width),
            row.id,
            row.id,
        );
    }

    if let Some(mutation) = market_mutation(
        StatePath::new([
            "klines",
            symbol,
            duration_ns.to_string().as_str(),
            "data",
            row.id.to_string().as_str(),
        ]),
        Some(ObjectKey::Kline {
            series: SeriesKey {
                primary: Symbol::new(symbol),
                secondary: Vec::new(),
                duration_ns,
                view_width: 0,
                right_id: None,
            },
            bar_id: row.id,
        }),
        kline_fields(row),
    ) {
        mutations.push(mutation);
    }
    push_underlying_symbol_mutation(&mut mutations, symbol, underlying_symbol);
    mutations
}

fn backtest_tick_mutations(
    symbol: &str,
    tick: &Tick,
    underlying_symbol: Option<&str>,
    tick_subscriptions: &mut HashMap<String, BacktestTickSubscription>,
) -> Option<Vec<NormalizedMutation>> {
    let subscription = tick_subscriptions.get_mut(symbol)?;
    let evicted = subscription
        .window
        .push(tick.id, subscription.max_view_width);
    let right_id = subscription.window.right_id();

    let mut mutations = Vec::with_capacity(
        subscription
            .charts
            .len()
            .saturating_mul(2)
            .saturating_add(3),
    );
    for chart in &subscription.charts {
        push_chart_bounds_mutation(
            &mut mutations,
            &chart.chart_id,
            subscription.window.left_id(chart.view_width),
            right_id,
        );
    }
    push_underlying_symbol_mutation(&mut mutations, symbol, underlying_symbol);

    if !evicted.is_empty() {
        let fields = evicted
            .into_iter()
            .map(|id| FieldMutation {
                field: id.to_string(),
                value: Value::Null,
            })
            .collect();
        if let Some(mutation) =
            market_mutation(StatePath::new(["ticks", symbol, "data"]), None, fields)
        {
            mutations.push(mutation);
        }
    }
    if let Some(mutation) = market_mutation(
        StatePath::new(["ticks", symbol, "data", tick.id.to_string().as_str()]),
        Some(ObjectKey::Tick {
            symbol: Symbol::new(symbol),
            tick_id: tick.id,
        }),
        tick_fields(tick),
    ) {
        mutations.push(mutation);
    }
    Some(mutations)
}

fn quote_field_mutations(
    quote_fields: Vec<(String, Vec<FieldMutation>)>,
) -> Vec<NormalizedMutation> {
    if quote_fields.is_empty() {
        return Vec::new();
    }
    if quote_fields.len() == 1 {
        let (symbol, fields) = quote_fields
            .into_iter()
            .next()
            .expect("one quote field update must contain one entry");
        return market_mutation(
            StatePath::new(["quotes", symbol.as_str()]),
            Some(ObjectKey::Quote {
                symbol: Symbol::new(symbol),
            }),
            fields,
        )
        .into_iter()
        .collect();
    }

    let mut latest_by_symbol = BTreeMap::new();
    for (symbol, fields) in quote_fields {
        if !fields.is_empty() {
            latest_by_symbol.insert(symbol, fields);
        }
    }

    latest_by_symbol
        .into_iter()
        .filter_map(|(symbol, fields)| {
            market_mutation(
                StatePath::new(["quotes", symbol.as_str()]),
                Some(ObjectKey::Quote {
                    symbol: Symbol::new(symbol),
                }),
                fields,
            )
        })
        .collect()
}

fn push_chart_bounds_mutation(
    mutations: &mut Vec<NormalizedMutation>,
    chart_id: &str,
    left_id: i64,
    right_id: i64,
) {
    if let Some(mutation) = market_mutation(
        StatePath::new(["charts", chart_id]),
        Some(ObjectKey::Chart {
            chart_id: ChartId::new(chart_id),
        }),
        vec![
            FieldMutation {
                field: "left_id".to_string(),
                value: Value::from(left_id),
            },
            FieldMutation {
                field: "right_id".to_string(),
                value: Value::from(right_id),
            },
        ],
    ) {
        mutations.push(mutation);
    }
}

fn push_underlying_symbol_mutation(
    mutations: &mut Vec<NormalizedMutation>,
    symbol: &str,
    underlying_symbol: Option<&str>,
) {
    let Some(underlying_symbol) = underlying_symbol.filter(|value| !value.is_empty()) else {
        return;
    };
    if let Some(mutation) = market_mutation(
        StatePath::new(["quotes", symbol]),
        Some(ObjectKey::Quote {
            symbol: Symbol::new(symbol),
        }),
        vec![FieldMutation {
            field: "underlying_symbol".to_string(),
            value: Value::from(underlying_symbol.to_string()),
        }],
    ) {
        mutations.push(mutation);
    }
}

fn kline_fields(row: &Kline) -> Vec<FieldMutation> {
    let mut fields = Vec::with_capacity(9);
    fields.push(FieldMutation {
        field: "id".to_string(),
        value: Value::from(row.id),
    });
    fields.push(FieldMutation {
        field: "datetime".to_string(),
        value: Value::from(row.datetime),
    });
    push_f64_if_finite(&mut fields, "open", row.open);
    push_f64_if_finite(&mut fields, "high", row.high);
    push_f64_if_finite(&mut fields, "low", row.low);
    push_f64_if_finite(&mut fields, "close", row.close);
    push_i64_if_nonzero(&mut fields, "volume", row.volume);
    push_i64_if_nonzero(&mut fields, "open_oi", row.open_oi);
    push_i64_if_nonzero(&mut fields, "close_oi", row.close_oi);
    fields
}

fn tick_fields(row: &Tick) -> Vec<FieldMutation> {
    let mut fields = Vec::with_capacity(14);
    fields.push(FieldMutation {
        field: "id".to_string(),
        value: Value::from(row.id),
    });
    fields.push(FieldMutation {
        field: "datetime".to_string(),
        value: Value::from(row.datetime),
    });
    push_f64_if_finite(&mut fields, "last_price", row.last_price);
    push_f64_if_finite(&mut fields, "average", row.average);
    push_f64_if_finite(&mut fields, "highest", row.highest);
    push_f64_if_finite(&mut fields, "lowest", row.lowest);
    push_f64_if_finite(&mut fields, "ask_price1", row.ask_price1);
    push_i64_if_nonzero(&mut fields, "ask_volume1", row.ask_volume1);
    push_f64_if_finite(&mut fields, "bid_price1", row.bid_price1);
    push_i64_if_nonzero(&mut fields, "bid_volume1", row.bid_volume1);
    push_i64_if_nonzero(&mut fields, "volume", row.volume);
    push_f64_if_finite(&mut fields, "amount", row.amount);
    push_i64_if_nonzero(&mut fields, "open_interest", row.open_interest);
    fields
}

fn market_mutation(
    path: StatePath,
    object: Option<ObjectKey>,
    mut fields: Vec<FieldMutation>,
) -> Option<NormalizedMutation> {
    if fields.is_empty() {
        return None;
    }
    fields.sort_unstable_by(|left, right| left.field.cmp(&right.field));
    Some(NormalizedMutation {
        path,
        object,
        fields,
        source: MutationSource::MarketDiff,
    })
}

fn push_string_if_present(fields: &mut Vec<FieldMutation>, key: &str, value: &str) {
    if !value.is_empty() {
        fields.push(FieldMutation {
            field: key.to_string(),
            value: Value::from(value.to_string()),
        });
    }
}

fn push_f64_if_finite(fields: &mut Vec<FieldMutation>, key: &str, value: f64) {
    if let Some(value) = Number::from_f64(value) {
        fields.push(FieldMutation {
            field: key.to_string(),
            value: Value::Number(value),
        });
    }
}

fn push_i64_if_nonzero(fields: &mut Vec<FieldMutation>, key: &str, value: i64) {
    if value != 0 {
        fields.push(FieldMutation {
            field: key.to_string(),
            value: Value::from(value),
        });
    }
}

fn kline_value(row: &Kline) -> Value {
    let mut value = Map::new();
    value.insert("id".to_string(), Value::from(row.id));
    value.insert("datetime".to_string(), Value::from(row.datetime));
    insert_f64_if_finite(&mut value, "open", row.open);
    insert_f64_if_finite(&mut value, "high", row.high);
    insert_f64_if_finite(&mut value, "low", row.low);
    insert_f64_if_finite(&mut value, "close", row.close);
    insert_i64_if_nonzero(&mut value, "volume", row.volume);
    insert_i64_if_nonzero(&mut value, "open_oi", row.open_oi);
    insert_i64_if_nonzero(&mut value, "close_oi", row.close_oi);
    Value::Object(value)
}

fn tick_value(row: &Tick) -> Value {
    let mut value = Map::new();
    value.insert("id".to_string(), Value::from(row.id));
    value.insert("datetime".to_string(), Value::from(row.datetime));
    insert_f64_if_finite(&mut value, "last_price", row.last_price);
    insert_f64_if_finite(&mut value, "average", row.average);
    insert_f64_if_finite(&mut value, "highest", row.highest);
    insert_f64_if_finite(&mut value, "lowest", row.lowest);
    insert_f64_if_finite(&mut value, "ask_price1", row.ask_price1);
    insert_i64_if_nonzero(&mut value, "ask_volume1", row.ask_volume1);
    insert_f64_if_finite(&mut value, "bid_price1", row.bid_price1);
    insert_i64_if_nonzero(&mut value, "bid_volume1", row.bid_volume1);
    insert_i64_if_nonzero(&mut value, "volume", row.volume);
    insert_f64_if_finite(&mut value, "amount", row.amount);
    insert_i64_if_nonzero(&mut value, "open_interest", row.open_interest);
    Value::Object(value)
}

fn effective_underlying_symbol<'a>(
    quote: &'a Quote,
    event_underlying_symbol: Option<&'a str>,
) -> &'a str {
    if !quote.underlying_symbol.is_empty() {
        &quote.underlying_symbol
    } else {
        event_underlying_symbol.unwrap_or("")
    }
}

fn insert_quote_underlying_update(body: &mut Value, symbol: &str, underlying_symbol: Option<&str>) {
    let Some(underlying_symbol) = underlying_symbol else {
        return;
    };
    if underlying_symbol.is_empty() {
        return;
    }
    let Some(object) = body.as_object_mut() else {
        return;
    };
    object.insert(
        "quotes".to_string(),
        json!({
            symbol: {
                "underlying_symbol": underlying_symbol
            }
        }),
    );
}

fn insert_string_if_present(value: &mut Map<String, Value>, key: &str, field: &str) {
    if !field.is_empty() {
        value.insert(key.to_string(), Value::from(field));
    }
}

fn insert_f64_if_finite(value: &mut Map<String, Value>, key: &str, field: f64) {
    if let Some(number) = Number::from_f64(field) {
        value.insert(key.to_string(), Value::Number(number));
    }
}

fn insert_i64_if_nonzero(value: &mut Map<String, Value>, key: &str, field: i64) {
    if field != 0 {
        value.insert(key.to_string(), Value::from(field));
    }
}

fn kline_chart_id(symbol: &str, duration_ns: i64, view_width: usize) -> String {
    let symbol = sanitize_chart_token(symbol);
    format!("wait-kline-{symbol}-{duration_ns}-{view_width}")
}

fn tick_chart_id(symbol: &str, view_width: usize) -> String {
    let symbol = sanitize_chart_token(symbol);
    format!("wait-tick-{symbol}-{view_width}")
}

fn sanitize_chart_token(raw: &str) -> String {
    raw.chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tqsdk_core::{FieldMutation, ObjectKey};

    use super::{
        BacktestTickWindowState, ReplayTickSpec, backtest_tick_subscriptions, quote_field_mutations,
    };

    #[test]
    fn backtest_tick_subscriptions_precompute_chart_ids_and_capacity() {
        let subscriptions = backtest_tick_subscriptions(&[
            ReplayTickSpec {
                symbol: "SHFE.au2606".to_string(),
                view_width: 10,
            },
            ReplayTickSpec {
                symbol: "SHFE.au2606".to_string(),
                view_width: 100,
            },
            ReplayTickSpec {
                symbol: "SHFE.ag2606".to_string(),
                view_width: 5,
            },
        ]);

        let subscription = subscriptions
            .get("SHFE.au2606")
            .expect("gold subscription should exist");
        assert_eq!(subscription.max_view_width, 100);
        assert_eq!(
            subscription
                .charts
                .iter()
                .map(|chart| (chart.view_width, chart.chart_id.as_str()))
                .collect::<Vec<_>>(),
            vec![
                (10, "wait-tick-SHFE_au2606-10"),
                (100, "wait-tick-SHFE_au2606-100"),
            ]
        );
        assert_eq!(subscriptions.len(), 2);
    }

    #[test]
    fn backtest_tick_window_preserves_duplicate_row_identity_without_linear_scan() {
        let mut window = BacktestTickWindowState::default();

        assert!(window.push(10, 2).is_empty());
        assert!(window.push(11, 2).is_empty());
        assert!(window.push(11, 2).is_empty());
        assert_eq!(
            window.row_ids.iter().copied().collect::<Vec<_>>(),
            vec![10, 11]
        );

        assert_eq!(window.push(12, 2), vec![10]);
        assert_eq!(
            window.row_ids.iter().copied().collect::<Vec<_>>(),
            vec![11, 12]
        );
    }

    #[test]
    fn backtest_tick_window_clamps_zero_capacity_to_one_row() {
        let mut window = BacktestTickWindowState::default();

        assert!(window.push(10, 0).is_empty());
        assert_eq!(window.left_id(0), 10);
        assert_eq!(window.right_id(), 10);
        assert_eq!(window.push(11, 0), vec![10]);
        assert_eq!(window.left_id(0), 11);
    }

    #[test]
    fn quote_field_mutations_fast_path_emits_the_single_symbol_update() {
        let mutations = quote_field_mutations(vec![(
            "SHFE.au2606".to_string(),
            vec![FieldMutation {
                field: "last_price".to_string(),
                value: json!(610.0),
            }],
        )]);

        assert_eq!(mutations.len(), 1);
        assert!(matches!(
            &mutations[0].object,
            Some(ObjectKey::Quote { symbol }) if symbol.as_str() == "SHFE.au2606"
        ));
        assert_eq!(mutations[0].fields[0].field, "last_price");
        assert_eq!(mutations[0].fields[0].value, json!(610.0));
    }

    #[test]
    fn quote_field_mutations_keep_last_update_for_duplicate_symbol() {
        let mutations = quote_field_mutations(vec![
            (
                "SHFE.au2606".to_string(),
                vec![FieldMutation {
                    field: "last_price".to_string(),
                    value: json!(610.0),
                }],
            ),
            (
                "SHFE.au2606".to_string(),
                vec![FieldMutation {
                    field: "bid_price1".to_string(),
                    value: json!(609.8),
                }],
            ),
        ]);

        assert_eq!(mutations.len(), 1);
        assert_eq!(mutations[0].fields.len(), 1);
        assert_eq!(mutations[0].fields[0].field, "bid_price1");
        assert_eq!(mutations[0].fields[0].value, json!(609.8));
    }
}
