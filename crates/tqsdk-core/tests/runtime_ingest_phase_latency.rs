#![forbid(unsafe_code)]

use std::{
    env,
    hint::black_box,
    time::{Duration, Instant},
};

use serde_json::{Map, Number, Value};
use tqsdk_core::{
    AdapterRegistry, CommitScope, InputPayload, IoEvent, ProtocolDomain, RuntimeHandle,
    RuntimeInput,
};

const DEFAULT_ITERS: usize = 64;
const DEFAULT_SYMBOLS: usize = 1_000;

#[test]
#[ignore = "benchmark-style decode probe; run explicitly with --ignored --nocapture"]
fn large_market_decode_latency_is_reported() {
    let iterations = env_usize("TQSDK_INGEST_PROBE_ITERS", DEFAULT_ITERS);
    let symbol_count = env_usize("TQSDK_INGEST_PROBE_SYMBOLS", DEFAULT_SYMBOLS);
    let symbols = bench_symbols(symbol_count);
    let inputs = market_inputs(iterations, &symbols);
    let mut adapters = AdapterRegistry::new();
    adapters.register_default_adapters();

    let mut latencies = Vec::with_capacity(inputs.len());
    let mut mutations_per_iter = 0_usize;
    for input in &inputs {
        let start = Instant::now();
        let mutations = adapters
            .decode_input(input)
            .expect("market input decode succeeds");
        latencies.push(start.elapsed());
        mutations_per_iter = mutations.len();
        black_box(mutations_per_iter);
    }

    eprintln!(
        "large_market_decode iterations={iterations} symbols={symbol_count} mutations_per_iter={mutations_per_iter} latency={:?}",
        latency_summary(latencies),
    );
}

#[test]
#[ignore = "benchmark-style ingest probe; run explicitly with --ignored --nocapture"]
fn large_market_ingest_latency_is_reported() {
    let iterations = env_usize("TQSDK_INGEST_PROBE_ITERS", DEFAULT_ITERS);
    let symbol_count = env_usize("TQSDK_INGEST_PROBE_SYMBOLS", DEFAULT_SYMBOLS);
    let symbols = bench_symbols(symbol_count);
    let inputs = market_inputs(iterations, &symbols);
    let handle = runtime_handle();

    let mut latencies = Vec::with_capacity(inputs.len());
    let mut commits = 0_usize;
    for input in inputs {
        let start = Instant::now();
        let commit = handle
            .ingest(input, Vec::new(), CommitScope::RealtimeUpdate)
            .expect("market ingest succeeds");
        latencies.push(start.elapsed());
        if commit.is_some() {
            commits += 1;
        }
        black_box(commits);
    }

    eprintln!(
        "large_market_ingest iterations={iterations} symbols={symbol_count} commits={commits} latency={:?}",
        latency_summary(latencies),
    );
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LatencySummary {
    p50: Duration,
    p95: Duration,
    p99: Duration,
    max: Duration,
}

fn latency_summary(mut latencies: Vec<Duration>) -> LatencySummary {
    assert!(!latencies.is_empty());
    latencies.sort_unstable();
    LatencySummary {
        p50: percentile(&latencies, 50),
        p95: percentile(&latencies, 95),
        p99: percentile(&latencies, 99),
        max: *latencies.last().expect("non-empty latency samples"),
    }
}

fn percentile(latencies: &[Duration], percentile: usize) -> Duration {
    let index = (((latencies.len() * percentile) + 99) / 100)
        .saturating_sub(1)
        .min(latencies.len() - 1);
    latencies[index]
}

fn runtime_handle() -> RuntimeHandle {
    let mut adapters = AdapterRegistry::new();
    adapters.register_default_adapters();
    RuntimeHandle::with_adapters(adapters)
}

fn market_inputs(iterations: usize, symbols: &[String]) -> Vec<RuntimeInput> {
    (0..iterations)
        .map(|sequence| market_input(quote_rtn_data(symbols, sequence as u64)))
        .collect()
}

fn market_input(payload: Value) -> RuntimeInput {
    RuntimeInput::Io(IoEvent {
        route: "market".to_string(),
        domains: vec![ProtocolDomain::Market],
        payload: InputPayload::Json(payload),
    })
}

fn bench_symbols(count: usize) -> Vec<String> {
    (0..count)
        .map(|index| format!("SHFE.phase{index:04}"))
        .collect()
}

fn quote_rtn_data(symbols: &[String], sequence: u64) -> Value {
    let mut quotes = Map::with_capacity(symbols.len());
    for (index, symbol) in symbols.iter().enumerate() {
        let price = 600.0 + index as f64 * 0.01 + sequence as f64 * 0.001;
        let mut fields = Map::new();
        fields.insert(
            "datetime".to_string(),
            Value::String(format!("202606221002{sequence:08}")),
        );
        fields.insert("last_price".to_string(), number(price));
        fields.insert("bid_price1".to_string(), number(price - 0.2));
        fields.insert("bid_volume1".to_string(), Value::from(10 + index as i64));
        fields.insert("ask_price1".to_string(), number(price + 0.2));
        fields.insert("ask_volume1".to_string(), Value::from(12 + index as i64));
        fields.insert(
            "volume".to_string(),
            Value::from(sequence as i64 + index as i64),
        );
        fields.insert(
            "open_interest".to_string(),
            Value::from(10_000_i64 + index as i64),
        );
        quotes.insert(symbol.clone(), Value::Object(fields));
    }

    let mut root = Map::new();
    root.insert("quotes".to_string(), Value::Object(quotes));

    let mut envelope = Map::new();
    envelope.insert("aid".to_string(), Value::String("rtn_data".to_string()));
    envelope.insert("data".to_string(), Value::Array(vec![Value::Object(root)]));
    Value::Object(envelope)
}

fn number(value: f64) -> Value {
    Value::Number(Number::from_f64(value).expect("finite benchmark price"))
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}
