#![forbid(unsafe_code)]

use std::{
    sync::{Arc, Barrier},
    thread,
    time::{Duration, Instant},
};

use futures::executor::block_on;
use serde_json::{Map, Number, Value};
use tqsdk_core::{
    AdapterRegistry, CommitScope, InputPayload, IoEvent, MarketCommand, ProtocolDomain, Runtime,
    RuntimeCommand, RuntimeHandle, RuntimeInput, Symbol,
};

#[test]
#[ignore = "benchmark-style tail latency probe; run explicitly with --ignored --nocapture"]
fn command_submit_latency_under_large_market_ingest_is_reported() {
    let handle = runtime_handle();
    let symbols = bench_symbols(1_000);
    let market_inputs = (0..64)
        .map(|sequence| market_input(quote_rtn_data(&symbols, sequence)))
        .collect::<Vec<_>>();

    let no_load_p95 = command_submit_p95(&handle, 64);
    let start = Instant::now();
    let mut command_latencies = Vec::new();

    for input in market_inputs {
        let barrier = Arc::new(Barrier::new(2));
        let ingest_barrier = Arc::clone(&barrier);
        let ingest_handle = handle.clone();
        let ingest_thread = thread::spawn(move || {
            ingest_barrier.wait();
            let ingest_start = Instant::now();
            let commit = ingest_handle
                .ingest(input, Vec::new(), CommitScope::RealtimeUpdate)
                .expect("market ingest succeeds");
            (ingest_start.elapsed(), commit.is_some())
        });

        barrier.wait();
        let command_start = Instant::now();
        let command_id = block_on(handle.submit(RuntimeCommand::Market(
            MarketCommand::SubscribeQuotes {
                symbols: vec![Symbol::new("SHFE.tail_probe")],
            },
        )))
        .expect("command submission succeeds");
        assert!(command_id.get() > 0);
        let command_latency = command_start.elapsed();
        command_latencies.push(command_latency);

        let (ingest_elapsed, committed) = ingest_thread.join().expect("ingest probe thread joins");
        assert!(committed);

        eprintln!(
            "large_ingest_elapsed={ingest_elapsed:?} command_submit_during_ingest={command_latency:?}"
        );
    }

    let p95 = percentile_95(command_latencies);
    eprintln!(
        "tail probe total={:?} no_load_command_submit_p95={:?} under_large_ingest_p95={:?}",
        start.elapsed(),
        no_load_p95,
        p95
    );
}

fn command_submit_p95(handle: &RuntimeHandle, iterations: usize) -> Duration {
    let mut latencies = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let start = Instant::now();
        let command_id = block_on(handle.submit(RuntimeCommand::Market(
            MarketCommand::SubscribeQuotes {
                symbols: vec![Symbol::new("SHFE.tail_probe")],
            },
        )))
        .expect("no-load command submission succeeds");
        assert!(command_id.get() > 0);
        latencies.push(start.elapsed());
    }
    percentile_95(latencies)
}

fn percentile_95(mut latencies: Vec<Duration>) -> Duration {
    assert!(!latencies.is_empty());
    latencies.sort_unstable();
    latencies[latencies.len() * 95 / 100]
}

fn runtime_handle() -> RuntimeHandle {
    let mut adapters = AdapterRegistry::new();
    adapters.register_default_adapters();
    RuntimeHandle::with_adapters(adapters)
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
        .map(|index| format!("SHFE.tail{index:04}"))
        .collect()
}

fn quote_rtn_data(symbols: &[String], sequence: u64) -> Value {
    let mut quotes = Map::with_capacity(symbols.len());
    for (index, symbol) in symbols.iter().enumerate() {
        let mut fields = Map::new();
        fields.insert(
            "datetime".to_string(),
            Value::String(format!("202606101002{sequence:08}")),
        );
        fields.insert(
            "last_price".to_string(),
            number(600.0 + index as f64 * 0.01 + sequence as f64 * 0.001),
        );
        fields.insert(
            "bid_price1".to_string(),
            number(599.8 + index as f64 * 0.01),
        );
        fields.insert("bid_volume1".to_string(), Value::from(10 + index as i64));
        fields.insert(
            "ask_price1".to_string(),
            number(600.2 + index as f64 * 0.01),
        );
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
