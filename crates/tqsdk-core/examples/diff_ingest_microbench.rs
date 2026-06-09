#![forbid(unsafe_code)]

use std::{
    env,
    error::Error,
    hint::black_box,
    time::{Duration, Instant},
};

use serde_json::{Map, Number, Value};
use tqsdk_core::{
    AdapterRegistry, CommitScope, InputPayload, IoEvent, ProtocolDomain, RuntimeHandle,
    RuntimeInput, RuntimeReader, Symbol,
};

const DEFAULT_SINGLE_ITERS: u64 = 20_000;
const DEFAULT_BATCH_ITERS: u64 = 1_000;
const DEFAULT_NOOP_ITERS: u64 = 20_000;
const DEFAULT_READ_ITERS: u64 = 50_000;
const DEFAULT_BATCH_SYMBOLS: usize = 100;
const DEFAULT_LARGE_BATCH_SYMBOLS: usize = 1_000;

fn main() -> Result<(), Box<dyn Error>> {
    let single_iters = env_u64("TQSDK_DIFF_BENCH_SINGLE_ITERS", DEFAULT_SINGLE_ITERS);
    let batch_iters = env_u64("TQSDK_DIFF_BENCH_BATCH_ITERS", DEFAULT_BATCH_ITERS);
    let noop_iters = env_u64("TQSDK_DIFF_BENCH_NOOP_ITERS", DEFAULT_NOOP_ITERS);
    let read_iters = env_u64("TQSDK_DIFF_BENCH_READ_ITERS", DEFAULT_READ_ITERS);
    let batch_symbols = env_usize("TQSDK_DIFF_BENCH_BATCH_SYMBOLS", DEFAULT_BATCH_SYMBOLS);
    let large_batch_symbols = env_usize(
        "TQSDK_DIFF_BENCH_LARGE_BATCH_SYMBOLS",
        DEFAULT_LARGE_BATCH_SYMBOLS,
    );

    println!("tqsdk-core DIFF ingest microbench");
    println!("profile: run with --release for useful numbers");
    println!();
    println!(
        "{:<32} {:>10} {:>10} {:>10} {:>14} {:>14}",
        "case", "iters", "items/it", "commits", "ns/iter", "ns/item"
    );

    let single_symbols = bench_symbols(1);
    print_result(run_parse_case(
        "parse_json_single_quote",
        single_iters,
        &single_symbols,
    )?);
    print_result(run_ingest_case(
        "ingest_single_quote",
        single_iters,
        &single_symbols,
    )?);
    print_result(run_noop_case(
        "ingest_noop_single_quote",
        noop_iters,
        &single_symbols,
    )?);

    let batch_symbols = bench_symbols(batch_symbols);
    print_result(run_parse_case(
        "parse_json_quote_batch",
        batch_iters,
        &batch_symbols,
    )?);
    print_result(run_ingest_case(
        "ingest_quote_batch",
        batch_iters,
        &batch_symbols,
    )?);

    let large_batch_symbols = bench_symbols(large_batch_symbols);
    print_result(run_ingest_case(
        "ingest_large_quote_batch",
        batch_iters.saturating_div(4).max(1),
        &large_batch_symbols,
    )?);
    print_result(run_typed_read_case(
        "read_market_quote_typed",
        read_iters,
        &large_batch_symbols,
    )?);

    Ok(())
}

fn run_parse_case(
    name: &'static str,
    iterations: u64,
    symbols: &[String],
) -> Result<BenchResult, serde_json::Error> {
    let text = quote_rtn_data(symbols, 1).to_string();
    let start = Instant::now();
    for _ in 0..iterations {
        let value: Value = serde_json::from_str(&text)?;
        black_box(value);
    }
    Ok(BenchResult {
        name,
        iterations,
        items_per_iter: symbols.len(),
        commits: 0,
        elapsed: start.elapsed(),
    })
}

fn run_ingest_case(
    name: &'static str,
    iterations: u64,
    symbols: &[String],
) -> tqsdk_core::Result<BenchResult> {
    let handle = runtime_handle();
    let start = Instant::now();
    let mut commits = 0_u64;
    for sequence in 0..iterations {
        let input = market_input(quote_rtn_data(symbols, sequence));
        if let Some(commit) = handle.ingest(input, Vec::new(), CommitScope::RealtimeUpdate)? {
            commits += 1;
            black_box(commit.revision);
        }
    }
    Ok(BenchResult {
        name,
        iterations,
        items_per_iter: symbols.len(),
        commits,
        elapsed: start.elapsed(),
    })
}

fn run_noop_case(
    name: &'static str,
    iterations: u64,
    symbols: &[String],
) -> tqsdk_core::Result<BenchResult> {
    let handle = runtime_handle();
    let payload = quote_rtn_data(symbols, 1);
    let first_commit = handle.ingest(
        market_input(payload.clone()),
        Vec::new(),
        CommitScope::RealtimeUpdate,
    )?;
    black_box(first_commit);

    let start = Instant::now();
    let mut commits = 0_u64;
    for _ in 0..iterations {
        let commit = handle.ingest(
            market_input(payload.clone()),
            Vec::new(),
            CommitScope::RealtimeUpdate,
        )?;
        if commit.is_some() {
            commits += 1;
        }
        black_box(commit);
    }

    Ok(BenchResult {
        name,
        iterations,
        items_per_iter: symbols.len(),
        commits,
        elapsed: start.elapsed(),
    })
}

fn run_typed_read_case(
    name: &'static str,
    iterations: u64,
    symbols: &[String],
) -> tqsdk_core::Result<BenchResult> {
    let handle = runtime_handle();
    let reader = handle.reader();
    seed_quotes(&handle, symbols)?;

    let symbols = symbols
        .iter()
        .map(|symbol| Symbol::new(symbol.clone()))
        .collect::<Vec<_>>();
    let start = Instant::now();
    for index in 0..iterations {
        let quote = read_one_quote(&reader, &symbols[index as usize % symbols.len()])?;
        black_box(quote);
    }

    Ok(BenchResult {
        name,
        iterations,
        items_per_iter: 1,
        commits: 0,
        elapsed: start.elapsed(),
    })
}

fn seed_quotes(handle: &RuntimeHandle, symbols: &[String]) -> tqsdk_core::Result<()> {
    let commit = handle.ingest(
        market_input(quote_rtn_data(symbols, 1)),
        Vec::new(),
        CommitScope::RealtimeUpdate,
    )?;
    black_box(commit);
    Ok(())
}

fn read_one_quote(
    reader: &RuntimeReader,
    symbol: &Symbol,
) -> tqsdk_core::Result<Option<tqsdk_core::Quote>> {
    reader.read_market_state().quote(symbol)
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

fn quote_rtn_data(symbols: &[String], sequence: u64) -> Value {
    let mut quotes = Map::with_capacity(symbols.len());
    for (index, symbol) in symbols.iter().enumerate() {
        quotes.insert(symbol.clone(), quote_fields(sequence, index));
    }

    let mut root = Map::new();
    root.insert("quotes".to_string(), Value::Object(quotes));

    let mut envelope = Map::new();
    envelope.insert("aid".to_string(), Value::String("rtn_data".to_string()));
    envelope.insert("data".to_string(), Value::Array(vec![Value::Object(root)]));
    Value::Object(envelope)
}

fn quote_fields(sequence: u64, index: usize) -> Value {
    let price = 600.0 + index as f64 * 0.01 + sequence as f64 * 0.001;
    let mut fields = Map::new();
    fields.insert(
        "datetime".to_string(),
        Value::String(format!("202606101000{sequence:08}")),
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
    Value::Object(fields)
}

fn number(value: f64) -> Value {
    Value::Number(Number::from_f64(value).expect("finite benchmark price"))
}

fn bench_symbols(count: usize) -> Vec<String> {
    (0..count)
        .map(|index| format!("SHFE.bench{index:04}"))
        .collect()
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

struct BenchResult {
    name: &'static str,
    iterations: u64,
    items_per_iter: usize,
    commits: u64,
    elapsed: Duration,
}

fn print_result(result: BenchResult) {
    let ns_per_iter = result.elapsed.as_nanos() as f64 / result.iterations as f64;
    let ns_per_item = ns_per_iter / result.items_per_iter as f64;
    println!(
        "{:<32} {:>10} {:>10} {:>10} {:>14.1} {:>14.1}",
        result.name,
        result.iterations,
        result.items_per_iter,
        result.commits,
        ns_per_iter,
        ns_per_item
    );
}
