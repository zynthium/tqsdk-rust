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
const DEFAULT_TICK_ITERS: u64 = 20_000;
const DEFAULT_BATCH_SYMBOLS: usize = 100;
const DEFAULT_LARGE_BATCH_SYMBOLS: usize = 1_000;
const DEFAULT_TICK_WINDOW: usize = 100;

fn main() -> Result<(), Box<dyn Error>> {
    let single_iters = env_u64("TQSDK_DIFF_BENCH_SINGLE_ITERS", DEFAULT_SINGLE_ITERS);
    let batch_iters = env_u64("TQSDK_DIFF_BENCH_BATCH_ITERS", DEFAULT_BATCH_ITERS);
    let noop_iters = env_u64("TQSDK_DIFF_BENCH_NOOP_ITERS", DEFAULT_NOOP_ITERS);
    let read_iters = env_u64("TQSDK_DIFF_BENCH_READ_ITERS", DEFAULT_READ_ITERS);
    let tick_iters = env_u64("TQSDK_DIFF_BENCH_TICK_ITERS", DEFAULT_TICK_ITERS);
    let batch_symbols = env_usize("TQSDK_DIFF_BENCH_BATCH_SYMBOLS", DEFAULT_BATCH_SYMBOLS);
    let large_batch_symbols = env_usize(
        "TQSDK_DIFF_BENCH_LARGE_BATCH_SYMBOLS",
        DEFAULT_LARGE_BATCH_SYMBOLS,
    );
    let tick_window = env_usize("TQSDK_DIFF_BENCH_TICK_WINDOW", DEFAULT_TICK_WINDOW);

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
    print_result(run_rolling_tick_ingest_case(
        "ingest_rolling_tick_window",
        tick_iters,
        &single_symbols[0],
        tick_window,
    )?);

    let batch_symbols = bench_symbols(batch_symbols);
    print_result(run_parse_case(
        "parse_json_quote_batch",
        batch_iters,
        &batch_symbols,
    )?);
    print_result(run_decode_case(
        "decode_prebuilt_quote_batch",
        quote_inputs(batch_iters, &batch_symbols),
        batch_symbols.len(),
    )?);
    print_result(run_text_decode_case(
        "decode_text_quote_batch",
        quote_texts(batch_iters, &batch_symbols),
        batch_symbols.len(),
    )?);
    print_result(run_ingest_case(
        "ingest_quote_batch",
        batch_iters,
        &batch_symbols,
    )?);
    print_result(run_ingest_case(
        "ingest_prebuilt_quote_batch",
        batch_iters,
        &batch_symbols,
    )?);
    print_result(run_text_ingest_case(
        "ingest_text_quote_batch",
        quote_texts(batch_iters, &batch_symbols),
        batch_symbols.len(),
    )?);

    let large_batch_symbols = bench_symbols(large_batch_symbols);
    print_result(run_ingest_case(
        "ingest_large_quote_batch",
        batch_iters.saturating_div(4).max(1),
        &large_batch_symbols,
    )?);
    print_result(run_ingest_case(
        "ingest_prebuilt_large_quote_batch",
        batch_iters.saturating_div(4).max(1),
        &large_batch_symbols,
    )?);
    print_result(run_sparse_ingest_case(
        "ingest_sparse_quote_batch_1000x10x3",
        batch_iters,
        &large_batch_symbols,
        10,
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
    let inputs = quote_inputs(iterations, symbols);
    run_ingest_inputs_case(name, inputs, symbols.len())
}

fn run_sparse_ingest_case(
    name: &'static str,
    iterations: u64,
    symbols: &[String],
    changed_per_iter: usize,
) -> tqsdk_core::Result<BenchResult> {
    let handle = runtime_handle();
    seed_quotes(&handle, symbols)?;
    let items_per_iter = changed_per_iter.min(symbols.len());
    let inputs = sparse_quote_inputs(iterations, symbols, changed_per_iter);
    run_ingest_inputs_with_handle(name, handle, inputs, items_per_iter)
}

fn run_rolling_tick_ingest_case(
    name: &'static str,
    iterations: u64,
    symbol: &str,
    window: usize,
) -> tqsdk_core::Result<BenchResult> {
    let inputs = rolling_tick_inputs(iterations, symbol, window);
    run_ingest_inputs_case(name, inputs, 1)
}

fn run_text_ingest_case(
    name: &'static str,
    texts: Vec<String>,
    items_per_iter: usize,
) -> Result<BenchResult, Box<dyn Error>> {
    let handle = runtime_handle();
    let iterations = texts.len() as u64;
    let start = Instant::now();
    let mut commits = 0_u64;
    for text in &texts {
        let input = market_input(serde_json::from_str(text)?);
        if let Some(commit) = handle.ingest(input, Vec::new(), CommitScope::RealtimeUpdate)? {
            commits += 1;
            black_box(commit.revision);
        }
    }
    Ok(BenchResult {
        name,
        iterations,
        items_per_iter,
        commits,
        elapsed: start.elapsed(),
    })
}

fn run_ingest_inputs_case(
    name: &'static str,
    inputs: Vec<RuntimeInput>,
    items_per_iter: usize,
) -> tqsdk_core::Result<BenchResult> {
    run_ingest_inputs_with_handle(name, runtime_handle(), inputs, items_per_iter)
}

fn run_ingest_inputs_with_handle(
    name: &'static str,
    handle: RuntimeHandle,
    inputs: Vec<RuntimeInput>,
    items_per_iter: usize,
) -> tqsdk_core::Result<BenchResult> {
    let iterations = inputs.len() as u64;
    let start = Instant::now();
    let mut commits = 0_u64;
    for input in inputs {
        if let Some(commit) = handle.ingest(input, Vec::new(), CommitScope::RealtimeUpdate)? {
            commits += 1;
            black_box(commit.revision);
        }
    }
    Ok(BenchResult {
        name,
        iterations,
        items_per_iter,
        commits,
        elapsed: start.elapsed(),
    })
}

fn run_decode_case(
    name: &'static str,
    inputs: Vec<RuntimeInput>,
    items_per_iter: usize,
) -> tqsdk_core::Result<BenchResult> {
    let iterations = inputs.len() as u64;
    let mut adapters = AdapterRegistry::new();
    adapters.register_default_adapters();
    let start = Instant::now();
    for input in &inputs {
        let mutations = adapters.decode_input(input)?;
        black_box(mutations.len());
    }
    Ok(BenchResult {
        name,
        iterations,
        items_per_iter,
        commits: 0,
        elapsed: start.elapsed(),
    })
}

fn run_text_decode_case(
    name: &'static str,
    texts: Vec<String>,
    items_per_iter: usize,
) -> Result<BenchResult, Box<dyn Error>> {
    let iterations = texts.len() as u64;
    let mut adapters = AdapterRegistry::new();
    adapters.register_default_adapters();
    let start = Instant::now();
    for text in &texts {
        let input = market_input(serde_json::from_str(text)?);
        let mutations = adapters.decode_input(&input)?;
        black_box(mutations.len());
    }
    Ok(BenchResult {
        name,
        iterations,
        items_per_iter,
        commits: 0,
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

    let inputs = noop_inputs(iterations, payload);
    let start = Instant::now();
    let mut commits = 0_u64;
    for input in inputs {
        let commit = handle.ingest(input, Vec::new(), CommitScope::RealtimeUpdate)?;
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

fn quote_inputs(iterations: u64, symbols: &[String]) -> Vec<RuntimeInput> {
    (0..iterations)
        .map(|sequence| market_input(quote_rtn_data(symbols, sequence)))
        .collect()
}

fn quote_texts(iterations: u64, symbols: &[String]) -> Vec<String> {
    (0..iterations)
        .map(|sequence| quote_rtn_data(symbols, sequence).to_string())
        .collect()
}

fn noop_inputs(iterations: u64, payload: Value) -> Vec<RuntimeInput> {
    (0..iterations)
        .map(|_| market_input(payload.clone()))
        .collect()
}

fn sparse_quote_inputs(
    iterations: u64,
    universe: &[String],
    changed_per_iter: usize,
) -> Vec<RuntimeInput> {
    (0..iterations)
        .map(|sequence| {
            let start = sequence as usize % universe.len().max(1);
            market_input(sparse_quote_rtn_data(
                universe,
                start,
                changed_per_iter,
                sequence,
            ))
        })
        .collect()
}

fn rolling_tick_inputs(iterations: u64, symbol: &str, window: usize) -> Vec<RuntimeInput> {
    (1..=iterations)
        .map(|tick_id| market_input(rolling_tick_rtn_data(symbol, tick_id as i64, window)))
        .collect()
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

fn sparse_quote_rtn_data(
    universe: &[String],
    start: usize,
    changed_count: usize,
    sequence: u64,
) -> Value {
    let count = changed_count.min(universe.len());
    let mut quotes = Map::with_capacity(count);
    for offset in 0..count {
        let index = (start + offset) % universe.len();
        let symbol = &universe[index];
        let mut fields = Map::new();
        fields.insert(
            "datetime".to_string(),
            Value::String(format!("202606101001{sequence:08}")),
        );
        fields.insert(
            "last_price".to_string(),
            number(600.0 + index as f64 * 0.01 + sequence as f64 * 0.001),
        );
        fields.insert(
            "volume".to_string(),
            Value::from(sequence as i64 + index as i64),
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

fn rolling_tick_rtn_data(symbol: &str, tick_id: i64, window: usize) -> Value {
    let mut data = Map::with_capacity(2);
    if tick_id > window as i64 {
        data.insert((tick_id - window as i64).to_string(), Value::Null);
    }
    data.insert(tick_id.to_string(), tick_fields(tick_id));

    let mut serial = Map::with_capacity(2);
    serial.insert("last_id".to_string(), Value::from(tick_id));
    serial.insert("data".to_string(), Value::Object(data));

    let mut ticks = Map::with_capacity(1);
    ticks.insert(symbol.to_string(), Value::Object(serial));

    let mut root = Map::with_capacity(1);
    root.insert("ticks".to_string(), Value::Object(ticks));

    let mut envelope = Map::with_capacity(2);
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

fn tick_fields(tick_id: i64) -> Value {
    let price = 600.0 + tick_id as f64 * 0.001;
    let mut fields = Map::new();
    fields.insert("id".to_string(), Value::from(tick_id));
    fields.insert(
        "datetime".to_string(),
        Value::from(1_780_000_000_000_000_000_i64 + tick_id),
    );
    fields.insert("last_price".to_string(), number(price));
    fields.insert("average".to_string(), number(price - 0.01));
    fields.insert("highest".to_string(), number(price + 0.2));
    fields.insert("lowest".to_string(), number(price - 0.2));
    fields.insert("ask_price1".to_string(), number(price + 0.01));
    fields.insert("ask_volume1".to_string(), Value::from(12_i64));
    fields.insert("bid_price1".to_string(), number(price - 0.01));
    fields.insert("bid_volume1".to_string(), Value::from(10_i64));
    fields.insert("volume".to_string(), Value::from(tick_id));
    fields.insert(
        "open_interest".to_string(),
        Value::from(10_000_i64 + tick_id),
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
