#![forbid(unsafe_code)]

use std::{
    env,
    error::Error,
    hint::black_box,
    time::{Duration, Instant},
};

use serde_json::json;
use tqsdk_core::{
    AdapterRegistry, CommitScope, InputPayload, IoEvent, ProtocolDomain, RuntimeHandle,
    RuntimeInput,
};
use tqsdk_session::testing::ManualSession;
use tqsdk_task::{RiskEngine, TaskHost};
use tqsdk_wait::TqApi;

const ACCOUNT_ID: &str = "bench";
const DEFAULT_ITERATIONS: u64 = 10_000;
const DEFAULT_SYMBOL_COUNTS: &[usize] = &[1];

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let iterations = env_u64("TQSDK_TASK_BENCH_ITERS", DEFAULT_ITERATIONS);
    let symbol_counts = env_usize_list("TQSDK_TASK_BENCH_SYMBOL_COUNTS")
        .unwrap_or_else(|| DEFAULT_SYMBOL_COUNTS.to_vec());

    println!("tqsdk-task commit/risk/submit microbench");
    println!("profile: run with --release for useful numbers");
    println!();
    for symbol_count in symbol_counts {
        let result = run_case(iterations, symbol_count).await?;
        println!(
            "symbols={} iters={} events={} p50_ns={:.1} p95_ns={:.1} p99_ns={:.1} p999_ns={:.1} events/s={:.1} dispatches={}",
            result.symbols,
            result.iterations,
            result.iterations,
            result.percentile_ns(0.50),
            result.percentile_ns(0.95),
            result.percentile_ns(0.99),
            result.percentile_ns(0.999),
            result.events_per_second(),
            result.dispatches,
        );
    }

    Ok(())
}

async fn run_case(iterations: u64, symbol_count: usize) -> Result<BenchResult, Box<dyn Error>> {
    let mut adapters = AdapterRegistry::new();
    adapters.register_default_adapters();
    let handle = RuntimeHandle::with_adapters(adapters);
    let session = ManualSession::from_runtime(handle.clone());
    let api = TqApi::new(session.client_clone());
    let mut host = TaskHost::new(api).with_risk(RiskEngine::new().max_price_deviation(20.0));

    let mut latencies = Vec::new();
    let mut dispatches = 0_usize;
    let symbols = benchmark_symbols(symbol_count);
    let symbol_count = u64::try_from(symbols.len())?;
    let started = Instant::now();
    for sequence in 0..iterations {
        let iteration_started = Instant::now();
        let symbol_index = usize::try_from(sequence % symbol_count)?;
        let symbol = &symbols[symbol_index];
        handle.ingest(
            quote_input(sequence, symbol),
            Vec::new(),
            CommitScope::RealtimeUpdate,
        )?;

        let ticket = host
            .orders(ACCOUNT_ID)
            .buy_open(symbol, 1)
            .limit(3_600.0)
            .send_once(format!("bench-{sequence}"))
            .await?;
        assert!(ticket.was_submitted());

        let dispatched = handle.drain_dispatches()?;
        dispatches = dispatches.saturating_add(dispatched.len());
        black_box((ticket.command_id(), dispatched));
        latencies.push(iteration_started.elapsed());

        // ManualSession does not receive an exchange terminal-order update.
        // Release the submitted record after timing the submit path so a long
        // benchmark cannot mistake the deliberately bounded intent ledger for
        // throughput backpressure.
        assert!(
            session
                .client_clone()
                .forget_order_intent(ticket.order().account_id(), ticket.client_order_id())?
                .is_some(),
            "submitted benchmark order intent must be present for cleanup"
        );
    }

    assert_eq!(dispatches, usize::try_from(iterations)?);
    Ok(BenchResult {
        symbols: symbols.len(),
        iterations,
        elapsed: started.elapsed(),
        latencies,
        dispatches,
    })
}

fn quote_input(sequence: u64, symbol: &str) -> RuntimeInput {
    RuntimeInput::Io(IoEvent {
        route: "market".to_string(),
        domains: vec![ProtocolDomain::Market],
        payload: InputPayload::Json(json!({
            "aid": "rtn_data",
            "data": [{
                "quotes": {
                symbol: {
                        "datetime": format!("2026061010{sequence:08}"),
                        "last_price": 3_600.0 + sequence as f64 * 0.001,
                    }
                }
            }]
        })),
    })
}

fn benchmark_symbols(count: usize) -> Vec<String> {
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

fn env_usize_list(name: &str) -> Option<Vec<usize>> {
    env::var(name).ok().as_deref().and_then(parse_usize_list)
}

fn parse_usize_list(value: &str) -> Option<Vec<usize>> {
    let values = value
        .split(',')
        .filter_map(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .collect::<Vec<_>>();
    (!values.is_empty()).then_some(values)
}

struct BenchResult {
    symbols: usize,
    iterations: u64,
    elapsed: Duration,
    latencies: Vec<Duration>,
    dispatches: usize,
}

impl BenchResult {
    fn events_per_second(&self) -> f64 {
        let seconds = self.elapsed.as_secs_f64();
        if seconds == 0.0 {
            return 0.0;
        }

        self.iterations as f64 / seconds
    }

    fn percentile_ns(&self, percentile: f64) -> f64 {
        if self.latencies.is_empty() {
            return 0.0;
        }

        let mut samples = self
            .latencies
            .iter()
            .map(Duration::as_nanos)
            .collect::<Vec<_>>();
        samples.sort_unstable();
        let index = ((samples.len() - 1) as f64 * percentile).ceil() as usize;
        samples[index] as f64
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{BenchResult, parse_usize_list};

    #[test]
    fn parses_symbol_counts() {
        assert_eq!(parse_usize_list("1, 10, 0, 100"), Some(vec![1, 10, 100]));
        assert_eq!(parse_usize_list("0, nope"), None);
    }

    #[test]
    fn reports_tail_latency_and_rate() {
        let result = BenchResult {
            symbols: 1,
            iterations: 5,
            elapsed: Duration::from_secs(2),
            latencies: vec![
                Duration::from_nanos(1),
                Duration::from_nanos(2),
                Duration::from_nanos(3),
                Duration::from_nanos(4),
                Duration::from_nanos(5),
            ],
            dispatches: 5,
        };

        assert_eq!(result.percentile_ns(0.50), 3.0);
        assert_eq!(result.percentile_ns(0.999), 5.0);
        assert_eq!(result.events_per_second(), 2.5);
    }
}
