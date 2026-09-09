#![forbid(unsafe_code)]

use std::{
    env,
    error::Error,
    hint::black_box,
    sync::Arc,
    time::{Duration, Instant},
};

use tqsdk_relay::{ClientId, DownstreamCommand, RelayEngine, RelayTickRow};

const SYMBOL: &str = "SHFE.bench0000";
const DEFAULT_ITERATIONS: u64 = 20_000;
const DEFAULT_CLIENT_COUNTS: &[usize] = &[1, 10, 100, 500];

fn main() -> Result<(), Box<dyn Error>> {
    let iterations = env_u64("TQSDK_RELAY_FANOUT_BENCH_ITERS", DEFAULT_ITERATIONS);
    let client_counts = env_client_counts("TQSDK_RELAY_FANOUT_BENCH_CLIENTS")
        .unwrap_or_else(|| DEFAULT_CLIENT_COUNTS.to_vec());

    println!("tqsdk-relay quote fanout microbench");
    println!("profile: run with --release for useful numbers");
    println!();
    println!(
        "{:<10} {:>10} {:>11} {:>11} {:>11} {:>11} {:>13}",
        "clients", "iters", "p50 ns", "p95 ns", "p99 ns", "p999 ns", "events/s"
    );

    for client_count in client_counts {
        let result = run_case(client_count, iterations)?;
        print_result(&result);
    }

    Ok(())
}

fn run_case(client_count: usize, iterations: u64) -> Result<BenchResult, Box<dyn Error>> {
    let mut engine = RelayEngine::new_memory_only(16, 16);
    for client in 0..client_count {
        engine.handle_command(
            ClientId::new(u64::try_from(client.saturating_add(1))?),
            DownstreamCommand::SubscribeQuote {
                symbols: vec![SYMBOL.to_string()],
            },
        )?;
    }

    let warm_frames = engine.ingest_tick(SYMBOL, tick(-1))?;
    assert_eq!(warm_frames.len(), client_count);
    assert_shared_payload(&warm_frames);

    let start = Instant::now();
    let mut latencies = Vec::new();
    for id in 0..iterations {
        let iteration_start = Instant::now();
        let frames = engine.ingest_tick(SYMBOL, tick(i64::try_from(id)?))?;
        debug_assert_eq!(frames.len(), client_count);
        black_box(frames);
        latencies.push(iteration_start.elapsed());
    }

    Ok(BenchResult {
        client_count,
        iterations,
        elapsed: start.elapsed(),
        latencies,
    })
}

fn assert_shared_payload(frames: &[tqsdk_relay::DownstreamFrame]) {
    let Some(first) = frames.first() else {
        return;
    };

    assert!(
        frames
            .iter()
            .skip(1)
            .all(|frame| Arc::ptr_eq(&first.payload, &frame.payload)),
        "quote fanout must share one immutable payload across downstream clients"
    );
}

fn tick(id: i64) -> RelayTickRow {
    RelayTickRow {
        id,
        datetime: 1_713_660_000_000_000_000_i64.saturating_add(id.saturating_mul(1_000_000)),
        last_price: 600.0 + id as f64 * 0.01,
        volume: id.saturating_mul(10),
        open_interest: 10_000_i64.saturating_add(id),
    }
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn env_client_counts(name: &str) -> Option<Vec<usize>> {
    env::var(name).ok().as_deref().and_then(parse_client_counts)
}

fn parse_client_counts(value: &str) -> Option<Vec<usize>> {
    let counts = value
        .split(',')
        .filter_map(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .collect::<Vec<_>>();
    (!counts.is_empty()).then_some(counts)
}

struct BenchResult {
    client_count: usize,
    iterations: u64,
    elapsed: Duration,
    latencies: Vec<Duration>,
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

fn print_result(result: &BenchResult) {
    println!(
        "{:<10} {:>10} {:>11.1} {:>11.1} {:>11.1} {:>11.1} {:>13.1}",
        result.client_count,
        result.iterations,
        result.percentile_ns(0.50),
        result.percentile_ns(0.95),
        result.percentile_ns(0.99),
        result.percentile_ns(0.999),
        result.events_per_second(),
    );
    println!(
        "clients={} iters={} events={} fanout_frames={}",
        result.client_count,
        result.iterations,
        result.iterations,
        result
            .iterations
            .saturating_mul(u64::try_from(result.client_count).unwrap_or(u64::MAX)),
    );
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{BenchResult, parse_client_counts};

    #[test]
    fn parses_nonzero_client_counts() {
        assert_eq!(
            parse_client_counts("1, 10, 0, invalid, 100"),
            Some(vec![1, 10, 100])
        );
    }

    #[test]
    fn reports_tail_latency_and_rate() {
        let result = BenchResult {
            client_count: 1,
            iterations: 5,
            elapsed: Duration::from_secs(2),
            latencies: vec![
                Duration::from_nanos(1),
                Duration::from_nanos(2),
                Duration::from_nanos(3),
                Duration::from_nanos(4),
                Duration::from_nanos(5),
            ],
        };

        assert_eq!(result.percentile_ns(0.50), 3.0);
        assert_eq!(result.percentile_ns(0.999), 5.0);
        assert_eq!(result.events_per_second(), 2.5);
    }

    #[test]
    fn client_count_parser_uses_empty_input_as_none() {
        assert_eq!(parse_client_counts("0, invalid"), None);
    }
}
