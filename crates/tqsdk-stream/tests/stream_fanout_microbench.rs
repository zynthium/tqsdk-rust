#![forbid(unsafe_code)]

use std::time::{Duration, Instant};

use futures::StreamExt;
use tqsdk_stream::{QuoteBatchSubscription, StreamFacadeError, TqStream};

mod support;

const NEXT_TIMEOUT: Duration = Duration::from_secs(2);

#[tokio::test(flavor = "current_thread")]
#[ignore = "benchmark-style fan-out probe; run explicitly with --ignored --nocapture"]
async fn quote_batches_fanout_reports_delivery_counts() {
    for consumer_count in [1_usize, 10, 100, 500] {
        let stream = support::core_seed::seeded_stream_with_capacity(2048);
        let mut consumers = quote_batch_consumers(&stream, consumer_count).await;
        let commits = 4_usize;

        let start = Instant::now();
        for sequence in 0..commits {
            support::core_seed::seed_quote_commit(&stream, "SHFE.au2602", 620.0 + sequence as f64);
        }

        let mut delivered = 0_usize;
        for consumer in &mut consumers {
            for _ in 0..commits {
                let batch = tokio::time::timeout(NEXT_TIMEOUT, consumer.next())
                    .await
                    .expect("quote batch consumer should not time out")
                    .expect("quote batch consumer should stay open")
                    .expect("quote batch consumer should decode update");
                assert_eq!(batch.quotes.len(), 1);
                assert_eq!(batch.quotes[0].symbol.as_str(), "SHFE.au2602");
                delivered += 1;
            }
        }

        assert_eq!(delivered, consumer_count * commits);
        println!(
            "quote_batches consumers={consumer_count} commits={commits} delivered={delivered} elapsed={:?}",
            start.elapsed()
        );
    }
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "benchmark-style path fan-out probe; run explicitly with --ignored --nocapture"]
async fn path_quote_fanout_filters_unmatched_symbols() {
    for symbol_count in [100_usize, 500] {
        let stream = support::core_seed::seeded_stream_with_capacity(2048);
        let symbols = bench_symbols(symbol_count);
        let mut consumers = symbols
            .iter()
            .map(|symbol| {
                stream
                    .quote_stream(symbol)
                    .expect("quote stream should open")
            })
            .collect::<Vec<_>>();

        let unmatched_revision = stream.reader().head_revision();
        support::core_seed::seed_quote_commit(&stream, "DCE.i2609", 712.0);
        assert_ne!(stream.reader().head_revision(), unmatched_revision);

        let quotes = symbols
            .iter()
            .enumerate()
            .map(|(index, symbol)| (symbol.as_str(), 620.0 + index as f64))
            .collect::<Vec<_>>();

        let start = Instant::now();
        support::core_seed::seed_quote_batch_commit(&stream, &quotes);
        let expected_revision = stream
            .reader()
            .head_revision()
            .expect("matching quote batch should advance the head revision");

        let mut delivered = 0_usize;
        for (index, consumer) in consumers.iter_mut().enumerate() {
            let update = tokio::time::timeout(NEXT_TIMEOUT, consumer.next())
                .await
                .expect("path quote consumer should not time out")
                .expect("path quote consumer should stay open")
                .expect("path quote consumer should decode update");
            assert_eq!(update.commit.revision, expected_revision);
            assert_eq!(update.value.instrument_id, symbols[index]);
            assert_eq!(update.value.last_price, 620.0 + index as f64);
            delivered += 1;
        }

        assert_eq!(delivered, symbol_count);
        println!(
            "path_quote_streams symbols={symbol_count} delivered={delivered} elapsed={:?}",
            start.elapsed()
        );
    }
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "benchmark-style lag probe; run explicitly with --ignored --nocapture"]
async fn slow_consumer_reports_lag_when_commit_stream_is_not_drained() {
    let stream = support::core_seed::seeded_stream_with_capacity(2);
    let mut commits = stream
        .commit_stream()
        .expect("commit stream should open and start the driver");

    let start = Instant::now();
    for sequence in 0..64 {
        support::core_seed::seed_quote_commit(&stream, "SHFE.au2602", 620.0 + sequence as f64);
    }
    tokio::time::sleep(Duration::from_millis(25)).await;

    let event = tokio::time::timeout(NEXT_TIMEOUT, commits.next())
        .await
        .expect("slow commit consumer should receive lag or commit")
        .expect("slow commit consumer should stay open");

    match event {
        Err(StreamFacadeError::Lagged { skipped }) => {
            assert!(skipped > 0);
            println!(
                "slow_consumer_lag capacity=2 seeded=64 skipped={skipped} elapsed={:?}",
                start.elapsed()
            );
        }
        Ok(commit) => panic!(
            "slow consumer should report lag before delivering commit revision {:?}",
            commit.revision
        ),
        Err(error) => panic!("slow consumer should report lag, got {error:?}"),
    }
}

async fn quote_batch_consumers(
    stream: &TqStream,
    consumer_count: usize,
) -> Vec<QuoteBatchSubscription> {
    let mut consumers = Vec::with_capacity(consumer_count);
    for _ in 0..consumer_count {
        consumers.push(
            stream
                .quote_batches(["SHFE.au2602"])
                .await
                .expect("quote batch subscription should open"),
        );
    }
    consumers
}

fn bench_symbols(count: usize) -> Vec<String> {
    (0..count)
        .map(|index| format!("SHFE.bench{index:04}"))
        .collect()
}
