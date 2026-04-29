use std::io::Cursor;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tqsdk_core::{Kline, Quote, Tick};
use tqsdk_data::{
    MarketCacheCompaction, MarketCacheDaemon, MarketCacheDaemonConfig, MarketCacheEvent,
    MarketCacheIndex, MarketCacheLock, MarketCacheLockOptions, MarketCachePayload,
    MarketCachePayloadKind, MarketCacheQueue, MarketCacheReader, MarketCacheReaderCheckpoint,
    MarketCacheReaderManifest, MarketCacheRecoveryFileKind, MarketCacheRecoveryScan,
    MarketCacheReplay, MarketCacheSupervisorConfig, MarketCacheWriter,
};

#[test]
fn market_cache_event_constructors_preserve_standard_payloads() {
    let mut quote = Quote {
        last_price: 480.5,
        ..Quote::default()
    };
    quote.datetime = "2026-04-27 10:00:00.000000".into();

    let quote_event =
        MarketCacheEvent::quote("live", "SHFE.au2602", 1_000, Some(900), quote).unwrap();
    assert_eq!(quote_event.source, "live");
    assert_eq!(quote_event.symbol, "SHFE.au2602");
    assert_eq!(quote_event.event_time_ns(), 900);
    match quote_event.payload {
        MarketCachePayload::Quote(payload) => assert_eq!(payload.last_price, 480.5),
        _ => panic!("expected quote payload"),
    }

    let kline = Kline {
        datetime: 2_000,
        close: 481.0,
        ..Kline::default()
    };
    let kline_event = MarketCacheEvent::kline(
        "history",
        "SHFE.au2602",
        2_100,
        Some(2_000),
        60_000_000_000,
        kline,
    )
    .unwrap();
    assert_eq!(kline_event.event_time_ns(), 2_000);
    match kline_event.payload {
        MarketCachePayload::Kline { duration_ns, row } => {
            assert_eq!(duration_ns, 60_000_000_000);
            assert_eq!(row.close, 481.0);
        }
        _ => panic!("expected kline payload"),
    }

    let tick = Tick {
        datetime: 3_000,
        last_price: 482.0,
        ..Tick::default()
    };
    let tick_event = MarketCacheEvent::tick("history", "SHFE.au2602", 3_100, None, tick).unwrap();
    assert_eq!(tick_event.event_time_ns(), 3_100);
    match tick_event.payload {
        MarketCachePayload::Tick(payload) => assert_eq!(payload.last_price, 482.0),
        _ => panic!("expected tick payload"),
    }
}

#[test]
fn market_cache_event_rejects_invalid_identity_and_times() {
    assert!(MarketCacheEvent::quote("live", "", 1, None, Quote::default()).is_err());
    assert!(MarketCacheEvent::quote("", "SHFE.au2602", 1, None, Quote::default()).is_err());
    assert!(MarketCacheEvent::quote("live", "SHFE.au2602", -1, None, Quote::default()).is_err());
    assert!(
        MarketCacheEvent::kline("history", "SHFE.au2602", 1, None, 0, Kline::default()).is_err()
    );
}

#[test]
fn market_cache_writer_and_reader_roundtrip_jsonl_events() {
    let quote = Quote {
        last_price: 481.0,
        ..Quote::default()
    };
    let event = MarketCacheEvent::quote("live", "SHFE.au2602", 1_000, Some(900), quote).unwrap();

    let mut bytes = Vec::new();
    {
        let mut writer = MarketCacheWriter::new(&mut bytes);
        writer.write_event(&event).unwrap();
        writer.flush().unwrap();
    }

    let decoded: Vec<_> = MarketCacheReader::new(Cursor::new(bytes))
        .collect::<tqsdk_data::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].source, event.source);
    assert_eq!(decoded[0].symbol, event.symbol);
    assert_eq!(decoded[0].received_at_ns, event.received_at_ns);
    assert_eq!(decoded[0].exchange_time_ns, event.exchange_time_ns);
    match &decoded[0].payload {
        MarketCachePayload::Quote(payload) => assert_eq!(payload.last_price, 481.0),
        _ => panic!("expected quote payload"),
    }
}

#[test]
fn market_cache_replay_orders_events_by_event_time_then_receive_time() {
    let late_received_early_exchange =
        MarketCacheEvent::quote("live", "SHFE.au2602", 2_000, Some(1_000), Quote::default())
            .unwrap();
    let early_received_late_exchange =
        MarketCacheEvent::quote("live", "SHFE.au2602", 1_000, Some(3_000), Quote::default())
            .unwrap();
    let no_exchange_time =
        MarketCacheEvent::quote("live", "SHFE.au2602", 1_500, None, Quote::default()).unwrap();

    let replay = MarketCacheReplay::new(vec![
        early_received_late_exchange,
        no_exchange_time,
        late_received_early_exchange,
    ]);
    let ordered: Vec<_> = replay.collect();
    let order_keys: Vec<_> = ordered
        .iter()
        .map(|event| (event.event_time_ns(), event.received_at_ns))
        .collect();

    assert_eq!(
        order_keys,
        vec![(1_000, 2_000), (1_500, 1_500), (3_000, 1_000)]
    );
}

#[test]
fn market_cache_index_groups_events_by_source_symbol_and_payload_kind() {
    let events = [
        quote_event("live", "SHFE.au2602", 2_000, Some(1_000), 480.0),
        quote_event("live", "SHFE.au2602", 3_000, Some(1_500), 481.0),
        MarketCacheEvent::tick(
            "history",
            "SHFE.au2602",
            4_000,
            Some(2_000),
            Tick {
                datetime: 2_000,
                last_price: 482.0,
                ..Tick::default()
            },
        )
        .unwrap(),
    ];

    let index = MarketCacheIndex::from_events(events.iter());

    assert_eq!(index.total_events(), 3);
    let quote_entry = index
        .entry("live", "SHFE.au2602", MarketCachePayloadKind::Quote)
        .unwrap();
    assert_eq!(quote_entry.events, 2);
    assert_eq!(quote_entry.min_event_time_ns, 1_000);
    assert_eq!(quote_entry.max_event_time_ns, 1_500);
    assert!(
        index
            .entry("history", "SHFE.au2602", MarketCachePayloadKind::Tick)
            .is_some()
    );
}

#[test]
fn market_cache_reader_manifest_records_checkpoints_and_reports_reader_lag() {
    let manifest_path = temp_path("market-cache-readers.json");
    let _ = std::fs::remove_file(&manifest_path);

    let first = quote_event("live", "SHFE.au2602", 2_000, Some(1_000), 480.0);
    let second = quote_event("live", "DCE.m2601", 3_000, Some(1_500), 3_100.0);
    let manifest = MarketCacheReaderManifest::open(&manifest_path).unwrap();
    manifest
        .record_checkpoint(MarketCacheReaderCheckpoint::from_event(
            "research-a",
            "last-close-study",
            &first,
        ))
        .unwrap();
    manifest
        .record_checkpoint(MarketCacheReaderCheckpoint::from_event(
            "replay-b",
            "risk-replay",
            &second,
        ))
        .unwrap();

    let reopened = MarketCacheReaderManifest::open(&manifest_path).unwrap();
    let checkpoint = reopened.checkpoint("research-a").unwrap().unwrap();
    assert_eq!(checkpoint.reader_id, "research-a");
    assert_eq!(checkpoint.checkpoint_id, "last-close-study");
    assert_eq!(checkpoint.source, "live");
    assert_eq!(checkpoint.symbol, "SHFE.au2602");
    assert_eq!(checkpoint.payload_kind, MarketCachePayloadKind::Quote);
    assert_eq!(checkpoint.event_time_ns, 1_000);
    assert_eq!(checkpoint.received_at_ns, 2_000);
    assert_eq!(
        reopened.compaction_floor_event_time_ns().unwrap(),
        Some(1_000)
    );

    let lag = reopened.reader_lag_report(2_500).unwrap();
    assert_eq!(lag.len(), 2);
    assert_eq!(lag[0].reader_id, "research-a");
    assert_eq!(lag[0].lag_event_time_ns, 1_500);
    assert_eq!(lag[1].reader_id, "replay-b");
    assert_eq!(lag[1].lag_event_time_ns, 1_000);

    reopened.remove_reader("research-a").unwrap();
    assert_eq!(
        reopened.compaction_floor_event_time_ns().unwrap(),
        Some(1_500)
    );
    assert!(reopened.checkpoint("research-a").unwrap().is_none());
}

#[test]
fn market_cache_reader_manifest_rejects_invalid_checkpoints() {
    let manifest_path = temp_path("market-cache-invalid-readers.json");
    let _ = std::fs::remove_file(&manifest_path);
    let manifest = MarketCacheReaderManifest::open(&manifest_path).unwrap();
    let event = quote_event("live", "SHFE.au2602", 2_000, Some(1_000), 480.0);

    assert!(
        MarketCacheReaderCheckpoint::from_event("", "last-close-study", &event)
            .validate()
            .is_err()
    );
    assert!(
        MarketCacheReaderCheckpoint::from_event("research-a", "", &event)
            .validate()
            .is_err()
    );

    let mut invalid =
        MarketCacheReaderCheckpoint::from_event("research-a", "last-close-study", &event);
    invalid.event_time_ns = -1;
    assert!(manifest.record_checkpoint(invalid).is_err());
}

#[test]
fn market_cache_recovery_scan_reports_pending_files_and_recovery_flags() {
    let cache_path = temp_path("market-cache-recovery.cache");
    let queue_path = temp_path("market-cache-recovery.queue");
    let processing_path = temp_path("market-cache-recovery.processing");
    let staging_path = temp_path("market-cache-recovery.compact");
    let _ = std::fs::remove_file(&cache_path);
    let _ = std::fs::remove_file(&queue_path);
    let _ = std::fs::remove_file(&processing_path);
    let _ = std::fs::remove_file(&staging_path);

    write_events(
        &cache_path,
        &[
            quote_event("live", "SHFE.au2602", 1_000, Some(500), 479.0),
            quote_event("live", "SHFE.au2602", 2_000, Some(1_500), 480.0),
        ],
    );
    write_events(
        &queue_path,
        &[quote_event(
            "live",
            "DCE.m2601",
            3_000,
            Some(2_500),
            3_100.0,
        )],
    );
    write_events(
        &processing_path,
        &[quote_event(
            "live",
            "DCE.m2601",
            4_000,
            Some(3_500),
            3_101.0,
        )],
    );
    write_events(
        &staging_path,
        &[quote_event(
            "live",
            "SHFE.au2602",
            2_000,
            Some(1_500),
            480.0,
        )],
    );

    let report = MarketCacheRecoveryScan::new(&cache_path)
        .queue_path(&queue_path)
        .processing_queue_path(&processing_path)
        .compaction_staging_path(&staging_path)
        .scan()
        .unwrap();

    assert_eq!(report.cache.kind, MarketCacheRecoveryFileKind::Cache);
    assert_eq!(report.cache.readable_events, 2);
    assert_eq!(report.cache.first_event_time_ns, Some(500));
    assert_eq!(report.cache.last_event_time_ns, Some(1_500));
    assert_eq!(report.queue.readable_events, 1);
    assert_eq!(report.processing_queue.readable_events, 1);
    assert_eq!(report.compaction_staging.readable_events, 1);
    assert!(report.has_pending_queue_events());
    assert!(report.has_interrupted_drain());
    assert!(report.has_interrupted_compaction());
    assert!(report.requires_writer_recovery());
}

#[test]
fn market_cache_recovery_scan_reports_corrupt_file_without_hiding_progress() {
    let cache_path = temp_path("market-cache-recovery-corrupt.cache");
    let queue_path = temp_path("market-cache-recovery-corrupt.queue");
    let _ = std::fs::remove_file(&cache_path);
    let _ = std::fs::remove_file(&queue_path);

    write_events(
        &queue_path,
        &[quote_event(
            "live",
            "SHFE.au2602",
            2_000,
            Some(1_500),
            480.0,
        )],
    );
    {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&queue_path)
            .unwrap();
        writeln!(file, "not-json").unwrap();
    }

    let report = MarketCacheRecoveryScan::new(&cache_path)
        .queue_path(&queue_path)
        .scan()
        .unwrap();

    assert!(!report.cache.exists);
    assert_eq!(report.queue.readable_events, 1);
    assert!(report.queue.read_error.is_some());
    assert!(report.has_read_errors());
    assert!(report.requires_writer_recovery());
}

#[test]
fn market_cache_queue_drains_to_writer_after_success() {
    let queue_path = temp_path("market-cache-queue.jsonl");
    let cache_path = temp_path("market-cache-drain.jsonl");
    let _ = std::fs::remove_file(&queue_path);
    let _ = std::fs::remove_file(&cache_path);

    let queue = MarketCacheQueue::open(&queue_path).unwrap();
    queue
        .enqueue_event(&quote_event(
            "live",
            "SHFE.au2602",
            2_000,
            Some(1_000),
            480.0,
        ))
        .unwrap();
    queue
        .enqueue_event(&quote_event(
            "live",
            "SHFE.au2602",
            3_000,
            Some(1_500),
            481.0,
        ))
        .unwrap();

    let mut writer = MarketCacheWriter::create(&cache_path).unwrap();
    let report = queue.drain_to_writer(&mut writer).unwrap();

    assert_eq!(report.read_events, 2);
    assert_eq!(report.written_events, 2);
    assert!(queue.is_empty().unwrap());

    let drained = MarketCacheReader::open(&cache_path)
        .unwrap()
        .collect::<tqsdk_data::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(drained.len(), 2);
}

#[test]
fn market_cache_lock_blocks_second_holder_until_released() {
    let lock_path = temp_path("market-cache.lock");
    let _ = std::fs::remove_file(&lock_path);

    let first = MarketCacheLock::acquire(&lock_path).unwrap();
    assert!(MarketCacheLock::acquire(&lock_path).is_err());

    drop(first);
    let second = MarketCacheLock::acquire(&lock_path).unwrap();
    assert_eq!(second.path(), lock_path.as_path());
}

#[test]
fn market_cache_lock_recovers_stale_lease_file_and_can_renew() {
    let lock_path = temp_path("market-cache-stale.lock");
    let _ = std::fs::remove_file(&lock_path);
    std::fs::write(&lock_path, "pid=999999\nlease_started_at_ns=0\n").unwrap();

    let mut lock = MarketCacheLock::acquire_with_options(
        MarketCacheLockOptions::new(&lock_path).stale_after(Duration::from_secs(1)),
    )
    .unwrap();
    assert_eq!(lock.path(), lock_path.as_path());

    let before = std::fs::read_to_string(&lock_path).unwrap();
    lock.renew().unwrap();
    let after = std::fs::read_to_string(&lock_path).unwrap();
    assert_ne!(before, after);
    assert!(after.contains("lease_started_at_ns="));
}

#[test]
fn market_cache_lock_renew_detects_replaced_lease_file() {
    let lock_path = temp_path("market-cache-replaced.lock");
    let _ = std::fs::remove_file(&lock_path);

    let mut lock = MarketCacheLock::acquire(&lock_path).unwrap();
    std::fs::remove_file(&lock_path).unwrap();
    std::fs::write(&lock_path, "pid=1\nlease_started_at_ns=1\n").unwrap();

    assert!(lock.renew().is_err());
    drop(lock);
    assert!(lock_path.exists());
}

#[test]
fn market_cache_queue_drain_error_reports_progress_and_keeps_queue() {
    let queue_path = temp_path("market-cache-queue-error.jsonl");
    let _ = std::fs::remove_file(&queue_path);

    let queue = MarketCacheQueue::open(&queue_path).unwrap();
    queue
        .enqueue_event(&quote_event(
            "live",
            "SHFE.au2602",
            2_000,
            Some(1_000),
            480.0,
        ))
        .unwrap();
    {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&queue_path)
            .unwrap();
        writeln!(file, "not-json").unwrap();
    }

    let mut bytes = Vec::new();
    let mut writer = MarketCacheWriter::new(&mut bytes);
    let error = queue.drain_to_writer_with_report(&mut writer).unwrap_err();

    assert_eq!(error.report.read_events, 1);
    assert_eq!(error.report.written_events, 1);
    assert!(!queue.is_empty().unwrap());
}

#[test]
fn market_cache_compaction_filters_by_event_time_and_builds_index() {
    let mut input = Vec::new();
    {
        let mut writer = MarketCacheWriter::new(&mut input);
        writer
            .write_event(&quote_event("live", "SHFE.au2602", 1_000, Some(500), 479.0))
            .unwrap();
        writer
            .write_event(&quote_event(
                "live",
                "SHFE.au2602",
                2_000,
                Some(1_500),
                480.0,
            ))
            .unwrap();
        writer
            .write_event(&quote_event(
                "history",
                "DCE.m2601",
                3_000,
                Some(2_500),
                3_100.0,
            ))
            .unwrap();
        writer.flush().unwrap();
    }

    let mut compacted = Vec::new();
    let report = {
        let mut output = MarketCacheWriter::new(&mut compacted);
        let report = MarketCacheCompaction::new()
            .retain_event_time_from(1_000)
            .retain_symbol("SHFE.au2602")
            .compact_reader_to_writer(MarketCacheReader::new(Cursor::new(input)), &mut output)
            .unwrap();
        output.flush().unwrap();
        report
    };

    assert_eq!(report.read_events, 3);
    assert_eq!(report.written_events, 1);
    assert_eq!(report.dropped_events, 2);
    assert_eq!(report.index.total_events(), 1);

    let events = MarketCacheReader::new(Cursor::new(compacted))
        .collect::<tqsdk_data::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_time_ns(), 1_500);
    assert_eq!(events[0].symbol, "SHFE.au2602");
}

#[test]
fn market_cache_compaction_rotates_cache_file_after_success() {
    let cache_path = temp_path("market-cache-rotate.jsonl");
    let staging_path = temp_path("market-cache-rotate.tmp");
    let _ = std::fs::remove_file(&cache_path);
    let _ = std::fs::remove_file(&staging_path);
    {
        let mut writer = MarketCacheWriter::create(&cache_path).unwrap();
        writer
            .write_event(&quote_event("live", "SHFE.au2602", 1_000, Some(500), 479.0))
            .unwrap();
        writer
            .write_event(&quote_event(
                "live",
                "SHFE.au2602",
                2_000,
                Some(1_500),
                480.0,
            ))
            .unwrap();
        writer.flush().unwrap();
    }

    let report = MarketCacheCompaction::new()
        .retain_event_time_from(1_000)
        .compact_file_in_place(&cache_path, &staging_path)
        .unwrap();

    assert_eq!(report.compaction.written_events, 1);
    assert!(!staging_path.exists());

    let events = MarketCacheReader::open(&cache_path)
        .unwrap()
        .collect::<tqsdk_data::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_time_ns(), 1_500);
}

#[test]
fn market_cache_daemon_shutdown_flushes_queue_and_compacts_cache() {
    let cache_path = temp_path("market-cache-daemon.jsonl");
    let queue_path = temp_path("market-cache-daemon.queue");
    let lock_path = temp_path("market-cache-daemon.lock");
    let staging_path = temp_path("market-cache-daemon.tmp");
    let _ = std::fs::remove_file(&cache_path);
    let _ = std::fs::remove_file(&queue_path);
    let _ = std::fs::remove_file(&lock_path);
    let _ = std::fs::remove_file(&staging_path);

    let config = MarketCacheDaemonConfig::new(&cache_path)
        .queue_path(&queue_path)
        .lock_path(&lock_path)
        .compaction_staging_path(&staging_path)
        .stale_lock_after(Duration::from_secs(30))
        .compaction_policy(MarketCacheCompaction::new().retain_event_time_from(1_000));
    let daemon = MarketCacheDaemon::open(config).unwrap();
    daemon
        .enqueue_event(&quote_event("live", "SHFE.au2602", 1_000, Some(500), 479.0))
        .unwrap();
    daemon
        .enqueue_event(&quote_event(
            "live",
            "SHFE.au2602",
            2_000,
            Some(1_500),
            480.0,
        ))
        .unwrap();

    let report = daemon.shutdown().unwrap();

    assert_eq!(report.flush_report.written_events, 2);
    assert_eq!(
        report
            .compaction_report
            .as_ref()
            .unwrap()
            .compaction
            .written_events,
        1
    );
    assert!(report.queue_empty);

    let events = MarketCacheReader::open(&cache_path)
        .unwrap()
        .collect::<tqsdk_data::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_time_ns(), 1_500);
}

#[test]
fn market_cache_supervisor_flushes_periodically_renews_lease_and_shuts_down() {
    let cache_path = temp_path("market-cache-supervisor.jsonl");
    let queue_path = temp_path("market-cache-supervisor.queue");
    let lock_path = temp_path("market-cache-supervisor.lock");
    let staging_path = temp_path("market-cache-supervisor.tmp");
    let processing_path = temp_path("market-cache-supervisor.processing");
    let _ = std::fs::remove_file(&cache_path);
    let _ = std::fs::remove_file(&queue_path);
    let _ = std::fs::remove_file(&lock_path);
    let _ = std::fs::remove_file(&staging_path);
    let _ = std::fs::remove_file(&processing_path);

    let daemon = MarketCacheDaemon::open(
        MarketCacheDaemonConfig::new(&cache_path)
            .queue_path(&queue_path)
            .lock_path(&lock_path)
            .compaction_staging_path(&staging_path)
            .stale_lock_after(Duration::from_secs(30))
            .compaction_policy(MarketCacheCompaction::new().retain_event_time_from(1_000)),
    )
    .unwrap();
    let supervisor = daemon
        .spawn_supervisor(
            MarketCacheSupervisorConfig::new()
                .flush_interval(Duration::from_millis(10))
                .lease_renew_interval(Duration::from_millis(10))
                .processing_queue_path(&processing_path),
        )
        .unwrap();

    supervisor
        .enqueue_event(&quote_event("live", "SHFE.au2602", 1_000, Some(500), 479.0))
        .unwrap();
    supervisor
        .enqueue_event(&quote_event(
            "live",
            "SHFE.au2602",
            2_000,
            Some(1_500),
            480.0,
        ))
        .unwrap();

    wait_until(Duration::from_secs(2), || {
        MarketCacheReader::open(&cache_path)
            .map(|reader| reader.count() >= 2)
            .unwrap_or(false)
    });

    let report = supervisor.shutdown().unwrap();

    assert!(report.periodic_flushes > 0 || report.shutdown.flush_report.written_events > 0);
    assert!(report.lease_renewals > 0);
    assert!(report.shutdown.queue_empty);

    let events = MarketCacheReader::open(&cache_path)
        .unwrap()
        .collect::<tqsdk_data::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_time_ns(), 1_500);
}

fn quote_event(
    source: &str,
    symbol: &str,
    received_at_ns: i64,
    exchange_time_ns: Option<i64>,
    last_price: f64,
) -> MarketCacheEvent {
    MarketCacheEvent::quote(
        source,
        symbol,
        received_at_ns,
        exchange_time_ns,
        Quote {
            last_price,
            ..Quote::default()
        },
    )
    .unwrap()
}

fn write_events(path: impl AsRef<std::path::Path>, events: &[MarketCacheEvent]) {
    let mut writer = MarketCacheWriter::create(path).unwrap();
    for event in events {
        writer.write_event(event).unwrap();
    }
    writer.flush().unwrap();
}

fn temp_path(file_name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "tqsdk-data-{}-{nanos}-{file_name}",
        std::process::id()
    ))
}

fn wait_until(timeout: Duration, mut condition: impl FnMut() -> bool) {
    let started_at = SystemTime::now();
    while !condition() {
        if SystemTime::now()
            .duration_since(started_at)
            .unwrap_or_default()
            >= timeout
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}
