use futures::StreamExt;
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tqsdk_core::Quote;
use tqsdk_stream::{
    CommitSink, StreamCommitJournal, StreamFacadeError, StreamSinkFuture, StreamSinkOptions,
    StreamSinkProfile, StreamSinkRetryPolicy, StreamSinkStatus, StreamSinkWalCompaction,
    StreamSinkWalFsyncPolicy, StreamSinkWalRecord, StreamSinkWalRecordKind, StreamSinkWalRecovery,
};

mod support;

#[tokio::test(flavor = "current_thread")]
async fn commit_stream_emits_commit_and_reader_sees_matching_state() {
    let stream = support::core_seed::seeded_stream();
    let mut commits = stream.commit_stream().unwrap();

    support::core_seed::seed_quote_commit(&stream, "SHFE.au2602", 618.5);

    let commit = commits
        .next()
        .await
        .expect("commit stream should yield an item")
        .expect("first item should be a commit");

    assert!(commit.changes.path_hits.iter().any(|path| {
        matches!(
            path.segments(),
            [first, second] if first == "quotes" && second == "SHFE.au2602"
        )
    }));

    let snapshot = stream.reader().read();
    let quote = snapshot
        .decode_path::<Quote>(&["quotes", "SHFE.au2602"])
        .unwrap()
        .expect("quote snapshot should be readable");
    assert_eq!(quote.last_price, 618.5);
    assert_eq!(snapshot.revision(), commit.revision);
}

#[tokio::test(flavor = "current_thread")]
async fn multiple_commit_receivers_observe_same_revision() {
    let stream = support::core_seed::seeded_stream();
    let mut first = stream.commit_stream().unwrap();
    let mut second = stream.commit_stream().unwrap();

    support::core_seed::seed_quote_commit(&stream, "SHFE.au2602", 619.0);

    let first_commit = first
        .next()
        .await
        .expect("first receiver should yield an item")
        .expect("first receiver should observe a commit");
    let second_commit = second
        .next()
        .await
        .expect("second receiver should yield an item")
        .expect("second receiver should observe a commit");

    assert_eq!(first_commit.revision, second_commit.revision);
}

#[tokio::test(flavor = "current_thread")]
async fn commit_stream_wakes_after_becoming_idle() {
    let stream = support::core_seed::seeded_stream();
    let mut commits = stream.commit_stream().unwrap();

    tokio::task::yield_now().await;
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;

    support::core_seed::seed_quote_commit(&stream, "SHFE.au2602", 619.5);

    let commit = tokio::time::timeout(std::time::Duration::from_millis(50), commits.next())
        .await
        .expect("idle commit stream should wake on the next commit")
        .expect("commit stream should yield a commit after waking")
        .expect("woken commit stream should receive a commit event");

    assert_eq!(commit.revision.get(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn lagged_receiver_reports_backpressure_explicitly() {
    let stream = support::core_seed::seeded_stream_with_capacity(1);
    let mut commits = stream.commit_stream().unwrap();

    support::core_seed::seed_quote_commit(&stream, "SHFE.au2602", 618.0);
    support::core_seed::seed_quote_commit(&stream, "SHFE.au2602", 619.0);
    support::core_seed::seed_quote_commit(&stream, "SHFE.au2602", 620.0);
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    let update = commits
        .next()
        .await
        .expect("commit stream should yield lag information");

    assert!(matches!(
        update,
        Err(StreamFacadeError::Lagged { skipped }) if skipped >= 1
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn managed_commit_sink_runs_outside_core_consumer_loop_and_flushes_on_shutdown() {
    let stream = support::core_seed::seeded_stream();
    let mut strategy_commits = stream.commit_stream().unwrap();
    let blocker = Arc::new(tokio::sync::Notify::new());
    let sink = BlockingSink::new(Arc::clone(&blocker));
    let sink_state = sink.state();
    let sink_handle = stream.spawn_commit_sink("warehouse", sink).unwrap();

    support::core_seed::seed_quote_commit(&stream, "SHFE.au2602", 618.0);

    let strategy_commit = tokio::time::timeout(
        std::time::Duration::from_millis(50),
        strategy_commits.next(),
    )
    .await
    .expect("strategy consumer should not wait for slow sink")
    .expect("strategy consumer should receive a stream update")
    .expect("strategy consumer should receive a commit");
    assert_eq!(strategy_commit.revision.get(), 1);

    wait_until(|| sink_state.started.load(Ordering::Acquire) == 1).await;
    assert_eq!(sink_handle.stats().processed_commits(), 0);
    assert_eq!(sink_handle.status(), StreamSinkStatus::Running);

    blocker.notify_waiters();
    wait_until(|| sink_handle.stats().processed_commits() == 1).await;

    let report = sink_handle.shutdown().await.unwrap();
    assert_eq!(report.name(), "warehouse");
    assert_eq!(report.status(), StreamSinkStatus::Stopped);
    assert_eq!(report.stats().processed_commits(), 1);
    assert!(report.flushed());
    assert_eq!(sink_state.flushed.load(Ordering::Acquire), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn managed_commit_sink_retries_failures_and_records_jsonl_wal() {
    let wal_path = temp_wal_path("stream-sink-policy");
    let _ = std::fs::remove_file(&wal_path);

    let stream = support::core_seed::seeded_stream();
    let sink = FlakySink::new(2);
    let sink_state = sink.state();
    let options = StreamSinkOptions::new()
        .retry_policy(StreamSinkRetryPolicy::limited(3).unwrap())
        .jsonl_wal(wal_path.clone());
    let sink_handle = stream
        .spawn_commit_sink_with_options("warehouse", sink, options)
        .unwrap();

    support::core_seed::seed_quote_commit(&stream, "SHFE.au2602", 618.0);

    wait_until(|| sink_handle.stats().processed_commits() == 1).await;
    assert_eq!(sink_state.attempts.load(Ordering::Acquire), 3);
    assert_eq!(sink_handle.stats().retry_attempts(), 2);
    assert_eq!(sink_handle.stats().errors(), 2);

    let report = sink_handle.shutdown().await.unwrap();
    assert_eq!(report.status(), StreamSinkStatus::Stopped);
    assert_eq!(report.stats().processed_commits(), 1);
    assert_eq!(report.stats().retry_attempts(), 2);
    assert_eq!(report.stats().errors(), 2);
    assert_eq!(report.stats().wal_records(), 5);
    assert!(report.flushed());

    let records = read_wal_records(&wal_path);
    assert_eq!(records[0]["kind"], "received");
    assert_eq!(records[0]["revision"], 1);
    assert_eq!(records[0]["attempt"], 1);
    assert_eq!(
        records
            .iter()
            .filter(|record| record["kind"] == "attempt_failed")
            .count(),
        2
    );
    assert!(records.iter().any(|record| {
        record["kind"] == "delivered" && record["revision"] == 1 && record["attempt"] == 3
    }));
    assert!(
        records
            .iter()
            .any(|record| record["kind"] == "flush_succeeded")
    );

    let _ = std::fs::remove_file(&wal_path);
}

#[tokio::test(flavor = "current_thread")]
async fn managed_commit_sink_records_replayable_jsonl_commit_journal() {
    let journal_path = temp_wal_path("stream-commit-journal");
    let _ = std::fs::remove_file(&journal_path);

    let stream = support::core_seed::seeded_stream();
    let sink = CountingSink::new();
    let options = StreamSinkOptions::new().jsonl_commit_journal(journal_path.clone());
    let sink_handle = stream
        .spawn_commit_sink_with_options("warehouse", sink, options)
        .unwrap();

    support::core_seed::seed_quote_commit(&stream, "SHFE.au2602", 618.0);
    support::core_seed::seed_quote_commit(&stream, "SHFE.au2602", 619.0);

    wait_until(|| sink_handle.stats().processed_commits() == 2).await;
    let report = sink_handle.shutdown().await.unwrap();
    assert_eq!(report.stats().journal_records(), 2);

    let records = StreamCommitJournal::new()
        .read_jsonl(&journal_path)
        .expect("commit journal should be readable");
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].revision(), 1);
    assert_eq!(records[1].revision(), 2);
    assert!(
        records[0]
            .paths()
            .iter()
            .any(|path| path == &["quotes".to_string(), "SHFE.au2602".to_string()])
    );

    let replay_sink = CountingSink::new();
    let replay_state = replay_sink.state();
    let replay_report = StreamCommitJournal::new()
        .replay_jsonl(&journal_path, replay_sink)
        .await
        .expect("commit journal should replay into a sink");

    assert_eq!(replay_report.replayed_commits(), 2);
    assert_eq!(replay_report.last_replayed_revision(), Some(2));
    assert_eq!(
        *replay_state
            .revisions
            .lock()
            .expect("test sink revisions mutex poisoned"),
        vec![1, 2]
    );

    let replay_sink = CountingSink::new();
    let replay_state = replay_sink.state();
    let replay_report = StreamCommitJournal::new()
        .after_revision(1)
        .replay_jsonl(&journal_path, replay_sink)
        .await
        .expect("commit journal should replay after a checkpoint");

    assert_eq!(replay_report.replayed_commits(), 1);
    assert_eq!(replay_report.last_replayed_revision(), Some(2));
    assert_eq!(
        *replay_state
            .revisions
            .lock()
            .expect("test sink revisions mutex poisoned"),
        vec![2]
    );

    let _ = std::fs::remove_file(&journal_path);
}

#[test]
fn stream_sink_options_expose_wal_fsync_policy() {
    let options = StreamSinkOptions::new().wal_fsync_policy(StreamSinkWalFsyncPolicy::EveryRecord);

    assert_eq!(
        options.fsync_policy(),
        StreamSinkWalFsyncPolicy::EveryRecord
    );
}

#[test]
fn stream_sink_profile_builds_reliable_jsonl_options() {
    let wal = std::env::temp_dir().join("profile-wal.jsonl");
    let journal = std::env::temp_dir().join("profile-journal.jsonl");

    let options = StreamSinkProfile::reliable_jsonl(wal.clone(), journal.clone())
        .retry_policy(StreamSinkRetryPolicy::limited(5).unwrap())
        .fsync_policy(StreamSinkWalFsyncPolicy::EveryRecord)
        .into_options();

    assert_eq!(options.wal_path(), Some(wal.as_path()));
    assert_eq!(options.commit_journal_path(), Some(journal.as_path()));
    assert_eq!(options.retry_policy_config().max_attempts(), 5);
    assert_eq!(
        options.fsync_policy(),
        StreamSinkWalFsyncPolicy::EveryRecord
    );
}

#[test]
fn stream_sink_wal_compaction_trims_old_revision_records() {
    let wal_path = temp_wal_path("stream-sink-compaction");
    let _ = std::fs::remove_file(&wal_path);
    write_wal_records(
        &wal_path,
        &[
            wal_record(StreamSinkWalRecordKind::Delivered, Some(1)),
            wal_record(StreamSinkWalRecordKind::Delivered, Some(2)),
            wal_record(StreamSinkWalRecordKind::FlushSucceeded, None),
            wal_record(StreamSinkWalRecordKind::Delivered, Some(3)),
        ],
    );

    let report = StreamSinkWalCompaction::new()
        .retain_revisions_from(2)
        .retain_non_revision_records(false)
        .compact_jsonl(&wal_path)
        .unwrap();

    assert_eq!(report.original_records(), 4);
    assert_eq!(report.retained_records(), 2);
    assert_eq!(report.dropped_records(), 2);

    let records = read_wal_records(&wal_path);
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["revision"], 2);
    assert_eq!(records[1]["revision"], 3);

    let _ = std::fs::remove_file(&wal_path);
}

#[test]
fn stream_sink_wal_recovery_reports_delivered_pending_and_failed_revisions() {
    let wal_path = temp_wal_path("stream-sink-recovery");
    let _ = std::fs::remove_file(&wal_path);
    write_wal_records(
        &wal_path,
        &[
            wal_record(StreamSinkWalRecordKind::Received, Some(1)),
            wal_record(StreamSinkWalRecordKind::Delivered, Some(1)),
            wal_record(StreamSinkWalRecordKind::Received, Some(2)),
            wal_record(StreamSinkWalRecordKind::AttemptFailed, Some(2)),
            wal_record(StreamSinkWalRecordKind::Lagged, None),
            wal_record(StreamSinkWalRecordKind::FlushFailed, None),
        ],
    );

    let report = StreamSinkWalRecovery::new().scan_jsonl(&wal_path).unwrap();

    assert_eq!(report.total_records(), 6);
    assert_eq!(report.delivered_revisions(), &[1]);
    assert_eq!(report.pending_revisions(), &[2]);
    assert_eq!(report.failed_revisions(), &[2]);
    assert_eq!(report.last_delivered_revision(), Some(1));
    assert_eq!(report.lagged_records(), 1);
    assert_eq!(report.flush_failed_records(), 1);
    assert!(report.has_incomplete_deliveries());

    let _ = std::fs::remove_file(&wal_path);
}

#[tokio::test(flavor = "current_thread")]
async fn graceful_shutdown_closes_driver_and_flushes_all_managed_sinks() {
    let stream = support::core_seed::seeded_stream();
    let first = CountingSink::new();
    let first_state = first.state();
    let second = CountingSink::new();
    let second_state = second.state();
    let first_handle = stream.spawn_commit_sink("warehouse", first).unwrap();
    let second_handle = stream.spawn_commit_sink("audit", second).unwrap();

    support::core_seed::seed_quote_commit(&stream, "SHFE.au2602", 618.0);

    wait_until(|| first_handle.stats().processed_commits() == 1).await;
    wait_until(|| second_handle.stats().processed_commits() == 1).await;

    let report = stream
        .graceful_shutdown()
        .sink(first_handle)
        .sink(second_handle)
        .shutdown()
        .await
        .unwrap();

    assert!(report.graceful());
    assert!(report.driver_closed());
    assert_eq!(report.outbound_flush_error(), None);
    assert_eq!(report.sink_reports().len(), 2);
    assert!(report.sink_errors().is_empty());
    assert!(
        report
            .sink_reports()
            .iter()
            .all(|sink| sink.flushed() && sink.stats().processed_commits() == 1)
    );
    assert_eq!(first_state.flushed.load(Ordering::Acquire), 1);
    assert_eq!(second_state.flushed.load(Ordering::Acquire), 1);
}

struct BlockingSink {
    state: Arc<BlockingSinkState>,
    blocker: Arc<tokio::sync::Notify>,
}

struct BlockingSinkState {
    started: AtomicUsize,
    revisions: Mutex<Vec<u64>>,
    flushed: AtomicUsize,
}

impl BlockingSink {
    fn new(blocker: Arc<tokio::sync::Notify>) -> Self {
        Self {
            state: Arc::new(BlockingSinkState {
                started: AtomicUsize::new(0),
                revisions: Mutex::new(Vec::new()),
                flushed: AtomicUsize::new(0),
            }),
            blocker,
        }
    }

    fn state(&self) -> Arc<BlockingSinkState> {
        Arc::clone(&self.state)
    }
}

impl CommitSink for BlockingSink {
    fn handle_commit(&mut self, commit: tqsdk_core::CommitResult) -> StreamSinkFuture {
        let state = Arc::clone(&self.state);
        let blocker = Arc::clone(&self.blocker);
        Box::pin(async move {
            state.started.fetch_add(1, Ordering::AcqRel);
            blocker.notified().await;
            state
                .revisions
                .lock()
                .expect("test sink revisions mutex poisoned")
                .push(commit.revision.get());
            Ok(())
        })
    }

    fn flush(&mut self) -> StreamSinkFuture {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            state.flushed.fetch_add(1, Ordering::AcqRel);
            Ok(())
        })
    }
}

struct FlakySink {
    state: Arc<FlakySinkState>,
    failures_before_success: usize,
}

struct FlakySinkState {
    attempts: AtomicUsize,
}

impl FlakySink {
    fn new(failures_before_success: usize) -> Self {
        Self {
            state: Arc::new(FlakySinkState {
                attempts: AtomicUsize::new(0),
            }),
            failures_before_success,
        }
    }

    fn state(&self) -> Arc<FlakySinkState> {
        Arc::clone(&self.state)
    }
}

impl CommitSink for FlakySink {
    fn handle_commit(&mut self, _commit: tqsdk_core::CommitResult) -> StreamSinkFuture {
        let attempt = self.state.attempts.fetch_add(1, Ordering::AcqRel) + 1;
        let should_fail = attempt <= self.failures_before_success;
        Box::pin(async move {
            if should_fail {
                Err(StreamFacadeError::InvalidState("transient sink failure"))
            } else {
                Ok(())
            }
        })
    }
}

struct CountingSink {
    state: Arc<CountingSinkState>,
}

struct CountingSinkState {
    revisions: Mutex<Vec<u64>>,
    flushed: AtomicUsize,
}

impl CountingSink {
    fn new() -> Self {
        Self {
            state: Arc::new(CountingSinkState {
                revisions: Mutex::new(Vec::new()),
                flushed: AtomicUsize::new(0),
            }),
        }
    }

    fn state(&self) -> Arc<CountingSinkState> {
        Arc::clone(&self.state)
    }
}

impl CommitSink for CountingSink {
    fn handle_commit(&mut self, commit: tqsdk_core::CommitResult) -> StreamSinkFuture {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            state
                .revisions
                .lock()
                .expect("test sink revisions mutex poisoned")
                .push(commit.revision.get());
            Ok(())
        })
    }

    fn flush(&mut self) -> StreamSinkFuture {
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            state.flushed.fetch_add(1, Ordering::AcqRel);
            Ok(())
        })
    }
}

async fn wait_until(mut condition: impl FnMut() -> bool) {
    for _ in 0..50 {
        if condition() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    assert!(condition(), "condition did not become true before timeout");
}

fn temp_wal_path(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "{name}-{}-{}.jsonl",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock should be after unix epoch")
            .as_nanos()
    ))
}

fn wal_record(kind: StreamSinkWalRecordKind, revision: Option<u64>) -> StreamSinkWalRecord {
    StreamSinkWalRecord {
        sink: "warehouse".to_string(),
        kind,
        revision,
        attempt: 1,
        scope: Some("realtime_update".to_string()),
        domains: vec!["market".to_string()],
        paths: vec!["quotes/SHFE.au2602".to_string()],
        error: None,
    }
}

fn write_wal_records(path: &Path, records: &[StreamSinkWalRecord]) {
    let mut file = std::fs::File::create(path).expect("test wal should be created");
    for record in records {
        serde_json::to_writer(&mut file, record).expect("test wal record should serialize");
        use std::io::Write;
        file.write_all(b"\n")
            .expect("test wal record should be written");
    }
}

fn read_wal_records(path: &Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .expect("wal file should be readable")
        .lines()
        .map(|line| serde_json::from_str(line).expect("wal line should be valid json"))
        .collect()
}
