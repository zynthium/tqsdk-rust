use futures::StreamExt;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tqsdk_core::Quote;
use tqsdk_stream::{CommitSink, StreamFacadeError, StreamSinkFuture, StreamSinkStatus};

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

async fn wait_until(mut condition: impl FnMut() -> bool) {
    for _ in 0..50 {
        if condition() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    assert!(condition(), "condition did not become true before timeout");
}
