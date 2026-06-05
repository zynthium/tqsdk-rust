use std::time::{Duration, Instant};

use tqsdk_relay::{BootstrapQueue, BootstrapRequest, SourceKey};

fn request(symbol: &str, duration_ns: i64, start_id: i64, end_id: i64) -> BootstrapRequest {
    BootstrapRequest {
        source: SourceKey {
            symbols: vec![symbol.to_string()],
            duration_ns,
            view_width: 64,
        },
        start_id,
        end_id,
    }
}

#[test]
fn queue_coalesces_overlapping_requests() {
    let mut queue = BootstrapQueue::new(2, Duration::from_millis(100));

    queue.enqueue(request("SHFE.au2602", 60_000_000_000, 10, 20));
    queue.enqueue(request("SHFE.au2602", 60_000_000_000, 15, 30));

    assert_eq!(queue.len(), 1);
    let next = queue.poll_ready(Instant::now()).unwrap();
    assert_eq!(next.start_id, 10);
    assert_eq!(next.end_id, 30);
}

#[test]
fn queue_respects_concurrency_limit() {
    let mut queue = BootstrapQueue::new(1, Duration::from_millis(0));
    let now = Instant::now();

    queue.enqueue(request("SHFE.au2602", 60_000_000_000, 10, 20));
    queue.enqueue(request("DCE.m2609", 60_000_000_000, 10, 20));

    assert!(queue.poll_ready(now).is_some());
    assert_eq!(queue.inflight(), 1);
    assert!(queue.poll_ready(now).is_none());
    queue.complete_one();
    assert_eq!(queue.inflight(), 0);
    assert!(queue.poll_ready(now).is_some());
}

#[test]
fn queue_respects_min_request_interval() {
    let mut queue = BootstrapQueue::new(2, Duration::from_millis(100));
    let now = Instant::now();

    queue.enqueue(request("SHFE.au2602", 60_000_000_000, 10, 20));
    queue.enqueue(request("DCE.m2609", 60_000_000_000, 10, 20));

    assert!(queue.poll_ready(now).is_some());
    queue.complete_one();
    assert!(queue.poll_ready(now + Duration::from_millis(50)).is_none());
    assert!(queue.poll_ready(now + Duration::from_millis(100)).is_some());
}

#[test]
fn complete_one_saturates_when_nothing_is_inflight() {
    let mut queue = BootstrapQueue::new(1, Duration::from_millis(0));

    queue.complete_one();

    assert_eq!(queue.inflight(), 0);
    assert!(queue.is_empty());
}

#[test]
#[should_panic(expected = "max_inflight must be greater than zero")]
fn queue_rejects_zero_concurrency_limit() {
    let _ = BootstrapQueue::new(0, Duration::from_millis(0));
}
