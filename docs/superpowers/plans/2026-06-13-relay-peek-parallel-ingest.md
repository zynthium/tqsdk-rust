# Relay Peek and Parallel Ingest Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce `tqsdk-relay` upstream market diff receive latency by ensuring `peek_message` is never blocked by local JSON decode, then add measured async/parallel ingest improvements only where p95/p99 evidence justifies them.

**Architecture:** Keep relay as an optional market-only service; do not change SDK runtime, trade/query/auth routing, or dashboard read-only boundaries. The first batch moves `peek_message` earlier in the upstream IO path. Later batches split raw frame IO from decode and optionally shard per-symbol ingest so different contracts can run in parallel while preserving same-symbol ordering.

**Tech Stack:** Rust edition 2024, Tokio, existing `tqsdk-relay` websocket transport through `yawc`, `serde_json`, existing relay `MetricsSnapshot` / `HealthSnapshot`, GitNexus impact checks before symbol edits.

---

## Inputs

- Current upstream receive path in `crates/tqsdk-relay/src/upstream.rs`.
- Current server pump path in `crates/tqsdk-relay/src/server.rs`.
- Current relay engine and interest registry in `crates/tqsdk-relay/src/engine.rs` and `crates/tqsdk-relay/src/interest.rs`.
- Current observability surface in `crates/tqsdk-relay/src/observability.rs`.
- Current tests in `crates/tqsdk-relay/tests/upstream.rs`, `tests/server_ws.rs`, `tests/observability.rs`, and `tests/integration.rs`.

## Execution Status

- 2026-06-13: Batches 1-3 have been implemented in scoped commits:
  - `808794f perf(relay): send upstream peek before decode`
  - `07cfdb9 feat(relay): expose upstream peek and decode timing`
  - `e7fc135 test(relay): guard upstream peek and 200-symbol decode path`
- Batch 4 and later remain gated on the newly exposed `last_upstream_peek_delay_ms` and `last_upstream_decode_ms` evidence. Do not start IO/decode task splitting or symbol-sharded ingest unless those metrics show decode/ingest p95/p99 is the current bottleneck.

Accepted findings:

- `peek_message` is currently sent after a raw upstream frame is parsed and decoded into events.
- Downstream fan-out does not block `peek_message`; decode and event extraction are the head-of-line risk.
- Different symbols can be decoded/ingested in parallel if same-symbol event order is preserved.
- K-line synthesis can be parallelized per `(symbol, duration/source)` if each source remains single-owner ordered.
- SIMD, memory alignment, and payload zero-copy are plausible later optimizations, but they need latency/allocation evidence before adding dependencies or more complex data layouts.

Rejected for the first implementation pass:

- Do not introduce unordered multi-worker processing for the same symbol.
- Do not split the relay into multiple upstream websocket connections unless metrics show one connection is the limiting factor.
- Do not replace `serde_json` or payload `Value` cloning before adding decode/fan-out metrics.
- Do not change public SDK runtime semantics; this plan is only for `tqsdk-relay`.

---

## File Structure

- Modify `crates/tqsdk-relay/src/upstream.rs`
  - Move `send_peek_message()` before JSON decode for each raw text/binary/ping/pong frame.
  - Add decode/peek timing fields to `UpstreamSourceProgress`.
  - Later batch: introduce a raw frame queue or raw/decode split helper behind existing `UpstreamTickSource`.

- Modify `crates/tqsdk-relay/src/server.rs`
  - Keep downstream dispatch non-blocking.
  - Later batch: wire decoded-event batches from the async decode pipeline into the existing `RelayEngine`.

- Modify `crates/tqsdk-relay/src/engine.rs`
  - Record new upstream timing metrics.
  - Later batch: isolate per-symbol ingest state behind shard-friendly APIs if metrics justify sharding.

- Modify `crates/tqsdk-relay/src/observability.rs`
  - Add `last_upstream_peek_delay_ms`, `last_upstream_decode_ms`, and optional queue-depth metrics to `HealthSnapshot` / `MetricsSnapshot`.

- Modify `crates/tqsdk-relay/src/interest.rs`
  - No first-batch changes.
  - Later batch: evaluate `HashMap`/`HashSet` replacement only under a benchmark guard.

- Modify tests:
  - `crates/tqsdk-relay/tests/upstream.rs`
  - `crates/tqsdk-relay/tests/observability.rs`
  - `crates/tqsdk-relay/tests/server_ws.rs`
  - Add `crates/tqsdk-relay/tests/performance_guards.rs` if source-level guards become clearer than behavior tests.

- Modify docs after implementation:
  - `crates/tqsdk-relay/README.md`
  - `docs/architecture/validation.md`

---

## Hard Gates

- Before editing any function, run GitNexus impact with repo name:

```bash
gitnexus impact <symbol> --direction upstream --repo tqsdk-rust
```

- Before every commit:

```bash
gitnexus detect-changes --scope staged --repo tqsdk-rust
git diff --cached --check
```

- Standard focused validation:

```bash
cargo test -p tqsdk-relay --test upstream
cargo test -p tqsdk-relay --test observability
cargo test -p tqsdk-relay --test server_ws
cargo test -p tqsdk-relay --tests
cargo clippy -p tqsdk-relay --tests -- -D warnings
cargo fmt -p tqsdk-relay -- --check
git diff --check
```

---

## Batch 1: Send Peek Before Decode

### Task 1.1: Lock the behavior with an invalid-json test

**Files:**

- Modify `crates/tqsdk-relay/tests/upstream.rs`
- Modify `crates/tqsdk-relay/src/upstream.rs`

Required pre-edit analysis:

```bash
gitnexus impact recv_events --direction upstream --repo tqsdk-rust
gitnexus impact send_peek_message --direction upstream --repo tqsdk-rust
```

- [ ] **Step 1: Add a failing behavior test**

Add this test to `crates/tqsdk-relay/tests/upstream.rs` near the existing peek tests:

```rust
#[tokio::test]
async fn websocket_upstream_tick_source_peeks_before_json_decode() {
    use tqsdk_relay::{UpstreamTickChart, UpstreamTickSource, WebSocketUpstreamTickSource};
    use websocket_support::TestWebSocketServer;

    let server = TestWebSocketServer::spawn(|mut socket| {
        socket
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        expect_set_chart(&mut socket, "SHFE.au2602");
        expect_peek_message(&mut socket);

        socket.send_text("{not-json".to_string()).unwrap();
        expect_peek_message(&mut socket);
    });

    let chart = UpstreamTickChart::new("relay-upstream-tick-SHFE_au2602-1", ["SHFE.au2602"], 1)
        .unwrap();
    let mut source = WebSocketUpstreamTickSource::connect_with_tick_chart(server.url(), chart)
        .await
        .unwrap();

    assert!(source.next_update().await.is_none());
    server.join();
}
```

- [ ] **Step 2: Verify the test fails**

```bash
cargo test -p tqsdk-relay --test upstream websocket_upstream_tick_source_peeks_before_json_decode
```

Expected: FAIL because invalid JSON currently exits before sending the post-frame `peek_message`.

- [ ] **Step 3: Move peek before JSON decode**

In `crates/tqsdk-relay/src/upstream.rs`, change `recv_events()` text and binary branches to this shape:

```rust
Ok(RawFrame::Text(text)) => {
    self.send_peek_message().await?;
    let value = serde_json::from_str::<Value>(&text).map_err(|err| {
        RelayError::invalid_protocol(format!("invalid upstream JSON frame: {err}"))
    })?;
    let report = self.decode_market_report(value)?;
    self.record_decode_report(&report);
    let events = report.into_events();
    self.record_frame_received(events.len());
    Ok(Some(events))
}
Ok(RawFrame::Binary(bytes)) => {
    self.send_peek_message().await?;
    let value = serde_json::from_slice::<Value>(&bytes).map_err(|err| {
        RelayError::invalid_protocol(format!("invalid upstream JSON frame: {err}"))
    })?;
    let report = self.decode_market_report(value)?;
    self.record_decode_report(&report);
    let events = report.into_events();
    self.record_frame_received(events.len());
    Ok(Some(events))
}
Ok(RawFrame::Ping | RawFrame::Pong) => {
    self.record_frame_received(0);
    self.send_peek_message().await?;
    Ok(Some(Vec::new()))
}
```

Keep ping/pong behavior unchanged unless a test requires it.

- [ ] **Step 4: Run focused tests**

```bash
cargo test -p tqsdk-relay --test upstream websocket_upstream_tick_source_peeks_before_json_decode
cargo test -p tqsdk-relay --test upstream websocket_upstream_tick_source_peeks_after_each_received_frame
cargo test -p tqsdk-relay --test server_ws relay_configured_websocket_upstream_fans_out_to_downstream_client
```

Expected: PASS.

- [ ] **Step 5: Commit Batch 1**

```bash
git add crates/tqsdk-relay/src/upstream.rs crates/tqsdk-relay/tests/upstream.rs
gitnexus detect-changes --scope staged --repo tqsdk-rust
git diff --cached --check
git commit -m "perf(relay): send upstream peek before decode"
```

---

## Batch 2: Add Peek and Decode Timing Metrics

### Task 2.1: Expose timing in upstream progress and metrics snapshots

**Files:**

- Modify `crates/tqsdk-relay/src/upstream.rs`
- Modify `crates/tqsdk-relay/src/engine.rs`
- Modify `crates/tqsdk-relay/src/observability.rs`
- Modify `crates/tqsdk-relay/tests/observability.rs`
- Modify `crates/tqsdk-relay/tests/upstream.rs`

Required pre-edit analysis:

```bash
gitnexus impact UpstreamSourceProgress --direction upstream --repo tqsdk-rust
gitnexus impact record_upstream_progress --direction upstream --repo tqsdk-rust
gitnexus impact metrics_snapshot_at --direction upstream --repo tqsdk-rust
gitnexus impact health_snapshot_at --direction upstream --repo tqsdk-rust
```

- [ ] **Step 1: Extend progress fields**

Add fields to `UpstreamSourceProgress`:

```rust
pub last_peek_delay_ms: Option<u64>,
pub last_decode_ms: Option<u64>,
```

Update `is_empty()` so timing-only progress is not dropped:

```rust
&& self.last_peek_delay_ms.is_none()
&& self.last_decode_ms.is_none()
```

- [ ] **Step 2: Measure timing in `recv_events()`**

Use `std::time::Instant` inside each raw frame branch:

```rust
let frame_received_at = Instant::now();
self.send_peek_message().await?;
let peek_delay_ms = millis_u64(frame_received_at.elapsed());
let decode_started_at = Instant::now();
let value = serde_json::from_str::<Value>(&text).map_err(|err| {
    RelayError::invalid_protocol(format!("invalid upstream JSON frame: {err}"))
})?;
let report = self.decode_market_report(value)?;
let decode_ms = millis_u64(decode_started_at.elapsed());
self.record_frame_received(events.len(), Some(peek_delay_ms), Some(decode_ms));
```

Add a small helper:

```rust
fn millis_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}
```

- [ ] **Step 3: Store the latest timing metrics in `RelayEngine`**

Add private fields:

```rust
last_upstream_peek_delay_ms: Option<u64>,
last_upstream_decode_ms: Option<u64>,
```

In `record_upstream_progress`, copy non-`None` progress values into these fields.

- [ ] **Step 4: Expose metrics**

Add to `HealthSnapshot` and `MetricsSnapshot`:

```rust
pub last_upstream_peek_delay_ms: Option<u64>,
pub last_upstream_decode_ms: Option<u64>,
```

Populate both from `RelayEngine`.

- [ ] **Step 5: Add tests**

In `tests/upstream.rs`, assert progress includes timing after a valid frame:

```rust
let progress = source.take_progress();
assert_eq!(progress.frames_received, 1);
assert!(progress.last_peek_delay_ms.is_some());
assert!(progress.last_decode_ms.is_some());
```

In `tests/observability.rs`, assert metrics snapshots carry the values after calling `record_upstream_progress` with explicit timing:

```rust
engine.record_upstream_progress(UpstreamSourceProgress {
    frames_received: 1,
    events_decoded: 2,
    unix_secs: 1_700_000_002,
    last_peek_delay_ms: Some(1),
    last_decode_ms: Some(7),
    ..Default::default()
});
let metrics = engine.metrics_snapshot_at(1_700_000_003);
assert_eq!(metrics.last_upstream_peek_delay_ms, Some(1));
assert_eq!(metrics.last_upstream_decode_ms, Some(7));
```

- [ ] **Step 6: Run focused tests and commit**

```bash
cargo test -p tqsdk-relay --test upstream
cargo test -p tqsdk-relay --test observability
git add crates/tqsdk-relay/src/upstream.rs crates/tqsdk-relay/src/engine.rs crates/tqsdk-relay/src/observability.rs crates/tqsdk-relay/tests/upstream.rs crates/tqsdk-relay/tests/observability.rs
gitnexus detect-changes --scope staged --repo tqsdk-rust
git diff --cached --check
git commit -m "feat(relay): expose upstream peek and decode timing"
```

---

## Batch 3: Add a 200-Symbol Receive Microbenchmark

### Task 3.1: Create deterministic benchmark-style tests before larger architecture changes

**Files:**

- Create `crates/tqsdk-relay/tests/performance_guards.rs`
- Modify `crates/tqsdk-relay/Cargo.toml` only if a dev dependency is truly needed; prefer no new dependency.

- [ ] **Step 1: Add a source-level guard for immediate peek**

Create `tests/performance_guards.rs`:

```rust
#[test]
fn upstream_recv_events_sends_peek_before_json_decode() {
    let source = include_str!("../src/upstream.rs");
    let start = source.find("async fn recv_events").expect("recv_events exists");
    let end = source[start..]
        .find("fn record_decode_report")
        .map(|offset| start + offset)
        .expect("record_decode_report follows recv_events");
    let body = &source[start..end];
    let peek = body.find("self.send_peek_message().await?").expect("peek is sent");
    let parse = body.find("serde_json::from_str").expect("text JSON parse exists");
    assert!(peek < parse, "peek_message must be sent before text JSON decode");
}
```

- [ ] **Step 2: Add a synthetic 200-symbol decode guard**

Add a test that builds one `rtn_data` frame with 200 symbols and one row per symbol, then calls `decode_upstream_market_report`:

```rust
#[test]
fn decode_upstream_market_report_handles_200_symbols() {
    let mut ticks = serde_json::Map::new();
    for idx in 0..200 {
        let symbol = format!("TEST.s{idx:03}");
        ticks.insert(
            symbol,
            serde_json::json!({
                "data": {
                    "1": {
                        "datetime": 1_780_000_000_000_000_000_i64,
                        "last_price": 10.0 + idx as f64,
                        "volume": idx as i64,
                        "open_interest": 1000 + idx as i64
                    }
                }
            }),
        );
    }
    let frame = serde_json::json!({
        "aid": "rtn_data",
        "data": [{ "ticks": ticks }]
    });

    let report = tqsdk_relay::decode_upstream_market_report(frame).unwrap();
    assert_eq!(report.ticks().len(), 200);
    assert_eq!(report.invalid_rows(), 0);
}
```

- [ ] **Step 3: Run guards**

```bash
cargo test -p tqsdk-relay --test performance_guards
```

Expected: PASS.

- [ ] **Step 4: Commit Batch 3**

```bash
git add crates/tqsdk-relay/tests/performance_guards.rs
gitnexus detect-changes --scope staged --repo tqsdk-rust
git diff --cached --check
git commit -m "test(relay): guard upstream peek and 200-symbol decode path"
```

---

## Batch 4: Split IO From Decode With a Bounded Raw Frame Queue

### Task 4.1: Introduce an async IO/decode pipeline without symbol parallelism yet

**Files:**

- Modify `crates/tqsdk-relay/src/upstream.rs`
- Modify `crates/tqsdk-relay/src/config.rs`
- Modify `crates/tqsdk-relay/src/observability.rs`
- Modify `crates/tqsdk-relay/src/engine.rs`
- Modify `crates/tqsdk-relay/tests/config.rs`
- Modify `crates/tqsdk-relay/tests/upstream.rs`
- Modify `crates/tqsdk-relay/tests/observability.rs`

Required pre-edit analysis:

```bash
gitnexus impact WebSocketUpstreamTickSource --direction upstream --repo tqsdk-rust
gitnexus impact RelayConfig --direction upstream --repo tqsdk-rust
```

- [ ] **Step 1: Add queue capacity configuration**

Add to config:

```rust
pub raw_frame_queue_capacity: usize,
```

Default:

```rust
raw_frame_queue_capacity: 1024,
```

Environment variable:

```rust
const ENV_RAW_FRAME_QUEUE_CAPACITY: &str = "TQSDK_RELAY_RAW_FRAME_QUEUE_CAPACITY";
```

Validation: reject `0`.

- [ ] **Step 2: Refactor raw frame handling behind a small enum**

In `upstream.rs`:

```rust
enum UpstreamRawPayload {
    Text(String),
    Binary(Vec<u8>),
    Control,
}
```

The IO side must do:

```rust
let raw = self.recv_raw_payload().await?;
self.send_peek_message().await?;
self.enqueue_raw_payload(raw).await?;
```

The decode side must do:

```rust
let raw = self.raw_rx.recv().await?;
let report = decode_raw_payload(raw, &mut self.tick_row_cache)?;
```

Keep output type as `Vec<UpstreamMarketEvent>` so `UpstreamTickSource` callers do not change.

- [ ] **Step 3: Preserve same behavior under the existing public source**

`WebSocketUpstreamTickSource::next_update()` should continue returning one buffered event at a time. The only behavior change is that raw IO can send peek and enqueue before decode consumes the frame.

- [ ] **Step 4: Add queue metrics**

Expose:

```rust
pub upstream_raw_queue_depth: Option<usize>,
pub upstream_raw_queue_capacity: Option<usize>,
```

These can be `None` for fake sources and `Some(...)` for websocket sources.

- [ ] **Step 5: Test queue behavior**

Add tests that:

- one valid frame still decodes to one event;
- invalid JSON still sends peek before decode failure;
- a tiny queue capacity rejects `0`;
- metrics expose queue capacity/depth after a websocket source is active.

- [ ] **Step 6: Run and commit**

```bash
cargo test -p tqsdk-relay --test upstream
cargo test -p tqsdk-relay --test config
cargo test -p tqsdk-relay --test observability
git add crates/tqsdk-relay/src/upstream.rs crates/tqsdk-relay/src/config.rs crates/tqsdk-relay/src/engine.rs crates/tqsdk-relay/src/observability.rs crates/tqsdk-relay/tests/upstream.rs crates/tqsdk-relay/tests/config.rs crates/tqsdk-relay/tests/observability.rs
gitnexus detect-changes --scope staged --repo tqsdk-rust
git diff --cached --check
git commit -m "perf(relay): decouple upstream io from decode"
```

---

## Batch 5: Add Symbol-Sharded Ingest Behind a Feature Gate or Config Gate

### Task 5.1: Preserve per-symbol ordering while parallelizing different symbols

**Files:**

- Modify `crates/tqsdk-relay/src/engine.rs`
- Modify `crates/tqsdk-relay/src/server.rs`
- Create `crates/tqsdk-relay/src/sharded_ingest.rs`
- Modify `crates/tqsdk-relay/src/lib.rs`
- Modify `crates/tqsdk-relay/tests/integration.rs`
- Modify `crates/tqsdk-relay/tests/server_ws.rs`

Required pre-edit analysis:

```bash
gitnexus impact ingest_tick --direction upstream --repo tqsdk-rust
gitnexus impact ingest_quote --direction upstream --repo tqsdk-rust
gitnexus impact pump_upstream_until --direction upstream --repo tqsdk-rust
gitnexus impact kline_frames --direction upstream --repo tqsdk-rust
```

- [ ] **Step 1: Add shard owner type**

Create `src/sharded_ingest.rs`:

```rust
pub struct SymbolShardRouter {
    shard_count: usize,
}

impl SymbolShardRouter {
    pub fn new(shard_count: usize) -> Self {
        assert!(shard_count > 0, "shard_count must be greater than zero");
        Self { shard_count }
    }

    pub fn shard_for_symbol(&self, symbol: &str) -> usize {
        let mut hash = 0xcbf29ce484222325_u64;
        for byte in symbol.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        usize::try_from(hash % self.shard_count as u64).unwrap_or(0)
    }
}
```

- [ ] **Step 2: Add routing tests**

```rust
#[test]
fn router_sends_same_symbol_to_same_shard() {
    let router = SymbolShardRouter::new(8);
    assert_eq!(
        router.shard_for_symbol("SHFE.au2602"),
        router.shard_for_symbol("SHFE.au2602")
    );
}
```

- [ ] **Step 3: Split event batches by shard**

Process all events from a decoded frame into `Vec<Vec<UpstreamMarketEvent>>` by `symbol`. Same-symbol events remain in their original order because they are appended to the same shard vector in input order.

- [ ] **Step 4: Keep first implementation conservative**

Use `tokio::task::JoinSet` or `spawn_blocking` only after measuring Batch 2/3. The initial implementation may keep shard grouping but still call existing `RelayEngine` serially. Commit this as a structural preparation only if it simplifies review.

- [ ] **Step 5: Move per-symbol mutable state into shard owners**

Only after tests prove grouping is stable, move these state groups into shard-owned structs:

```rust
struct SymbolShardState {
    cache: MarketCache,
    klines: HashMap<SourceKey, KlineSynthesis>,
    symbol_metrics: SymbolMetricsStore,
}
```

Keep `InterestRegistry` global until there is evidence it contends.

- [ ] **Step 6: Add order-preservation tests**

Add tests for:

- two ticks for the same symbol emit in input order;
- two symbols can be routed to different shards;
- completed K-line payload for a `(symbol, duration)` is identical to the existing serial engine.

- [ ] **Step 7: Commit only if behavior is fully equivalent**

```bash
cargo test -p tqsdk-relay --test integration
cargo test -p tqsdk-relay --test server_ws
cargo test -p tqsdk-relay --tests
git add crates/tqsdk-relay/src/sharded_ingest.rs crates/tqsdk-relay/src/lib.rs crates/tqsdk-relay/src/engine.rs crates/tqsdk-relay/src/server.rs crates/tqsdk-relay/tests/integration.rs crates/tqsdk-relay/tests/server_ws.rs
gitnexus detect-changes --scope staged --repo tqsdk-rust
git diff --cached --check
git commit -m "perf(relay): prepare symbol-sharded ingest"
```

---

## Batch 6: Evaluate Low-Level Optimizations Only With Evidence

### Task 6.1: Decide whether SIMD JSON, pre-serialized payloads, or collection swaps are worth it

**Files:**

- Modify only after Batch 2/3 metrics show the bottleneck:
  - `crates/tqsdk-relay/Cargo.toml`
  - `crates/tqsdk-relay/src/upstream.rs`
  - `crates/tqsdk-relay/src/engine.rs`
  - `crates/tqsdk-relay/src/interest.rs`
  - `crates/tqsdk-relay/src/server.rs`
  - `crates/tqsdk-relay/tests/performance_guards.rs`

- [ ] **Step 1: If decode p99 dominates, test `simd-json` behind an internal helper**

Do not replace all parsing at once. Add helper:

```rust
fn parse_upstream_json_text(text: &str) -> RelayResult<Value> {
    serde_json::from_str::<Value>(text)
        .map_err(|err| RelayError::invalid_protocol(format!("invalid upstream JSON frame: {err}")))
}
```

Then benchmark swapping only this helper. Adopt `simd-json` only if it improves measured decode p95/p99 and does not force unsafe code into relay.

- [ ] **Step 2: If fan-out p99 dominates, pre-serialize downstream frames**

Change `DownstreamFrame` from:

```rust
pub payload: Value,
```

to a new internal payload enum:

```rust
pub enum DownstreamPayload {
    Json(Value),
    Text(String),
}
```

Only switch to `Text(String)` when multiple clients share the same payload and tests show less clone/serialization cost.

- [ ] **Step 3: If interest lookup dominates, benchmark `HashMap`/`HashSet`**

Do not replace `BTreeMap/BTreeSet` blindly. Add tests that assert deterministic output order where it matters. If deterministic order is user-visible in tests, sort at the boundary after using hash maps internally.

- [ ] **Step 4: Defer memory alignment unless profiles show CPU cache stalls**

Do not add `repr(align)` to `RelayTickRow` or cache structs unless a profiler shows cacheline contention or false sharing. The current tick row is small, and alignment changes can increase memory footprint.

- [ ] **Step 5: Commit chosen low-level optimization separately**

Use one commit per optimization:

```bash
git add <focused files>
gitnexus detect-changes --scope staged --repo tqsdk-rust
git diff --cached --check
git commit -m "perf(relay): <specific measured optimization>"
```

---

## Final Verification

Run after the last implemented batch:

```bash
cargo test -p tqsdk-relay --tests
cargo clippy -p tqsdk-relay --tests -- -D warnings
cargo fmt -p tqsdk-relay -- --check
git diff --check
gitnexus detect-changes --scope all --repo tqsdk-rust
```

If dashboard fields change, also run:

```bash
cd crates/tqsdk-relay/dashboard-ui
pnpm run check
pnpm run test
pnpm run build
```

---

## Completion Criteria

- `peek_message` is sent before JSON decode for each received upstream raw frame.
- Metrics expose enough information to distinguish upstream silence, local decode delay, raw queue backlog, and downstream fan-out delay.
- Same-symbol ordering remains deterministic.
- Different-symbol ingest and K-line synthesis are parallelized only after tests prove serial equivalence.
- Any SIMD, zero-copy, or collection-layout optimization is backed by measured p95/p99 evidence and lands in a separate commit.
