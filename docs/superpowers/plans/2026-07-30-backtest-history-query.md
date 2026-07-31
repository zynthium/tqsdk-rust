# Backtest History Query Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 tqsdk-data 提供面向回测缓存的异步区间查询：Tick 与 60s K 线按原始缓存读取，15s 等小于 60s 周期从 Tick 聚合，5m/15m/30m/60m 等周期从 canonical 60s 聚合，并在缓存缺口时复用官方 server-side backtest 数据源。

**Architecture:** tqsdk-data 拥有查询规划、metadata sidecar、缓存补数、异步 chunk stream、single-flight 与共享聚合内核；tqsdk-session 提供不依赖 tqsdk-wait 的 server-backtest 历史流 substrate；tqsdk-task 复用并兼容导出同一聚合内核，tqsdk facade 的回测准备过程委托给 tqsdk-data。存储主线先保持同步文件格式与 reader contract，通过有限 blocking scan workers 接入异步链路；native-async reader 只在基准门槛通过后迁移。

**Tech Stack:** Rust 2024（MSRV 1.85）、Tokio、futures 0.3、serde/serde_json、SHA-1 snapshot hash、fs2 advisory locks、现有 TQBN daily v2 Tick cache、现有 canonical minute monthly v4 cache、官方 DIFF set_chart/server-backtest stream。

---

## Scope Check

本计划是一个端到端但不可拆开的能力切片：查询 API、聚合内核、metadata、远端补数和 backtest 复用共同决定同一份数据的语义。任何一层单独落地都会产生第二套来源或第二套聚合规则，因此保留为一个计划、按可验证提交分阶段实施。

本轮交付：

- 独立 BacktestHistoryClient；不扩展专业历史下载 DataClient。
- Tick、Kline、单 request、batch request。
- 主入口为异步 chunk stream；单 request 提供 collect()，batch 提供显式内存上限的 collect_all(max_bytes)。
- 默认 RemoteOnMiss，可选 CacheOnly；完整缓存命中不联网、不读取认证。
- 认证只通过 auth_env() 或 BacktestHistoryAuthProvider 显式配置，并在真正远端补数时惰性加载。
- 小于 60s 的 K 线从 Tick 聚合；60s 直接读取 MinuteKlineCache；大于 60s 只接受 N × 60s 并从已关闭 60s 聚合。
- 15s 必须是正式支持和验收周期；61s、90s 等拒绝。
- 只持久化 Tick 与 canonical 60s；派生周期只存在于单次查询或回测内存。
- 同 symbol/range 的多个派生周期共享一次 Tick 或 minute scan 并 fan-out。
- Tick 与 minute cache 都不做自动 retention、容量淘汰或 TTL 清理。
- KQ.i、KQ.m、具体期货合约都以逻辑 symbol 接受查询；KQ.m 映射与 session/calendar snapshot 持久化，CacheOnly 可真正离线。
- 用户可见链路全异步；底层 reader 先由最多 min(8, CPU) 个 blocking scan workers 承载。
- 默认逻辑并发 32，每个活跃 symbol 缓冲 16 MiB；不再叠加全局 buffer cap。
- 单 request collect 默认上限 512 MiB；batch collect_all 必须由调用方给出总上限。
- 同进程合并重叠补数；跨进程已有 fill owner 时异步等待并周期复查 coverage。
- 每个 request 独立完成或失败；一个失败不取消 batch 其他 request。
- chunk 在 RequestCompleted 前是 provisional delivery；失败报告包含已输出行数，collect 丢弃该 request 的部分结果。
- 当前交易日默认 final-only；显式 provisional_as_of 只允许 Tick 和小于 60s 的派生 K 线。
- 第一版用户查询只支持期货。

本轮明确不做：

- 不增加 tqsdk-cache query CLI。
- 不改变 TQBN daily v2 或 minute monthly v4 文件格式和分区方式。
- 不持久化 15s、5m、15m、30m、60m 等派生 K 线。
- 不把 BacktestHistoryClient 加入 tqsdk prelude。
- 不使用专业历史下载权限或 tq_dl 作为回测缓存缺口来源。
- 不让 tqsdk-data 依赖 tqsdk-wait。
- 不在查询 API 暴露 purge/refresh；显式 metadata 刷新属于独立 maintenance API。
- 不预设 KQ.m 换月聚合细节；真实 server-backtest K 线是 oracle。

## Locked Semantics

### Range 与 finality

- 所有用户区间都是纳秒半开区间 [start_ns, end_ns)。
- Tick 返回 datetime 位于该区间内的行。
- K 线按 bar-start 过滤：只返回 datetime 位于 [start_ns, end_ns) 的完整 bar；在 session window 结束处关闭的 truncated bar 也属于完整 bar。
- planner 可向前/向后扩展 source range，以覆盖完整 bucket、session 边界和 Tick 累计成交量基线；报告必须同时给出 requested_range 与 expanded_source_range。
- 60s 与更高周期只消费 canonical final minute。当前交易日即使指定 provisional_as_of 也不允许走 minute path。
- Tick 与小于 60s K 线只有显式 provisional_as_of_ns 时才可以读取 provisional coverage，报告 finality 为 Provisional { as_of_ns }。

### 聚合

- bar boundary 按 snapshot 中的交易日与 session window 对齐，不能跨午休、夜盘间隔或交易日。
- Tick cumulative volume 在新交易日从 0 建立基线；session break 不清零累计量。查询 mid-session 时底层 scan 扩展到该交易日累计量起点。
- open/high/low/close、volume、open_oi、close_oi 与官方 server-backtest K 线对齐。
- 派生 Kline.id 使用稳定且唯一的 synthetic id：bar_start_ns。它单调、不会在 session break 后碰撞，但不承诺与服务端序列 id 相等。
- 价格对照先按合约精度 canonicalize；datetime、volume、open_oi、close_oi 必须精确一致。

### 错误与并发

- 损坏、不兼容或 snapshot hash 不匹配的分区 fail closed；不得自动删除、覆盖或隔离。
- 分区读取持有该日/月的共享锁与固定文件句柄，消费完该分区立即释放。
- 已可读的 request 立即产出，不等待同 batch 中仍在补数的 request。
- 共享 fill 的消费者引用计数降为 0 时协作取消；已经落盘的 partial rows可保留，但不得提交 final coverage。
- 仍有任一消费者时共享 fill 继续。
- telemetry 使用独立 stream，阶段固定为 Inspect、WaitForFill、Fill、Retry、Read、Aggregate。

## Public API Contract

第一版对外名称在后续任务中保持一致：

~~~rust
use std::path::PathBuf;
use std::time::Duration;

use tqsdk_core::{Kline, Tick};

pub type BacktestHistoryRequestId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BacktestHistoryPolicy {
    CacheOnly,
    RemoteOnMiss,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BacktestHistoryKind {
    Tick,
    Kline { duration: Duration },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacktestHistoryRequest {
    request_id: BacktestHistoryRequestId,
    symbol: String,
    kind: BacktestHistoryKind,
    start_ns: i64,
    end_ns: i64,
    provisional_as_of_ns: Option<i64>,
}

impl BacktestHistoryRequest {
    pub fn tick(
        request_id: BacktestHistoryRequestId,
        symbol: impl Into<String>,
        start_ns: i64,
        end_ns: i64,
    ) -> Self;

    pub fn kline(
        request_id: BacktestHistoryRequestId,
        symbol: impl Into<String>,
        duration: Duration,
        start_ns: i64,
        end_ns: i64,
    ) -> Self;

    #[must_use]
    pub fn with_provisional_as_of_ns(self, as_of_ns: i64) -> Self;
}

#[derive(Debug, Clone)]
pub enum BacktestHistoryRows {
    Ticks(Vec<Tick>),
    Klines {
        duration_ns: i64,
        rows: Vec<Kline>,
    },
}

#[derive(Debug, Clone)]
pub enum BacktestHistoryEvent {
    Chunk(BacktestHistoryChunk),
    RequestCompleted(BacktestHistoryRequestReport),
    RequestFailed(BacktestHistoryRequestFailure),
}

pub struct BacktestHistoryRun;

impl BacktestHistoryRun {
    pub async fn next(&mut self) -> Option<BacktestHistoryEvent>;
    pub fn take_telemetry(&mut self) -> Option<BacktestHistoryTelemetryStream>;
    pub async fn finish(self) -> BacktestHistoryBatchReport;
    pub async fn collect(self) -> Result<BacktestHistoryCollected>;
    pub async fn collect_all(
        self,
        max_total_bytes: usize,
    ) -> Result<BacktestHistoryCollectedBatch>;
}

pub struct BacktestHistoryClient;

impl BacktestHistoryClient {
    pub fn builder(cache_dir: impl Into<PathBuf>) -> BacktestHistoryClientBuilder;
    pub async fn query(&self, request: BacktestHistoryRequest) -> Result<BacktestHistoryRun>;
    pub async fn query_batch(
        &self,
        requests: impl IntoIterator<Item = BacktestHistoryRequest>,
    ) -> Result<BacktestHistoryRun>;
}
~~~

BacktestHistoryRun 同时实现 futures::Stream<Item = BacktestHistoryEvent>。固有 next() 让普通用户无需额外导入 StreamExt；Stream 实现用于 select、组合与生态适配。

### Report contract

~~~rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BacktestHistoryFinality {
    Final,
    Provisional { as_of_ns: i64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacktestHistoryPhysicalSegment {
    pub physical_symbol: String,
    pub start_ns: i64,
    pub end_ns: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacktestHistoryCoverageReport {
    pub requested_range: (i64, i64),
    pub expanded_source_range: (i64, i64),
    pub cached_ranges: Vec<(i64, i64)>,
    pub remote_filled_ranges: Vec<(i64, i64)>,
    pub finality: BacktestHistoryFinality,
}

#[derive(Debug, Clone)]
pub struct BacktestHistoryRequestReport {
    pub request_id: BacktestHistoryRequestId,
    pub symbol: String,
    pub kind: BacktestHistoryKind,
    pub rows: usize,
    pub physical_segments: Vec<BacktestHistoryPhysicalSegment>,
    pub snapshot_hash: String,
    pub coverage: BacktestHistoryCoverageReport,
    pub remote_used: bool,
}

#[derive(Debug, Clone)]
pub struct BacktestHistoryRequestFailure {
    pub request_id: BacktestHistoryRequestId,
    pub symbol: String,
    pub error: String,
    pub emitted_rows: usize,
}

#[derive(Debug, Clone, Default)]
pub struct BacktestHistoryBatchReport {
    pub completed: Vec<BacktestHistoryRequestReport>,
    pub failed: Vec<BacktestHistoryRequestFailure>,
}

pub struct BacktestHistoryTelemetryStream;

impl BacktestHistoryTelemetryStream {
    pub async fn next(&mut self) -> Option<BacktestHistoryTelemetryEvent>;
}
~~~

### Builder defaults

~~~rust
pub struct BacktestHistoryClientBuilder {
    cache_dir: PathBuf,
    policy: BacktestHistoryPolicy,
    logical_concurrency: usize,
    blocking_workers: usize,
    per_symbol_buffer_bytes: usize,
    collect_limit_bytes: usize,
}

const DEFAULT_LOGICAL_CONCURRENCY: usize = 32;
const DEFAULT_PER_SYMBOL_BUFFER_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_COLLECT_LIMIT_BYTES: usize = 512 * 1024 * 1024;

fn default_blocking_workers() -> usize {
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
        .min(8)
        .max(1)
}
~~~

builder 默认 policy=RemoteOnMiss，并提供 policy(...)、auth_env()、auth_provider(...)、logical_concurrency(...)、blocking_workers(...)、per_symbol_buffer_bytes(...)、collect_limit_bytes(...)。任何 0 值都返回 DataError::Validation，不静默改写。

## Impact Analysis Gates

执行 Rust 修改前必须对目标 symbol 做 GitNexus upstream impact。至少包括：

- BacktestTickCache::load_series
- HistorySeriesCache::open_tick_data_series_reader
- MinuteKlineCache::open_reader
- MinuteKlineReader::next_kline
- KlineSynthesizer::update
- MinuteKlineAggregator::update
- HistoryBacktestReplayStream::new_projected
- SessionClientBuilder::build
- SessionClient::progress_once
- BacktestPump::handle_commit
- BacktestBuilder::prepare
- PreparedBacktest::connect

若结果为 HIGH 或 CRITICAL，先向用户报告直接调用方、受影响 execution flows 与风险，再编辑。修改 server-backtest/DIFF 处理时只使用 docs/diff_protocol_spec.md 的 set_chart（L373-L430）、charts/klines/ticks（L1241 起）和数据树（L256-L318）相关段落。

## File Structure

### 新建

- crates/tqsdk-data/src/aggregation/mod.rs：共享聚合公共入口与兼容常量。
- crates/tqsdk-data/src/aggregation/session.rs：snapshot-backed 交易日/session window 定位。
- crates/tqsdk-data/src/aggregation/tick.rs：TickKlineAggregator。
- crates/tqsdk-data/src/aggregation/minute.rs：MinuteKlineAggregator。
- crates/tqsdk-data/src/backtest_history/mod.rs：模块边界与 public re-export。
- crates/tqsdk-data/src/backtest_history/request.rs：request、policy、认证 provider、builder config。
- crates/tqsdk-data/src/backtest_history/report.rs：chunk、event、coverage、finality、collect/report。
- crates/tqsdk-data/src/backtest_history/metadata.rs：sidecar snapshot、hash、原子写、maintenance。
- crates/tqsdk-data/src/backtest_history/planner.rs：source policy、range expansion、physical segments、shared scan plan。
- crates/tqsdk-data/src/backtest_history/store_worker.rs：有限 blocking scan workers 与 chunk fan-out。
- crates/tqsdk-data/src/backtest_history/fill.rs：coverage inspection、single-flight、跨进程等待、远端写入。
- crates/tqsdk-data/src/backtest_history/executor.rs：batch scheduler、stream、collect、取消。
- crates/tqsdk-data/src/backtest_history/telemetry.rs：独立 telemetry stream。
- crates/tqsdk-session/src/backtest_history.rs：低层 server-backtest Tick/60s 流。
- crates/tqsdk-data/tests/backtest_history_api.rs：public contract 与 feature-independent local API。
- crates/tqsdk-data/tests/backtest_history_aggregation.rs：聚合一致性 fixtures。
- crates/tqsdk-data/tests/backtest_history_metadata.rs：sidecar 与离线映射。
- crates/tqsdk-data/tests/backtest_history_query.rs：planner、shared scan、错误/finality。
- crates/tqsdk-data/tests/backtest_history_async.rs：并发、single-flight、取消、内存限制。
- crates/tqsdk-data/tests/backtest_history_live.rs：ignored 官方 oracle 对照。
- crates/tqsdk-data/tests/support/backtest_history.rs：只被上述 integration tests 使用的 cache/session fixtures。
- crates/tqsdk-data/examples/api_contract_s48_backtest_history_query.rs：用户契约。
- crates/tqsdk-data/examples/backtest_history_query_bench.rs：cold/warm 与 1/32 路基准。
- docs/reviews/backtest-history-native-async-spike.md：记录基准、门槛与最终 reader 决策。

### 修改

- Cargo.toml：Tokio native-async spike 需要时增加 fs feature。
- crates/tqsdk-data/Cargo.toml：futures、serde、sha1；本地路径不依赖 default features。
- crates/tqsdk-data/src/lib.rs：导出 aggregation 与 backtest_history。
- crates/tqsdk-data/src/error.rs：feature-disabled、collect memory、request failure 错误。
- crates/tqsdk-data/src/history_series_cache.rs 与 history_series_cache/tqbn/mod.rs：固定句柄共享读锁、chunk reader seam。
- crates/tqsdk-data/src/minute_kline_cache.rs：固定句柄共享读锁、chunk reader seam。
- crates/tqsdk-session/src/lib.rs、Cargo.toml、README.md：server-backtest history substrate。
- crates/tqsdk-wait/src/backtest.rs、builder.rs、api.rs：保留策略 pump，移除 cache-fill 专用复制逻辑。
- crates/tqsdk-task/src/kline_synth.rs：删除；由共享 TickKlineAggregator 替代私有实现。
- crates/tqsdk-task/src/minute_kline_aggregate.rs：改成 tqsdk-data 兼容 re-export。
- crates/tqsdk-task/src/history_backtest_replay.rs：消费共享聚合内核。
- crates/tqsdk-task/src/lib.rs 与相关 tests/README.md：兼容导出与契约。
- crates/tqsdk/src/backtest_remote.rs：保留 facade compatibility types/handlers，委托 tqsdk-data。
- crates/tqsdk/src/backtest_history_remote.rs：删除；逻辑迁移到 tqsdk-data。
- crates/tqsdk/src/lib.rs、Cargo.toml、README.md、examples 与 tests：BacktestBuilder 委托及 advanced::data curated re-export。
- README.md、docs/README.md、docs/architecture/ai-workflow.md、docs/architecture/README.md、docs/architecture/crate-boundaries.md、docs/architecture/api-data.md、docs/architecture/api-task.md、docs/architecture/validation.md：架构与验收同步。

## Task 1: Lock the Public Request, Event, and Collection Contract

**Files:**

- Create: crates/tqsdk-data/src/backtest_history/mod.rs
- Create: crates/tqsdk-data/src/backtest_history/request.rs
- Create: crates/tqsdk-data/src/backtest_history/report.rs
- Create: crates/tqsdk-data/tests/backtest_history_api.rs
- Create: crates/tqsdk-data/examples/api_contract_s48_backtest_history_query.rs
- Modify: crates/tqsdk-data/src/lib.rs
- Modify: crates/tqsdk-data/src/error.rs
- Modify: crates/tqsdk-data/Cargo.toml

- [ ] **Step 1: Write the failing public contract test**

在 backtest_history_api.rs 写入可编译契约，明确 request id、source policy、provisional 与 batch 内存行为：

~~~rust
use std::time::Duration;

use tqsdk_data::{
    BacktestHistoryClient, BacktestHistoryEvent, BacktestHistoryPolicy,
    BacktestHistoryRequest,
};

#[tokio::test]
async fn local_query_contract_is_available_without_remote_configuration() {
    let root = std::env::temp_dir().join("tqsdk-backtest-history-api-contract");
    let client = BacktestHistoryClient::builder(root)
        .policy(BacktestHistoryPolicy::CacheOnly)
        .logical_concurrency(32)
        .blocking_workers(1)
        .per_symbol_buffer_bytes(16 * 1024 * 1024)
        .collect_limit_bytes(512 * 1024 * 1024)
        .build()
        .unwrap();

    let request = BacktestHistoryRequest::kline(
        7,
        "SHFE.au2602",
        Duration::from_secs(15),
        1_000,
        2_000,
    );
    let mut run = client.query(request).await.unwrap();
    while let Some(event) = run.next().await {
        assert!(matches!(
            event,
            BacktestHistoryEvent::Chunk(_)
                | BacktestHistoryEvent::RequestCompleted(_)
                | BacktestHistoryEvent::RequestFailed(_)
        ));
    }
    let report = run.finish().await;
    assert_eq!(report.completed.len() + report.failed.len(), 1);
}
~~~

- [ ] **Step 2: Run the contract test and confirm the missing API**

Run:

~~~bash
rtk cargo test -p tqsdk-data --no-default-features --test backtest_history_api
~~~

Expected: FAIL with unresolved imports for BacktestHistoryClient and BacktestHistoryRequest.

- [ ] **Step 3: Add complete request validation**

在 request.rs 实现 Public API Contract 中的 request 类型，并让 validate() 返回内部 normalized spec。固定校验：

~~~rust
pub(crate) struct ValidatedBacktestHistoryRequest {
    pub request_id: BacktestHistoryRequestId,
    pub symbol: String,
    pub kind: BacktestHistoryKind,
    pub duration_ns: Option<i64>,
    pub start_ns: i64,
    pub end_ns: i64,
    pub provisional_as_of_ns: Option<i64>,
}

impl BacktestHistoryRequest {
    pub(crate) fn validate(&self) -> Result<ValidatedBacktestHistoryRequest> {
        if self.symbol.trim().is_empty() {
            return Err(DataError::Validation(
                "backtest history symbol must not be empty".to_string(),
            ));
        }
        if self.start_ns >= self.end_ns {
            return Err(DataError::Validation(format!(
                "backtest history range must satisfy start_ns < end_ns: [{}, {})",
                self.start_ns, self.end_ns
            )));
        }
        let duration_ns = match self.kind {
            BacktestHistoryKind::Tick => None,
            BacktestHistoryKind::Kline { duration } => {
                let value = i64::try_from(duration.as_nanos()).map_err(|_| {
                    DataError::Validation(
                        "backtest history Kline duration exceeds i64 nanoseconds".to_string(),
                    )
                })?;
                if value <= 0 {
                    return Err(DataError::Validation(
                        "backtest history Kline duration must be positive".to_string(),
                    ));
                }
                Some(value)
            }
        };
        if self.provisional_as_of_ns.is_some_and(|value| {
            value < self.start_ns || value > self.end_ns
        }) {
            return Err(DataError::Validation(
                "provisional_as_of_ns must be inside the requested range".to_string(),
            ));
        }
        Ok(ValidatedBacktestHistoryRequest {
            request_id: self.request_id,
            symbol: self.symbol.clone(),
            kind: self.kind.clone(),
            duration_ns,
            start_ns: self.start_ns,
            end_ns: self.end_ns,
            provisional_as_of_ns: self.provisional_as_of_ns,
        })
    }
}
~~~

query_batch 必须拒绝重复 request_id，并在启动任何 task 前返回 Validation。

- [ ] **Step 4: Add report and collection types**

在 report.rs 实现本计划 Report contract，并增加：

~~~rust
#[derive(Debug, Clone)]
pub struct BacktestHistoryChunk {
    pub request_id: BacktestHistoryRequestId,
    pub symbol: String,
    pub rows: BacktestHistoryRows,
}

#[derive(Debug, Clone)]
pub struct BacktestHistoryCollected {
    pub request: BacktestHistoryRequestReport,
    pub rows: BacktestHistoryRows,
}

#[derive(Debug, Clone, Default)]
pub struct BacktestHistoryCollectedBatch {
    pub completed: Vec<BacktestHistoryCollected>,
    pub failed: Vec<BacktestHistoryRequestFailure>,
}
~~~

BacktestHistoryRows 增加 len()、is_empty() 与 estimated_heap_bytes()；估算只使用 capacity × size_of::<row>() 加 enum/vector 固定开销，所有加法使用 checked_add，溢出返回 DataError::CollectLimitExceeded。

DataError 增加并完整接入 Display/source：

~~~rust
FeatureDisabled(&'static str),
CollectLimitExceeded {
    limit_bytes: usize,
    attempted_bytes: usize,
},
RequestFailed {
    request_id: BacktestHistoryRequestId,
    message: String,
    emitted_rows: usize,
},
~~~

- [ ] **Step 5: Add builder defaults and lazy auth contract**

定义：

~~~rust
pub struct BacktestHistoryCredentials {
    user: String,
    pass: String,
}

pub trait BacktestHistoryAuthProvider: Send + Sync {
    fn load<'a>(
        &'a self,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<BacktestHistoryCredentials>>
                + Send
                + 'a,
        >,
    >;
}
~~~

BacktestHistoryCredentials 不实现 Debug；提供 new(user, pass)，空值在 load 后验证。auth_env() 只保存 private EnvironmentBacktestHistoryAuthProvider，不立即读取 TQ_AUTH_USER/TQ_AUTH_PASS。provider 本身也是 async-capable，不能在 Tokio worker上阻塞外部认证读取。测试 provider 用 AtomicUsize 计数，为后续“完整缓存不碰认证”回归测试准备。

- [ ] **Step 6: Add a minimal closed-run implementation**

先实现 BacktestHistoryRun 的 mpsc receiver、terminal report 存储、固有 next()、Stream、finish() 框架。Task 7/8 再接 planner/executor；此任务只需让空缓存 CacheOnly 产生一个 RequestFailed，而不是 panic 或悬挂。

Stream poll_next 与固有 next() 必须消费同一个 receiver；finish(self) 先 drain receiver，再 await coordinator JoinHandle，保证用户没有手动读完整 stream 时仍能获得完整 report。

- [ ] **Step 7: Run API and feature-independent checks**

Run:

~~~bash
rtk cargo test -p tqsdk-data --no-default-features --test backtest_history_api
rtk cargo check -p tqsdk-data --no-default-features --example api_contract_s48_backtest_history_query
~~~

Expected: both commands exit 0；不要求 live/services。

- [ ] **Step 8: Commit**

~~~bash
rtk git add crates/tqsdk-data/Cargo.toml crates/tqsdk-data/src/lib.rs crates/tqsdk-data/src/error.rs crates/tqsdk-data/src/backtest_history crates/tqsdk-data/tests/backtest_history_api.rs crates/tqsdk-data/examples/api_contract_s48_backtest_history_query.rs
rtk git commit -m "feat(data): define backtest history query contract"
~~~

## Task 2: Move Both Aggregators into tqsdk-data and Fix Session/Volume Semantics

**Files:**

- Create: crates/tqsdk-data/src/aggregation/mod.rs
- Create: crates/tqsdk-data/src/aggregation/session.rs
- Create: crates/tqsdk-data/src/aggregation/tick.rs
- Create: crates/tqsdk-data/src/aggregation/minute.rs
- Create: crates/tqsdk-data/tests/backtest_history_aggregation.rs
- Modify: crates/tqsdk-data/src/lib.rs
- Modify: crates/tqsdk-task/src/kline_synth.rs
- Modify: crates/tqsdk-task/src/minute_kline_aggregate.rs
- Modify: crates/tqsdk-task/src/history_backtest_replay.rs
- Modify: crates/tqsdk-task/src/lib.rs
- Modify: crates/tqsdk-task/tests/minute_kline_aggregate.rs
- Modify: crates/tqsdk-task/tests/history_backtest_replay.rs

- [ ] **Step 1: Run impact analysis before moving symbols**

对 KlineSynthesizer::update、MinuteKlineAggregator::new、MinuteKlineAggregator::update、HistoryBacktestReplayStream::new_projected 执行 upstream impact。若风险不高于 MEDIUM，继续；否则先按 Impact Analysis Gates 报告。

- [ ] **Step 2: Write failing cross-crate aggregation tests**

覆盖以下确切 fixtures：

1. 15s：交易日第一笔累计 volume=3，第二笔=8，bar volume 必须为 8，不得为 5 或 0。
2. mid-day 查询从交易日起点预热：请求首 bar 前的 ticks 不输出，但建立正确 volume baseline。
3. 午休前后的 bars 不跨 window。
4. 新交易日 cumulative volume 从 0 重置；午休不重置。
5. 5m 从五根 closed 60s 聚合 OHLC/volume/OI。
6. 15m bucket 被 session break 截断时不得拼接 break 两侧。
7. synthetic id 等于 bar_start_ns，并在 session break 两侧保持唯一。

核心断言示例：

~~~rust
let mut agg = TickKlineAggregator::new(
    "KQ.i@SHFE.au",
    15_000_000_000,
    session_snapshot(),
).unwrap();
let first = agg.update(&tick(1, session_start + 1, 100.0, 3, 10)).unwrap();
let second = agg.update(&tick(2, session_start + 2, 101.0, 8, 11)).unwrap();
assert_eq!(first.unwrap().updated.volume, 3);
assert_eq!(second.unwrap().updated.volume, 8);
~~~

- [ ] **Step 3: Run tests and confirm current first-bar failure**

Run:

~~~bash
rtk cargo test -p tqsdk-data --test backtest_history_aggregation
~~~

Expected: FAIL because aggregation module and TickKlineAggregator do not exist.

- [ ] **Step 4: Implement snapshot-backed session location**

在 session.rs 定义：

~~~rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct KlineSessionWindow {
    pub start_offset_ns: i64,
    pub end_offset_ns: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct KlineSessionTemplate {
    snapshot_hash: String,
    windows: Vec<KlineSessionWindow>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KlineSessionPosition {
    pub trading_day_start_ns: i64,
    pub trading_day_end_ns: i64,
    pub window_start_ns: i64,
    pub window_end_ns: i64,
}
~~~

locate(timestamp_ns) 使用 backtest_tick_trading_day_for_timestamp_ns 与 backtest_tick_trading_day_range。windows 为空时整个 canonical CST trading day 是一个 window；显式 windows 必须递增、不重叠、不超出 trading day。timestamp 位于 break 时返回 Ok(None)。

- [ ] **Step 5: Implement TickKlineAggregator**

状态固定为：

~~~rust
pub struct TickKlineAggregator {
    symbol: String,
    duration_ns: i64,
    session: KlineSessionTemplate,
    current: Option<TickAggregateBar>,
    trading_day_start_ns: Option<i64>,
    previous_cumulative_volume: i64,
}

pub struct TickKlineAggregationUpdate {
    pub opened: Option<Kline>,
    pub updated: Kline,
    pub closed: Option<Kline>,
    pub event_time_ns: i64,
}
~~~

update 规则按顺序实现：

1. 忽略 datetime<0 或 last_price 非有限值。
2. locate tick；break 外 tick 不产出。
3. 新 trading day 时 previous_cumulative_volume=0 且关闭旧 current。
4. bar_start = window_start + (tick.datetime-window_start).div_euclid(duration) × duration。
5. effective_bar_end=min(bar_start+duration, window_end)；session 尾部不足一个 nominal duration 的 bar 在 window_end 关闭，属于官方语义下的完整 truncated bar，不得丢弃。
6. 新 bar 的 row.volume 从 0 开始。
7. 每笔 tick 的 volume_delta = tick.volume>=previous_cumulative_volume 时两者之差，否则为 0；把 delta 累加到当前 bar，而不是反复用 current-baseline 覆盖。
8. 每笔 tick 后 previous_cumulative_volume=tick.volume；累计量下降只把本次 delta 置 0，已经累加到当前 bar 的 volume 不丢失。
9. finish_closed_through(source_end_ns) 只在 effective_bar_end<=source_end_ns 时返回当前完整 bar。

duration 仅接受 0 < duration < 60s；60s 走 canonical minute，不允许 Tick 聚合器偷偷接管。

- [ ] **Step 6: Move MinuteKlineAggregator without changing canonical rules**

把现有 MinuteKlineAggregator 迁入 aggregation/minute.rs，输入仍只允许 duration > 60s 且 duration % 60s == 0。补充 closed 字段与 finish_closed_through，使 query 与 replay 共用同一状态机：

~~~rust
pub struct MinuteKlineAggregationUpdate {
    pub opened: Option<Kline>,
    pub updated: Kline,
    pub closed: Option<Kline>,
    pub event_time_ns: i64,
}
~~~

只在 closed_minute.datetime + 60s 已到达时 update；不得消费 partial minute。大周期同样以 effective_bar_end=min(nominal_bar_end, window_end) 判断 session 尾部 truncated bar 完成。

- [ ] **Step 7: Preserve tqsdk-task compatibility**

minute_kline_aggregate.rs 只做：

~~~rust
pub use tqsdk_data::{
    CANONICAL_MINUTE_KLINE_NS, KlineSessionTemplate as MinuteKlineSessionTemplate,
    KlineSessionWindow as MinuteKlineSessionWindow, MinuteKlineAggregationUpdate,
    MinuteKlineAggregator,
};
~~~

history_backtest_replay.rs 直接使用 tqsdk_data::TickKlineAggregator。删除私有 KlineSynthesizer 实现及其重复测试，但保留等价 task integration tests，证明 public task 行为没有变化。

- [ ] **Step 8: Run data and task aggregation tests**

Run:

~~~bash
rtk cargo test -p tqsdk-data --test backtest_history_aggregation
rtk cargo test -p tqsdk-task --test minute_kline_aggregate
rtk cargo test -p tqsdk-task --test history_backtest_replay
~~~

Expected: all pass；15s 首 bar volume、session break 与 5m 聚合断言通过。

- [ ] **Step 9: Commit**

~~~bash
rtk git add crates/tqsdk-data/src/aggregation crates/tqsdk-data/src/lib.rs crates/tqsdk-data/tests/backtest_history_aggregation.rs crates/tqsdk-task/src/kline_synth.rs crates/tqsdk-task/src/minute_kline_aggregate.rs crates/tqsdk-task/src/history_backtest_replay.rs crates/tqsdk-task/src/lib.rs crates/tqsdk-task/tests/minute_kline_aggregate.rs crates/tqsdk-task/tests/history_backtest_replay.rs
rtk git commit -m "refactor(data): share session-aware kline aggregation"
~~~

## Task 3: Make Tick and Minute Readers Partition-Stable

**Files:**

- Modify: crates/tqsdk-data/src/history_series_cache.rs
- Modify: crates/tqsdk-data/src/history_series_cache/store.rs
- Modify: crates/tqsdk-data/src/history_series_cache/tqbn/mod.rs
- Modify: crates/tqsdk-data/src/backtest_tick_cache.rs
- Modify: crates/tqsdk-data/src/minute_kline_cache.rs
- Modify: crates/tqsdk-data/tests/history_series_cache.rs
- Modify: crates/tqsdk-data/tests/minute_kline_cache.rs
- Create: crates/tqsdk-data/tests/backtest_history_async.rs

- [ ] **Step 1: Run reader impact analysis**

分析 HistorySeriesCache::open_tick_data_series_reader、TickDataSeriesReader::next_tick、MinuteKlineCache::open_reader、MinuteKlineReader::next_kline。记录现有同步调用方，明确这些方法的签名必须保留。

- [ ] **Step 2: Write failing lock-lifetime tests**

测试流程：

1. 写入两个 Tick 日分区与两个 minute 月分区。
2. 打开 reader 并读到第一个分区第一行。
3. 对当前分区尝试 writer/purge，断言 CacheBusy。
4. 消费完当前分区并进入下一分区。
5. 再写第一个分区，断言成功，证明锁已立即释放。
6. reader 创建后尝试替换 path，读取仍来自最初固定 handle 或被 exclusive lock 阻止。
7. 损坏文件返回 error 且文件仍存在、字节不变。

- [ ] **Step 3: Run tests and observe missing shared locks**

Run:

~~~bash
rtk cargo test -p tqsdk-data --test history_series_cache reader_holds_partition_shared_lock
rtk cargo test -p tqsdk-data --test minute_kline_cache reader_holds_month_shared_lock
~~~

Expected: FAIL because current readers do not hold partition-level shared lock through consumption.

- [ ] **Step 4: Add a locked Tick partition reader**

TQBN reader 每次进入日分区时：

~~~rust
struct TqbnStreamingPartition {
    file: std::fs::File,
    lock_file: std::fs::File,
    blocks: Vec<TqbnStreamingBlockPlan>,
    next_block_index: usize,
    active: Vec<TqbnStreamingBlockCursor>,
    spare_records: Vec<u8>,
}

impl Drop for TqbnStreamingPartition {
    fn drop(&mut self) {
        let _ = fs2::FileExt::unlock(&self.lock_file);
    }
}
~~~

先打开既有 partition lock file并 lock_shared，再在锁内 open data file并从固定 handle构建 reader。writable cache可以为新分区创建 lock sidecar；open_read_only 绝不创建目录或 lock，缺失 lock 时 fail closed。current=None 时立即 drop 当前 guard。不要把所有日期锁一次性持有到整个 range 完成。

- [ ] **Step 5: Add a locked minute month reader**

把现有 MonthFileLock 拆为 shared read 与 exclusive write 两种 mode。MonthRowReader::open 接受已经打开且锁定的 File，不再按 path 二次打开。MinuteKlineReader 保持 public 字段/方法语义不变，只把 current 改为 Option<LockedMonthRowReader>：

~~~rust
struct LockedMonthRowReader {
    lock_file: std::fs::File,
    reader: MonthRowReader,
}

impl Drop for LockedMonthRowReader {
    fn drop(&mut self) {
        let _ = fs2::FileExt::unlock(&self.lock_file);
    }
}
~~~

- [ ] **Step 6: Add crate-private chunk methods without breaking sync APIs**

为两个 reader 增加：

~~~rust
pub(crate) fn next_tick_chunk(&mut self, target_bytes: usize) -> Result<Vec<Tick>>;
pub(crate) fn next_kline_chunk(&mut self, target_bytes: usize) -> Result<Vec<Kline>>;
~~~

target_bytes=0 返回 Validation。至少读一行，之后在 estimated row bytes 达到 target 时停止；不得跨过 error。public next_tick/next_kline 继续逐行工作，并与 chunk 方法共享同一个 cursor。

另增加 crate-private backtest query reader 构造：Final 路径仍调用 require_coverage；Provisional 路径必须先验证 BacktestTickProvisionalCoverage.complete_through_ns >= effective_end_ns，再打开同一底层 Tick reader。不得放宽 public BacktestTickCache::load_series 的 final-only 语义。

- [ ] **Step 7: Re-run reader tests**

Run:

~~~bash
rtk cargo test -p tqsdk-data --test history_series_cache
rtk cargo test -p tqsdk-data --test minute_kline_cache
rtk cargo test -p tqsdk-data --test backtest_history_async partition
~~~

Expected: all pass；损坏文件未被删除，现有 load_series/read_range 仍通过。

- [ ] **Step 8: Commit**

~~~bash
rtk git add crates/tqsdk-data/src/history_series_cache.rs crates/tqsdk-data/src/history_series_cache/store.rs crates/tqsdk-data/src/history_series_cache/tqbn/mod.rs crates/tqsdk-data/src/backtest_tick_cache.rs crates/tqsdk-data/src/minute_kline_cache.rs crates/tqsdk-data/tests/history_series_cache.rs crates/tqsdk-data/tests/minute_kline_cache.rs crates/tqsdk-data/tests/backtest_history_async.rs
rtk git commit -m "fix(data): hold stable partition handles while reading"
~~~

## Task 4: Persist Session, Calendar, and Continuous Mapping Metadata

**Files:**

- Create: crates/tqsdk-data/src/backtest_history/metadata.rs
- Create: crates/tqsdk-data/tests/backtest_history_metadata.rs
- Create: crates/tqsdk-data/tests/support/backtest_history.rs
- Modify: crates/tqsdk-data/src/backtest_history/mod.rs
- Modify: crates/tqsdk-data/Cargo.toml

- [ ] **Step 1: Write failing metadata round-trip tests**

覆盖：

- concrete symbol snapshot round trip。
- KQ.i logical snapshot round trip。
- KQ.m 两个 physical segments 的换月 snapshot round trip。
- sidecar active snapshot 不因 captured_at 变旧而自动失效。
- 缺失 CacheOnly 返回 typed miss。
- JSON/hash 不匹配 fail closed，原文件保留。
- refresh 写新 snapshot 后旧 snapshot 文件仍保留；只更新 active pointer，不自动清理。

- [ ] **Step 2: Run tests and confirm the sidecar is absent**

Run:

~~~bash
rtk cargo test -p tqsdk-data --no-default-features --test backtest_history_metadata
~~~

Expected: FAIL with missing BacktestHistoryMetadataCache types.

- [ ] **Step 3: Define the canonical snapshot**

~~~rust
pub const BACKTEST_HISTORY_METADATA_FORMAT_ID: &str =
    "tqsdk.backtest-history-metadata.v1";
pub const BACKTEST_HISTORY_METADATA_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BacktestHistoryMetadataSnapshot {
    pub schema_version: u32,
    pub market_kind: BacktestHistoryMarketKind,
    pub logical_symbol: String,
    pub captured_at_ns: i64,
    pub trading_days: Vec<BacktestHistoryTradingDay>,
    pub session: KlineSessionTemplate,
    pub physical_segments: Vec<BacktestHistoryPhysicalSegment>,
    pub snapshot_hash: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum BacktestHistoryMarketKind {
    Futures,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BacktestHistoryTradingDay {
    pub date: String,
    pub is_trading_day: bool,
    pub start_ns: i64,
    pub end_ns: i64,
}

#[derive(Clone)]
pub struct BacktestHistoryMetadataCache {
    root_dir: std::path::PathBuf,
    writable: bool,
}

impl BacktestHistoryMetadataCache {
    pub fn open(root_dir: impl AsRef<std::path::Path>) -> Result<Self>;
    pub fn open_read_only(root_dir: impl AsRef<std::path::Path>) -> Self;
    pub fn load_active(
        &self,
        logical_symbol: &str,
    ) -> Result<Option<BacktestHistoryMetadataSnapshot>>;
    pub fn store_snapshot(
        &self,
        snapshot: BacktestHistoryMetadataSnapshot,
    ) -> Result<BacktestHistoryMetadataSnapshot>;
}
~~~

hash 输入是除 snapshot_hash 外字段的 canonical JSON bytes；对象字段顺序由单独 CanonicalSnapshotBody struct 固定，数组保持原有排序。使用 sha1 crate 输出小写 hex。load 时重算并恒等比较。RemoteOnMiss 可以补齐 snapshot 尚未覆盖的请求区间，但不得因为 captured_at_ns 变旧而重取已经覆盖的区间；显式 maintenance refresh 才能重取已覆盖 metadata。

- [ ] **Step 4: Implement the sidecar namespace and atomic commit**

路径固定为：

~~~text
<cache_root>/backtest-history-metadata-v1/
  <escaped-logical-symbol>/
    active.json
    snapshots/<snapshot-hash>.json
    .metadata.lock
~~~

store_snapshot：

1. 验证 logical symbol、trading day、session windows 与 physical segments。
2. 获取 .metadata.lock exclusive。
3. 把 snapshot 写到同目录 create_new 临时文件，flush + sync_all。
4. rename 到 snapshots/hash.json；已存在同 hash 时逐字节验证。
5. 用同样方式原子替换 active.json。
6. 不删除任何旧 snapshot。

load_active 使用 shared lock；缺失返回 Ok(None)，损坏/不兼容返回 DataError，不回退到其他 snapshot。

- [ ] **Step 5: Add independent maintenance API**

定义 BacktestHistoryMaintenanceClient，只提供显式 refresh_metadata(symbol, range) 和 inspect_metadata；查询 client 不出现 refresh/purge 方法：

~~~rust
pub struct BacktestHistoryMaintenanceClient;
pub struct BacktestHistoryMaintenanceClientBuilder;

impl BacktestHistoryMaintenanceClientBuilder {
    #[must_use]
    pub fn auth_env(self) -> Self;

    #[must_use]
    pub fn auth_provider(
        self,
        provider: impl BacktestHistoryAuthProvider + 'static,
    ) -> Self;

    pub fn build(self) -> Result<BacktestHistoryMaintenanceClient>;
}

impl BacktestHistoryMaintenanceClient {
    pub fn builder(
        cache_dir: impl Into<std::path::PathBuf>,
    ) -> BacktestHistoryMaintenanceClientBuilder;

    pub fn inspect_metadata(
        &self,
        symbol: &str,
    ) -> Result<Option<BacktestHistoryMetadataSnapshot>>;

    pub async fn refresh_metadata(
        &self,
        symbol: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<BacktestHistoryMetadataSnapshot>;
}
~~~

maintenance builder 复用 auth_env()/auth_provider()，refresh 使用同一个 async auth provider 与 metadata resolver，但必须由调用方显式调用。

Remote metadata resolver 在 all(feature="live", feature="services") 下编译；no-default-features 下 maintenance inspect 与 CacheOnly load 仍可用。

- [ ] **Step 6: Preserve physical Tick identity**

为 KQ.m planner 提供 resolved_tick_segments()，返回 logical replay symbol 和 physical cache symbol。去重 key 固定为：

~~~rust
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct PhysicalTickKey {
    physical_symbol: String,
    tick_id: i64,
}
~~~

不得只按 tick_id 跨换月去重。Chunk 对外仍标 logical symbol，report 暴露 physical_segments。

- [ ] **Step 7: Run metadata tests**

Run:

~~~bash
rtk cargo test -p tqsdk-data --no-default-features --test backtest_history_metadata
~~~

Expected: all pass；测试显式断言旧 snapshot 未清理。

- [ ] **Step 8: Commit**

~~~bash
rtk git add crates/tqsdk-data/Cargo.toml crates/tqsdk-data/src/backtest_history/mod.rs crates/tqsdk-data/src/backtest_history/metadata.rs crates/tqsdk-data/tests/backtest_history_metadata.rs crates/tqsdk-data/tests/support/backtest_history.rs
rtk git commit -m "feat(data): persist backtest history metadata snapshots"
~~~

## Task 5: Extract a Server-Backtest History Stream into tqsdk-session

**Files:**

- Create: crates/tqsdk-session/src/backtest_history.rs
- Create: crates/tqsdk-session/tests/server_backtest_history.rs
- Modify: crates/tqsdk-session/src/lib.rs
- Modify: crates/tqsdk-session/Cargo.toml
- Modify: crates/tqsdk-wait/src/backtest.rs
- Modify: crates/tqsdk-wait/src/builder.rs
- Modify: crates/tqsdk-wait/src/api.rs
- Create: crates/tqsdk-wait/tests/backtest.rs

- [ ] **Step 1: Run impact analysis and inspect only the relevant DIFF contract**

对 SessionClient::progress_once、SessionClient::ensure_chart、BacktestPump::ensure_tick_serial、BacktestPump::handle_commit 做 upstream impact。重新核对 set_chart focus_datetime/focus_position/left_kline_id、charts.ready/more_data、mdhis_more_data、ticks/klines 数据路径；不得把新的状态树或 cursor 放进 session substrate。

- [ ] **Step 2: Write failing manual-session protocol tests**

测试以下确定行为：

- Tick 首页使用 duration_ns=0、focus_datetime_ns=start、focus_position=0、view_width=10_000。
- 后续 Tick 页使用 left_kline_id，不能重复输出上一页 id。
- 60s chart 使用 duration_ns=60_000_000_000。
- Chart 只有 ready=true、chart.more_data=false、mdhis_more_data=false 才可标记当前页完成。
- 空区间也必须在 server terminal 后发 ChartCompleted。
- 多 chart 可交错产出，某一 chart ready 不等待其他 chart。
- 取消/transport error 不产生 StreamCompleted。

使用 tqsdk_session::testing::ManualSession 注入 DIFF；断言 outbound set_chart body 与事件序列。

- [ ] **Step 3: Run tests and confirm tqsdk-session lacks the substrate**

Run:

~~~bash
rtk cargo test -p tqsdk-session --test server_backtest_history
~~~

Expected: FAIL with unresolved ServerBacktestHistoryStream types.

- [ ] **Step 4: Define the low-level session contract**

在 backtest_history.rs 定义：

~~~rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerBacktestMarketKind {
    Futures,
    Stock,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerBacktestHistoryKind {
    Tick,
    CanonicalMinute,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerBacktestHistoryChart {
    pub chart_id: String,
    pub symbol: String,
    pub kind: ServerBacktestHistoryKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerBacktestHistoryRequest {
    pub market_kind: ServerBacktestMarketKind,
    pub start_ns: i64,
    pub end_ns: i64,
    pub charts: Vec<ServerBacktestHistoryChart>,
}

#[derive(Debug, Clone)]
pub enum ServerBacktestHistoryEvent {
    Ticks {
        chart_id: String,
        symbol: String,
        rows: Vec<tqsdk_core::Tick>,
    },
    CanonicalMinutes {
        chart_id: String,
        symbol: String,
        rows: Vec<tqsdk_core::Kline>,
    },
    ChartCompleted {
        chart_id: String,
        symbol: String,
    },
    StreamCompleted,
}
~~~

ServerBacktestHistoryStream::open(session, request).await 验证 session market target 是对应 backtest target；next_event(deadline).await 推进唯一 SessionClient、读取 RuntimeReader/cursor，并返回上述事件。tqsdk-data 不得操作 wait refs。

request/event value types 在 no-default-features 下也编译，便于 local planner 与 fake tests；真实 websocket open/next I/O 由 tqsdk-session 的 live feature 门控。

- [ ] **Step 5: Move cache-fill Tick pagination, not the strategy facade**

从 tqsdk-wait::BacktestPumpMode::CacheFill 抽出分页状态机到 session。规则保持：

- page rows 按 id 排序。
- [start_ns, end_ns) 过滤。
- page prefetch 不覆盖正在消费页。
- terminal page 允许 right_id 对应行不存在。
- 去重按 (chart_id, row_id)。
- 每个 event rows 有界，最大 10_000 行。

tqsdk-wait 保留策略用 BacktestPump 与 Python-style synthetic commits；移除 backtest_cache_fill_mode() 及 CacheFill 分支。现有普通 server-side backtest API 行为不变。

- [ ] **Step 6: Implement canonical-minute extraction**

读取路径 klines/<symbol>/60000000000/data/<id>。每个 chart 维护 last_emitted_id；仅输出 datetime 位于 request range 的 rows。ChartCompleted 权威条件是 chart ready、chart.more_data=false 与全局 mdhis_more_data=false，不能从 timestamp gaps 推断 completion。

- [ ] **Step 7: Release chart leases and propagate failures**

stream 持有 MarketChartLease；chart 完成或 stream drop 时异步 cancel 不可在 Drop 中 await，因此 Drop 只标记 shared cancellation，coordinator task 负责 close leases。任何 decode/session error 直接返回 SessionFacadeError，调用方决定是否 retry。

- [ ] **Step 8: Run session and wait regression tests**

Run:

~~~bash
rtk cargo test -p tqsdk-session --test server_backtest_history
rtk cargo test -p tqsdk-wait backtest
rtk cargo check -p tqsdk-wait --examples
~~~

Expected: all pass；tqsdk-wait 的普通策略 backtest contract 不变。

- [ ] **Step 9: Commit**

~~~bash
rtk git add crates/tqsdk-session/Cargo.toml crates/tqsdk-session/src/lib.rs crates/tqsdk-session/src/backtest_history.rs crates/tqsdk-session/tests/server_backtest_history.rs crates/tqsdk-wait/src/backtest.rs crates/tqsdk-wait/src/builder.rs crates/tqsdk-wait/src/api.rs crates/tqsdk-wait/tests/backtest.rs
rtk git commit -m "refactor(session): own server backtest history stream"
~~~

## Task 6: Move Remote Fill and Single-Flight Coordination into tqsdk-data

**Files:**

- Create: crates/tqsdk-data/src/backtest_history/fill.rs
- Create: crates/tqsdk-data/src/backtest_history/telemetry.rs
- Modify: crates/tqsdk-data/src/backtest_history/mod.rs
- Modify: crates/tqsdk-data/src/backtest_history/request.rs
- Modify: crates/tqsdk-data/src/backtest_tick_cache.rs
- Modify: crates/tqsdk-data/src/minute_kline_cache.rs
- Modify: crates/tqsdk-data/tests/backtest_history_async.rs
- Modify: crates/tqsdk-data/tests/backtest_history_query.rs

- [ ] **Step 1: Write failing fill coordination tests with a fake server stream**

建立 crate-private ServerHistorySource factory seam；production factory 使用 tqsdk-session，tests 注入 scripted events。测试：

- 完整 Tick/minute coverage 时 fake factory 与 auth provider 调用次数均为 0。
- 两个重叠同 root/symbol/range 的 request 只启动一次 overlap fill。
- 非重叠尾部只补 union 中缺少的 slice。
- fill owner 持有跨进程 lock 时，waiter 不返回 CacheBusy；它每 250ms 复查 coverage，owner 提交后继续 read。
- 不同 cache symbol 的跨进程 fill locks 可以并行；同 symbol 才互斥。
- 一个 consumer drop 后其他 consumer 仍收到完成。
- 最后 consumer drop 后 fill 停止；partial Tick rows 可存在但 final coverage 仍 missing。
- minute 只有 StreamCompleted 后调用 store_final_range。
- 一个 request 连续 retry 失败不影响 batch 中已命中 request。

- [ ] **Step 2: Run tests and confirm current facade-only fill cannot satisfy them**

Run:

~~~bash
rtk cargo test -p tqsdk-data --test backtest_history_async fill
~~~

Expected: FAIL because remote fill and single-flight coordinator are not in tqsdk-data.

- [ ] **Step 3: Move compatibility fill configuration into data**

先定义 crate-private、可注入的异步 seam：

~~~rust
trait ServerHistorySource: Send {
    fn next_event<'a>(
        &'a mut self,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<Option<tqsdk_session::ServerBacktestHistoryEvent>>,
                > + Send
                + 'a,
        >,
    >;
}

trait ServerHistorySourceFactory: Send + Sync {
    fn open<'a>(
        &'a self,
        credentials: BacktestHistoryCredentials,
        request: tqsdk_session::ServerBacktestHistoryRequest,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<Box<dyn ServerHistorySource>>>
                + Send
                + 'a,
        >,
    >;
}
~~~

迁移现有 BacktestRemoteFillConfig、BacktestRemoteFillCancellation、BacktestRemoteFillProgress 与 handlers 的实现到 tqsdk-data，保持字段和环境变量解析兼容。新增 query defaults 不改写旧 facade config；query client 默认 logical_concurrency=32，而每个 server batch 的安全 chart/page limit仍受现有 server 容量约束。

旧 TQSDK_REMOTE_FILL_* 环境变量只在显式 from_environment() 时读取；BacktestHistoryClientBuilder 默认值不隐式读取它们。

- [ ] **Step 4: Implement overlap-aware in-process single-flight**

registry key 前缀：

~~~rust
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct FillSeriesKey {
    canonical_cache_root: PathBuf,
    family: FillFamily,
    cache_symbol: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum FillFamily {
    Tick,
    CanonicalMinute,
}

static FILL_REGISTRY: std::sync::OnceLock<
    std::sync::Mutex<std::collections::BTreeMap<
        FillSeriesKey,
        Vec<std::sync::Weak<SharedFill>>,
    >>,
> = std::sync::OnceLock::new();
~~~

每个 key 保存按 start_ns 排序的 active intervals。subscribe(requested_range)：

1. 对每个 active interval 计算 overlap，并附加 consumer guard。
2. 对尚未覆盖的 gaps 创建 SharedFill。
3. 返回多个 FillSubscription；caller join all。
4. SharedFill terminal 后从 registry 删除。

不要把两个不相交 range 强行合成一个远端请求。

- [ ] **Step 5: Implement cooperative cancellation**

~~~rust
struct SharedFill {
    range: (i64, i64),
    state: SharedFillState,
    result: std::sync::Mutex<Option<std::result::Result<(), String>>>,
}

struct SharedFillState {
    consumers: std::sync::atomic::AtomicUsize,
    cancel_requested: std::sync::atomic::AtomicBool,
    terminal: tokio::sync::Notify,
}
~~~

FillConsumerGuard::drop 使用 fetch_sub；旧值为 1 时设置 cancel_requested 并 notify。server loop、retry sleep 前后、每个 cache write chunk 前检查。若取消：

- Tick 已写 row blocks 保留。
- 不调用 mark_complete/append final coverage。
- minute 尚未 terminal 的 rows 不调用 store_final_range。
- telemetry 发 terminal failure/cancel event。

- [ ] **Step 6: Implement cross-process wait**

不要复用现有 cache-root-wide exclusive remote-fill lock来串行化 32 个 symbols。新增不属于数据格式的 lease namespace：

~~~text
<cache_root>/.backtest-history-fill-locks/
  tick/<escaped-cache-symbol>.lock
  minute/<escaped-cache-symbol>.lock
~~~

同 family/cache symbol 跨进程互斥，不同 symbol 可并行；文件不自动清理。series lease 返回 CacheBusy 时：

~~~rust
loop {
    if coverage_is_complete()? {
        break;
    }
    if cancel_requested() {
        return Err(cancelled_error());
    }
    tokio::time::sleep(Duration::from_millis(250)).await;
    if let Ok(lock) = try_acquire_series_fill_lock() {
        replan_missing_ranges_under_lock(lock)?;
        break;
    }
}
~~~

拿到 lock 后必须重新 inspect，防止重复远端请求。等待不占 blocking scan worker。

- [ ] **Step 7: Implement Tick and canonical-minute final commit rules**

Tick：

- server events 可按 8_192 rows 写 partial segments。
- physical cache symbol 作为落盘 key；logical symbol 只进入 query report。
- terminal 后用 BacktestTickFillReport 验证 id/date range，再提交每个 final missing slice。
- 空合法 range 只有 server terminal 时可 final。
- final missing slices 只允许完整关闭的交易日；默认请求触及当前交易日时返回 non-final cache miss，不把当前日标成 final。
- 显式 provisional_as_of 的当前日 fill 只写 rows 与 BacktestTickProvisionalCoverage(complete_through_ns, as_of_ns)，绝不调用 mark_complete。

Minute：

- 只请求 duration=60s。
- 单个 fill slice不得超过 10_000 × 60s wall-clock span。
- 每个 request 在内存中按 datetime 去重。
- StreamCompleted 后逐 request store_final_range；当前交易日继续由 MinuteKlineCache 拒绝。

- [ ] **Step 8: Add independent telemetry stream**

~~~rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BacktestHistoryPhase {
    Inspect,
    WaitForFill,
    Fill,
    Retry,
    Read,
    Aggregate,
}

#[derive(Debug, Clone)]
pub struct BacktestHistoryTelemetryEvent {
    pub request_id: Option<BacktestHistoryRequestId>,
    pub symbol: String,
    pub phase: BacktestHistoryPhase,
    pub completed_rows: usize,
    pub message: String,
}
~~~

使用独立 shared telemetry buffer + Notify，而不是 producer await 的 mpsc。buffer 为每个 (request_id, phase) 只保留最新 progress；terminal 事件单独按 request_id 保留到被读取或 run finish。producer 只做短 Mutex 临界区与 notify，绝不等待 telemetry consumer，因此不得阻塞 row event channel；report 仍是最终权威。

- [ ] **Step 9: Gate production remote implementation**

production ServerHistorySource 只在 all(feature="live", feature="services") 编译。缺 feature 且 CacheOnly 命中时正常工作；RemoteOnMiss 遇到缺口时返回：

~~~text
remote backtest history fill requires tqsdk-data features "live" and "services"
~~~

不得在 build() 或 query() 的完整 cache path 提前报 feature/auth 错误。

- [ ] **Step 10: Run coordination tests**

Run:

~~~bash
rtk cargo test -p tqsdk-data --test backtest_history_async fill
rtk cargo test -p tqsdk-data --no-default-features --test backtest_history_query cache_hit_does_not_load_auth
~~~

Expected: all pass；single-flight invocation count 是 1，最后取消不产生 final coverage。

- [ ] **Step 11: Commit**

~~~bash
rtk git add crates/tqsdk-data/src/backtest_history/fill.rs crates/tqsdk-data/src/backtest_history/telemetry.rs crates/tqsdk-data/src/backtest_history/mod.rs crates/tqsdk-data/src/backtest_history/request.rs crates/tqsdk-data/src/backtest_tick_cache.rs crates/tqsdk-data/src/minute_kline_cache.rs crates/tqsdk-data/tests/backtest_history_async.rs crates/tqsdk-data/tests/backtest_history_query.rs
rtk git commit -m "feat(data): coordinate server backtest cache fills"
~~~

## Task 7: Build the Query Planner and Shared Scan Graph

**Files:**

- Create: crates/tqsdk-data/src/backtest_history/planner.rs
- Create: crates/tqsdk-data/tests/backtest_history_query.rs
- Modify: crates/tqsdk-data/src/backtest_history/metadata.rs
- Modify: crates/tqsdk-data/src/backtest_history/request.rs
- Modify: crates/tqsdk-data/src/backtest_history/report.rs

- [ ] **Step 1: Write the failing source-policy matrix**

表驱动测试固定为：

| Request | Base source | Result |
| --- | --- | --- |
| Tick | Tick cache | accepted |
| 15s | Tick cache | accepted |
| 59s | Tick cache | accepted |
| 60s | canonical minute | accepted |
| 5m | canonical minute + aggregate | accepted |
| 15m | canonical minute + aggregate | accepted |
| 30m | canonical minute + aggregate | accepted |
| 60m | canonical minute + aggregate | accepted |
| 61s | none | validation error |
| 90s | none | validation error |
| zero | none | validation error |

同时测试 provisional_as_of：Tick/15s accepted，60s/5m rejected。

- [ ] **Step 2: Run the planner tests and observe failure**

Run:

~~~bash
rtk cargo test -p tqsdk-data --test backtest_history_query source_policy
~~~

Expected: FAIL because planner is absent.

- [ ] **Step 3: Implement exact source classification**

~~~rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PlannedBaseSource {
    Tick,
    CanonicalMinute,
}

fn classify_duration(duration_ns: i64) -> Result<PlannedBaseSource> {
    match duration_ns {
        value if value > 0 && value < MINUTE_KLINE_DURATION_NS => {
            Ok(PlannedBaseSource::Tick)
        }
        MINUTE_KLINE_DURATION_NS => Ok(PlannedBaseSource::CanonicalMinute),
        value if value > MINUTE_KLINE_DURATION_NS
            && value % MINUTE_KLINE_DURATION_NS == 0 =>
        {
            Ok(PlannedBaseSource::CanonicalMinute)
        }
        _ => Err(DataError::Validation(
            "Kline duration must be below 60s, exactly 60s, or an integer multiple of 60s"
                .to_string(),
        )),
    }
}
~~~

- [ ] **Step 4: Expand source ranges deterministically**

planner 使用 metadata snapshot 定位每个 requested bar 的 session bucket：

- requested bar starts 仍按 [start_ns,end_ns) filter。
- base source start 向前扩到可能包含首个 requested bar 的 bucket start。
- base source end 向后扩到最后一个 requested bar 的 bucket end。
- Tick 派生再把 start 扩到首个涉及 trading day 的 start，用于 cumulative volume warmup。
- 多 trading day range 保留每个 day/session slice，不把休市间隔声明为数据缺口。

报告 expanded_source_range 是所有 slice 的 min start/max end；内部保留精确 Vec<SourceSlice>。

- [ ] **Step 5: Resolve logical symbols**

固定规则：

- concrete：Tick 与 minute 都使用相同 logical/cache symbol，report 产生一个 physical_symbol=logical_symbol 的 segment。
- KQ.i：Tick 与 minute 都直接以 KQ.i logical symbol 查询/cache；不伪造 KQ.m mapping，report 同样产生 logical self-segment。
- KQ.m：Tick 使用 metadata physical segments，minute 使用 KQ.m logical symbol。
- KQ.m CacheOnly 缺 metadata sidecar 时立即 RequestFailed，不联网。
- RemoteOnMiss 缺 metadata 时惰性 resolve + persist 后再 inspect data coverage。
- active snapshot 的 market_kind 必须为 Futures；remote resolver 识别出股票、期权等非期货时返回 Validation。public request 没有 stock selector。

所有 report 都复制 active snapshot hash 与 physical segments。

MinuteKlineCacheSnapshot 从 active metadata 的 calendar/session hashes 构造。已有 minute v4 文件若 hash 不匹配必须 fail closed；查询不得重写或删除，用户只能通过独立 maintenance API 显式处理。

- [ ] **Step 6: Deduplicate base scans**

~~~rust
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct BaseScanKey {
    family: PlannedBaseSource,
    cache_symbol: String,
    source_slices: Vec<(i64, i64)>,
    snapshot_hash: String,
    finality: BacktestHistoryFinality,
}
~~~

planner 先按 (family, cache_symbol, snapshot_hash, finality) 分组，把 batch 内重叠或相接的 source slices 规范化为 union，再用该 union 构造同一个 BaseScanKey；每个 consumer仍按自己的 requested/source slices过滤。Tick scan fan-out 给 Tick request 与所有小于 60s aggregators；minute scan fan-out 给 60s passthrough 与所有大周期 aggregators。不同 physical segment 各自 scan，logical request 在 segment boundary 稳定 merge。

- [ ] **Step 7: Add counting-reader tests**

fake reader 记录 open/next_chunk 次数。一个 batch 同时查询同 symbol/range 的 15s、30s、60s、5m、15m、30m、60m，断言：

- Tick base scan 恰好 1 次。
- canonical minute base scan 恰好 1 次。
- 15s rows 来自 Tick。
- 60s rows与源逐行相同。
- 5m/15m/30m/60m 来自同 minute fan-out。
- 再加入一个范围与前述 range 部分重叠的 15s request，overlap 内 Tick rows 仍只扫描一次。

- [ ] **Step 8: Add range/finality/corruption tests**

覆盖：

- 非 bucket-aligned requested range 只返回 bar-start 在范围内的完整 bars。
- expanded source rows 不泄漏到输出。
- current day final-only 缺口失败。
- explicit provisional Tick/15s 返回 Provisional report。
- provisional effective_end=min(end_ns, as_of_ns)；Tick 只输出 datetime<effective_end，15s 只输出 bar_end<=effective_end 的完整 bar。
- minute snapshot mismatch 在任何 chunk 前失败。
- scan 中途损坏时允许先前 chunks，但最终 RequestFailed.emitted_rows 精确。

- [ ] **Step 9: Run planner tests**

Run:

~~~bash
rtk cargo test -p tqsdk-data --test backtest_history_query
~~~

Expected: all pass；counting reader 两个 base scan count 均为 1。

- [ ] **Step 10: Commit**

~~~bash
rtk git add crates/tqsdk-data/src/backtest_history/planner.rs crates/tqsdk-data/src/backtest_history/metadata.rs crates/tqsdk-data/src/backtest_history/request.rs crates/tqsdk-data/src/backtest_history/report.rs crates/tqsdk-data/tests/backtest_history_query.rs
rtk git commit -m "feat(data): plan shared backtest history scans"
~~~

## Task 8: Implement the Async Chunk Executor, Batch Isolation, and Memory Limits

**Files:**

- Create: crates/tqsdk-data/src/backtest_history/store_worker.rs
- Create: crates/tqsdk-data/src/backtest_history/executor.rs
- Modify: crates/tqsdk-data/src/backtest_history/mod.rs
- Modify: crates/tqsdk-data/src/backtest_history/report.rs
- Modify: crates/tqsdk-data/tests/backtest_history_async.rs
- Modify: crates/tqsdk-data/tests/backtest_history_api.rs

- [ ] **Step 1: Write failing scheduler and collection tests**

测试：

- 40 requests 中最多 32 个处于 inspect/fill/read logical active 状态。
- blocking scanner 最大并发等于配置 min(8, CPU)，测试配置为 2 时观测值不得超过 2。
- 一个 cache-hit request 先输出，不等待一个被 fake fill 阻塞的 request。
- 同一 symbol 同时存在 Tick/minute base scans 时，共享 byte budget 仍不得超过 16 MiB。
- 一个 request failure 后其他 request 继续 RequestCompleted。
- RequestCompleted 之前收到的 chunks 计为 provisional；中途失败时 emitted_rows 正确。
- collect() 遇 request failure 丢弃 partial rows。
- collect() 超过 512 MiB 返回 CollectLimitExceeded。
- collect_all 没有无参重载；显式总限制生效，已完成成功项也不得绕过总限制。

- [ ] **Step 2: Run tests and confirm the executor is absent**

Run:

~~~bash
rtk cargo test -p tqsdk-data --test backtest_history_async executor
~~~

Expected: FAIL with missing executor/store worker behavior.

- [ ] **Step 3: Implement bounded blocking scans**

每个 BaseScan 在获得 semaphore permit 后启动一个 spawn_blocking closure；closure 独占 reader 并循环 next_*_chunk(target_chunk_bytes)，通过 tokio mpsc::Sender::blocking_send 发送。每个 logical symbol 建立一个被其 Tick/minute scans 共用的 SymbolBufferBudget，chunk envelope 持有 BytePermit，drop 后归还 bytes。固定：

~~~rust
#[derive(Clone)]
struct SymbolBufferBudget {
    capacity_bytes: usize,
    shared: std::sync::Arc<(
        std::sync::Mutex<usize>,
        std::sync::Condvar,
    )>,
}

struct BytePermit {
    bytes: usize,
    shared: std::sync::Arc<(
        std::sync::Mutex<usize>,
        std::sync::Condvar,
    )>,
}

struct BufferedSourceChunk {
    rows: BacktestHistoryRows,
    _permit: BytePermit,
}
~~~

- semaphore permits = blocking_workers。
- target_chunk_bytes = min(1 MiB, per_symbol_buffer_bytes)，至少一行。
- SymbolBufferBudget 总量 = per_symbol_buffer_bytes；不是每个 base scan 各一份。
- channel capacity 可按 chunk 数估算，但 BytePermit 是最终字节上限。
- blocking closure 每个 chunk 前后检查 cancel flag。
- async side drop receiver 后 blocking_send 返回 error，closure 协作退出。

BytePermit 使用 std::sync::Mutex + Condvar 获取，允许 blocking producer 等待；Drop 只做短临界区归还与 notify。不要在 Tokio async worker 上阻塞获取 permit。不为每行调用 spawn_blocking，不在 Tokio worker 上做文件解码/zstd。32 个活跃 symbols 的理论 stream buffers 因此约 512 MiB，没有额外全局 buffer cap。

- [ ] **Step 4: Implement logical request scheduling**

使用 tokio::sync::Semaphore(32) 控制 request planning/fill/read 生命周期。base scan shared actor 不重复占用每个 consumer permit；消费者只持自己的 logical permit。fill wait 不占 blocking worker。

每个 request task 的 terminal 必须恰好一次：

~~~rust
enum RequestTerminal {
    Completed(BacktestHistoryRequestReport),
    Failed(BacktestHistoryRequestFailure),
}
~~~

coordinator 收 terminal 后发对应 event 并写 batch report；panic/join error 转 RequestFailed，不能丢 request。

- [ ] **Step 5: Preserve per-request ordering**

Tick chunks 按 (datetime, physical segment rank, tick id)；Kline chunks 按 (datetime, synthetic id)。fan-out aggregator 只在 bar complete 后输出。chunk 边界可变，但拼接后的 row order 必须稳定且无重复。

跨 request 不承诺全局顺序；request_id 是唯一关联键。

- [ ] **Step 6: Implement collect semantics**

collect(self) 要求 run 恰好一个 request，否则 Validation。它按 chunk kind 合并 rows，并在每次 reserve 前检查 collect_limit_bytes；收到 RequestFailed 时 drop 已收 rows并返回 DataError::RequestFailed。

collect_all(self, max_total_bytes) 拒绝 0。每个 request 暂存独立 rows；RequestFailed 立即 drop 对应 partial，其他继续；total capacity 估算超过上限时取消整个 run并返回 CollectLimitExceeded。

- [ ] **Step 7: Implement finish and drop cancellation**

finish(self) drain 未消费 event、await coordinator、返回 report。BacktestHistoryRun::drop：

- drop event receiver。
- decrement所有 request consumer guards。
- 设置 run cancellation。
- 不阻塞等待 join。

coordinator 在最后 consumer 取消后结束；已有其他 shared fill consumers 不受影响。

- [ ] **Step 8: Run async tests under constrained settings**

Run:

~~~bash
rtk cargo test -p tqsdk-data --test backtest_history_async
rtk cargo test -p tqsdk-data --no-default-features --test backtest_history_api
~~~

Expected: all pass；observed logical max=32、blocking max=2，ready request 先完成。

- [ ] **Step 9: Commit**

~~~bash
rtk git add crates/tqsdk-data/src/backtest_history/store_worker.rs crates/tqsdk-data/src/backtest_history/executor.rs crates/tqsdk-data/src/backtest_history/mod.rs crates/tqsdk-data/src/backtest_history/report.rs crates/tqsdk-data/tests/backtest_history_async.rs crates/tqsdk-data/tests/backtest_history_api.rs
rtk git commit -m "feat(data): stream backtest cache queries asynchronously"
~~~

## Task 9: Delegate Facade Backtests and Preserve Compatibility

**Files:**

- Modify: crates/tqsdk/src/backtest_remote.rs
- Delete: crates/tqsdk/src/backtest_history_remote.rs
- Modify: crates/tqsdk/src/lib.rs
- Modify: crates/tqsdk/Cargo.toml
- Modify: crates/tqsdk/tests/facade_contract.rs
- Modify: crates/tqsdk/examples/api_contract_s43_facade_backtest_history_cache.rs
- Modify: crates/tqsdk/examples/api_contract_s44_facade_backtest_remote_on_miss.rs
- Modify: crates/tqsdk/examples/api_contract_s45_facade_backtest_cache_warmup.rs
- Modify: crates/tqsdk-task/src/history_backtest_replay.rs
- Modify: crates/tqsdk-task/src/lib.rs
- Modify: crates/tqsdk-task/tests/history_backtest_replay.rs

- [ ] **Step 1: Run facade/replay impact analysis**

分析 BacktestBuilder::prepare、BacktestBuilder::warmup、PreparedBacktest::connect、HistoryBacktestReplayStream::new_projected。若 public API blast radius为 HIGH/CRITICAL，先报告；兼容类型不得无提示删除。

- [ ] **Step 2: Write failing facade delegation tests**

测试：

- cache-only backtest 的 15s replay 与 BacktestHistoryClient CacheOnly collect 逐 bar 相同。
- 60s 与 5m backtest 使用 canonical minute 与共享 MinuteKlineAggregator。
- RemoteOnMiss facade 通过 data fill seam，不实例化 tqsdk-wait cache-fill mode。
- 现有 BacktestRemoteFillConfig/telemetry handlers 仍从 tqsdk 根导入。
- tqsdk::advanced::data 可导入 BacktestHistoryClient 与 request/report types。
- tqsdk::prelude 不包含 BacktestHistoryClient（compile_fail doc test）。

- [ ] **Step 3: Route prepare/warmup through tqsdk-data**

BacktestBuilder 保留现有用户方法和 BacktestCachePolicy，包括 facade maintenance 的 Refresh；内部把 CacheOnly/RemoteOnMiss requirements 转为 data planner/fill requests。Refresh 仍先走现有显式 purge maintenance，然后用 RemoteOnMiss 补齐；不要把 Refresh 传进 BacktestHistoryClient public query policy。

warmup report 字段继续累加 tick/minute rows 与 remote_used；底层 fill report由 tqsdk-data 返回。

- [ ] **Step 4: Use the shared aggregation kernel in replay**

HistoryBacktestReplayStream 保留同步 BacktestMarketStream contract，继续读取现有 sync cache readers，但：

- 小于 60s 只使用 tqsdk_data::TickKlineAggregator。
- 60s passthrough MinuteKlineReader。
- 大于 60s 只使用 tqsdk_data::MinuteKlineAggregator。
- source-range expansion helper来自 data planner 的 public(crate-to-task 不可用)稳定 public helper或共享 value object，不复制 boundary 算法。

如果需要跨 crate 调用，把最小纯函数作为 tqsdk_data::aggregation::expanded_* public API；不要把整个 query planner公开。

- [ ] **Step 5: Turn facade remote modules into adapters**

backtest_remote.rs 保留 tqsdk root 当前公开类型，通过 pub use 或薄 wrapper 指向 tqsdk-data；handler 回调由 telemetry stream adapter 驱动。删除 backtest_history_remote.rs 后，全仓不得再有 fill_backtest_minute_kline_cache 的第二份实现。

- [ ] **Step 6: Add curated advanced::data exports**

只在 tqsdk::advanced::data 导出：

~~~rust
pub use tqsdk_data::{
    BacktestHistoryBatchReport, BacktestHistoryClient, BacktestHistoryClientBuilder,
    BacktestHistoryEvent, BacktestHistoryKind, BacktestHistoryPolicy,
    BacktestHistoryRequest, BacktestHistoryRequestFailure, BacktestHistoryRequestReport,
    BacktestHistoryRows, BacktestHistoryTelemetryEvent, BacktestHistoryTelemetryStream,
};
~~~

不加入 prelude，不在 tqsdk 根重复实现 client。

- [ ] **Step 7: Run facade and task compatibility tests**

Run:

~~~bash
rtk cargo test -p tqsdk --test facade_contract
rtk cargo test -p tqsdk-task --test history_backtest_replay
rtk cargo check -p tqsdk --examples
rtk cargo check -p tqsdk --no-default-features --examples
~~~

Expected: all pass；S43/S44/S45 examples 保持可编译。

- [ ] **Step 8: Commit**

~~~bash
rtk git add crates/tqsdk/Cargo.toml crates/tqsdk/src/lib.rs crates/tqsdk/src/backtest_remote.rs crates/tqsdk/src/backtest_history_remote.rs crates/tqsdk/tests/facade_contract.rs crates/tqsdk/examples/api_contract_s43_facade_backtest_history_cache.rs crates/tqsdk/examples/api_contract_s44_facade_backtest_remote_on_miss.rs crates/tqsdk/examples/api_contract_s45_facade_backtest_cache_warmup.rs crates/tqsdk-task/src/history_backtest_replay.rs crates/tqsdk-task/src/lib.rs crates/tqsdk-task/tests/history_backtest_replay.rs
rtk git commit -m "refactor(tqsdk): delegate backtest history to data"
~~~

## Task 10: Lock Feature Flags, Retention, and No-CLI Boundaries

**Files:**

- Modify: crates/tqsdk-data/Cargo.toml
- Modify: crates/tqsdk-session/Cargo.toml
- Modify: crates/tqsdk/Cargo.toml
- Modify: crates/tqsdk-data/tests/backtest_history_api.rs
- Modify: crates/tqsdk-data/tests/backtest_history_metadata.rs
- Modify: crates/tqsdk-data/tests/backtest_history_query.rs

- [ ] **Step 1: Write no-cleanup and feature-gate regressions**

测试创建一个时间很早的 Tick daily partition、一个旧 minute monthly partition 与两个 metadata snapshots，然后执行多次 query/build/RemoteOnMiss fake fill。最终断言：

- 旧 Tick 文件存在且 bytes 未变。
- 旧 minute 文件存在且 bytes 未变。
- 旧 metadata snapshot 存在。
- 没有基于 mtime、age、max bytes 的隐式 purge。
- CacheOnly local query 在 no-default-features 下通过。
- RemoteOnMiss 完整命中在 no-default-features 下通过。
- RemoteOnMiss 缺口在 no-default-features 下返回 feature-disabled error，而不是 auth error。

- [ ] **Step 2: Run tests before finalizing features**

Run:

~~~bash
rtk cargo test -p tqsdk-data --no-default-features --test backtest_history_api
rtk cargo test -p tqsdk-data --no-default-features --test backtest_history_query
rtk cargo test -p tqsdk-data --no-default-features --test backtest_history_metadata
~~~

Expected: all pass after earlier tasks；若 remote module意外无条件编译，此处会暴露。

- [ ] **Step 3: Make the feature graph explicit**

tqsdk-data：

~~~toml
[features]
default = ["live", "services", "tqbn-zstd"]
live = ["tqsdk-session/live"]
services = ["dep:reqwest", "tqsdk-session/services"]

[dependencies]
futures.workspace = true
serde.workspace = true
sha1.workspace = true
~~~

local request/planner/metadata/aggregation/executor 不加 cfg。production fill connector使用 cfg(all(feature = "live", feature = "services"))。tqsdk 的 live/services 继续向 data/session 传播，不新增默认依赖 tqsdk-wait。

- [ ] **Step 4: Prove no CLI surface was added**

不修改 crates/tqsdk-cache/src 的 command enum/parser。用 public-surface test 断言只有 library client/example；文档将 tqsdk-cache query 标为本轮非目标，不能留下半成品 flag。

- [ ] **Step 5: Run feature matrix**

Run:

~~~bash
rtk cargo check -p tqsdk-data --no-default-features
rtk cargo check -p tqsdk-data --no-default-features --examples
rtk cargo test -p tqsdk-session --no-default-features
rtk cargo check -p tqsdk --no-default-features --examples
rtk cargo check -p tqsdk-data --all-features --examples
~~~

Expected: all exit 0。

- [ ] **Step 6: Commit**

~~~bash
rtk git add crates/tqsdk-data/Cargo.toml crates/tqsdk-data/tests/backtest_history_api.rs crates/tqsdk-data/tests/backtest_history_metadata.rs crates/tqsdk-data/tests/backtest_history_query.rs crates/tqsdk-session/Cargo.toml crates/tqsdk/Cargo.toml
rtk git commit -m "test(data): lock backtest history feature boundaries"
~~~

## Task 11: Benchmark the Blocking Store Path and Run the Native-Async Reader Spike

**Files:**

- Create: crates/tqsdk-data/examples/backtest_history_query_bench.rs
- Create: crates/tqsdk-data/src/backtest_history/native_async_probe.rs
- Create: docs/reviews/backtest-history-native-async-spike.md
- Modify: Cargo.toml
- Modify: crates/tqsdk-data/Cargo.toml
- Modify: crates/tqsdk-data/src/backtest_history/mod.rs
- Modify: crates/tqsdk-data/src/backtest_history/store_worker.rs

- [ ] **Step 1: Add a reproducible local benchmark dataset**

benchmark 接受：

~~~text
--cache-dir <path>
--mode sync-baseline|async-blocking|native-async
--concurrency 1|32
--path cold|warm
--iterations <n>
~~~

默认使用 deterministic generated fixtures：8 symbols、6 个完整月 minute rows，以及足够覆盖 15s 查询的 Tick partitions。generation 是显式 --prepare，不在测量区间。性能阶段设置 BacktestHistoryPolicy::CacheOnly，若 telemetry 出现 Fill/Retry 立即失败。

- [ ] **Step 2: Measure the current sync reader baseline and async-blocking path**

每种组合至少 10 iterations，记录：

- total rows/s。
- end-to-end completion time p50/p95。
- peak RSS。
- CPU time。
- read/decompress worker utilization。

Run:

~~~bash
rtk cargo run -p tqsdk-data --release --example backtest_history_query_bench -- --cache-dir /tmp/tqsdk-history-bench --prepare
rtk cargo run -p tqsdk-data --release --example backtest_history_query_bench -- --cache-dir /tmp/tqsdk-history-bench --mode sync-baseline --concurrency 1 --path warm --iterations 10
rtk cargo run -p tqsdk-data --release --example backtest_history_query_bench -- --cache-dir /tmp/tqsdk-history-bench --mode async-blocking --concurrency 1 --path warm --iterations 10
rtk cargo run -p tqsdk-data --release --example backtest_history_query_bench -- --cache-dir /tmp/tqsdk-history-bench --mode async-blocking --concurrency 32 --path cold --iterations 10
rtk cargo run -p tqsdk-data --release --example backtest_history_query_bench -- --cache-dir /tmp/tqsdk-history-bench --mode async-blocking --concurrency 32 --path warm --iterations 10
~~~

Expected: benchmark prints machine-readable summary；任何路径 remote_used=false。

- [ ] **Step 3: Add the isolated native-async probe**

增加 non-default feature native-async-spike，并只在该 feature 下启用 tokio fs。probe 从固定 file handle 使用 tokio::fs::File 与 AsyncReadExt；解压/record decode 若仍是 CPU blocking，则放入同一个有界 CPU semaphore，不能假装 async。

共享测试 seam：

~~~rust
trait PartitionChunkSource: Send {
    fn next_chunk<'a>(
        &'a mut self,
        target_bytes: usize,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<BacktestHistoryRows>> + Send + 'a>,
    >;
}
~~~

AsyncBlockingChunkSource 包装 Task 8 的 scan actor；NativeAsyncProbeChunkSource 只用于 benchmark，功能测试必须逐行与 sync reader 相等。

- [ ] **Step 4: Run the same 1/32 cold/warm matrix**

Run:

~~~bash
rtk cargo run -p tqsdk-data --release --features native-async-spike --example backtest_history_query_bench -- --cache-dir /tmp/tqsdk-history-bench --mode native-async --concurrency 1 --path warm --iterations 10
rtk cargo run -p tqsdk-data --release --features native-async-spike --example backtest_history_query_bench -- --cache-dir /tmp/tqsdk-history-bench --mode native-async --concurrency 32 --path cold --iterations 10
rtk cargo run -p tqsdk-data --release --features native-async-spike --example backtest_history_query_bench -- --cache-dir /tmp/tqsdk-history-bench --mode native-async --concurrency 32 --path warm --iterations 10
~~~

- [ ] **Step 5: Apply the pre-agreed decision gate**

迁移 production store trait 仅当同时满足：

- 32 concurrency total throughput 相对 async-blocking 提升至少 20%。
- 32 concurrency completion p95 改善至少 20%。
- 单 query throughput/完成时间退化不超过 5%。
- peak RSS 不超过配置缓冲预算加 25%。
- cold 与 warm 都没有 correctness mismatch。

若全部满足：把 NativeAsyncProbeChunkSource 改名 NativeAsyncChunkSource，移除 feature gate与 AsyncBlockingChunkSource production 选择，保留 sync public reader compatibility wrapper。

若任一不满足：从 production module 移除 native_async_probe 与 feature，Tokio fs 若无其他使用也移除；保留 Task 8 的有限 blocking workers。

两种结果都必须在 docs/reviews/backtest-history-native-async-spike.md 写入实际数值、机器信息、判定式与最终唯一 production path，不保留运行时 backend 开关。

- [ ] **Step 6: Verify the chosen path**

Run:

~~~bash
rtk cargo test -p tqsdk-data --test backtest_history_async
rtk cargo test -p tqsdk-data --test backtest_history_query
rtk cargo check -p tqsdk-data --no-default-features
~~~

Expected: all pass；单 query 相对 sync baseline不超过 5%退化，RSS 在门槛内。

- [ ] **Step 7: Commit**

~~~bash
rtk git add Cargo.toml crates/tqsdk-data/Cargo.toml crates/tqsdk-data/src/backtest_history/mod.rs crates/tqsdk-data/src/backtest_history/store_worker.rs crates/tqsdk-data/examples/backtest_history_query_bench.rs docs/reviews/backtest-history-native-async-spike.md
~~~

若 gate 通过且 native_async_probe.rs 被保留，在 commit 前额外运行：

~~~bash
rtk git add crates/tqsdk-data/src/backtest_history/native_async_probe.rs
~~~

然后提交：

~~~bash
rtk git commit -m "perf(data): validate async backtest history storage"
~~~

## Task 12: Validate Against Official Server-Backtest Klines

**Files:**

- Create: crates/tqsdk-data/tests/backtest_history_live.rs
- Modify: crates/tqsdk-data/tests/support/backtest_history.rs
- Modify: docs/architecture/validation.md

- [ ] **Step 1: Add ignored, environment-gated live tests**

所有 live tests：

~~~rust
#[tokio::test]
#[ignore = "requires TQ_AUTH_* and official server-backtest network access"]
async fn kqi_au_six_complete_months_matches_server_oracle() {
    let user = std::env::var("TQ_AUTH_USER").expect("TQ_AUTH_USER is required");
    let pass = std::env::var("TQ_AUTH_PASS").expect("TQ_AUTH_PASS is required");
    // The test never prints either value.
}
~~~

测试不提交真实行情 fixture，不打印认证，不连接交易 route，不产生下单/撤单。

- [ ] **Step 2: Compute the exact validation window**

以运行时 CST 日期为基准，end 是当前月 1 日 00:00 CST，start 是向前六个月的 1 日 00:00 CST。例如 2026-07-30 执行时窗口是 [2026-01-01 00:00 CST, 2026-07-01 00:00 CST)。不得使用“180 天”近似。

- [ ] **Step 3: Compare KQ.i@SHFE.au at all required durations**

周期固定：

~~~rust
const DURATIONS: [Duration; 6] = [
    Duration::from_secs(15),
    Duration::from_secs(60),
    Duration::from_secs(5 * 60),
    Duration::from_secs(15 * 60),
    Duration::from_secs(30 * 60),
    Duration::from_secs(60 * 60),
];
~~~

本地侧：

- 15s 只从 Tick cache/query 聚合。
- 60s 直接从 minute cache。
- 其余只从同一 60s scan 聚合。

oracle 侧使用 Task 5 的官方 ServerBacktestHistoryStream 直接请求对应 duration 的服务端 K 线；若 substrate production contract只允许 Tick/60s，则 test helper可用 SessionClient::ensure_chart 构造任意 duration chart，但不得走专业 DataClient download。

- [ ] **Step 4: Stream-compare without six-month materialization**

两个有序 stream做 merge comparison。每个 duration 统计：

- local/server bar count。
- datetime mismatch。
- open/high/low/close mismatch。
- volume mismatch。
- open_oi/close_oi mismatch。

价格先按 symbol metadata 的 price precision canonicalize；其他字段精确比较。验收条件全部为 0 mismatch。错误输出只展示前 20 个差异及总计，避免日志爆炸。

此前已有 5m=12,834、15m=4,278、30m=2,198、60m=1,274 且零 mismatch 的观察值只作为历史参考，不硬编码为未来月份断言。

- [ ] **Step 5: Add a real KQ.m rollover oracle**

查询 KQ.m@SHFE.au active metadata，在最近六个月 physical_segments 中找到第一个 underlying 变化边界；取边界前后各三个完整交易日。断言：

- CacheOnly 在断网/fake remote panic provider 下仍能解析 persisted mapping。
- Tick 唯一 key 使用 (physical_symbol, tick_id)。
- local 15s/60s/5m 与 server logical KQ.m oracle 零字段 mismatch。
- report physical_segments 与 active snapshot hash准确。

如果最近六个月没有 segment change，test 失败并打印 metadata segments；不得静默跳过。

- [ ] **Step 6: Add the 32-way mixed concurrency acceptance**

8 个指数合约固定候选：

~~~text
KQ.i@SHFE.au
KQ.i@SHFE.ag
KQ.i@SHFE.cu
KQ.i@SHFE.rb
KQ.i@DCE.i
KQ.i@DCE.m
KQ.i@CZCE.SR
KQ.i@CFFEX.IF
~~~

每个请求 5m/15m/30m/60m，共 32 requests。先完成 remote warmup，然后 CacheOnly、禁止联网运行 cold/warm performance acceptance。断言 32 个 RequestCompleted、0 RequestFailed、每个 report remote_used=false。

- [ ] **Step 7: Run the exact live acceptance commands**

TQ_AUTH_* 已由本地环境全局注入，命令中不展开值：

~~~bash
rtk cargo test -p tqsdk-data --features live,services --test backtest_history_live kqi_au_six_complete_months_matches_server_oracle -- --ignored --nocapture
rtk cargo test -p tqsdk-data --features live,services --test backtest_history_live kqm_rollover_matches_server_oracle -- --ignored --nocapture
rtk cargo test -p tqsdk-data --features live,services --test backtest_history_live mixed_32_way_cache_only_acceptance -- --ignored --nocapture
~~~

Expected:

~~~text
all requested durations: count/datetime/OHLC/volume/open_oi/close_oi mismatches = 0
32 completed, 0 failed, remote_used = false during measured runs
~~~

- [ ] **Step 8: Commit**

~~~bash
rtk git add crates/tqsdk-data/tests/backtest_history_live.rs crates/tqsdk-data/tests/support/backtest_history.rs docs/architecture/validation.md
rtk git commit -m "test(data): verify local klines against server backtest"
~~~

## Task 13: Synchronize Architecture, API, and User Documentation

**Files:**

- Modify: README.md
- Modify: docs/README.md
- Modify: docs/architecture/ai-workflow.md
- Modify: docs/architecture/README.md
- Modify: docs/architecture/crate-boundaries.md
- Modify: docs/architecture/api-data.md
- Modify: docs/architecture/api-task.md
- Modify: docs/architecture/validation.md
- Modify: crates/tqsdk-data/README.md
- Modify: crates/tqsdk-session/README.md
- Modify: crates/tqsdk-task/README.md
- Modify: crates/tqsdk/README.md
- Modify: crates/tqsdk-data/examples/api_contract_s48_backtest_history_query.rs

- [ ] **Step 1: Document the user path with executable code**

S48 example 显示：

~~~rust
let client = BacktestHistoryClient::builder(cache_dir)
    .policy(BacktestHistoryPolicy::RemoteOnMiss)
    .auth_env()
    .build()?;

let requests = [
    BacktestHistoryRequest::tick(1, "KQ.i@SHFE.au", start_ns, end_ns),
    BacktestHistoryRequest::kline(
        2,
        "KQ.i@SHFE.au",
        Duration::from_secs(15),
        start_ns,
        end_ns,
    ),
    BacktestHistoryRequest::kline(
        3,
        "KQ.i@SHFE.au",
        Duration::from_secs(5 * 60),
        start_ns,
        end_ns,
    ),
];
let mut run = client.query_batch(requests).await?;
while let Some(event) = run.next().await {
    match event {
        BacktestHistoryEvent::Chunk(chunk) => consume(chunk),
        BacktestHistoryEvent::RequestCompleted(report) => log_complete(report),
        BacktestHistoryEvent::RequestFailed(failure) => log_failure(failure),
    }
}
let report = run.finish().await;
~~~

example 不调用 collect_all；另用注释明确 batch materialization 必须给出 max_total_bytes。

- [ ] **Step 2: Update crate ownership**

权威文档明确：

- core 不拥有查询/缓存/聚合。
- session 只拥有 server-backtest connection/history chart substrate，不拥有 coverage、文件或 query policy。
- data 拥有 BacktestHistoryClient、metadata、single-flight、fill、cache readers、aggregation。
- task 拥有 replay/backtest event semantics，消费 data 聚合器。
- wait 保留 single-owner Python-style strategy pump，不再作为 data fill 依赖。
- tqsdk 只委托并 curated re-export。

- [ ] **Step 3: Document source/cache matrix**

加入表格：

| 用户请求 | Durable source | Derived | Provisional |
| --- | --- | --- | --- |
| Tick | Tick daily TQBN v2 | no | explicit only |
| 15s / <60s | Tick daily TQBN v2 | in memory | explicit only |
| 60s | minute monthly v4 | no | never |
| N×60s >60s | minute monthly v4 | in memory | never |

同时写明 Tick/minute 无自动清理，derived 不落盘，61s/90s拒绝。

- [ ] **Step 4: Document API and failure semantics**

api-data.md 与 tqsdk-data README 覆盖：

- request id/chunks/terminal events/finish。
- collect 512 MiB 默认与 collect_all 显式上限。
- chunks provisional until RequestCompleted。
- batch failure isolation。
- lazy auth 与 no-default-features behavior。
- KQ.m sidecar/offline mapping 与 snapshot hash。
- current-day finality。

- [ ] **Step 5: Update validation commands**

validation.md 加入 Task 10 feature matrix、Task 11 benchmark门槛、Task 12 三条 ignored live commands。CI matrix只运行 synthetic fixtures；live comparison 保持 ignored。

- [ ] **Step 6: Run documentation and example checks**

Run:

~~~bash
rtk cargo check -p tqsdk-data --example api_contract_s48_backtest_history_query
rtk cargo check -p tqsdk-data --no-default-features --example api_contract_s48_backtest_history_query
rtk git diff --check README.md docs crates/tqsdk-data/README.md crates/tqsdk-session/README.md crates/tqsdk-task/README.md crates/tqsdk/README.md
~~~

Expected: commands exit 0；diff check无输出。

- [ ] **Step 7: Commit**

~~~bash
rtk git add README.md docs/README.md docs/architecture/ai-workflow.md docs/architecture/README.md docs/architecture/crate-boundaries.md docs/architecture/api-data.md docs/architecture/api-task.md docs/architecture/validation.md crates/tqsdk-data/README.md crates/tqsdk-session/README.md crates/tqsdk-task/README.md crates/tqsdk/README.md crates/tqsdk-data/examples/api_contract_s48_backtest_history_query.rs
rtk git commit -m "docs: define backtest history query architecture"
~~~

## Task 14: Final Self-Review and Workspace Verification

**Files:**

- Verify all files changed by Tasks 1-13

- [ ] **Step 1: Check spec coverage**

逐项映射本计划 Scope Check 与 Locked Semantics：

- API/stream/batch/report：Tasks 1、8。
- 15s 与官方聚合：Tasks 2、7、12。
- 60s-only persistence：Tasks 2、7、13。
- metadata/KQ.m offline：Tasks 4、7、12。
- async/single-flight/cancel：Tasks 6、8。
- stable locks/corruption：Task 3。
- facade/shared kernel：Task 9。
- performance/native-async gate：Task 11。
- no cleanup/no CLI/features：Task 10。
- architecture docs：Task 13。

任何没有对应测试的条目必须在相应 task 增加测试后再继续。

- [ ] **Step 2: Scan plan and code for unresolved marker words**

Run:

~~~bash
rtk grep -n "T[B]D|T[O]DO|implement l[a]ter|similar t[o] Task" docs/superpowers/plans/2026-07-30-backtest-history-query.md crates/tqsdk-data/src/backtest_history crates/tqsdk-session/src/backtest_history.rs
~~~

Expected: no matches。

- [ ] **Step 3: Check type-name consistency**

Run:

~~~bash
rtk grep -n "BacktestHistory(Client|Request|Event|Run|BatchReport|RequestReport|RequestFailure|Finality|Policy)" crates/tqsdk-data/src crates/tqsdk-data/tests crates/tqsdk/src
~~~

Expected: names match Public API Contract；没有额外的同义 client 名称。

- [ ] **Step 4: Format and run focused tests**

Run:

~~~bash
rtk cargo fmt --all
rtk cargo fmt --all --check
rtk cargo test -p tqsdk-data --tests
rtk cargo test -p tqsdk-session --test server_backtest_history
rtk cargo test -p tqsdk-task --test history_backtest_replay
rtk cargo test -p tqsdk --test facade_contract
~~~

Expected: all exit 0。

- [ ] **Step 5: Run full feature and lint verification**

Run:

~~~bash
rtk cargo test
rtk cargo test --all-features
rtk cargo check --examples
rtk cargo check --no-default-features
rtk cargo check --no-default-features --examples
rtk cargo clippy --examples --all-targets -- -D warnings
rtk /usr/bin/env RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features
rtk git diff --check
~~~

Expected: all exit 0；RUSTDOCFLAGS=-D warnings 的 release check 也必须通过。

- [ ] **Step 6: Confirm measured performance and memory gates**

核对 docs/reviews/backtest-history-native-async-spike.md：

- 单 query 相对 sync reader baseline退化不超过 5%。
- 32-way chosen path的 throughput/p95结论有实际数值。
- peak RSS <= configured buffers × 1.25。
- measured path CacheOnly 且无网络。
- production 只保留一个 storage execution path。

- [ ] **Step 7: Run GitNexus change detection before the final commit**

Run:

~~~bash
rtk gitnexus detect-changes
~~~

Expected: affected scope仅包含 session server-backtest substrate、data history/cache/aggregation/query、task replay compatibility、tqsdk facade delegation、tests/examples/docs；没有 core runtime contract 或 relay/dashboard flow。

- [ ] **Step 8: Review the final diff and commit**

Run:

~~~bash
rtk git status --short
rtk git diff --stat
rtk git diff --check
~~~

Expected: only authorized files；无 secrets、真实行情 fixtures 或自动生成 cache files进入 diff。

Tasks 1-13 已按独立边界提交；本步不执行宽泛的 aggregate git add。若验证产生修复，只暂存该修复的精确路径并使用与所属 task 对应的 commit message。

## Rollback Boundaries

- 任一新 query failure 不得破坏现有 BacktestTickCache::load_series 或 MinuteKlineCache::open_reader/read_range；它们是同步兼容底座。
- native-async spike 未过门槛时只移除 probe，保留完整 async orchestration + bounded blocking workers。
- facade 委托若暴露无法接受的兼容风险，可暂时保留薄 adapter，但不得恢复 tqsdk-wait 作为 tqsdk-data 依赖，也不得复制聚合器。
- metadata sidecar损坏时 fail closed；回滚不得通过删除 sidecar“自愈”。
- 远端 fill失败或取消时可以保留 partial Tick rows，但不得补写 final coverage。
- 任何阶段都不得回退到专业历史下载接口作为回测缓存缺口来源。
