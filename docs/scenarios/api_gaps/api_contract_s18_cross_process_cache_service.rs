//! Scenario: 本地行情缓存跨进程管理服务（剩余 API gap）
//!
//! User goal:
//! - 一个或多个实盘进程把标准行情事件写入本地共享缓存
//! - 多个研究进程、回放进程或策略进程读取同一缓存
//! - writer 崩溃、进程重启、compaction 和 reader checkpoint 不破坏缓存一致性
//! - 不要求用户理解 lock lease 文件、queue rotation 或 reader manifest
//!
//! API contract:
//! - 当前 `tqsdk-data` 已承诺同步本地 file service facade 与 process-local
//!   daemon / supervisor foundation
//! - 完整跨进程 daemon orchestration 必须显式组合 writer election、
//!   lease ownership、recovery action、compaction ownership 和 shutdown report
//! - service 不拥有 live session；live attach 只能作为 `tqsdk-stream` 到 cache 的 adapter
//! - service 不内置 HTTP health endpoint、GUI 或系统级进程管理器
//! - 不要求用户手写 Tokio task、channel 或 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - 用户手写 lock lease 文件格式
//! - 用户手写 reader checkpoint / manifest 文件格式
//! - 用户手写 processing queue recovery / compaction ownership
//! - provider 私有 protocol type
//! - 把 cache service 下沉到 `tqsdk-core` / `tqsdk-session`
//!
//! Regression signal:
//! - 多进程共享只能靠用户拼接 `MarketCacheLock`、`MarketCacheQueue` 和
//!   `MarketCacheCompaction`
//! - writer 崩溃后必须人工判断 queue / processing / cache 哪个文件可信
//! - compaction 可能删除仍被 reader checkpoint 依赖的数据
//! - live stream adapter 反向拥有 session 或 runtime state
//!
//! Review questions:
//! - 跨进程 cache service 应继续落在 `tqsdk-data`，还是拆成独立 tooling crate？
//! - writer election、recovery action、compaction ownership 和 service facade 是否必须一起冻结？
//! - service 的 public report 能否表达 crash recovery 与 reader lag，而不暴露文件细节？
//!
//! Current API note:
//! `MarketCacheWriter` / `MarketCacheReader` / `MarketCacheReplay` 已提供离线
//! cache record、JSONL reader/writer 和 ordered replay；`MarketCacheStreamWriter`
//! 已提供单进程 live stream pipe；`MarketCacheQueue` / `MarketCacheLock` /
//! `MarketCacheIndex` / `MarketCacheCompaction` 已提供本地 queue、lock、index 和
//! compaction foundation；`MarketCacheDaemon` / `MarketCacheSupervisor` 已提供
//! process-local flush、lease renewal 和 graceful shutdown foundation；
//! `MarketCacheReaderManifest` 已提供本地 reader checkpoint、compaction floor 和
//! typed reader lag report foundation；`MarketCacheRecoveryScan` 已提供本地
//! cache / queue / processing queue / compaction staging recovery scan foundation；
//! `MarketCacheWriterElection` / `MarketCacheWriterLease` /
//! `MarketCacheRecoveryAction` 已提供本地 writer election、lease ownership 和
//! queue recovery action foundation；`MarketCacheCompactionOwnership` 已提供
//! reader-protected compaction ownership foundation；`MarketCacheService` 已提供
//! 同步、本地 file service facade foundation，组合 writer election、recovery、
//! reader checkpoint、queue flush 和 reader-protected compaction。
//!
//! 这些 API 已覆盖本地 file service foundation，但还不能自然表达完整跨进程
//! daemon orchestration / 多进程 cache 管理服务。
//!
//! 理想用户代码草案：
//! ```ignore
//! let service = CrossProcessMarketCacheService::builder("./cache/market")
//!     .writer_id("prod-market-writer-1")
//!     .lease_timeout(Duration::from_secs(30))
//!     .flush_interval(Duration::from_millis(200))
//!     .reader_retention(MarketCacheReaderRetention::keep_until_all_checkpointed())
//!     .recovery_policy(MarketCacheRecoveryPolicy::scan_and_resume())
//!     .build()?;
//!
//! let writer = service.claim_writer().await?;
//! writer
//!     .attach_stream(
//!         stream
//!             .market_events()
//!             .quotes(["SHFE.au2602", "DCE.m2601"]),
//!     )
//!     .await?;
//!
//! let reader = service
//!     .reader("research-job-20260429")
//!     .symbol("SHFE.au2602")
//!     .from_checkpoint("last-close-study")
//!     .build()
//!     .await?;
//!
//! for event in reader.replay() {
//!     strategy.on_market(event?)?;
//! }
//!
//! let report = service.shutdown().await?;
//! assert!(report.queue_drained());
//! assert!(report.active_readers().is_empty());
//! ```
//!
//! 期望的最小 public report：
//! ```ignore
//! struct MarketCacheServiceReport {
//!     writer: MarketCacheWriterLeaseReport,
//!     recovery: MarketCacheRecoveryReport,
//!     readers: Vec<MarketCacheReaderLag>,
//!     compaction: Option<MarketCacheAtomicCompactionReport>,
//!     shutdown: MarketCacheSupervisorShutdownReport,
//! }
//! ```
//!
//! 建议迭代切分：
//! 1. reader manifest / checkpoint substrate 已作为本地 data-layer helper 落地，
//!    不改变现有 writer API。
//! 2. recovery scan 已作为本地 data-layer helper 落地，能识别 cache / queue /
//!    processing / compact staging 的可恢复状态，并输出 typed report。
//! 3. writer election / recovery action 已作为本地 data-layer helper 落地，能在
//!    明确 lease ownership 下恢复 processing queue / queue。
//! 4. compaction ownership 已作为本地 data-layer helper 落地，能在 writer lease
//!    下结合 reader manifest floor 运行 atomic compaction。
//! 5. 本地 file service facade 已落地，能同步组合 writer election、recovery、
//!    reader manifest、queue flush 与 reader-protected compaction ownership。
//! 6. 下一步若继续推进，应设计跨进程 daemon orchestration / 管理服务层，
//!    避免把 live session、HTTP endpoint、GUI 或系统进程管理器下沉到 data/core/session。

fn main() {}
