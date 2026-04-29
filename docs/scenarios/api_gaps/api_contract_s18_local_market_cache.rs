//! Scenario: 本地行情缓存读写
//!
//! User goal:
//! - 实盘进程写本地行情缓存
//! - 其他进程 / 策略读取缓存
//! - 缓存数据仍有标准 schema 和时间顺序
//!
//! API contract:
//! - cache writer/reader 是明确 public API
//! - 写入不拖慢核心行情循环
//! - 读取端不依赖 live session
//! - 不手动创建 channel
//! - 不手动使用 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - 用户自己定义缓存文件格式
//! - 业务代码直接写 state tree dump
//! - provider 私有 protocol type
//! - cache 读写污染 `tqsdk-core` / `tqsdk-session`
//!
//! Regression signal:
//! - 多进程共享只能靠用户手写 CSV/JSON
//! - cache replay 无法复用标准 market event
//! - 写库慢消费者反压 live session
//!
//! Review questions:
//! - 当前 API 是否自然表达本地缓存？
//! - 缓存职责应落在 `tqsdk-data` 还是新 crate？
//! - 是否存在热路径性能风险？
//!
//! API gap:
//! `tqsdk-data` 已提供离线 cache record、JSONL reader/writer 和 ordered
//! replay iterator；`MarketCacheStreamWriter` 已提供单进程 live `MarketEvent`
//! -> cache writer pipe foundation；`MarketCacheQueue` / `MarketCacheLock` /
//! `MarketCacheIndex` / `MarketCacheCompaction` 已提供本地 queue、lock file、
//! index 和保留策略 compaction foundation；`MarketCacheDaemon` 已提供同步、
//! process-local daemon foundation，覆盖 stale lease recovery、queue flush
//! report、in-place rotation 和 shutdown report；`MarketCacheSupervisor` 已提供
//! process-local background supervisor foundation，覆盖 periodic rotating flush、
//! lease renewal 和 graceful shutdown report；`MarketCacheReaderManifest` 已提供
//! 本地 reader checkpoint、compaction floor 和 reader lag report foundation。
//! 剩余 gap 是跨进程 cache 管理服务。
//! 更完整的 desired API sketch 见
//! `api_contract_s18_cross_process_cache_service.rs`。
//!
//! 理想用户代码草案：
//! ```ignore
//! let mut cache = MultiProcessMarketCacheService::new("./cache.tqcache")
//!     .queue("./cache.queue")
//!     .coordinate_process_readers()
//!     .build()
//!     .await?;
//! cache.attach(stream.market_events().quote("SHFE.au2602")).await?;
//!
//! let reader = MarketCache::open_reader("./cache.tqcache").await?;
//! let mut replay = reader.replay().symbol("SHFE.au2602").build().await?;
//! while let Some(event) = replay.next().await.transpose()? {
//!     strategy.on_market(event)?;
//! }
//! ```

fn main() {}
