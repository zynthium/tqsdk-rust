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
//! replay iterator。剩余 gap 是 live market stream pipe、可靠 sink runtime、
//! 跨进程锁/index、cache compaction 和将 cache replay 接入 `StrategyHost`
//! 同构 context。
//!
//! 理想用户代码草案：
//! ```ignore
//! let writer = MarketCache::open_writer("./cache.tqcache").await?;
//! stream.market_events().quote("SHFE.au2602").pipe_to_cache(writer).await?;
//!
//! let reader = MarketCache::open_reader("./cache.tqcache").await?;
//! let mut replay = reader.replay().symbol("SHFE.au2602").build().await?;
//! while let Some(event) = replay.next().await.transpose()? {
//!     strategy.on_market(event)?;
//! }
//! ```

fn main() {}
