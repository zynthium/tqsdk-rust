//! Scenario: 看盘软件历史序列缓存
//!
//! Primary user layer:
//! - 看盘软件 / 交易终端用户
//! - 研究 / 回放用户
//!
//! Intended crate path:
//! - `tqsdk-data`
//!
//! Lower-level escape hatch:
//! - `DataClient::{get_kline_data_series,get_tick_data_series,kline_data_download,tick_data_download}`
//! - `MarketCacheEvent` / `MarketCacheReplay` for replay-oriented event cache
//!
//! Non-goal:
//! - live quote/tick hot path
//! - 高频交易柜台决策链路
//! - GUI viewport/state management
//! - 默认全局隐藏磁盘缓存
//! - 跨进程行情中台或 cache service
//!
//! User goal:
//! - 看盘软件启动后能快速加载最近 K 线 / tick 历史窗口
//! - 图表缩放、拖拽和回看时只补齐缺失历史区间
//! - 同一合约周期的重复打开不反复全量下载
//! - 历史缓存能输出现有 `KlineDataSeries` / `TickDataSeries`
//! - 缓存损坏、schema 变化或未完成写入有 typed report，而不是静默读坏数据
//!
//! API contract:
//! - 历史序列缓存是显式 opt-in object，不改变 `get_*_data_series` 默认无缓存语义
//! - cache key 包含 symbol、duration、payload kind、schema version、source/route 和
//!   调整参数
//! - cache 能计算已有 range、请求 range 和缺失 range，并复用 `data_download` 补齐
//! - mutable tail 需要可配置重验策略，不能把未收盘 K 线永久当成稳定历史
//! - 写入采用 temp segment + atomic publish + manifest update
//! - 读取输出 typed series 或 typed row view，不要求用户解析 JSONL / CSV / 文件名
//! - mmap / memmap 只能是后续实现策略或可选 feature，不是 public contract 前提
//! - 不要求用户手写 channel、lock 文件或 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - `tqsdk-core` / `tqsdk-session` / `tqsdk-wait` / `tqsdk-stream` 拥有历史文件缓存
//! - `TqApi::get_kline_serial` 或 `TqStream` live window 依赖 data cache
//! - `get_*_data_series` 默认读写用户全局目录
//! - GUI 组件状态、缩放窗口或图表渲染对象进入 SDK
//! - 用户必须手写 cache 文件格式、manifest、range 合并或损坏恢复
//! - 把该缓存放进高频交易 hot path
//!
//! Regression signal:
//! - 看盘软件每次启动或切换周期都全量重新下载历史窗口
//! - 图表向左回看需要业务代码手写分页、落盘和 range 合并
//! - 缓存中断写入后下次读取静默返回半截数据
//! - 历史序列缓存污染 live runtime revision / commit 语义
//! - mmap 实现细节被提前冻结为稳定 public API
//!
//! Review questions:
//! - 历史序列缓存是否应作为 `tqsdk-data` 的 materialization/cache foundation？
//! - 是否保持显式 opt-in，而不是改变现有 `DataClient` 默认行为？
//! - manifest、schema version、source/route 和 mutable tail policy 是否足以避免缓存误读？
//! - mmap 是否应推迟到 feature-gated backend，而不是第一版 public contract？
//!
//! Current API gap:
//! `tqsdk-data` 已有 history page / series / download、CSV export、JSONL
//! `MarketCache*` event cache 和 history series -> replay adapter。但当前没有
//! 面向看盘软件的 typed history series range cache：用户若要复用本地历史窗口，
//! 仍需自己管理路径、range coverage、缺口下载、segment publish、manifest、
//! schema version、mutable tail 重验和损坏恢复。
//!
//! 理想用户代码草案：
//! ```ignore
//! let cache = HistorySeriesCache::open("./cache/history")?
//!     .tail_refresh_policy(TailRefreshPolicy::refresh_last_bars(2))
//!     .max_segment_rows(200_000)
//!     .verify_schema_on_open(true);
//!
//! let series = cache
//!     .get_kline_series(
//!         &client,
//!         KlineDataSeriesRequest::new(
//!             "SHFE.au2602",
//!             Duration::from_secs(60),
//!             start_ns,
//!             end_ns,
//!         ),
//!     )
//!     .await?;
//!
//! println!(
//!     "symbol={} rows={} cache_hit={} downloaded_ranges={:?}",
//!     series.symbol(),
//!     series.len(),
//!     series.cache_report().hit_rows(),
//!     series.cache_report().downloaded_ranges()
//! );
//! ```

fn main() {}
