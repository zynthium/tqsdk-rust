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
//! - 跨进程行情中台或 cache service
//! - Python 与 Rust SDK 同时写同一缓存目录
//!
//! User goal:
//! - 看盘软件启动后能快速加载最近 K 线 / tick 历史窗口
//! - 图表缩放、拖拽和回看时只补齐缺失历史区间
//! - 同一合约周期的重复打开不反复全量下载
//! - 历史缓存能输出现有 `KlineDataSeries` / `TickDataSeries`
//! - 缓存损坏、schema 变化或未完成写入有 typed report，而不是静默读坏数据
//!
//! API contract:
//! - `DataClient::from_session(...)` 默认不启用历史序列缓存
//! - `DataClientBuilder::history_cache_enabled(true)` 显式开启缓存
//! - 开启缓存后，原 `get_kline_data_series` / `get_tick_data_series` 隐式读写缓存
//! - 未设置目录时使用 Python 兼容默认目录 `~/.tqsdk/data_series_1`
//! - `DataClientBuilder::history_cache_dir(...)` 可以覆盖缓存目录
//! - 首版缓存 backend 是 mmap，文件名和二进制列布局兼容 Python 官方 `DataSeries`
//! - cache miss 使用官方 `DataSeries` 同类 `set_chart` 接口和差量算法：
//!   首包 `focus_datetime=start_dt`、`focus_position=0`、`view_width=2000`，
//!   后续按 `left_kline_id=current_id` 翻页，结束后空 `ins_list` 释放 chart
//! - mutable tail 按官方行为重验最后一个 datetime range，不能把未收盘 K 线永久当成稳定历史
//! - 写入采用 temp segment + atomic publish，并在读前合并相邻/尾部重复 segment
//! - 读取输出 typed series 或 typed row view，不要求用户解析 JSONL / CSV / 文件名
//! - 不要求用户手写 channel、lock 文件或 `Arc<Mutex<_>>`
//!
//! Forbidden:
//! - `tqsdk-core` / `tqsdk-session` / `tqsdk-wait` / `tqsdk-stream` 拥有历史文件缓存
//! - `TqApi::get_kline_serial` 或 `TqStream` live window 依赖 data cache
//! - 未经 builder/config opt-in 时 `get_*_data_series` 默认读写用户全局目录
//! - GUI 组件状态、缩放窗口或图表渲染对象进入 SDK
//! - 用户必须手写 cache 文件格式、manifest、range 合并或损坏恢复
//! - 把该缓存放进高频交易 hot path
//!
//! Regression signal:
//! - 看盘软件每次启动或切换周期都全量重新下载历史窗口
//! - 图表向左回看需要业务代码手写分页、落盘和 range 合并
//! - 缓存中断写入后下次读取静默返回半截数据
//! - 历史序列缓存污染 live runtime revision / commit 语义
//! - cache miss 下载接口与官方 Python `DataSeries` 不一致
//!
//! Review questions:
//! - builder/config opt-in 是否足够明确，同时符合看盘软件的无感复用需求？
//! - Python 兼容默认目录是否应继续作为默认，还是仅作为迁移选项？
//! - mmap 首版 backend 是否仍保持在 `tqsdk-data` 的离线 materialization 边界内？
//! - 首版不支持 Python/Rust 同时写同一目录是否可以接受？
//!
//! Current API gap:
//! `tqsdk-data` 已落地 `DataClientBuilder` 和 `HistorySeriesCache`，首版覆盖
//! builder opt-in、Python 兼容默认目录 / 自定义目录、mmap 读、官方 `DataSeries`
//! miss 下载序列、mutable tail refresh、temp segment publish、相邻 segment merge、
//! typed `cache_report()` 和损坏文件 typed error。剩余问题是 manifest/schema
//! version、更丰富的 cache-only reader、容量/保留策略和 Python/Rust 同时写协调。
//!
//! 理想用户代码草案：
//! ```ignore
//! let session = SessionClientBuilder::new(user, pass)
//!     .futures_market()
//!     .build()?;
//! let client = DataClientBuilder::new()
//!     .with_session(session)
//!     .history_cache_enabled(true)
//!     .history_cache_dir("./cache/data_series_1")
//!     .build()?;
//!
//! let series = client
//!     .get_kline_data_series(KlineDataSeriesRequest::new(
//!         "SHFE.au2602",
//!         Duration::from_secs(60),
//!         start_ns,
//!         end_ns,
//!     ))
//!     .await?;
//!
//! let report = series.cache_report().expect("cache is enabled");
//! println!(
//!     "symbol={} rows={} cache_hit={} downloaded_ranges={:?}",
//!     series.symbol(),
//!     series.len(),
//!     report.hit_rows,
//!     report.downloaded_ranges
//! );
//! ```

fn main() {}
