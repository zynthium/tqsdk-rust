# tqsdk-rust

面向天勤 / TQSDK 生态的 Rust SDK 工作区，用一套共享的异步 runtime 支撑行情、
交易、策略执行和研究数据工作流。

[![CI](https://github.com/zynthium/tqsdk-rust/actions/workflows/ci.yml/badge.svg)](https://github.com/zynthium/tqsdk-rust/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

## 项目定位

普通用户优先从顶层 `tqsdk` crate 开始：连接账号、订阅行情、等待更新、读取账户/持仓、设置目标持仓，或按需下钻到 `wait` / `task` 执行接口、访问历史数据。内部 crate 仍保持独立边界，但第一次阅读不需要先理解整个 workspace taxonomy。

`tqsdk-rust` 的核心约束是所有可见状态变化都经过同一套 runtime state tree、commit/revision 和 cursor 语义。`tqsdk` 只是默认 facade；它不会复制 direct query、task 或 data 实现。

## 当前状态

项目正在积极开发中，当前 crate 版本为 `0.1.0`。建议先通过本仓库 workspace 或 Git
dependency 使用；正式 crates.io 发布前，public API 仍可能继续收敛。

仓库中的 `crates/*/examples` 不只是示例代码，也承担 public API contract 的作用。涉及
用户可见 API 的改动应保持这些示例清晰、可编译。

## 默认入口与高级 crate

高级用户可以按需要下钻：

| Crate | 适合场景 |
| --- | --- |
| [`tqsdk`](crates/tqsdk) | 默认用户入口：`prelude`、`Tq` 主循环、常用 live refs、target position、history helper，以及 `advanced::*` 下钻入口 |
| [`tqsdk-core`](crates/tqsdk-core) | 底层 async protocol substrate、状态树、commit/revision、runtime reader、cursor、adapter 和 schema types |
| [`tqsdk-session`](crates/tqsdk-session) | 共享 session、lazy connection、命令推进、one-shot direct query、metadata、schema 和 service query |
| [`tqsdk-wait`](crates/tqsdk-wait) | Python 风格 `TqApi`、`wait_update()`、`is_changing()`、live object refs、serial window 和 wait-style 交易命令 |
| [`tqsdk-task`](crates/tqsdk-task) | `TargetPosTask`、scheduler、typed order builder、pre-trade risk gate、strategy host、fake market / fake broker、task-owned replay source、streaming local backtest execution、Python-compatible local backtest sim、kline default price tick、cash/equity drawdown summary、低延迟 trading desk profile |
| [`tqsdk-data`](crates/tqsdk-data) | 历史数据 page/series/download、CSV export、option greeks、主连数据、`BacktestHistoryClient` 异步缓存查询、TQBN daily v3 (`.tqbn`) tick cache、canonical final-60s K cache、native final-1d K cache、tick companion-lock repair API 和共享 universe selector |
| [`tqsdk-cache`](crates/tqsdk-cache) | 可选 tick / canonical-minute / native-daily cache 运维与区间查询 CLI：统一 fill progress/schema-v3 report、默认文本摘要、按需 versioned JSON、lossless JSONL / token-aware LLM CSV、inventory/inspect/verify/doctor/purge，以及显式 `--history-root` 的 immutable snapshot clone/import、prewarm/query-smoke、publish/recover/rollback/scrub 和 lease-aware GC；不进入默认策略 hot path |
| [`tqsdk-relay`](crates/tqsdk-relay) | 可选 market relay / cache service：用共享上游 tick 源服务多个 SDK 客户端，并可在独立 listener/runtime 上启用只读 CacheOnly history sibling；未配置 relay 时 SDK 仍直连天勤 |

一般使用建议：

- 普通策略、目标持仓和轻量历史访问：先用 `tqsdk`。
- 已明确需要 Python 风格单 owner 推进点：直接用 `tqsdk-wait`。
- 只做合约、日历、metadata、schema 等一次性查询：用 `tqsdk-session`。
- 做历史数据、批量导出和 history series cache：用 `tqsdk-data`。
- 对固定 root 做历史 tick、canonical-minute 或 native-daily cache 预检、补齐、验收，或导出 cache-backed Tick/Kline 区间给脚本或模型：使用可选 `tqsdk-cache` CLI；普通策略不需要启动它。
- 做确定性 replay / 本地回测行情输入：用 `tqsdk_task::replay::ReplayMarketSource`。
- 需要策略下单并撮合成交的回测：优先用 `tqsdk` 的
  `.backtest(start_ns, end_ns)` 单一入口；默认复用共享 history cache，显式
  `.disabled_cache()` 时直连官方服务端回测行情；需要自定义 cache root 时配置
  `cache_dir` / `market_cache`，需要显式自定义 replay source 时用
  `.replay_backtest(source)`；官方单日复盘用 `.server_replay(...)`。
- 做执行工具、风控、策略 host、fake broker 或本地 sim：用 `tqsdk-task`。
- 自建 facade、多个异步消费者或极低层热路径：用 `tqsdk-core + tqsdk-session`。

## 环境要求

- Rust 1.85 或更新版本
- Tokio runtime
- 天勤 / TQSDK 账号，用于 live 行情、交易、query 和历史数据示例

live 示例默认读取以下环境变量：

```bash
export TQ_AUTH_USER="your-account"
export TQ_AUTH_PASS="your-password"
```

交易、调仓、导出和特定市场查询示例还会读取更细的 `TQ_*` 环境变量。下方
“常用示例”表标明了默认是否有限运行、是否会写文件、以及是否可能下单；更完整的
变量清单见对应 crate README 和 example 源码。

## 安装

在本仓库内开发时，直接使用 workspace crate：

```toml
[dependencies]
tqsdk = { path = "crates/tqsdk" }
tokio = { version = "1", features = ["macros", "rt", "time"] }
```

在其他项目中使用时，可以先依赖 Git 仓库：

```toml
[dependencies]
tqsdk = { git = "https://github.com/zynthium/tqsdk-rust" }
tokio = { version = "1", features = ["macros", "rt", "time"] }
```

高级用户仍可直接依赖 `tqsdk-session`、`tqsdk-wait`、`tqsdk-task`
或 `tqsdk-data`，但普通策略和研究入口应先尝试 `tqsdk`。
需要多个异步消费者或事件管道时，当前推荐基于 `tqsdk-session` 的
`RuntimeReader` / `UpdateCursor` 自建消费层。

`tqsdk-relay` 是可选基础设施。普通 SDK 使用不需要启动 relay；只有需要降低多进程、
全品种、多周期行情订阅压力时，才显式把 market endpoint 指向 relay。
relay 侧统一使用 `TQSDK_RELAY_FUTURES_UNIVERSE` 描述合约集合，例如
`active:all`、`main:all;index:all;!CFFEX` 或 `file:./futures-symbols.txt`，
由 relay 动态查询当前活跃合约集合，并默认在本地时间每天 `08:30:00` 重新发现。relay 会暴露上游
合约数、上游命令中的最大 `ins_list` 长度和阈值命中 metrics，并可用 `TQSDK_RELAY_DRY_RUN=1` 在启动前
检查订阅规模；`/health` 会区分下游监听、上游连接、订阅/补历史阶段、合约集合刷新和数据 freshness；
`/metrics` 和 `/dashboard` 会暴露上游 `connecting` / `subscribing` / `backfilling` / `live` 阶段，
dashboard 还会展示 backfilling 已持续时间、frame 速率和最近 frame idle；
产品发现会按批调用 `query_symbol_info` 获取官方 `trading_time`，供 `/symbol-metrics`
判断合约是否处于交易时间段；
可用 `main:all` 只订阅每品种主力合约；
可用 `top:2:all` 将产品发现结果限制为每品种主力和次主力；
也可用 `main:all;index:all;!CFFEX` 组合真实主力、加权指数、主连连续合约、top-N 和排除规则；
`index` 只生成天勤支持的 `KQ.i@EX.product` 加权 / 指数连续代码，`KQD` 外盘行情不会生成
不存在的加权 / 指数连续合约；
可用 `TQSDK_RELAY_UPSTREAM_TICK_VIEW_WIDTH=1` 调小后续动态 tick chart 补订的上游历史窗口；
`/symbol-metrics` 可用于查看每个合约的数据接收状态与延迟；
等待首样本或补历史尚未完成的合约会显示为 `initializing`，不计入问题数；
静态完整合约文件通过 `file:<path>` selector 接入。

## 快速开始

最小普通策略入口：

```rust
use tqsdk::prelude::*;

let mut tq = Tq::futures()
    .auth_env()?
    .trade_target_tqkq()
    .connect()
    .await?;

let quote = tq.quote("SHFE.au2602").await?;
let target = tq.target_pos_tqkq("SHFE.au2602").await?;

while tq.next().await? {
    if quote.load()?.last_price > 3600.0 {
        target.set(1)?;
    }
}
```

如果只想先验证不依赖真实账号和网络的策略测试 harness，可以运行：

```bash
cargo run -p tqsdk-task --example api_contract_s24_testable_strategy
```

如果要验证不依赖真实账号和网络的 Python-compatible 本地回测模拟账户闭环，可以运行：

```bash
cargo run -p tqsdk-task --example api_contract_s32_python_backtest_sim
```

如果要让普通策略主体在 backtest / tqkq-sim / live 间保持同一套 `Tq::next()` /
`quote()` / `target_pos_default(...)` 写法，优先使用默认 `tqsdk` facade。
`Tq::futures().backtest(start_ns, end_ns)` 是唯一推荐回测入口：默认使用
`tqsdk-data` 共享 history cache root（`$HOME/.tqsdk/data_series_1`，可用
`TQSDK_HISTORY_CACHE_DIR` 覆盖）。tick 使用 `BacktestTickCache`；持久 K 线输入包括独立
`MinuteKlineCache` 的 canonical final-60s monthly files（v5，按 logical symbol × trading month
分区）和 `DailyKlineCache` 的 native final-1d 单 logical-symbol 文件（v1，不按时间分区），再由本地
`TqSim` 回放。两者缺口只通过官方 server-side backtest Kline stream 填补，并且仅在远端 terminal
成功后标记 final coverage；不回退到专业历史下载路径。显式配置 `cache_dir(...)`、
`cache_store(...)` 或 `.market_cache(...)` 会覆盖默认 cache；显式 `.disabled_cache()`
才使用官方 server-side backtest market stream 且不落盘。默认 `RemoteOnMiss`
会先检查本地缓存；缓存完整时不需要 auth，缓存缺失时使用官方 server-side backtest market
stream 补齐缺口并写入持久缓存。这个补缓存路径不使用专业历史下载接口，也不需要专业历史下载权限。
全品种策略使用和 relay 一致的 universe selector 语法；facade 目前在
`.backtest(...).universe(...)`、实时 `quotes_universe(...)` 和
`MarketCachePolicy::record_universe(...)` 复用同一套解析与解析器封装。显式
`.tick(symbol, width)` 会复用 tick cache；`.kline(symbol, duration, width)` 中 `<60s` 的 K 线从本地 tick
流合成，`60s` 到 `<1d` 的整数分钟从 canonical-minute cache 读取/聚合，`1d` 读取服务端原生 1d
cache，`2d` 到 `28d` 从完整 1d rows 本地聚合。`61s` / `90s`、非整数日和大于 `28d` 的日周期会直接
validation error；K-only `>=60s` 不会隐式补 tick。分钟高周期的盘中 break 不会重置 bucket，break 内不会
虚构 minute row，但同一高周期 K 线可跨 break。K 线 replay 需要 quote synthesis metadata；可在 backtest builder 上用 `.price_tick(...)`、`.instrument_spec(...)` 或
`.default_price_tick(...)` 显式提供。

回测历史查询的 durable source 固定如下；派生 K 线只在查询/回放期间存在，不写入另一套 cache：

| 请求 | durable source | 本地处理 |
| --- | --- | --- |
| Tick | 按 CST trading day 的 TQBN v3 tick 分区 | 原样读取 |
| `15s` 和其他 `<60s` K | 同一 tick 分区 | 按官方 session 从 tick 聚合 |
| `60s` K | `logical symbol × trading month` 的 final canonical-minute v5 分区 | 原样读取 |
| `N × 60s`（`N > 1` 且 `<1d`） | 同一 60s 分区 | 从 closed 60s K 按固定 CST `18:00` trading-day grid 聚合；盘中 break 不重置 bucket |
| `1d` K | `daily-kline-v1/<escaped-logical-symbol>.tqdk` 的 native final-1d file | 原样读取 |
| `2d` 到 `28d` K | 同一 native 1d file | 从 complete final 1d rows 本地聚合；不落盘 |

三层来源由 `BacktestHistoryClient` 统一规划和填充：tick 服务 tick 与 `<60s`，canonical minute 服务
`60s..<1d`，native daily 服务 `1d..=28d`。daily 缓存缺失或损坏时必须失败，不回退到 minute。

tick、60s minute 与 1d cache 均没有自动 retention、max-byte eviction 或后台清理；1d 是单 symbol
完整文件，不按时间分区，只有显式 `purge_symbol` 才删除。daily row 当前只保存 `Kline` 的
OHLC、volume、open/close OI；结算价与涨跌停价未支持、不会伪造为 1d 字段。`tqsdk::advanced::data::BacktestHistoryClient` 是面向区间查询的异步入口：它按 request id
流式交付 chunk，只有收到 `RequestCompleted` 后 chunk 才成为成功结果；需要一次性收集时，单请求
`collect()` 使用配置的内存上限，批量 `collect_all(max_total_bytes)` 必须由调用方显式给出总内存预算。
`RemoteOnMiss` run 与普通 facade/CLI fill 共用 cache-root shared gate；不同 symbol 可并行，重叠的
`family × physical symbol` fill 再由跨进程 lease 串行。refresh、stale repair、verify、doctor 和真实 purge
使用 exclusive gate，锁冲突返回可重试的 `cache_busy`。该协议是 advisory coordination，部署时不要让
不理解 shared/exclusive gate 的旧版本进程与新版本长期混跑。

回测声明 `KQ.m@EX.product` 主连时，facade 会通过
`tqsdk-data` 持久化的 metadata sidecar 取得 calendar、session 与历史 date → concrete-contract
映射，按 CST 交易日切成物理合约 tick range。缓存、coverage、remote-on-miss 和 `.warmup()`
都以具体合约为 key：主连与同一时段的具体合约共用同一份 `.tqbn` tick 文件，replay 事件仍保留
主连 symbol 并附带 `underlying_symbol`。minute cache 则以逻辑主连 symbol 为 key，不复制 physical
minute 文件；主连支持 canonical 60s 和整数分钟本地聚合。`RemoteOnMiss` 只在 sidecar 缺失或覆盖不足时
刷新它；`.cache_only()` 必须已有可覆盖窗口的 sidecar，且不会访问 metadata service。
minute 的 remote metadata refresh 会扩展到涉及的完整 CST trading month；更窄的新 snapshot 不会取代
更宽的 active pointer，覆盖请求范围的 retained snapshot 会被复用，以避免同月增量查询产生不兼容 cache identity。

需要从命令行导出同一份回测缓存时，使用可选 `tqsdk-cache query`：它只是
`BacktestHistoryClient` 的 CLI adapter，可输出 lossless JSONL，或为模型上下文压缩的
`tqllm-csv/3`；详情见 [`tqsdk-cache` README](crates/tqsdk-cache/README.md)。

每个 `.tqmk` 月分区绑定写入时的 immutable metadata snapshot；active pointer 后续前移本身不会使
旧分区失效。若 active snapshot 只是向后扩展日期，读取方会加载月文件绑定的旧 immutable sidecar，并逐个
cached range 比较 schema、market、logical symbol、session、交易日和主连 physical mapping；该区间语义
完全相同即可复用旧 coverage，新增日期只作为缺口填充，当前月下一次原子写入时迁移到新 snapshot hash。
缺少任一 sidecar、session/交易日/映射变化、损坏文件或语义冲突的混合分区仍默认 fail closed，不会自动
删除、重写或拼接数据。只有
操作者显式传 `tqsdk-cache --kind minute fill --repair-stale` 时，CLI 才会在 active snapshot 覆盖窗口时，
删除该窗口内与它冲突的整月分区，再走普通 `remote-on-miss` 补齐；它不适用于 tick 或
`--dry-run`，普通读取与 fill 仍不会删除数据。

cache-backed local backtest 当前只支持 futures；`Tq::stock().backtest(...)` 必须显式
`.disabled_cache()` 并使用官方股票 server-backtest 行情。futures universe selector 不适用于股票，
股票策略应显式声明 symbol。
tick、minute 与 daily cache 都不会因读取、写入、retention 或 max-byte 配置而自动删除数据。清除或
重拉必须显式使用 backtest builder 的 `refresh` / purge API 或 `tqsdk-cache`：tick 按 trading-day
range 删除分区，minute 按 range 删除相交整月，daily 只删除整 logical-symbol 文件；真实 CLI purge
都要求 `--yes` 和 exclusive root lock。混合 minute snapshot 的受控重拉可显式使用 minute fill 的
`--repair-stale`，它只删除已定位的冲突整月分区。

自有多资产调度器可在 `.prepare().await?` 后读取 `PreparedBacktest::tick_sources()`，
复用同一 logical-to-physical 投影；必须按每项的半开有效区间读取物理 cache。

```rust
let mut tq = Tq::futures()
    .auth_env()? // only needed when RemoteOnMiss has to fill missing cache ranges
    .backtest(start_ns, end_ns)
    .default_price_tick(1.0)
    .kline("KQ.i@SHFE.au", std::time::Duration::from_secs(60), 200)?
    .universe("active:all;!CFFEX")?
    .remote_on_miss()
    .connect()
    .await?;
```

实盘或模拟盘策略如果要维护同一份回测可复用的 tick 缓存，推荐先定义共享
`MarketCachePolicy`：live 连接时自动记录指定 tick，回测侧可复用同一个 policy 作为默认
cache 目录和 symbol 集合。policy 可以用 `.record_ticks([...])` 显式列 symbol，也可以用
`.record_universe("active:all;!CFFEX")?` 复用同一套 selector。

```rust
let cache = MarketCachePolicy::new(".tqsdk/backtest_ticks")
    .record_universe("symbol:KQ.i@SHFE.au")?;

let mut tq = Tq::futures()
    .auth_env()?
    .market_cache(cache.clone())
    .connect()
    .await?;
while tq.next().await? {
    // normal strategy body
}

let backtest = Tq::futures()
    .market_cache(cache)
    .backtest(start_ns, end_ns)
    .cache_only();
```

底层 `record_ticks(cache_dir, symbols)` 仍可作为显式运行时入口。当前 policy/recording
只记录 policy 解析出的 symbol；它不自动记录所有行情订阅，也不启动守护进程。
facade 会在每次 `next()` / `wait_update()` 收集新 tick，但为避免同步 fsync 阻塞策略循环，
连续 rows 按每 symbol 最多 `128` 行或约 `250 ms` 批量落盘；首批和检测到跳号时立即提交，
正常销毁 `Tq` 时也会强制 flush。首次初始化或失败重扫之外，每个 update 只解码变更集命中的 tick
serial；写入端只在 tick id 连续时推进 coverage，断线、跳号或异常
退出前尚未提交的尾部会保留缺口；
`record_ticks_health()` 可查看累计写入行数、最近 flush、每个 symbol 的 last id 和 gap 状态。
`recorded_market_cache_policy()` 可以从当前 recording health 派生出同一份 cache policy，后续显式
配合 `.auth_env()?`、`.warmup()` / `.remote_on_miss()` 使用官方 server-side backtest 流补齐；
运行中的 `Tq` 不会隐式保存或复用明文 auth。
直接下钻 `LiveTickCacheWriter` 时，连续的单 tick `push_ticks(...)` 同样会聚合到 128 行；
批量输入、跳号、约 250 ms 后的下一次 push、显式 `flush()` 或最后一个 writer 销毁会提交短尾。
`LiveTickCacheWriteReport::appended_rows` 只统计本次实际落盘行数。

显式离线校验可加 `.cache_only()`；需要强制重新走官方回测流并覆盖本地缓存时用
`.refresh()`，这是显式破坏性操作：它会对每个已解析的 physical tick symbol 删除其全部 tick
日分区，并对 canonical-minute cache 只删除请求窗口相交的月文件，之后再由官方回测流补齐；
需要自定义内存 replay source、测试 fixture 或外部数据源时用
`.replay_backtest(source)`。官方服务端 market-data-only 回测就是同一个
`.backtest(start_ns, end_ns).disabled_cache()` 的行为；单日服务端复盘仍用
`.server_replay(date)?`。服务端回测和 `server_replay` 不绑定交易目标，也会拒绝自动交易登录；
需要策略下单并本地撮合时配置 cache-backed `.backtest(...)` 或使用 `.replay_backtest(...)`。

显式运维缓存时，可在 builder 上调用 `.inspect_cache()` / `.purge_cache_symbols()` 操作 tick，
或 `.inspect_history_cache()` / `.purge_history_cache()` 获得或删除 tick 与 canonical-minute 的 typed report。
需要先预热全品种缓存而不运行策略时，用同一个 builder 调 `.warmup().await?`；它会按
`.batch_size(n)` 记录兼容 batch hint、解析 universe、跳过完整缓存、用官方 server-side
backtest 流按每个物理 cache symbol 的 `missing_ranges` 只补缺口，并返回每个 symbol 的 skipped /
missing / filled 报告。普通 final 补齐成功后只
compact 本次实际 fill 涉及的 tick 交易日分区（同日去重），用于合并回填时产生的碎片 blocks；
provisional fill 不 compact。Tick 远端行按交易日顺序消费并以 8192 行短批追加，fill-only 路径不再为
统计行数回读 cache；完整 cache hit 的 `rows_written` 为 0，同一 shared fill 的物理写入只计入一个报告。
当前交易日若要盘中增量快照，可固定
`.backtest(day_start_ns, as_of_ns)` 后调用
`.provisional_open_day_fill(day_start_ns, as_of_ns)?`；它只写 non-final checkpoint，
不满足普通 coverage/cache-hit，且 checkpoint 的范围和 as-of 必须位于同一 TQBN 日分区。
远端明确成功结束的空增量可以推进 checkpoint；取消、超时或未确认结束不能推进。
重复运行从 checkpoint 前 5 分钟重叠续填，并延后 compaction，避免每次盘中续填重写全历史；
TQBN 18:00 分区结束后必须再次运行普通 warmup，完成全日 final 重对账、compaction 并淘汰 checkpoint。
运维 CLI 对显式 `--end-day` 等于当前 TQBN 交易日的请求自动采用该模式，单次 horizon 固定为
启动时刻减 5 秒；严格任务可传 `--require-final` 拒绝当前日。`--include-open-day` 仅保留为兼容参数。

多合约策略主体复用可参考 `api_contract_s39_facade_same_body`：同一个两腿价差策略只接收
`&mut Tq`，策略内使用 `target_pos_default(...)`；`TQ_EXAMPLE_MODE=local-backtest|tqkq-sim|live`
决定本地 `TqSim` 回测、快期模拟或实盘账户构造。快期模拟可用 `.tqkq_sim()`，实盘可用
`.trade_account_env()` 自动读取并登录 `TQ_TRADE_BROKER_ID` / `TQ_TRADE_ACCOUNT_ID` /
`TQ_TRADE_PASSWORD`。若已通过 `tqsdk-session` 查询到合约 metadata，可把
`InstrumentSpec` 传入 `instrument_spec(...)`，用于 kline quote synthesis 的
`price_tick` 和本地撮合合约乘数。

当前本地路径已覆盖 quote/tick/kline replay event 的最小 quote synthesis、replay symbol
自动追踪、`TargetPos` 执行闭环与增量 execution events/trades 读取，以及轻量
`summary()` / `performance_metrics()` / `performance_report(window)` / trade log /
cash + mark-to-market equity 曲线 / 平仓盈亏观测 / 胜率 / 盈亏额比例。交易日历、单主连
date -> underlying 映射和 contiguous segment 压缩可用
`tqsdk-data::DataClient::query_trading_calendar_holidays(...)` /
`query_trading_calendar(...)` / `query_trading_days(...)` /
`tqsdk-data::DataClient::query_his_cont_underlyings(...)` /
`query_his_cont_underlying_segments(...)`。
历史序列和回测 tick cache 默认写按交易日分区的 TQBN daily v3 (`.tqbn`)；
tick 路径形如 `series/<YYYYMMDD>/tick/<escaped-symbol>.tqbn`。默认 features 启用
`tqbn-zstd`，hot append 的 TQBN records block 使用 zstd level 1，append-log compaction
重写 records block 时使用 zstd level 3；两者都只在压缩后更小时写入压缩 block。
Tick v3 首 snapshot 是完整 keyframe，后续 snapshot 只写变化字段和 id/time delta；不会按固定 tick
频率填充，也不会删除无成交或重复盘口 snapshot。旧 v2 cache 需先执行
`tqsdk-cache migrate --apply --backup-dir DIR`；新 writer 拒绝向 v2 Tick 文件混写。
canonical-minute v5 保留全部 60s Kline row，并仅在 zstd 更小时压缩 row payload；零成交或重复分钟
不会被删除或合成。旧 minute v4 cache 需先执行
`tqsdk-cache --kind minute migrate --apply --backup-dir DIR`，并由该命令把原文件硬链接备份到
cache root 外；v3 minute 文件仍按 fail-closed 处理。
当前日 checkpoint 使用独立的 non-final provisional record，不进入普通 coverage；同日 final
coverage 成功后会覆盖并在 compaction 中淘汰它。
market-data records block 以 8 MiB 未压缩 payload 为目标上限，并紧跟 crate-internal `TQRI`
时间索引；范围读取只解压与请求相交的 block，旧文件或缺失/不匹配索引逐 block 回退完整解码。
`--no-default-features` 可关闭该支持，不新增用户可选 store API。

如果已经配置好天勤账号，可以运行一次 `wait_update()` 行情示例：

```bash
export TQ_AUTH_USER="your-account"
export TQ_AUTH_PASS="your-password"
TQ_WAIT_ONCE=1 cargo run -p tqsdk-wait --example quote_wait
```

让 AI 助手在你的项目里使用 TQSDK Rust 上下文：

```bash
npx skills add https://github.com/zynthium/tqsdk-rust
```

## API 形态示例

### Python 风格 wait facade

适合单 owner 的策略主循环：

```rust
let mut api = tqsdk_wait::TqApiBuilder::new(user, pass).build().await?;
let quotes = api.quotes(["SHFE.au2602", "DCE.m2609"]).await?;
api.wait_update(None).await?;
let snapshot = quotes.get("SHFE.au2602").unwrap().load()?;
```

`tqsdk-wait` 的 `quotes(...)` 会一次表达批量 quote interest；`quote(...)` 仍是单合约便利入口。`kline(...)` / `tick(...)` 会立即返回 live serial handle；如果需要在启动阶段等待 chart 初始化，使用 `kline_ready(...)` / `tick_ready(...)`。多合约 K 线序列使用 `kline_multi([...], ...)`：它提交一个共享 `chart_id` 的逗号 `ins_list`，服务端初始 `view_width=10000`，客户端按主合约 `binding` 对齐副合约；Tick 序列保持单合约，逗号合约输入会报错。

### Direct query / metadata

适合一次性查询，不需要绑定 `wait_update()`：

```rust
let session = tqsdk_session::SessionClientBuilder::new(user, pass)
    .enable_query()
    .build()?;
let rows = session.query_symbol_info(&["SHFE.au2602"]).await?;
let info = &rows[0];
println!(
    "{} day={:?} night={:?}",
    info.instrument_id, info.trading_time.day, info.trading_time.night
);
```

`query_symbol_info(...)` 返回 typed `SymbolInfo`，用于官方合约信息表字段：
交易时间段、涨跌停、昨结算、开仓限额、到期/行权字段等。只需要 tick size、
合约乘数、交易所和品种等下单校验字段时，用更窄的
`query_instrument_specs(...)`。

### 历史数据与研究工作流

适合 kline/tick 历史数据、导出、history series cache 和研究查询：

```rust
use std::time::Duration;

let session = tqsdk_session::SessionClientBuilder::new(user, pass)
    .futures_market()
    .build()?;
let client = tqsdk_data::DataClient::from_session(session);
let request = tqsdk_data::KlineDataPageRequest::new(
    "SHFE.au2602",
    Duration::from_secs(60),
    128,
);
let page = client.get_kline_data_page(request).await?;
```

## 常用示例

| 场景 | 命令 | 运行说明 |
| --- | --- | --- |
| 不依赖真实账号的策略测试 harness | `cargo run -p tqsdk-task --example api_contract_s24_testable_strategy` | 使用 fake market / fake broker，不连接真实服务 |
| Python-compatible 本地回测模拟账户 | `cargo run -p tqsdk-task --example api_contract_s32_python_backtest_sim` | 使用本地 quote/tick/kline replay + `TqSim`，不连接真实服务 |
| 默认 facade 服务端回测 | `cargo run -p tqsdk --example api_contract_s37_facade_server_backtest` | `Tq::futures().backtest(...).disabled_cache().connect()` 切换到官方服务端 market-data-only 回测；需要账号 |
| 默认 facade 本地 replay 回测 | `cargo run -p tqsdk --example api_contract_s38_facade_local_backtest` | `Tq::futures().replay_backtest(...)` 使用本地 replay + `TqSim`，不连接真实服务 |
| 默认 facade 持久缓存回测 | `cargo run -p tqsdk --example api_contract_s43_facade_backtest_history_cache` | `.backtest(...).universe(...)` 默认通过共享 history cache root 复用持久 tick 缓存；`.cache_dir(...)` 可覆盖 |
| 默认 facade remote-on-miss 缓存回测 | `cargo run -p tqsdk --example api_contract_s44_facade_backtest_remote_on_miss` | 使用官方 server-side backtest tick stream 填补缺失缓存；需要账号，但不需要专业历史下载权限 |
| 默认 facade 缓存预热 | `cargo run -p tqsdk --example api_contract_s45_facade_backtest_cache_warmup` | `.warmup()` 只预热缓存；示例也编译检查当前日 `.provisional_open_day_fill(...)` 配置 |
| 默认 facade 实时 tick 记录 | `TQ_RUN_LIVE_RECORD_TICKS=1 cargo run -p tqsdk --example api_contract_s46_facade_record_ticks` | 显式 `record_ticks(...)` 把指定合约 live tick 写入同一份回测缓存；需要账号 |
| 默认 facade 共享缓存 policy | `TQ_RUN_LIVE_RECORD_TICKS=1 cargo run -p tqsdk --example api_contract_s47_facade_market_cache_policy` | `MarketCachePolicy` 同时驱动 live tick recording 和 cache-backed local backtest 输入 |
| 回测缓存运维 CLI | `cargo run -p tqsdk-cache -- --help` | 可选 binary；以 `--kind tick|minute|daily|all` 管理 daily TQBN tick、canonical-minute 与 native-1d cache；daily 支持 closed-day inspect/fill/verify/report 与整文件 purge，不启动 relay 或守护进程 |
| 修复遗失的 tick companion lock | `cargo run -p tqsdk-cache -- --kind tick repair-locks` | 默认检查每个 Tick 分区的 legacy `.tqbn.lock` 与逐文件 sidecar；停止同一 root 的读写者后，才用 `--apply` 补建缺失 lock，不填数、不重写 TQBN |
| 回测历史查询 / LLM 上下文 CLI | `cargo run -p tqsdk-cache -- query --help` | 同一 cache-backed history query 的 CLI adapter；`jsonl` 用于无损 rows，`llm-csv` 用于 token-aware 模型输入 |
| 默认 facade 多合约同主体 | `cargo run -p tqsdk --example api_contract_s39_facade_same_body` | 同一两腿价差策略只接受 `&mut Tq`，`TQ_EXAMPLE_MODE` 决定本地回测、快期模拟或实盘 |
| 默认 facade 本地回测 TargetPos | `cargo run -p tqsdk --example api_contract_s40_facade_local_backtest_target_pos` | 同一 `Tq::next()` 策略主体在本地 replay 中读取持仓并用 `TargetPos` 调仓 |
| `wait_update()` 行情更新 | `TQ_WAIT_ONCE=1 cargo run -p tqsdk-wait --example quote_wait` | 需要 `TQ_AUTH_USER` / `TQ_AUTH_PASS`；去掉 `TQ_WAIT_ONCE=1` 后持续运行 |
| 合约 metadata 查询 | `cargo run -p tqsdk-session --example query_symbol_info` | 需要账号；可用 `TQ_TEST_SYMBOL` 覆盖默认合约 |
| command wait helper | `cargo run -p tqsdk-session --example query_command_wait` | 需要账号；默认查询 `SSE.000300`，可用 `TQ_QUERY_SYMBOL` 覆盖 |
| K 线分页查询 | `cargo run -p tqsdk-data --example kline_data_page` | 需要账号和历史数据权限；可用 `TQ_TEST_SYMBOL` 覆盖默认合约 |
| K 线 CSV 导出 | `cargo run -p tqsdk-data --example kline_export_csv` | 需要账号和历史数据权限；默认写入 `/tmp/tqsdk-kline-export.csv`，可用 `TQ_EXPORT_PATH` 覆盖 |
| 目标持仓任务 | `cargo run -p tqsdk-task --example target_pos` | 需要账号；默认 TqKq dry-run，不会下单；只有设置 `TQ_TASK_ALLOW_ORDERS=1` 和 `TQ_TARGET_VOLUME` 才进入调仓循环 |
| 低延迟 trading desk profile | `cargo run -p tqsdk-task --example api_contract_s31_low_latency_trading_desk` | 需要账号；默认不会下单；只有设置 `TQ_DESK_ALLOW_ORDER=1` 才提交示例订单 |

更多场景契约示例见各 crate 的 `examples/` 目录。

## 架构概览

仓库采用“稳定底座 + 可替换 facade”的分层。下图表示用户能力层级，不是 Cargo 依赖图：

```text
tqsdk-core
    ^
    |
tqsdk-session
    ^
    |
tqsdk-wait / tqsdk-data
    ^
    |
tqsdk-task
    ^
    |
tqsdk
```

实际 Cargo 依赖中，`tqsdk` 作为默认入口会直接依赖 `tqsdk-core`、`tqsdk-session`、
`tqsdk-wait`、`tqsdk-task` 和 `tqsdk-data`。内部能力归属仍由这些 crate 自己维护。

所有对外可见的状态变化都经过同一套 runtime commit model：

```text
RuntimeCommand / RuntimeInput
    -> ProtocolAdapter
    -> NormalizedMutation
    -> RuntimeHandle
    -> StateStore
    -> CommitResult
    -> RuntimeReader / UpdateCursor
```

这样可以保证 `wait_update()`、task tooling 和 research pipeline
看到的是同一棵状态树、同一套 revision 和同一套因果解释。底层 crate 保持克制：
direct query 属于 `tqsdk-session`，live diff 消费属于 `tqsdk-wait`，
执行工具属于 `tqsdk-task`，研究和离线数据属于 `tqsdk-data`。

完整架构说明见 [docs/architecture](docs/architecture)，验证矩阵见
[docs/architecture/validation.md](docs/architecture/validation.md)。

## 本地开发

克隆仓库并检查默认 SDK crates：

```bash
git clone https://github.com/zynthium/tqsdk-rust.git
cd tqsdk-rust
cargo check --examples
```

`tqsdk-relay` 是可选基础设施，不在默认 SDK validation set 中；修改 relay 时显式运行
`cargo test -p tqsdk-relay --tests` 等 relay gate。
`tqsdk-cache` 同样是可选运维二进制；修改它时显式运行
`cargo test -p tqsdk-cache` 和 `cargo clippy -p tqsdk-cache --all-targets -- -D warnings`。

常用验证命令：

文档-only 或工作流入口改动：

```bash
git diff --check
```

Rust 代码改动的快速自检：

```bash
cargo check --examples
```

可提交改动单元的默认验证：

```bash
cargo test
cargo clippy --examples --all-targets -- -D warnings
```

如果改动会影响格式化，提交前补充：

```bash
cargo fmt --all --check
```

修改 feature flags、workspace 依赖或 crate feature 传播时，补充：

```bash
cargo check --no-default-features
cargo check --no-default-features --examples
cargo test -p tqsdk-session --no-default-features
cargo check --all-features --examples
```

更完整的 release-check / contract 验证矩阵见 `docs/architecture/validation.md`。

## 文档入口

- [文档索引](docs/README.md)
- [架构总览](docs/architecture/README.md)
- [回测 Tick 持久缓存预热与验收](docs/architecture/backtest-tick-cache-operations.md)
- [回测 Tick Cache CLI](docs/architecture/backtest-tick-cache-cli.md)
- [runtime core overview](docs/architecture/runtime-core/overview.md)
- [crate 边界审计](docs/architecture/crate-boundaries.md)
- [验证矩阵](docs/architecture/validation.md)
- [路线图](ROADMAP.md)

每个 crate 也有自己的 README，说明该 crate 的职责边界、示例和 public surface。

## 贡献

欢迎 issue 和 pull request。开始改动前，请先阅读架构总览和受影响 crate 的 README。
改动应尽量聚焦，并保持 crate 归属边界清晰。

如果改动涉及 public API、feature flags、runtime contract 或 facade 职责归属，请同步更新
相关架构文档或 crate README。影响用户可见行为时，优先补充 focused tests 或
`api_contract_sXX_*` 示例。

## License

本项目采用 [MIT License](LICENSE)。
