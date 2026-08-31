# tqsdk

`tqsdk` 是 `tqsdk-rust` 的默认用户入口。它不物理合并内部 crate，也不改变
runtime contract；它只提供一个更容易开始的 facade：

- `tqsdk::prelude::*`
- `Tq::new()` / `Tq::futures()` and `Tq::stock()` market builders
- Unified strategy backtest (`.backtest(start_ns, end_ns)`): default shared history cache-backed local simulated backtest; `.disabled_cache()` means official server-side market stream; `cache_dir` / `market_cache` override the cache root
- Resumable current-day cache snapshot (`.provisional_open_day_fill(day_start_ns, as_of_ns)?`): never promotes an open day to ordinary final coverage
- Shared tick cache policy for live recording and cache-backed local backtests (`MarketCachePolicy`, `.market_cache(...)`, `.record_universe(...)`)
- Explicit live tick recording into the shared backtest cache (`.record_ticks(cache_dir, symbols)`)
- Shared futures universe helpers for live quotes (`quotes_universe(...)` / `quotes_universe_spec(...)`)
- Market-data-only server-side single-day replay (`.server_replay(date)?`)
- Advanced custom replay backtest (`.replay_backtest(source)`, optional `.instrument_spec(...)` / `.default_price_tick(...)`)
- `Tq::next()` 主循环
- 常用 wait-style live refs 和 `Quote` 统一定义
- `TargetPos` 轻量 wrapper
- Local backtest 默认模拟账户常量 `LOCAL_BACKTEST_ACCOUNT_ID`
- 默认账户 helper：`default_account_id()` / `account_default()` / `position_default()` / `target_pos_default(...)`
- 交易账户构造 helper：`.tqkq_sim()` / `.tqkq_sim_numbered(...)` / `.trade_account(...)` / `.trade_account_env()`，仅用于 live/sim 连接，不可与 server-side backtest/replay 组合
- Local backtest summary / performance metrics / performance report、cash/equity 曲线点、买卖/开平次数、日收益统计（含显式交易日窗口）和最大回撤
- `Tq::history()` helper
- `tqsdk::advanced::*` 下钻到底层 crate

普通 `.universe(...)` / `.universe_spec(...)` 是当前 snapshot；历史动态合约集合必须消费 pinned
artifact。旧 `.historical_universe_plan(plan)?` 签名继续读取 plan v1–v3；新
`.historical_universe_artifact(artifact)?` 读取 V1–V5，并在 prepare/warmup 前验证 artifact hash、回测
区间以及 V5 acquisition/catalog chain。V4 artifact 的 acquisition/catalog/V3 rollback chain 会在迁移
为 V5 时验证。V5 timeline 控制当时可见的 physical、continuous、index instrument，kind-specific tick
targets 控制物理 cache dependency 与首可用边界；成员变化和同时间
行情仍在一个 replay revision 可见。provider-history 起点表示数据 membership，不表示法定挂牌日。

`.backtest(start_ns, end_ns)` 是默认 Python-style 策略回测入口。它默认使用
`tqsdk-data` 共享 history cache root（`$HOME/.tqsdk/data_series_1`，可用
`TQSDK_HISTORY_CACHE_DIR` 覆盖）。tick 使用 `BacktestTickCache`；持久 K 线输入使用独立
`MinuteKlineCache` 的 canonical 60s monthly files 与 `DailyKlineCache` 的 native 1d single file，
三者都在本地回放到 `TqSim`。
配置 `.cache_dir(...)`、`.cache_store(...)` 或 `.market_cache(...)` 会覆盖默认 cache；
显式 `.disabled_cache()` 才直接使用官方 server-side backtest market stream 且不落盘。
显式 `.cache_only()` 只读本地缓存；默认 `RemoteOnMiss` 在缓存完整时直接复用本地数据且不需要
auth，缓存缺失时通过官方 server-side backtest stream 拉取 tick、canonical 60s 或 native 1d rows、
推进本地回测并写入持久缓存。
这个路径不使用专业历史下载接口，也不需要专业历史下载权限。`.universe(...)` 使用和 relay
对齐的 legacy-first selector；`snapshot(...)` 强制 Universe Language V2，省略 wrapper 的 V2 默认
snapshot。typed 调用可直接传 `tqsdk::advanced::data::UniverseSpec` 给 `.universe_spec(...)`、
`quotes_universe_spec(...)` 或 `record_universe_spec(...)`。同一套 snapshot 选择被实时
quotes 和 `MarketCachePolicy` 复用；可重复的 `universe_symbol_file(s)` 在 DSL 外组合 exact symbols。
所有 snapshot-only 入口都在网络动作前拒绝 `timeline(...)`。最终 resolved
universe 会排除当前不受本地 history cache / relay 支持的 `KQD` 外盘合约，因此 V2
`continuous:all`
不会请求不存在的 `KQ.m@KQD.*` 历史主连映射。

`KQ.m@EX.product` 主连回测通过 `tqsdk-data` 的 persisted metadata sidecar 解析历史映射，并按 CST
交易日把逻辑主连投影到具体合约 tick range。缓存文件、coverage、remote-on-miss、refresh 和
warmup 均使用具体合约 symbol，所以主连与相同日期的具体合约共用一份 tick cache；回放仍使用
主连 symbol，quote 的 `underlying_symbol` 标注当时实际合约。`RemoteOnMiss` 只在 sidecar 缺失或
覆盖不足时刷新 metadata；`.cache_only()` 必须已有本地 sidecar，绝不访问公开 metadata 服务。
minute/daily K cache 始终以逻辑 `KQ.m@...` 为 key，dated physical contract 只保留在 replay metadata；
`60s..<1d` 的整数分钟与 `1d..=28d` 的整数日均受支持。

cache-backed local backtest 当前只支持 futures。`Tq::stock()` 选择股票 market / server-backtest
endpoint，但股票回测必须显式 `.disabled_cache()`；futures universe selector 不适用于股票，股票策略
应显式声明 symbol。
minute 的 RemoteOnMiss / Refresh 不能为当前或未来 CST trading day 声称 final coverage，必须等该
trading day 关闭后再填充。

调用方自带多资产回放调度器时，可先调用 `.prepare().await?`，再通过
`PreparedBacktest::tick_sources()` 取得同一份 logical-to-physical 投影。每项都包含稳定的
`replay_symbol`、缓存使用的 `cache_symbol` 和权威半开区间 `[start_ns, end_ns)`；具体合约不得
脱离该区间扩展到整个回测窗口。默认 `.connect()` 继续消费同一投影。

cache-backed local backtest 可以显式声明 serial 输入：`.tick(symbol, view_width)` 复用
tick cache；`.kline(symbol, duration, view_width)` 使用固定三层来源：`<60s` 从 tick 本地合成，
`60s..<1d` 的整数分钟从 canonical 60s cache 读取/聚合，`1d..=28d` 的整数日从 native 1d cache
读取/聚合。daily miss 必须失败，不回退 minute。`61s` / `90s`、非整数日和大于 `28d` 明确拒绝；
K-only `>=60s` 不会隐式拉取 tick。K 线 replay 需要 quote
synthesis metadata；可在 backtest builder 上用 `.price_tick(...)`、`.instrument_spec(...)` 或
`.default_price_tick(...)` 显式提供。

需要直接按时间区间读取缓存，而非启动策略回放时，使用
`tqsdk::advanced::data::BacktestHistoryClient`。它以 request id 的 `Chunk` / terminal event stream
返回 Tick 或本地聚合 K 线；只有 `RequestCompleted` 后 chunk 才成功。`collect()` 有默认内存上限，
`collect_all(max_total_bytes)` 必须显式指定批量内存预算。`RemoteOnMiss` 查询在 materialize/验证期间
持 shared cache-root gate，和普通 warmup 并发、与 refresh/repair/稳定检查互斥；结果收集完成后即释放，
后续大输出格式化不会延长 gate 生命周期。prelude 故意不导出该高级 API。

`BacktestHistoryClient` 也是 tick/minute/daily fill scheduling 的唯一 owner。默认 symbol batch size 1、
concurrency 2、idle timeout 60 秒、无 batch timeout；batch size/concurrency 只接受 `1..=4`。facade
只把统一 progress/terminal report 适配为既有用户表面。

缓存运维入口保留在同一个 builder 心智里：`.inspect_cache()` / `.purge_cache_symbols()` 是
tick-only 兼容 API；`.inspect_history_cache()` 返回 tick、canonical-minute 与 native-daily typed status，
`.purge_history_cache()` 是三类缓存的显式 destructive operation；两条 purge API 都先取得 exclusive
cache-root gate，不能与普通 fill/query 穿插。`.warmup().await?` 只预热
缓存、不创建策略 runtime；它会先跳过完整缓存，再把物理 tick range 和逻辑 minute/daily symbol 的
`missing_ranges` 交给对应的官方 server-side backtest stream 补齐。默认不做
时间切片；只有设置 `TQSDK_REMOTE_FILL_SLICE_SECS` 时才按时间切片 fallback。普通 final 补齐成功后只
按本轮实际远端回填的 `symbol × trading day` 去重 compact 相交 tick 日分区，provisional fill 跳过 compaction，并返回
每个 symbol 的报告。远端填充并发由
`TQSDK_REMOTE_FILL_SYMBOL_CONCURRENCY` 控制，symbol 合并会话大小由
`TQSDK_REMOTE_FILL_SYMBOL_BATCH_SIZE` 控制，默认值保持保守以避免放大官方服务压力。
默认不设置整批墙钟超时，长区间的持续进展不会被固定时限中断；60 秒无 tick 进展仍会触发
保护，可用 `TQSDK_REMOTE_FILL_IDLE_TIMEOUT_SECS` 调整。只有显式设置正数
`TQSDK_REMOTE_FILL_BATCH_TIMEOUT_SECS` 时才会启用整批限时，适合诊断或外层作业预算。
tick 按 trading day 顺序补缺，以 8192 rows 短批落盘；每个成功 slice 都先校验 tick id 连续性后独立
提交 coverage。取消时已接受短尾先 flush，物理 tail checkpoint 可随之推进，但未 terminal 的范围不提交
final/provisional coverage。普通 final
fill 全部成功后才 compact 相交日分区，provisional fill 延后到最终重对账。fill-only warmup 不回读刚写入
cache；`rows_written` 只统计实际物理落盘 rows，共享 fill 被多个 logical request 复用时只计一次，完整
cache hit 合法为 `0`。失败或未确认 slice 留下缺口，后续 warmup 继续补齐。
`.refresh()` 会在准备远端补齐前清空请求范围：tick 仍按 symbol 全 series 文件粒度，minute
只删除与请求窗口相交的 `trading-YYYYMM` files。tick 和 minute cache 都没有自动 retention、
max-byte eviction 或后台清理；`CacheOnly` minute inspection 是只读的，不会创建 v3 namespace。
需要自行编排当前日盘中快照时，可在固定的
`.backtest(day_start_ns, as_of_ns)` builder 上调用
`.provisional_open_day_fill(day_start_ns, as_of_ns)?`。它只提交 non-final checkpoint，
普通 CacheOnly/coverage 仍报告缺口；checkpoint 的范围和 as-of 必须位于同一 TQBN 日分区。
远端明确成功结束的空增量也可推进 checkpoint，取消、超时或未确认结束则不可。后续运行应从
checkpoint 前 5 分钟重叠续填，并在 TQBN 18:00 分区结束后改走普通 final warmup 做全日重对账。
调用方若需要把 warmup 接入自己的轻量进度或调度器，可配置
`.on_remote_fill_telemetry(...)`：每检查一个 physical cache range 就先给出累计的 `Inspecting`
快照（已检查/总范围、命中、缺口和当前 physical symbol）；coverage inspection 完成后（远端模式已
取得 shared root gate）给出 `RemoteFillPlan`，随后按 physical cache symbol 给出低频生命周期快照。
流式更新每个 symbol 至多 500ms 一次，handler 在检查和填充路径同步调用，必须保持快速且不得做
终端 I/O 或阻塞网络。已有 `.on_remote_fill_progress(...)` 保持兼容；telemetry 额外提供检查、plan、
cursor、retry 和 split identity，适合 CLI/UI reducer，而不是策略热路径。

root gate 只区分普通并发操作与稳定维护：普通 warmup/RemoteOnMiss query 取 shared gate；refresh、stale
repair、verify、doctor 和真实 purge 取 exclusive gate。每个 `cache family × physical/logical cache
symbol` 另有跨进程 fill lease，竞争者等待并重查 coverage，避免相同 series 的重复远端补数；TQBN/
minute per-file lock 再保护物理文件。该 advisory 协议不保证新旧 binary 进程长期混跑，升级同一 root
的持续进程时应同步重启。

固定 cache root 的运维作业可选用 workspace 的 `tqsdk-cache` binary：它通过同一个
history client 路径为 tick/minute/daily 执行 `inventory`、`inspect`、`fill`、report-bound `verify`、
`doctor` 和显式 purge；daily fill 只请求官方 native 1d。closed-day fill 可按本地通用交易日历
选择最近 N 个已结束交易日；显式日期结束于当前日时自动把单次 horizon 固定为启动时刻减 5 秒，
严格任务可用 `--require-final` 拒绝当前日。CLI 将 JSON 保持在 stdout、进度保持在 stderr，
不改变 facade 默认行为，也不替代 live `MarketCachePolicy` recording；完整命令合同见
[回测 Tick Cache CLI](../../docs/architecture/backtest-tick-cache-cli.md)。

实盘或模拟盘运行时推荐用 `MarketCachePolicy` 一次声明共享 cache 目录和要维护的 tick
symbols，然后通过 `.market_cache(policy)` 挂到 live builder。symbol 集合可以用
`.record_ticks([...])` 显式列出，也可以用
`.record_universe("snapshot(contract:all;!CFFEX.*)")?`
复用共享期货 selector。facade 会在 `connect()` 后启动 tick recording；回测 builder 也能
复用同一个 policy 作为默认 cache 目录和 symbol 集合。
仍可显式调用 `.record_ticks(cache_dir, symbols).await?` 作为运行时入口。两种方式都只记录
policy 解析出的 symbol，不会自动记录所有订阅，也不会后台运行；正常策略继续调用 `next()` / `wait_update()`。
facade 在每次更新收集 rows，并按每 symbol 最多 `128` 行或约 `250 ms` 批量持久化，避免 fsync
阻塞策略热路径；首次初始化或失败重扫之外，每次 update 只解码变更集命中的 tick serial，首批、跳号和
正常 `Tq` 销毁时会立即 flush。`record_ticks_health()` 返回累计写入行数、
最近 flush、每个 symbol 的 last id 和 gap 状态；`recorded_market_cache_policy()` 可从当前 recording
health 派生补洞用 policy。coverage 只在 tick id 连续且 rows 已提交后推进；断线、跳号或异常退出前
未确认的尾部会留下缺口，后续仍可显式配合 `.auth_env()?`、`.warmup()` / `.remote_on_miss()` 补齐。

```rust
use tqsdk::prelude::*;

# async fn run(start_ns: i64, end_ns: i64) -> tqsdk::Result<()> {
let cache = MarketCachePolicy::new(".tqsdk/backtest_ticks")
    .record_universe("symbol:KQ.i@SHFE.au")?;

let warmup = Tq::futures()
    .auth_env()?
    .market_cache(cache.clone())
    .backtest(start_ns, end_ns)
    .remote_on_miss()
    .warmup()
    .await?;
assert!(warmup.symbols_total > 0);

let mut tq = Tq::futures()
    .auth_env()? // only needed when RemoteOnMiss has to fill missing cache ranges
    .market_cache(cache)
    .backtest(start_ns, end_ns)
    .default_price_tick(1.0)
    .kline("KQ.i@SHFE.au", std::time::Duration::from_secs(60), 200)?
    .remote_on_miss()
    .connect()
    .await?;
# Ok(())
# }
```

`.replay_backtest(source)` 是高级入口，用于测试 fixture、caller-owned 数据源或小型显式
replay source。它仍支持 `.quote_symbol(...)`、`.price_tick(...)`、
`.instrument_spec(...)` 和 `.default_price_tick(...)` 这类本地 replay metadata。
如果已经通过 `tqsdk-session` 查询到合约 metadata，可以把 `InstrumentSpec` 传给
`.instrument_spec(...)`，让本地 kline replay 自动获得 `price_tick` 和合约乘数。
显式 `.backtest(...).disabled_cache()` 和 `.server_replay(date)?` 只接入官方历史行情 / 复盘行情，
不会绑定交易目标，也会拒绝 `.trade_target_*()`、`.tqkq_sim()` 和
`.trade_account(...)` / `.trade_account_env()` 等交易登录入口。需要策略下单并撮合成交的
回测闭环应使用默认 cache-backed `.backtest(...)` 或 `.replay_backtest(...)`。
服务端单日复盘可用 `.server_replay(date)?`：connect 时创建官方 replay session，
把返回的 `md_url` 接入正常行情 loop，并自动发送 replay heartbeat。复盘速度和
terminate 可通过 `Tq::set_replay_speed(...)` / `terminate_server_replay()` 显式控制。
本地回测结束前会 drain 已进入 runtime 的 task updates，因此 `TargetPos` 的
`execution_report()` 能看到最后一个 replay step 产生的本地模拟成交；需要类似
task channel 的增量消费时，可用 `execution_events_since(cursor)` /
`execution_trades_since(cursor)` 读取新执行事件和新成交。

## 示例

```rust
use tqsdk::prelude::*;

# async fn run() -> tqsdk::Result<()> {
let mut tq = Tq::futures()
    .auth_env()?
    .tqkq_sim()
    .connect()
    .await?;

let near = tq.quote("SHFE.rb2610").await?;
let far = tq.quote("SHFE.rb2701").await?;
let near_target = tq.target_pos_default("SHFE.rb2610")?;
let far_target = tq.target_pos_default("SHFE.rb2701")?;

while tq.next().await? {
    let spread = near.load()?.last_price - far.load()?.last_price;
    if spread > 250.0 {
        near_target.set(-1)?;
        far_target.set(1)?;
    } else if spread < 200.0 {
        near_target.close()?;
        far_target.close()?;
    }
}
# Ok(())
# }
```

## Features

- `default = ["live", "services"]`：默认用户入口，包含 live 连接与服务查询能力。
- `live`：向内部 `session` / `wait` / `task` / `data` crate 传播 live feature，并启用 TQ auth 派生的 TQKQ helper。
- `services`：向内部 crate 传播服务查询相关 HTTP 能力。
- `default-features = false`：保留 facade 类型和不依赖 live auth 的组合入口；live-only helper 不参与编译。

`tqsdk::advanced::*` 是 curated convenience，不是完整 sibling crate mirror。它只暴露默认 facade 常见下钻点：

```rust
use tqsdk::advanced::session::SessionClientBuilder;
use tqsdk::advanced::session::InstrumentSpec;
use tqsdk::advanced::runtime::RuntimeReader;
use tqsdk::advanced::task::replay::StrategyReplaySourceBuilder;
```

需要完整 task、data、session、wait 或 core surface 的用户应直接依赖对应 sibling crate。这样可以让 `tqsdk` 的 semver surface 保持小，同时不限制高级用户使用底层能力。
需要多消费者 async 消费层的用户应基于 `tqsdk-session` 与 `RuntimeReader` / `UpdateCursor`
自建 facade；普通单 owner 策略仍应优先通过 `tqsdk::prelude::*` / `Tq::next()`
或直接使用 `tqsdk-wait`。

## 边界

`tqsdk` 不拥有第二棵状态树，不复制 direct query、task 或 data
实现。能力归属仍然保持在内部 crate：

- direct query / metadata：`tqsdk-session`
- single-owner `wait_update()`：`tqsdk-wait`
- async multi-consumer facade：调用方基于 `tqsdk-session + RuntimeReader/UpdateCursor` 自建
- execution tooling：`tqsdk-task`
- research/offline data：`tqsdk-data`
