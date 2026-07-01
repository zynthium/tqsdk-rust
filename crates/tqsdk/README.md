# tqsdk

`tqsdk` 是 `tqsdk-rust` 的默认用户入口。它不物理合并内部 crate，也不改变
runtime contract；它只提供一个更容易开始的 facade：

- `tqsdk::prelude::*`
- `Tq::new()` (and `Tq::futures()` alias)
- Cache-backed local simulated backtest (`.backtest(start_ns, end_ns)`, `.cache_dir(...)`, `.cache_only()`, `.remote_on_miss()`, `.universe(...)`)
- Explicit live tick recording into the shared backtest cache (`.record_ticks(cache_dir, symbols)`)
- Market-data-only server-side backtest (`.server_backtest()`, optional `.replay_url(...)`)
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

`.backtest(start_ns, end_ns)` 是默认 Python-style 本地撮合回测入口。它通过
`BacktestTickCache` 复用 `tqsdk-data::HistorySeriesCache` 的持久 tick 缓存，并把 tick
流式回放到本地 `TqSim`。显式 `.cache_only()` 只读本地缓存；默认
`RemoteOnMiss` 在缓存完整时直接复用本地数据且不需要 auth，缓存缺失时通过官方
server-side backtest market stream 拉取 tick、推进本地回测并写入持久缓存。这个路径不使用
专业历史下载接口，也不需要专业历史下载权限。`.universe(...)` 使用和 relay 对齐的期货
selector 语法，适合全品种策略。

缓存运维入口保留在同一个 builder 心智里：`.inspect_cache()` 返回每个显式 symbol 的
backend、文件路径、覆盖区间和缺口；`.purge_cache_symbols()` 删除这些 symbol 的 tick
缓存文件。`.warmup().await?` 只预热缓存、不创建策略 runtime；它会按 `.batch_size(n)`
分批跳过完整缓存、用官方 server-side backtest 流按 2 小时时间片补缺口，补齐成功后只
compact 本次 symbol 的 tick 文件，并返回每个 symbol 的报告。
`.refresh()` 会在准备远端补齐前先按 symbol tick 文件粒度清空旧缓存。

实盘或模拟盘运行时可以显式调用 `.record_ticks(cache_dir, symbols).await?`，把指定合约的
tick serial 写入同一份 `BacktestTickCache`。它不会自动记录所有订阅，也不会后台运行；
正常策略继续调用 `next()` / `wait_update()`，facade 在每次更新后把新 tick 行追加到缓存。
coverage 只在 tick id 连续时推进；断线、跳号或程序退出前未确认的尾部会留下缺口，后续仍可用
`.warmup()` / `.remote_on_miss()` 补齐。

```rust
use tqsdk::prelude::*;

# async fn run(start_ns: i64, end_ns: i64) -> tqsdk::Result<()> {
let warmup = Tq::futures()
    .auth_env()?
    .backtest(start_ns, end_ns)
    .cache_dir(".tqsdk/backtest_ticks")?
    .universe("active:all;!CFFEX")?
    .remote_on_miss()
    .batch_size(20)
    .warmup()
    .await?;
assert!(warmup.symbols_total > 0);

let mut tq = Tq::futures()
    .auth_env()? // only needed when RemoteOnMiss has to fill missing cache ranges
    .backtest(start_ns, end_ns)
    .cache_dir(".tqsdk/backtest_ticks")?
    .universe("active:all;!CFFEX")?
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
服务端 `.server_backtest(...)` 和 `.server_replay(date)?` 只接入官方历史行情 / 复盘行情，
不会绑定交易目标，也会拒绝 `.trade_target_*()`、`.tqkq_sim()` 和
`.trade_account(...)` / `.trade_account_env()` 等交易登录入口。需要策略下单并撮合成交的
回测闭环应使用 `.backtest(...)` 或 `.replay_backtest(...)`。
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
