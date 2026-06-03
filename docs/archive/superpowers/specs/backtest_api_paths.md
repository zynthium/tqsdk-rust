# 策略回测 API 路径总览

目前仓库中策略回测有 **4 条** 不同层级 / 不同场景的 API 路径：

---

## 路径一览表

| # | 路径名称 | 入口 Crate | 核心入口类型 | 撮合方式 | 数据来源 | 典型场景 |
|---|---------|-----------|-------------|---------|---------|---------|
| 1 | **Wait 服务端回测** | `tqsdk-wait` | `TqApiBuilder::futures_backtest()` | 服务端 backtest adapter | 服务端推送 | Python 风格、live/backtest 同一策略体 |
| 2 | **Task StrategyReplay** | `tqsdk-task` | `StrategyReplay::builder()` | FakeBroker (测试级) | `MarketCacheReplay` (本地缓存/历史) | 离线历史 K 线/Tick 回放、checkpoint 恢复 |
| 3 | **Task StrategyBacktest** | `tqsdk-task` | `StrategyBacktest::builder()` | `TqSim` (Python 兼容本地撮合) | `MarketCacheReplay` (本地缓存/历史) | Python `TqSim` 风格纯本地回测 |
| 4 | **Task Deployment/Environment Replay** | `tqsdk-task` | `StrategyEnvironment::from_replay_builder()` | FakeBroker / 可配置 | `StrategyReplayBuilder` | live/sim/replay 统一部署切换 |

---

## 路径 1: Wait 服务端回测 (`tqsdk-wait`)

> [TqApiBuilder](file:///Users/joeslee/Projects/GitHub/tqsdk-rust/crates/tqsdk-wait/src/builder.rs#L11-L57) + [TqBacktest](file:///Users/joeslee/Projects/GitHub/tqsdk-rust/crates/tqsdk-wait/src/backtest.rs#L24-L82)

```rust
let api = TqApiBuilder::new(user, pass)
    .futures_backtest(start_datetime_ns, end_datetime_ns)?
    .build()
    .await?;

while let Some(step) = api.step().await? {
    // 与 live 完全相同的策略体
}
```

**特点**：
- 连接真实天勤服务端的 backtest adapter，由服务端按时间推送回测行情
- live/backtest 同一 `TqApi` + `step()` 循环，策略体无分支
- 支持 `futures_backtest` 和 `stock_backtest` 两种市场
- 内部有 [BacktestPump](file:///Users/joeslee/Projects/GitHub/tqsdk-rust/crates/tqsdk-wait/src/backtest.rs#L98-L102) 负责 tick 分页拉取与合成
- **合约示例**: [api_contract_s36](file:///Users/joeslee/Projects/GitHub/tqsdk-rust/crates/tqsdk-wait/examples/api_contract_s36_wait_live_backtest_same_body.rs)

---

## 路径 2: Task StrategyReplay (`tqsdk-task`)

> [StrategyReplay](file:///Users/joeslee/Projects/GitHub/tqsdk-rust/crates/tqsdk-task/src/replay.rs#L41-L49) + [StrategyReplayBuilder](file:///Users/joeslee/Projects/GitHub/tqsdk-rust/crates/tqsdk-task/src/replay.rs#L22-L32)

```rust
let replay = StrategyReplay::source_builder()
    .events(series.into_market_cache_events("history")?)
    .build();

let mut strategy = StrategyReplay::builder(replay)
    .market(FakeMarket::new().account("sim", 100_000.0))
    .broker(FakeBroker::new().fill_all())
    .account("sim")
    .kline(symbol, duration, 64)
    .speed(StrategyReplaySpeed::FASTEST)
    .build().await?;

while let Some(mut ctx) = strategy.next().await? {
    ctx.quote(symbol)?;
    ctx.kline(symbol, duration)?;
    ctx.orders("sim").buy_open(symbol, 1).limit(price).send_once("id").await?;
    ctx.finish_test_step().await?;
}
```

**特点**：
- 纯离线，不连接服务端；数据来自 `MarketCacheReplay`（可从 `DataClient` 拉取后转换）
- 使用 `FakeBroker` 做成交模拟（测试级 fill 策略）
- 支持 quote / kline / tick 三种市场数据
- 支持 [StrategyReplaySpeed](file:///Users/joeslee/Projects/GitHub/tqsdk-rust/crates/tqsdk-task/src/replay.rs#L74-L77)（`FASTEST` / `REAL_TIME` / `scaled()`）控制回放节奏
- 支持 [StrategyReplayCheckpoint](file:///Users/joeslee/Projects/GitHub/tqsdk-rust/crates/tqsdk-task/src/replay.rs#L61-L65) + [StrategyReplayCheckpointStore](file:///Users/joeslee/Projects/GitHub/tqsdk-rust/crates/tqsdk-task/src/replay.rs#L68-L71) 做断点恢复
- 支持 [StrategyReplaySourceBuilder](file:///Users/joeslee/Projects/GitHub/tqsdk-rust/crates/tqsdk-task/src/replay.rs#L36-L38) 合并多个序列
- **合约示例**: [api_contract_s16](file:///Users/joeslee/Projects/GitHub/tqsdk-rust/crates/tqsdk-task/examples/api_contract_s16_history_replay_strategy.rs)

---

## 路径 3: Task StrategyBacktest (`tqsdk-task`)

> [StrategyBacktest](file:///Users/joeslee/Projects/GitHub/tqsdk-rust/crates/tqsdk-task/src/backtest.rs#L26-L33) + [StrategyBacktestBuilder](file:///Users/joeslee/Projects/GitHub/tqsdk-rust/crates/tqsdk-task/src/backtest.rs#L18-L23) + [TqSim](file:///Users/joeslee/Projects/GitHub/tqsdk-rust/crates/tqsdk-task/src/sim.rs)

```rust
let mut backtest = StrategyBacktest::builder(replay)
    .sim(TqSim::new().with_margin(symbol, 1_000.0))
    .quote(symbol)
    .price_tick(symbol, 1.0)
    .build().await?;

while let Some(mut ctx) = backtest.next().await? {
    ctx.quote(symbol)?;
    ctx.orders("TQSIM").buy_open(symbol, 1).limit(price).send_once("id").await?;
    ctx.finish_sim_step()?;
}

let summary = backtest.summary();
```

**特点**：
- 纯离线 + Python `TqSim` 兼容的本地撮合（限价单穿过对手价时全部成交）
- 不使用 `FakeBroker`，用 `TqSim` 做 Python 语义的模拟账户
- 支持 quote / tick / kline 三种市场事件
- 提供 [StrategyBacktestSummary](file:///Users/joeslee/Projects/GitHub/tqsdk-rust/crates/tqsdk-task/src/backtest.rs#L46-L55)（事件计数、订单、成交、最终账户/持仓快照）
- Kline 回测需要 `price_tick()` 合成 ask/bid（High→Low→Close 三步回测法）
- **合约示例**: [api_contract_s32](file:///Users/joeslee/Projects/GitHub/tqsdk-rust/crates/tqsdk-task/examples/api_contract_s32_python_backtest_sim.rs)

---

## 路径 4: Task Deployment/Environment Replay (`tqsdk-task`)

> [StrategyEnvironment](file:///Users/joeslee/Projects/GitHub/tqsdk-rust/crates/tqsdk-task/src/environment.rs#L19-L22) + [StrategyDeploymentConfig](file:///Users/joeslee/Projects/GitHub/tqsdk-rust/crates/tqsdk-task/src/deployment.rs) + [StrategySupervisor](file:///Users/joeslee/Projects/GitHub/tqsdk-rust/crates/tqsdk-task/src/lib.rs#L53-L61)

```rust
let environment = StrategyEnvironment::from_replay_builder(replay_builder)
    .account(account_id)
    .quote(symbol)
    .build().await?;

let deployment = StrategyDeployment::from_environment(environment)
    .lifecycle(StrategyLifecycle::new().max_steps(1))
    .build().await?;

let mut supervisor = StrategySupervisor::new(deployment)
    .shutdown_signal(StrategyShutdownSignal::ctrl_c())
    .retry_policy(StrategyRetryPolicy::new().max_retries(1));

supervisor.run(|ctx| { /* 统一的 StrategyEnvironmentContext */ }).await?;
```

**特点**：
- 把路径 2（`StrategyReplay`）包装进 `StrategyEnvironment` enum，与 live/test TaskHost 共享同一 `StrategyEnvironmentContext`
- 策略代码通过 `StrategyEnvironmentContext` 统一接口，**不区分 live / sim / replay**
- 叠加 `StrategySupervisor` 获得 lifecycle 管理、graceful shutdown、retry、health metrics
- **合约示例**: [api_contract_s15](file:///Users/joeslee/Projects/GitHub/tqsdk-rust/crates/tqsdk-task/examples/api_contract_s15_live_sim_replay_switch.rs)

---

## 对比矩阵

| 维度 | Wait 服务端回测 | StrategyReplay | StrategyBacktest | Deployment Replay |
|-----|---------------|---------------|-----------------|-------------------|
| **是否连接服务端** | ✅ 是 | ❌ 否 | ❌ 否 | ❌ 否 |
| **撮合引擎** | 服务端 | FakeBroker | TqSim (Python 兼容) | FakeBroker/可选 |
| **数据来源** | 服务端推送 | MarketCacheReplay | MarketCacheReplay | MarketCacheReplay |
| **Kline 支持** | ✅ | ✅ | ✅ (需 price_tick) | ✅ |
| **Tick 支持** | ✅ (分页拉取) | ✅ | ✅ | ✅ |
| **断点恢复** | ❌ | ✅ Checkpoint | ❌ | ✅ (通过 Replay) |
| **回放速度控制** | ❌ (服务端控制) | ✅ Speed policy | ❌ (始终最快) | ✅ (通过 Replay) |
| **与 live 代码统一** | ✅ 同一 TqApi | ❌ 独立入口 | ❌ 独立入口 | ✅ EnvironmentContext |
| **Supervisor/Lifecycle** | ❌ | ❌ | ❌ | ✅ |
| **回测报告** | ❌ | ❌ | ✅ Summary | ✅ SupervisorReport |
| **所属 Crate** | tqsdk-wait | tqsdk-task | tqsdk-task | tqsdk-task |
