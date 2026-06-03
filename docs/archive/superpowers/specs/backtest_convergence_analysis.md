# 回测 API 收敛分析：Python 官方设计 vs Rust 现状

## 一、Python SDK 的设计：一个入口，两个正交轴

Python 官方 SDK 的回测只有 **一条用户路径**：

```python
# 唯一入口：TqApi 构造函数的两个正交参数
api = TqApi(
    account=TqSim(),                                        # 轴1: 撮合账户
    backtest=TqBacktest(start_dt=date(2018,5,1),            # 轴2: 行情来源
                        end_dt=date(2018,10,1)),
    auth=TqAuth("user", "pass")
)

# 策略主体 — 与实盘代码完全一致
klines = api.get_kline_serial("DCE.m1901", 5*60, data_length=15)
target_pos = TargetPosTask(api, "DCE.m1901")
while True:
    api.wait_update()
    if api.is_changing(klines):
        ...
```

### Python 的正交分解

| 维度 | 可选值 | 说明 |
|------|--------|------|
| **`account`** (撮合引擎) | `TqSim` / `TqSimStock` / `TqAccount` / `TqKq` / ... | 模拟or实盘，**回测时强制 TqSim** |
| **`backtest`** (行情来源) | `None` / `TqBacktest(start, end)` / `TqReplay(date)` | None=实盘行情，TqBacktest=时间段回测，TqReplay=单日复盘 |

> [!IMPORTANT]
> Python SDK 也有两个回测相关类型（`TqBacktest` + `TqReplay`），但它们都只是 **`TqApi` 构造函数的一个参数**。
> 用户的策略代码 (`wait_update` / `get_kline_serial` / `TargetPosTask`) 始终是同一套——**零分支**。

### Python 设计的核心约束

1. **单一 `TqApi` 入口** — 不管 live/sim/backtest/replay，都是同一个 class、同一个 constructor
2. **策略 body 零分支** — 策略逻辑不知道自己跑在 live 还是 backtest
3. **撮合 vs 行情正交** — `account` 和 `backtest` 是独立参数，组合得到模式
4. **`TqSim` 是内部撮合** — Python 的 TqSim 直接在客户端做限价穿价全成撮合
5. **无离线回测** — Python 的 `TqBacktest` 必须连服务端拉取行情

---

## 二、Rust 现状的 4 条路径映射

| Rust 路径 | 对应 Python 概念 | 差异点 |
|-----------|-----------------|--------|
| ① Wait 服务端回测 | ≈ `TqApi(backtest=TqBacktest(...))` | **最接近 Python 官方设计** |
| ② StrategyReplay | Python 无对应 | Rust 独有：纯离线 + FakeBroker |
| ③ StrategyBacktest | ≈ 本地 `TqSim` 但用离线数据 | Rust 独有：纯离线 + Python TqSim 语义 |
| ④ Deployment Replay | Python 无对应 | Rust 独有：统一生命周期管理 |

### 关键发现

> [!NOTE]
> Python 没有路径②③④的原因很简单：**Python 没有离线回测能力**。Python 的 `TqBacktest` 每次回测都必须连接
> `wss://backtest.shinnytech.com` 拉取行情数据。Rust SDK 通过 `tqsdk-data` 实现了本地历史数据缓存和
> `MarketCacheReplay`，这是 Python 不具备的能力维度。

---

## 三、是否需要收敛到唯一路径？

### 回答：**对外用户路径需要收敛，内部分层应保留**

理由分析：

```
┌──────────────────────────────────────────────────────────┐
│ 用户看到的应该是：                                          │
│                                                          │
│   TqApi (tqsdk facade)                                   │
│     .backtest(start, end)    ← 服务端回测                  │
│     .local_backtest(cache)   ← 本地离线回测                 │
│     .replay(events)          ← 历史事件回放                 │
│                                                          │
│   策略 body：                                             │
│     api.step() / api.quote() / api.kline()               │
│     — 与 live 完全一致                                     │
│                                                          │
└──────────────────────────────────────────────────────────┘
          │
          │ 内部实现分层（用户不需要感知）
          ▼
┌──────────────────────────────────────────────────────────┐
│  tqsdk-wait:  TqBacktest pump / backtest driver          │
│  tqsdk-task:  StrategyReplay / StrategyBacktest / TqSim  │
│  tqsdk-task:  StrategyEnvironment / Deployment           │
│  tqsdk-data:  MarketCacheReplay                          │
│  tqsdk-core:  CommitScope::ReplayStep / adapter          │
└──────────────────────────────────────────────────────────┘
```

### 保留内部分层的理由

| 现有路径 | 为什么不能删 | 对应能力 |
|---------|------------|---------|
| ② StrategyReplay | 是离线回测的 runtime substrate | checkpoint 恢复、speed control、multi-series merge |
| ③ StrategyBacktest | 是 Python TqSim 语义的实现 | 限价穿价全成、资金不足拒单 |
| ④ Deployment/Environment | 是生产级策略执行的统一封装 | supervisor、retry、graceful shutdown |

这些是不同抽象层的实现，**不是重复功能**。问题不在于路径多，而在于：

> [!WARNING]
> 当前 4 条路径全部直接暴露给用户，用户需要自己选择 `StrategyReplay` 还是 `StrategyBacktest` 还是
> `TqApiBuilder::futures_backtest()`，甚至需要了解 `FakeBroker` vs `TqSim` 的区别。
> 这违反了 Python SDK "一个入口 + 正交参数" 的设计哲学。

---

## 四、建议的收敛方案

### 4.1 用户视角：`tqsdk` facade 提供统一入口

参照 Python 的 `TqApi(account=..., backtest=...)` 模式：

```rust
// ========================
// 路径 A: 服务端回测（≈ Python TqBacktest）
// ========================
let api = TqApi::builder("user", "pass")
    .backtest(start_ns, end_ns)       // 自动切换到 backtest adapter
    .build().await?;

// ========================
// 路径 B: 本地离线回测（Rust 独有能力）
// ========================
let cache = MarketCacheReplay::from_parquet("data/rb2501.parquet")?;
let api = TqApi::builder("user", "pass")
    .local_backtest(cache)            // 自动使用 TqSim 撮合
    .build().await?;

// ========================
// 策略 body — 两种模式完全一致
// ========================
let quote = api.quote("SHFE.rb2501").await?;
let bars = api.kline("SHFE.rb2501", Duration::from_secs(60), 32).await?;
while let Some(step) = api.step().await? {
    if step.is_changing(&quote) { ... }
    if step.is_changing(&bars) { ... }
}
```

### 4.2 两轴正交设计

| 轴 | Python 等价 | Rust builder method |
|----|-----------|-------------------|
| 行情来源 | `backtest=TqBacktest(...)` / `TqReplay(...)` / `None` | `.backtest(start, end)` / `.local_backtest(cache)` / 默认 live |
| 撮合引擎 | `account=TqSim()` / `TqAccount(...)` | `.sim()` (默认) / `.trade_target(broker, account)` |

### 4.3 内部路由（用户不感知）

```
TqApi::builder()
  .backtest(start, end)
  └─→ 内部: TqApiBuilder::futures_backtest()         [tqsdk-wait 路径①]

TqApi::builder()
  .local_backtest(cache)
  └─→ 内部: StrategyBacktest::builder(cache)          [tqsdk-task 路径③]
       └─→ TqSim 撮合

TqApi::builder()
  .local_backtest(cache)
  .replay_speed(StrategyReplaySpeed::REAL_TIME)
  └─→ 内部: StrategyReplay::builder(cache)            [tqsdk-task 路径②]
       └─→ FakeBroker 撮合

// Deployment/Supervisor (路径④) 保留给高级用户
// 通过 tqsdk-task 直接引用，不进入 tqsdk facade 的 "simple" API
```

### 4.4 收敛后的暴露层级

```
┌───────────────────────────────────────────────────────────┐
│ tqsdk (facade)                                           │
│   TqApi::builder().backtest() / .local_backtest()        │
│   → 普通用户唯一入口，live/backtest same body             │
├───────────────────────────────────────────────────────────┤
│ tqsdk-wait (中级)                                        │
│   TqApiBuilder::futures_backtest() / .stock_backtest()   │
│   → 需要精细控制 wait-style backtest pump 的用户          │
├───────────────────────────────────────────────────────────┤
│ tqsdk-task (高级)                                        │
│   StrategyReplay / StrategyBacktest / TqSim              │
│   StrategyDeployment / StrategySupervisor                │
│   → 需要 checkpoint、supervisor、自定义 broker 的高级用户  │
└───────────────────────────────────────────────────────────┘
```

---

## 五、与 Python 设计的对齐总结

| 对齐项 | Python 现状 | Rust 建议 |
|--------|-----------|----------|
| 单一入口 class | `TqApi(...)` | `TqApi::builder().build()` |
| 回测=构造参数 | `backtest=TqBacktest(...)` | `.backtest(start, end)` |
| 策略 body 零分支 | `wait_update()` + `is_changing()` | `step()` + `is_changing()` |
| 本地模拟撮合 | `account=TqSim()` | `.sim()` (默认) |
| 离线回测 | ❌ 无 | `.local_backtest(cache)` — **Rust 独有增量** |
| 高级定制 | ❌ 无 | `tqsdk-task` 直接引用 — **Rust 独有增量** |

---

## 六、开放问题

1. **`local_backtest` 默认用 `TqSim` 还是 `FakeBroker`？**
   - 建议：默认 `TqSim`（Python 兼容语义），高级用户可通过 `.broker(FakeBroker::new())` 覆盖

2. **服务端回测是否需要同时支持 `TqBacktest`（时间段）和 `TqReplay`（单日复盘）？**
   - Python 两者都支持，建议对齐

3. **`tqsdk` facade 是否需要暴露 `StrategyBacktestSummary`？**
   - 建议：提供 `.summary()` 但隐藏实现细节

4. **是否在 `crate-blueprint.md` 建议的 `tqsdk-backtest` 独立 crate 时机做这个收敛？**
   - 当前复杂度看，可以先在 `tqsdk` facade 层做薄封装，不急于独立 crate

5. **路径④（Deployment/Supervisor）是否应进入 `tqsdk` facade？**
   - 建议：不进入。这是 power-user API，直接引用 `tqsdk-task` 即可
