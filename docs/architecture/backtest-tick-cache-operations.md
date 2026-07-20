# 回测 Tick 持久缓存预热与验收

## 适用范围

本文档说明如何为 `tqsdk` 的 cache-backed local backtest 补齐历史 tick，并确认缓存可被
严格本地回放。它适用于直接 symbol、`KQ.i@...` 指数，以及经映射解析后的 `KQ.m@...` 主连。

这不是专业历史下载流程。缺失数据由官方 server-side backtest market stream 提供，因此远端
填充需要 `TQ_AUTH_USER` / `TQ_AUTH_PASS`，但不需要 `tq_dl` 或专业历史下载权限。完整缓存的
`CacheOnly` 回测不需要 auth。历史原始格式和路径合同见
[History Cache Format](history-cache-format.md)。

固定 cache root 的 operator 作业可使用可选 [`tqsdk-cache` CLI](backtest-tick-cache-cli.md)。
它复用本文相同的 remote-on-miss / CacheOnly 语义，并额外提供 root lock、JSON report 和
deep TQBN doctor；它不是 relay、守护进程或另一套缓存格式。

只在用户明确授权连接远端并写入目标 cache root 后执行远端预热。预热只读行情，不登录交易账户，
也不会提交订单。

## 成功标准

一次完整的填充必须同时满足：

1. `RemoteOnMiss` 报告中所有目标 symbol 的 `after` coverage complete，且
   `symbols_missing == 0`。
2. 对同一 cache root、symbol/universe 和时间窗口运行 `CacheOnly` 后，
   `symbols_missing == 0`。
3. 在预期有行情的窗口实际回放 tick，得到非零 replay tick 数。

`.tqbn` 文件存在、日分区数量正确或 `rows_written > 0` 都只是辅助信号。已完整缓存的重复运行
可以合法地得到 `rows_written == 0` 和 `remote_used == false`。

## 1. 选择 cache root、symbol 与完成窗口

- 默认共享 root 为 `$HOME/.tqsdk/data_series_1`，可由 `TQSDK_HISTORY_CACHE_DIR` 覆盖；
  长期生产作业应显式传递 `cache_dir(...)`，避免环境差异。
- 使用官方交易日历（`DataClient::query_trading_days(...)`）选择“最近 N 个交易日”，不要把
  N 个工作日当作交易日。休市日的空覆盖分区是正常结果。
- 只填到最后一个已结束交易日。盘中或尚未经过尾部确认的交易日不能视为完整缓存。
- `KQ.i@...` 直接按 index symbol 缓存；`KQ.m@...` 会按日期解析到具体合约并共享具体合约的
  tick 文件。不要用一个 symbol 的 coverage 推断另一个 symbol 完整。
- 对 SHFE 贵金属等夜盘品种，常用窗口从首个交易日前一天 `18:00:00` CST 开始，到最后交易日
  `15:00:01` CST 结束。其他市场必须以合约 `trading_time` 为准。

同一 cache root 同时只运行一个远端 warmup owner。TQBN 文件锁保证写入互斥，但不会去重多个
进程发出的远端补数请求。

## 2. 预检已有 coverage

对显式 symbol，可先用同一 builder 的 `inspect_cache()` 查看当前 coverage、文件路径和缺口：

```rust
use tqsdk::prelude::*;

let status = Tq::futures()
    .backtest(start_ns, end_ns)
    .cache_dir(cache_dir)?
    .tick(symbol, 1_024)
    .inspect_cache()?;

println!("{status:#?}");
```

不要手工删除或编辑 `.tqbn`。正常补缺使用 `RemoteOnMiss`；只有用户明确要求全量刷新时才使用
`.refresh()` 或 `.purge_cache_symbols()`。

## 3. 用 SDK 增量预热

这是 SDK 客户端的推荐路径。`RemoteOnMiss` 会跳过完整 coverage，只请求和写入每个物理 cache
symbol 的 `missing_ranges`。

```rust
use tqsdk::prelude::*;

const SYMBOL: &str = "KQ.i@SHFE.ag";
const CACHE_DIR: &str = "/var/lib/tqsdk/history";

async fn warmup(start_ns: i64, end_ns: i64) -> tqsdk::Result<()> {
    let report = Tq::futures()
        .auth_env()?
        .backtest(start_ns, end_ns)
        .cache_dir(CACHE_DIR)?
        .universe(format!("symbol:{SYMBOL}"))?
        .remote_on_miss()
        .warmup()
        .await?;

    println!(
        "symbols={} skipped={} filled={} missing={} rows={} remote={}",
        report.symbols_total,
        report.symbols_skipped,
        report.symbols_filled,
        report.symbols_missing,
        report.rows_written,
        report.remote_used,
    );
    assert_eq!(report.symbols_missing, 0);
    assert!(report.symbols.iter().all(|entry| entry.after.is_complete()));
    Ok(())
}
```

`batch_size(...)` 只保留兼容报告 hint，不再串行切远端任务。多 symbol 的网络并发由
`TQSDK_REMOTE_FILL_SYMBOL_CONCURRENCY` 控制，合并会话大小由
`TQSDK_REMOTE_FILL_SYMBOL_BATCH_SIZE` 控制；未做基准测试时保持默认值。

长区间正常以持续进展为准。默认 60 秒无 tick 进展会触发保护；可按作业环境设置
`TQSDK_REMOTE_FILL_IDLE_TIMEOUT_SECS`。`TQSDK_REMOTE_FILL_BATCH_TIMEOUT_SECS` 默认关闭，
只有需要强制墙钟预算或诊断时才设置，并应留出足够余量。

## 4. 用 CacheOnly 和实际回放验收

预热成功后，必须在不提供 auth 的条件下验证相同窗口。第一步验证 coverage，第二步实际消费
缓存中的 tick：

```rust
use tqsdk::prelude::*;

const SYMBOL: &str = "KQ.i@SHFE.ag";
const CACHE_DIR: &str = "/var/lib/tqsdk/history";

async fn verify(start_ns: i64, end_ns: i64) -> tqsdk::Result<()> {
    let coverage = Tq::futures()
        .backtest(start_ns, end_ns)
        .cache_dir(CACHE_DIR)?
        .universe(format!("symbol:{SYMBOL}"))?
        .cache_only()
        .warmup()
        .await?;
    assert_eq!(coverage.symbols_missing, 0);
    assert!(coverage.symbols.iter().all(|entry| entry.after.is_complete()));

    let mut tq = Tq::futures()
        .backtest(start_ns, end_ns)
        .cache_dir(CACHE_DIR)?
        .universe(format!("symbol:{SYMBOL}"))?
        .cache_only()
        .tick(SYMBOL, 1_024)
        .connect()
        .await?;
    while tq.next().await? {}

    let replay_ticks = tq
        .backtest_summary()
        .map(|summary| summary.tick_count())
        .unwrap_or_default();
    assert!(replay_ticks > 0, "expected ticks in the requested trading range");
    Ok(())
}
```

当窗口本来没有任何行情时，最后一项应改为检查预期的空 coverage，而不是强制 replay 非空。

## 5. 仓库内端到端 runner

仓库维护者可使用 [smoke_market_cache_e2e.py](../../scripts/smoke_market_cache_e2e.py)，它会依次
执行远端 warmup、CacheOnly warmup 和 CacheOnly replay。它默认会删除目标 cache directory，
因此操作共享缓存时必须传 `--keep-cache`。

以下是本次白银指数 60 个交易日填充的真实参数。交易日为 `2026-04-21` 到 `2026-07-17`；由于
按日分区，最终包含 64 个目录日期，其中休市日为空 coverage：

```bash
TQSDK_REMOTE_FILL_IDLE_TIMEOUT_SECS=180 \
TQSDK_REMOTE_FILL_BATCH_TIMEOUT_SECS=3600 \
python3 scripts/smoke_market_cache_e2e.py \
  --symbols KQ.i@SHFE.ag \
  --start-ns 1776679200000000000 \
  --end-ns 1784271601000000000 \
  --batch-size 10000 \
  --cache-dir "$HOME/.tqsdk/data_series_1" \
  --keep-cache \
  --profile release \
  --timeout-secs 3600 \
  --min-rows 1
```

该次运行输出：

```text
E2E_OK remote_rows=2691170 remote_missing=0 cache_only_missing=0 replay_ticks=3917033 live_updates=0 process_s=790.211 warnings=
```

远端写入数小于 replay tick 数是预期行为：执行前 cache 已有部分日分区，`RemoteOnMiss` 只补了
缺口。重跑同一命令时，若 cache 已完整，应把 `--min-rows` 改为 `0`，并预期
`remote_rows=0`、`remote_used=false`。

## 6. 运行后维护

- 用 `MarketCachePolicy::record_ticks(...)` 或 `.record_universe(...)` 接入 live 策略，持续把
  显式声明的 tick 追加到同一 root。策略必须持续调用 `next()` / `wait_update()`。
- 观察 `record_ticks_health()`；断线、跳号或未确认尾部会保留 coverage gap，后续由同一 warmup
  owner 再次补齐。
- 多个策略消费者使用相同 root 加 `.cache_only()`，避免重复远端下载。不要把 relay 的内存缓存当作
  canonical historical cache。
- `duration <= 60s` 的 K 线从 tick 合成；`duration > 60s` 使用独立的 native K 线
  `HistorySeriesCache` 路径，不能用本流程的 tick coverage 代替。

## 常见问题

| 现象 | 处理 |
| --- | --- |
| `CacheOnly` 报缺口 | 不删除已有文件；确认 range、symbol 和 root 后重新运行 `RemoteOnMiss` warmup。 |
| 远端填充无进展 | 检查 auth、官方服务连接、目标是否为已结束交易日；再根据实际任务预算调整 idle 或 batch timeout。 |
| 盘中日被当成完整数据 | 不要把当前交易日放入预热范围；等该交易日结束后再补。 |
| 日文件数与交易日数不同 | 正常。夜盘归属下一交易日，休市日可以有空 coverage 分区；以 CacheOnly coverage 为准。 |
| `KQ.i` 与 `KQ.m` 结果不同 | 正常。前者是 index symbol，后者按日期解析到具体合约；分别检查报告。 |
| 想重新下载全部数据 | 这是破坏性维护操作；先取得用户确认，再显式使用 `.refresh()` 或 purge。 |

相关入口：

- [持久缓存预热 contract example](../../crates/tqsdk/examples/api_contract_s45_facade_backtest_cache_warmup.rs)
- [`tqsdk` facade cache 语义](../../crates/tqsdk/README.md)
- [TQBN daily v2 格式合同](history-cache-format.md)
