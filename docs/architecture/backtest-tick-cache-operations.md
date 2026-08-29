# 回测 Tick 持久缓存预热与验收

## 适用范围

### 历史动态 universe

`active:all`、`main:all`、`cont:all` 与 `--universe` 都是当前时刻的静态 selector，不能用于推断历史已退市或后来上市的物理合约。动态回放须由调用方提供完整 `CatalogSnapshot`，离线编译为带版本、calendar identity 与 SHA-256 的 `HistoricalUniversePlan`，并显式给出 `UniverseBudget`。`Tq::futures().backtest(start, end).historical_universe_plan(plan)?` 只接受区间完全相等且哈希有效的计划；CacheOnly/RemoteOnMiss 覆盖检查与实际读 tick 均裁剪到每个物理合约的生命周期。当前 `tqsdk-cache fill --universe` 仍保持静态语义，动态计划的下载可按计划中的物理 interval 分别 warmup，不能把 `cont:all` 当作全历史物理合约集合。

本文档说明如何为 `tqsdk` 的 cache-backed local backtest 补齐历史 tick，并确认缓存可被
严格本地回放。它适用于直接 symbol、`KQ.i@...` 指数，以及经映射解析后的 `KQ.m@...` 主连。

这不是专业历史下载流程。缺失数据由官方 server-side backtest market stream 提供，因此远端
填充需要 `TQ_AUTH_USER` / `TQ_AUTH_PASS`，但不需要 `tq_dl` 或专业历史下载权限。完整缓存的
`CacheOnly` 回测不需要 auth。历史原始格式和路径合同见
[History Cache Format](history-cache-format.md)。

固定 cache root 的 operator 作业可使用可选 [`tqsdk-cache` CLI](backtest-tick-cache-cli.md)。
它复用本文相同的 remote-on-miss / CacheOnly 语义，并对 tick、canonical minute、native daily
提供统一 fill progress/schema-v3 report、inventory、inspect、verify、doctor 和受控 purge；它不是
relay、守护进程或另一套缓存格式。

K 线缓存遵循固定三层来源：tick 合成 `<60s`，canonical 60s minute 聚合 `60s..<1d`，native server 1d
聚合 `1d..=28d`。daily 缺口必须从官方 native 1d stream 补齐；CacheOnly 缺失时直接失败，不回退 minute。

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

显式日期结束于当前 TQBN 交易日时自动生成的盘中快照不满足上述“全日完整”标准。它成功时要求所有 physical
symbols 至少推进到同一个 `complete_through_ns`，report 使用
`coverage_state=provisional`、`day_complete=false`；普通 CacheOnly 仍应报告当前日缺口。
只有 TQBN 18:00 分区边界过去后再次运行，才会按普通 final coverage 全日重对账，并恢复以上
三项完成标准。

## 1. 选择 cache root、symbol 与完成窗口

- 默认共享 root 为 `$HOME/.tqsdk/data_series_1`，可由 `TQSDK_HISTORY_CACHE_DIR` 覆盖；
  长期生产作业应显式传递 `cache_dir(...)`，避免环境差异。
- 使用官方交易日历（`DataClient::query_trading_days(...)`）选择“最近 N 个交易日”，不要把
  N 个工作日当作交易日。休市日的空覆盖分区是正常结果。
- 固定 root 的 operator 作业可以让 `tqsdk-cache fill --last-trading-days N --calendar auto`
  管理这一步：它优先复用
  `<cache-root>/meta/trading-calendar-holidays-v1/active.json` 指向的 immutable raw-holiday
  snapshot；本地 snapshot 不覆盖所需年份时才拉取公开日历。旧
  `<cache-root>/meta/trading-calendar-v1.json` 不参与选择。日历只决定 selector 和进度分母，
  不能替代 coverage。
- `--last-trading-days` 只选择已结束交易日。显式指定
  `--start-day/--end-day <当前交易日>` 时自动写 provisional checkpoint，不能视为完整缓存；
  必须拒绝盘中日的严格任务使用 `--require-final`。`--include-open-day` 仅作为兼容参数保留，
  不与 `--last-trading-days` 或 `--require-final` 组合。
- `KQ.i@...` 直接按 index symbol 缓存；`KQ.m@...` 会按日期解析到具体合约并共享具体合约的
  tick 文件。不要用一个 symbol 的 coverage 推断另一个 symbol 完整。
- 对 SHFE 贵金属等夜盘品种，常用窗口从首个交易日前一天 `18:00:00` CST 开始，到最后交易日
  `15:00:01` CST 结束。其他市场必须以合约 `trading_time` 为准。

同一 cache root 的普通 `RemoteOnMiss` warmup 共享 root gate，因此不同 series 可以并发；refresh、
stale repair、verify、doctor 和真实 purge 使用 exclusive gate。每个实际缺口再由
`cache family × cache symbol` 的跨进程 fill lease 串行化：竞争进程等待 owner，并在取得 lease 前后
重查 coverage，已由 owner 补齐时不再发重复远端请求。TQBN per-file lock 继续保护物理 append/compact。

对于长期运行的运维任务，推荐直接使用 CLI 的 closed-day selector，而不是由外层脚本倒推工作日：

```bash
TQ_AUTH_USER='your-account' TQ_AUTH_PASS='your-password' \
tqsdk-cache --cache-dir /var/lib/tqsdk/history fill \
  --universe 'main:all;index:all;!CFFEX' \
  --last-trading-days 60 --calendar auto \
  --progress-max-bars 8
```

盘中需要提前获得当前 TQBN 交易日快照时，固定本次启动时刻减 5 秒为 horizon：

```bash
TQ_AUTH_USER='your-account' TQ_AUTH_PASS='your-password' \
tqsdk-cache --cache-dir /var/lib/tqsdk/history fill \
  --symbol CZCE.AP610 \
  --start-day 2026-07-24 --end-day 2026-07-24
```

重复运行会从已持久化高水位前 5 分钟重新请求，利用 tick id 去重后向新 horizon 延伸。
checkpoint 的范围、高水位和 as-of 必须在同一 TQBN 日分区。远端明确 terminal 成功的空增量
可以推进 checkpoint；进程取消、超时或未确认结束时只保留已落盘 rows，不推进 checkpoint。
盘中续填不做全历史 compaction。TQBN 分区在 18:00 CST 后转为 closed day；同一命令再次运行时
不再使用 provisional checkpoint，而是请求尚缺的 final coverage，并在成功后 compact、淘汰 checkpoint。

tick 补洞按 trading day 顺序处理，接受 rows 以 8192 行缓冲后追加，避免逐事件持锁/fsync 和长窗口
全量 materialization。fill-only warmup 不回读刚写入的 rows；报告的 `rows_written` 是实际物理写入数，
同一 shared fill 被多个 logical request 复用时只累计一次，完整命中为 `0`。final 成功后只对本轮实际远端
回填的 `symbol × trading day` 范围去重 compact；provisional fill 跳过 compaction，等 closed-day reconcile。
交易日仍是 coverage/recovery checkpoint，不再是连接生命周期：同一有界 source lane 在显式 terminal 和
chart cleanup 成功后复用 session；取消、网络/协议错误或 cleanup 失败时销毁该 lane。

默认先动态显示 cache inspection 的 `已检查范围/总范围`、命中、缺口和当前 physical symbol，再显示
logical batch 和当前 active physical symbol 的 `完整接收日/待填日`。非交互任务可显式使用
`--progress auto` 降级为稳定的 stderr `key=value` 行，并使用 `--output-format json` 请求机器结果。
一个日只有在其 TQBN partition 已完整跨越或成功 terminal event 确认后才计入“完整接收”，因此夜盘和盘中尾部不会被提前计为完成。
`--calendar required` 禁止无日历 fallback，`--calendar off` 保留纯 partition 规划并拒绝
`--last-trading-days`。

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

需要由 SDK 调用方自行编排当前日时，可在 `.backtest(day_start_ns, as_of_ns)` 后调用
`.provisional_open_day_fill(day_start_ns, as_of_ns)?`。该配置只改变 warmup 的远端补缺与
checkpoint 提交方式，不改变普通 replay/CacheOnly coverage；调用方仍应固定单次运行的
`as_of_ns`，不能在同一次 warmup 中随墙钟漂移。

`batch_size(...)` 只保留兼容报告 hint，不再串行切远端任务。多 symbol 的网络并发由
`TQSDK_REMOTE_FILL_SYMBOL_CONCURRENCY` 控制，合并会话大小由
`TQSDK_REMOTE_FILL_SYMBOL_BATCH_SIZE` 控制；data client 最多保留 logical concurrency 个 clean source
lanes。pool 饱和时 overflow 不等待且不会回池。未做基准测试时保持默认值，也不要设置过小的
`TQSDK_REMOTE_FILL_SLICE_SECS`。

长区间正常以持续进展为准。默认 60 秒无 tick 进展会触发保护；可按作业环境设置
`TQSDK_REMOTE_FILL_IDLE_TIMEOUT_SECS`。`TQSDK_REMOTE_FILL_BATCH_TIMEOUT_SECS` 默认关闭，
只有需要强制墙钟预算或诊断时才设置，并应留出足够余量。

需要把相同计划/生命周期信息接入调用方 UI 或调度器时，可在 builder 上安装
`.on_remote_fill_telemetry(...)`。它在每个 physical range coverage inspection 后先发出累计
`Inspecting`（已检查/总范围、命中、缺口和当前范围），完成后发出 `PlanReady`，再按 physical symbol
发出开始、流式、重试、split、完成、失败或取消状态；流式事件每个 symbol 至多 500ms 一次，检查和
生命周期事件立即发出。handler 与检查和远端填充共享执行路径，必须只做快速内存操作，不能写终端、
阻塞或等待网络。

CLI 第一次 Ctrl-C/SIGTERM 请求协作取消：不再启动新 batch，已接受 tick 短尾会 flush，但未 terminal
范围不提交 final 或 provisional checkpoint；minute 未 terminal buffer 不落盘。已完成 terminal 范围
保持有效，命令收敛后返回 130。第二次信号立即退出 130。SDK 调用方使用
`BacktestRemoteFillCancellation` 获得同一协作取消语义。

TQBN 并发 reader 在 per-file shared lock 内打开 data file、验证 tail checkpoint 并固定 confirmed
prefix；随后可从 opened-file snapshot 读取而不长期挡住 writer/compaction。首次文件初始化是 sync 后
原子 rename；checkpoint 后未确认的截断/坏 checksum suffix 不进入 coverage/read，下一 writer 可从确认
边界恢复。无有效 checkpoint 的旧文件严格全量校验。该协议不承诺新旧 binary 进程长期混用，升级访问
同一 root 的服务时应同步重启。

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

新生成的 tick、minute、daily fill report 统一使用 schema `3`，默认写入 `reports/tick/`、
`reports/minute/`、`reports/daily/`。reader 仍兼容 tick v1/v2、minute v1、daily v1。无论报告版本，
最终验收都必须使用 report 绑定的同 root、同 symbol/window 做 CacheOnly readback。

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
  显式声明的 tick 追加到同一 root。策略必须持续调用 `next()` / `wait_update()`；facade 会按
  每 symbol 最多 `128` 行或约 `250 ms` 批量提交连续 rows；首次初始化或失败重扫之外，只解码当前
  commit 变更集命中的 tick serial。首批、跳号和正常对象销毁时强制 flush。
- 观察 `record_ticks_health()`；断线、跳号或未确认尾部会保留 coverage gap，后续由同一 warmup
  owner 再次补齐。异常退出前仍在内存 batch 的 rows 不会被标为 complete。
- 多个策略消费者使用相同 root 加 `.cache_only()`，避免重复远端下载。不要把 relay 的内存缓存当作
  canonical historical cache。
- `duration <= 60s` 的 K 线从 tick 合成；`duration > 60s` 使用独立的 native K 线
  `HistorySeriesCache` 路径，不能用本流程的 tick coverage 代替。

## 常见问题

| 现象 | 处理 |
| --- | --- |
| `CacheOnly` 报缺口 | 不删除已有文件；确认 range、symbol 和 root 后重新运行 `RemoteOnMiss` warmup。 |
| 远端填充无进展 | 检查 auth、官方服务连接、目标是否为已结束交易日；再根据实际任务预算调整 idle 或 batch timeout。 |
| 严格任务不允许盘中日 | 传 `--require-final`；当前日会被拒绝，等 18:00 分区结束后再运行。 |
| 日文件数与交易日数不同 | 正常。夜盘归属下一交易日，休市日可以有空 coverage 分区；以 CacheOnly coverage 为准。 |
| `KQ.i` 与 `KQ.m` 结果不同 | 正常。前者是 index symbol，后者按日期解析到具体合约；分别检查报告。 |
| 想重新下载全部数据 | 这是破坏性维护操作；先取得用户确认，再显式使用 `.refresh()` 或 purge。 |

相关入口：

- [持久缓存预热 contract example](../../crates/tqsdk/examples/api_contract_s45_facade_backtest_cache_warmup.rs)
- [`tqsdk` facade cache 语义](../../crates/tqsdk/README.md)
- [TQBN daily v3 格式合同](history-cache-format.md)
