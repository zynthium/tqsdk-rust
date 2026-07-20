# 回测 Tick Cache CLI

## 目的与边界

`tqsdk-cache` 是 canonical daily TQBN tick cache 的可选 operator CLI。它把已有
`tqsdk` remote-on-miss warmup 与 `tqsdk-data::BacktestTickCache` 的 inspection/lock 能力
组合成适合 cron、CI 或人工运维的命令，而不改变 SDK 的默认运行路径。

它的边界是明确的：

- 只管理 `series/<YYYYMMDD>/tick/<escaped-symbol>.tqbn`，不管理 native K 线 cache。
- 不拥有 session、状态树、回测推进、live subscription、recording loop 或 store adapter。
- 不是 relay、守护进程、监控面板、通用 downloader 或自定义 store 管理器。
- workspace member 但不属于 Cargo default-members；策略程序不需要依赖或启动它。

API 使用者仍应通过 `.backtest(...).warmup()`、`.cache_only()`、`MarketCachePolicy` 和
`record_ticks(...)` 工作。live 增量记录仍由已运行策略显式调用 `next()` / `wait_update()` 驱动；
CLI 只负责历史 cache 的离线/显式运维。

## 运行模型

```text
tqsdk-cache fill
  -> Tq::futures().backtest(range).remote_on_miss().warmup()
  -> BacktestTickCache coverage / TQBN daily partitions
  -> official server-side backtest market stream only for missing ranges
```

远端填充仍直接使用 SDK 的官方网络路径，不接入系统 proxy。完整 cache 的 static symbol fill
不会创建远端 session，也不需要 `TQ_AUTH_USER` / `TQ_AUTH_PASS`；发生缺口时才需要这对凭证。
该路径不使用 `tq_dl` 或专业历史下载权限。

动态 universe 沿用 SDK 的现有 resolver。其 metadata/query 需求与普通
`.backtest(...).universe(...)` 相同；不要为 CLI 另建网络 client 或 proxy 规则。

## 命令合同

所有成功和可机器读取的结果都写 stdout JSON，当前顶层 `schema_version` 为 `1`。进度、错误和
交互 spinner 只写 stderr。`--pretty` 只改变 stdout 的 JSON 缩进，不改变字段或 stderr。

| 命令 | 作用 | 写入 / 网络 | 一致性 |
| --- | --- | --- | --- |
| `inventory` | 快速枚举 tick day partitions、文件数、字节数和 magic 问题 | 不创建不存在的 root，不解码 records，不联网 | 可在 fill 中运行，结果可能是中间状态 |
| `inspect` | 对显式 physical symbol 检查 coverage/missing ranges | read-only，不联网 | 不取稳定视图锁 |
| `fill --dry-run` | 解析目标并用 CacheOnly 检查 coverage | 不取 lock，不远端补数，不创建目录或 report | 缺 coverage 退出 `1` |
| `fill` | 对 closed trading days 只补 missing ranges | 可能写 TQBN/report，只有 miss 才联网 | 每 root 排他 fill lock |
| `verify` | CacheOnly coverage，选配实际 replay | 不远端补数；stable-view lock 文件可能在 root 内创建 | shared stable-view lock |
| `doctor` | 深度解码所有 TQBN tick partitions | 不修改 records；stable-view lock 文件可能在 root 内创建 | shared stable-view lock |

`inspect` 接受的是 physical cache symbol。`fill` 可以同时接受重复的 `--symbol` 和
`--universe`；它会依赖 facade 的 resolver 去重。`KQ.m@...` 主连的 logical 请求可能解析到多条
物理合约 cache symbols，fill report 会同时保留 `logical_symbols` 和 `physical_symbols`。

## 交易日与 closed-day 保护

TQBN day partition 的日界线固定为 CST `18:00:00`：

- 一个交易日的 storage window 是前一自然日 18:00 到该交易日 18:00。
- 周五晚和周末会归一到下一交易日；休市日的空 coverage partition 合法。
- `fill` 和不带 report 的 `verify` 只接受最后一个已结束交易日前的窗口。
- 当前 open trading day 或未来 trading day 会拒绝。V1 的 `--include-open-day` 明确退出 `2`，
  不允许把盘中尾部错误标为 complete。

不同交易所的实际收盘时间以合约 `trading_time` 为准。TQBN day window 是 partition / coverage
边界，不是“所有品种都交易到 18:00”的市场断言。

## 填充、报告与验证

基础命令示例使用历史 closed dates；生产作业应先从官方交易日历选择日期，而不是倒推工作日：

```bash
# 检查目标 cache root，不会修改它。
cargo run -p tqsdk-cache -- \
  --cache-dir /var/lib/tqsdk/history inventory --pretty

# 无副作用预检；当 coverage 不完整时返回 1。
cargo run -p tqsdk-cache -- \
  --cache-dir /var/lib/tqsdk/history fill \
  --universe 'main:all;index:all;!CFFEX' \
  --start-day 2026-06-01 --end-day 2026-06-30 --dry-run --pretty

# 缺口需要账号；完整 static cache 命中时这两个变量不需要。
TQ_AUTH_USER='your-account' TQ_AUTH_PASS='your-password' \
cargo run -p tqsdk-cache -- \
  --cache-dir /var/lib/tqsdk/history fill \
  --universe 'main:all;index:all;!CFFEX' \
  --start-day 2026-06-01 --end-day 2026-06-30 \
  --symbol-concurrency 2 --symbol-batch-size 1 --pretty
```

正常 fill 会把 credential-free JSON report 写到
`<cache-root>/reports/tqsdk-cache-fill-<utc>-<pid>.json`，也可以用 `--report PATH` 覆盖。
报告包含 canonical absolute `cache_dir`、请求 trading-day window、logical/physical symbols、
coverage before/after、`rows_written`、是否实际远端填充和生效的调度配置。不要把 report
看成写入成功的唯一依据：`complete=true`、CacheOnly coverage 和实际 replay 共同构成验收。

```bash
# report 是验证时的权威 root/range/symbol 输入；不需要 auth。
cargo run -p tqsdk-cache -- verify \
  --report /var/lib/tqsdk/history/reports/june.json \
  --replay --min-rows 1 --pretty

# 显式查 coverage；没有 report 时需要给 complete closed-day window。
cargo run -p tqsdk-cache -- \
  --cache-dir /var/lib/tqsdk/history verify \
  --symbol KQ.i@SHFE.au \
  --start-day 2026-06-01 --end-day 2026-06-30 --pretty
```

`verify --report` 忽略调用环境中可能不同的默认 cache root，并使用 report 的 canonical root。
如果额外传入 `--cache-dir`，它必须完全相同，否则是 usage error。`--min-rows` 必须与
`--replay` 一起使用；一个预期空市场窗口应省略 `--min-rows`，只验证合法空 coverage。

## 锁、并发与取消

每个 root 的 `.tqsdk-cache-operation.lock` 是 advisory root lock，目标是防止多个 fill owner
同时发起重复远端补数：

- `fill` 获取 exclusive lock。默认失败即返回 `75`；`--lock-wait-secs N` 每 200ms 重试，
  同时响应取消。
- `verify` 和 `doctor` 获取 shared lock，因此不会与 fill 混合读取中间覆盖状态。
- `inventory` 刻意不取 lock，适用于低频状态页，但其文件数/体积可反映未完成 fill 的中间状态。
- 单个 TQBN 文件仍有文件级写锁；它不是远端去重机制，根锁才负责 fill owner 协调。

Ctrl-C 或 SIGTERM 将触发协作式取消：已经接收的 row batch 会 flush 到对应 daily file，但该
range 不会 commit coverage，命令返回 `130`。下次 `fill` 依据缺口继续完成；不要手工补 coverage
或编辑 `.tqbn`。

进度会在 stderr 显示当前 batch、symbol、trading day 和已接收 rows。`--daily-slices` 是诊断
fallback，会按一天切远端请求；默认保持 SDK 的单会话长 range 调度。CLI flags
`--symbol-batch-size`、`--symbol-concurrency`、`--idle-timeout-secs`、
`--batch-timeout-secs` 覆盖当前进程的 `TQSDK_REMOTE_FILL_*` defaults，不会修改全局环境。

## 诊断与维护界限

```bash
# Fast inventory: 不解码 record blocks。
cargo run -p tqsdk-cache -- --cache-dir /var/lib/tqsdk/history inventory --pretty

# Deep diagnostic: rows、schema、file status、incomplete/corrupt block error。
cargo run -p tqsdk-cache -- --cache-dir /var/lib/tqsdk/history doctor --pretty
```

`doctor` 的非零退出码表示至少一个 tick file 状态异常。它不是修复器。V1 故意没有
`purge`、`refresh`、`compact` 命令；这些操作会删除或重写数据，仍然只能通过明确的 SDK data/facade
API 并经过操作者确认执行。
`doctor` 输出中的 `trading_day` 使用 ISO `YYYY-MM-DD`，即使底层 partition 目录使用
`YYYYMMDD`。

live 增量应当使用 `MarketCachePolicy::record_ticks(...)` / `.record_universe(...)` 配合
`.market_cache(...)`，让正在运行的策略把已订阅的 tick 写入同一 root。断线、跳号或未确认尾部
会留 coverage gap，之后由本 CLI 或 SDK `.warmup()` 补洞。relay 的内存 cache 不是 canonical
historical cache，也不代替本工具。

## 验证

本 crate 的无网络回归入口：

```bash
rtk cargo test -p tqsdk-cache
rtk cargo clippy -p tqsdk-cache --all-targets -- -D warnings
rtk cargo run -p tqsdk-cache -- --help
```

涉及真实填充时，仅在用户明确授权并提供凭证后运行 `fill`。远端 fill、CacheOnly verification
和 replay 的完整 acceptance flow 见
[回测 Tick 持久缓存预热与验收](backtest-tick-cache-operations.md)。
