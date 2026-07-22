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

默认将每个命令结果渲染为人工摘要，不区分 TTY、pipe、重定向或 CI。`--output-format json`
显式请求机器输出，`--output-format text` 等同于默认摘要输出；后者不能与 `--pretty` 或
`--output-schema` 组合。V3 固定 envelope 包含 `kind` `tqsdk-cache.result`、`command`、
`status`、`exit_code`、`generated_at`、`duration_ms`、`tool`、`warnings`、`result` 和 `error`；`result`
保留原 schema-v2 command body。预期 coverage 不完整以 `status=incomplete`、退出 `1` 表示，运行/usage
错误以 `status=error` 和稳定的 error `code`、`message`、`retryable` 表示。`--output-schema v2`
保留旧的顶层 result shape 及 stderr fatal-error 行为，作为有限期迁移兼容层；它需要
`--output-format json`。进度和交互 spinner 只写 stderr。`--pretty` 只改变 JSON 缩进，也需要
`--output-format json`。`verify --report`
兼容读取 persisted report schema version `1`，缺少的 selector、日历和 per-symbol day stats 视为未知
而不是覆盖事实。

当前稳定错误码为 `usage`、`cache_busy`、`data_error`、`sdk_error`、`io_error` 和 `json_error`。
`cache_busy` 始终 `retryable=true`；可重试的 I/O 中断、超时和 would-block 也会标记为 true。调用方应
以 `exit_code` 和 `error.code` 决策，`message` 只用于日志和人工诊断。

| 命令 | 作用 | 写入 / 网络 | 一致性 |
| --- | --- | --- | --- |
| `inventory` | 快速枚举 tick day partitions、文件数、字节数和 magic 问题 | 不创建不存在的 root，不解码 records，不联网 | 可在 fill 中运行，结果可能是中间状态 |
| `inspect` | 对显式 physical symbol 检查 coverage/missing ranges | read-only，不联网 | 不取稳定视图锁 |
| `fill --dry-run` | 解析目标并用 CacheOnly 检查 coverage | 不取 lock，不请求远端 tick 或创建 report；universe/calendar selector 可查询 metadata | 缺 coverage 退出 `1` |
| `fill` | 对 closed trading days 只补 missing ranges | 可能写 TQBN/report，只有 miss 才联网 | 每 root 排他 fill lock |
| `verify` | CacheOnly coverage，选配实际 replay | 不远端补数；stable-view lock 文件可能在 root 内创建 | shared stable-view lock |
| `doctor` | 深度解码所有 TQBN tick partitions | 不修改 records；stable-view lock 文件可能在 root 内创建 | shared stable-view lock |

`inspect` 接受的是 physical cache symbol。`fill` 可以同时接受重复的 `--symbol` 和
`--universe`；它会依赖 facade 的 resolver 去重。`KQ.m@...` 主连的 logical 请求可能解析到多条
物理合约 cache symbols，fill report 会同时保留 `logical_symbols` 和 `physical_symbols`。共享
universe resolver 会在最终集合中剔除不受本地历史缓存支持的 `KQD` 外盘合约，因此 `cont:all`
不会生成不存在历史映射的 `KQ.m@KQD.*`。

## 交易日与 closed-day 保护

TQBN day partition 的日界线固定为 CST `18:00:00`：

- 一个交易日的 storage window 是前一自然日 18:00 到该交易日 18:00。
- 周五晚和周末会归一到下一交易日；休市日的空 coverage partition 合法。
- `fill` 和不带 report 的 `verify` 只接受最后一个已结束交易日前的窗口。
- 当前 open trading day 或未来 trading day 会拒绝。V1 的 `--include-open-day` 明确退出 `2`，
  不允许把盘中尾部错误标为 complete。

不同交易所的实际收盘时间以合约 `trading_time` 为准。TQBN day window 是 partition / coverage
边界，不是“所有品种都交易到 18:00”的市场断言。

## 日历选择与进度

`fill` 提供两种日期选择方式：显式 `--start-day/--end-day`，或
`--last-trading-days N [--end-day YYYY-MM-DD]`。后者按通用交易日历选择最近 N 个已结束交易日，
不能把 N 个工作日当作 N 个交易日。

日历模式由 `--calendar auto|required|off` 控制：

- `auto` 先读 `<cache-root>/meta/trading-calendar-v1.json`（损坏快照按不可用处理）。显式日期范围没有可用快照时，先按
  TQBN 日分区规划；只有 `PlanReady` 已确认远端缺口后才请求通用日历。`--last-trading-days` 必须
  在计划前获得日历以确定窗口；新快照只会在 root fill lock 已取得后原子写入。
- `required` 不允许 partition fallback。缺少或不覆盖窗口的快照会触发查询；查询失败时 fill 失败。
- `off` 不读取或查询日历，使用 TQBN partition days，并拒绝 `--last-trading-days`。

通用日历只用于 selector、分母和进度显示，绝不提交或推断 cache coverage。CST `18:00` 的 TQBN
分区、连续 tick id 和最终 `CacheOnly` 检查仍是完整性的唯一依据；休市日可以是合法空 coverage。

进度仅写 stderr，默认 `--progress tty` 先使用一个 cache-inspection bar 显示已检查/总 physical
range、命中、缺口和当前 symbol；`PlanReady` 到达后切换为一个 logical-batch 全局 bar 和最多
`--progress-max-bars` 个 active physical-symbol bar。`auto` 仅在检测到交互终端时绘制动态条，否则自动降级为
稳定 `key=value` 文本；`plain` 强制文本，`off` 关闭进度。`jsonl` 为每个 state revision 写一条 schema-v1
`tqsdk-cache.progress` JSON record，包含 sequence、状态、batch/coverage/calendar 汇总和 active
physical symbols；检查阶段使用 `event=inspection`，并附带累计范围计数及当前 physical range，供日志采集或
调度器消费。每条 physical-symbol 状态包含当前处理的 trading day、coverage day count、完整接收 day count、
rows、retry 和 split 状态。流式 cursor 只有跨过完整 TQBN 日分区后才增加接收计数，最后一个日分区由成功
terminal event 确认。

## 填充、报告与验证

基础命令示例使用历史 closed dates；生产作业应先从官方交易日历选择日期，而不是倒推工作日：

```bash
# 检查目标 cache root，不会修改它。
cargo run -p tqsdk-cache -- \
  --cache-dir /var/lib/tqsdk/history inventory

# 不请求远端 tick 的预检；当 coverage 不完整时返回 1。
cargo run -p tqsdk-cache -- \
  --cache-dir /var/lib/tqsdk/history fill \
  --universe 'main:all;index:all;!CFFEX' \
  --start-day 2026-06-01 --end-day 2026-06-30 --dry-run

# 缺口需要账号；完整 static cache 命中时这两个变量不需要。
TQ_AUTH_USER='your-account' TQ_AUTH_PASS='your-password' \
cargo run -p tqsdk-cache -- \
  --cache-dir /var/lib/tqsdk/history fill \
  --universe 'main:all;index:all;!CFFEX' \
  --start-day 2026-06-01 --end-day 2026-06-30 \
  --symbol-concurrency 2 --symbol-batch-size 1

# 生产定时作业：按日历补齐最近 60 个已结束交易日，并保留 stdout JSON 给调度器。
cargo run -p tqsdk-cache -- \
  --cache-dir /var/lib/tqsdk/history fill \
  --universe 'main:all;index:all;!CFFEX' \
  --last-trading-days 60 --calendar auto \
  --progress auto --progress-max-bars 8 \
  --output-format json --pretty
```

正常 fill 会把 credential-free persisted schema-v2 JSON report 写到
`<cache-root>/reports/tqsdk-cache-fill-<utc>-<pid>.json`，也可以用 `--report PATH` 覆盖。
报告包含 canonical absolute `cache_dir`、原始 selector、请求和解析后的 trading-day window、
日历模式/快照 metadata、logical/physical symbols、coverage before/after、每 physical cache report
range 的日统计、`rows_written`、是否实际远端填充和生效的调度配置。不要把 report
看成写入成功的唯一依据：`complete=true`、CacheOnly coverage 和实际 replay 共同构成验收。

```bash
# report 是验证时的权威 root/range/symbol 输入；不需要 auth。
cargo run -p tqsdk-cache -- verify \
  --report /var/lib/tqsdk/history/reports/june.json \
  --replay --min-rows 1

# 显式查 coverage；没有 report 时需要给 complete closed-day window。
cargo run -p tqsdk-cache -- \
  --cache-dir /var/lib/tqsdk/history verify \
  --symbol KQ.i@SHFE.au \
  --start-day 2026-06-01 --end-day 2026-06-30
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

进度会在 stderr 显示当前 batch、physical symbol、trading day、完整接收日和 rows；它不改变
远端调度或 cache 写入热路径。`--daily-slices` 是诊断 fallback，会按一天切远端请求；默认保持
SDK 的单会话长 range 调度。CLI flags
`--symbol-batch-size`、`--symbol-concurrency`、`--idle-timeout-secs`、
`--batch-timeout-secs` 覆盖当前进程的 `TQSDK_REMOTE_FILL_*` defaults，不会修改全局环境。

## 诊断与维护界限

```bash
# Fast inventory: 不解码 record blocks。
cargo run -p tqsdk-cache -- --cache-dir /var/lib/tqsdk/history inventory

# Deep diagnostic: rows、schema、file status、incomplete/corrupt block error。
cargo run -p tqsdk-cache -- --cache-dir /var/lib/tqsdk/history doctor
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
