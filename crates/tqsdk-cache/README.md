# `tqsdk-cache`

`tqsdk-cache` 是 `tqsdk-rust` 的可选运维 CLI，用于检查、补齐和验证 canonical daily
TQBN tick cache。它是 workspace member，但不属于 Cargo default-members，也不会进入普通策略、
回测或 live 行情的 hot path。

```bash
cargo run -p tqsdk-cache -- --help
```

它只管理 `series/<YYYYMMDD>/tick/<symbol>.tqbn` 的回测 tick cache，并复用既有
`tqsdk` backtest builder 与 `tqsdk-data::BacktestTickCache`：不引入第二种持久格式、
session owner、后台守护进程、relay、监控面板或 custom store plugin。

完整操作合同见
[回测 Tick Cache CLI](../../docs/architecture/backtest-tick-cache-cli.md)。

`--universe` 使用 shared futures resolver；最终 resolved universe 会排除当前不受本地
history cache / relay 支持的 `KQD` 外盘合约，因此 `cont:all` 不会生成不存在历史映射的
`KQ.m@KQD.*`。

## 常用命令

默认输出是紧凑的人工摘要，不区分 TTY、pipe、重定向或 CI。需要机器结果时显式传入
`--output-format json`；`--output-format text` 等同于默认值。JSON V3 将原有命令结果保留在
`result`，并固定提供 `kind`、`command`、`status`、`exit_code`、`generated_at`、`duration_ms`、
`tool`、`warnings` 和 `error`。`--pretty` 与 `--output-schema v2|v3` 需要和
`--output-format json` 组合。`--output-schema v2` 保留旧的顶层 JSON 字段和 fatal-error stderr
行为，供尚未迁移的脚本使用。

```bash
# 快速文件系统盘点；不存在的 root 不会被创建。
cargo run -p tqsdk-cache -- \
  --cache-dir /var/lib/tqsdk/history inventory

# 检查一个或多个物理缓存 symbol 的 coverage。
cargo run -p tqsdk-cache -- \
  --cache-dir /var/lib/tqsdk/history inspect \
  --symbol KQ.i@SHFE.au \
  --start-day 2026-06-01 --end-day 2026-06-30

# 不请求远端 tick、不加 lock、不写文件的预检。缺 coverage 时退出码为 1。
cargo run -p tqsdk-cache -- \
  --cache-dir /var/lib/tqsdk/history fill \
  --universe 'main:all;index:all;!CFFEX' \
  --start-day 2026-06-01 --end-day 2026-06-30 --dry-run

# 按通用交易日历选择最近 60 个已结束交易日。默认动态显示全局 batch 和当前物理合约日进度。
cargo run -p tqsdk-cache -- \
  --cache-dir /var/lib/tqsdk/history fill \
  --universe 'main:all;index:all;!CFFEX' \
  --last-trading-days 60 --calendar auto \
  --progress-max-bars 8
```

正常 `fill` 使用与 `Tq::futures().backtest(...).remote_on_miss().warmup()` 相同的
cache-first 语义：已完整的 static symbol 不需要账号；缺口才需要
`TQ_AUTH_USER` / `TQ_AUTH_PASS`，并通过官方 server-side backtest stream 补齐，不需要
`tq_dl` / 专业历史下载权限。

```bash
export TQ_AUTH_USER='your-account'
export TQ_AUTH_PASS='your-password'

cargo run -p tqsdk-cache -- \
  --cache-dir /var/lib/tqsdk/history fill \
  --universe 'main:all;index:all;!CFFEX' \
  --start-day 2026-06-01 --end-day 2026-06-30 \
  --symbol KQ.i@SHFE.au \
  --symbol-concurrency 2 --report /var/lib/tqsdk/history/reports/june.json

# 当前 TQBN 交易日盘中快照；成功只推进 provisional checkpoint。
cargo run -p tqsdk-cache -- \
  --cache-dir /var/lib/tqsdk/history fill \
  --symbol CZCE.AP610 \
  --start-day 2026-07-24 --end-day 2026-07-24

# 以 report 固定的 canonical root、range 和物理 symbol 做严格本地验收。
cargo run -p tqsdk-cache -- verify \
  --report /var/lib/tqsdk/history/reports/june.json \
  --replay --min-rows 1

# 解码全部 tick partitions，检查 TQBN 结构与损坏状态。
cargo run -p tqsdk-cache -- \
  --cache-dir /var/lib/tqsdk/history doctor
```

TQBN trading day 在 CST `18:00:00` 开始，周末会归一到下一交易日。显式
`--start-day/--end-day` 的结束日期等于当前 TQBN 交易日时，`fill` 自动执行盘中快照；
`--last-trading-days` 仍只选择已结束交易日。严格任务可传 `--require-final` 拒绝当前日。
`--include-open-day` 仅作为兼容参数保留，不能与 `--last-trading-days` 或 `--require-final` 组合。
盘中单次运行把 horizon 固定为启动时刻减 5 秒，只写 non-final provisional checkpoint；普通
coverage、CacheOnly 和 `verify` 仍把该日视为缺口。checkpoint 的范围、高水位和 as-of 必须全部
留在同一个 TQBN 日分区；远端明确 terminal
成功的空增量可以推进高水位，取消、超时或未确认结束则不能。重复运行从 checkpoint 前 5 分钟
重叠请求并按 tick id 去重，且盘中不做全历史 compaction；18:00 分区边界过去后，同一日期会
自动走普通 final fill，全日重对账成功后 compact 并淘汰 provisional checkpoint。传入 `KQ.m@...` 或动态
universe 时，report 会分别记录 logical symbols 和已解析的 physical cache symbols；`inspect`
只接受后者。

## 日历、进度与报告

`fill` 的 `--calendar auto|required|off` 只用于选择日期和显示进度，永远不替代 TQBN coverage：

- `auto` 优先读取 `<cache-root>/meta/trading-calendar-v1.json`（损坏快照按不可用处理）。对显式日期范围且没有可用快照时，
  先按 TQBN 分区规划；只有 `PlanReady` 已确认存在远端缺口时才拉取通用交易日历。使用
  `--last-trading-days N` 时必须有日历，必要时会先查询以确定窗口，但只在 fill lock 已取得后原子写入快照。
- `required` 要求完整日历；没有本地快照时会查询，查询失败即让 fill 失败。
- `off` 固定按 TQBN 分区日显示，且拒绝 `--last-trading-days`。

可用 `--end-day YYYY-MM-DD` 作为 `--last-trading-days` 的历史锚点。通用日历只描述工作日和假期，
不判断某个合约是否有行情；空 coverage 和夜盘归属仍由 CST `18:00` TQBN 边界决定。

进度始终写 stderr，默认 `--progress tty` 先显示 cache inspection bar（已检查/总 physical range、
命中、缺口和当前 symbol），随后显示全局 logical-batch bar 和最多 `--progress-max-bars` 个当前
physical symbol bar。`--progress auto` 仅在检测到交互终端时绘制动态条，否则输出稳定的 `key=value`
行；`--progress plain` 强制文本，`--progress off` 关闭进度。`--progress jsonl` 为每次 progress state
revision 输出一条 schema `v1` 的 `tqsdk-cache.progress` JSONL 记录；检查阶段的 `event=inspection`
额外包含累计范围计数和当前 physical range，适合调度器和日志采集器。
每个 symbol 显示当前处理 trading day、
总规划日、已完整接收日和 row 数；“已接收日”只会在完整 TQBN 日分区跨越后递增，避免把盘中日误报为完成。
命令最终默认输出摘要；无论在终端还是自动化环境中，需要原始 JSON 都显式传入
`--output-format json`。

正常 fill 仍写 schema version `2` persisted report，增加原始 selector、已解析日历来源/快照元数据和每个
physical cache report range 的日计数。盘中 report 使用 `coverage_state=provisional`、
所有 physical symbols 的共同最小高水位 `complete_through_ns` 和 `day_complete=false`；
closed-day report 使用 `coverage_state=final`。`verify --report` 仍可读取 schema version `1`
报告；旧报告缺少这些字段时按 `final` / null / `complete` 解释。

## 协调与退出码

- `fill` 取得每个 cache root 的排他 advisory lock。默认 fail-fast，竞争时退出 `75`；可用
  `--lock-wait-secs` 等待已有 fill owner。
- `verify` 和 `doctor` 取得 shared stable-view lock；`inventory` 可以在 fill 过程中运行，
  但只是可能看到中间状态的快速盘点。
- Ctrl-C 或 SIGTERM 会 flush 已接收的 tick rows，但不会提交 final coverage，也不会推进
  provisional checkpoint，退出 `130`；下一次 fill 会继续补洞。
- persisted fill report 当前为 schema version `2`，并兼容读取 schema version `1`。CLI 显式 JSON
  输出默认使用 output schema `v3`，可使用 `--output-format json --output-schema v2` 迁移旧脚本。
  正常 fill 默认写入
  `<cache-root>/reports/`；`verify --report` 始终使用报告记录的 canonical root，并拒绝与
  `--cache-dir` 不一致的调用。

V1 不提供 `purge`、`refresh` 或 `compact` 命令。它们是破坏性维护操作，仍应由显式 SDK API
并取得操作者确认后执行。实时增量缓存由普通策略显式配置
`MarketCachePolicy::record_ticks(...)` / `.record_universe(...)`，而不是由本 CLI 启动守护进程。
