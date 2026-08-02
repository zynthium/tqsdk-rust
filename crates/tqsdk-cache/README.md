# `tqsdk-cache`

`tqsdk-cache` 是 `tqsdk-rust` 的可选 cache operator CLI。它管理同一 history root 中的
daily TQBN tick cache 和 canonical final-60s Kline cache，也可通过同一份回测历史查询合同导出
时间区间；它是 workspace member，但不属于 Cargo default-members，也不会进入普通策略、回测或
live 行情的 hot path。

```bash
cargo run -p tqsdk-cache -- --help
```

CLI 只编排已有的 `tqsdk` backtest builder 与 `tqsdk-data` cache API：不引入新的 store format、
session owner、后台守护进程、relay 或监控服务。完整合同见
[回测缓存 CLI](../../docs/architecture/backtest-tick-cache-cli.md)。

## Cache family

全局 `--kind` 选择目标 cache family，默认是 `tick`：

| `--kind` | 管理对象 | 可用命令 |
| --- | --- | --- |
| `tick` | `series/<YYYYMMDD>/tick/<symbol>.tqbn` | `inventory`、`inspect`、`fill`、`verify`、`doctor` |
| `minute` | canonical final-60s `.tqmk` | `inventory`、`inspect`、`fill`、`verify`、`doctor`、`purge` |
| `all` | 两类 cache 的汇总 | 仅 `inventory`、`doctor` |

minute 的 format id 为 `tqsdk.minute-kline.monthly.v4`，但 namespace 有意保持
`minute-kline-v3/trading-YYYYMM/<escaped-symbol>.tqmk`。每个文件属于一个
`logical symbol × trading month`；旧 v3 文件不会自动迁移、覆盖或删除，deep doctor 会将其标记为
`legacy_unsupported`。

每个 v4 月文件绑定写入时的 immutable metadata snapshot。active metadata pointer 随后前移不会单独
使已有分区失效：`inspect`、`fill --dry-run` 和 `verify` 只会在一个保留 snapshot 覆盖整个窗口、
schema/session identity 与 active 相同并能精确验证现有月文件时使用它。不能满足这些条件的旧、损坏或
混合分区仍 fail closed；CLI 不会为此自动删除、重写或重新下载数据。

`--market futures|stock` 只影响 `--kind minute fill` 的 server-side backtest endpoint：
futures 是默认值，允许 `--universe`；stock 必须提供一个或多个显式 `--symbol`，不支持 futures
universe selector。tick fill 不支持 stock market；`--kind tick --market stock` 是 usage error。

tick 与 minute cache 都没有自动 retention、max-byte eviction 或后台清理。读写、普通回测和
`--kind minute fill` 都不会删除已有数据。

## 常用命令

默认输出是紧凑的人工摘要。自动化调用可显式使用 `--output-format json`；默认 JSON envelope 是
V3，`--output-schema v2` 仅用于旧脚本迁移。

```bash
# 默认是 tick：快速文件系统盘点；不存在的 root 不会被创建。
cargo run -p tqsdk-cache -- \
  --cache-dir /var/lib/tqsdk/history inventory

# 两类 cache 的快速盘点，或两类 cache 的深度诊断。
cargo run -p tqsdk-cache -- \
  --cache-dir /var/lib/tqsdk/history --kind all inventory
cargo run -p tqsdk-cache -- \
  --cache-dir /var/lib/tqsdk/history --kind all doctor

# minute coverage 以 logical cache symbol 检查，不联网、不写文件。
cargo run -p tqsdk-cache -- \
  --cache-dir /var/lib/tqsdk/history --kind minute inspect \
  --symbol KQ.i@SHFE.au \
  --start-day 2026-06-01 --end-day 2026-06-30

# tick dry-run：可使用 futures universe；缺 coverage 时退出码为 1。
cargo run -p tqsdk-cache -- \
  --cache-dir /var/lib/tqsdk/history fill \
  --universe 'main:all;index:all;!CFFEX' \
  --start-day 2026-06-01 --end-day 2026-06-30 --dry-run

# final-only minute fill：只选择已结束 trading day。
TQ_AUTH_USER='your-account' TQ_AUTH_PASS='your-password' \
cargo run -p tqsdk-cache -- \
  --cache-dir /var/lib/tqsdk/history --kind minute --market futures fill \
  --symbol KQ.i@SHFE.au \
  --start-day 2026-06-01 --end-day 2026-06-30 \
  --symbol-concurrency 2

# stock minute fill 必须显式给出 symbol，不能传 --universe。
TQ_AUTH_USER='your-account' TQ_AUTH_PASS='your-password' \
cargo run -p tqsdk-cache -- \
  --cache-dir /var/lib/tqsdk/history --kind minute --market stock fill \
  --symbol SSE.000300 \
  --start-day 2026-06-01 --end-day 2026-06-30
```

完整 static cache 命中时 fill 不创建远端 session，也不需要 `TQ_AUTH_USER` /
`TQ_AUTH_PASS`。发生缺口时，tick 和 minute 都只通过官方 server-side backtest stream 补齐，
不使用 `tq_dl` 或专业历史下载权限。minute 只请求 60-second Kline stream；每个 batch 必须收到
远端 terminal 成功才写 final coverage，合法的零行窗口也可成为 final。取消、超时或失败 batch
不会标记其未完成范围。

## 历史查询与 LLM 上下文

`query` 直接复用 `tqsdk-data::BacktestHistoryClient`，不解析 `.tqbn` 文件，也不增加另一份
history store。它接受 RFC 3339 的半开区间 `[start, end)`，支持 Tick、任意合法 Kline 周期，以及
主连所需的 logical → physical segment 投影。当前 cache-backed query 只支持 futures。
行类型由 `--series tick|kline` 选择；query 保留默认 `--kind tick`，而 `--kind minute|all` 和
`--market stock` 都是 usage error，避免把 cache 运维选择器误当成 row shape 或另一条数据源。

默认 `--policy remote-on-miss`：先检查本地 durable coverage，只有缺口才懒加载
`TQ_AUTH_USER` / `TQ_AUTH_PASS` 并走官方 server-side backtest stream。`--policy cache-only`
严格离线，不读取认证、不联网也不补写 cache；缺 coverage、终态失败或损坏 metadata 都不会偷偷返回
不完整数据。远端补齐只有在 terminal success 后才获得 final coverage。

```bash
# 可逐行解析的 lossless JSONL；字段别名会规范化为 t/lp/v。
cargo run -p tqsdk-cache -- \
  --cache-dir /var/lib/tqsdk/history --output-format jsonl query \
  --symbol KQ.m@SHFE.au --series tick \
  --start 2026-06-01T00:00:00Z --end 2026-06-01T01:00:00Z \
  --policy cache-only --timestamp offset \
  --fields time,last_price,volume

# 输出面向 GPT-5.6 的类 CSV 上下文；超过预算时按 price focus 确定性压缩。
TQ_AUTH_USER='your-account' TQ_AUTH_PASS='your-password' \
cargo run -p tqsdk-cache -- \
  --cache-dir /var/lib/tqsdk/history --output-format llm-csv query \
  --symbol KQ.i@SHFE.au --series kline --period 5m \
  --start 2026-06-01T00:00:00Z --end 2026-06-01T04:00:00Z \
  --fields time,open,high,low,close,volume,close_oi \
  --data-token-budget 12000 --focus price

# 发现可选字段及其长别名。
cargo run -p tqsdk-cache -- query schema --series tick
```

`--fields` 是严格 projection：只输出所选字段，长别名可输入、输出固定使用短别名与 schema 顺序。
JSONL 的默认时间是完整 ISO 8601；`--timestamp offset` 改为相对于 block start 的整数纳秒。
LLM CSV 使用 `--llm-time iso|offset|both`：默认 `iso` 按 block 实际精度无损裁剪（例如分钟 K
线为 `2026-07-31T01:00Z`，不填充无意义纳秒）；`offset` 将已选择的 `t` 列改为相对于 `ref`
的整数，并声明精确 `unit`（例如 `1m`）；`both` 仅在选择 `t` 时紧随它增加派生 `dt` 列，便于
人工与模型逐行对照。未显式传 `--llm-time` 时，`--timestamp offset` 仍会选择 LLM 的 `offset`
模式。所有 LLM 时间区间都显式标明 `end_exclusive=true`，Kline 还会标明其时间是 bar start。
默认数字是可读 decimal；`--number-format scaled-int` 必须显式传
`--price-tick`（或在 request file 的对应 block 提供），避免猜测合约精度；price tick 必须有限且为正，
价格必须是 tick 的整数倍，CLI 不会静默四舍五入。缺失或非有限浮点以空 CSV cell / JSON `null` 表示，
真实零值始终为 `0`。LLM CSV 在 scaled-int 模式会仅在相关 block 写出 `price_tick`，从不让模型猜测
缩放比例。

`--output-format jsonl` 的稳定协议为 `tqsdk-history-jsonl/1`。`--output-format llm-csv` 的稳定
协议为 `tqllm-csv/2`：每个 symbol × series × period 是独立 block，按 `block`、`time`、`columns`、
`summary`、`data`、`block_end` 和 `document_end` 组织。`period` 使用 `1m` 这类人类单位，`columns`
一次性定义短字段名与语义（包括 Kline `bar_volume` / Tick `cumulative_volume`）；source 只报告实际
`cache`、`remote` 或 `cache+remote`。默认不输出 query ID/hash、token estimate、drill-down ID、coverage
纳秒范围或 session JSON；active metadata snapshot 仍在输出前严格验证。需要完整审计 provenance 时使用
JSONL。它不调用模型、也不读取 OpenAI 凭证。两种 raw format 都会先通过 `collect_all()` 收齐 batch（默认
`--max-memory-bytes 128 MiB`）再生成完整 payload，因此 JSONL 是逐行格式而非在线 streaming export。
LLM 输出会先收齐所有 terminal success、校验完整 coverage 和 active metadata snapshot，再一次性生成
摘要、哈希与可能的 `lossy` 压缩；默认缺失即失败。`--allow-partial` 是唯一允许已完成 sibling block
输出的 opt-in，明确输出 `gap` 且仍以 exit code `1` 结束；它不放宽 finality 或完整 coverage gate。

`--data-token-budget` 使用保守的本地估算，不替代上游 agent/API 的精确 token 计数。预算不足时，
`--compression auto` 按 block weight（简单 CLI 默认相等；TOML 可设 `weight`）分配残余，并保留
首尾、focus 关键行和每 block summary；`--compression off` 则 fail closed。原始 `jsonl` 不做压缩。
raw data 默认写 stdout、诊断写 stderr；`text` / `json` 只提供摘要。传 `--output PATH` 时仅
`jsonl` / `llm-csv` 可用，stdout 为空，并以同目录临时文件 + sync + rename 原子写入；stdout 本身
不提供原子发布保证。`--pretty` / `--output-schema` 只适用于 JSON 摘要。

简单同质批次可重复 `--symbol`；混合 symbol、series、period 或字段时使用 `--request-file`。TOML 必须
有 `version = 1` 和至少一个 `[[request]]`，未知字段会拒绝；它不能与 `--symbol`、`--series`、
`--start`、`--end`、`--period` 或 `--fields` 混用。`weight` 只影响 LLM 预算分配，block 内的
`price_tick` 会覆盖全局值：

```toml
version = 1

[[request]]
symbol = "KQ.m@SHFE.au"
series = "tick"
start = "2026-06-01T00:00:00Z"
end = "2026-06-01T01:00:00Z"
fields = ["time", "last_price", "volume"]
weight = 2

[[request]]
symbol = "KQ.i@SHFE.au"
series = "kline"
period = "5m"
start = "2026-06-01T00:00:00Z"
end = "2026-06-01T04:00:00Z"
fields = ["time", "open", "high", "low", "close", "volume"]
```

```bash
cargo run -p tqsdk-cache -- \
  --cache-dir /var/lib/tqsdk/history --output-format llm-csv query \
  --request-file analysis.toml --policy cache-only --data-token-budget 12000
```

## Inspect、verify 与 doctor

| 命令 | tick | minute |
| --- | --- | --- |
| `inventory` | 快速枚举日分区和文件问题，不解码 records | 快速枚举月文件，不解码文件内容 |
| `inspect` | 显式 physical cache symbol 的 coverage | 显式 logical cache symbol 的 final-60s coverage |
| `verify` | CacheOnly coverage，选配完整本地 tick replay | CacheOnly final coverage，选配流式读取 minute rows |
| `doctor` | 深度解码 TQBN tick partitions | 深度解码 monthly minute files，并报告 `readable`、`legacy_unsupported`、`unsupported_version` 或 `corrupt` |

`verify` 从不远端补数或写 cache。可以给出显式 closed-day window，也可以使用匹配的 fill report：

```bash
# 复用 minute report 的 canonical root、window 和 logical symbols，不需要账号。
cargo run -p tqsdk-cache -- \
  --kind minute verify \
  --report /var/lib/tqsdk/history/reports/minute/june.json \
  --replay --min-rows 1

# 明确验证一个 minute cache window。
cargo run -p tqsdk-cache -- \
  --cache-dir /var/lib/tqsdk/history --kind minute verify \
  --symbol KQ.i@SHFE.au \
  --start-day 2026-06-01 --end-day 2026-06-30
```

tick `fill` 保留 CST `18:00` TQBN partition、当前交易日 provisional checkpoint 与
`--require-final` 的既有合同。minute fill 则严格 final-only：当前或未来 trading day 会拒绝，
`--include-open-day` 也不适用于 minute。`--last-trading-days` 与 `--calendar auto|required|off`
仍可用于选择 closed-day 窗口；日历只做选择和进度，不替代 cache coverage。

## 报告与进度

普通 tick fill 默认把 schema-v2 report 写入 `<cache-root>/reports/`，兼容读取旧 schema-v1 report。
minute fill 使用独立的 `cache_kind=minute` report（当前 schema v1），默认写入
`<cache-root>/reports/minute/tqsdk-cache-minute-fill-<utc>-<pid>.json`。minute report 只包含
logical cache symbols；`--kind minute verify --report` 只接受此类 report，且以 report 记录的
canonical root、range 和 symbols 为准。

进度总是写 stderr。`--progress jsonl` 对两类 fill 都输出 schema-v2
`tqsdk-cache.progress` JSONL，并带 `cache_kind: "tick" | "minute"`；不要再按旧 schema-v1
解析。tick 的 progress 以 physical cache symbol 展示，minute 则以 logical minute symbol 展示。

## 显式破坏性维护

CLI 目前只提供 minute purge：必须恰好一个 `--symbol`、完整的 `--start-day` / `--end-day` window
以及 `--yes`。`--dry-run` 不写入，只列出将删除的月文件、路径和大小；真实 purge 会删除与请求窗口
相交的整个 monthly partition，而不是单条 K 线。

```bash
# 先看会删除哪些整月分区。
cargo run -p tqsdk-cache -- \
  --cache-dir /var/lib/tqsdk/history --kind minute purge \
  --symbol KQ.i@SHFE.au \
  --start-day 2026-06-01 --end-day 2026-06-30 --dry-run

# 明确确认后才删除。
cargo run -p tqsdk-cache -- \
  --cache-dir /var/lib/tqsdk/history --kind minute purge \
  --symbol KQ.i@SHFE.au \
  --start-day 2026-06-01 --end-day 2026-06-30 --yes
```

tick 的 refresh / purge / compact 仍是显式 SDK API，不由 CLI 自动执行。minute purge 使用每月文件锁，
不是跨 cache family 的全局稳定快照；应先用 dry-run 核对范围。无论哪一类 cache，都不会自行清理。

## 验证

无网络回归入口：

```bash
rtk cargo test -p tqsdk-cache
rtk cargo clippy -p tqsdk-cache --all-targets -- -D warnings
rtk cargo run -p tqsdk-cache -- --help
```

真实 fill 只在用户明确授权、使用已注入的凭证且选择历史 closed window 时运行。对少量指数合约的
验收应同时检查 local canonical 60s 聚合出的高周期 K 与官方 server-side backtest Kline，比较固定
CST `18:00` trading-day bucket、盘中 break 不重置和可跨 gap 的行为、OHLC、volume 和 open interest，
并只记录汇总差异或少量样本，不记录凭证。
