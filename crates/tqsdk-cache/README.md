# `tqsdk-cache`

`tqsdk-cache` 是 `tqsdk-rust` 的可选 cache operator CLI。它管理同一 history root 中的
daily TQBN tick cache、canonical final-60s Kline cache 和 native final-1d Kline cache，也可通过
同一份回测历史查询合同导出时间区间；它是 workspace member，但不属于 Cargo default-members，也不会
进入普通策略、回测或 live 行情的 hot path。

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
| `tick` | `series/<YYYYMMDD>/tick/<symbol>.tqbn` | `inventory`、`inspect`、`fill`、`verify`、`doctor`、`repair-locks` |
| `minute` | canonical final-60s `.tqmk` | `inventory`、`inspect`、`fill`、`verify`、`doctor`、`purge` |
| `daily` | `daily-kline-v1/<logical-symbol>.tqdk` native final-1d single file | `inspect`、`fill`、`verify`、`purge` |
| `all` | tick 与 minute cache 的汇总 | 仅 `inventory`、`doctor` |

daily 的 `inspect` / `verify` 都针对显式 logical symbol 和 closed-day window；它们严格离线、
不补数、不写 cache。`verify --report` 只接受 native-daily fill report。daily 不支持 `inventory` 或
`doctor`，因为单文件 layout 目前没有可靠的全量 logical-symbol 枚举；`--kind all` 也不包含 daily。
`metadata-refresh` 不属于 cache family；保留默认 `--kind tick`，只支持 `--market futures`。
它显式调用官方 metadata source，在 exclusive root remote-fill lock 内保存 immutable sidecar；不会改写
`.tqbn` 或 minute 文件。新 snapshot 覆盖请求窗口即可供 `CacheOnly` 解析；若已有更宽、兼容的 active
snapshot，显式维护仍原子推进 active pointer；旧 snapshot 按 content hash 保留，可供已绑定旧 cache
partition 的 reader 使用。

```bash
TQ_AUTH_USER='your-account' TQ_AUTH_PASS='your-password' \
cargo run -p tqsdk-cache -- \
  --cache-dir /var/lib/tqsdk/history metadata-refresh \
  --symbol SHFE.op2601 \
  --start 2025-09-25T00:00:00+08:00 \
  --end 2025-09-26T00:00:00+08:00
```

minute 的 format id 为 `tqsdk.minute-kline.monthly.v5`，但 namespace 有意保持
`minute-kline-v3/trading-YYYYMM/<escaped-symbol>.tqmk`。每个文件属于一个
`logical symbol × trading month`。v5 将完整 60s 行 payload 以 zstd 无损压缩（仅在更小
时启用），仍保留每一行及其精确字段；reader 流式解压并校验原始 payload checksum。旧 v4
文件不会被普通 read/fill 静默覆盖，必须先显式迁移；v3 仍不会自动迁移、覆盖或删除，deep doctor
会将两者标记为 `legacy_unsupported`。

```bash
tqsdk-cache --cache-dir DIR --kind minute migrate --apply --backup-dir DIR-v5-backup
```

每个 v5 月文件绑定写入时的 immutable metadata snapshot。active metadata pointer 随后前移不会单独
使已有分区失效：`inspect`、`fill --dry-run`、`verify` 和普通 `fill` 遇到滚动扩展的 active snapshot 时，
会加载月文件绑定的旧 immutable sidecar，并只在实际 cached range 内比较 schema、market、logical symbol、
session、交易日和 physical mapping。区间语义相同的旧 coverage 直接复用，新增日期保持为缺口；当前月写入
新数据时才原子迁移 header。缺少 sidecar、session/交易日/映射变化、损坏或语义冲突的混合分区仍默认
fail closed；CLI 不会为此自动删除、重写或重新下载数据。只有操作者显式传
`--kind minute fill --repair-stale` 时，CLI 才会在同一次 facade remote-on-miss warmup 中，取得
root remote-fill lock 并完成认证预检后，删除与覆盖窗口快照冲突的整月分区，再补齐缺口；若没有任何已持久化
snapshot 覆盖整个请求窗口，则该 flag 会删除窗口内所有已存在的 minute 月分区，让官方 metadata refresh
建立唯一的目标 snapshot。锁忙或 repair 所需认证缺失时，命令失败且不删除任何分区。该 flag 不支持 tick
或 `--dry-run`。
remote-on-miss metadata 会覆盖涉及的完整 CST trading month；短查询生成的 snapshot 不会替换更宽的 active
pointer，后续查询会优先复用覆盖其范围的 retained snapshot。

`--market futures|stock` 只影响 `--kind minute fill` 的 server-side backtest endpoint：
futures 是默认值，允许 `--universe`；stock 必须提供一个或多个显式 `--symbol`，不支持 futures
universe selector。tick 与 daily fill 不支持 stock market；`--kind tick|daily --market stock` 是 usage error。
daily fill 同样支持 futures `--symbol` / `--universe`，但它只请求 server-side native `1d` chart；当前或
未来交易日、`--include-open-day`、`--repair-stale`、`--daily-slices` 与无法映射到 history client 的
remote-scheduler 参数都会直接以 usage error 拒绝，绝不降级成 tick/minute 聚合。

tick、minute 与 daily cache 都没有自动 retention、max-byte eviction 或后台清理。读写、普通回测和
普通 `--kind minute|daily fill` 都不会删除已有数据；`--repair-stale` 是唯一用于混合 snapshot 的显式、
受控删除例外。

### Tick companion lock repair

`repair-locks` 只接受 `--kind tick`（也是默认 kind）；`--kind minute|all repair-locks` 是 usage error。
它用于补回**既有** tick TQBN 缺失的 legacy `<partition>/.tqbn.lock` 和逐文件
`<file>.tqbn.lock` companion lock，而不是修复行情数据。先停止同一 cache root 的所有 reader/writer，再由
可写 owner 执行：CLI 在整个操作中持有 exclusive root stable-view gate；`--apply` 先以非截断方式创建每个
唯一 Tick 分区缺失的 legacy lock，再为缺失的逐文件 sidecar 取得 normal exclusive TQBN/file lock。

```bash
# 默认 DryRun：检查每个 Tick 分区的 legacy lock，并逐文件检查 sidecar。
cargo run -p tqsdk-cache -- \
  --cache-dir /var/lib/tqsdk/history --kind tick repair-locks

# 只有确认计划且 reader/writer 已停止后，才创建缺失的 legacy 和逐文件 lock file。
cargo run -p tqsdk-cache -- \
  --cache-dir /var/lib/tqsdk/history --kind tick repair-locks --apply
```

DryRun 不创建 TQBN companion lock；`--apply` 只创建缺失的 regular lock，绝不改写 TQBN bytes、rows、coverage、
index 或 remote/auth state，也绝不调用 fill 或 compaction。JSON 将 unique parent 的 legacy 结果放在
`legacy_partition_locks[]`（并提供 `legacy_partition_locks_*` 计数），逐文件结果保持在 `files[]`。无效 lock、
I/O 或 lock 错误会标为 `failed`，但后续目标仍会继续尝试；任一 legacy 或逐文件失败都以 exit code `1` 结束。

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

# final-only native daily fill：远端只请求官方 1d chart，不从 60s/tick 聚合。
TQ_AUTH_USER='your-account' TQ_AUTH_PASS='your-password' \
cargo run -p tqsdk-cache -- \
  --cache-dir /var/lib/tqsdk/history --kind daily fill \
  --symbol KQ.i@SHFE.au \
  --start-day 2026-06-01 --end-day 2026-06-30 \
  --symbol-concurrency 2 \
  --report /var/lib/tqsdk/history/reports/daily/june.json

# 仅在已确认 mixed snapshot 时显式修复冲突整月分区，再由同一次 fill 补齐。
TQ_AUTH_USER='your-account' TQ_AUTH_PASS='your-password' \
cargo run -p tqsdk-cache -- \
  --cache-dir /var/lib/tqsdk/history --kind minute fill \
  --symbol KQ.m@SHFE.au \
  --start-day 2026-06-01 --end-day 2026-06-30 \
  --repair-stale

# stock minute fill 必须显式给出 symbol，不能传 --universe。
TQ_AUTH_USER='your-account' TQ_AUTH_PASS='your-password' \
cargo run -p tqsdk-cache -- \
  --cache-dir /var/lib/tqsdk/history --kind minute --market stock fill \
  --symbol SSE.000300 \
  --start-day 2026-06-01 --end-day 2026-06-30
```

完整 static cache 命中时 fill 不创建远端 session，也不需要 `TQ_AUTH_USER` /
`TQ_AUTH_PASS`。发生缺口时，tick、minute 与 daily 都只通过官方 server-side backtest stream 补齐，
不使用 `tq_dl` 或专业历史下载权限。minute 只请求 60-second Kline stream；daily 只请求 native 1d
chart；每个 batch 必须收到远端 terminal 成功才写 final coverage，合法的零行窗口也可成为 final。取消、
超时或失败 batch 不会标记其未完成范围。

tick fill 按 trading day 顺序执行，以 8192 rows 缓冲追加；取消会 flush 已接受短尾，但不推进未 terminal
范围的 final/provisional coverage。fill-only 不回读刚写入 rows；`rows_written` 只统计实际物理落盘，
同一 shared fill 被多个 logical request 复用时只计一次，完整 cache hit 为 `0`。final fill 只按本轮
实际远端回填的 `symbol × trading day` 去重 compact 相交分区；provisional fill 跳过 compaction。

## Immutable history snapshot publisher

`snapshot` 是独立的 publisher 命令树，必须显式传 `--history-root`；它不会重新解释现有
`--cache-dir`。writable cache 先在 exclusive stable-view gate 内按 data-owned file-role
allowlist clone/import 到 staging：`.tqbn` 与 pointer 只独立复制，`.tqmk` / `.tqdk` / immutable
metadata 可 hardlink（失败时普通 copy），lock/sidecar 只重建、不进入 manifest。

```bash
# 完全只读；不会创建 history root、staging 或 operation lock。
cargo run -p tqsdk-cache -- snapshot \
  --history-root /var/lib/tqsdk/history-published dry-run \
  --source-cache-dir /var/lib/tqsdk/history-writable

# 生成 staging generation；import 强制独立 copy，clone 可共享安全 immutable inode。
cargo run -p tqsdk-cache -- snapshot \
  --history-root /var/lib/tqsdk/history-published clone \
  --source-cache-dir /var/lib/tqsdk/history-writable \
  --catalog-complete --catalog-symbol SHFE.au2612

# 含数据文件的 generation 在 publish 前必须完成 strict inspect + 真实 CacheOnly query smoke。
cargo run -p tqsdk-cache -- snapshot \
  --history-root /var/lib/tqsdk/history-published verify \
  --snapshot-id s-20260829-8d19c4af \
  --request-file verify.json

cargo run -p tqsdk-cache -- snapshot \
  --history-root /var/lib/tqsdk/history-published publish \
  --snapshot-id s-20260829-8d19c4af
```

verification request file 使用窄 JSON schema：`series` 是 `tick`、`minute` 或 `daily`；minute
另带纳秒 `duration_ns`。每项都带 `request_id`、`symbol`、`start_ns`、`end_ns`。请求必须覆盖
manifest 中每种实际数据 role；metadata-only generation 不需要伪造 query。

```json
{"requests":[{"series":"tick","request_id":1,"symbol":"SHFE.au2612","start_ns":1787932800000000000,"end_ns":1788019200000000000}]}
```

`prewarm` 只写隔离 staging 副本，并可能返回新的 `snapshot_id`。`publish` 固定执行 data/manifest
sync、generation rename、`snapshots/` sync、CURRENT temp sync、CURRENT rename、history-root sync；
CURRENT rename 后 root sync 失败属于 indeterminate，必须运行 `snapshot recover`。`rollback`、
`recover`、`scrub`、`gc` 默认只读，只有 rollback/recover/gc 的 `--apply` 执行 mutation。GC 默认
保留 CURRENT 加两个 previous compatible generation；它只在取得 exclusive generation lease 后把
目标原子移到 staging tombstone，再删除并 fsync，shared lease 忙时跳过。relay/reader 永不执行 GC。

## 历史查询与 LLM 上下文

`query` 直接复用 `tqsdk-data::BacktestHistoryClient`，不解析 `.tqbn` 文件，也不增加另一份
history store。它接受 RFC 3339 的半开区间 `[start, end)`，支持 Tick、任意合法 Kline 周期，以及
主连所需的 logical → physical segment 投影。当前 cache-backed query 只支持 futures。
行类型由 `--series tick|kline` 选择；query 保留默认 `--kind tick`，而 `--kind minute|daily|all` 和
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
线为 `2026-07-31T09:00+08:00`，不填充无意义纳秒）；默认 `--llm-timezone shanghai`，在 `time`
行显式写为 `timezone,Asia/Shanghai`，不使用有歧义的 `CST`；可传 `--llm-timezone utc` 保持 UTC。
`offset` 将已选择的 `t` 列改为相对于 `ref`
的整数，并声明精确 `unit`（例如 `1m`）；`both` 仅在选择 `t` 时紧随它增加派生 `dt` 列，便于
人工与模型逐行对照。未显式传 `--llm-time` 时，`--timestamp offset` 仍会选择 LLM 的 `offset`
模式。所有 LLM 时间区间都显式标明 `end_exclusive=true`，Kline 还会标明其时间是 bar start；显示
时区绝不改变 UTC instant、cache coverage 或夜盘的 trading day 语义。
默认数字是可读 decimal；`--number-format scaled-int` 必须显式传
`--price-tick`（或在 request file 的对应 block 提供），避免猜测合约精度；price tick 必须有限且为正，
价格必须是 tick 的整数倍，CLI 不会静默四舍五入。缺失或非有限浮点以空 CSV cell / JSON `null` 表示，
真实零值始终为 `0`。LLM CSV 在 scaled-int 模式会仅在相关 block 写出 `price_tick`，从不让模型猜测
缩放比例。

`--output-format jsonl` 的稳定协议为 `tqsdk-history-jsonl/1`。`--output-format llm-csv` 的稳定
协议为 `tqllm-csv/3`：每个 symbol × series × period 是独立 block，按 `block`、`time`、`columns`、
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

| 命令 | tick | minute | daily |
| --- | --- | --- | --- |
| `inventory` | 快速枚举日分区和文件问题，不解码 records | 快速枚举月文件，不解码文件内容 | 不支持：不能可靠枚举 logical symbol |
| `repair-locks` | 默认 DryRun 检查每个 legacy 分区 lock 并逐文件检查 sidecar；`--apply` 只补既有 `.tqbn` 缺失的 lock | 不支持 | 不支持 |
| `inspect` | 显式 physical cache symbol 的 coverage | 显式 logical cache symbol 的 final-60s coverage | 显式 logical cache symbol 的 native-1d final coverage |
| `verify` | CacheOnly coverage，选配完整本地 tick replay | CacheOnly final coverage，选配流式读取 minute rows | CacheOnly final coverage，选配读取 native-1d rows |
| `doctor` | 深度解码 TQBN tick partitions | 深度解码 monthly minute files，并报告 `readable`、`legacy_unsupported`、`unsupported_version` 或 `corrupt` | 不支持：不枚举 symbol 文件 |

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

# 复用 daily report 的 root、window 和 logical symbols；可选重放本地 1d rows。
cargo run -p tqsdk-cache -- \
  --kind daily verify \
  --report /var/lib/tqsdk/history/reports/daily/june.json \
  --replay --min-rows 1
```

tick `fill` 保留 CST `18:00` TQBN partition、当前交易日 provisional checkpoint 与
`--require-final` 的既有合同。minute fill 则严格 final-only：当前或未来 trading day 会拒绝，
`--include-open-day` 也不适用于 minute。`--last-trading-days` 与 `--calendar auto|required|off`
仍可用于选择 closed-day 窗口；日历只做选择和进度，不替代 cache coverage。

## 并发锁与任务中断

root advisory gate 的规则固定如下：

| 操作 | root gate |
| --- | --- |
| 普通 tick/minute/daily fill、`query --policy remote-on-miss` | shared |
| tick `repair-locks`（含 DryRun） | exclusive |
| cache refresh、`fill --repair-stale`、tick/minute/daily verify、tick/minute doctor、真实 minute/daily purge | exclusive |
| inventory、fill dry-run、minute/daily purge dry-run | none |

shared gate 允许不同 series 并发，同时阻止 destructive/stable-view maintenance 穿插。每个实际补洞再取
`cache family × cache symbol` 的跨进程 lease；等待者重查 coverage 后复用已有结果，不重复发远端请求。
TQBN 日分区和 minute 月分区仍使用各自文件锁。TQBN reader 在锁内打开文件并固定 checkpoint-confirmed
prefix，之后从 opened-file snapshot 读取；首次初始化用 sync + atomic rename，未确认坏 suffix 可在下一次
fill 恢复，无 checkpoint 的旧文件严格全量校验。该协议不保证新旧 binary 进程长期混跑。

query 的 shared gate 在 `collect_all()` 和 terminal/coverage 验证完成后释放；JSONL/LLM payload 渲染与
stdout/文件发布不持锁。第一次 Ctrl-C/SIGTERM 请求协作取消：停止新任务，tick flush 已接受短尾但不
提交未完成 coverage，minute 不提交未 terminal buffer，收敛后返回 130。第二次信号立即退出 130。

### 原始节假日日历

`fill` 通过 `DataClient::query_trading_calendar_holidays()` 复用 Shinny 的公开节假日源，不需要
账号。它不再把有限日期范围写成日历快照，而是在 cache root 下保存不可变、内容寻址的原始集合：

```text
meta/trading-calendar-holidays-v1/
  active.json
  snapshots/<content-hash>.json
```

每个 snapshot 记录 source URL、fetch 时间、排序去重后的 holiday dates、content hash 与支持年份。
旧 `meta/trading-calendar-v1.json` 不会被删除或改写，但 `--last-trading-days` 不再读取它。
`--calendar auto` 在本地 raw snapshot 不能覆盖所需年份时才拉取；`--last-trading-days` 因此不会在
日历不足时退化为 weekday 猜测。`--calendar required` 同样 fail closed，`--calendar off` 不允许
`--last-trading-days`。`--refresh-calendar` 强制重新拉取并推进 active pointer；配合 `--dry-run`
只报告 remote candidate hash，不写任何文件。

默认 `--last-trading-days N` 只选当前 open TQBN trading day 之前的 N 个交易日。显式
`--end-day` 必须早于当前 open TQBN trading day；落在周末或 holiday 的 anchor 会向后解析到最近的
closed trading day。显式 `--start-day/--end-day` 的数据窗口仍由 TQBN 规则确定，日历不会把它们改写为
另一段数据范围。

## 报告与进度

普通 tick fill 默认把 schema-v2 report 写入 `<cache-root>/reports/`，兼容读取旧 schema-v1 report。
minute fill 使用独立的 `cache_kind=minute` report（当前 schema v1），默认写入
`<cache-root>/reports/minute/tqsdk-cache-minute-fill-<utc>-<pid>.json`。minute report 只包含
logical cache symbols；`--kind minute verify --report` 只接受此类 report，且以 report 记录的
canonical root、range 和 symbols 为准。

daily fill 使用独立的 `cache_kind=daily` report（当前 schema v1），默认写入
`<cache-root>/reports/daily/tqsdk-cache-daily-fill-<utc>-<pid>.json`。它绑定 logical symbol、
closed-day window 和 canonical root；`--kind daily verify --report` 只接受此类 report。

进度总是写 stderr。`--progress jsonl` 对 tick、minute 与 daily fill 都输出 schema-v2
`tqsdk-cache.progress` JSONL，并带 `cache_kind: "tick" | "minute" | "daily"`；不要再按旧 schema-v1
解析。tick 的 progress 以 physical cache symbol 展示，minute 与 daily 则以 logical symbol 展示。

## 显式破坏性维护

CLI 的常规显式维护是 minute purge：必须恰好一个 `--symbol`、完整的 `--start-day` / `--end-day`
window 以及 `--yes`。`--dry-run` 不写入，只列出将删除的月文件、路径和大小；真实 purge 会删除与请求
窗口相交的整个 monthly partition，而不是单条 K 线。`fill --repair-stale` 是另一条更窄的显式维护路径：
它只针对已判定为 mixed snapshot 的月文件，并在同一 root fill lock 与 auth preflight 成功后用同一次
remote fill 补齐；锁忙或认证缺失时不删除分区，且不能和 `--dry-run` 使用。

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

daily purge 同样必须恰好一个 `--symbol` 和 `--yes`，但**不接受** `--start-day` / `--end-day`：
native 1d cache 的一个 logical symbol 就是一整个 `.tqdk` 文件。`--dry-run` 只列出该文件和大小；
真实 purge 在稳定视图锁内删除整个文件。

```bash
# 先确认会删除的整只 logical symbol 文件。
cargo run -p tqsdk-cache -- \
  --cache-dir /var/lib/tqsdk/history --kind daily purge \
  --symbol KQ.i@SHFE.au --dry-run

# 明确确认后删除该 .tqdk 文件。
cargo run -p tqsdk-cache -- \
  --cache-dir /var/lib/tqsdk/history --kind daily purge \
  --symbol KQ.i@SHFE.au --yes
```

tick 的 refresh / purge / compact 仍是显式 SDK API，不由 CLI 自动执行。真实 minute purge 在 exclusive
root gate 内再取每月文件锁；daily purge 在同一 root gate 内再取 symbol file lock。两种 dry-run 都
故意不取稳定视图 gate，应先核对范围。无论哪一类 cache，都不会自行清理。

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
