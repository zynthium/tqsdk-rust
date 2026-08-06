# 回测缓存 CLI

> 文件名保留 `backtest-tick-cache-cli.md` 作为既有链接兼容；本页定义的对象已包含 tick 与
> canonical-minute cache。

## 目的与边界

`tqsdk-cache` 是固定 history root 的可选 operator CLI。它复用 `tqsdk` 的 cache-backed
backtest/warmup 与 `tqsdk-data` 的 cache API，适合 cron、CI 或人工检查；它不改变 SDK 的默认运行
路径，也不拥有 session、状态树、remote protocol client、live recording loop、store adapter、relay 或
daemon。

它不是新的 cache format 定义者：tick 的持久化合同属于 `BacktestTickCache` / TQBN daily v2，minute
的持久化合同属于 `MinuteKlineCache`。格式细节见
[history-cache-format.md](history-cache-format.md)。

## Cache family 与命令路由

全局 `--kind tick|minute|all` 选择 cache family，缺省为 `tick`：

| kind | 物理分区 | symbol 语义 | 命令 |
| --- | --- | --- | --- |
| `tick` | `series/<YYYYMMDD>/tick/<escaped-symbol>.tqbn` | physical cache symbol | `inventory`、`inspect`、`fill`、`verify`、`doctor` |
| `minute` | `minute-kline-v3/trading-YYYYMM/<escaped-symbol>.tqmk` | logical cache symbol | `inventory`、`inspect`、`fill`、`verify`、`doctor`、`purge` |
| `all` | 上述两类汇总 | 不接受 symbol/range 操作 | 仅 `inventory`、`doctor` |

`--kind all` 与 `inspect`、`fill`、`verify`、`purge` 组合是 usage error。`purge` 当前只支持
`--kind minute`。

`query` 不使用 `--kind`：它以 `--series tick|kline` 表达所需 rows，并可同时读取 Tick 与
canonical-minute durable sources；`--kind minute|all query` 是 usage error，避免把运维 cache family
误当成用户查询的 row shape。

minute 的 `--market futures|stock` 仅在 minute `fill` 时选择 server-side backtest endpoint：

- `futures` 是默认值，可用 `--symbol` 与 futures `--universe`；dynamic universe 仍复用 SDK resolver。
- `stock` 只能使用显式 `--symbol`，拒绝 futures `--universe`。
- tick fill 不支持 stock market；`--kind tick --market stock` 是 usage error。

## 数据来源与 finality

```text
tick fill
  -> Tq::futures().backtest(range).remote_on_miss().warmup()
  -> TQBN tick coverage / official server-side backtest tick stream on miss

minute fill
  -> Tq::{futures,stock}().backtest(range).kline(symbol, 60s).remote_on_miss().warmup()
  -> MinuteKlineCache final coverage / official server-side backtest 60s Kline stream on miss
```

minute cache 的唯一持久 K 线是 official server-side backtest terminal 成功确认的 `60s` bar；
不回退到 `DataClient` 历史下载路径，也不写 native higher-period K 线。单个 remote batch 在 terminal
成功前保留 rows，成功后才提交该 batch 的 final coverage；合法的零行窗口也可提交 final coverage。
取消、超时、未确认结束或失败 batch 不得把未完成范围标记为 final。

tick fill 按 TQBN trading day 顺序消费每个 physical symbol 的缺口，并以 8192 rows 为短批写入；这限制
长窗口的峰值内存和每行 fsync 开销。取消时已接受的短尾先 flush，但不提交本轮 final/provisional
coverage。fill-only materialization 不再为了生成报告回读刚写入的 cache；`rows_written` 只统计本进程
实际物理落盘 rows，同一 shared fill 被多个 logical request 复用时只计一次。完整 cache hit 合法返回
`rows_written=0`。

minute 缓存当前的 format id 是 `tqsdk.minute-kline.monthly.v4`，schema/file version 为 4，按
`logical symbol × trading month` 分区。目录名有意保持 `minute-kline-v3`：旧 v3 文件不自动迁移、
覆盖、删除或当作命中；coverage/read fail closed，deep doctor 将其分类为 `legacy_unsupported`。

每个 v4 月文件绑定写入时的 immutable metadata snapshot。active pointer 后续变化不会单独令历史
月文件失效：minute `inspect`、`fill --dry-run`、`verify`、普通 `fill` 和 cache-backed reader 在 hash
不同时加载月文件绑定的旧 sidecar 与目标 sidecar，只对实际 cached range 比较 schema、market、logical
symbol、session、交易日和 physical mapping。语义相同的旧 coverage 继续命中，新增尾部日期仍是 miss；
当前月下一次原子写入才迁移 header。缺少 sidecar、session/交易日/映射变化、损坏文件或语义冲突的混合
分区仍 fail closed，且不会
自动 purge、重写、下载或拼接数据。唯一例外是显式的 `--kind minute fill --repair-stale`：active snapshot
覆盖窗口时，它仅在同一 root remote-fill lock 已取得且 repair 所需认证预检成功后删除与 active snapshot
冲突的整月分区，随后由同一次 remote-on-miss fill 重建；锁忙或认证缺失时不删除分区。普通 `fill`、
`inspect`、`verify` 和 cache-backed reader 仍 fail closed。

本地 replay 的周期合同不变：`<60s` 从 tick cache 按 session 合成，`60s` 从 canonical minute cache
读取，`>60s` 只允许 `N × 60s` 并从 closed 60s K 按固定 CST `18:00` trading-day grid 本地聚合；
盘中 break 不重置高周期 bucket，且 break 内不虚构 60s row。`61s`、`90s` 等拒绝。K-only `>=60s`
不会隐式补 tick。

## 区间查询

`tqsdk-cache query` 是 `BacktestHistoryClient` 的 CLI adapter，不直接解析 TQBN、不会创建新的
metadata/store/session owner，也不复制数据进入 query-specific cache。每个请求都是 RFC 3339 半开
区间 `[start, end)`，按 request id 交给 `query_batch(...).collect_all(max_memory_bytes)`；因此 LLM
输出在 emit 前已具备明确的内存上限和每个 request 的 terminal report。

默认 `--policy remote-on-miss` 按 durable coverage 先读 cache；只有确认缺口后才懒加载
`TQ_AUTH_USER` / `TQ_AUTH_PASS`，通过官方 futures server-backtest stream 补齐并复用既有 cache fill
协调。`--policy cache-only` 严格不联网。cache-backed query 当前只支持 futures，stock 不会暗中改走
另一条历史下载路径。

普通 flags 表达同质 batch（可重复 `--symbol`）；`--request-file` TOML 的 `version = 1` / 多个
`[[request]]` 表达异质 batch。每个 request 可指定 `symbol`、`series`、`start`、`end`、Kline `period`、
strict `fields`、`weight` 与可选 `price_tick`。`weight` 只影响 LLM 预算分配，不改变查询或 cache
coverage。`query schema --series tick|kline` 是字段和 alias 的发现入口。

query 只在每个 request 取得 terminal report 后输出。每个 emitted block 的 finality 必须为 `Final`，且
`cached_ranges` 与 `remote_filled_ranges` 的并集必须完整覆盖请求 `[start, end)`；non-final 或 coverage
不完整都是 hard failure。`--allow-partial` 只允许其他 request failure（以及下述 LLM metadata failure）
以 `gap` 形式出现，不放宽上述 finality/coverage gate。

主连及 session-sensitive Kline 的 metadata 沿用 immutable sidecar。JSONL 会如实标记缺失的 sidecar；
`llm-csv` 还必须能由 active snapshot 验证 terminal report 的 snapshot hash，才会输出可供模型解释的
block（连续合约会保留必要的 underlying / segment mapping）。这是 LLM export 特有的更严格 fail-closed
规则：V2 默认不把 session reference、snapshot hash 或 session JSON 放进模型上下文，但仍在输出前完成
同样的验证。底层 retained-snapshot reader 在 active pointer 前移后仍可读取符合其验证条件的历史分区，但该 export
不会把它们当作已通过 active-snapshot 验证的模型输入。缺失或不匹配时默认 fail closed；
`--allow-partial` 可以省略该 block 并记录 gap，绝不伪造 session。

## 各命令的读写语义

| 命令 | tick | minute |
| --- | --- | --- |
| `inventory` | 快速枚举日分区、文件/字节/已知问题；不解码、不建 root | 快速枚举月文件、文件/字节；不解码、不建 root |
| `inspect` | read-only coverage/missing ranges，要求 explicit physical symbols | read-only final-60s coverage/missing ranges，要求 explicit logical symbols |
| `fill --dry-run` | CacheOnly 预检，不取 fill lock、不写 report/rows | 同样只做 final coverage 预检 |
| `fill` | missing tick ranges 远端补齐；当前日可走 explicit provisional 规则 | 仅 closed-day final ranges，按 60s Kline stream 补齐；显式 `--repair-stale` 才会在 root fill lock 和 auth preflight 后删除已定位的 mixed-snapshot 月分区 |
| `verify` | CacheOnly coverage，选配 local tick replay | CacheOnly final coverage，选配流式读取 local minute rows |
| `doctor` | exclusive root stable view 下深度解码 TQBN | exclusive root stable view 下深度解码 `.tqmk`，状态为 `readable` / `legacy_unsupported` / `unsupported_version` / `corrupt` |
| `purge` | 不提供 CLI purge | 受控的整月分区删除 |

`verify` 绝不访问远端或写 cache。它接受 explicit closed-day window，或通过 `--report` 绑定一次
fill 记录的 canonical root、window 和 symbols；额外给出的 `--cache-dir` 必须与 report root 一致。
minute `verify --report` 只接受 `cache_kind=minute` report；tick report 继续兼容 persisted schema v1/v2。

## 交易日与 open-day 规则

TQBN tick partition 使用 CST `18:00:00` 作为交易日边界。tick fill 可以在显式 window 结束于当前
TQBN trading day 时写 non-final provisional checkpoint；`--require-final` 会拒绝该情形，
`--last-trading-days` 只选择已结束日。checkpoint 不进入 normal coverage/cache-hit，最终 closed-day
reconcile 才提交 final coverage。

minute fill 没有 provisional 语义：当前或未来 trading day 一律不能 claim final，因而被拒绝；
`--include-open-day` 也不适用于 minute。`--calendar auto|required|off` 与
`--last-trading-days` 只用于选择 closed-day window 和进度分母，不能推断任一 cache 的 coverage。

calendar 的 durable sidecar 是 raw holiday set，不是一个有限的每日展开：

```text
meta/trading-calendar-holidays-v1/
  active.json
  snapshots/<content-hash>.json
```

snapshot 是 immutable，包含 Shinny holiday source URL、`fetched_at`、排序去重的 raw holiday dates、
content hash 和支持年份；`active.json` 原子地选择当前 snapshot。旧
`meta/trading-calendar-v1.json` 保留以保证非破坏性迁移，但不参与新的 `--last-trading-days` 解析。
`--calendar auto` 只在本地 raw snapshot 不能覆盖所需 anchor year(s) 时请求远端；`required` 在同一
情形强制成功，`off` 拒绝 `--last-trading-days`。任何无法获得所需年份或不足以选择 N 个 closed days
的情况都 fail closed，绝不退回 weekday-only 推断。`--refresh-calendar` 强制 remote refetch 并更新
active pointer；`--refresh-calendar --dry-run` 可读取远端 candidate，但绝不写 snapshot 或 pointer。

没有显式 anchor 时，`--last-trading-days` 从当前 open TQBN trading day 之前严格选择；显式
`--end-day` 必须小于当前 open TQBN trading day。周末或 holiday anchor 按 raw set 向前选择最近的
closed trading day。显式 `--start-day/--end-day` 仍表达调用者的数据窗口，日历不会改写它们。

完整 cache 命中时两类 fill 都不需要认证或远端 session。需要补洞时才要求 `TQ_AUTH_USER` /
`TQ_AUTH_PASS`，且不使用 `tq_dl` / 专业历史下载权限。

## 输出、报告与进度

默认 stdout 是人工摘要；`--output-format json` 请求稳定机器输出，默认 V3 envelope，
`--output-schema v2` 仅保留旧兼容 shape。coverage 不完整退出 `1`，usage 错误退出 `2`，cache busy
退出 `75`，协作式取消退出 `130`。

- tick normal fill report 为 schema v2，默认路径
  `<cache-root>/reports/tqsdk-cache-fill-<utc>-<pid>.json`，兼容读取 schema v1。
- minute normal fill report 为独立 schema v1，带 `cache_kind=minute` 与 logical symbols，默认路径
  `<cache-root>/reports/minute/tqsdk-cache-minute-fill-<utc>-<pid>.json`。
- tick 与 minute fill report 的 `calendar`（存在时）只记录 mode、source、是否已持久化以及 raw
  snapshot 的 source URL、fetch 时间、hash、支持年份和 holiday count；不会嵌入完整 holiday list。
  text output 会显示 `local holidays, years YYYY–YYYY`；dry-run remote candidate 还会显示
  `not persisted` 与 candidate hash。
- `--progress jsonl` 始终写 stderr，schema 为 v2、kind 为 `tqsdk-cache.progress`，并含
  `cache_kind: "tick" | "minute"`。脚本不得继续按 schema v1 解析。

`query` 的 raw format 只适用于 query 命令，stdout 是 data、stderr 是诊断：

| format | stable protocol | 语义 |
| --- | --- | --- |
| `jsonl` | `tqsdk-history-jsonl/1` | lossless rows，附 `manifest`、每 block 的 `block` / 零或多个 `row` / `complete`、可选 `gap`、最终 `end` records；不压缩 |
| `llm-csv` | `tqllm-csv/3` | GPT-5.6-oriented CSV-like blocks；每 block 依次写 `block` / `time` / `columns` / 可选 `segment` / `summary` / `data` / `block_end`，最终以 `document_end` 收尾；默认省略内部 query/session/blob provenance |

两者都遵守 strict `--fields` projection：输入可使用长 alias，输出固定为 canonical short alias 和
schema order。JSONL 默认 row timestamp 是完整 ISO 8601；`--timestamp offset` 改为相对 block start
的整数 ns。LLM CSV 的 `--llm-time iso|offset|both` 默认 `iso`：它按该 block 的最小无损精度以
`Asia/Shanghai` 的显式 `+08:00` 写出时间（分钟 Kline 不填充秒/纳秒），`time.timezone` 固定说明时区；
`--llm-timezone utc` 可覆写为 UTC。`offset` 用 `time.ref` 加声明的 `time.unit` 表示整数 `t`；`both` 在
已选择 `t` 后额外写派生 `dt` 以供对照。未显式指定时 `--timestamp offset` 选择 LLM `offset` 模式。
`time.end_exclusive=true` 固定表达半开区间，Kline 的 `time.bar_time=start` 明确其 bar timestamp 语义。
显示时区仅改变同一 instant 的文本表示，绝不改变 cache range、server-backtest 输入或 night session 的
trading day 归属。
默认数字为 decimal；`--number-format scaled-int` 必须显式给出有效的 `price_tick`，非有限数为 missing
而不是零；LLM CSV 会把该缩放比例仅写在相关 block。LLM `columns` 是其唯一有序 projection（`both`
的派生 `dt` 是显式 opt-in 例外）；JSONL 的有序 projection 仍是 `block.fields`，JSON `row.data` object
的成员顺序不是 contract，consumer 必须按字段名和 `fields` 解析。

`llm-csv` 是原子产物：terminal success、coverage、metadata、summary、data hash 和 token budget
决策都完成后才写出。`--data-token-budget` 采用保守本地 GPT-5.6 estimate；不做模型/API 调用。若 full
payload 超预算，`--compression auto` 会按每 block weight 分配 metadata/summary 之外的残余，以
`balanced|price|volume-oi|microstructure` 确定性保留行，并将输出标为 `lossy`；`off` 则错误退出。
这不是精确 tokenizer 的替代品，最终 payload 的精确计数属于上游 agent/API。CLI 会在写出前完整构建
payload，但 stdout 本身没有 atomic-write 保证；`--output PATH` 仅用于 raw formats，stdout 随之为空，
并以同目录临时文件写完、sync 后 rename 原子发布，成功提示写入 stderr。

## 锁、取消与显式维护

同一 cache root 使用 advisory root gate 协调普通数据流和需要稳定视图的维护；它不替代 TQBN 日文件锁
或 minute 月文件锁：

| 操作 | root gate | 目的 |
| --- | --- | --- |
| 普通 tick/minute `fill` | shared | 多个互不冲突 series 可并发；阻止 refresh/repair/verify/doctor/purge 穿插 |
| `query --policy remote-on-miss` | shared | coverage inspection、远端补洞和 cache materialization 处于同一普通操作窗口 |
| cache refresh、`fill --repair-stale` | exclusive | 删除/重建与普通 fill/read plan 互斥 |
| tick/minute `verify`、`doctor` | exclusive | coverage/replay/深度诊断获得 root-wide stable view |
| 真实 minute `purge` | exclusive | 月文件删除不与普通 fill/query 交错 |
| `inventory`、`fill --dry-run`、minute `purge --dry-run` | none | 快速或计划视图；允许显示并发 fill 中间状态 |

`RemoteOnMiss` query 只在收集/验证 durable 结果期间持 shared gate；大 JSONL/LLM payload 的格式化和
stdout/文件发布在释放 gate 后完成，慢消费者不会阻塞 maintenance。每个实际远端补洞另有
`cache family × cache symbol` 的跨进程 lease：竞争者等待并重查 coverage，owner 完成后直接复用，避免
多个 shared root users 对同一 series 重复请求官方流。同进程请求仍复用 single-flight。

TQBN reader/writer 再用 per-file shared/exclusive lock。reader 在锁内打开 data file 并确认 tail
checkpoint 后，只读取该 opened file 的 confirmed prefix，可提前释放锁；首次初始化采用临时文件 sync +
原子 rename。无有效 checkpoint 的旧文件严格校验锁内捕获的完整物理长度，不能忽略坏 suffix，但无需在
整个解码期间持锁。coverage/provisional record 与紧邻 `TQCI` 是恢复原子对；孤立 record 属于未确认
tail，恢复时从 record 起点截断。root gate 与这些 sidecar lock 是当前版本的协作协议，不保证新旧版本
进程长期混跑；升级时应同步重启访问同一 root 的进程。

第一次 Ctrl-C 或 SIGTERM 触发协作取消：停止启动新 batch，tick flush 已接受短尾并可推进物理 tail
checkpoint，但不提交本轮 final/provisional coverage；minute 丢弃尚未 terminal 的内存 batch。已成功
terminal 并提交的范围继续有效。CLI 等待任务收敛后以 `interrupted` / 130 返回。第二次 shutdown signal
立即 `exit(130)`，不再等待 flush；tick warmup 成功后的 calendar 收尾期间该二次信号路径仍保持有效。

tick 和 minute 都没有自动 retention、max-byte eviction 或后台 cleanup。refresh/purge/compact 均是
明确的破坏性维护：tick 的对应操作仍通过显式 SDK API；CLI 常规 minute purge 须同时满足：

1. `--kind minute purge`；
2. 恰好一个 `--symbol`；
3. `--start-day` 与 `--end-day`；
4. 真正删除时传 `--yes`。

`--dry-run` 只列出会移除的月文件、路径与大小，不写任何内容。真实 purge 删除所有与请求 window
相交的整月分区，并在 exclusive root gate 内以每月文件锁执行；它不是跨 cache family 的原子事务。
`fill --repair-stale` 是另一条显式 minute maintenance path，不能和 `--dry-run` 或 tick 使用；它只在
同一 root remote-fill lock 和 repair 所需 auth preflight 成功后删除已由 active snapshot 比较定位的冲突整月
分区，并立刻由同一 remote fill 请求补齐。lock busy 或 auth 缺失时不删除任何分区。

## 验收

无网络回归至少覆盖 `tqsdk-data` minute cache ops、`tqsdk-cache` CLI 以及 `tqsdk` local backtest：

```bash
rtk cargo test -p tqsdk-data --test minute_kline_cache
rtk cargo test -p tqsdk-data --test minute_kline_cache_ops
rtk cargo test -p tqsdk-cache
rtk cargo test -p tqsdk --lib
```

真实验收只在用户授权、凭证已注入且使用 historical closed window 时进行。用少量指数合约填充
canonical 60s cache 后，比较 local closed-minute 聚合的 5m/15m K 与同窗口 official server-side
backtest Kline：检查固定 CST `18:00` trading-day bucket、跨盘中 break 行为、OHLC、volume 与 open
interest，记录 mismatch 数和少量
脱敏样本即可。详情见 [validation.md](validation.md)。
