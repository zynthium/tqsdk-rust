# `tqsdk-cache`

`tqsdk-cache` 是 `tqsdk-rust` 的可选 cache operator CLI。它管理同一 history root 中的
daily TQBN tick cache 和 canonical final-60s Kline cache；它是 workspace member，但不属于
Cargo default-members，也不会进入普通策略、回测或 live 行情的 hot path。

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
