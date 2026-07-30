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

minute 缓存当前的 format id 是 `tqsdk.minute-kline.monthly.v4`，schema/file version 为 4，按
`logical symbol × trading month` 分区。目录名有意保持 `minute-kline-v3`：旧 v3 文件不自动迁移、
覆盖、删除或当作命中；coverage/read fail closed，deep doctor 将其分类为 `legacy_unsupported`。

本地 replay 的周期合同不变：`<60s` 从 tick cache 合成，`60s` 从 canonical minute cache 读取，
`>60s` 只允许 `N × 60s` 并从 closed 60s K 本地聚合；`61s`、`90s` 等拒绝。K-only `>=60s`
不会隐式补 tick。

## 各命令的读写语义

| 命令 | tick | minute |
| --- | --- | --- |
| `inventory` | 快速枚举日分区、文件/字节/已知问题；不解码、不建 root | 快速枚举月文件、文件/字节；不解码、不建 root |
| `inspect` | read-only coverage/missing ranges，要求 explicit physical symbols | read-only final-60s coverage/missing ranges，要求 explicit logical symbols |
| `fill --dry-run` | CacheOnly 预检，不取 fill lock、不写 report/rows | 同样只做 final coverage 预检 |
| `fill` | missing tick ranges 远端补齐；当前日可走 explicit provisional 规则 | 仅 closed-day final ranges，按 60s Kline stream 补齐 |
| `verify` | CacheOnly coverage，选配 local tick replay | CacheOnly final coverage，选配流式读取 local minute rows |
| `doctor` | 深度解码 TQBN；tick stable-view lock 协调检查 | 深度解码 `.tqmk`，状态为 `readable` / `legacy_unsupported` / `unsupported_version` / `corrupt` |
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
- `--progress jsonl` 始终写 stderr，schema 为 v2、kind 为 `tqsdk-cache.progress`，并含
  `cache_kind: "tick" | "minute"`。脚本不得继续按 schema v1 解析。

## 锁、取消与显式维护

fill 复用 facade 的 root-scoped remote-fill 协调；tick file 仍有自己的文件级写锁。inventory 故意
不取稳定视图锁，可以显示 fill 的中间状态。minute doctor/verify 是 read-only 操作，不承诺 tick
doctor 所使用的全局 stable-view lock 语义。

tick 和 minute 都没有自动 retention、max-byte eviction 或后台 cleanup。refresh/purge/compact 均是
明确的破坏性维护：tick 的对应操作仍通过显式 SDK API；CLI 只暴露 minute purge，且须同时满足：

1. `--kind minute purge`；
2. 恰好一个 `--symbol`；
3. `--start-day` 与 `--end-day`；
4. 真正删除时传 `--yes`。

`--dry-run` 只列出会移除的月文件、路径与大小，不写任何内容。真实 purge 删除所有与请求 window
相交的整月分区，并以每月文件锁执行；它不是跨 cache family 的 root-wide transaction。

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
backtest Kline：检查 bucket/session 边界、OHLC、volume 与 open interest，记录 mismatch 数和少量
脱敏样本即可。详情见 [validation.md](validation.md)。
