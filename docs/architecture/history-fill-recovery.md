# Fill 中断与续填

本合同属于 `tqsdk-data` 的历史获取/持久化边界；CLI 负责跨进程启动串行化、信号收尾期限和显示进度。
连接调度与 DIFF 保留策略归 data；初始握手配置、认证和 HTTP 复用归 session；
core 提供通用连接尝试预算与结构化 HTTP 状态。不改变状态树结构、commit/cursor 或交易语义。

## 远端准入与认证

一个 history client 的独立 `(symbol, kind)` 序列可共享活跃 WebSocket，分页统一使用 8964
双 chart 轮换。连接的污染与 attach 在同一临界区检查；所有 reader 退出后才允许回池。
这一改动不合并下面的 durable checkpoint，也不允许回收被裁剪过的 DIFF 连接。
完整对齐范围、证据及剩余差异见 [回测网络对齐](backtest-wire-parity.md)。

官方历史 source 在进程内共享最多 2 个 WebSocket（包含空闲连接），独立于 client、
cache root 和 logical concurrency。池满不创建 overflow 连接；fill 先释放 series lease，
再可取消地等待额度，重新检查 coverage 后续填。额度等待不消耗远端 retry attempt。
等待采用 250 ms 轮询，不保证 FIFO 或等待时间上界；持续热任务可能延迟其他任务取得额度。

首次认证/建连共享至少 1 秒的起始间隔，准入锁保持到建立完成；干净的已建立连接
不重复申请建连时间槽。fill 显式设置 `websocket_connect_attempts(NonZeroUsize::new(1).unwrap())`
与 `ReconnectPolicy::max_attempts = Some(0)`，每个外层 attempt 只尝试一次 socket。
普通 session 的初始默认 3 次尝试和自动重连策略保持独立。

只有 terminal、chart cleanup 成功且未裁剪过 DIFF 的 session 才回池。每次消费检查
ticks/klines/charts/quotes 的合计 JSON 编码体积，超过 4 MiB 后沿用原裁剪路径，并在
source 关闭时销毁；不能让已裁剪 session 被后续 slice 复用。检查不分配完整 JSON 副本。
4 MiB 是保留策略阈值，不是 heap/RSS 硬上限；单个入站页及 Value/map 有额外开销。
commit log 同时限制为 8 项和 4 MiB。干净 session 最多复用 64 个 source，
空闲最多 30 秒；跨 Tokio runtime、不同完整凭证或认证/行情端点不会复用 socket。Tick、Minute、Daily
的切片、checkpoint、terminal 和 final coverage 规则不变。

同凭证和端点的拒绝状态跨 client/cache root 共享。认证/权限或 401/403 拒绝后，本进程不再
用该凭证创建 source。token 端点的其他 4xx（如 400 invalid_grant，429 除外）也归为认证拒绝；
行情地址发现的其他 4xx 保持 HTTP 错误，不误判为凭证拒绝。
429 停止当前 source，后续调用在至少 60 秒的共享冷却期内
fail closed；HTTP `Retry-After` 的秒数或日期可延长冷却期。WebSocket 依赖只保留状态码，
不能读取其 Retry-After，故使用 60 秒下限。其他暂时性错误最多尝试 3 次，
等待 2 秒、4 秒，各附加 0–1 秒随机抖动。legacy 字符串错误仍保留保守拒绝识别。

拒绝记录最多保留 16 组完整凭证；容量满时先淘汰未使用且未拒绝的条目，全部占用时
返回本地 Validation，不能伪装成服务端权限错误。token 缓存仍仅作用于无 trade target
的 backtest session：按完整凭证/认证端点隔离，最多 16 项，提前 30 秒失效，
单项最多保留 300 秒。token 不落盘、不进入诊断。地址发现 401/403 会失效 token。
backtest 认证和地址发现共享当前 Tokio runtime 的 HTTP 连接池（最多保留 8 个 runtime
的池，每 host 最多 2 个空闲连接、30 秒空闲期）；Authorization 在请求级设置。
地址发现结果没有新增缓存，避免猜测服务端有效期。

CLI 在同一用户可能访问远端的 fill 之间持有跨 cache root 的全局文件锁。无凭证及静态
dry-run 不取锁；带凭证的动态 universe dry-run 仍须准入。
Unix 锁为 `/tmp/tqsdk-cache-fill-<euid>.lock`，检查 owner、0600、单链接并拒绝符号链接；
Windows 使用用户 LOCALAPPDATA。默认最多等全局锁 30 秒，争用超时返回 `cache_busy`/exit 75，
`--lock-wait-secs` 可覆盖等待时间，耗时从显式的后续 root-lock 等待预算中扣除。
这是同用户 CLI 串行化，不是跨机器/不同用户/任意 SDK 调用的账号级配额；
冷却和 token 仍只在进程内，不能通过频繁重启来绕过服务端拒绝。

`ContractError::HttpStatus { status, retry_after_secs }` 保留 HTTP/握手状态而不包含
敏感 response body。401/403 的 kind 为 Auth；429/5xx 为 Http，只有 5xx 提供自动
backoff hint。此公开枚举新增 variant，外部穷举匹配需要适配。
本地默认值不是服务端承诺的安全阈值。

### 验收

使用本机 mock HTTP/WebSocket 和 ManualSession 验证：连接额度饱和不溢出、
取消后可继续获取、共享拒绝/冷却、单次握手及状态码、HTTP keep-alive 与请求级认证隔离、
小页保留/复用和超阈值裁剪/销毁。不得以静态配置或单元测试代替生产限流与吞吐验收。

## 提交与恢复粒度

- 日线沿用最多 32 个自然日的物理请求。每片只有本次 attempt 的 chart terminal、
  stream terminal 和 source close 均成功，才提交 coverage。重启从提交索引补缺口，
  不重新下载已完成片段；当前失败片段重拉。
- 分钟线父片段仍不超过 10,000 分钟，内部按最多 24 小时请求，最多 7 个子请求。
  子请求成功后保存独立 journal；整个父片段成功才提交 canonical coverage。
  该策略增加 source/session 请求次数，换取长片段的可验证续传边界。
- 未完成 attempt 的行不能混入下一次 attempt，即使下一次返回空终态。
  Kline 事件必须匹配本 attempt 的 chart ID；ChartCompleted 必须在 StreamCompleted 之前。
  空窗口也必须收到明确终态。最大 timestamp、稀疏 row ID 都不构成覆盖证明。

## 分钟暂存

路径：`<root>/.backtest-history-staging/minute-v1/<identity-sha256>.json`。
identity 包含 schema version、物理 symbol、精确父 range、metadata snapshot 和 provisional as-of。
独立 namespace 不参与 CacheOnly、普通 coverage、Timeline 或发布快照；旧程序忽略它并正常重拉缺口。
已有 KLOG 正式数据无需迁移；不同 identity 不复用暂存。

持久化记录包括连续 terminal-confirmed 前缀、最后确认子窗口的起点以及暂存行。
每完成一个子窗口、未提交行新增达到 1,024 条或协作取消/请求失败收尾时，
使用唯一临时文件、文件 fsync、rename、父目录 fsync 保存。
JSON 外层校验 SHA-256，浮点数保存 bit pattern；单文件限制 4 MiB、最多 10,000 行。
目录/文件符号链接被拒绝；写入始终在已有 root 生命周期锁及 per-series fill lease 内。

恢复时丢弃内存中的未确认候选，重拉最后一个已确认子窗口及其后缀。
重叠窗口按所有字段（含浮点位）比较；不一致时只丢弃该私有 journal，显式失败，
下次运行从整个父窗口重新获取，不混合不同结果。损坏或未知 journal 不作为覆盖依据，返回明确错误。
首次没有确认前缀的 journal 需要重拉整个子窗口；暂存行数不等于可跳过下载行数。

正式提交后删除对应 journal。提交成功但清理前崩溃时，重启以 canonical coverage 为准，
不会重复下载；残留私有 journal 不改变查询结果。清理失败可报告失败，但不回滚已完成的正式提交。
强杀时只保证已 fsync 的提交/journal，未刷盘的当前缓冲仍可能重新下载。

## 有限收尾

`fill --kind minute --repair-stale` 在等待 exclusive root gate、刷新 metadata 与逐月 purge 前也检查同一
cancellation token。尚未开始 purge 的取消不得改动 cache 分区并返回 130；已删除月份必须作为 repair
receipt 写进 interrupted/failed report，随后不会启动 remote fill。

CLI 第一次 Ctrl+C/SIGTERM/SIGHUP 请求 stop：停止派发批次、下一物理窗口和下一次 source 重试；
给正在执行的远端窗口最多 5 秒完成提交或保存暂存。超时调用原有立即取消路径；
第二次信号直接退出 130。5 秒是协作等待期限，不是磁盘 I/O/操作系统退出的硬上限。
Tick 保留已接收短尾 flush、不提交未完成 coverage 的合同。

`BacktestHistoryFillCancellation::request_stop()` 与 facade 同名方法共享同一 data token；
`cancel()` 保持立即取消语义。`is_stop_requested()` 表示 stop 或 cancel 已请求，
`is_cancelled()` 与 `cancelled().await` 只表示强制协作取消。直接 SDK 调用者应自行限定 stop 的等待时间。
一个 shared fill 只有全部消费者都要求 stop 才停止下一 source；其他消费者不被牵连。
已完成的请求保持 Complete，因收尾未完成的请求标记 Interrupted；完整完成赢得迟到信号竞争。

## 进度语义

minute history fill 的流式 cursor、收到行与 durability checkpoint 都不是 final coverage。CLI 只能在
terminal 成功后推进 `coverage`；durability 的 `redownload_range` 前缀可推进独立的
`checkpointed_days`，包括零成交日，但必须在 plain/TTY 和 JSONL 中明确标为
`durably_checkpointed_prefixes_not_final_coverage`。因此长时间无新增数据行时，进度仍可显示已完成的
扫描/持久化工作，而不会把无成交或未终态窗口伪报为 cache coverage。

`BacktestHistoryClient::on_fill_durability(...)` 回调接收独立 `BacktestHistoryDurabilityEvent`；
facade 通过 `BacktestRemoteFillTelemetry::durability()` 转发物理窗口状态。
旧 `BacktestHistoryTelemetryEvent` struct literal 不变。回调在 fill worker 上同步调用，
必须快速返回、不得重入 cache maintenance；缺省不安装回调。CLI TTY/plain 显示 received_rows、committed_rows、staged_rows；
JSONL 增加 `durability` 对象和逐窗口 `redownload_range`。范围采用半开纳秒区间。

- received_rows：本窗口当前候选行，可能包含从 journal 恢复的行；不是网络字节数。Tick 尚未
  上报 durability 时，TTY/plain 可显示 streaming telemetry 的精确已接受行数；此时
  committed_rows/staged_rows 必须保持 `n/a`，JSONL 的 durability 仍为 `known=false`。
- committed_rows：已写入正式缓存或 provisional checkpoint 的行。
- final_coverage：仅正式 final 提交为 true，provisional 即使持久化仍为 false。
- staged_rows：已 fsync 的私有暂存行，不能算完整 coverage。
- redownload_range：重启需重新请求的窗口后缀，包含重叠验证；成功提交后为 null。

这些字段只覆盖本轮观察到的 fill 窗口，不是整个缓存存量。旧字段保留，
没有 durability 遥测（例如旧 producer 事件路径）时显示 n/a，JSONL 为 known=false/null，不伪报 0。
不得把旧 `rows`、cursor 或 received_days 当成持久化证据。
终态窗口不会被迟到的未提交 telemetry 回退；遥测仍为 best-effort，磁盘 coverage 才是恢复依据。

## 验证

离线测试覆盖：日线 32 天提交后重启、分钟已确认前缀和空窗口、
失败 attempt 后成功空重试、chart 混入、缺少终态、close 失败、浮点位与损坏校验、
重叠变化拒绝合并、grace 不开启下一窗口、多消费者 stop 隔离、进度持久化与迟到事件。

```bash
cargo test -p tqsdk-data -p tqsdk-cache --offline
cargo clippy -p tqsdk-data -p tqsdk-cache --all-targets --offline -- -D warnings
cargo check --examples --offline
```

Mock socket 和 CLI 子进程信号测试需要本机相应权限；不需要真实账号，不运行 ignored/live 测试。
