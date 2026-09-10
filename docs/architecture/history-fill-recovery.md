# Fill 中断与续填

本合同属于 `tqsdk-data` 的历史获取/持久化边界；CLI 仅设置信号收尾期限和显示进度。
不修改 core/session 的状态树、commit/cursor、交易或报文合同。

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

- received_rows：本窗口当前候选行，可能包含从 journal 恢复的行；不是网络字节数。
- committed_rows：已写入正式缓存或 provisional checkpoint 的行。
- final_coverage：仅正式 final 提交为 true，provisional 即使持久化仍为 false。
- staged_rows：已 fsync 的私有暂存行，不能算完整 coverage。
- redownload_range：重启需重新请求的窗口后缀，包含重叠验证；成功提交后为 null。

这些字段只覆盖本轮观察到的 fill 窗口，不是整个缓存存量。旧字段保留，
没有 durability 遥测（例如 Tick 旧事件路径）时显示 n/a，JSONL 为 known=false/null，不伪报 0。
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
