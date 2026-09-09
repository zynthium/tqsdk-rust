# 验收标准与测试矩阵

Tick schema 4 当前格式：默认 root 的迁移后深验必须满足 `legacy_files=0`、
`pack_source_files=0`、`pending_month_packs=0`、`problem_files=0`。另运行
`cargo test -p tqsdk-data --test backtest_tick_cache_ops pre_schema4_tick_files_fail_closed_without_mutation`，
证明旧 Tick 文件的 read/coverage/write/compact 全部 fail closed 且源字节不变。回滚旧备份必须配套冻结迁移器。

公共容器验证：
`cargo test -p tqsdk-data --lib history_container --offline` 及相同命令追加
`--no-default-features`，覆盖 31-word golden、8,192 行 XOR/重复时间戳、长度上下界、
规范 varint、首行和 delta epoch、限额先于 I/O、追加/尾部恢复/旧 FD。
另运行 `cargo test -p tqsdk-data --lib history_series_cache::tqbn::container --offline`
及无默认特性版本，覆盖 current store 分派、未确认行不计 coverage、区间切分、
provisional 不降级 final、顺序追加跳过旧块、修订/已打开 reader、空覆盖、断尾恢复、
共享 inode、inventory 和非法 proof 不得发布。
canonicalization 回归覆盖 ID 重用/回退、跨块重放传播链、同日 last-write-wins 和 payload replay。
pre-schema-4 行为只由 fail-closed/源字节不变测试覆盖，不再保留旧 Tick 解码、迁移或 SQLite spill 测试。

统一历史容器 P1（日线/分钟接入）验证：

```bash
cargo test -p tqsdk-data --lib history_container
cargo test -p tqsdk-data --lib minute_kline_cache
cargo test -p tqsdk-data --test minute_kline_cache --test minute_kline_cache_ops
cargo test -p tqsdk-data --lib daily
cargo test -p tqsdk-data --lib common_container
cargo test -p tqsdk-data --test daily_kline_cache
cargo test -p tqsdk-data --test backtest_history_snapshot
cargo test -p tqsdk-data --test kline_append_recovery
cargo test -p tqsdk-data --test kline_cache_migration
```

须覆盖显式空 coverage、独立块跳读与 doctor 全量校验、epoch presence 极值、
浮点位模式、解码分配预算、坏提交索引与 torn slot 区分、未提交尾部恢复、旧 FD
在追加/替换后保持视图、stale writer 拒绝、symlink 写入拒绝、备份/迁移等价及幂等。
新增 manifest feature 和 clean-tail 发布门禁必须有回归。Windows cross-check 仅证明
Rust 类型正确，不替代原生运行、旧 FD/替换/断电持久性验收。

TQBN 共享 inode 与 COW 崩溃遗留验证：

```bash
cargo test -p tqsdk-data --lib hardlink_tests
cargo test -p tqsdk-data --lib snapshot_excludes_only_well_formed_tqbn_cow_orphans
cargo test -p tqsdk-cache --test snapshot_cli clone_omits_crash_cow_orphans_without_placeholder
cargo test -p tqsdk-data --lib kline_codec
cargo test -p tqsdk-data --no-default-features --lib
```

覆盖 append/coverage/provisional/恢复前分离 data inode、shared checkpoint 拒绝、purge
仅 unlink、压实检查位于锁内、失败仅清理自身临时文件、快照不复制 COW orphan 或创建占位。
共用 Kline codec 的 golden bytes 必须保留 NaN payload、负零和整数极值。
下载超时用例要求 `live` 与 `services`；无此组合时另行证明在启动 source 前明确拒绝。

Fill 暂存/收尾/durability 变更执行 [恢复合同验证](history-fill-recovery.md#验证)，
包含跨重启、retry 隔离、非终态拒绝、grace 与共享消费者隔离；禁止使用真实账号替代离线证明。

Canonical Kline 追加／恢复变更须运行：

```bash
cargo test -p tqsdk-data --test kline_append_recovery
cargo test -p tqsdk-data --test kline_cache_migration
cargo test -p tqsdk-data
cargo test -p tqsdk-cache
cargo clippy -p tqsdk-data -p tqsdk-cache --all-targets -- -D warnings
```

验收包括旧 raw 显式迁移／普通 reader 拒绝、外部备份、coverage 索引、payload 深度校验、追加前缀不变、损坏 slot
恢复、未提交 suffix、64 片段空间上界、opened-reader snapshot、硬链接隔离、诊断等待在途追加，以及
SIGINT/SIGTERM/SIGHUP 的 interrupted/130 收尾。信号与本地 mock socket 测试需要允许相应
系统调用的环境；沙箱阻止信号投递不等于生产取消逻辑失败。不得以 coverage 命中替代
snapshot 发布前的完整验证；含追加封装的 manifest 必须声明 `kline-append-v1`。

TradingTimeline 热路径性能：使用独立只读 release 基准
[TradingTimeline 性能结论](../research/2026-09-07-trading-timeline-performance.md)。
覆盖生产 75 品种、顺序/随机查询、跨休市位移、历史规模增长、零分配及独立系统调用跟踪。
加载成本必须单列；批平均分位数不得称为单次调用尾延迟，不将微基准等同于策略吞吐。

Runtime DIFF 基线使用：

```bash
cargo run -p tqsdk-core --release --example diff_ingest_microbench
```

该基准覆盖 wire JSON parse、decode、state commit、rolling tick 和 typed market read；默认含
1、100、1,000 symbol 规模。每项输出单次迭代的 p50/p95/p99/p999、events/s；typed read 额外
输出 COW snapshot 与 live partition guard 的 lock wait/hold、shared roots、clone counters。
它是单进程基线，不替代 CPU/event、allocation、RSS 或端到端 relay/backtest 的 profiler 证据。

Relay quote fan-out 基线使用：

```bash
cargo run -p tqsdk-relay --release --example quote_fanout_microbench
```

该基准对 1、10、100、500 个 quote client 逐次测量 `RelayEngine` 的 market tick fan-out，输出
p50/p95/p99/p999 与 events/s，并在 warm-up 阶段断言所有 client frame 共享同一个 `Arc<Value>`。
它刻意不冒充 WebSocket socket write、per-client queue 或网络调度基准；这些必须以独立端到端场景测量。

TQBN streaming reader 基线使用：

```bash
cargo run -p tqsdk-data --release --example history_series_cache_microbench
```

Tick 热日/冷月分区必须额外验证：

```bash
cargo test -p tqsdk-data history_series_cache::tqbn::tests::tick_month_pack_preflights_every_partition_before_mutation
cargo test -p tqsdk-data history_series_cache::tqbn::tests::closed_tick_month_pack_resumes_routes_updates_and_purges_by_day
cargo test -p tqsdk-data history_series_cache::tqbn::tests::lazy_reader_pins_root_gate_until_all_enumerated_partitions_are_opened
cargo test -p tqsdk-data history_series_cache::tqbn::tests::nonempty_read_only_legacy_root_without_operation_lock_fails_closed
cargo test -p tqsdk-data exclusive_caller_root_gate_allows_tick_rows_and_coverage_commit
cargo test -p tqsdk-data --test history_series_tqbn_compaction caller_held_exclusive_root_gate_can_compact_tick_range
cargo test -p tqsdk-cache tick_migration_reports_but_rejects_a_pre_schema4_fixture_without_mutation
cargo test -p tqsdk-cache tick_migration_backup_survives_atomic_source_replacement
cargo test -p tqsdk-cache tick_verify_
```

前三项覆盖月包原子发布故障注入、同一 Tick id 跨日保留、发布后更新路由、残留日文件只校验删除而不覆盖晚到修订、purge 后不复活、lazy reader 在候选路径打开/固定前的 root gate、缺根锁非空旧目录 fail-closed 和源 companion lock 清理；随后两项证明 caller-held exclusive token 能提交 Tick rows/coverage 并做范围 compact，且不发生自锁；快速库存回归覆盖纯月包与热日/月包混合统计；迁移回归覆盖 DryRun 计划、完整 generation 备份、逐文件 SHA-256、错代际/替换拒绝、同一 manifest 目录中断重试、pre-schema-4 拒绝且源字节不变，以及封闭月打包；verify 回归覆盖空 root、覆盖到流式回放全程同一 root generation 及精确行数。默认真实缓存迁移前后还必须各运行一次 `tqsdk-cache --kind tick migrate` DryRun：迁移前记录 `legacy_files`、`pack_source_files`、`pending_month_packs`、wall time 和峰值 RSS，迁移后要求三类待办计数均为零。DryRun/doctor 可全量深验；普通 fill/CacheOnly 更新不得借此引入全缓存扫描。

2026-09-09 默认缓存迁移前基线：292,967 个 legacy 文件、292,358 个封闭月源文件、15,820 个目标月包、0 problem，源数据 46,504,459,771 bytes；release DryRun wall/user/system 分别为 548.467/476.510/27.752 秒。容量估算要求 146,325,000,003 bytes，实测可用 243,003,564,032 bytes。该次运行未取得可靠 peak RSS；Apply 与迁移后复验必须补录。

其中 `stream_tick_reader_1pct` 专测 partial-range streaming 读取；输出的
`blocks_decoded` 必须只按触及 records block 计数，且 `materialized_rows=0`。

除 write/read、coverage、compaction 外，该示例还逐行测量 `TickDataSeriesReader`，输出
p50/p95/p99/p999、rows/s 与 `bytes_read`、`bytes_decompressed`、`blocks_skipped`、
`blocks_decoded`、`materialized_rows`、`io_read_ns` 与 `decode_ns`。后两项仅覆盖 Records
payload，分别定位 storage 与 codec 瓶颈。总耗时包含 reader open/planning；行延迟只计 `next_tick()`，
因此 planner double decode 或 materialization fallback 会在 telemetry 中显式可见。

非单调 legacy Tick page 必须额外覆盖 bounded spill：结果与同一 canonicalization 规则逐行一致、
`materialized_rows=0`、reader drop 后临时目录消失。该路径的临时 SQLite page cache 上限 4 MiB；
64 KiB 单行与 8 GiB 输入边界必须 fail closed，不能退回 full-partition heap materialization。

CacheOnly backtest merge/callback 基线使用：

```bash
cargo run -p tqsdk-task --release --example history_backtest_replay_microbench
```

该示例先写入临时 TQBN cache，再经 `HistoryBacktestReplayStream` 做 cache-only reader、N-way `BinaryHeap` merge 和无分配 strategy callback；分别输出 single-event 与 bounded batch 的每 event 分位延迟、events/s 和 `strategy_callback_ns_per_event`。默认覆盖 10/100/500/1,000 symbol、每 symbol 256 tick，结束后删除它自己创建的临时目录。它是 synthetic、单机 cache 基线，不代表真实日/月/半年数据、策略业务逻辑或 broker 行为。

Task commit/risk/submit 基线使用：

```bash
cargo run -p tqsdk-task --release --example commit_risk_submit_microbench
```

每个样本先 commit 一条 quote，再由 `RiskEngine::max_price_deviation` 读取当前市场状态并提交
一个唯一 client-order id；输出 p50/p95/p99/p999、events/s 与 dispatch 数。它只使用
`ManualSession` 并逐轮 drain dispatch，因此不访问真实账户、网络或交易所，也不代表 broker latency。
`TQSDK_TASK_BENCH_SYMBOL_COUNTS=10,100,500,1000` 会在固定 symbol catalog 间轮转；每轮仍只提交一笔订单，因此可比较 active-universe 增长而不混入批量下单。

四条路径的统一本地 runner：

```bash
python3 scripts/run_performance_matrix.py --output-dir /tmp/tqsdk-performance-matrix
```

真实数据必须另附显式 corpus，而不是把 synthetic 小样本误称为日/月基线：

```json
{
  "cases": [
    {"name":"one-day","cache_dir":"/data/tq-cache","symbol":"SHFE.rb2601","start_ns":0,"end_ns":1},
    {"name":"one-month","cache_dir":"/data/tq-cache","symbol":"SHFE.rb2601","start_ns":0,"end_ns":1},
    {"name":"six-months","cache_dir":"/data/tq-cache","symbol":"SHFE.rb2601","start_ns":0,"end_ns":1}
  ]
}
```

将真实纳秒边界替换 `0/1` 后运行：

```bash
python3 scripts/run_performance_matrix.py \
  --output-dir /tmp/tqsdk-performance-matrix-real \
  --history-corpus /path/to/history-corpus.json
```

runner 将每个 case 作为单独的 `tqbn-streaming-reader-corpus-*` 样本写入 manifest，保留 cache path、
symbol、范围、原始输出与资源统计；`--quick` 禁止搭配 corpus。真实 corpus case 必须设置
`TQSDK_HISTORY_CACHE_BENCH_STREAM_ONLY=1`：只读打开既有 cache、只跑 streaming reader，
不得把 write/read materialized baseline 或 benchmark 自身无界 latency 样本计入 reader RSS。

runner 在采样窗口外预构建 release example，随后直接执行二进制；仅在 benchmark 明示 `events=` 时才在 manifest 的 `derived_metrics` 推导 CPU ns/event 与 filesystem blocks/event。

`--quick` 仅做 wiring smoke，不可用于性能结论。正常运行生成 11 个基础命令：runtime batch symbol、task symbol catalog、TQBN cache scan symbol、cache-only backtest merge/callback、relay downstream client 各覆盖 10/100/500/1000；task、backtest 与 relay 在单个 benchmark 进程中输出四组样本。`--history-corpus` 会额外加入每个真实 cache range 的 streaming-only reader 命令。TQBN streaming reader 的 synthetic 基础样本仍只是 rows/codec 指标，backtest merge 使用 synthetic bounded cache；两者都不应误读为真实日/月/半年数据集。每条路径保留原始 stdout，并写入
`tqsdk.performance-matrix.v3` manifest（git/toolchain/host metadata、命令、环境、wall time，以及
POSIX 可用时的 user/system CPU、单个子进程 peak RSS、文件系统 input/output blocks）。manifest 的
`reported_metrics` 只结构化摘录 benchmark 自己输出的 latency table 或 `key=value` 指标，不推导缺失
指标。这些是命令级统计，不等同于 CPU/event、alloc/event、聚合进程树 RSS、锁等待或磁盘 MB/s；后者仍须
由具体 benchmark 或 profiler 输出。
输出目录必须是新的，避免将不同机器或 revision 的结果混写；该 runner 不是 CI SLO gate。

TradingTimeline 锁粒度回归：`cargo test -p tqsdk-data trading_timeline --lib`、
`cargo test -p tqsdk-cache timeline --bin tqsdk-cache`。覆盖跨进程根共享共存、
所有目标月分区在 metadata/coverage 前固定、部分获取失败释放、缺 sidecar 不创建、
不同月份并发、256 分区预算，以及同产品并发增量合并无丢更新。
根独占维护仍互斥；CLI fill 后维护不再因无关 root-shared fill 返回 busy。

TradingTimeline 缓存确认专项：见
[2026-09-07 受限 IM/CF 验证记录](../research/2026-09-07-trading-timeline-cache-confirmed.md)。
确认目录须逐项匹配 evidence hash，完整/增量重建保持 identity，加载 active 后验证
跨午休、周末、长假正反向运算；无 coverage 仍应报错。该记录不是全市场或性能验收。

## 文档定位
本文档定义的是 runtime contract 的验收标准，以及未来 facade/adapters 的派生验收基线。

重点服务于：

- V1 protocol-complete runtime contract
- V2+ wait / fan-out / callback adapters

相关文档：

- [总架构入口](README.md)
- [runtime-core 总览](runtime-core/overview.md)
- [协议交互](runtime-core/protocol-flow.md)
- [未来 wait adapter](api-wait.md)

## 本页覆盖范围
本页主要负责：

- 约束 V1 的 command-to-commit 完整性
- 约束统一状态树、统一 revision、统一 cursor/log 语义
- 为未来 `wait_update`、fan-out、callback adapter 提供同一套底层验收基线

## 验收原则
V1 的验收不应看 facade 好不好用，而应看 contract 是否完整。

必须同时满足：

1. 统一性成立
   - 所有远端交互都进入同一 runtime contract
2. 可见性成立
   - 所有上层可见结果都进入同一 runtime state tree
   - 这棵状态树必须能通过 `RuntimeReader` 以 revision-bound 方式稳定读取
3. 可解释性成立
   - 所有可见变化都能被 `Revision` / `ChangeSet` / causality 解释
4. 隔离性成立
   - adapter 不绕过 commit 模型直接通知上层
5. schema 完整性成立
   - 官方对象 schema 必须以纯 typed contract 形式进入 core，但不得夹带 facade/view 行为

## V1 核心验收条目
### 统一命令链路
- 所有远端交互都必须经过：
  `RuntimeCommand -> RuntimeInput / NormalizedMutation -> CommitResult`
- `submit()` 只返回 `CommandId`，不返回完成态
- command-scoped 结果不得通过旁路 future 暴露
- route/outbox 测试应以 public `OutboundDispatch` 为合同；raw outbox envelope 是 runtime 内部队列细节

### 统一状态树
- market / trade / replay / query / schema / system 状态都必须进入同一 runtime state tree
- retention 内任意已提交 revision 都必须能提供内部一致的 immutable-root snapshot；被裁剪 revision 必须显式报告 lag，不能返回当前 head 冒充历史
- query/schema 结果不得躲在独立 side cache 中绕开 snapshot
- schema 状态键必须由 `schema_id` 决定，不能退化成 transport route label
- core 不得内部创建 Tokio runtime 作为 sync fallback；需要网络 IO 的调用方必须自带 async runtime
- 本地控制面对象允许 retention-bounded 裁剪，但裁剪语义必须显式、可验证且不破坏幂等重放

### 统一 revision / change 模型
- 只有形成可见 commit 时才推进 `Revision`
- `ChangeSet` 必须支持 path/object/field 三级命中
- 不同协议域的变化不能各自维护独立 revision

### 统一 causality
- 每个命令都必须可追踪到 `CommandId`
- `CommitResult` 必须能表达由哪些命令导致
- trade/replay/query/system 错误都必须进入同一 causality 模型
- terminal 命令在状态树落地后可以释放 active ledger 元数据，但重复 terminal 写入仍必须保持幂等

### 统一 cursor / log 语义
- 所有消费者都必须通过 `RuntimeReader::cursor()` / `RuntimeReader::next()` 或兼容的 `CommitLog` / `UpdateCursor` 读取提交结果
- `RuntimeReader::next()` / `CommitLog::next()` 返回共享提交句柄 `SharedCommitResult = Arc<CommitResult>`；同一轮写侧返回值和 log 中提交必须指向同一个不可变 payload，不得通过深拷贝制造第二份 commit 元数据
- 需要 exact revision 读面的消费者必须能通过 `RuntimeReader::next_view()` 获得一致视图，或明确得到 lagged 信号
- runtime core 不得为不同 future facade 维护不同的提交通道
- 多个 cursor 必须能独立推进，不互相污染
- `CommitLog` 不得因为 revision 扫描而在长会话中退化为线性读取
- `CommitLog` 必须有 hard entry 与 accounted-byte retention；活动 cursor 不得无限 pin 住保留区
- lagged cursor 必须保持位置并提供显式恢复：resync-to-head、持久重放或失败退出；兼容 `next()` 可选择 documented resync-to-head，不能永久卡死
- `SessionClientBuilder` 的显式 commit retention / byte limit 必须传播到所建 runtime；未配置时保持 core 默认值

### adapter 边界
- adapter 可以编解码和保留短期协议态
- 恰好一个 adapter 接受输入时，registry 可以把 `RuntimeInput` 所有权交给该 adapter 的消费式解码路径；这不得改变 mutation、commit 或 revision 语义
- 多个 adapter 接受同一输入时，registry 必须保留借用式 fan-out；未覆写消费式解码的自定义 adapter 必须继续通过默认借用式实现兼容
- adapter 不得直接推进 revision
- adapter 不得直接发通知给上层
- adapter 不得直接改 cursor

## V1 测试矩阵
| 场景 | 输入条件 | 预期行为 | 对应核心语义 |
| :--- | :--- | :--- | :--- |
| bootstrap schema commit | 初始 schema / metadata 拉取完成 | 产生 `InitialReady` commit，状态写入 snapshot | schema 进入统一状态树 |
| market diff commit | 一个有效 market diff | 形成新 revision，market 状态可见 | DIFF 对象进入统一提交 |
| trade command reject | 下单命令被远端拒绝 | 不走旁路 future；错误进入 snapshot 与 commit | trade 因果统一 |
| replay step commit | 一次 replay step 产生多对象变化 | 形成单轮或可解释多轮 commit，归属对应 `CommandId` | replay 因果统一 |
| query response commit | GraphQL / HTTP 查询返回结果 | 结果写入 `query/*`，形成可见 commit | query 结果进入 snapshot |
| session error commit | auth 失效或 transport 异常 | session 错误进入 `system/*` 并形成 commit | system 错误统一可见 |
| websocket 初始建连黑洞 | TCP 已接入但 TLS/WebSocket 握手不返回 | 单次尝试按时取消，最多重试 3 次后返回脱敏 transport error | 初始瞬时故障有界恢复，不改变 session reconnect 状态机 |
| cursor isolation | 两个 cursor 从不同 revision 开始消费 | 各自独立推进 | cursor 独立性 |
| single-adapter input ownership | 一个输入只被一个 adapter 接受 | 可消费输入且产出与借用式解码相同的 mutation | 输入所有权不改变 commit 语义 |
| multi-adapter observation | 一个输入被多个 adapter 观察 | 保持借用式 fan-out，只通过 mutation/commit 对外可见 | adapter 无提交权 |

## 当前实现验证入口
当前仓库已经有直接对应 V1 contract 的验证入口，可作为“功能全景”验收基线：

| 能力面 | 主要验证文件 | 说明 |
| :--- | :--- | :--- |
| DIFF 协议对象 | `crates/tqsdk-core/tests/runtime_contract_v1_capability.rs`、`crates/tqsdk-core/tests/runtime_contract_batch_commit.rs`、`crates/tqsdk-core/tests/runtime_contract_adapters.rs`、`crates/tqsdk-core/src/adapter.rs` 单元测试 | 覆盖 market diff、trade diff、query/schema/replay 输入归一化与提交，以及单 adapter 消费式解码和多 adapter 借用式广播 |
| trade 命令与状态 | `crates/tqsdk-core/tests/runtime_contract_v1_capability.rs`、`crates/tqsdk-core/tests/runtime_contract_command_ledger.rs` | 覆盖 `req_login`、`insert_order`、`pre_insert_order` 及命令状态写回 |
| replay/feed 推进 | `crates/tqsdk-core/tests/runtime_contract_v1_capability.rs`、`crates/tqsdk-core/tests/runtime_contract_pending_route_executor.rs` | 覆盖 replay pending route 执行与 replay state 提交 |
| auth/session/system 控制 | `crates/tqsdk-core/tests/runtime_contract_v1_capability.rs`、`crates/tqsdk-core/tests/runtime_contract_auth_context.rs`、`crates/tqsdk-core/tests/runtime_contract_session_state.rs`、`crates/tqsdk-core/tests/runtime_contract_session_runtime.rs`、`crates/tqsdk-core/tests/runtime_contract_ws_transport.rs`、`crates/tqsdk-core/src/transport/websocket.rs` 单元测试 | 覆盖 auth context、topology/bootstrap、refresh-auth、session state，以及初始 WebSocket 黑洞的有界超时、重试和脱敏错误 |
| GraphQL / HTTP query | `crates/tqsdk-core/tests/runtime_contract_v1_capability.rs`、`crates/tqsdk-core/tests/runtime_contract_pending_route_executor.rs`、`crates/tqsdk-core/tests/runtime_contract_adapters.rs` | 覆盖 GraphQL query 的 HTTP request 合同、pending route 执行与 query snapshot |
| schema / metadata / bootstrap 交互 | `crates/tqsdk-core/tests/runtime_contract_v1_capability.rs`、`crates/tqsdk-core/tests/runtime_contract_pending_route_executor.rs`、`crates/tqsdk-core/tests/runtime_contract_session.rs`、`crates/tqsdk-core/tests/runtime_contract_bootstrap.rs` | 覆盖 schema HTTP 请求、bootstrap topology 与 metadata/state 写入 |
| reader-first 读契约 | `crates/tqsdk-core/tests/runtime_contract_reader_surface.rs`、`crates/tqsdk-core/tests/runtime_contract_surface.rs`、`crates/tqsdk-core/tests/runtime_contract_runtime_core.rs`、`crates/tqsdk-core/tests/runtime_contract_domain_state.rs` | 覆盖 `RuntimeReader`、`SnapshotReadGuard`、`CommitReadGuard`、`MarketTradeStateReadGuard`、`CursorLagged`、共享 commit identity、COW snapshot clone=0 / lock telemetry 与兼容 surface |
| 官方对象 typed schema | `crates/tqsdk-core/tests/runtime_contract_types.rs`、`crates/tqsdk-core/tests/runtime_contract_reader_surface.rs` | 覆盖 `objs.py` 对象族和 core 补充 diff 对象的 typed schema surface、期货 `Order`/`Trade` 协议枚举字段解码，以及 reader 侧 `decode<T>()` 接入 |
| 纯交易时段 helper | `crates/tqsdk-core/tests/trading_session.rs` | 覆盖 `TradingSessionSchedule` 的 open / pre-close / closed、跨午夜 rollover、空 schedule 和非法空窗口 |
| 默认 facade crate | `crates/tqsdk/tests/facade_contract.rs`、`crates/tqsdk/examples/api_contract_s33_default_facade.rs`、`crates/tqsdk/examples/api_contract_s37_facade_server_backtest.rs`、`crates/tqsdk/examples/api_contract_s38_facade_local_backtest.rs`、`crates/tqsdk/examples/api_contract_s39_facade_same_body.rs`、`crates/tqsdk/examples/api_contract_s40_facade_local_backtest_target_pos.rs`、`crates/tqsdk/examples/api_contract_s41_facade_server_replay.rs`、`crates/tqsdk/examples/api_contract_s43_facade_backtest_history_cache.rs`、`crates/tqsdk/examples/api_contract_s44_facade_backtest_remote_on_miss.rs`、`crates/tqsdk/examples/api_contract_s45_facade_backtest_cache_warmup.rs`、`crates/tqsdk/examples/api_contract_s46_facade_record_ticks.rs`、`crates/tqsdk/examples/api_contract_s47_facade_market_cache_policy.rs` | 覆盖 `tqsdk::prelude::*`、`Tq` / `TqBuilder`、统一 `.backtest(...)` 默认共享 history cache mode、显式 `.disabled_cache()` server mode、cache-backed local backtest builder、`BacktestTickCache` facade export、server replay session endpoint 接入、自动 heartbeat 和显式 replay 控制、resolved TQKQ target-position helper、`TargetPos` intent API 与增量 execution report、curated `advanced::*` 下钻命名空间，以及默认 facade 的 persistent-cache backtest / warmup / remote-on-miss cache fill / live record_ticks cache writer / shared `MarketCachePolicy` / universe selector / server replay / custom replay backtest / live-backtest same-body 策略入口和 local backtest TargetPos 执行闭环 |
| 远端 tick 缓存补齐 | `crates/tqsdk-data/src/backtest_history/fill.rs` 单元测试、`crates/tqsdk-session/tests/server_backtest_history.rs`、`crates/tqsdk/src/backtest_remote.rs` 单元测试、`crates/tqsdk/tests/facade_contract.rs` | 覆盖 clean source lane 顺序复用、取消/错误 lane discard、显式 chart cleanup、每日 coverage checkpoint、不设默认 batch 墙钟超时、显式超时解析、流式 tick id 区间合并与公开 accumulator 语义一致性、future range 拒绝、idle/exhausted terminal coverage 保护，以及 facade cache warmup / remote-on-miss 契约 |
| 可选 market relay | `cargo test -p tqsdk-relay --tests` | 覆盖 relay 配置、dry-run 启动自检、结构化启动诊断、分层 HTTP `/health`、`/metrics`、`/symbol-metrics`、原子 `/dashboard-snapshot`、dashboard 5 分钟 `timeline_history` 服务端内存缓存、内置 `/dashboard`、上游连接/订阅/补历史阶段 telemetry 和 backfilling 可观测进度、等待首样本或补历史无样本合约 `initializing` 非问题状态、frame/event idle 秒级告警、raw frame 后先发 `peek_message` 再 JSON decode 的顺序 guard、上游 idle 期间周期性 `peek_message` 恢复守卫、peek/decode timing metrics、200 合约 decode guard、可恢复 decode health、每日合约集合刷新调度、typed metadata 期货产品发现与分批查询、每品种主力-only 快捷选择、每品种活跃度前 N 合约选择、上游一合约一 tick chart 订阅、tick row 连续性缺口/重复/乱序 telemetry、当前 universe ∪ 当前订阅健康集合、dashboard read-model 低频缓存、dashboard read-model 锁外分类、进程内固定容量事件账本、单 chart `ins_list` 长度防线、tick view width 配置、下游 market 协议、interest/chart-id 隔离、K 线 `[start,end)` 合成、tick-ring 冷启动回放、bootstrap 队列限流、observability、WebSocket loopback、upstream tick scaffold 和 quote-only 远月行情更新 |
| relay dynamic desired-set | `cargo test -p tqsdk-relay --test server_ws relay_configured_upstream_reconciles` | 覆盖基础 universe 外的 chart 动态补订，以及 chart 删除或 downstream 断开时将 quote 集收敛回基础 universe、用 `set_chart(ins_list: "")` 删除上游 tick chart；防止失活 interest 永久占用上游资源。 |
| relay endpoint opt-in | `cargo test -p tqsdk-session --test session_builder builder_accepts_explicit_market_relay_url_without_enabling_other_routes` | 确认 relay 只显式改 market endpoint，不启用 trade/query/auth |

Relay downstream queue verification must cover a full reliable-frame queue
(client disconnect), quote latest-wins coalescing by symbol, and the explicit
per-client encoded-byte cap. These checks belong in
`cargo test -p tqsdk-relay --tests` and do not require live credentials.
The same gate must cover cache LRU eviction under `max_symbols` / retained-byte
reservation, and reject a configuration that cannot fit one tick ring.

### Provider-unavailable membership maintenance

`cargo test -p tqsdk-data --test historical_universe_artifact` 必须覆盖 retry receipt 的 canonical
round-trip、过期/未过期退避、timeout→timeout、timeout→terminal success、stable-roster/cutoff
fail-closed 与 root operation lock。CLI 至少运行 `refresh-provider-membership --help` 和已存在
acquisition 的 `--dry-run`，后者不得认证、请求 provider 或写 cache/artifact。真实远端验证应限制
`--max-symbols`，并确认 canary/认证/传输/取消失败不会发布 receipt/proof；正常成功只允许发布 receipt
或新的 acquisition/catalog，绝不发布 plan。

### Trading timeline

2024 全年 IM 与 02-06 起 CF 的冻结缓存审计及逐日资料缺口，见
[2024 闭环审计](../research/2026-09-06-trading-timeline-2024-closeout.md)。
该审计不是生产放行：缺失官方例外完整性材料时仍禁止激活。

reviewed-2024 当前 `exception_review_complete: false`：只支持审计匹配，禁止激活。
新增门禁回归覆盖：无例外完整性声明拒绝激活、重复增量与全量 hash 一致、
紧凑 V1 单文件的 round-trip/hash 校验、旧 layout 的独占转换，以及单文件 rename
后 fsync 失败报告 `indeterminate`（重新 load 后决定可见 generation）。

2026-09-06 修复验证：周一双日期锚点、按月扫描和 opt-in fill 收尾维护已落地。
具体回归与冻结缓存抽样结果见 [TradingTimeline](trading-timeline.md#verification)。
规则资料仍非全量：reviewed-2024 仅覆盖有官方模板证据的 CF 与四种股指；
缺失节假日、其他品种或历史时期仍 fail-closed，不能声明全市场全年度验收通过。

```bash
cargo test -p tqsdk-data trading_timeline --lib
cargo test -p tqsdk-cache --bin tqsdk-cache timeline
cargo run -p tqsdk-data --example api_contract_s40_trading_timeline
```

维护验收要求每月校验扫描次数与月份数相关；普通 fill 和 historical-universe fill 的
收尾路径均须验证。填充锁冲突、coverage 缺口、未知规则不得替换 active；数据阶段成功
但 timeline 维护失败时，保留数据并明确返回非零命令状态。不要把前端 npm 缺失警告
忽略成 Dashboard 构建通过，也不要把暖缓存逻辑 read 次数写成物理磁盘 IOPS。

`cargo test -p tqsdk-data trading_timeline --lib` 覆盖交易时长/跨休盘移动、未知覆盖 fail-closed、
稀疏指数的正证据规则推断、final minute cache 的 timeline 重建、draft catalog 的 confirmed
gate/例外日 fail-closed，以及唯一规则日与歧义日的编译边界。relay 仅保留 range query：
`cargo test -p tqsdk-relay --test history_http` 必须确认 `/v1/history/context` 为 404，且 schema
不再声明 `context_query`。

### Relay CacheOnly history

新增 history sibling 时，以下四组契约必须一起成立：

| 验证面 | 主要验证文件 |
| --- | --- |
| data schema、typed failure、strict inspect、prepared live plan、shared root gate lifecycle、manifest、lease/pinning | `crates/tqsdk-data/tests/backtest_history_snapshot.rs`、`crates/tqsdk-data/src/backtest_history/snapshot.rs` 单元测试 |
| publisher role-aware clone/import 与 source/history 隔离、prewarm auth-on-miss、strict inspect + real query smoke、publish crash/recover、rollback/scrub、invalid-CURRENT fail-closed 与 tombstone/lease-aware GC | `crates/tqsdk-cache/tests/snapshot_cli.rs` |
| HTTP grammar、JSON、error、ETag、gzip、limits/cancel，以及 live cache commit 无发布/重启即时可见、maintenance 互斥恢复 | `crates/tqsdk-relay/tests/history_http.rs` |
| dedicated runtime、market-lock isolation、reload、generation health、默认 wildcard CORS 与无 identity `OPTIONS` preflight | `crates/tqsdk-relay/tests/history_runtime.rs` |
| readiness/reload/query/buffer/compression metrics（含 CPU shed）、单条结构化 audit | `crates/tqsdk-relay/src/history/observability.rs`、`crates/tqsdk-relay/src/metrics_http_impl.rs` 单元测试 |
| market send-to-downstream p99、顺序/无丢失、gzip/fallback 容量特征 | `crates/tqsdk-relay/tests/history_isolation_gate.rs` ignored same-spec candidate gate；非阻塞，不作为低并发功能验收 |

还必须验证 `cargo check -p tqsdk-relay --no-default-features` 和
`cargo check -p tqsdk-relay --no-default-features --features history`，证明 history 只传播本地
reader feature。低并发功能验收不承诺 history 吞吐量或 market p99 SLO；部署侧必须由受控 gateway
设置符合实际业务的低并发配额。8 并发 history、512 MiB budget、4 个 shared CPU-work permit、2 gzip workers 的测试继续观察
market 无丢失/乱序/异常断连，以及 p99 增量是否不超过 `max(1 ms, 10%)`，但作为非阻塞容量特征项。
2026-08-29 当前生产宿主机未达到该 p99 目标；这不是已通过的性能门，但不阻塞低并发功能验收。
完整合同见
[history-relay.md](history-relay.md)、[history-relay-http.md](history-relay-http.md) 与
[history-snapshot-manifest.md](history-snapshot-manifest.md)。

本地 candidate gate 必须使用 release profile，并显式提供 market、history、driver 三组可用且
两两互斥的 CPU set；driver 至少包含三个逻辑 CPU，分别承载 downstream 计时、upstream fixture
和 history load clients。测试输出是候选机证据，不得用来声明普遍的生产性能保证：

gate 固定执行 loaded/baseline/baseline/loaded 四阶段；每阶段先完成 market warmup，再收集
2,048 个有效 forward-latency 样本。最终分别合并两组 raw baseline 与 raw loaded 样本后各算
一次 p99；不得使用 majority、best-of 或各阶段 p99 的中位数掩盖违规。

```bash
TQSDK_RELAY_ISOLATION_GATE_MARKET_CPU_SET=<market-cpus> \
TQSDK_RELAY_ISOLATION_GATE_HISTORY_CPU_SET=<history-cpus> \
TQSDK_RELAY_ISOLATION_GATE_DRIVER_CPU_SET=<at-least-three-driver-cpus> \
cargo test --release -p tqsdk-relay --test history_isolation_gate -- --ignored --nocapture
```

修改 relay dashboard、dashboard UI 或 symbol telemetry 时，补充运行：

```bash
cargo test -p tqsdk-relay --test symbol_metrics
cargo test -p tqsdk-relay --test binary_smoke relay_binary_serves_embedded_dashboard_assets
cd crates/tqsdk-relay/dashboard-ui
pnpm install --frozen-lockfile
pnpm run check
pnpm run test
pnpm run build
pnpm run size-check
pnpm run test:e2e
```

dashboard UI 依赖必须固定到明确版本，不能使用 `"latest"`。Svelte source 改动后必须重新
生成 `crates/tqsdk-relay/dashboard-ui/dist/**`；Rust 侧通过 build script 构建并嵌入该目录，
dashboard job 用 `pnpm run build` 和 `pnpm run size-check` 防止源码、内嵌静态产物和
JS/CSS 预算回退。dashboard 页面不得展示会被误认为
真实 telemetry 的静态 trend/sparkline；全屏控制必须使用浏览器 fullscreen API 并在不支持
时禁用；完整合约表应展示当前过滤页内全部行，不再额外截断到关注列表或时间带行数。连续性
热力图默认使用 `/dashboard-snapshot.timeline` 的后端聚合样本；页面首轮可通过
`/dashboard-snapshot?timeline_history=1` 恢复服务端缓存的 5 分钟压缩历史，后续普通轮询
不得重复携带完整历史；交易所展开行只能维护当前 page rows 的 bounded symbol history，
不应恢复轮询全量 `global_symbols` 后再在前端重算全市场时间带。连续性面板里的近期平均 tick 接收延迟必须来自服务端逐合约计算的
`avg_receive_gap_ms`，UI 只负责逐品种同规格展示，不应从前端 bucket 再推导平均延迟；
`session=closed` 的合约不参与活跃延迟聚合，数值位置显示 `--`。普通
`/dashboard-snapshot` 轮询必须保持 compact page row wire format，省略 dashboard 不使用的
raw timestamp / DIFF row-id 明细；完整合约级审计字段保留在 `/symbol-metrics`。

推荐的 V1 回归入口：

1. `cargo test -p tqsdk-core -q --test runtime_contract_v1_capability`
2. `cargo test -p tqsdk-core -q --test runtime_contract_reader_surface --test runtime_contract_surface`
3. `cargo test -p tqsdk-core -q`

history cache / cache-backed backtest current focused validation:

历史动态 universe 的最小验证还应包含：

```bash
cargo test -p tqsdk-data --test historical_universe
cargo test -p tqsdk-data --test historical_universe_artifact
cargo test -p tqsdk-data --test historical_universe_resolution
cargo test -p tqsdk-data --test universe_spec
cargo test -p tqsdk-data --test universe_except
cargo test -p tqsdk-data --test provider_timeline_bootstrap_scope
cargo test -p tqsdk-data --test universe_spec_compiler
cargo test -p tqsdk-data --test universe_compatibility
cargo test -p tqsdk-data --test universe_input
cargo test -p tqsdk-data --test historical_universe_v4
cargo test -p tqsdk-data --test historical_universe_v4_resolution
cargo test -p tqsdk-data --test provider_history_observed
cargo test -p tqsdk-data --test provider_history_empty_expiry
cargo test -p tqsdk-task --test strategy_backtest
cargo test -p tqsdk --lib
cargo test -p tqsdk --test facade_contract
cargo test -p tqsdk --test facade_universe_except
cargo test -p tqsdk-cache --bin tqsdk-cache
cargo test -p tqsdk-relay --tests
```

它们分别覆盖 legacy parser/顺序结果冻结、Universe Language V2 grammar/canonical AST/hash、`except(...)` 与旧 `!` 表示的 identity 等价、typed
exclusion/capability/file identity、V1–V3 hash compatibility 与 V3 execution pin、V4 fixed wire/hash/
chain、V4 → V3 projection 等价与 rollback、V5 fixed wire/hash/canonical-byte gate、V4 → V5
source-preserving migration、complete authoritative 或 provider-history
acquisition/semantic fact chain/content-addressed durability、daily-origin membership、
provider-unavailable 的精确 timeout 判定、零完成/比例熔断与旧 complete-observation JSON 兼容、
用户起点优先的 physical/derived tick/minute/daily request floor、
membership/dependency/kind target resolution、同时间 replay revision、
facade V5 区间/chain/首可用边界、relay timeline 触网前拒绝与 file-refresh last-known-good，以及 CLI
的 V5 default writer、V4 compatibility token、dry-run/apply migration receipt、统一 CacheOnly/RemoteOnMiss 参数与
terminal path。

```bash
rtk cargo test -p tqsdk-data --test history_series_single_file_store
rtk cargo test -p tqsdk-data --test history_series_cache
rtk cargo test -p tqsdk-data --test history_series_tqbn_compaction
rtk cargo test -p tqsdk-data --test history_series_tqbn_corruption
rtk cargo test -p tqsdk-data --lib tqbn
rtk cargo test -p tqsdk-data --features tqbn-zstd --lib tqbn
rtk cargo check -p tqsdk-data --features tqbn-zstd --example history_series_cache_microbench
TQSDK_HISTORY_CACHE_BENCH_INPUT_CACHE_DIR=<cache-root> TQSDK_HISTORY_CACHE_BENCH_INPUT_SYMBOL=<symbol> TQSDK_HISTORY_CACHE_BENCH_INPUT_START_NS=<start-ns> TQSDK_HISTORY_CACHE_BENCH_INPUT_END_NS=<end-ns> TQSDK_HISTORY_CACHE_BENCH_STREAM_ONLY=1 rtk cargo run -p tqsdk-data --release --example history_series_cache_microbench
rtk cargo test -p tqsdk-data
rtk cargo test -p tqsdk-data --test backtest_tick_cache_ops
rtk cargo test -p tqsdk-data --test backtest_tick_cache_ops repair_tick_locks
rtk cargo test -p tqsdk-data --test minute_kline_cache
rtk cargo test -p tqsdk-data --test minute_kline_cache_ops
rtk cargo test -p tqsdk-data --test backtest_history_api
rtk cargo test -p tqsdk-data --test backtest_history_metadata
rtk cargo test -p tqsdk-data --test backtest_history_query
rtk cargo test -p tqsdk-cache
rtk cargo test -p tqsdk-cache history_streaming_cursor_advances_received_without_committing_coverage
rtk cargo test -p tqsdk-cache --test cli query_
rtk cargo test -p tqsdk-cache --test cli repair_locks
rtk cargo clippy -p tqsdk-cache --all-targets -- -D warnings
rtk cargo clippy -p tqsdk-data --all-targets -- -D warnings
rtk cargo test -p tqsdk-data --test universe_selector
rtk cargo test -p tqsdk-data --test historical_fill_universe
rtk cargo test -p tqsdk-data --test historical_universe
rtk cargo test -p tqsdk-data --test historical_universe_artifact
rtk cargo test -p tqsdk-data --test universe_spec
rtk cargo test -p tqsdk-data --test universe_spec_compiler
rtk cargo test -p tqsdk-data --test universe_compatibility
rtk cargo test -p tqsdk-data --test universe_input
rtk cargo test -p tqsdk-data --test historical_universe_v4
rtk cargo test -p tqsdk-data --test historical_universe_v4_resolution
rtk cargo test -p tqsdk-task --test history_tick_replay
rtk cargo test -p tqsdk-task --test history_backtest_replay
rtk cargo test -p tqsdk-task --test minute_kline_aggregate
rtk cargo test -p tqsdk-task kline_synth
rtk cargo test -p tqsdk backtest_kline
rtk cargo test -p tqsdk --lib
rtk cargo test -p tqsdk --test facade_contract
rtk cargo test -p tqsdk --test facade_contract facade_backtest_warmup
rtk cargo check -p tqsdk --example api_contract_s43_facade_backtest_history_cache
rtk cargo check -p tqsdk --example api_contract_s44_facade_backtest_remote_on_miss
rtk cargo check -p tqsdk --example api_contract_s45_facade_backtest_cache_warmup
rtk cargo check -p tqsdk --example api_contract_s46_facade_record_ticks
rtk cargo check -p tqsdk --example api_contract_s47_facade_market_cache_policy
rtk cargo check -p tqsdk-data --example api_contract_s48_backtest_history_query
rtk cargo check -p tqsdk-data --no-default-features --example api_contract_s48_backtest_history_query
rtk cargo check -p tqsdk-data --example api_contract_s49_tick_lock_repair
rtk cargo check -p tqsdk-data --no-default-features --example api_contract_s49_tick_lock_repair
rtk python3 scripts/smoke_market_cache_e2e.py --symbols KQ.i@SHFE.au --timeout-secs 300
rtk python3 scripts/bench_backtest_tick_cache.py --profile release --tqbn-zstd --cargo-offline --verify-existing-cache --cache-root <existing-zstd-cache-root> --batch-sizes 32 --slice-secs none
```

backtest-history query 的 feature matrix（不需要真实凭证）必须保持：

```bash
rtk cargo check -p tqsdk-data --no-default-features
rtk cargo check -p tqsdk-data --no-default-features --examples
rtk cargo test -p tqsdk-session --no-default-features
rtk cargo check -p tqsdk --no-default-features --examples
rtk cargo check -p tqsdk-data --all-features --examples
```

`BacktestHistoryClient` 的 reader 使用 async orchestration 加有界 blocking TQBN decode worker；
任何未来 native-async storage 替换必须以可复现的 CacheOnly cold/warm、1/32 concurrency benchmark
证明吞吐和 p95 都提高至少 20%、单查询不退化超过 5%、且逐行结果一致，不能留下运行时 backend
切换或把 `tokio::fs` 当作未经测量的性能优化。

`history_series_single_file_store`、`history_series_cache`、`history_series_tqbn_compaction`
和 `history_series_tqbn_corruption` 覆盖当前默认 TQBN history cache 行为、embedded coverage、
coverage index chain 的多段读取、tail checkpoint 的 confirmed length/checksum/index head、未确认截断或坏
checksum suffix 的 read 隔离与 writer 恢复、无 checkpoint 旧文件的严格全量校验、首次原子初始化等待、
opened-file snapshot 不阻塞并发 append/atomic replace、引用 coverage block 损坏保护、daily partition file
identity、records range index 的无关 block 跳过与未知 flags 拒绝、scan、损坏报告；其中 TQRI v1
必须保守回退，v2 仅在完整覆盖且顺序事实成立时跳过 planner payload scan、
size-limit maintenance，以及通过
`enforce_limits(...)` 执行的 append-log compaction 和
`BacktestTickCache::compact_symbol_ticks(...)` 的按 symbol 全部 tick 日分区 compact、range 版本只触碰相交
trading day。
`backtest_tick_cache_ops` 还覆盖 provisional checkpoint 不进入 final coverage、最新 checkpoint
round-trip、compaction 保留，以及 final coverage 覆盖后立即隐藏并物理淘汰。
`minute_kline_cache` 单元测试覆盖 minute provisional sidecar 不进入 final coverage、显式 reader
可见、未闭合 60s bar 过滤、final 对账后清理，以及 session close + grace 后冻结 observed rows、
空 full-day fallback 不提前 final；`tqsdk-cache` bin test 还证明后续 fill 在远端请求前优先冻结
满足条件的遗留 sidecar。snapshot manifest 测试保证 `.tqmp` 永不发布。
`backtest_tick_cache_ops` 与 `tqsdk-cache` tests 覆盖 TQBN CST `18:00` 交易日边界、read-only
inspection、fast inventory、deep diagnostic、shared ordinary fill/exclusive maintenance root gate、minute
verify/doctor/真实 purge 在并发 shared fill 时返回 `cache_busy`/75、purge dry-run 不取 gate、closed-day
dry-run、完整 cache 无 auth fill、
当前日显式 opt-in、已有 checkpoint 无 auth 复用、partial shared high-water、closed-day final
reconciliation、旧 report 的 `day_complete=complete` 兼容、日历快照 round-trip、
raw holiday snapshot 的 content-addressed pointer / immutable round-trip、legacy daily snapshot 非破坏性
忽略、`--last-trading-days` 的本地选择、周末 anchor、current open-day rejection、支持年份 fail-closed 与
dry-run no-write，以及 progress reducer 的完整分区计数、默认 text /
explicit V3 stdout、legacy V2 output、JSONL stderr progress，以及 fill report v1/v2 对 canonical root
的 verify binding。`tqsdk` 单元测试还覆盖 provisional planner 的 5 分钟 overlap 和远端提交不污染
final coverage；`facade_contract` 还覆盖
`on_remote_fill_telemetry(...)` 在每个 physical cache range 检查后发出累计 inspection telemetry，并在
已检查完整 cache 时发出 physical plan；CLI tests 还覆盖 JSONL `inspection` record。真实远端 fill
仍只在用户明确授权、提供凭证后手动运行，不进入普通 unit test。
`BacktestTickCache::repair_tick_locks`、S49 与 CLI tests 必须覆盖：默认 `DryRun`
不创建 companion lock；`Apply` 只以 non-truncating open 补齐逐文件
`<file>.tqbn.lock`。Apply 必须幂等并保留 TQBN bytes/hash、rows、final/provisional coverage
与 index；invalid/non-regular sidecar 的单目标失败进入 report，同时继续后续目标。
CLI 只接受 `--kind tick`，在 exclusive root gate 下运行且不访问 remote/auth、不调用 fill/compaction。
旧分区锁字段固定为空或零；`failed_files > 0` 返回 exit code `1`。

Unix CLI tests 还必须覆盖 SIGINT/SIGTERM/SIGHUP 在 root lock 等待阶段能立即设置原子 stop 标志、
提交唯一 interrupted terminal report，并以 `130` 退出，而不是等到 lock timeout 后返回 `CacheBusy`。

`backtest_history_api` / `backtest_history_query` 还覆盖 public `RemoteOnMiss` client 必须加入 shared root
gate、exclusive maintenance 存在时返回 `CacheBusy`，以及 fill-only materialization 对完整命中不回读 rows、
报告 `rows=0`。fill executor 单元测试覆盖跨进程 per-family/per-symbol lease 的争用与 coverage 重查、
8192-row bounded tick append 与取消短尾不提交 coverage；facade 单元测试覆盖 shared fill 物理写入只计一次、
final compaction day-range 去重与 provisional skip。

`minute_kline_cache` 与 `minute_kline_cache_ops` 覆盖 v5 zstd `logical symbol × trading month` `.tqmk`
partition、snapshot hash fail-closed、current-day final-coverage guard、opened month snapshot 不阻塞并发
atomic replacement、streaming reader、Refresh 只移除
相交月文件、缺失 root 的 read-only fast inventory，以及 readable v5 / legacy v4-v3
`LegacyUnsupported` diagnosis。测试还覆盖显式 v4 迁移的逐行等价性；旧 v3 不得被自动迁移、覆盖
或当作 cache hit。
`backtest_history_query`、`facade_contract` 与 `tqsdk-cache` CLI tests 还覆盖 active metadata pointer
前移后，完整历史 minute 分区继续离线读取；滚动 snapshot 扩展必须复用语义相同的缓存前缀、只报告新增
尾部缺口，并在追加 final minute 后原子迁移当前月 header。真实 physical mapping 冲突必须仍进入 stale
repair 或 fail closed；`RemoteOnMiss` 完整命中不得读取 auth。远端 minute/daily fill 必须保持终态前
内存有界：minute 子区间不超过 10,000 分钟，daily 子区间不超过 1,024 天；消费后的两类 Kline page
data 必须从专用 session runtime state 清理，同时 final coverage 仍只能在明确 terminal 后发布。
`verify` 必须使用 metadata-backed snapshot，
而不是固定 CST 默认值；缺失 sidecar、session/交易日/映射变化、损坏或混合分区仍必须 fail closed。
显式 `fill --repair-stale` 仅在 active snapshot 覆盖完整窗口时，才 purge 与它冲突的整月分区；该 purge
必须在同一 root remote-fill lock 和 repair 所需 auth preflight 成功后发生。lock busy 或认证缺失必须保留
所有分区；tick 与 `--dry-run` 必须拒绝该 destructive flag。
metadata tests 还覆盖 remote-on-miss 的短 snapshot 不会降级更宽 active pointer、更宽 snapshot 会升级 active
pointer、以及 partial range 的 metadata refresh 扩展到完整 CST trading month。
`daily_kline_cache` 覆盖 v1 单 logical-symbol `.tqdk` 的重叠范围／快照变更 atomic replace（rename 后 parent directory fsync）、
final coverage、retained-sidecar 对既有 coverage 的 compatible reheader、mapping 变化时 fail-closed 且文件 bytes
不变、fixed-header/embedded-symbol fast inventory、全文件 checksum/rows diagnosis、显式 `purge_symbol()` 与
当前/未来 CST trading day final-coverage rejection。
`server_backtest_history` 覆盖 native daily 的 `set_chart.duration=86400000000000`、
`klines/<symbol>/86400000000000` 与 `CanonicalDaily` event。`minute_kline_aggregate` 和 `history_backtest_replay` 覆盖 60s open/final、`N × 60s` 固定 CST `18:00`
trading-day grid 聚合（盘中 break 不重置 bucket）、same-timestamp batch、以及主连 minute cache 保持 logical key 而 replay 保留 dated
`underlying_symbol`。当前 76 项 `tqsdk-cache --test cli` 矩阵还覆盖 tick/minute/daily/all routing、三类
inventory/inspect/verify/doctor、daily fast inventory 与 deep doctor、统一 fill defaults/validation/progress、
新 schema-v3 report 与 tick v1/v2、minute v1、daily v1 兼容读取，以及 tick trading-day range purge、
minute month purge、daily whole-symbol purge（真实删除均要求 `--yes` 和 exclusive root lock）。facade contract 还覆盖
61s/90s rejection、K-only minute path 不请求 tick、CacheOnly 不创建 minute namespace、typed history
inspect/purge、stock backtest builder selection，以及 `DataClient` 的 retention/max-byte 配置只在显式
`run_configured_history_cache_maintenance()` 时执行；任何 tick/minute/daily history read/write 都不能自动删除数据。

`tqsdk-cache query` 的离线 CLI tests 必须在移除 `TQ_AUTH_*` 后覆盖同一
`BacktestHistoryClient` 路径的 CacheOnly Tick 与 canonical-minute Kline：`tqsdk-history-jsonl/1` 的
manifest/block/row/complete/gap/end、canonical fields、timestamp/number codecs、final coverage、source 与
data hash；以及 `tqllm-csv/3` 的 verified metadata gate、默认 Asia/Shanghai / 可覆写 UTC、紧凑 ISO /
有单位 offset / both 时间模式、
无预算 lossless、预算内 deterministic lossy selection、`--compression off` 超预算失败和 `--output`
原子发布。metadata 缺失或 active hash 与 terminal report 不一致时，LLM 默认 fail closed；
`--allow-partial` 只能以 `gap` 省略整个 block。non-Final 或不完整 coverage 必须 hard fail，不能由
`--allow-partial` 放宽。`RemoteOnMiss` query 在 exclusive root gate 存在时必须返回 `cache_busy`/75；
大 payload rendering 必须发生在 data run/其 shared gate 生命周期之后。

`facade_contract` 覆盖 `record_ticks(...)` 和 `MarketCachePolicy` 在 live/session mode 下把显式
symbol 或 selector 解析出的 tick serial 写入同一份 `BacktestTickCache`，并通过
`LiveTickCacheWriter` 的连续 id 语义提交回测可读 coverage；同一个 `MarketCachePolicy`
也必须能为 cache-backed local backtest 提供默认 cache 目录和 symbol 集合。
data writer contract test 还必须覆盖连续单 tick push 的 128 行合并、显式 `flush()`、Drop 短尾提交、
跳号立即提交和失败后重试；`appended_rows` 只统计实际落盘行数。
连续 live tail 的持久化是有界批写：首次、跳号、每 symbol `128` 行、约 `250 ms` 到期或正常
`Tq` 销毁时提交；contract test 必须验证异常退出前未提交 rows 不会获得 coverage，正常销毁会 flush。
首次初始化或 writer 失败后的重扫之外，recorder 必须只读取当前 commit 变更集命中的 tick serial；
contract test 必须覆盖多 symbol 更新不会遗漏变化行，以及失败后从最后持久化 id 恢复。
`quotes_universe(...)`、`.backtest(...).universe(...)` 和
`MarketCachePolicy::record_universe(...)` 复用同一套 universe selector 语义。live recording health 必须暴露累计写入、最近 flush、
per-symbol last id 和 gap 状态，且跳号 tick 应被保留为 `gap_detected`。从 active recording
health 派生的 `MarketCachePolicy` 必须能进入 cache-backed local backtest warmup 路径，用于显式
检查或补齐缺口；该路径不得隐式复用 live session auth。
`scripts/smoke_market_cache_e2e.py` 是真实服务端到端 smoke：生成临时 Cargo harness，
用同一份 `MarketCachePolicy` 跑 remote-on-miss warmup、cache-only warmup 和 cache-only
replay，并要求远端写入行数与 replay tick 数可对齐；交易时段可加 `--live-seconds <N>`
和 `--live-min-rows 1` 验证 live recording health，非交易时段默认不跑 live。
旧 `.tqseries` 和旧单文件 `.tqbn` layout 不是默认 backend，也不提供兼容读取或迁移 store。

TQBN format/store checks currently live in internal lib tests and are covered by
`rtk cargo test -p tqsdk-data --lib tqbn`; `history_series_tqbn_compaction` and
`history_series_tqbn_corruption` cover integration-level compaction and corruption paths.

These TQBN tests should cover file identity, record header, little-endian scalar, price
fixed-point encoding, compatibility skip rules, `HistorySeriesCache` / `BacktestTickCache`
store semantics, records range-index selection/fallback, corrupted input reporting, and append-log rewrite/compaction via
`enforce_limits(...)` and symbol-scoped backtest tick compaction. 旧 `.tqseries`
和旧单文件 `.tqbn` layout 不是当前默认格式；旧 Python-compatible binary/mmap backend 已废弃。

需要真实 `TQ_AUTH_USER` / `TQ_AUTH_PASS` 时，可手动运行 ignored smoke：
`rtk cargo test -p tqsdk --test facade_contract facade_backtest_remote_on_miss_live_smoke -- --ignored`；
该 smoke 验证首次 server-side backtest 按时间片补 tick 缓存、二次无 auth 本地缓存命中。
cache warmup runner 的远端 smoke：
`rtk cargo test -p tqsdk --test facade_contract facade_backtest_warmup_remote_on_miss_live_smoke -- --ignored`。

canonical-minute 的凭证门控验收同样只在用户明确授权时执行，且必须选择已结束的历史 trading-day
window、不得连接交易账户或输出 `TQ_AUTH_*`。至少选取少量实际可用的指数合约（例如
`KQ.i@SHFE.au` 与另一交易所的可用指数代码），先用 `tqsdk-cache fill --kind minute` materialize
60s cache，再对相同 closed window：

1. 从本地 canonical 60s cache 聚合 5m 与 15m bars；
2. 通过 official server-side backtest Kline stream 获取同周期 bars；
3. 对 `<60s` 按 session 边界、对 `N × 60s` 按固定 CST `18:00` trading-day bucket（允许跨盘中 break）
   对齐，比较 timestamp、open/high/low/close、volume、open interest；
4. 仅记录总 bar 数、mismatch 数与少量脱敏样本。

仓库提供了凭证门控的回归入口；它会填充两个指数合约的 canonical 60s cache，并逐根比较本地
5m/15m 聚合与远端 chart：

```bash
rtk cargo test -p tqsdk --test facade_contract canonical_minute_aggregation_matches_remote_index_klines -- --ignored --nocapture
```

完整的 durable-source acceptance 使用 `KQ.i@SHFE.au` 的最近六个完整 CST 月：先通过
`RemoteOnMiss` 物化 Tick 与 canonical 60s 覆盖，再强制 `CacheOnly` 查询本地 15s、60s、5m、15m、
30m、60m K；官方 oracle 以避免 chart 10,000-row 上限的四自然日分片读取相同六个周期。逐根比较
datetime、OHLC（以服务端 quote `price_decs` 规范化）、volume、open_oi、close_oi，不比较 row id；
失败最多输出 20 个差异。该 ignored test 不删除任何 cache partition：

```bash
rtk cargo test -p tqsdk --features live,services --test backtest_history_live kqi_au_six_complete_months_matches_server_oracle -- --ignored --nocapture
```

验收通过条件是六个周期的所有 mismatch 计数为零。它只使用市场/metadata route，不连接交易账户、
不下单且不输出 `TQ_AUTH_*`。第一次运行可能持续较久，因为六个月 Tick coverage 必须完整落盘；
后续同窗口应以 CacheOnly 命中复用这些分区。

任何明显的固定 grid/session 偏移、盘中 break 错误重置高周期 bucket、未来 OHLC 泄漏或成片
OHLC/volume/open-interest 差异都应阻止发布。
对 remote minute fill 的回归还必须确认：只有 terminal-success batch 才提交 final coverage；合法空
range 可以提交 coverage；取消、超时和失败 batch 留下缺口，不能污染 final coverage。

daily native source 的发布门禁不连接普通 CI 的实时账号：tag workflow 只能在 self-hosted
`tqsdk-daily-golden` runner 上读取 `TQSDK_DAILY_KLINE_GOLDEN_PACKET` 指向的外置 immutable packet，先要求
`manifest.sha256` 精确覆盖 packet 内除自身外的全部常规文件并以 strict SHA-256 校验，再运行 ignored
`daily_kline_golden`。schema v2 必须 pin official `tqsdk-python` commit/version，并为 source、expected、metadata
每个 artifact 声明并在反序列化前复核 SHA-256；`main_roll` 必须是有 manifest-declared、请求区间内 underlying
transition 的 `KQ.m@` metadata case（声明逐字段匹配 metadata），`index` 必须是 `KQ.i@`，physical case 必须声明
并由 source rows 证明夜盘/假期边界。每类包含
`1d`、`2d`、`5d`、`28d` 的 expected rows；缺 packet、manifest、hash 或对齐任一失败都使 tag CI 失败。
这项门禁是 native 1d phase/高周期聚合的 official conformance 要求；当前普通 unit test 不声称已通过真实远端验证。

## 场景驱动 public API 契约

`crates/*/examples/api_contract_sXX_*.rs` 是面向终端用户的 public API
契约样本。它们不是普通 demo：每次 public API、crate 拆分、feature flag、
facade 或 runtime 消费方式重构后，都必须确认这些 examples 仍能用清晰、
简洁、类型安全且性能合理的方式表达目标场景。

重构后至少运行：

1. `cargo check --examples`
2. `cargo test`
3. `cargo clippy --examples --all-targets -- -D warnings`

如果 feature flags、workspace 依赖或 crate feature 传播被修改，还必须运行：

1. `cargo check --no-default-features`
2. `cargo check --no-default-features --examples`
3. `cargo test -p tqsdk-session --no-default-features`
4. `cargo check --all-features --examples`

examples 的处理原则：

1. 已经成为正式 API 契约的 example 必须保持可编译。
2. 当前 API 尚不支持、或只能用明显绕路方式表达的场景，可以先作为
   desired API sketch 保存在 `docs/scenarios/api_gaps/`，但不得伪装成已经支持。
3. 一旦某个 gap 被修复，应将其提升为正式 `crates/*/examples/api_contract_sXX_*.rs`，
   并纳入 CI。
4. 如果重构导致 example 变长、变绕、暴露更多内部细节，应优先判定为 API
   退化。

当前场景审查报告见 [`../reviews/public-api-scenario-review.md`](../reviews/public-api-scenario-review.md)；
API gap sketches 见 [`../scenarios/api_gaps/`](../scenarios/api_gaps/)。
S31 低延迟交易柜台 profile 的正式 contract 位于
`crates/tqsdk-task/examples/api_contract_s31_low_latency_trading_desk.rs`，并由
`cargo check -p tqsdk-task --example api_contract_s31_low_latency_trading_desk`
单独覆盖。

S49 tick companion-lock repair 的正式 contract 位于
`crates/tqsdk-data/examples/api_contract_s49_tick_lock_repair.rs`：它演示 caller 预先取得并复用
`try_acquire_consistency_read_lock()`；API 本身也会为未附加 gate 的 Apply 取得 exclusive gate。example 默认只运行
`BacktestTickCache::repair_tick_locks(BacktestTickCacheLockRepairMode::DryRun)`；只有显式 opt-in 才调用
`Apply`。该 example 由 `cargo check -p tqsdk-data --example api_contract_s49_tick_lock_repair` 覆盖。

## Durable hard-risk authority validation

`tqsdk-hard-risk` 是 opt-in SQLite/WAL durable authority，不属于 default-members；workspace
测试和该 crate 的显式 gate 都必须覆盖它。它的验证不得连接真实账号、发送订单或依赖 broker。

```bash
cargo test -p tqsdk-hard-risk
cargo clippy -p tqsdk-hard-risk --all-targets --no-deps -- -D warnings
cargo check -p tqsdk-hard-risk --examples
```

单元测试必须证明：同一 stable identity 在 usage check 前去重；不同 fingerprint fail closed；
`BEGIN IMMEDIATE` 下两独立进程只允许一个 daily-limit winner；WAL/full-sync 已启用；未提交事务
不留下 usage；lease expiry 只进入 `Indeterminate`；incomplete trade snapshot 拒绝 recovery；
runtime command id 重启/复用不改变 durable identity；terminal evidence/audit 保留且不释放
`CountPreparedAttempt` reservation；未知/更高 schema version 拒绝隐式 rewrite。调用方自己的
broker adapter 还必须在每个 network/crash 边界做 credential- and live-order-gated integration test，
包括 matching/mismatched client id、complete/incomplete snapshot 和 terminal reconciliation。

## Public API Documentation Batch Validation

For docs-only public API audit batches, run:

```bash
git diff --check
cargo check --examples
```

If public API source, feature flags, or crate dependencies change, also run:

```bash
cargo fmt --all --check
cargo test
cargo check --no-default-features
cargo check --no-default-features --examples
cargo check --all-features --examples
```

Regression guards fixed in the facade iteration and still required before a source API narrowing batch:

- `cargo test -p tqsdk-task --test public_surface` verifies active docs and
  examples use `tqsdk-task` family paths for broad task foundations; crate root
  aliases remain compatibility-only.
- `cargo test -p tqsdk-task --test scheduler -- --test-threads=1` verifies
  scheduler order dispatch assertions separately from market-interest
  side-effects.
- `cargo test -p tqsdk-session --no-default-features` verifies
  `tests/live_smoke.rs` keeps service-only smoke coverage behind the matching
  feature surface.

## 内部生产发布门禁

内部生产版本发布前，必须在离线 CI 或本地 release-check 环境通过：

1. `cargo fmt --all --check`
2. `cargo check --examples`
3. `cargo test`
4. `cargo test --all-features`
5. `cargo clippy --examples --all-targets -- -D warnings`
6. `cargo check --no-default-features`
7. `cargo check --no-default-features --examples`
8. `cargo test -p tqsdk-session --no-default-features`
9. `cargo check --all-features --examples`
10. `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features`
11. `cargo deny check`
12. `cargo package --no-verify`
13. `cargo test -p tqsdk-relay --tests`
14. `cargo clippy -p tqsdk-relay --all-targets -- -D warnings`
15. `cargo check -p tqsdk-relay --no-default-features`
16. `RUSTDOCFLAGS="-D warnings" cargo doc -p tqsdk-relay --no-deps --all-features`
17. `git diff --check`
18. `cargo +nightly fuzz build`

`cargo package --no-verify` 是默认 SDK crates 的 manifest/package metadata gate；
`tqsdk-relay` 作为可选部署工具用 `-p tqsdk-relay` 单独验证。relay 单包 package
依赖尚未发布的 sibling crates，不作为默认 CI gate。
如果切换到 crates.io 或要求完整 registry verify，必须按依赖顺序发布或验证：
`tqsdk-core` -> `tqsdk-session` -> `tqsdk-wait` ->
`tqsdk-data` -> `tqsdk-task` -> `tqsdk`。

`cargo +nightly fuzz build` 只验证 fuzz targets 可编译，不执行长时间 fuzz campaign。
`fuzz/` 是独立 crate，不属于 workspace members；它只通过 `cfg(fuzzing)` 访问窄 helper，
不得扩大普通 public API 或改变 runtime contract。需要本地短跑时可执行：

```bash
cargo +nightly fuzz run core_frame_payload -- -runs=1000
cargo +nightly fuzz run core_adapter_decode -- -runs=1000
cargo +nightly fuzz run data_history_cache_scan -- -runs=1000
```

## Feature / no-default build matrix
以下命令用于固定 feature flags 与最小依赖构建基线，防止默认 feature 构建通过但 `--no-default-features` 或单独 feature 组合退化。

1. `cargo build -p tqsdk-core`
2. `cargo build -p tqsdk-core --no-default-features`
3. `cargo build -p tqsdk-session --no-default-features`
4. `cargo build -p tqsdk-session --no-default-features --features live`
5. `cargo build -p tqsdk-session --no-default-features --features services`
6. `cargo build -p tqsdk-wait --no-default-features`
7. `cargo build -p tqsdk-task --no-default-features`
8. `cargo build -p tqsdk-data --no-default-features`
9. `cargo build -p tqsdk --no-default-features`
10. `cargo build -p tqsdk --no-default-features --features live`
11. `cargo build -p tqsdk --no-default-features --features services`
12. `cargo build -p tqsdk --all-features`
13. `cargo test -p tqsdk`
14. `cargo test -p tqsdk-core`
15. `cargo test -p tqsdk-session --no-default-features`

生产发布联机 smoke 入口：

本地合约信息 typed metadata 回归入口：

```bash
cargo test -p tqsdk-session parse_symbol_info_maps_graphql_payload_to_symbol_info_schema
cargo test -p tqsdk-session instrument_spec_normalizes_contract_metadata_from_symbol_info
```

1. `cargo test -p tqsdk-session live_query_symbol_info_smoke -- --ignored --nocapture`
2. `cargo test -p tqsdk-session live_query_command_wait_smoke -- --ignored --nocapture`
3. `cargo test -p tqsdk-session live_raw_and_control_plane_requests_smoke -- --ignored --nocapture`
4. `cargo test -p tqsdk-session live_metadata_query_pack_smoke -- --ignored --nocapture`
5. `cargo test -p tqsdk-session live_service_query_pack_smoke -- --ignored --nocapture`
6. `cargo test -p tqsdk-session live_quote_progress_smoke -- --ignored --nocapture`
7. `cargo test -p tqsdk-session live_tqkq_trade_login_smoke -- --ignored --nocapture`
8. `cargo test -p tqsdk-wait live_quote_wait_smoke -- --ignored --nocapture`
9. `cargo test -p tqsdk-wait live_quote_wait_with_session_query_smoke -- --ignored --nocapture`
10. `cargo test -p tqsdk-task live_task_host_trade_account_ready_smoke -- --ignored --nocapture`
11. `cargo test -p tqsdk-task live_insert_cancel_guarded_smoke -- --ignored --nocapture`
12. `cargo test -p tqsdk-task live_scheduler_pause_step_smoke -- --ignored --nocapture`
13. `cargo test -p tqsdk-data live_history_request_pack_smoke -- --ignored --nocapture`
14. `cargo test -p tqsdk-data live_option_greeks_smoke -- --ignored --nocapture`
15. `cargo test -p tqsdk-data live_export_kline_csv_smoke -- --ignored --nocapture`
16. `cargo test -p tqsdk-data live_export_tick_csv_smoke -- --ignored --nocapture`
   默认走官方内置 `TqKq` 主模拟账户；可选用 `TQ_TRADE_ACCOUNT_NO=<1..99>` 切到辅模拟账户，或同时设置 `TQ_TRADE_BROKER_ID` / `TQ_TRADE_ACCOUNT_ID` / `TQ_TRADE_PASSWORD` 显式覆盖
   只有显式设置 `TQ_SMOKE_ALLOW_ORDER=1` 且提供 `TQ_SMOKE_ORDER_SYMBOL` / `TQ_SMOKE_ORDER_LIMIT_PRICE` 时才会真正发单；
   EDB service query 默认在账号无 EDB 权限时跳过 EDB 断言，设置
   `TQ_REQUIRE_EDB=1` 可将该权限错误作为失败处理。

实盘期货 smoke 需要官方 `tqsdk-ctpse`；若它安装在虚拟环境中，设置
`TQ_TRADE_CTPSE_PYTHON=/absolute/path/to/python`，并建议同时设置
`TQ_TRADE_REQUIRE_CLIENT_SYSTEM_INFO=1`，确保采集失败发生在登录 dispatch 前。

### CTP helper 验证

默认离线验证运行 `cargo test -p tqsdk-ctpse-helper` 以及 `cargo build -p tqsdk-ctpse-helper --bin tqsdk-ctpse-helper`。`tools/vendor_ctpse.py --accept-redistribution-license --version 1.2.0` 必须生成 Linux x64、macOS universal2（两种 Apple target）、Windows x64/x86 的清单；它校验官方 SHA-256，并排除 wheel 内的测试 collector。

macOS 和 Windows 的加载验证必须在对应原生 CI runner 上运行 helper，不得以 Linux cross-compile 代替。显式官方库联调设置 `TQ_TRADE_CTPSE_HELPER=/absolute/path/to/tqsdk-ctpse-helper` 与 `TQ_TRADE_CTPSE_LIBRARY=/absolute/path/to/official-library`，再以 `TQ_TRADE_REQUIRE_CLIENT_SYSTEM_INFO=1` 运行已授权的实盘登录 smoke。Python fallback 则设置 `TQ_TRADE_CTPSE_PYTHON=/absolute/path/to/python`。构建过程不联网。

## V2+ adapter 验收基线
### wait adapter
- 能只靠 `RuntimeReader` / `SnapshotReadGuard` / `UpdateCursor` 实现 `wait_update()`
- 能只靠 `ChangeSet` 实现 `is_changing()`

### callback adapter
- 能只靠 `RuntimeReader::next()` / `UpdateCursor` 实现回调 fan-out
- callback 慢消费者不改变 commit 生成逻辑

## 测试策略总表
| 测试层级 | 目标 |
| :--- | :--- |
| 单元测试 | 验证命令归一化、mutation 生成、state apply、change 归并 |
| 集成测试 | 验证 command-to-commit 全链路与 snapshot 一致性 |
| contract 测试 | 验证不同协议域共享同一 revision / causality / cursor 模型 |
| 重连专项 | 验证 session error、重连与 resync 仍走统一提交模型；覆盖有限重试耗尽和默认无限重试直到成功 |
| adapter 验证 | 验证 wait / fan-out / callback 只消费 contract，不回改 contract |

## History context

Context coverage validates Tick `(timestamp_ns, id)` anchors, Kline floor anchors,
actual counts across non-trading gaps, complete main-contract provenance, fixed
metadata/source basis, and 409 coverage/provisional/metadata failures. HTTP tests
cover row-limit rejection before admission, anchor response encoding, and audit
parity. Performance fixtures must record probe/final-scan ranges and rows for a
representative 1,800-row window; context probing is capped at 16 scans and the
90-day total window, under the listener's 10-second deadline.

## Workspace 测试放置原则
为保持多个 crate 的测试结构一致，新增或移动测试时遵守以下规则：

1. 白盒单元测试放在被测模块旁边，并通过 `#[cfg(test)]` 只在测试构建中编译。
   小模块优先使用同文件 `mod tests { ... }`；当测试体量会显著干扰实现阅读时，
   使用旁置目录 `src/<module>/tests.rs`，再由 `src/<module>.rs` 以
   `#[cfg(test)] mod tests;` 引入。
2. `src/*_tests.rs` 这种平铺旁置测试文件不再新增。已有大模块测试应逐步迁移到
   `src/<module>/tests.rs`，和 `client/tests.rs` 等模块目录形态保持一致。
3. 只通过 public crate API 验证行为、跨模块协作、facade surface、runtime contract
   或回归场景的测试放在 `crates/<crate>/tests/*.rs`。这些测试按能力面命名：
   `runtime_contract_*`、`session_*`、`wait_api_*`，或清晰的领域名
   如 `target_pos`、`history_series_cache`、`relay_*`。
4. 集成测试共享 fixture 放在 `crates/<crate>/tests/support/`，由需要的测试文件
   `mod support;` 引入；不要为了复用测试 helper 扩大生产 public API。
5. 需要真实账号、网络、交易权限或会发单的 smoke 测试统一放在
   `crates/<crate>/tests/live_smoke.rs`，必须 `#[ignore]`，并用显式环境变量门控。
6. `crates/*/examples/api_contract_sXX_*.rs` 只放 public API 场景契约；不要把它们当作
   普通 integration test 或 live smoke 的替代品。
7. async 测试默认使用 `#[tokio::test(flavor = "current_thread")]`，除非测试明确需要
   Tokio 多线程 runtime。需要多线程时，应在测试附近保留原因。

history HTTP 验证还必须覆盖 paired CPU-set fail-fast、实际 worker affinity 握手、64 KiB
threshold、gzip level 1、两 worker 有界 try admission、共享 CPU-work budget 耗尽时的 identity
fallback/telemetry，以及
identity/gzip ETag、304、`Vary`、q=0 和 timeout（压缩计入 10 s）。512 MiB 检查应覆盖
scan、JSON 和 compression buffers。生产同规格性能 gate 是非阻塞容量特征项，不得由单元测试
或 affinity 配置本身宣称通过。2026-08-29 当前生产宿主机未达到该 p99 目标；需要高并发或明确
p99 SLO 时必须重新实测并单独验收。

无有效 snapshot 的集成测试还必须证明：请求起点晚于服务器时间超过 5 秒时，coverage/query
在 admission 与 source lookup 之前返回 `409 coverage_incomplete`，且
`details.reason=range_starts_in_future`。另需证明 `start < server_time < end` 默认裁到唯一连续 final
前缀并返回带 truncation metadata 的 `200`；存在内部缺口或没有非空连续前缀时仍返回 typed 409。
# History source 分页一致性回归

- `cargo test -p tqsdk-data --lib backtest_history::fill::tests`：保留重叠/预取行、裁剪连接不回池、
  单 chart 门控，以及 32 页消费后仅保留边界和预取数据的内存上界。
- `cargo test -p tqsdk-session --test server_backtest_history`：canonical minute/daily 缺失窗口边界
  必须失败，允许内部 ID 稀疏；保留 Tick 与空终态的既有行为。
- 真实历史验证必须使用隔离缓存；不要用既有 final coverage 跳过读取后宣称修复通过。
  生产旧分区须单独重建，Timeline 的算术测试不能证明底层行情完整。
