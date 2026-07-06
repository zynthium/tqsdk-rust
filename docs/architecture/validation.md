# 验收标准与测试矩阵

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
- 任意已提交 revision 都必须能提供内部一致的 snapshot
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
- `CommitLog` 必须有 retention 策略，且不能截断仍被活动 cursor 需要的提交

### adapter 边界
- adapter 可以编解码和保留短期协议态
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
| cursor isolation | 两个 cursor 从不同 revision 开始消费 | 各自独立推进 | cursor 独立性 |
| multi-adapter observation | 一个输入被多个 adapter 观察 | 只通过 mutation/commit 对外可见 | adapter 无提交权 |

## 当前实现验证入口
当前仓库已经有直接对应 V1 contract 的验证入口，可作为“功能全景”验收基线：

| 能力面 | 主要验证文件 | 说明 |
| :--- | :--- | :--- |
| DIFF 协议对象 | `crates/tqsdk-core/tests/runtime_contract_v1_capability.rs`、`crates/tqsdk-core/tests/runtime_contract_batch_commit.rs`、`crates/tqsdk-core/tests/runtime_contract_adapters.rs` | 覆盖 market diff、trade diff、query/schema/replay 输入归一化与提交 |
| trade 命令与状态 | `crates/tqsdk-core/tests/runtime_contract_v1_capability.rs`、`crates/tqsdk-core/tests/runtime_contract_command_ledger.rs` | 覆盖 `req_login`、`insert_order`、`pre_insert_order` 及命令状态写回 |
| replay/feed 推进 | `crates/tqsdk-core/tests/runtime_contract_v1_capability.rs`、`crates/tqsdk-core/tests/runtime_contract_pending_route_executor.rs` | 覆盖 replay pending route 执行与 replay state 提交 |
| auth/session/system 控制 | `crates/tqsdk-core/tests/runtime_contract_v1_capability.rs`、`crates/tqsdk-core/tests/runtime_contract_auth_context.rs`、`crates/tqsdk-core/tests/runtime_contract_session_state.rs`、`crates/tqsdk-core/tests/runtime_contract_session_runtime.rs` | 覆盖 auth context、topology/bootstrap、refresh-auth、session state |
| GraphQL / HTTP query | `crates/tqsdk-core/tests/runtime_contract_v1_capability.rs`、`crates/tqsdk-core/tests/runtime_contract_pending_route_executor.rs`、`crates/tqsdk-core/tests/runtime_contract_adapters.rs` | 覆盖 GraphQL query 的 HTTP request 合同、pending route 执行与 query snapshot |
| schema / metadata / bootstrap 交互 | `crates/tqsdk-core/tests/runtime_contract_v1_capability.rs`、`crates/tqsdk-core/tests/runtime_contract_pending_route_executor.rs`、`crates/tqsdk-core/tests/runtime_contract_session.rs`、`crates/tqsdk-core/tests/runtime_contract_bootstrap.rs` | 覆盖 schema HTTP 请求、bootstrap topology 与 metadata/state 写入 |
| reader-first 读契约 | `crates/tqsdk-core/tests/runtime_contract_reader_surface.rs`、`crates/tqsdk-core/tests/runtime_contract_surface.rs`、`crates/tqsdk-core/tests/runtime_contract_runtime_core.rs`、`crates/tqsdk-core/tests/runtime_contract_domain_state.rs` | 覆盖 `RuntimeReader`、`SnapshotReadGuard`、`CommitReadGuard`、`MarketTradeStateReadGuard`、`CursorLagged`、共享 commit identity 与兼容 surface |
| 官方对象 typed schema | `crates/tqsdk-core/tests/runtime_contract_types.rs`、`crates/tqsdk-core/tests/runtime_contract_reader_surface.rs` | 覆盖 `objs.py` 对象族和 core 补充 diff 对象的 typed schema surface、期货 `Order`/`Trade` 协议枚举字段解码，以及 reader 侧 `decode<T>()` 接入 |
| 纯交易时段 helper | `crates/tqsdk-core/tests/trading_session.rs` | 覆盖 `TradingSessionSchedule` 的 open / pre-close / closed、跨午夜 rollover、空 schedule 和非法空窗口 |
| 默认 facade crate | `crates/tqsdk/tests/facade_contract.rs`、`crates/tqsdk/examples/api_contract_s33_default_facade.rs`、`crates/tqsdk/examples/api_contract_s37_facade_server_backtest.rs`、`crates/tqsdk/examples/api_contract_s38_facade_local_backtest.rs`、`crates/tqsdk/examples/api_contract_s39_facade_same_body.rs`、`crates/tqsdk/examples/api_contract_s40_facade_local_backtest_target_pos.rs`、`crates/tqsdk/examples/api_contract_s41_facade_server_replay.rs`、`crates/tqsdk/examples/api_contract_s43_facade_backtest_history_cache.rs`、`crates/tqsdk/examples/api_contract_s44_facade_backtest_remote_on_miss.rs`、`crates/tqsdk/examples/api_contract_s45_facade_backtest_cache_warmup.rs`、`crates/tqsdk/examples/api_contract_s46_facade_record_ticks.rs`、`crates/tqsdk/examples/api_contract_s47_facade_market_cache_policy.rs` | 覆盖 `tqsdk::prelude::*`、`Tq` / `TqBuilder`、统一 `.backtest(...)` no-cache server mode、cache-backed local backtest builder、`BacktestTickCache` facade export、server replay session endpoint 接入、自动 heartbeat 和显式 replay 控制、resolved TQKQ target-position helper、`TargetPos` intent API 与增量 execution report、curated `advanced::*` 下钻命名空间，以及默认 facade 的 persistent-cache backtest / warmup / remote-on-miss cache fill / live record_ticks cache writer / shared `MarketCachePolicy` / universe selector / server replay / custom replay backtest / live-backtest same-body 策略入口和 local backtest TargetPos 执行闭环 |
| 可选嵌入式监控 | `cargo test -p tqsdk-monitor`、`cargo test -p tqsdk --features monitoring replay_backtest_connect_starts_embedded_monitor --lib`、`cargo test -p tqsdk --features monitoring monitoring_uses_market_cache_inventory_by_default --lib`、`cargo check -p tqsdk --features monitoring --example api_contract_s48_facade_monitoring_dashboard`、`cargo check -p tqsdk --features monitoring --examples` | 覆盖低开销 `MonitorSink` / `MonitorRegistry` 聚合、只读 HTTP snapshot、cache inventory 后台刷新、facade `.monitoring(...)` 接入、market cache/backtest cache 目录自动作为默认 inventory 来源、disabled monitoring no-op、可运行 dashboard example，以及 monitoring feature 不破坏默认 facade examples |
| 可选 market relay | `cargo test -p tqsdk-relay --tests` | 覆盖 relay 配置、dry-run 启动自检、结构化启动诊断、分层 HTTP `/health`、`/metrics`、`/symbol-metrics`、原子 `/dashboard-snapshot`、dashboard 5 分钟 `timeline_history` 服务端内存缓存、内置 `/dashboard`、上游连接/订阅/补历史阶段 telemetry 和 backfilling 可观测进度、等待首样本或补历史无样本合约 `initializing` 非问题状态、frame/event idle 秒级告警、raw frame 后先发 `peek_message` 再 JSON decode 的顺序 guard、上游 idle 期间周期性 `peek_message` 恢复守卫、peek/decode timing metrics、200 合约 decode guard、可恢复 decode health、每日合约集合刷新调度、typed metadata 期货产品发现与分批查询、每品种主力-only 快捷选择、每品种活跃度前 N 合约选择、上游一合约一 tick chart 订阅、tick row 连续性缺口/重复/乱序 telemetry、当前 universe ∪ 当前订阅健康集合、dashboard read-model 低频缓存、dashboard read-model 锁外分类、进程内固定容量事件账本、单 chart `ins_list` 长度防线、tick view width 配置、下游 market 协议、interest/chart-id 隔离、K 线 `[start,end)` 合成、tick-ring 冷启动回放、bootstrap 队列限流、observability、WebSocket loopback、upstream tick scaffold 和 quote-only 远月行情更新 |
| relay endpoint opt-in | `cargo test -p tqsdk-session --test session_builder builder_accepts_explicit_market_relay_url_without_enabling_other_routes` | 确认 relay 只显式改 market endpoint，不启用 trade/query/auth |

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

```bash
rtk cargo test -p tqsdk-data --test history_series_single_file_store
rtk cargo test -p tqsdk-data --test history_series_cache
rtk cargo test -p tqsdk-data --test history_series_tqbn_compaction
rtk cargo test -p tqsdk-data --test history_series_tqbn_corruption
rtk cargo test -p tqsdk-data --lib tqbn
rtk cargo test -p tqsdk-data --features tqbn-zstd --lib tqbn
rtk cargo check -p tqsdk-data --features tqbn-zstd --example history_series_cache_microbench
rtk cargo test -p tqsdk-data
rtk cargo clippy -p tqsdk-data --all-targets -- -D warnings
rtk cargo test -p tqsdk-data --test universe_selector
rtk cargo test -p tqsdk-task --test history_tick_replay
rtk cargo test -p tqsdk-task --test history_backtest_replay
rtk cargo test -p tqsdk-task kline_synth
rtk cargo test -p tqsdk backtest_kline
rtk cargo test -p tqsdk --test facade_contract
rtk cargo check -p tqsdk --example api_contract_s43_facade_backtest_history_cache
rtk cargo check -p tqsdk --example api_contract_s44_facade_backtest_remote_on_miss
rtk cargo check -p tqsdk --example api_contract_s45_facade_backtest_cache_warmup
rtk cargo check -p tqsdk --example api_contract_s46_facade_record_ticks
rtk cargo check -p tqsdk --example api_contract_s47_facade_market_cache_policy
rtk cargo test -p tqsdk-monitor
rtk cargo test -p tqsdk --features monitoring replay_backtest_connect_starts_embedded_monitor --lib
rtk cargo test -p tqsdk --features monitoring monitoring_uses_market_cache_inventory_by_default --lib
rtk cargo check -p tqsdk --features monitoring --example api_contract_s48_facade_monitoring_dashboard
rtk cargo check -p tqsdk --features monitoring --examples
rtk python3 scripts/smoke_market_cache_e2e.py --symbols KQ.i@SHFE.au --timeout-secs 300
rtk python3 scripts/bench_backtest_tick_cache.py --profile release --tqbn-zstd --cargo-offline --verify-existing-cache --cache-root <existing-zstd-cache-root> --batch-sizes 32 --slice-secs none
```

`history_series_single_file_store`、`history_series_cache`、`history_series_tqbn_compaction`
和 `history_series_tqbn_corruption` 覆盖当前默认 TQBN history cache 行为、embedded coverage、
daily partition file identity、scan、损坏报告、size-limit maintenance，以及通过
`enforce_limits(...)` 执行的 append-log compaction 和
`BacktestTickCache::compact_symbol_ticks(...)` 的按 symbol 全部 tick 日分区 compact。
`facade_contract` 覆盖 `record_ticks(...)` 和 `MarketCachePolicy` 在 live/session mode 下把显式
symbol 的 tick serial 写入同一份 `BacktestTickCache`，并通过 `LiveTickCacheWriter` 的连续 id
语义提交回测可读 coverage；同一个 `MarketCachePolicy` 也必须能为 cache-backed local backtest
提供默认 cache 目录和 symbol 集合。live recording health 必须暴露累计写入、最近 flush、
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
store semantics, corrupted input reporting, and append-log rewrite/compaction via
`enforce_limits(...)` and symbol-scoped backtest tick compaction. 旧 `.tqseries`
和旧单文件 `.tqbn` layout 不是当前默认格式；旧 Python-compatible binary/mmap backend 已废弃。

需要真实 `TQ_AUTH_USER` / `TQ_AUTH_PASS` 时，可手动运行 ignored smoke：
`rtk cargo test -p tqsdk --test facade_contract facade_backtest_remote_on_miss_live_smoke -- --ignored`；
该 smoke 验证首次 server-side backtest 按时间片补 tick 缓存、二次无 auth 本地缓存命中。
cache warmup runner 的远端 smoke：
`rtk cargo test -p tqsdk --test facade_contract facade_backtest_warmup_remote_on_miss_live_smoke -- --ignored`。

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
