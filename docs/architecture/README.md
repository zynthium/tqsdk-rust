# tqsdk-rs 分层内核架构

## 文档定位
本文档目录描述的是“从头重写一个 Rust 版天勤 TqSdk”的基础架构主线。

这里的第一原则不是先做某种用户 API，而是先做一个足以承载所有远端协议与对象的统一 runtime contract。

仓库级文档职责和 AI 读取入口见 [`../README.md`](../README.md)。本目录是当前架构权威；`../reviews/`、`../archive/` 与 `../superpowers/` 中的审查记录和计划只能作为输入材料，不能覆盖本目录已经确认的 crate 边界和 runtime 不变量。`superpowers` 里的 spec / plan / execution review 以执行记录为主，闭环后应迁入 `../archive/superpowers/`。

重点回答：

- V1 到底交付什么
- 哪些能力必须进入 runtime kernel
- 为什么 `RuntimeReader` 而不是 `wait_update` / callback / fan-out 才是 V1 的主读契约
- 如何在不回改内核的前提下，同时承载 Python 风格和 Rust 风格的后续 facade
- `tqsdk-python` 与现有 `tqsdk-rs` 两种 facade 范式该如何取长补短

## V1 的总定位
V1 不是：

- `wait_update()` SDK
- callback / fan-out SDK
- `TqApi` SDK

V1 是：

- protocol-complete runtime contract
- 统一所有远端交互的提交模型
- 后续一切 facade 的公共底座

它必须覆盖：

- DIFF 协议对象
- trade 命令与状态
- replay/feed 推进
- auth/session/system 控制
- GraphQL / HTTP query
- schema / metadata / bootstrap 交互

交易登录的 CTP 穿透式责任按边界分工：`tqsdk-core::TradeLoginCommand`
只承载可选的 MAC、App ID 和系统信息字段；`tqsdk-session` 在命令提交边界使用官方
`tqsdk-ctpse` 采集、校验和补全字段。facade 只复用 session，不维护第二套采集逻辑。

它明确不提供：

- `TqApi`
- `wait_update()` facade
- fan-out facade
- callback facade
- 各类高层 view
- `TargetPosTask`
- DataFrame / polars / downloader / GUI / report

### CTP 原生采集隔离

官方原生 `tqsdk-ctpse` 只能由私有 `tqsdk-ctpse-helper` 子进程动态加载。`tqsdk-session` 只接收并验证其窄 JSON 输出，绝不复制或持有 OS 动态库句柄。审核后的 `1.2.0` 离线 bundle 在构建时按 Cargo `TARGET` 选择官方 Linux x64、macOS universal2、Windows x64/x86 产物；helper 在运行时解压至用户私有缓存。Windows ARM64 无官方产物，自动路径继续回退官方 Python 采集器。

helper 不是 facade、runtime 或 public API；它隔离 SDK 地址空间中的 native crash，但不是对官方库的 OS sandbox。

## 当前实现状态
当前仓库里的 V1 已经以“极简但协议完整”的 core contract 落地完成。

当前 public core 的稳定主线是：

- `RuntimeHandle`
  - 写入、命令提交、session/runtime 控制入口
- `RuntimeReader`
  - canonical read-side 入口
  - 提供 cursor 创建、commit 消费、zero-copy 状态读取
  - 提供 market/trade 分区读面，以及同 revision 的
    `read_market_trade_state()` 组合读面
- `TradingSessionSchedule`
  - 纯交易时段状态 helper，用于本地日内时段的 open / pre-close / closed 判断与倒计时计算
- `tqsdk_core::transport`
  - transport trait、session route/topology/config contract
  - yawc-backed `WebSocketTransport` 由默认 feature `websocket-transport` 提供；
    `--no-default-features` 保留 contract 但不拉取 websocket 实现依赖
  - 单次 WebSocket route 建立包含有界的 3 次尝试，每次 socket/TLS 握手最长
    15 秒；这只吸收初始链路瞬时黑洞，不替代 session-level `ReconnectPolicy`
- `SnapshotReadGuard` / `StateReadView`
  - revision-bound 的借用读视图
  - 为 `wait_update`、callback/fan-out facade 提供共同读面
- `UpdateCursor`
  - 独立推进的 commit 消费游标
- `CommitResult` / `SharedCommitResult`
  - `CommitResult` 是不可变提交元数据；`SharedCommitResult = Arc<CommitResult>`
    是 runtime 发布、cursor 消费和 fan-out 的共享所有权句柄

不属于稳定 public core 主线：

- raw outbox envelope（例如 `OutboundEnvelope`）是 runtime 内部队列细节；低层 route 消费者应使用 `OutboundDispatch`
- multi-source aggregation helper 不是 V1 public contract；需要时应先重新设计场景和文档

仍保留的兼容/底层原语：

- `StateSnapshot`
  - 需要 detached owned snapshot 时可直接使用
- `CommitLog`
  - 底层 commit buffer，可用于兼容层或测试

当前 public core 可以直接覆盖并验证：

- DIFF 协议对象
- trade 命令与状态
- replay/feed 推进
- auth/session/system 控制
- GraphQL / HTTP query
- schema / metadata / bootstrap 交互

验证入口见 [validation.md](validation.md) 与 `crates/tqsdk-core/tests/runtime_contract_v1_capability.rs`。

在 core 之上的第二层分拆也已经开始落地：

- `tqsdk`
  - 默认用户入口 crate
  - `prelude`、`Tq` / `TqBuilder`、轻量 `TargetPos` wrapper
  - 本地回测默认模拟账户 id `LOCAL_BACKTEST_ACCOUNT_ID`
  - Python-style `.backtest(start_ns, end_ns)` 统一回测入口：默认使用 `tqsdk-data`
  共享 history cache root（`$HOME/.tqsdk/data_series_1`，可用
  `TQSDK_HISTORY_CACHE_DIR` 覆盖）；默认 `RemoteOnMiss` 先复用本地 TQBN daily
  tick cache、独立 canonical final-60s monthly cache 与 native final-1d single-file cache，缺失时由 `tqsdk-data`
    `BacktestHistoryClient` 通过官方 server-side backtest stream 填充对应输入并驱动本地 `TqSim`。
    一个 client 最多保留 logical concurrency 个 clean source lanes；Tick 每日 coverage checkpoint 与
    minute bounded window 保持不变，但 clean terminal 后复用 lane/session，避免每段重复鉴权和建连。
    pool 饱和时 overflow 不在 series lease 内等待且不会回池；取消、协议/传输错误或 chart cleanup
    失败也会丢弃 lane。
minute cache 使用 v5 文件身份，只有远端 terminal 成功后才提交 final coverage；`KQ.m@...`
    使用 data 持久化的 calendar/session/physical-segment metadata sidecar 解析为 dated
    concrete-contract tick ranges，因而与具体合约共用物理 cache、coverage 和远端补缺请求。
    `RemoteOnMiss` 只在 sidecar 缺失或不覆盖窗口时刷新；CacheOnly 必须已有 sidecar 且完全离线。
    具体合约 cache hit 不需要 auth，cache fill 不使用专业历史下载接口；显式 `.disabled_cache()` 才使用官方
  server-side backtest market stream 且不落盘
  - canonical-minute 月文件绑定写入时的 immutable metadata snapshot。active pointer 后续移动时，只有保留
    snapshot 覆盖整个请求窗口、schema/session identity 不变且能精确验证现有月文件，才可回退读取该历史
    分区；缺失历史 snapshot、session 变化、损坏或混合分区均 fail closed，且不会自动删除、重写或拼接数据
- `PreparedBacktest::tick_sources()` 只读暴露上述 logical-to-physical 投影及各自有效区间，
  供调用方自有多资产回放器并行读取；跨品种 barrier、截面调度和策略状态不下沉到 facade
- `.provisional_open_day_fill(day_start_ns, as_of_ns)?` 为固定 as-of 的当前交易日 warmup
  提交 non-final checkpoint。Tick 使用 TQBN provisional record；canonical 60s minute 使用独立
  `.tqmp` sidecar 并过滤未闭合 bar。两者都不进入普通 final coverage/cache-hit。canonical-minute
  若非空 metadata session 证明最后一个 window 已结束，且 checkpoint 已到 close + grace，则原子冻结
  observed rows 为 final `.tqmk`，不会再下载盘后 vendor revision；缺少可靠 session 或完整收盘
  checkpoint 时不能声称严格 as-traded
  - cache-backed backtest 的显式 `.tick(...)` / `.kline(...)` serial 声明使用同一套
  本地 replay runtime：`<60s` 的 K 线从 tick cache 本地合成，`60s` 至 `<1d` 的整数分钟从 canonical
  minute cache 读取/聚合，`1d` 读取 native daily cache，`2d` 至 `28d` 由 data 查询从 final 1d rows 聚合；
  task 回放已关闭分钟/日线事件。分钟盘中 break 不重置高周期 bucket。`61s` / `90s`、非整数日和大于 `28d`
  的日周期拒绝，且 K-only
    `>=60s` 不请求 tick。K 线 quote synthesis 的 price tick / instrument
    spec 由 facade 显式 builder metadata 转发，不自动联网查询
  - `.universe(...)`、`quotes_universe(...)` 和 `MarketCachePolicy::record_universe(...)`
    复用 relay 对齐的期货 universe selector 语法，支持全品种回测、实时订阅和 live/cache policy
    symbol 集合声明
  - `MarketCachePolicy` / `.market_cache(...)` 用同一份配置维护 live tick recording
    和 cache-backed backtest 的 cache 目录及 symbol 集合；`record_ticks_health()`
    暴露写入与 gap 状态，`recorded_market_cache_policy()` 只派生补洞 policy，
    不携带或复用 live session 明文 auth
  - local backtest history `_as` helper 可把 underlying series/request 以主连等 caller-provided replay symbol 回放
  - cache-backed facade backtest / warmup 可自动查询 `KQ.m@...` 主连 underlying segment、按
    CST 交易日窗口裁剪并以具体合约缓存；同一物理 range 会合并，避免主连与具体合约重复回填
  - 主连 minute/daily cache 以逻辑 symbol 为 key；dated physical mapping 仅进入 replay
    `underlying_symbol` metadata。`60s`、整数分钟高周期、native `1d` 与 `2d` 至 `28d` 均支持，不复制 physical files
  - cache-backed local backtest 暂限 futures；股票 server-backtest 保留 `.disabled_cache()` 的官方
    直连路径，不能误用 futures-only durable fill source
  - tick、canonical 60s 与 native 1d cache 没有自动 retention、max-byte eviction 或后台清理；2d 至 28d
    派生 K 不落盘。daily 是不分区的单 logical-symbol 文件，destructive refresh/purge 必须显式调用
  - 本地 `TqSim` 可基于 replay quote 的 `underlying_symbol` 将主连等 replay symbol 订单映射到 actual underlying symbol 执行，并把持仓镜像回 replay symbol
  - `advanced::*` 作为 curated escape hatch 下钻到 core/session/wait/task/data
  - 不改变能力归属，不拥有第二套 runtime、状态树或 query/task/data 实现
- `tqsdk-session`
  - shared session shell
  - lazy establish + route / pending-route 驱动原语
  - `progress_once()` 这个最小 substrate 推进原语
  - `subscribe_quotes()` / `unsubscribe_quotes()` 这类低层命令 helper
  - session-scoped market interest registry，用于 quote、trading status 和 chart
    lease 的去重、引用计数与最后 owner 释放
  - `ServerBacktestHistoryStream::close().await` 在 session 复用前同步完成 chart lease cleanup；
    Drop 仍只承担无法显式 close 时的异步兜底
  - `wait_command_completed()` 这个最小 control-plane 等待原语
  - `command_status_typed()` 这个 additive typed 命令状态读取 helper
  - direct query / schema refresh 薄层入口
  - value-style GraphQL direct query 内部串行化完整 query lifecycle；raw
    command-style query 仍由调用方负责推进顺序
  - direct query surface 再细分为 `SessionRawQuery` / `SessionMetadataQuery` / `SessionServiceQuery`
  - `SymbolInfo` / `InstrumentSpec` / `InstrumentClass` 这类一次性 metadata
    标准化对象；`SymbolInfo` 对齐官方合约信息表，`InstrumentSpec` 是窄的
    下单校验规格对象
  - `ServerReplayBuilder` / `ServerReplaySession` 用于官方单日 replay session
    创建和 endpoint 解析；默认 facade 可用 `.server_replay(date)?` 接入返回的
    replay 行情 endpoint，并自动 heartbeat；控速和 terminate 为显式调用，
    terminate 不伪装成 async Drop
  - session-level error diagnostic / retry hint wrapper
  - session-scoped order intent ledger，供上层 facade 在同一 session 内对稳定
    client order id 做去重和命令关联
  - 保持“纯 async substrate，调用方自带 Tokio runtime”的约束
  - 供 `wait` 和自建消费层共同依赖
- `tqsdk-wait`
  - `TqApi` 单推进点 facade
  - market/trade 对象引用
  - 批量 quote 入口 `quotes(...)`，返回 symbol-indexed refs，并复用 session
    interest registry 管理订阅意图
  - serial window 视图；K 线支持单合约 `kline(...)` 和多合约
    `kline_multi([...])`，Tick serial 保持单合约
  - `kline` / `tick` non-blocking handle 与 `kline_ready` / `tick_ready` chart
    初始化等待路径
  - 基于 shared session 的 live `wait_update()` 驱动链路
  - trade 命令的 wait 风格薄包装
  - 允许通过 `session()` 落回同一个底层 `SessionClient`，但不复制 direct query API
- `tqsdk-task`
  - `TaskHost`
  - `TargetPosTask`
  - `TargetPosScheduler`
  - typed order builder / pre-trade risk gate
  - execution group foundation
  - account group foundation
  - strategy host / strategy context / strategy environment / deployment / supervisor adapter
  - supervisor typed health/metrics/shutdown report 和 telemetry/export hook；生产观测导出保持
    transport-neutral，不内置 GUI、web helper 或 HTTP health/metrics endpoint
  - strategy replay foundation with task-owned replay market source
  - streaming backtest market source trait 和 cached tick heap-merge replay stream
  - Python-compatible local backtest sim foundation
  - S31 低延迟 trading desk thin profile，使用 shared `SessionClient` +
    `RuntimeReader` hot path、task 层 `RiskEngine` / `TaskOrderIntent` 和 typed
    latency/order status report；慢日志、WAL、journal、落盘重试、audit sidecar
    和跨进程恢复由调用方或上层服务拥有，`TradingDeskProfile` 不持有 sink、WAL、
    journal 或 cache writer
  - public fake market / fake broker test harness
  - ownership / guarded order / execution report（事件流 + 聚合摘要）
- `tqsdk-data`
  - research/offline data crate
  - `DataClient`
- `BacktestHistoryClient`：metadata sidecar、source planner、single-flight official fill、统一的
  tick/minute/daily batch/concurrency/timeout/progress 调度、bounded async scan 与 query terminal report
  的唯一 owner；progress telemetry 同时发布累计接收行数和最新已接受源行 cursor，供 CLI 在流式阶段只推进已完整结束的交易日；coverage 仅在成功 terminal 后提交；tick 服务 tick 与 `<60s`，canonical minute 服务 `60s..<1d`，native daily 服务
  `1d..=28d`，daily miss 不回退到 minute；公开
  `RemoteOnMiss` run 自动持有 shared cache-root gate，facade 已持锁时传递同一守卫，避免嵌套自锁
- Universe Language V2 的 parser、normalized AST、纯 snapshot/timeline compiler、typed exclusion、
  legacy-first dispatcher 和外部 symbol file identity；Universe 只选择 instrument，不选择数据流
- `HistoricalUniversePlanArtifact` 的受控 flat v1–v5 reader/verifier、content-addressed store 与 V4 → V5
  source-preserving migration；normal writer/read path 使用 private-field、固定-wire V5，不修改旧 public V1–V3 plan
  - `query_his_cont_quotes`
  - `query_his_cont_underlyings`
  - `query_his_cont_underlying_segments`
  - `query_trading_calendar`
  - `query_trading_days`
  - `historical_cont_underlying_segments`
  - `HistoricalContQuotesRow`
  - `HistoricalContUnderlyingRow`
  - `HistoricalContUnderlyingSegment`
  - `TradingCalendarRow`
  - history page/series/download and CSV export substrate
  - history integrity report for owned kline/tick series
- TQBN daily v3 (`.tqbn`) 当前默认和 canonical 格式，按交易日分区存储
  - `HistorySeriesCache` public facade、crate 内部 store adapter、embedded final coverage commit、
    open-day provisional checkpoint、tick-only `BacktestTickCache` 和纯数据层
    `LiveTickCacheWriter`
  - 8 MiB 目标 records block、crate-internal `TQRI` 时间索引和按范围选择性解压；旧/不匹配索引
    逐 block 回退
  - 每日 TQBN 分区使用独立 `.tqbn.lock`，首次文件原子发布；tail checkpoint 记录确认长度、尾部
    checksum 和 coverage head。reader 固定 opened-file snapshot，只消费确认前缀，writer 可截断未确认坏后缀
  - `LiveTickCacheWriter` 合并连续单 tick push，并用显式 `flush()` / Drop 提交不足一批的尾部
  - 旧 `.tqseries` 和旧单文件 `.tqbn` layout 不是默认 backend，也不提供兼容读取或迁移 store
  - shared futures universe selector parser / resolver，relay 和 facade backtest 复用同一套语义
  - history page/series/download/export foundation
- `MinuteKlineCache` 是与 TQBN 并列的 canonical final-60s K 线 store：v5 格式按 logical
  symbol × trading month 分区，row payload 仅在 zstd 更小时无损压缩；目录名保持
  `minute-kline-v3`。v4 只能经显式备份迁移，v3 保持 `LegacyUnsupported`；不存在 automatic
  retention/max-byte eviction 或后台清理
  - `DailyKlineCache` 是 native final-1d K 线 store：v1 格式为 `daily-kline-v1/<escaped-symbol>.tqdk`，
  一个 logical symbol 一个原子替换文件、无时间分区。仅 official server-backtest terminal 1d rows 可写 final
  coverage；snapshot 变化只有 retained metadata sidecar 对既有 coverage 的 calendar/session/trading-day/
  physical mapping 全部证明一致时才能读旧行，并在 new-gap write 的 symbol lock 内原子 reheader；缺 sidecar、
  checksum/schema/snapshot 错误都 fail closed，结算价与涨跌停价未支持
- `tqsdk-cache`
- 可选 operator CLI；以 `--kind tick|minute|daily|all`（默认 tick）管理 canonical daily TQBN
  tick cache、canonical final-60s minute cache 与 native final-1d single-file cache。`all` 的 inventory /
  doctor 同时包含三类缓存；daily 还支持 inspect/fill/verify/purge，且只通过官方 native `1d` chart补洞，
  不从 tick/minute 聚合
- 历史 universe 的 `provider_unavailable` 使用独立 content-addressed retry receipt 维护；
  `refresh-provider-membership` 以 pinned acquisition、小批 due probes、stable-roster/cutoff
  revalidation 和 remote canary 处理，不改写原 proof，也不隐式生成 plan
- tick 保留 remote-on-miss / current-day provisional / `--require-final`、calendar-aware
   `--last-trading-days` 等合同；三类 fill 共享 batch/concurrency/timeout defaults 和 selectable stderr
progress（plain/TTY/JSONL）。新 fill report 统一写 schema v4 与 `cache_kind`，默认目录为
   `reports/tick/`、`reports/minute/`、`reports/daily/`；reader 兼容 tick v1/v2、minute v1、daily v1
 - tick purge 删除相交 trading-day partitions，minute purge 删除相交整月，daily purge 删除整 symbol
   文件；真实 purge 均要求 `--yes` 与 exclusive root gate，三类缓存都没有自动 eviction/retention
  - 普通 fill/query 取得 shared root gate；refresh、stale repair、verify、doctor 和真实 purge 取得
    exclusive root gate，冲突以退出码 75 / `cache_busy` fail fast。`inventory` 与 purge dry-run 不取稳定视图锁
- generic trading-calendar snapshot 只用于日期 selector 和进度分母；TQBN coverage、CST `18:00`
  partition 与 CacheOnly verification 仍是完整性权威
- historical fill 保留 legacy `physical:all` / legacy timeline 的 V3 路径；V2
  `timeline(...)` 默认发布 V5 并随后按 `--kind` 执行 targets。旧
  `v4-with-v3-rollback` token 仅作隐藏兼容；V4 artifact 先验证完整 V4/V3 chain 后迁移为 V5
- V2 timeline 在全量 provider discovery 后、native-daily membership bootstrap 前由 `tqsdk-data`
  计算 scoped physical closure；contract exclusion 不产生日线请求，仍保留的 derived view 则保留其
  underlying。完整 discovery 与 scoped proof 都是 immutable audit artifact
- 复用 `tqsdk` facade / `tqsdk-data` store，不定义或拥有任何缓存格式、session、状态树、live
  recording loop、回测推进或 relay 服务；不进入 Cargo default-members
- `tqsdk-relay`
  - 可选 market relay / cache service
  - 不改变 SDK 默认直连路径，不代理 trade/query/auth
  - relay 内部可用 metadata 查询动态发现当前活跃期货合约集合，按批调用
    `query_symbol_info` 获取 typed 字段，使用 `exchange_id`、`product_id`、
    `expired` 过滤合约，并使用 `trading_time` 判断合约交易时间段；不向下游代理
    query/auth
  - 产品发现可选择只保留每品种主力合约，或每品种活跃度前 N 合约：主力来自
    `query_cont_quotes`，N 大于 1 时其余按 quote `open_interest` / `volume` 排名补足
  - relay 只接受当前 snapshot；推荐显式写
    `snapshot(main:all;continuous:all;index:all;!CFFEX.*)`。`timeline(...)` 在 resolver/WebSocket
    之前失败，不在 relay 内产生第二套历史 membership
  - external exact-symbol 文件在 DSL 外通过 typed file setter 或
    `TQSDK_RELAY_FUTURES_UNIVERSE_FILES` 传入；V2 不接受 `file:`，刷新读取失败保留
    last-known-good 上游订阅
  - shared universe resolver 在最终集合中剔除当前不受本地 history cache / relay 支持的 `KQD`
    外盘合约；因此 `index` / `cont` 不会分别合成不存在的 `KQ.i@KQD.*` /
    `KQ.m@KQD.*` 连续代码
  - metadata 查询按批执行，避免产品发现自身制造过大的单次 query 负载
  - 产品发现模式默认按本地每日固定时间刷新合约集合，并在连接上游前检查 `ins_list`
    长度阈值；启动时上游先发送累计 quote 订阅用于首样本，
    quote update 会转成本地合成 tick 驱动 tick ring 和固定周期 K 线，
    只有下游 chart 或未覆盖合约需要真实 tick chart 时才动态补发每合约 tick chart；
    检查口径取这些上游命令中的最大 `ins_list` 长度
  - 提供 dry-run 启动自检、结构化启动日志、HTTP `/health`、`/metrics`、
    `/symbol-metrics` 和内置只读 `/dashboard`；`/health` 区分进程/下游监听、
    上游连接、订阅/补历史阶段、合约集合刷新和数据 freshness；dashboard 和
    `/symbol-metrics` 读取低频缓存的 read model，不在请求链路上获取 relay engine 全局锁
  - 新 K 线订阅可用内存 tick ring 回放已闭合的合成 K 线，减少冷启动空窗
  - 现有 SDK crates 不依赖 relay；用户显式配置 market endpoint 时才使用
  - relay 仍保留为 workspace member，但不属于 Cargo default-members；SDK 默认
    validation set 不含 relay，relay Rust 和 dashboard gate 单独运行

这两层当前仍然遵守同一个约束：

- 不反向修改 `tqsdk-core` 的 runtime contract
- 不在 facade 层复制第二棵状态树
- direct query 不重新塞回 `tqsdk-wait`
- `tqsdk-task` 拥有 deterministic replay / backtest 输入类型，并可以从
  `tqsdk-data` 的 history series rows 构建 replay source；这是上层集成路径，
  不代表 JSONL cache storage 进入 data public surface，也不代表 strategy
  execution 进入 data
- `tqsdk-task` 可以在 task/data 上层组合 `backtest::StrategyBacktest + sim::TqSim`，提供
  Python-compatible 本地回测模拟账户最小闭环；这不改变 core/session/wait
  的 runtime contract 和 facade 边界
- `tqsdk` 的 local replay / cache-backed backtest facade 可以复用同一套 `TargetPos` wrapper 驱动
  `backtest::StrategyBacktest + sim::TqSim`；策略主体仍只依赖 `Tq::next()`、quote/position refs
  和 `TargetPos`，不会创建 facade 私有状态树
- `tqsdk` 的 `.backtest(...)` 是 Python 心智入口：默认走共享 history cache-backed
  本地撮合，显式 `.disabled_cache()` 走官方远端行情；`local_backtest` 不再是独立用户概念；持久缓存、覆盖检查和
  universe 解析归 data/task 内部能力承接
- `tqsdk` 的 shared market cache policy 只是默认 facade 组合入口：live session/订阅由 facade
  拥有，tick rows/coverage 仍写入 `tqsdk-data::BacktestTickCache`，本地回测仍由
  `tqsdk-task` / `TqSim` 消费；它不新增后台守护进程、第二套状态树或 data-owned live session
- S31 trading desk profile 是 task 层的薄执行 profile，但 hot path 固定在
  `tqsdk-session + RuntimeReader`；它不进入 `tqsdk-data`，也不把 durable sidecar
  变成 task profile 的 public dependency。

Universe Language V2 的 `except(...)` 是 `tqsdk-data` parser 输入糖：`except(view:...)` 保持 view scope，`except(all:...)` 产生 global filter；二者均归一为既有 `!` AST，不能改变 artifact identity。

## API 归属总表

为了避免后续实现时再次把“一次性 direct query”误塞进 `wait` 或自建消费层，当前架构采用下面这条硬边界：

- `tqsdk-session` 负责所有一次性 request/response 接口
- `tqsdk-wait` 只负责 single-owner diff-backed 持续状态消费接口

| 接口类别 | 应归属的 crate | 原因 |
| :--- | :--- | :--- |
| GraphQL / HTTP query | `tqsdk-session` | 一次 `await` 请求/响应，不依赖 `wait_update()` |
| schema refresh / fetch | `tqsdk-session` | 一次性拉取/刷新，不是持续变化对象 |
| 合约元数据查询 / `SymbolInfo` / `InstrumentSpec` 标准化 | `tqsdk-session` | 属于 direct query / metadata，不需要模式化消费 |
| 交易日历 | `tqsdk-session` | 一次性结果，不应绑定某种 diff 消费形状；`TradingCalendarDay.date` 是 typed `NaiveDate` |
| `SymbolSettlement` / `SymbolRanking` / 其他 metadata query | `tqsdk-session` | 都是 query 结果，不是 live object |
| session 内订单 intent ledger | `tqsdk-session` | 是 shared session substrate，帮助 wait/task 和自建消费层复用同一 client order id 去重语义，但不拥有 live order object |
| 低层行情命令 helper | `tqsdk-session` | 是一次性 runtime command submission，不拥有 live quote object 或消费循环 |
| consumer fan-out capacity / lag diagnostics / health status | 调用方自建消费层 | 属于具体 consumer/channel 状态，不下沉到 core/session |
| `quote` / `trading_status` | `tqsdk-wait`；自建消费层可用 `RuntimeReader` | 返回持续变化对象，依赖 commit 持续推进 |
| `kline` / `tick` | `tqsdk-wait`；自建消费层可用 `RuntimeReader` | 返回持续更新窗口，依赖后续 diff |
| `account` / `position` / `order` / `trade` | `tqsdk-wait`；自建消费层可用 `RuntimeReader` | 读取的是同一棵状态树中的 live 对象 |
| `insert_order` / `cancel_order` / `confirm_settlement` | `tqsdk-wait` / `tqsdk-task` / `tqsdk-session` helper | 属于 trade diff-backed 消费语义或 task 执行语义 |

对用户形态的含义也应明确：

- `tqsdk-session` 不是只给 facade 内部用，用户也可以直接使用它来做 direct query / schema / metadata 访问
- 对性能极致敏感、希望自己掌控 cursor/commit 驱动的用户，也可以直接使用 `tqsdk-session::SessionClient + progress_once() + RuntimeReader`
- `tqsdk-wait` 即便提供 `session()` 访问底层 session，也只是复用路径，不改变 direct query 的 crate 归属
- 高并发、多消费者、事件流场景不再有内置 fan-out crate；调用方应基于 `RuntimeReader::cursor()` / `RuntimeReader::next()` 自建 fan-out
- 对性能极致敏感的用户，仍然可以直接使用 `tqsdk-core + tqsdk-session`

在 `tqsdk-session` 这一层里，建议再按“薄包装 vs 高层研究工具”继续收一刀：

- 应当进入 `tqsdk-session` 的 thin wrapper：
  - `query_symbol_info`
  - `query_instrument_specs`
  - `query_quotes`
  - `query_cont_quotes`
  - `query_options`
  - `query_atm_options`
  - `query_all_level_options`
  - `query_all_level_finance_options`
  - `get_trading_calendar`
  - `query_symbol_settlement`
  - `query_symbol_ranking`
  - `query_edb_data`
- 不应进入 `tqsdk-session` 的高层派生接口：
  - `query_his_cont_quotes`
  - `query_option_greeks`
  - DataFrame / polars 形状兼容层

原因也很简单：

- 前一组仍然只是“远端请求 -> 一次性结果”的薄包装
- 后一组已经开始包含研究工作流、衍生计算或 tabular/view 语义

## 参考仓库的使用方式
- `tqsdk-python` 是语义基准
  - 尤其是提交边界、对象一致性、初始截面、命令可见性、回放推进这些语义
- 现有 `tqsdk-rs` 适合参考工程经验
  - actor 化 I/O
  - market/trade/replay 分层
  - runtime 复用思路
- 但新的 V1 不应直接继承现有 `tqsdk-rs` 的宽 public surface

## 文档分工
本目录按“总架构 / diff core / runtime contract / future adapters / 验收矩阵”组织。

| 主题 | 当前落点 |
| :--- | :--- |
| 仓库级文档职责与权威层级 | [../README.md](../README.md) |
| AI 工作流与架构守则 | [ai-workflow.md](ai-workflow.md) |
| 总架构、阶段边界、路线图 | [README.md](README.md)、[roadmap.md](roadmap.md) |
| 当前 workspace crate 边界审计 | [crate-boundaries.md](crate-boundaries.md) |
| 未来 crate 蓝图与能力映射 | [crate-blueprint.md](crate-blueprint.md) |
| DIFF 协议的纯 merge 语义 | [diff-core.md](diff-core.md) |
| market DIFF、Quote/Tick 字段与实时性口径 | [market-diff-quote-tick.md](market-diff-quote-tick.md) |
| runtime contract：命令、状态、commit、cursor、adapter | [runtime-core/overview.md](runtime-core/overview.md)、[runtime-core/modules.md](runtime-core/modules.md)、[runtime-core/protocol-flow.md](runtime-core/protocol-flow.md)、[runtime-core/data-contracts.md](runtime-core/data-contracts.md)、[runtime-core/type-system.md](runtime-core/type-system.md)、[runtime-core/session-auth.md](runtime-core/session-auth.md) |
| Python / Rust facade 范式对比 | [facade-paradigms.md](facade-paradigms.md) |
| `wait_update` facade | [api-wait.md](api-wait.md) |
| task facade / execution tool | [api-task.md](api-task.md) |
| data facade / research tooling | [api-data.md](api-data.md) |
| relay 本地 CacheOnly history 架构 ADR | [history-relay.md](history-relay.md) |
| relay history HTTP v1 wire contract | [history-relay-http.md](history-relay-http.md) |
| history snapshot manifest / publish / lease contract | [history-snapshot-manifest.md](history-snapshot-manifest.md) |
| Universe Language V2：snapshot/timeline、typed scope、规范化和入口能力 | [universe-language.md](universe-language.md) |
| 历史 universe provider 数据 membership proof / pinned execution / artifact contract | [historical-universe-catalog.md](historical-universe-catalog.md) |
| 回测 tick / canonical-minute 缓存 CLI | [backtest-tick-cache-cli.md](backtest-tick-cache-cli.md) |
| 回测 Tick 持久缓存预热、检查和严格本地回放 | [backtest-tick-cache-operations.md](backtest-tick-cache-operations.md) |
| 未来 facade / adapter 的验收基线 | [validation.md](validation.md) |
| 场景审查和 public API disposition 输入 | [../reviews/README.md](../reviews/README.md) |

## 建议的概念分层
1. `diff-core`
   - 只负责天勤 DIFF 协议的理解、递归合并与 mutation 归一化
   - 不关心 session、不关心 facade
2. `runtime-contract`
   - 负责统一所有协议域的命令、状态、提交、revision、cursor
   - 是 V1 唯一 canonical public contract
3. `protocol-adapters`
   - 将 market diff、trade、query/schema、replay、system 接入同一个 runtime
   - 只负责编解码与 mutation 归一化
   - 没有提交权
4. `shared session layer`
   - 负责会话生命周期、query/schema/direct-query 封装，以及后续 facade 共享的 session 入口
   - 是 `wait` facade 和自建消费层之前的薄层
5. `consumption facades`
   - `wait_update`
   - callback
   - 都只是消费 `RuntimeReader` / `UpdateCursor` 的后续适配层
6. `user facades`
   - `tqsdk::Tq`
   - `TqApi`
   - typed views
   - task/tooling

## 阅读顺序
1. [AI 工作流与架构守则](ai-workflow.md)
2. [diff-core](diff-core.md)
3. [Market DIFF、Quote 与 Tick](market-diff-quote-tick.md)
4. [runtime-core 总览](runtime-core/overview.md)
5. [Session/Auth](runtime-core/session-auth.md)
6. [协议交互](runtime-core/protocol-flow.md)
7. [模块清单](runtime-core/modules.md)
8. [数据契约](runtime-core/data-contracts.md)
9. [类型约束](runtime-core/type-system.md)
10. [Python / Rust facade 范式对比](facade-paradigms.md)
11. [当前 crate 边界审计](crate-boundaries.md)
12. [未来 crate 蓝图与能力映射](crate-blueprint.md)
13. [Relay 内置只读历史查询 ADR](history-relay.md)：默认读取 live cache 已提交进度，published snapshot 仅作兼容回滚
14. [Relay History HTTP v1 Contract](history-relay-http.md)
15. [History Snapshot Manifest v1](history-snapshot-manifest.md)
16. [Universe Language V2](universe-language.md)
17. [历史 Universe Catalog 与填充合同](historical-universe-catalog.md)
18. [验收与测试矩阵](validation.md)
19. [wait facade](api-wait.md)
20. [task facade](api-task.md)
21. [data facade](api-data.md)
22. [演进路线](roadmap.md)

## 依赖方向
```text
diff-core
    ^
    |
runtime-contract
    ^
    |
protocol-adapters
    ^
    |
shared session layer
    ^
    |
consumption facades
    ^
    |
user facades / tools
```

## 当前总判断
- 真正的可复用底层不是原始 WebSocket 客户端，也不是某一种用户 API
- 真正的可复用底层是：`统一命令模型 + 统一状态树 + 统一 commit/revision/change 模型 + reader-first 读契约`
- `tqsdk-session` 会先承接 shared session、direct query、schema / metadata 这类薄层职责
- `wait_update` 和 callback/fan-out 的差异只能体现在“怎么消费 commit / 怎么读取同一棵状态树”，不能体现在“怎么生成 commit”
- V1 的完成标准是 contract 完整，不是 facade 完整
