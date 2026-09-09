# Runtime / Resource / Throughput Audit Follow-up — 2026-09-08

## 复核结论

外部评审的优先级正确：先 runtime correctness，再资源边界，最后吞吐。
本轮实现和复核已关闭所有列出的 P0 正确性/稳定性问题，并为 P1/P2 的
资源与吞吐项补齐可执行实现、回归测试和观测面。这里不把 microbenchmark
当作正确性证明。

真实 TQBN cache 已在默认 `tqsdk-cache` 路径以只读方式完成 1 日、1 月、6 月
矩阵。为避免把 materialized 对照路径或 benchmark 自身的逐行延迟样本误归因给
streaming reader，真实语料 case 强制 `TQSDK_HISTORY_CACHE_BENCH_STREAM_ONLY=1`，
并将延迟 reservoir 固定为 16,384 个样本。synthetic quick 仍只证明线路可运行，
不能替代该真实语料结果。

本文件记录审计 disposition；crate ownership 和 runtime contract 仍以
`docs/architecture/*` 为准。

## 验收矩阵

| 外审主题 | 复核结果与实现证据 |
| --- | --- |
| exact-revision read | `RuntimeReader::next_view()` 通过 `CommitLog::view_at()` 返回与 commit 同 revision 的 `StateSnapshot`；不会用 head snapshot 冒充历史 revision。`StateSnapshot` 以 immutable/COW partition roots 持有版本。连续发布、lag 和 resync 由 runtime contract tests 覆盖。 |
| CommitLog 资源边界 | `CommitLogRetention` 同时 hard-bound `max_entries` 和 `max_retained_bytes`；超出时 trim，落后的 cursor 显式 `CursorLagged`，可选择显式 lossy resync 或从持久 checkpoint 重建。telemetry 包含 `lag_revisions`、`lag_bytes`、`last_advance_at`；cursor 不再 pin retention。 |
| 多 partition 锁 | `StateRootSet` 是唯一多 root 获取入口，按 `StateRoot::ALL` 全局顺序取锁；Trade/Other 反转被 invariant 和并发回归覆盖。读侧 telemetry 在 guard 释放后记账，不延长临界区。 |
| Relay WebSocket | relay 使用 `yawc` 的有状态 decoder 加持久接收 buffer；保留应用层 fragmentation/control-frame 状态。握手、header、frame、message、连接数和写入超时均有上限；半帧输入与 outbound-ready 交错由 server tests 覆盖。 |
| StateStore / market hot path | revision snapshot 共享 partition roots，通常读路径的 `bytes_cloned` 和 `nodes_cloned` 为零；市场/交易热读可走 partition guard。保留 market-only multi-root fast path，并输出 snapshot/lock telemetry。协议兼容边界仍使用 `serde_json::Value`，未在无 profiling 证据时盲目进行全 SDK SoA 或 field-id 重构。 |
| Relay fan-out、cache、Kline | quote fan-out 先共享 `Arc<Value>`，server 以 payload identity 一次 JSON 编码为 `Arc<[u8]>`，多 client 共用同一 WebSocket frame。market mailbox 是 latest-wins，可靠事件队列满时淘汰慢 client 而不静默覆盖。cache 使用固定容量 ring、symbol/byte hard admission；Kline internal key 只含 `(symbol, duration)`，不受 `view_width` 影响，首次订阅从 borrowed ring 重建而不 clone 整 ring。 |
| TQBN / history | `TickDeltaCursor` 按行流式 decode；records index v2 带 first/last id、时间范围与单调性，减少规划阶段重复 decode。reader telemetry 包含 bytes、blocks、materialization、I/O/decode 时间。非单调 legacy tick 使用 reader-owned SQLite + fixed binary spill 做 bounded canonicalization，保留 IEEE float bits，完成后删除临时目录；不再把整 partition 物化为 `Vec<HistorySeriesRow>`。 |
| Backtest replay / shared scan / worker | N-way merge 维持 `BinaryHeap`；hot event metadata 使用 `Arc<str>`，提供 sync iterator 与有上限 batch API。shared scan 有每订阅者 bounded cursor、late join 和命中/重复扫描 telemetry；blocking scan 受 local + optional shared permits、byte budget 和 cancellation wakeup 控制，并拆分 I/O/decode 计时。 |
| 订单与风险状态 | prechecked order 的 intent 具备 abort/drop 回收，状态机覆盖 Prepared → Submitting → Submitted → Terminal。若部署要求 crash-safe hard risk，新增独立 `tqsdk-hard-risk` SQLite/WAL ledger，使用 policy CAS、不可 clone 的 consumed permit、scope/revision recovery 和 terminal cleanup；不改变 core/session 默认依赖。 |
| 订阅管理 | relay 上游信号 channel 有界并 coalesce，待发送 symbols 合并为 desired set；upstream 使用 declarative reconciliation，旧 subscription 不会以队列堆积方式覆盖新 universe。 |
| Universe、cache 一致性 | universe artifact identity 纳入 spec、metadata/catalog、resolved membership 与映射；CacheOnly 使用 snapshot metadata。TQBN 保留 checksum/checkpoint、跨进程 file lock 和 crash-recovery 测试；snapshot manifest 指向 immutable generation，lease pin 会阻止 GC 删除活跃 generation。 |
| 时间、序列化、API 边界 | relay tick 内部保留 integer ns，格式化 datetime 只在兼容输出边界且可复用；稳定 WebSocket payload 用 `serde_json::to_vec` 直接得到 shared bytes。core → session → wait → task/data/relay 的边界保持不变，relay 未进入默认 SDK 依赖。 |
| MSRV、CI、observability | workspace MSRV 统一为 Rust 1.88，并有真实 MSRV CI job；CI 覆盖 `cargo test --workspace`、cache crate 和 hard-risk crate。runtime、state、relay、TQBN、backtest 均有不在交易锁内记账的 telemetry。 |
| 性能基准 | `scripts/run_performance_matrix.py` 覆盖 wire→commit、commit→risk→submit、TQBN→merge→callback、upstream→relay fan-out，并记录 benchmark latency、events/s、子进程 CPU/RSS/I/O。full runner 覆盖 10/100/500/1000 合约或 client；`--history-corpus` 的真实 case 以 streaming-only 模式读取既有 cache，不写入输入 root。2026-09-09 的默认 cache 复测 14/14 通过：六个月 `KQ.i@SHFE.au` 读取 7,588,831 rows，3.97 M rows/s，`materialized_rows=0`，单子进程峰值 RSS 11.34 MiB。p50/p95/p99/p999 为 130/150/180/240 ns；这些是 per-row reader call latency 采样，而不是端到端交易延迟。 |

“改成 HashMap/HashSet”、“引入 SourceId/field-id”以及“全面 SoA”在外审中是
建议先 benchmark 的候选方案，不是已被证明的 correctness defect。当前实现保留
需要稳定排序的 `BTree*` 位置，避免在没有 profile 数据时改变协议确定性。

## 本轮验证

已通过：

```bash
git diff --check
cargo test -p tqsdk-core --test runtime_contract_reader_surface --test runtime_contract_runtime_core --quiet
cargo test -p tqsdk-relay --test server_ws --test cache --test performance_guards --quiet
cargo test -p tqsdk-data --lib tqbn_reader_spill --quiet
cargo test -p tqsdk-task --test trading_desk --test history_backtest_replay --quiet
cargo test -p tqsdk-hard-risk --quiet
```

此前同一工作树还通过了 workspace/all-features/no-default-features 检查、clippy、
docs、`cargo deny` 和 synthetic quick performance matrix。relay dashboard 的 Rust
build script 在未安装 `npm` 的环境会产生 warning，但不改变 Rust check/clippy/test
结果。

## 真实语料执行入口

```bash
python3 scripts/run_performance_matrix.py \
  --history-corpus /path/to/history-corpus.json \
  --output-dir /tmp/tqsdk-performance-matrix-real
```

manifest 应至少给出 one-day、one-month、six-months 三个 cache-backed case，并在
报告中按 TQBN decode、merge heap、strategy callback 和 socket write 分列。

### 2026-09-09 默认 cache 实测

执行 `python3 scripts/run_performance_matrix.py --history-corpus ...` 得到 14/14 passed。
真实 TQBN streaming-only case 读取 `/root/.tqsdk/data_series_1` 的
`KQ.i@SHFE.au`，不会创建、修改或 compact 输入 cache：

| 范围 | wall time | peak RSS | materialized rows |
| --- | ---: | ---: | ---: |
| 1 日 | 29.3 ms | 10.89 MiB | 0 |
| 1 月 | 328.3 ms | 11.23 MiB | 0 |
| 6 月（7,588,831 rows） | 1.93 s | 11.34 MiB | 0 |

该 RSS 是单 benchmark child 的 `getrusage(RUSAGE_CHILDREN)` 峰值，且已经排除
write/read materialized baseline；不是整个 benchmark process tree 的聚合 RSS。
