# 历史缓存锁粒度研究：Timeline 与 Tick fill 解耦

状态：研究建议，未修改锁实现、未调整正在运行的任务。
范围：本机受控缓存路径上的协作式文件锁；不扩展为分布式锁框架。

## 结论

推荐第一版仅调整 Timeline：**根生命周期共享锁 + 所需指数分钟月分区共享 pin + 产品发布独占锁**。
复用现有锁文件，不新增 Tick/分钟全局锁，不引入数据库、
通用锁管理器或整套 MVCC。Timeline 热路径的时间差/shift 仍只读内存，无文件锁。

不能只把 Timeline 根锁从 exclusive 改成 shared；必须同时补齐请求级分区 pin，
以及发布锁外 load_active/merge 所带来的并发丢更新风险。

## 当前事实与定位

现场只读 `lslocks` 显示进程 295575 的 Tick fill 在
`.tqsdk-cache-operation.lock` 上持有 READ，即共享锁，并非全程根独占。
`TradingTimelineStore::rebuild_from_cache` 的 audit 和 activate 均要求根独占，
所以与任何普通 fill 互斥。此前“Tick 持有大锁”的描述不够精确。

| 现有机制 | 代码入口 | 实际责任 |
| --- | --- | --- |
| 根共享/独占 gate | `backtest_tick_cache.rs` 的 `try_acquire_remote_fill_shared_lock`、`try_acquire_consistency_read_lock` | 普通 fill 共享；稳定全局检查、破坏性操作独占 |
| family × cache_symbol 独占 lease | `backtest_history/fill.rs` 的 `SeriesFillLease`、`run_shared_fill` | 防同一序列重复远端 fill；Tick/minute/daily 分目录 |
| symbol × trading_month 文件锁 | `minute_kline_cache.rs` 的 `MonthFileLock` | 分区写入、替换、清理及读取协调，公开底层写入口也使用 |
| 每逻辑 symbol 的 metadata 锁 | `backtest_history/metadata.rs` 的 `MetadataLock` | immutable snapshot 与 active 指针的短时读写 |
| 每产品 publish.lock | `trading_timeline.rs` 的 `publish` | 当前保护 generation/active 写入，但未覆盖之前的 load/merge |

`resolve_minute_cache_metadata_snapshot` 会结合分区 header 在 active/retained
metadata 中找兼容版本。故不能理解成“只固定 active.json 内容即可”。
必须先稳定全部目标分区，再执行元数据解析并固定本次选中的 immutable snapshot/hash。

普通 fill 的 metadata 准备与 series lease 不是统一的大事务。
此外直接使用 `MinuteKlineCache` 的合法写入口只需月锁，不必经过 `SeriesFillLease`。
因此只新增 series 共享读锁并不能证明所有写入路径被排除。

现有月 reader 在打开数据文件后即释放 sidecar 共享锁。已打开的旧文件描述符
可支持原子替换后的旧文件读取，但不能单独证明跨多个月的数据集身份一致。
请求级 pin 是额外持有**同一组既有 sidecar 锁**，而非改成逐行加锁。

## 推荐流程

1. 取得既有根 gate 的共享锁，贯穿请求；继续排除根级 refresh/repair/迁移等维护。
2. 由请求实际交易日范围计算需要读取的 `(canonical index symbol, trading_month)`。
   使用现有分月函数，包括首个交易日所属的前一自然日/月，不手写月份边界。
3. 去重、稳定排序，非阻塞取得这些既有 `.tqmk.lock` 的共享锁。任一失败则释放已取锁，
   返回具体资源的 busy；可在 CLI 现有有界等待/取消机制中整体重试。
4. 全部 pin 到手后解析并固定每产品 metadata snapshot；验证完整 final coverage，
   扫描并生成候选。metadata 短锁只保护加载，不持有到全年扫描结束。
5. 验证所有候选后，按稳定产品顺序取得 publish 独占锁；**锁内重新读取 active，
   merge、验证、写 generation 并替换 active**，保持现有 fsync/错误语义。
6. 分区 pin 和根共享 gate 持有至候选发布完成；释放全部句柄。

产品发布仍是逐产品事务，不声称多个产品 crash-atomic。保留现有“候选全部验证后
才开始更新指针”的语义；不要顺便改成遇到一个产品成功就提前发布。
审计无 activate 时不需要产品发布锁。

首版只做 crate-private pin helper，归 `tqsdk-data`，不让 CLI 拼锁路径，也不
扩大公开 API。只读路径不得创建目录/锁文件；缺少 sidecar 时明确失败，使用已有
维护流程准备它，不能静默降级。锁文件长期保留，不删除重建。

## 并发矩阵

| 与 Timeline 并发的任务 | 预期 |
| --- | --- |
| Tick 普通 fill（包括同名指数） | 可以；family 文件不同，根共享兼容 |
| Daily 普通 fill | 可以 |
| 其他 symbol 的分钟 fill | 可以 |
| 同 symbol、完全不相交月份的分钟 fill | 数据锁层面允许；共同 metadata 更新仍须固定兼容快照 |
| 相同 symbol/month 的分钟提交或底层直接写入 | 互斥；只挡受影响分区，不挡全缓存 |
| 根级破坏性维护或全局一致性检查 | 保留互斥，第一版不收窄其安全边界 |
| 同产品两个 Timeline 发布 | 产品锁串行化 read/merge/write，防丢更新 |
| 已加载 Timeline 的策略时间差/shift | 不需要这些锁或磁盘访问 |

## 为什么不选其他方案

| 方案 | 取舍 |
| --- | --- |
| 维持根独占 | 安全但无关 fill 仍阻塞，不解决问题 |
| Tick/minute/daily 各一把大锁 | 分钟任意品种仍会阻塞 Timeline；新增旧进程不认识的协议 |
| 仅复用 series lease 的共享模式 | 很有复用价值，但漏掉底层公开月分区写接口，不能单独作为稳定视图证明 |
| 请求级既有月锁 pin | 推荐；覆盖既有合法 writer，粒度足够且协议增量最小 |
| 每日/每行锁、重新设计 range lock | 文件数量、死锁和管理成本大，当前没有必要 |
| 乐观双读 hash 或全量 copy/MVCC | 双读不能替代多文件稳定视图；重扫/copy增加磁盘成本，代际管理扩大范围 |

## 性能及限制

2024-01 至 2026-09 共 33 月：IM/CF 两个产品约 66 个月锁句柄，
另有少量根、发布和读取句柄。资源开销按目标分区数增长，不按 K 线行数增长。
不读取其他合约、不复制缓存、不为每行调用 flock、不引入周期轮询热路径。
磁盘扫描仍由原 build 决定；本研究没有执行吞吐或冷盘基准。

多产品/超长年份请求须在拿锁前检查分区/FD预算；超过预算明确失败，或由显式批次
请求拆分，不能中途丢弃 pin 后继续声称单次稳定视图。不要顺便重构全部 reader。
同分区写入会等待 Timeline 扫描结束，这是保证稳定性的必要代价；若实测过长，
再考虑每产品批处理或 immutable generation，不先引入复杂设计。

`cache_busy` 应说明 root、minute partition 或 product publication 等资源类型，
CLI 可有截止时间和取消；不要靠删除锁文件、轮询 PID 或杀进程判断锁安全。

## 系统语义与兼容性

Linux flock 为协作式锁；共享兼容、独占排他，锁与打开文件描述关联。
不得依赖 shared→exclusive 升级是原子的，也不能通过更换锁文件绕过已持有的 inode。
NFS/SMB 行为不同，本方案先限已有本机文件系统部署。
参见 [flock(2)](https://man7.org/linux/man-pages/man2/flock.2.html)。

原子 rename 不会让已打开文件描述符切到新 inode，也不等于多文件事务；
持久化还需文件及父目录 fsync。保留现有发布实现的 durability-uncertain 语义。
参见 [rename(2)](https://man7.org/linux/man-pages/man2/rename.2.html)、
[fsync(2)](https://man7.org/linux/man-pages/man2/fsync.2.html)。

复用已有 root/月分区锁，使遵守现有锁协议的旧 fill 进程自然互斥；不要求把
正在运行的普通 Tick fill 停掉。旧 Timeline 仍拿根独占，只会保守阻塞。
这不是对任意外部写文件程序的安全保证；部署前须验证实际运行版本也走同一锁路径。

## 实现前验证清单

独立只读架构审查同意该最小方案，并将两点列为 HIGH 级实施门禁：
必须先固定全部月分区再做 metadata/coverage 检查；产品锁必须覆盖 load/merge/publish。
审查特别确认 `open_reader` 的初始 coverage 检查也不能位于请求 pin 之前。
旧版本兼容仅限遵守既有锁协议的 binary，不承诺新旧 binary 长期任意混跑；
不认识月锁的旧 writer 必须排空后升级。

- 真实多进程：Tick 持根共享时 Timeline 成功，不依赖睡眠来制造确定性。
- 同一指数月分区 writer、直接 `MinuteKlineCache` writer 与 Timeline 正确互斥。
- 不同 symbol/month 的 writer 不被无关 pin 阻塞。
- metadata active 在扫描期间切换：本请求固定兼容 hash，不产生混合 identity。
- 同产品两个增量发布同时完成：全部日期保留，无覆盖丢更新。
- 取得多个锁中途失败、取消、panic/进程退出：没有残留锁或半更新 active。
- 缺 sidecar、缺 coverage、provisional、metadata mismatch 仍失败，不伪装成休市。
- 根独占维护仍与新 Timeline 互斥；只读 bind mount 不产生写入。
- 多产品候选失败时不开始发布；fsync 故障保留既有可恢复/不确定状态语义。
- 本地 fixture 测 lock 开销、最大 FD、扫描次数及共存时延，另测热路径无新增 I/O。

这是并发/持久化契约变更，实施前须做符号 impact analysis 和独立审查；更新
TradingTimeline、history cache 的架构文档与测试。当前研究不授权直接上线修改。
