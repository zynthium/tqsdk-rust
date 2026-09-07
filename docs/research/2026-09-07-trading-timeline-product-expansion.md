# 其他品种 Timeline 扩展：67 个已激活，8 个待修复

后续更新：8 个陈旧 9 月分区已补回，并发布了仅覆盖 2026-09-01～2026-09-04 的
Timeline；原有 67 个完整范围不变。下文保留首轮审计结果，不代表最新阻断原因。
详见[修复及范围限制](2026-09-07-trading-timeline-eight-product-repair.md)。

日期：2026-09-07。范围按操作者的 universe 排除清单筛选现有规范指数分钟缓存。
缓存中识别到 87 个指数，筛选后 75 个目标品种；这不是对未缓存产品不存在的权威证明。
IM、CF 已有生产 active，本轮审计其余 73 个，新增激活 65 个，8 个拒绝发布。
cron 与每日填充自动化尚未推进。

## 当前覆盖

所有成功品种均覆盖 2024-01-01～2026-09-04（最近已结束交易日），各 700 个已决日桶，
其中包括已确认休市日。已写入原缓存 `/root/.tqsdk/data_series_1`，不再是隔离副本。

| 交易所 | 已激活（含原 IM/CF） | 缓存中目标品种 | 阻断 |
| --- | ---: | ---: | --- |
| CFFEX | 8 | 8 | 无 |
| CZCE | 17 | 19 | PL、PR |
| DCE | 18 | 19 | bz |
| GFEX | 2 | 5 | pd、ps、pt |
| INE | 5 | 5 | 无 |
| SHFE | 17 | 19 | ad、op |
| 合计 | 67 | 75 | 8 |

每产品 active 路径：`trading-timeline-v1/products/<exchange>/<product>/active.json`。
已有进程须重新 `load_active` 才会使用新 generation。

## 规则与证据

候选来自缓存中的 official trading-time metadata，共六类：
股指日盘、国债日盘、商品纯日盘、商品日盘加 23:00/01:00/02:30 夜盘。
不是按物理行数计算时长，也没有把全部产品套成同一时段。

65 个品种均完成整个范围的 final coverage 审计；合计 4161 个例外，
按操作者批准的 cache_inference 口径记录逐品种 evidence hash。
有夜盘品种的缺夜盘候选在激活时再次验证：删除夜盘后无区间外正成交量，
且每个保留日盘窗口都存在正成交量；整日休市必须为完整 final coverage 内无正成交量。
缺 coverage、provisional、session identity mismatch 不会标为休市。
该推断口径不冒充交易所公告认证，也不保证识别供应方遗漏或段内稀疏引起的端点变化。

确认 catalog 按产品独立由操作者维护，
避免以后一个品种目录 hash 变化使其他品种全量重建。它们只使用现有 catalog/schema/CLI，
没有新增 Rust API 或推断框架。IM、CF 的既有目录与 active 保持不变。

## 验证结果

- 65 个新增产品在正常 root shared、分钟月 pin、产品发布锁下激活成功，每个 700 unique 日桶。
- 从原缓存实际加载每个 active，每产品 1000 对确定性伪随机墙钟端点与线性交集参照一致。
- 全部新产品跨年 forward/backward shift 一致；已知末端后 1ns 被拒绝。
- 65 个产品逐一做最后一天增量重建：700 日桶全部保留，timeline/evidence hash 均与完整重建一致。
- 原 IM、CF 各重新验证 10000 对端点，仍通过。
- JSON 目录经实际 CLI 解析及完整证据校验；`git diff --check` 通过。
- 本轮没有修改 Rust 实现、补数、修改行情文件、停止 Tick 任务、安装 cron、提交或部署。

机器可读逐产品路径、身份与原始阻断信息：
逐产品机器审计附件未随仓库保留。

## 剩余阻断与下一步

八个产品都在 `minute-kline-v3/trading-202609/KQ.i%40<exchange>.<product>.tqmk`
报 `calendar/session snapshot mismatch; refusing stale minute Kline cache`。
没有为它们生成可激活确认目录，没有重写 header 或放宽读取门禁。
修复后仍需重新核验真实覆盖及必要的上市/规则起点，不能把上市前或不完整范围直接确认成休市。

目前 Tick fill 进程 295575 仍持 root shared。普通 Timeline 审计/发布已经可以并行，
但 `repair-stale` 一类破坏性维护有意保留 root exclusive，因此需要等待任务结束，
或经操作者批准让任务优雅退出后安排维护窗口；不能用 SIGSTOP“暂停”来释放锁。
维护后再重新审计八个产品，确认并激活，然后继续每日缓存/Timeline/cron 自动化目标。
