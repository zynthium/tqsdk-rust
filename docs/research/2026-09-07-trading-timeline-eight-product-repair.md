# 8 品种分钟缓存修复与受限 Timeline 激活

日期：2026-09-07。操作结果为部分闭环，不能表述为全品种全历史完成。

## 已完成

- 原 Tick PID 295575 收到一次 SIGINT 后正常退出，没有强杀。
- 标准 `fill --repair-stale` 清除了判定陈旧的分钟分区；最初从 2024 年起补数失败。
  随后缩小到 2026-09-01～2026-09-04，8 个产品全部正常补回并通过 final coverage 校验。
  没有直接修改缓存头或绕过 snapshot/session identity 校验。
- CZCE.PL、CZCE.PR、DCE.bz、GFEX.pd、GFEX.ps、GFEX.pt、SHFE.ad、SHFE.op
  已发布这四个交易日的 Timeline，每个四个唯一决策日桶，无新增休市例外。
- 精确已知墙钟范围为 `[2026-08-31 18:00+08:00, 2026-09-04 18:00+08:00)`；
  早于已知范围的查询继续报未知，不能将上市前时期计为零交易时长。
- Tick 已恢复，记录时 PID 3403170，使用用户最新 universe（包括中金所）、
  `--start-day 2024-01-01 --require-final`，其余原有批次/并发/超时设置保留。
  凭据仅经进程内存传递，未写入脚本、报告或仓库。

现有 75 个缓存内目标产品均有 active Timeline：67 个保留完整的
2024-01-01～2026-09-04 范围；上述 8 个仅有 9 月 1～4 日。不是全历史 75/75。

## 剩余阻断及定位证据

从上市日起补数，PL、PR、bz、ps、ad、op 均复现
`server-backtest canonical-minute chart was ready without its page rows`。
PL 采用现有 `--daily-slices` 的完整范围验证仍失败，已优雅停止该验证，未继续无效重试。
小范围成功不能证明长范围分页已正确；目前不足以断定是服务端还是客户端就绪判断的问题。

pd、pt 从 2025-11-27 起补数报告完整，但各 202 个日桶只有 8 个唯一匹配、194 个 ambiguous。
因此没有把这些日桶批量转为休市，没有扩大它们的已知范围。
下一步需要定位空页面/分页与证据缺失原因，而非放宽 Timeline finality 或匹配门禁。

用于限定补数起点的上市日期：

| 产品 | 起点 | 来源 |
| --- | --- | --- |
| PL | 2025-07-22 | [郑商所公告转载，含原文链接](https://www.citicsf.com/e-futures/content/000509/816289)；本轮原文仍无法打开 |
| PR | 2024-08-30 | [郑商所 2024 年 8 月公告汇编](https://www.czce.com.cn/cn/rootfiles/2024/09/14/1726389999932285-1726389999951648.pdf) |
| bz | 2025-07-08 | [大商所 2025 年 7 月月刊](https://www.dce.com.cn/qhxy/file/2025-08-05/17543892971982c9a882b9879b789653019879c0382e003d.pdf) |
| pd / pt | 2025-11-27 | [广期所通知转载](https://www.cjfco.com.cn/main/a/20251124/81643.shtml) |
| ps | 2024-12-26 | [央视上市报道](https://jingji.cctv.com/2024/12/26/ARTIGzMJOU9ahENVPfpMw5CR241226.shtml) |
| ad | 2025-06-10 | [上期所上市专题](https://www.shfe.cn/index/othercontents/2025_AD/index.html) |
| op | 2025-09-10 | [上期所上市通知](https://www.shfe.com.cn/publicnotice/notice/202508/t20250818_828697.html) |

这些日期仅约束补数尝试，不替代 final 分钟证据，不用于生成上市前休市记录。

## 验证与运维交接

- 新增 8 个：公开 API 各 1,000 对随机墙钟时间差对比线性区间交集；双向位移；两侧未知边界拒绝，全部通过。
- 8 个末日增量 `timeline --apply` 保留四日范围及相同 timeline/evidence hash。
- 原有 65 个各 1,000 对、IM/CF 各 10,000 对公开 API 回归均通过，合计本轮 93,000 对。
- 未修改 Rust 代码、锁协议或 public API；架构文档仅更新生产状态。
- 所有本轮维护补数进程已退出，只保留恢复的 Tick 任务；没有安装 Cron 或自动无限重试。
- 运行日志暂存 `/tmp/timeline-eight-repair.WWnxYx/`，其寿命不作为审计保证；
机器审计与可激活 catalog 均由操作者离线保存。

下一步先定位六品种空页面和 pd/pt 历史证据异常，再决定历史补齐与 Cron 配置。
