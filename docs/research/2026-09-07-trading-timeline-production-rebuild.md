# 八品种分钟缓存与 Timeline 生产重建

状态：本轮八品种重建、严格发布与生产回读验收完成。Tick 保持停止。

## 验收范围

| 品种 | 补数起点（上市日） | Timeline 首个已确认交易日 | 重建分钟行数 |
| --- | --- | --- | ---: |
| CZCE.PL | 2025-07-22 | 2025-07-23 | 93,810 |
| CZCE.PR | 2024-08-30 | 2024-09-02 | 166,455 |
| DCE.bz | 2025-07-08 | 2025-07-09 | 97,260 |
| GFEX.pd | 2025-11-27 | 2025-11-28 | 42,300 |
| GFEX.ps | 2024-12-26 | 2024-12-27 | 92,250 |
| GFEX.pt | 2025-11-27 | 2025-11-28 | 42,300 |
| SHFE.ad | 2025-06-10 | 2025-06-11 | 139,920 |
| SHFE.op | 2025-09-10 | 2025-09-11 | 81,390 |

共 755,685 行，均补至最近已结束交易日 2026-09-04。
表中 Timeline 起点是交易日标签；准确墙钟边界以审计摘要的 `known_ranges` 纳秒值为准。
八个上市首日均缺少指数正证据，明确排除，不能返回零交易时长或声称自上市首日全部覆盖。

## 验证结果

- 八个 fill 均 final/complete；候选审计的所有范围内日桶均为 unique，known range 连续。
- 审计与发布的 catalog hash、timeline hash 一致；真实 `load_active` 回读 hash 也一致。
- 检查 8,661 个相邻交易区间间隙的纳秒级 duration/双向 shift。
- 各产品全范围总时长等于交易区间长度之和；全范围双向 shift 与 duration 相符。
- known range 起点前、终点后 1ns 均拒绝，不把未知时间当作休市。
- 八个产品使用操作者维护的正式 catalog。
- 用仓库正式 catalog 逐一重跑生产只读审计，八个 catalog/timeline hash 均与已发布结果一致。
- `git diff --check`、`cargo fmt --all --check` 通过。
- 本轮未修改 Rust 代码；沿用此前通过的代码回归，新增生产回读及文档一致性检查。

机器可读验收摘要未随仓库保留；本文记录精确边界、
证据 identity、catalog/timeline hash、日桶数及例外统计。

## 操作与安全边界

用户确认停止 Tick 后，宿主机仍发现旧 PID 3403170 持有 root shared gate。
核验旧可执行文件后发送 SIGINT，确认正常退出和锁释放；未强杀、未删除锁文件。

正式 purge 预演解析精确文件范围，逐文件备份并比对后顺序清理：
102 个目标指数分钟分区，共 588,382 字节。随后以修复后的二进制两路并发重建。
未修改其他品种的行情分区，也未修改 Tick/daily 文件。

非 unique 日只用 CacheOnly final 指数数据复核，沿用用户批准的缓存推断口径：
整日无正成交证据才确认范围内整日休市；取消夜盘要求夜盘无正证据且每个保留日盘段有证据。
前置无指数证据日期不确认为休市。推断不是交易所原文认证；未来冲突证据应阻止对应发布。

## 运维交接

运行目录：`/tmp/timeline-production-rebuild.NP4Yob/`。

- `backup/`：清理前分钟分区备份，可恢复；不是正常增量数据源。
- `rebuild.py`：已完成，**不要直接重跑**，它会再次 purge。
- `finalize.log`：八个 activated，最终 pending 为空。
- 各品种 `.fill.json`、`.reviewed-audit.json`、`.activation.json`：原始运行证据。
- `readback-all-eight.log` 与 `probe/`：生产回读结果及探针。

正式 catalog 与精简审计证据已入工作区；临时原始证据和备份暂未删除。
本轮没有提交、部署、安装 cron 或恢复 Tick。
旧 Tick/daily 的潜在错误 final coverage 未由本轮分钟重建证明消除；原有 67 个品种也没有
在本轮重新补数。既有消费者须重新 `load_active` 才能使用新 generation。
