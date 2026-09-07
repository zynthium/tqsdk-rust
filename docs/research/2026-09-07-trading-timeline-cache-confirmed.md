# 2024 IM/CF 缓存确认闭环

日期：2026-09-07。操作者明确接受“依据缓存无成交确认例外”。
本记录取代这两个范围此前必须补齐交易所原文才能激活的阻断结论，
但不改变原文访问排查记录，也不把缓存推断改称官方事实。

## 范围与依据

- CFFEX.IM：2024-01-01 至 2024-12-31；20 个无成交工作日桶确认为休市。
- CZCE.CF：2024-02-06 至 2024-12-31；19 个休市日、6 个取消夜盘日。
- 共 45 个例外，全部标为 confirmed，authority.source_type 为 cache_inference；
  证据 URN 对应规范指数分钟缓存的逐日 evidence hash。
- 45 个 hash 均与本次重新读取缓存产生的 hash 一致。
- 只有完整 final coverage 可用；缺失、provisional、未知规则仍不能当作休市。
- 供应方遗漏及段内稀疏成交造成的边缘时刻误判是明确接受的风险。
  不能宣称仅凭无成交在逻辑上证明交易所休市。

新目录：
操作者维护的 cache-confirmed-2024 catalog。
原 draft/reviewed 不改为全量可激活；本次没有新增公共 API、存储格式或推断框架。

## 验证

从本机分钟缓存复制 IM/CF 的 24 个月文件、索引与元数据到
`/tmp/timeline-cache-confirmed.lpMnt7`，仅在此隔离目录发布 active。
生产缓存未写入。该临时目录不是持久化交付物。

| 产品 | 已决日桶 | 开放区间 | 完整重建与最后一天增量 |
| --- | ---: | ---: | --- |
| IM | 262 | 484 | timeline/evidence hash 一致 |
| CF | 236 | 862 | timeline/evidence hash 一致 |

实际调用 `TradingTimelineStore::open_read_only(...).load_active(...)`，
逐项验证下列 CST 端点之间为 900 交易秒；正向 shift 与反向 shift 均相等：

| 产品 | 起点 | 终点 |
| --- | --- | --- |
| IM | 2024-03-04 11:25 | 2024-03-04 13:10 |
| IM | 2024-03-08 14:55 | 2024-03-11 09:40 |
| IM | 2024-02-08 14:55 | 2024-02-19 09:40 |
| CF | 2024-02-08 14:55 | 2024-02-19 09:10 |
| CF | 2024-04-03 14:55 | 2024-04-08 09:10 |

CF 跨出 known range 的查询报错；并发 rebuild 发生竞争时返回 cache_busy，
串行重试成功。`cargo test -q -p tqsdk-data trading_timeline --lib`：20 passed。
机器审计附件与生成 hash 未随仓库保留。

本次没有执行远端 fill、真实账户访问、生产激活或 Docker 部署；
没有重新运行全 workspace 验证，也没有进行冷盘性能基准。

## 重现构建

在已准备好的隔离缓存目录执行，两个产品按各自范围分别处理：

```bash
target/debug/tqsdk-cache timeline \
  --cache-dir /tmp/timeline-cache-confirmed.lpMnt7 \
 --catalog /path/to/operator-catalog.json \
  --start-day 2024-01-01 --end-day 2024-12-31 \
  --symbol KQ.i@CFFEX.IM --apply --output-format json

target/debug/tqsdk-cache timeline \
  --cache-dir /tmp/timeline-cache-confirmed.lpMnt7 \
 --catalog /path/to/operator-catalog.json \
  --start-day 2024-02-06 --end-day 2024-12-31 \
  --symbol KQ.i@CZCE.CF --apply --output-format json
```

将每个请求范围改为 2024-12-31 单日，重建后的 hash 必须不变。
以后 fill 可使用现有 `--trading-timeline-catalog` 与 `--require-final` 钩子，
但这个目录不能覆盖 2025/2026 或其他品种；超出范围必须先复核并扩展目录。
新证据与例外冲突时保留旧 active、拒绝新候选，不能继续无条件标记 confirmed。
