# IM/CF TradingTimeline 生产激活闭环

2026-09-07，按操作者要求完成之前受缓存覆盖和根锁阻断的目标。

## 已生效范围

原缓存：`/root/.tqsdk/data_series_1`。
产品：`CFFEX.IM`、`CZCE.CF`；两者均覆盖 2024-01-01 至最近已结束交易日 2026-09-04。
已写入各产品 `trading-timeline-v1/products/<exchange>/<product>/active.json`，
不再仅是隔离副本。底层 known range 末端为 2026-09-04 18:00 CST；
不包含尚未结束的后续交易日，不代表其他品种或全市场已覆盖。

| 产品 | 已决日桶（含休市） | 开放区间 | 从 2024-01-01 00:00 到已知末端的交易秒 |
| --- | ---: | ---: | ---: |
| IM | 700 | 1298 | 9345600 |
| CF | 700 | 2578 | 13304700 |

使用操作者维护的 202609 cache-inference catalog。
例外可信度继续遵守操作者批准的缓存推断口径，不冒充交易所认证。

## 验证证据

1. 新 CLI 在源缓存的共享 root gate、目标月分区 pin、产品发布锁下完成完整范围
   rebuild 并返回 `activated:true`，两个产品均无 unresolved 日桶。
2. 再次以 2026-09-04 单日进行增量重建，两个产品仍各保留 700 个日桶，
   timeline hash 和 evidence identity 与完整重建完全一致。
3. 实际调用 `TradingTimelineStore::open_read_only(...).load_active(...)` 加载原缓存。
   每品种 10000 对确定性伪随机墙钟端点的结果与逐段求交参照一致；
   跨年份 forward/backward shift 互逆；known end 加 1ns 的查询明确报错。
4. Tick 任务未停止，没有远端补数，没有修改分钟或 Tick 行情文件。
   写入仅为请求授权的 Timeline generation、active 与协调锁目录。

精确 hash 与机器可读结果：
逐产品机器审计附件未随仓库保留。

## 调用入口

```rust,ignore
let store = TradingTimelineStore::open_read_only("/root/.tqsdk/data_series_1");
let timeline = store.load_active("CFFEX", "IM")?.expect("activated IM timeline");
let elapsed = timeline.trading_duration_between(start_ns, end_ns)?;
let other_end = timeline.shift(anchor_ns, duration, direction)?;
```

端点使用 Unix 纳秒，起止区间必须位于该产品 known range；加载完成后的时间运算
只访问不可变内存。已经持有旧 generation 的进程须重新 `load_active`，不会自动替换其对象。
本次未提交 git 或部署 Docker，也未配置常驻自动维护；将来有新的已结束交易日时，
仍需扩展受审查目录范围并通过正常 fill/rebuild 流程维护，不能外推当前结束边界。
