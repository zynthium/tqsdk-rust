# 延伸至最近已结束交易日：进度与阻断

## 后续复查：缺失分钟已补齐，仅剩源缓存占锁

再次检查时，IM 9 月 2～4 日分钟文件及对应 metadata 已更新。
刷新隔离副本后，这三日均为完整覆盖、unique；无需重复远端补数。
更新 202609 目录后，IM、CF 的 2024-01-01～2026-09-04 全范围 rebuild
与隔离激活均成功，各 700 个 unique 日桶；IM 1298 段、CF 2578 段。
重新读取新 active，每品种 10000 对墙钟时间差参考校验及跨年正反向 shift 通过。

源缓存仍由进程 295575 的另一项全市场 Tick fill 占锁。
未停止该任务，未绕过锁生产激活；其释放后仍须从源缓存有 gate 地审计并激活。
本次没有执行远端填充。以下原有缺失三日的描述是先前状态，已被本节取代。
最新复查的机器摘要未随仓库保留。

目标：2024-01-01 至 2026-09-04（不含尚未结束的当日），品种 IM、CF。

本机 `/root/.tqsdk/data_series_1` 的一致性锁被 `tqsdk-cache` 进程 295575
持有，沙箱内外只读 audit 均返回 cache_busy；未停止该进程或绕过原缓存锁发布。
以下结果来自 `/tmp/timeline-through-20260904.IKDSSC` 的选择性隔离副本。
副本读取时未取得源缓存 gate，因此不能视作源缓存原子快照；生产激活前必须重新
对源缓存执行有 gate 的完整审计。未连接远端补数，未修改生产 active。

## 已完成

新目录：
操作者维护的 cache-confirmed-202609 catalog。
原目录保持不变；扩展规则明确使用 cache_inference，不沿用公告认证覆盖新增年代。
共 120 个已确认例外（此前 45 个，加本次 75 个）。

| 品种 | 已验证交易日范围 | 唯一已决日桶 | 开放区间 | 隔离 active |
| --- | --- | ---: | ---: | --- |
| IM | 2024-01-01～2026-09-01 | 697 | 1292 | 成功 |
| CF | 2024-01-01～2026-09-04 | 700 | 2578 | 成功 |

两者 known_ranges 均为单一连续范围；读取 active 后，每品种 10000 对确定性
伪随机墙钟时间与线性区间交集参照计算一致，跨年份正反向 shift 一致，
超出 known range 末端 1ns 被拒绝。从 2024-01-01 00:00 CST 到已验证末端，
IM 合计 9302400 交易秒；CF 合计 13304700 交易秒。

机器摘要未随仓库保留。
没有修改 Rust 实现、公共 API 或持久化格式；本次不是新的性能基准。

## 尚未完成的目标

IM 的 2026-09-02、09-03、09-04 分日审计均报
`minute kline cache coverage is incomplete`。2024-01-01～2026-09-01 全范围通过。
这三天是缺少完整 coverage，不是可确认的“完整缓存无成交”；没有将其标为休市。

后续顺序：等待现有填充任务释放锁；确认上述三日规范指数缓存完整 final；
将 IM 目录结束日延伸至 09-04，重新审查新增日期；对源缓存执行完整 audit 后
方可 `--apply`。若期间源证据与目录例外冲突，必须复核，不能强制激活。

隔离副本的重现命令（两者分别运行，避免根级一致性锁冲突）：

```bash
target/debug/tqsdk-cache timeline \
  --cache-dir /tmp/timeline-through-20260904.IKDSSC \
 --catalog /path/to/operator-catalog.json \
  --start-day 2024-01-01 --end-day 2026-09-01 \
  --symbol KQ.i@CFFEX.IM --apply --output-format json

target/debug/tqsdk-cache timeline \
  --cache-dir /tmp/timeline-through-20260904.IKDSSC \
 --catalog /path/to/operator-catalog.json \
  --start-day 2024-01-01 --end-day 2026-09-04 \
  --symbol KQ.i@CZCE.CF --apply --output-format json
```
