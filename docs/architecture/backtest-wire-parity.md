# 回测历史网络请求对齐

目标优先级：数据与持久化正确性、官方应用层请求特征、局部性能优化。这里的对齐不是 TLS、
TCP 或 HTTP 客户端指纹伪装，也不保证真实网络的毫秒级时序相同。

## 已落地边界

- `tqsdk-session::BacktestChartPager` 是共享的协议状态机：窗口 8964，首次
  `focus_datetime=start/current_dt`、`focus_position=8964`；两个物理 chart ID 交替，
  `left_kline_id` 使用服务端原始 `right_id`，不使用过滤后的最后一行。
- 非空页在消费前发出下一页请求，包括含终点或终端短尾的页。不会每页取消、创建第三个 ID。
  重用 ID 时必须匹配当前请求的 state，拒绝旧窗口回显。history 事件仍携带逻辑 chart ID。
- `MarketChartLease::update` 只允许独占 lease 原位移动窗口；共享 lease 不得修改别人的请求。
  terminal/显式关闭释放两条 lease，Drop 使用原有 best-effort cleanup。`StreamCompleted` 只返回一次。
- data 的一个 client 内，同凭证、同端点、同 Tokio runtime 的独立序列可以共享一个活跃连接。
  序列按 `(symbol, kind)` 隔离，不能让两个消费者相互裁剪。准入/鉴权建连只执行一次；页消费和
  持久化不会占用建连锁。全局两连接预算不变，独立 client 不被强行合并成同一 job。
- 任一 reader 取消、失败或本地裁剪后，该连接不再接受新的共享 reader，也不能回池。污染标记与
  attach 检查在同一临界区；关闭期间禁止 attach，关闭 future 被取消也永久标脏。
  建连、读取和裁剪的错误出口统一标脏；已有独立 reader 可以继续完成。全部 reader 退出后才回收连接。
- wait 的 Tick 与单/多合约 Kline 复用分页器；Kline 保留开盘/收盘两个阶段与 binding。
  相同 Kline 序列的不同本地窗口共用远端订阅。动态订阅从当前回测时间定位，mid-bar 起点先
  materialize open-only 状态；不会为了等到收盘而使 handle 一直未就绪。空区间也发布一次 ready，
  不重复发出相同历史请求。
- 策略路径把当前可见的同时间 Tick/Kline 合并为一次 runtime commit。行情与时间一起经
  `RuntimeHandle::ingest_batch` 提交到现有 `replay/wait_backtest/cursor/dt`，不创建第二棵状态树，
  不修改 core 的 commit/revision/cursor 或 mutation guard。`step()` 与 `wait_update()` 共用时间。

## 仍有差异，不得宣称完全一致

- data 缺口请求仍以 Tick 交易日、Minute checkpoint、Daily 32 天为持久化/恢复边界重新定位。
  **尚未把跨片连续网络游标与 durable checkpoint 分离**。不能简单扩大 range 或保留已过滤游标：
  前者改变恢复合同，后者会丢失已读但属于下一片的行。需要 carry-over、边界证明和失败恢复测试。
- 不同本地长度的 Tick handle 尚未像 Kline 那样合并为一个 source。
- 取消报文仍使用通用 `CancelChart` 最小报文；官方保留上次窗口字段并先取消当前交替 ID。
  HTTP/token 复用、外层 retry/backoff、peek/flush 调度和连接销毁保护仍按本项目合同，未声称逐包一致。
- 本地缓存命中保持零远端请求。fill 没有策略消费过程，不人为复制 Python CPU 消耗或 sleep。
- 尚无真实网络抓包、生产吞吐/RSS 对照结果；离线测试不能证明这些指标无退化。

## 证据与验证

`scripts/check_python_backtest_pager.py` 从 `/opt/tqsdk-python/tqsdk/backtest.py` 提取并直接运行
官方 `_gen_serial` 的 AST，仅替换 channel/entity 和 quote 辅助函数，无登录、无网络。
输出源 SHA-256 和真实生成的 `set_chart` 序列；当前 Tick 样例为 A/B/A/B、续页 1/3/4、最后双取消。
它验证分页规则，不是 Python 全功能回测模拟器；分钟线样例仅检查首屏与终端前预取。

```bash
python3 scripts/check_python_backtest_pager.py
cargo test -p tqsdk-session --test server_backtest_history --test session_market_command_helpers --offline
cargo test -p tqsdk-wait --offline
cargo test -p tqsdk-data --lib backtest_history --offline
```

回归覆盖 state 回显、exact-start、overlap、短尾、日线/分钟线、慢 reader 跨过 8 项 commit 保留窗口、
独立序列共享连接、污染/取消销毁、attach 竞态、mid-bar、动态订阅、同时间合批，以及 `wait_update` 时间。
所有场景都必须保持 cache final coverage 与暂存分离；不得用减少连接数替代正确性验收。
