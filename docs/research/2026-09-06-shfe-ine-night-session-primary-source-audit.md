# SHFE / INE 夜盘历史规则：一手来源审计

研究日期：2026-09-06（Asia/Shanghai）
范围：只接受交易所官网承载的一手材料；不以期货公司转载、新闻报道或分钟行情补足权威性。

## 结论摘要

| 事项 | 可安全写入的日期语义 | 结论 | 置信度 |
| --- | --- | --- | --- |
| SHFE `rb` / `hc` / `bu` 的 `21:00–01:00` 改 `21:00–23:00` | 拟用的 civil session-start 为 `2016-05-03`；若系统规定夜盘归属下一交易日，则对应交易日是 `2016-05-04` | 未找到该变更公告的 SHFE 官网存档，故**不可按本审计的仅一手来源门槛设为 confirmed**。 | 不足（阻断） |
| INE 2024 国庆节前夜盘取消 | `2024-09-30` 是公告所述的 civil date；15:00 后休市，故该晚 21:00 夜盘不存在。若把夜盘归属下一交易日，原本将归属 `2024-10-01`，但该日是休市日，不能表示为 `effective_trading_day = 2024-09-30`。 | 已确认。 | 高 |
| INE 2024 国庆节后夜盘恢复 | `2024-10-08` 是公告所述的 civil session-start date；该日先恢复日盘，随后“that night”恢复连续交易。若夜盘归属下一交易日，其归属交易日为 `2024-10-09`。 | 已确认；后一层交易日归属是对项目既定映射规则的应用，不是公告逐字定义。 | 高（公告事实）；中（映射推论） |

## INE：2024 年国庆节

### 一手公告

- [Circular on Trading Adjustments for National Day 2024 (INE)](https://www.ine.cn/eng/circularnews/circular/202409/t20240924_823431.html)
  - 发布/更新日期：2024-09-24
  - 发布主体：上海国际能源交易中心（INE）官网。
  - 注：该页说明如中英文不一致以中文为准；本审计记录其明确的日期与时段语义，未将英文翻译扩展为公告未写明的“交易日归属”。

### 可验证的公告事实

该公告第 1 项的直接证据（英文原文短摘）是：市场 “closed from 3:00 p.m. September 30, 2024 to October 7, 2024”，并于 “October 8, 2024 ... central auction session from 8:55 a.m. to 9:00 a.m.” 恢复，且 “continuous trading on the night of that day”。这说明的是 civil date / actual session-start date，而不是夜盘的交易日归属。

因此：

1. `2024-09-30` 的日盘在 15:00 收市；该日 21:00 的夜盘被取消。该事实应记录为 `civil_session_start_date: 2024-09-30` 的 `remove_sessions: ["night"]`（或作为假期 closed span 的边界），而不是把这条夜盘写成归属 `2024-09-30` 的交易日例外。
2. `2024-10-08` 日盘恢复，且 **civil** `2024-10-08 21:00` 恢复夜盘。对“夜盘属于下一交易日”的本项目约定，后者归属交易日为 `2024-10-09`。所以草案名为 `2024-10-08 night_session_resumed` 若键表达交易日，应改为 `2024-10-09`；若键表达 civil session-start date，必须显式写明该语义。
3. 更小、更不易误用的建模是只记录节前的取消（或闭市范围）；无需单独的 `resume` exception。基础工作日规则自然会在 `2024-10-08` 晚恢复。若仍保留恢复审计记录，字段必须是 `civil_session_start_date: 2024-10-08` 与 `trading_day: 2024-10-09`。

### 适用范围限制

公告用语是“所有期货、期权合约”的市场安排，故其夜盘操作只应应用到 INE 中具有 `night` session ID 的品种；不能据此为无夜盘品种新增夜盘。

## SHFE：`rb` / `hc` / `bu` 收市由 01:00 调整为 23:00

### 已找到的一手上下文

- [中国证券报：上期所差异化安排夜盘交易（SHFE 官网连续交易专题）](https://www.shfe.com.cn/content/lxjy/mtbd5.html)
  - 页面日期：2014-12-24
  - 官网页面明确记载 `rb`、`hc`、`bu` 在连续交易推出时的时段为每周一至周五 `21:00` 至次日 `01:00`，并称法定节假日前第一个工作日不进行连续交易。

该材料足以证实旧 epoch 的规则和“节假日前不进行连续交易”的历史机制；它**不能**给出后来缩短收市时间的精确生效日。

### 审计结果：精确切换日尚不可确认

公开可见的非一手转载一致指向一份题为《**关于调整相关品种连续交易时间安排的通知**》、文号 `上期发〔2016〕67号` 的文件，并声称其发布日期为 2016-04-26，生效措辞为“自 2016 年 5 月 3 日当晚连续交易起”。但原始 SHFE 公告页/可下载件未能在本次官网检索中定位。

因本研究限定为一手来源，以上转载**不作为来源、也不在此给出转载链接**。结果是：

- 不得把 `2016-05-03` 或由项目交易日归属规则推得的 `2016-05-04` 标为 `confirmed`。
- 可在候选集合中保留两个 epoch；只有取得 SHFE 原公告 URL、PDF 或可验证的官网档案后，才能将切换边界固定。
- 若未来取得原文并确认为“2016 年 5 月 3 日当晚连续交易起”，应保存两个字段：`effective_civil_session_start_date: 2016-05-03` 与（按本项目约定推得）`effective_trading_day: 2016-05-04`。不要以单个无语义日期字段混合两者。

### 停止条件

没有发现互相矛盾的**一手**证据；问题是该精确变更的 SHFE 官网原文目前不可得。因此本条按 evidence policy 处于 `unknown`，而非以多家转载的一致性提升为 confirmed。

## 对 catalog / compiler 的最低要求（研究结论，不修改实现）

每条跨夜规则或例外必须同时能表达：

```text
civil_session_start_date  # 21:00 实际发生的公历日期
trading_day               # 夜盘按系统约定归属的下一交易日（如适用）
operation                 # closed / remove_sessions / replacement_sessions
session_ids               # 例如 ["night"]，不能按时段猜测
authority_url
published_on
validation_status         # confirmed / candidate / unknown
```

对本次 INE 例外，`authority_url` 和 `published_on: 2024-09-24` 可标 `confirmed`。对 SHFE 时间缩短，`validation_status` 必须保持 `unknown`，直到获得一手原文。

## 一手来源索引

| 适用结论 | 官方来源 | 发布日期 | 可用性 |
| --- | --- | --- | --- |
| INE 2024 国庆节休市、日盘及夜盘恢复语义 | [Circular on Trading Adjustments for National Day 2024 — Shanghai International Energy Exchange](https://www.ine.cn/eng/circularnews/circular/202409/t20240924_823431.html) | 2024-09-24（页面 Updated on） | 可直接访问；已用于 confirmed 结论。 |
| SHFE `rb` / `hc` / `bu` 初始 `21:00–01:00` epoch 及节前不进行连续交易的背景 | [中国证券报：上期所差异化安排夜盘交易 — 上海期货交易所](https://www.shfe.com.cn/content/lxjy/mtbd5.html) | 2014-12-24（页面日期） | 可直接访问；只证明旧 epoch，不证明 2016 年切换日。 |

没有为 SHFE 2016 年缩短时段加入任何非交易所转载链接。所需的 SHFE 原始通知《**关于调整相关品种连续交易时间安排的通知**》（据转载标作 `上期发〔2016〕67号`）仍未发现可验证的官网 URL 或附件；因此该边界继续为 `unknown`。
