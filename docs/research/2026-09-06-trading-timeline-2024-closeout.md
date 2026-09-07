# TradingTimeline 2024 闭环审计

状态：**缓存验证完成，权威资料验收未完成；禁止激活。**

本轮只补研究与验收记录，不更改 public API、推断算法或激活条件。
`exception_review_complete` 保持 false；不把未发现问题当成例外史完整的证明。

## 已取得的缓存证据

从本机缓存只复制 IM、CF 的 2024 年 24 个 canonical 月文件及对应 metadata，
在 `/tmp/tqsdk-timeline-2024.a3zmYY` 独立验证。生产 root gate 正被占用，未停止 fill，
未修改生产缓存。复制后逐文件 `cmp`，两个 metadata 目录 `diff -qr` 均通过。
这是独立冻结副本的验证，不是对生产源执行过全局一致性快照的声明。

| 范围 | 请求工作日 | 常规模板匹配 | 无正成交、待休市证明 | 缺 session、待停夜盘证明 |
| --- | ---: | ---: | ---: | ---: |
| IM，2024-01-01 至 12-31 | 262 | 242 | 20 | 0 |
| CF，2024-02-06 至 12-31 | 236 | 211 | 19 | 6 |

IM 的 20 日缺口：01-01、02-09、02-12 至 02-16、04-04 至 04-05、
05-01 至 05-03、06-10、09-16 至 09-17、10-01 至 10-04、10-07。
CF 在其限定范围内的休市缺口与上表相同，但不含 01-01。
CF 的六个缺 session 日期为 02-19、04-08、05-06、06-11、09-18、10-08。
这些是需要核对公告的**候选例外**，不能由无成交反推权威休市。

两次 debug CLI 审计分别约 0.94 秒、3.11 秒；只代表本次运行耗时，不是冷盘性能保证。
本次复制的 24 个行情文件合计 4,330,650 字节；没有扫描其他品种或其他年份。
逐日状态、catalog hash 与 evidence hash 保存在
机器审计附件未随仓库保留；本节表格为结论摘要。

## 权威资料的剩余缺口

- 中金所：已定位中金所发〔2023〕78号原始链接，但原文及全年公告归档读取未成功。
  详见 [独立一手资料复核](2026-09-06-cffex-2024-calendar-authority.md)。
- 郑商所：年度通知线索为郑商函〔2023〕1048号；劳动节通知已定位
  [中文原始地址](https://www.czce.com.cn/cn/gyjys/jysdt/ggytz/webinfo/2024/04/1715229891872387.htm)
  及 [英文原始地址](https://english.czce.com.cn/en/AboutUs/News/ggytz/webinfo/2024/04/1715229892095107.htm)。
  本轮网页工具与公共 HTTP GET 均未取得正文，返回 412；不据此新增 confirmed 例外。
- 五个模板的制度依据仍见 [此前复核](2026-09-06-trading-timeline-2024-authority.md)。
  模板规则不代替节假日及其他特殊调整的完整性审计。

所需输入是上述交易所 **2024 年年度休市通知、相关停夜盘通知和当年时段调整公告目录/导出**，
可以是可读取的官方网页或保留出处的官方 PDF/HTML。不能只提供“确认 true”的授权来代替资料。

## 可重复的验收与最终放行步骤

对可独占读取的缓存执行（不需要账户，不远端补数）：

```bash
target/debug/tqsdk-cache timeline --cache-dir /path/to/frozen-cache \
 --catalog /path/to/operator-catalog.json \
  --symbol KQ.i@CFFEX.IM --start-day 2024-01-01 --end-day 2024-12-31 \
  --output-format json
target/debug/tqsdk-cache timeline --cache-dir /path/to/frozen-cache \
 --catalog /path/to/operator-catalog.json \
  --symbol KQ.i@CZCE.CF --start-day 2024-02-06 --end-day 2024-12-31 \
  --output-format json
```

1. 核对原文，把上述候选转换为有来源、日期和适用品种的 catalog 例外。
2. 复核完整限定范围；确认没有遗漏的延迟开盘、提前收盘、规则换版等。
3. 重跑冻结缓存；休市日期须匹配明确的 closed 规则，停夜盘须只移除 night。
   不得通过跳过休市日期制造不连续 known coverage，也不得将未知强制改为闭市。
4. 对实际验证的品种和日期构造单独、范围明确的可激活 catalog。
   当前五模板 catalog 的全范围未验证前，不能全局开启 attestation。
5. 在临时缓存激活、重新加载，验证跨午休、周末、长假和增量重建；再安排生产维护。

本轮 `git diff --check` 通过；无 Rust 修改，不重复声称上一轮 956 项测试在本轮重新执行。
未执行生产 `--apply`、提交或部署。
