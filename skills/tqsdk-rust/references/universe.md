# Universe V2 与历史 Timeline

当用户问“选哪些合约”“当前全部合约”“历史全部合约”“随回测时间自动切换合约”、
`--universe`、主连/指数/具体合约或排除交易所/品种时，先用本文件确定 selector，
再选择订阅、回测或 fill API。

权威细节在仓库的 [Universe Language V2](../../../docs/architecture/universe-language.md) 与
[历史 Universe Catalog](../../../docs/architecture/historical-universe-catalog.md)；不要用旧示例反推新 DSL。

## 先区分两件事

Universe 只回答“选择哪些 instrument”，不选择数据流。tick、minute、daily 分别由订阅/
回测请求或 `tqsdk-cache --kind tick|minute|daily` 决定。

- `snapshot(...)`：调用时解析一次当前集合。省略 wrapper 的 V2 含义相同，但新配置应显式写出它，
  以避免 legacy v1 字符串路由歧义。适用于 live、relay、recording 和普通静态 backtest。
- `timeline(...)`：请求区间内随时间变化的集合。只由历史 compiler 生成不可变 V5 artifact；适用于
  `tqsdk-cache fill` 和消费已验证 artifact 的动态回测，不能用作 live 订阅。

## 选择器速查

```text
snapshot(contract:all)
snapshot(main:all;continuous:all;index:all;!CFFEX.*)
timeline(contract:all)
timeline(contract:all;continuous:all;index:all;!SHFE.au2506)
```

- `contract`：具体物理合约。snapshot 中是当前 metadata 可用、未过期的合约；timeline 中是请求
  区间内有 provider-data membership 的历史合约。
- `main`：当前具体主力，例如 `SHFE.au2606`；不是逻辑主连。
- `continuous`（`cont` 只是输入别名）：逻辑主连，例如 `KQ.m@SHFE.au`。
- `index`：逻辑指数，例如 `KQ.i@SHFE.au`。
- structural target 使用 `CFFEX.*`、`SHFE.au` 或 `SHFE.au2506`；V2 exclusion 写作
  `!CFFEX.*`、`!SHFE.au`、`!SHFE.au2506`。
- opaque provider code 必须写 `symbol:<provider-symbol>`。外部 exact-symbol 文件使用可重复的
  `--universe-file PATH`，不是 `file:` selector。

不要在新配置中使用 `active`、`physical`、`exchange:`、`product:`、bare scope、`!CFFEX`、`file:`
或 `~`。它们可能仍由 legacy v1 入口兼容处理，但不属于 Universe V2，也不能被重新规范化为 V2。

## 历史全合约下载

截至某个结束边界填充所有物理合约、主连和指数时，只传 `--universe`：不要求外部 plan 文件。

```bash
TQ_AUTH_USER='your-account' TQ_AUTH_PASS='your-password' \
cargo run -p tqsdk-cache -- \
  --cache-dir /var/lib/tqsdk/history --kind daily fill \
  --universe 'timeline(contract:all;continuous:all;index:all)' \
  --start-day 2025-01-01 --end-day 2026-06-30 \
  --symbol-concurrency 2
```

把 `--kind daily` 换成 `minute` 或 `tick` 即选择相应的缓存族。省略 `--end-day` 时，cutoff 固定为
本次启动时的最新可用闭市边界。timeline compiler 用 native-daily observation 建立数据 membership，
并把每个物理合约的下载起点限制在 `max(user_start, data-membership floor, kind first-available evidence)`；
它不宣称或猜测交易所法定挂牌日。

`timeline(main:...)` 与 `timeline(top:...)` 当前不支持，因为没有 hash-pinned historical ranking；
在访问远端前即会失败。选择所有物理合约应使用 `timeline(contract:all)`，不要尝试以 main/top 推导。

## 动态回测与 artifact

timeline 的可见 instrument 与底层物理数据依赖是两个集合。V5 artifact 固定 normalized AST、输入文件
identity、catalog、calendar、proof、可见 membership、tick/minute/daily targets 和 execution hash。动态
回测必须消费这个已验证 artifact，因而能随时间切换合约且保持可重现。

CLI 的正常入口始终是 `tqsdk-cache fill --universe 'timeline(...)'`。不要要求用户创建 `PLAN.json`，
也不要在新文档或代码中使用隐藏兼容入口 `--universe-plan` 或已移除的 `--universe-timeline`。旧
`physical:all` 与旧 timeline 只保留为 legacy v1/v3 兼容输入；V4 artifact 需先通过
`migrate-universe --plan-sha256 ...` 迁移，V1–V3 应重新编译。
