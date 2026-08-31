# Universe Language V2

本文定义期货 instrument 集合的统一选择语言。Universe 只回答“选择哪些 instrument”，不选择
tick、minute、daily 数据流；数据种类仍由订阅 API、回测请求或 `tqsdk-cache fill --kind` 决定。

## 两个独立版本轴

- `UNIVERSE_LANGUAGE_VERSION = 2`：语法、规范化 AST、集合运算和 compiler 语义。
- `HISTORICAL_UNIVERSE_PLAN_VERSION = 5`：历史 timeline 的持久化 wire、hash、execution closure
  与 artifact chain。

不能用笼统的“V2”同时指代两者。旧 `UniverseExpression` 是 legacy language v1；旧
`HistoricalUniversePlan` 仍承载 plan v1–v3。

## 快速心智模型

```text
contract:all
snapshot(main:all;continuous:all;index:all;!CFFEX.*)
timeline(contract:all;continuous:all;index:all;!SHFE.au2506)
```

- 省略 wrapper 等价于 `snapshot(...)`。
- `snapshot(...)` 在调用时解析一次当前集合，适合 live、relay 和普通 backtest 配置。
- `timeline(...)` 表示请求区间内随时间变化的集合，只能由历史 compiler 生成固定 artifact。
- `main` 返回当前具体主力合约，例如 `SHFE.au2606`。
- `continuous` 返回逻辑主连，例如 `KQ.m@SHFE.au`。它与 `main` 不是同一个 instrument。
- `index` 返回加权指数，例如 `KQ.i@SHFE.au`。
- `contract` 返回具体物理合约。

## V2 语法

```text
spec              := clause-list
                   | "snapshot(" clause-list ")"
                   | "timeline(" clause-list ")"
clause-list       := clause (";" clause)*
clause            := ["!"] view-selector
                   | "!" structural-target
view-selector     := "contract:" target-list
                   | "main:" target-list
                   | "top:" positive-integer ":" target-list
                   | ("continuous" | "cont") ":" target-list
                   | "index:" target-list
                   | "symbol:" symbol-list
target-list       := "all"
                   | structural-target ("," structural-target)*
structural-target := EXCHANGE ".*"
                   | EXCHANGE "." PRODUCT
                   | EXCHANGE "." CONTRACT
symbol-list       := provider-symbol ("," provider-symbol)*
```

合法 target 组合：

| view | `all` | exchange | product | physical contract | opaque symbol |
| --- | --- | --- | --- | --- | --- |
| `contract` | 是 | 是 | 是 | 是 | 否 |
| `main` / `top:N` | 是 | 是 | 是 | 否 | 否 |
| `continuous` / `index` | 是 | 是 | 是 | 否 | 否 |
| `symbol` | 否 | 否 | 否 | 否 | 是 |

国内 structural exchange 固定为 `CFFEX`、`SHFE`、`INE`、`DCE`、`CZCE`、`GFEX`。
exchange 大小写不敏感并规范化为大写；product、contract 尾部和 opaque provider symbol 除 trim
外保留原字节。无法按国内合约结构识别的代码必须写成 `symbol:<provider-symbol>`。

V2 不接受 `active`、`physical`、`exchange:`、`product:`、bare 自动识别、`file:` 或 `~`。
`cont` 只作为输入别名，规范化输出使用 `continuous`。`physical:all` 是 cache historical
legacy macro，不是 V2 selector。

## Snapshot 与 Timeline 能力

| view | snapshot | timeline |
| --- | --- | --- |
| `contract` | 当前 metadata 中 eligible、未过期的物理合约；显式合约按 metadata 校验 | 与请求区间及 provider-data membership 相交的物理合约 |
| `main` | 当前每品种具体主力 | 暂不支持；缺少 hash-pinned historical ranking 时在 acquisition 前失败 |
| `top:N` | 当前主力优先，再按 open interest / volume 排名 | 暂不支持；在 acquisition 前失败 |
| `continuous` | 当前可用产品的 `KQ.m@EX.product` | 逻辑主连 membership，加上对应物理数据依赖 |
| `index` | 当前可用产品的 `KQ.i@EX.product` | 逻辑指数 membership 及其固定数据依赖 |
| `symbol` | 精确 provider symbol | 必须能由 pinned catalog/capability 证明，否则失败 |

`KQD` 外盘不生成不存在的 `KQ.m@...` 或 `KQ.i@...`。Timeline 的生命周期依据 provider
数据 membership，不追求交易所法定挂牌日；详见
[历史 Universe Catalog](historical-universe-catalog.md)。

## 集合与排除规则

V2 固定执行：include union → typed exclusions → global filters → final symbol dedupe。子句顺序不影响
结果；legacy v1 仍保持原有顺序敏感行为。

- `!contract:<scope>` 只移除 contract provenance。
- `!main:<scope>`、`!top:N:<scope>`、`!continuous:<scope>`、`!index:<scope>` 只移除对应 view。
- `!symbol:X` 移除最终 symbol 为 X 的所有 provenance。
- `!CFFEX.*` 移除能分类到该交易所的所有 view。
- `!SHFE.au` 移除能分类到该品种的所有 view。
- `!SHFE.au2506` 移除该物理合约的 contract/main/top provenance，不移除同品种
  `continuous` 或 `index`。

完全重复的同 polarity selector 会去重；同一 selector 同时 include/exclude 会报
`ContradictorySelector`。同一 view 的 `all` 不能与其他 positive target 混用；宽 include 加窄
exclude 合法。

## 规范化与 identity

规范化排序固定为：

1. view：`contract` → `main` → `top:N`（N 升序）→ `continuous` → `index` → `symbol`；
2. include 在 view-qualified exclude 前，global filter 最后；
3. target：all → exchange → product → contract → symbol，同类按 UTF-8 byte 顺序。

Snapshot canonical text 省略 `snapshot(...)` wrapper；normalized AST 中始终显式存储
`mode=snapshot|timeline`。固定 identity 为：

```text
UNIVERSE_CANONICALIZER_ID = "tqsdk.universe.canonical.v2"
UNIVERSE_COMPILER_ID      = "tqsdk.universe.compiler.v2"
AST_SHA256 = SHA256("tqsdk.universe.ast.v2" || NUL || canonical_ast_json_bytes)
```

hash 输出使用 `sha256:<lowercase-hex>`。public Rust struct 的任意 serde 表示不是 hash preimage；
wire 字段、tag 或排序变化必须提升 language/canonicalizer version。

## Legacy-first 兼容路由

现有字符串 API 保持 source compatible：

1. 当前/live 入口先尝试完整 legacy `UniverseExpression`；成功即按 legacy v1 执行。
2. `snapshot(...)` 强制进入 V2，可用于消除迁移歧义。
3. legacy 拒绝但 V2 接受的输入（例如 `contract:all`）进入 V2。
4. historical cache 入口先保留完整 legacy `HistoricalFillUniverseSpec`；`physical:all` 和既有
   `timeline(active:all;cont:all)` 继续按 legacy evaluator 执行。
5. 直接调用 `UniverseSpec::parse_v2` 或 typed builder 不经过兼容 dispatcher。

运行报告应区分 `legacy-v1 / legacy-sequential-v1` 与 `universe-v2 / set-algebra-v2`，不能把
legacy 字符串重新规范化为 V2。

## 外部 symbol 文件

文件是输入源，不是 V2 AST selector：

- CLI 使用可重复的 `--universe-file PATH`；
- facade typed builder 使用 `universe_symbol_file(s)`；relay 使用
  `RelayRuntimeConfig::universe_symbol_file(s)`，既有 `RelayConfig` 字段集合保持源码兼容；
- relay binary 使用路径列表环境变量 `TQSDK_RELAY_FUTURES_UNIVERSE_FILES`；
- 每个文件接受换行或逗号分隔的 exact symbols，不接受嵌套 DSL；
- expander 每个文件只读一次，先 hash 原始 bytes，再校验 UTF-8 和展开 symbols；
- 路径只用于诊断，identity 由内容 hash 与规范化 symbols 决定；
- `!symbol:X` 和 structural filters 在文件展开后同样生效。

legacy `file:path` 继续由 legacy parser 处理，但新配置应使用外层 file API。relay 刷新时 file
读取/解析失败不会替换 last-known-good 上游订阅。

## 入口能力矩阵

| 入口 | legacy | V2 snapshot | V2 timeline |
| --- | --- | --- | --- |
| `Tq::quotes_universe` / `quotes_universe_spec` | 保持 | 支持 | 触网前拒绝 |
| `MarketCachePolicy::record_universe(_spec)` | 保持 | 支持 | 启动 recording 前拒绝 |
| `BacktestBuilder::universe(_spec)` | 当前静态集合 | 支持 | 拒绝隐式 acquire |
| relay config/runtime | 保持 | 支持 | resolver/WebSocket 前拒绝 |
| `tqsdk-cache fill --universe` | legacy historical 写 plan v3 | current snapshot fill | 默认发布 current V5 artifact |
| `BacktestBuilder::historical_universe_plan` | 读取 plan v1–v3 | 不适用 | 旧签名保持 |
| `BacktestBuilder::historical_universe_artifact` | wrap v1–v3 | 不适用 | 验证并消费 version-dispatched v1–v5 artifact |

## 历史 V5 与迁移

V5 artifact 固定 normalized AST、input source identity、acquisition、semantic catalog、calendar、
proof、可见 membership、physical dependencies、tick/minute/daily targets 与 execution hash。V2
timeline writer 默认直接发布 V5，不再写 V4/V3 rollback companion。

V4 是迁移输入而不是 normal writer format。迁移会先验证完整 V4/V3 rollback、acquisition 与 semantic
catalog chain，再发布新的 V5 内容寻址 artifact；原 V4 和 V3 文件绝不覆盖或删除：

```bash
tqsdk-cache migrate-universe --cache-dir <DIR> --plan-sha256 <V4_SHA256>
tqsdk-cache migrate-universe --cache-dir <DIR> --plan-sha256 <V4_SHA256> --apply
```

第一条只输出 immutable source-to-V5 mapping；第二条才发布。V1–V3 因没有可无损转换的 V4 execution
closure，必须重新编译。动态回测会校验 V5 自身 hash、回测区间和 acquisition/catalog chain，再用 timeline
限制可见 instrument，用 kind-specific tick targets 限制物理 cache symbol 与首可用边界。
