# Relay Universe Expression 设计

## 背景

`tqsdk-relay` 当前用环境变量描述上游期货合约集合：

- `TQSDK_RELAY_FUTURES_PRODUCTS` 表示动态产品发现入口。
- `TQSDK_RELAY_FUTURES_MAIN_ONLY=true` 表示只保留每品种真实主力合约。
- `TQSDK_RELAY_FUTURES_ACTIVE_CONTRACTS_PER_PRODUCT=N` 表示每品种主力优先，并按
  `open_interest` / `volume` 补足活跃度前 N。
- `TQSDK_RELAY_FUTURES_SYMBOLS` 和 `TQSDK_RELAY_FUTURES_SYMBOLS_FILE` 是静态完整合约
  覆盖入口。

这些入口能覆盖“全部活跃期货”“指定产品”“主力”“每品种前 N”几类场景，但组合能力弱：

- 不能直接表达“所有主力合约 + 所有加权指数合约”。
- 不能直接表达“全部加权指数合约”。
- 不能在最终集合上排除某个品种、交易所或精确符号。
- `main_only` 和 `active_contracts_per_product` 是全局选择器，难以作为多个 include 层中的某一层。

## 目标

新增一个组合式 universe 表达式入口：

```bash
TQSDK_RELAY_FUTURES_UNIVERSE="main:all;index:all;!CFFEX"
```

目标：

1. 支持订阅所有真实主力合约。
2. 支持订阅所有加权指数连续合约。
3. 支持排除某些品种、交易所或精确合约。
4. 支持分层 include 和统一 exclude，方便组合条件。
5. 保留旧环境变量兼容路径，但新旧 universe 入口不能混用。
6. 保持 relay 边界：universe resolution 仍是订阅前置模块；上游订阅仍只接收最终符号集合。

## 非目标

- 不把 relay 变成通用天勤代理；仍只代理行情路由。
- 不代理下游 query/auth/metadata。
- 不引入 TOML/YAML 配置文件作为首批主路径。
- 不支持复杂布尔表达式、括号、交集或优先级规则。
- 不在第一批实现按交易时段、成交量阈值、黑夜盘、合约月份等高级筛选。

## 用户配置入口

新增环境变量：

```text
TQSDK_RELAY_FUTURES_UNIVERSE
```

一旦设置该变量，以下旧 universe 入口不能同时设置：

- `TQSDK_RELAY_FUTURES_PRODUCTS`
- `TQSDK_RELAY_FUTURES_MAIN_ONLY`
- `TQSDK_RELAY_FUTURES_ACTIVE_CONTRACTS_PER_PRODUCT`
- `TQSDK_RELAY_FUTURES_SYMBOLS`
- `TQSDK_RELAY_FUTURES_SYMBOLS_FILE`

原因：新变量已经能表达旧入口语义；混用会让最终集合来源不透明。

## 表达式语法

```text
universe = clause (";" clause)*

clause   = [op] selector
op       = "!" | "~"
selector = kind ":" values
         | "top" ":" positive_int ":" values
         | values
values   = value ("," value)*
```

分隔符语义：

- `;` 分隔规则层。
- `,` 分隔同一规则层中的多个值。

操作符语义：

- 无操作符：include。
- `!`：exclude，主推写法。
- `~`：exclude 别名，主要用于用户偏好；文档示例优先使用 `!`。

求值语义：

```text
final = union(include clauses) - union(exclude clauses)
```

排除不依赖书写顺序。也就是说：

```bash
TQSDK_RELAY_FUTURES_UNIVERSE="!CFFEX;main:all;index:all"
```

与下面等价：

```bash
TQSDK_RELAY_FUTURES_UNIVERSE="main:all;index:all;!CFFEX"
```

## Selector kind

| Kind | 示例 | 含义 |
| --- | --- | --- |
| `active` | `active:all` | 全部未过期真实期货合约。 |
| `active` | `active:SHFE.au,DCE.m` | 指定产品下的未过期真实期货合约。 |
| `main` | `main:all` | 全部真实主力 underlying 合约，例如 `SHFE.au2602`。 |
| `main` | `main:SHFE.au` | 指定产品真实主力 underlying 合约。 |
| `index` | `index:all` | 全部加权指数连续合约，例如 `KQ.i@SHFE.au`。 |
| `index` | `index:SHFE.au` | 指定产品加权指数连续合约。 |
| `cont` | `cont:all` | 全部主连连续合约，例如 `KQ.m@SHFE.au`。 |
| `cont` | `cont:SHFE.au` | 指定产品主连连续合约。 |
| `top:N` | `top:2:all` | 每品种主力优先，再按活跃度补足前 N 个真实合约。 |
| `top:N` | `top:2:SHFE.au,DCE.m` | 指定产品每品种前 N 个真实合约。 |
| `symbol` | `symbol:SHFE.au2602,KQ.i@DCE.m` | 精确符号。 |
| `product` | `product:SHFE.au,DCE.m` | 产品匹配；主要用于 exclude，也允许 include 时作为 `active:` 的窄别名。 |
| `exchange` | `exchange:CFFEX` | 交易所匹配；主要用于 exclude。 |

`main` 必须表示真实主力 underlying 合约，不表示 `KQ.m@...`。需要主连连续代码时用
`cont`。

`index` 表示天勤 `KQ.i@EXCHANGE.product` 形式的加权指数连续符号。

## 裸值自动识别

没有 `kind:` 的 selector 按 token 自动识别：

| Token | 类型 |
| --- | --- |
| `KQ.i@SHFE.au` | 精确 symbol。 |
| `KQ.m@SHFE.au` | 精确 symbol。 |
| `SHFE.au2602` | 精确真实期货 symbol。 |
| `SHFE.au` | 交易所限定产品。 |
| `CFFEX` | 交易所。 |
| `au` | 跨交易所 product_id。允许，但 dry-run 给 warning。 |

裸值主要为 exclude 服务：

```bash
TQSDK_RELAY_FUTURES_UNIVERSE="main:all;index:all;!SHFE.au,DCE.m,CFFEX"
```

等价于：

```bash
TQSDK_RELAY_FUTURES_UNIVERSE="main:all;index:all;!product:SHFE.au,DCE.m;!exchange:CFFEX"
```

## Include 和 exclude 匹配模型

resolver 内部使用结构化符号：

```text
UniverseSymbol {
  symbol,
  kind,
  exchange_id,
  product_id,
}
```

`kind` 建议值：

- `active`
- `main`
- `index`
- `cont`
- `symbol`

exclude 匹配规则：

- `symbol`：只匹配 `symbol` 完全相等。
- `product`：匹配 `exchange_id + product_id`；如果只写 `product_id`，匹配所有交易所该产品。
- `exchange`：匹配 `exchange_id`。
- 裸 token 先解析成上面三类之一，再匹配。

示例：

```bash
TQSDK_RELAY_FUTURES_UNIVERSE="main:all;index:all;cont:all;!SHFE.au"
```

`!SHFE.au` 会移除：

- `SHFE.au2602` 等真实合约。
- `main:SHFE.au` 解析出来的真实主力。
- `KQ.i@SHFE.au`。
- `KQ.m@SHFE.au`。

## 场景示例

全部真实活跃期货：

```bash
TQSDK_RELAY_FUTURES_UNIVERSE="active:all"
```

全部真实主力：

```bash
TQSDK_RELAY_FUTURES_UNIVERSE="main:all"
```

全部加权指数连续合约：

```bash
TQSDK_RELAY_FUTURES_UNIVERSE="index:all"
```

全部主连连续合约：

```bash
TQSDK_RELAY_FUTURES_UNIVERSE="cont:all"
```

真实主力 + 加权指数：

```bash
TQSDK_RELAY_FUTURES_UNIVERSE="main:all;index:all"
```

真实主力 + 加权指数，排除股指：

```bash
TQSDK_RELAY_FUTURES_UNIVERSE="main:all;index:all;!CFFEX"
```

真实主力 + 加权指数，排除黄金和豆粕：

```bash
TQSDK_RELAY_FUTURES_UNIVERSE="main:all;index:all;!SHFE.au,DCE.m"
```

指定品种的真实主力和加权指数：

```bash
TQSDK_RELAY_FUTURES_UNIVERSE="main:SHFE.au,DCE.m;index:SHFE.au,DCE.m"
```

每品种主力 + 次主力，再加全部加权指数：

```bash
TQSDK_RELAY_FUTURES_UNIVERSE="top:2:all;index:all"
```

每品种主力 + 次主力，排除黄金：

```bash
TQSDK_RELAY_FUTURES_UNIVERSE="top:2:all;!SHFE.au"
```

显式真实合约 + 显式加权指数：

```bash
TQSDK_RELAY_FUTURES_UNIVERSE="symbol:SHFE.au2602,KQ.i@DCE.m"
```

混合：全市场加权指数 + 指定真实主力，排除某个真实合约：

```bash
TQSDK_RELAY_FUTURES_UNIVERSE="index:all;main:SHFE.au;!SHFE.au2602"
```

白名单式配置：只订两个品种加权指数，并排除其中一个连续符号：

```bash
TQSDK_RELAY_FUTURES_UNIVERSE="index:SHFE.au,DCE.m;!KQ.i@DCE.m"
```

按交易所排除：

```bash
TQSDK_RELAY_FUTURES_UNIVERSE="active:all;!exchange:CFFEX"
```

跨交易所产品排除，使用裸 product id：

```bash
TQSDK_RELAY_FUTURES_UNIVERSE="main:all;index:all;!au"
```

该写法允许，但 dry-run 应提示可能跨交易所匹配，推荐写成 `!SHFE.au`。

## 旧配置兼容映射

| 旧配置 | 新表达式 |
| --- | --- |
| `TQSDK_RELAY_FUTURES_PRODUCTS=ALL` | `active:all` |
| `TQSDK_RELAY_FUTURES_PRODUCTS=SHFE.au,DCE.m` | `active:SHFE.au,DCE.m` |
| `TQSDK_RELAY_FUTURES_MAIN_ONLY=true` + `TQSDK_RELAY_FUTURES_PRODUCTS=ALL` | `main:all` |
| `TQSDK_RELAY_FUTURES_MAIN_ONLY=true` + `TQSDK_RELAY_FUTURES_PRODUCTS=SHFE.au` | `main:SHFE.au` |
| `TQSDK_RELAY_FUTURES_ACTIVE_CONTRACTS_PER_PRODUCT=2` + `TQSDK_RELAY_FUTURES_PRODUCTS=ALL` | `top:2:all` |
| `TQSDK_RELAY_FUTURES_ACTIVE_CONTRACTS_PER_PRODUCT=2` + `TQSDK_RELAY_FUTURES_PRODUCTS=SHFE.au` | `top:2:SHFE.au` |
| `TQSDK_RELAY_FUTURES_SYMBOLS=SHFE.au2602,DCE.m2609` | `symbol:SHFE.au2602,DCE.m2609` |
| `TQSDK_RELAY_FUTURES_SYMBOLS_FILE=./symbols.txt` | 首批继续保留旧入口，不强制映射到表达式。 |

旧入口仍可用。新实现只新增更强的主路径，不强制迁移已有部署。

## Resolver 数据流

新增解析层：

```text
TQSDK_RELAY_FUTURES_UNIVERSE
  -> UniverseExpression
  -> include clauses + exclude clauses
  -> FuturesUniversePlan
  -> Vec<UniverseSymbol>
  -> Vec<UpstreamTickChart>
```

各 include layer 的数据源：

- `active`：继续用 `query_quotes(Some("FUTURE"), ..., expired=false)` 获取候选，再按批
  `query_symbol_info()` 得到 `exchange_id`、`product_id`、`expired`、`trading_time`。
- `main`：继续用 `query_cont_quotes()` 得到真实主力 underlying，再用 active metadata
  或按需 `query_symbol_info()` 填充结构化字段。
- `top:N`：沿用现有主力优先 + quote snapshot 活跃度排序逻辑。
- `index`：从产品 universe 生成 `KQ.i@EXCHANGE.product`。
- `cont`：从产品 universe 生成 `KQ.m@EXCHANGE.product`。
- `symbol`：直接加入精确符号；如果是 `EX.productYYYY` 可从 symbol parse 出产品；
  如果是 `KQ.*@EX.product` 可从 `@` 后解析产品。

产品 universe 来源：

- 对 `all`：来自 active futures metadata 中的非过期产品集合。
- 对指定产品：来自表达式 values。
- 对 `main`：如果需要精确产品字段，可将 `query_cont_quotes()` 返回的真实主力与 active
  metadata 做 join；join 不到时保留 symbol，但 dry-run 记录 warning。

最终输出：

- 按 `symbol` 去重。
- 稳定排序，建议按 `symbol` 字典序，保持 dry-run 和测试 deterministic。

## Dry-run 和诊断

`TQSDK_RELAY_DRY_RUN=1` 输出应扩展以下字段：

- `futures_universe_expression`：原始表达式。
- `futures_universe_include_clauses`：include clause 数。
- `futures_universe_exclude_clauses`：exclude clause 数。
- `futures_universe_include_symbols`：exclude 前符号数。
- `futures_universe_excluded_symbols`：被排除符号数。
- `futures_universe_final_symbols`：最终符号数。
- `futures_universe_symbols_by_kind`：按 `active` / `main` / `index` / `cont` / `symbol` 统计。
- `futures_universe_warnings`：裸 `product_id`、metadata join 失败等非致命告警。

旧字段 `upstream_symbols` 继续表示最终订阅符号数。

## 错误规则

必须报错：

- 空表达式。
- 空 clause，例如 `main:all;;index:all`。
- 未知 kind。
- `top:0:all`。
- `top:x:all`。
- 空 values，例如 `main:`。
- 空 value，例如 `main:SHFE.au,,DCE.m`。
- 新旧 universe env 混用。
- `!` 或 `~` 后没有 selector。

允许但 warning：

- 裸 `product_id`，例如 `!au`，因为它跨交易所匹配。
- `main` 返回真实主力但 metadata join 失败。
- include 最终被 exclude 清空；进程可以只启动下游服务，`upstream_symbols=0`。

## 测试计划

Parser 单元测试：

- `main:all;index:all;!CFFEX`
- `main:SHFE.au,DCE.m;index:SHFE.au,DCE.m`
- `top:2:all;!SHFE.au`
- `symbol:SHFE.au2602,KQ.i@DCE.m`
- `~SHFE.au` 等价于 `!SHFE.au`
- 空 clause / 未知 kind / `top:0` / 空 value 报错

Resolver 单元测试：

- `main:all` 只选择真实主力 underlying。
- `index:all` 生成 `KQ.i@EX.product`。
- `cont:all` 生成 `KQ.m@EX.product`。
- `main:all;index:all;!SHFE.au` 同时排除真实主力和 `KQ.i@SHFE.au`。
- `top:2:all;index:all` 保留现有主力优先 + 活跃度排序语义。
- `symbol:KQ.i@SHFE.au;!SHFE.au` 最终为空。
- 裸 `CFFEX` 作为 exchange exclude。
- 裸 `au` 作为跨交易所 product exclude 并产生 warning。

Config 测试：

- 新 env 能加载为 universe expression。
- 新 env 与旧 `FUTURES_PRODUCTS` 混用报错。
- 旧 env 兼容路径不变。
- dry-run report 包含新统计字段。

Smoke 测试：

- binary dry-run 静态 expression 不绑定端口、不连接上游 market websocket。
- `index:SHFE.au;!SHFE.au` dry-run 输出 `upstream_symbols=0`。

## 文档更新范围

实现时同步更新：

- `crates/tqsdk-relay/README.md`
- 根 `README.md` 的 relay 配置摘要
- `docs/architecture/README.md` 中 relay 当前能力摘要
- `docs/architecture/validation.md` 如果新增验证命令或 contract test

本设计不改变 crate 边界，不改变 SDK 默认直连路径，不改变 core runtime contract。
