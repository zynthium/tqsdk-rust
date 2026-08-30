# GitNexus Engineering Plan

> Task: 以兼容迁移方式演进 Universe DSL：snapshot 默认、timeline 显式、typed scope、稳定 V4 artifact，并适配缓存填充、relay 与动态回测。
>
> Evidence verified at commit `eaebae7beba7b57e5fc8d158c8606d50d48312a2`; GitNexus index pinned to the same commit with PDG available. Deepen pass seeded from the architecture review and re-verified the load-bearing source paths.
>
> Evidence provenance schema 2; global dirty digest `0a9c85780067d9afcd0764f307b60891e3cee927ee11eaeb5ec7826d10fd82cd`; cited-path manifest 38 sorted entries; this generated plan path is the only excluded path.

## 1. Objective

交付一套新手可推断、机器可规范化、历史结果可重现的 Universe Language V2，同时满足以下硬约束：

- 所有当前有效的 legacy 表达式继续由 legacy parser/evaluator 执行，结果、子句顺序语义和 V1–V3 artifact 字节均不改变。
- V2 的普通表达式表示 snapshot；历史动态选择必须显式写成 `timeline(...)`。
- Universe 只选择 instrument，不选择 tick/minute/daily 数据流；数据种类继续由 `--kind`、订阅 API 或回测请求决定。
- 历史 `contract:all` 依据 provider-data membership，而不是交易所挂牌日期：只有开始出现可用行情数据后才进入历史成员集合。
- V4 使用新的 Rust 类型和固定 wire/hash 契约，不给现有公开 `HistoricalUniversePlan` 增加字段。
- V4 writer 必须 reader-first 上线，并同时生成可供旧二进制回滚使用的 V3 projection。

推荐的 V2 形式：

```text
contract:all
snapshot(main:all;index:all;!CFFEX.*)
timeline(contract:all;continuous:all;index:all;!SHFE.au2506)
```

两个版本轴必须始终使用完整名称，禁止在代码或文档中笼统称为“V2 plan”：

| 版本轴 | 常量 | 含义 |
| --- | --- | --- |
| Universe language | `UNIVERSE_LANGUAGE_VERSION = 2` | 语法、AST、规范化和选择语义 |
| Historical plan | `HISTORICAL_UNIVERSE_PLAN_VERSION = 4` | 持久化 plan/wire/hash/artifact-chain 格式 |

## 2. Current Behaviour

- [verified] `UniverseExpression::parse` 接受 `active/main/index/cont/top/symbol/file/product/exchange`、bare `Auto`、`!` 与 `~`，并保留输入顺序用于 display（`crates/tqsdk-data/src/universe_expression.rs:13-29, 88-130, 141-239`）。
- [verified] 当前 data 与 relay 的 snapshot resolver 先合并 include，最后统一应用 exclusion（`crates/tqsdk-data/src/universe.rs:424-484`; `crates/tqsdk-relay/src/universe.rs:476-535`）。
- [verified] 历史 parser 是独立的 `HistoricalFillUniverseSpec`；它接受 `physical:all` 或 legacy `timeline(...)`，并拒绝 legacy `main/top/file/Auto`（`crates/tqsdk-data/src/historical_fill_universe.rs:12-96`）。
- [verified] 历史 `resolve_selection` 按子句顺序原地修改集合；排除一个 physical scope 还会删除相关 continuous/index product（`crates/tqsdk-data/src/historical_universe_resolution.rs:159-205`）。
- [verified] `HistoricalUniversePlan` 是公开导出的、字段全公开、未标注 `#[non_exhaustive]` 的结构体（`crates/tqsdk-data/src/historical_universe.rs:520-530`; `crates/tqsdk-data/src/lib.rs:159-164`）。
- [verified] V3 plan hash 使用固定 tuple；artifact store 先 `verify` 再序列化发布，同一 hash 下不同 JSON 字节会冲突（`crates/tqsdk-data/src/historical_universe.rs:615-623, 672-804`; `crates/tqsdk-data/src/historical_universe_artifact.rs:829-934`）。
- [verified] artifact-chain 当前以 `plan_version < 3` 分支，否则直接读取 `v3_identity`，因此不能把 V4 简单塞入现有结构体（`crates/tqsdk-data/src/historical_universe_artifact.rs:843-865`）。
- [verified] facade 的 string entrypoints 直接调用 legacy parser；动态回测则消费已验证的 historical plan（`crates/tqsdk/src/lib.rs:759-773, 973-976, 2887-2926`）。
- [verified] legacy `file:` 在 data 和 relay 内部直接读取路径，文件内容按行和逗号拆成 symbol（`crates/tqsdk-data/src/universe.rs:817-843`; `crates/tqsdk-relay/src/universe.rs:831-858`）。

## 3. Relevant Architecture

- [verified] `tqsdk-data` 拥有 Universe AST、纯语义编译、历史 catalog/proof/plan/artifact；`tqsdk-cache` 负责编排 provider acquisition 和 fill；facade/task 只消费结果；relay 只负责当前行情订阅与本地恢复（`docs/architecture/historical-universe-catalog.md:80-117`; `docs/architecture/crate-boundaries.md`）。
- 新 V2 compiler 必须是纯函数边界：不能读文件、访问网络、获取 session、刷新 metadata 或选择数据流。
- snapshot capability adapter 可以读取当前 metadata/ranking；timeline capability adapter 只能使用 hash-pinned catalog/calendar/ranking evidence。
- legacy parser/evaluator 是兼容边界，不迁移为 V2 set algebra；V2 可以复用 metadata 适配器，但不能复用会改变 legacy 结果的 evaluation policy。
- relay/facade 遇到 timeline 必须在任何网络/刷新动作之前返回 capability error；动态回测只能接受经过 artifact-chain 验证且时间区间匹配的 plan。
- 这是 public API 与持久化格式演进，实施时必须同步根 README、相关 crate README、架构文档、contract example 和 validation matrix。

## 4. GitNexus Findings

- [graph] `gitnexus query`（“Universe DSL historical timeline plan artifact V4 cache relay facade”）确认主流程横跨 data parser/resolver/artifact、cache CLI、relay config/runtime 与 facade/backtest；因此不能只修改 parser。
- [graph] 对 `HistoricalUniversePlan` struct UID 的 upstream impact 为 LOW、2 个直接构造函数，但 CodeGraph/source verification 找到公开 re-export、artifact store、facade 和 contract example 等额外类型消费者；图结果不能代表 Rust source compatibility 的完整边界。
- [graph] `UniverseExpression` upstream impact 返回 UNKNOWN/0，并带 unresolved-caller 风险；源码确认 facade、relay、cache 和 historical parser 均直接使用它，因此计划按 public compatibility 高风险处理。
- [verified] relay 的 `universe_expression.rs` 目前只是 data legacy 类型的 re-export；V2 也应从 data re-export，不能再建一套 relay parser（`crates/tqsdk-relay/src/universe_expression.rs:3`）。
- [verified] 当前测试已经覆盖 V3 resolution/artifact、cache plan input 和 relay refresh，但没有冻结 arbitrary legacy timeline 的顺序/跨视图排除语义；本计划把它们提升为迁移前置 golden（`crates/tqsdk-data/tests/historical_universe_resolution.rs`; `crates/tqsdk-data/tests/historical_universe_artifact.rs`; `crates/tqsdk-cache/tests/cli.rs:2536-2739`; `crates/tqsdk-relay/tests/upstream.rs:1088-1460`）。

## 5. Statement-Level PDG Findings

- [graph] 既有 PDG slice 表明 historical proof/semantic gates 位于 selection/plan construction 之前（`crates/tqsdk-data/src/historical_universe_resolution.rs:67-99`）；V2 unsupported capability 也必须在 adapter acquisition 之前失败。
- [graph] 本次 Deepen 对 `resolve_selection` 和 `HistoricalUniversePlan::verify` 的 `pdg_query controls/flows` 没有返回可用 edge；以下顺序约束以当前源码为权威，不把空 PDG 当作无依赖证明。
- [verified] legacy historical selection 的结果依赖 clause loop 的先后次序，因此不能先转换为 order-independent AST 再执行（`crates/tqsdk-data/src/historical_universe_resolution.rs:159-205`）。
- [verified] plan verification 由 `match plan_version` 决定 hash preimage；V4 必须使用独立 match branch/type，而不是将 `>= 3` 继续解释成 V3（`crates/tqsdk-data/src/historical_universe.rs:672-804`）。
- [verified] artifact publish 是 verify → canonical bytes → content-addressed publish；V4 的 serializer 和 hash preimage必须在 writer 启用前由 fixed-byte golden 锁定（`crates/tqsdk-data/src/historical_universe_artifact.rs:829-934`）。
- [verified] legacy file resolution每次从 path 读取；V2 source expansion 必须只读一次，将原始 bytes hash 与展开结果一起交给纯 compiler，避免 TOCTOU 身份漂移。

## 6. Proposed Changes

### 6.1 冻结 legacy，并建立无歧义 dispatch

保留 `UniverseExpression`、`UniverseSelectorKind`、`HistoricalFillUniverseSpec` 及现有 resolver 的 public surface 和执行语义。新增 `UniverseInputLanguage::{LegacyV1, V2}` 与入口级 dispatcher，但不修改 legacy parser 的接收集合。

兼容 dispatcher 使用以下固定顺序：

1. cache historical 入口先识别精确字符串 `physical:all`，继续走 legacy V3。
2. cache historical 入口随后尝试完整的现有 `HistoricalFillUniverseSpec::parse + validate`；成功即走 legacy evaluator。因此 `timeline(cont:all)` 和所有当前有效历史命令保持原义。
3. snapshot-only facade/relay 如果看到顶层 `timeline(`，在调用 legacy parser 前直接返回 `TimelineNotAllowed`。
4. 顶层 `snapshot(...)` 强制进入 V2；wrapper 只用于解决兼容歧义。
5. 其余 string 先尝试 legacy parser/context validation；成功即标记 `legacy-v1`。只有 legacy 拒绝时才尝试 V2。
6. 直接调用 `UniverseSpec::parse_v2` 或 typed builder 时不经过兼容 dispatcher。

因此，`main:all;index:all` 在旧 string API 中仍是 legacy；需要 V2 exclusion/set 语义时写 `snapshot(main:all;index:all;...)`。V2-only 关键字 `contract`、`continuous` 可以在 snapshot 中省略 wrapper。

解析/执行报告必须带：

```text
input_language = legacy-v1 | universe-v2
evaluation_policy = legacy-sequential-v1 | set-algebra-v2
canonical_universe = <legacy display or V2 canonical text>
```

### 6.2 V2 grammar 与合法组合

在 `tqsdk-data` 新建深模块 `universe_spec/`，public façade 只导出 `UniverseSpec`、normalized read-only types、error 与 capability traits；parser/normalizer/wire/compiler 保持私有子模块。

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

target-list       := "all" | structural-target ("," structural-target)*
structural-target := EXCHANGE ".*"
                   | EXCHANGE "." PRODUCT
                   | EXCHANGE "." CONTRACT
symbol-list       := provider-symbol ("," provider-symbol)*
```

约束：

- 省略 wrapper 时 mode 为 snapshot；persisted normalized AST 始终显式存储 mode。
- canonical view 只有 `contract/main/top/continuous/index/symbol`；`cont` 仅为 V2 输入 alias，输出总是 `continuous`。
- `active`、`physical`、`exchange:`、`product:`、bare `Auto`、`file:`、`~` 均不属于 V2。精确 `physical:all` 只保留为 cache legacy macro。
- `contract` 允许 All/Exchange/Product/Contract；`main/top/continuous/index` 只允许 All/Exchange/Product；`symbol` 只允许 opaque symbol，不允许 `all`。
- bare structural target 只允许作为 negative global filter，例如 `!CFFEX.*`；positive selection 必须写 view，避免“选择哪类 instrument”不明确。
- domestic structural exchange 仅接受 `CFFEX/SHFE/INE/DCE/CZCE/GFEX`；exchange 输入 ASCII case-insensitive 并规范化为 uppercase。PRODUCT/CONTRACT 尾部及 provider symbol 除 trim 外保持原字节和大小写。
- structural contract 以非空 trailing ASCII digit run 识别；不符合国内结构的值必须写 `symbol:<provider-symbol>`。
- `top:N` 要求 `N > 0`；不同 N 是不同 selector，结果取并集，不做代数化简。
- 禁止嵌套 wrapper、空 clause/value 和 unknown keyword。

### 6.3 view、scope 与生命周期语义

| View | Snapshot | Timeline |
| --- | --- | --- |
| `contract:all/exchange/product` | 当前 metadata 中 eligible、non-expired 的 physical contracts | 在请求 `[start,end)` 内与 provider-data membership 相交的所有 physical contracts |
| `contract:EX.productYYMM` | metadata 已知时显式返回，即使已经 expired | 仅在其 membership 与请求区间相交时进入 timeline/targets |
| `main` | 当前 ranking 对每个 product 选出的 physical main contract | 本轮无 pinned ranking artifact，parse 成功但 capability-fail |
| `top:N` | 当前 ranking 的 N 个 physical contracts | 本轮无 pinned ranking artifact，parse 成功但 capability-fail |
| `continuous` | product 的 logical continuous instrument | logical timeline 加完整 physical dependency closure |
| `index` | product 的 logical index instrument | logical timeline 加完整 physical dependency closure |
| `symbol` | 精确 provider symbol | 必须能由 historical capability 分类/证明；未知 opaque symbol fail-closed |

`main` 与 `continuous` 永不等价：main 的可见结果是 physical contract，continuous 的可见结果是 logical instrument，后者复用 physical history storage 只是 execution dependency，不改变 universe identity。

Timeline membership 使用 provider observation：

- 起点是首个可用 provider data membership evidence，不要求交易所挂牌日期。
- `daily` 可作为低成本 discovery evidence；tick/minute 的 fill start 仍由各自 first-available boundary 与请求窗口共同裁剪。
- catalog 中不存在可证明数据 membership 的 contract 不进入 `contract:all`。
- Universe 编译不决定 tick/minute/daily；每种 kind 的 targets 在 execution closure 中分别计算。

### 6.4 V2 exclusion matrix

Compiler 必须保留 candidate provenance，完成 exclusion 后才按最终 symbol 去重。若同一 symbol 由多个 view 产生，只排除其中一个 view 时，其他 provenance 仍可使它存活。

| Exclusion | 影响 |
| --- | --- |
| `!contract:<scope>` | 仅 contract provenance |
| `!main:<scope>` | 仅 main provenance |
| `!top:N:<scope>` | 仅该 N 的 top provenance |
| `!continuous:<scope>` | 仅 continuous provenance |
| `!index:<scope>` | 仅 index provenance |
| `!symbol:X` | 最终 symbol 为 X 的所有 provenance |
| `!EXCHANGE.*` | 所有能分类到该 exchange 的 contract/main/top/continuous/index；可分类 symbol 同样排除 |
| `!EXCHANGE.PRODUCT` | 所有能分类到该 product 的上述 view |
| `!EXCHANGE.CONTRACT` | 等于该 physical contract 的 contract/main/top；continuous/index 不受影响 |

V2 固定为 include union → typed exclusions → final symbol dedupe，与 clause 顺序无关。legacy historical evaluator继续保留当前逐 clause 变更及跨视图删除行为。

### 6.5 Canonicalization 与稳定 AST bytes

规范化策略是已决策，不留给实现者选择：

- 同 polarity、同 normalized view/target 的重复项接受并去重。
- 完全相同的 view/target 同时出现在 include 与 view-qualified exclude 时返回 `ContradictorySelector`。
- `contract:all;!contract:CFFEX.*`、`contract:CFFEX.*;!CFFEX.*` 等“宽 include + 窄 exclude”合法。
- 同一个 view 出现 `all` 与任何其他 positive target（无论同 clause 或不同 clause）返回 `MixedAll`；不做隐式吞并。
- 更宽/更窄但都非 `all` 的 scopes 可以共存；compiler 取集合并集，normalizer 不做 metadata-dependent subsumption。
- canonical selector 顺序：contract → main → top（N 升序）→ continuous → index → symbol；include 在 view-qualified exclude 前，global filters 最后。
- target 顺序：All → Exchange → Product → Contract → ExplicitSymbol；同类按 normalized UTF-8 unsigned-byte lexicographic order。
- canonical snapshot text 省略 `snapshot(...)`；canonical timeline text保留 `timeline(...)`。持久化身份不依赖 display text。

新增固定身份：

```text
UNIVERSE_CANONICALIZER_ID = "tqsdk.universe.canonical.v2"
UNIVERSE_COMPILER_ID = "tqsdk.universe.compiler.v2"
```

Normalized AST 使用私有、字段顺序固定的 wire DTO：

```json
{
  "language_version": 2,
  "mode": "snapshot|timeline",
  "includes": [
    {
      "view": {"kind": "contract|main|top|continuous|index|symbol", "limit": null},
      "targets": [{"kind": "all|exchange|product|contract|symbol", "exchange": null, "value": null}]
    }
  ],
  "excludes": [],
  "global_filters": []
}
```

`top` 的 `limit` 必须是整数，其他 view 必须为 null。可选值是否发出、enum tag、字段顺序和数组顺序均由 private wire DTO 固定，禁止直接 hash public struct 的任意 serde 表示。

AST hash 的精确 preimage：

```text
SHA256(
  UTF8("tqsdk.universe.ast.v2") || NUL ||
  canonical_ast_json_bytes
)
```

输出格式固定为 `sha256:<lowercase-hex>`。任何 wire 字段增删、tag 改名或排序改变必须提升 language/canonicalizer version，而不能覆盖 V2 golden。

### 6.6 `file:` 的外层迁移

- legacy `file:path1,path2` 的 parser、cwd-relative 路径和逐次读取行为原样保留。
- V2 parser 始终拒绝 `file:`；文件不是 Universe AST 的 selector。
- 新增外层 `UniverseInput`（private fields、constructor/getter、`#[non_exhaustive]`）组合一个 optional `UniverseSpec` 与零个或多个 `UniverseSymbolFile`。
- cache CLI 新增 repeatable `--universe-file PATH`；它可以与 `--universe` 合用，也可以单独使用。relay/facade typed builder 增加等价的 `universe_symbol_file(s)` setter；现有 string API 不变。
- 每个文件沿用现有“每行或逗号分隔 symbol，empty value 报错”格式，不允许嵌入 DSL。
- relative path 在 expansion 时相对于进程 cwd 解析；absolute canonical path只用于 diagnostics，不参与 identity。
- expander 每个文件只读取一次：先 hash 原始 bytes，再按 UTF-8 解析并展开成 `symbol` include candidates。文件顺序和重复 symbol 不影响最终身份。
- `input_sources_sha256` 对按 content hash 排序的 `(raw_content_sha256, normalized_expanded_symbols)` fixed tuple 求 hash，并进入 V4 identity。相同内容位于不同路径产生相同 identity。
- 表达式 exclusion 在文件展开后应用；`!symbol:X` 能排除文件中的 X。
- relay refresh 若文件读取/解析或 V2 compile 失败，保留 last-known-good subscription；错误 report 包含 path、content hash（读取成功时）和阶段，但不记录文件内容。

### 6.7 Pure compiler 与 capability adapters

模块建议：

```text
crates/tqsdk-data/src/universe_spec/
  mod.rs          # narrow public façade
  ast.rs          # typed raw/normalized AST
  parser.rs       # V2 grammar only
  normalize.rs    # deterministic rules and wire DTO
  source.rs       # UniverseInput/file expansion value types; no implicit IO in compiler
  compiler.rs     # candidate provenance, inclusion/exclusion
  compatibility.rs# dispatch result types and legacy/V2 reporting; never emulate legacy semantics
```

- `SnapshotCapabilities` 提供 current contracts、product mapping、continuous/index symbols 和 current ranking。
- `TimelineCapabilities` 提供 hash-pinned catalog/calendar/data-membership evidence 与 kind-specific first-available boundaries。
- `HistoricalRankingCapabilities` 独立；未提供时 `timeline(main/top)` 在任何 adapter query/acquisition 之前返回 `UnsupportedTimelineRanking`。
- data 与 relay 的 V2 路径共享 compiler；各自只实现 adapter 和 error mapping。
- legacy data/relay resolver 保留原函数或显式 `LegacySequentialV1` adapter，不得改走 V2 `SetAlgebraV2`。
- compiler 输出 `CompiledUniverse`，包含 normalized AST hash、visible candidates/provenance、physical dependencies、kind-specific targets 和 capability identities；不包含网络对象或 path。

### 6.8 Source-compatible V4 Rust/wire model

冻结现有 `HistoricalUniversePlan`、`HistoricalUniversePlanV3Identity`、`HistoricalUniversePlanV3Execution` 的字段、serde 与方法签名。新增：

```rust
#[non_exhaustive]
pub struct HistoricalUniversePlanV4 { /* private fields + getters */ }

#[non_exhaustive]
pub enum HistoricalUniversePlanArtifact {
    Legacy(HistoricalUniversePlan), // versions 1..=3
    V4(HistoricalUniversePlanV4),
}
```

- enum 使用 custom flat serde：磁盘顶层仍以 `plan_version` 判别，不引入 `{"V4": ...}` wrapper。
- `HistoricalUniversePlanV4`、V4 identity/execution 使用 private fields、validated constructors/getters 和 private fixed-order wire DTO，避免后续 additive field 再次破坏 struct literals。
- 旧 `HistoricalUniverseArtifactStore::publish_plan/load_plan` 保持只处理 V1–V3；新增 `publish_plan_artifact/load_plan_artifact` 和 enum-dispatched chain verification。
- 旧 `BacktestBuilder::historical_universe_plan(HistoricalUniversePlan)` 签名不变；新增 `historical_universe_artifact(HistoricalUniversePlanArtifact)`。内部字段可改成 enum，旧方法只负责 wrap Legacy。
- 新 V2 historical compiler 返回新的 `HistoricalUniverseResolutionV4`；旧 compiler 继续返回现有 resolution/V3 plan。
- V4 artifact-chain 使用显式 enum match，不能使用 `plan_version >= 3 => v3_identity`。
- unknown plan version、V4 缺 identity/execution、identity chain 不一致均 fail-closed。

V4 identity至少固定：

```text
language_version
normalized_ast_json
normalized_ast_sha256
canonicalizer_identity
compiler_identity
input_sources_sha256
acquisition_sha256
semantic_catalog_sha256
calendar_identity
proof
execution_sha256
rollback_v3_plan_sha256
continuous_identity?
ranking_identity?
```

V4 plan hash 的精确 preimage是 private tuple 的 `serde_json::to_vec`：

```text
(
  "tqsdk.historical-universe-plan.v4",
  4_u32,
  timeline_wire_v4,
  budget_wire_v4,
  identity_wire_v4,
  execution_wire_v4
)
```

V4 artifact file bytes由独立 fixed-order `HistoricalUniversePlanArtifactWireV4` 产生。V4 loader 解析、验证并重新序列化；非 canonical bytes 即使语义等价也拒绝。V1–V3 loader 行为保持不变。

### 6.9 Reader-first rollout 与 V3 rollback projection

新增 cache 编排策略：

```text
HistoricalPlanWritePolicy::LegacyOnly
HistoricalPlanWritePolicy::V4WithV3Rollback
CLI: --historical-plan-write-policy legacy-only|v4-with-v3-rollback
```

固定发布步骤：

1. **Reader release**：先发布 V4 type/reader/verifier/facade consumer，默认 `legacy-only`；legacy 输入继续写 V3。V2 timeline 在此策略下于 acquisition 前返回明确的 writer-disabled 错误。
2. **Writer canary**：仅在所有 plan consumers 已部署 reader release 后，对 canary 使用 `v4-with-v3-rollback`。
3. **Dual write**：每次 V2 timeline compile 从同一 materialized timeline/execution 同时生成 canonical V4 和 V3 rollback projection；两者都发布成功后命令才报告成功。先成功的孤儿 content-addressed artifact可保留，不更新任何 mutable active pointer。
4. **General enablement**：canary golden/回放验证通过后，在单独发布中把部署默认切换为 `v4-with-v3-rollback`；CLI flag继续保留以便回退。
5. **Rollback**：切回 `legacy-only`，旧 binary 使用报告中保留的 `rollback_v3_plan_sha256`。V4 文件不删除、不覆盖；恢复新版后可继续使用。

V3 projection：

- 复用相同 timeline、budget、proof、catalog identities 和 materialized V3-compatible execution closure。
- `canonical_universe` 固定为 `universe-v2-ast:<normalized_ast_sha256>`。
- `canonicalization_identity` 与 `compiler_identity` 使用新的 projection 常量，但不修改任何已有 V3 artifact。
- V4 identity单向 pin `rollback_v3_plan_sha256`，V3 不反向 pin V4，避免循环 hash。
- V4 与 projection 的 visible membership、dependencies、tick/minute/daily targets必须逐字节等价。
- cache JSON report同时输出 `plan_version=4`、`plan_sha256`、`rollback_plan_version=3`、`rollback_v3_plan_sha256`、language/compiler identities。

### 6.10 Entrypoint capability matrix

| Entrypoint | Legacy | V2 snapshot | V2 timeline |
| --- | --- | --- | --- |
| `tqsdk-cache fill --universe` | 保持现状；historical legacy写 V3 | current snapshot fill | 仅 writer policy允许时编译 V4 + V3 projection |
| `--universe-file` / typed files | 外层兼容，不改 legacy `file:` | 支持，展开后编译 | 支持，file hash进入 V4 identity |
| facade `quotes_universe` | 保持现状 | 支持 | 网络前拒绝 |
| `MarketCachePolicy::record_universe` | 保持现状 | 支持 | 网络前拒绝 |
| relay config/runtime | 保持现状 | 支持，失败保留 last-known-good | 刷新前拒绝 |
| `BacktestBuilder::universe` | 保持静态/current snapshot语义 | 支持 snapshot | 拒绝；不得隐式 acquire |
| `BacktestBuilder::historical_universe_plan` | V1–V3 | 不适用 | 保持旧签名 |
| `BacktestBuilder::historical_universe_artifact` | wrap V1–V3 | 不适用 | 接受并验证 V4 |

### 6.11 Documentation contract

新增 `docs/architecture/universe-language.md`，内容必须包含：

- 完整 EBNF、dispatch 规则与“为什么 shared-only string 需要 `snapshot(...)` 才强制 V2”；
- view/target 表、exclusion matrix、duplicate/contradiction/case/order规则；
- `contract/main/continuous/index` 的身份差异；
- snapshot 与 provider-data timeline 生命周期；
- Universe 和数据流/数据种类的边界；
- legacy alias 与 `file:` 迁移；
- V4 wire/hash/reader-first/rollback contract；
- 正反例和 CLI/facade/relay capability matrix。

同步 `historical-universe-catalog.md`、cache CLI/operations、validation、根 README 与 data/cache/relay/facade README。所有 public API 变化同步 `api_contract_s48_facade_historical_universe.rs`。

## 7. Implementation Sequence

1. **冻结兼容基线。** 为所有当前有效 legacy grammar 增加 parser/display golden；新增两个历史顺序 golden：
   - `timeline(cont:SHFE.au;!symbol:SHFE.au2506)` 保持当前 continuous 被删除；
   - `timeline(!product:SHFE.au;cont:SHFE.au)` 保持当前后续 continuous include 存活。
   同时保存 V1/V2/V3 JSON bytes、hash 和 artifact-chain fixture。未通过前不修改 caller。
2. **实现 V2 AST/parser/normalizer。** 建立 `universe_spec/` 深模块、EBNF、typed errors、canonical wire与 AST hash goldens。Gate：所有 permutation/duplicate/case/order测试产生相同 canonical bytes。
3. **实现 compatibility dispatch 与外层 files。** legacy-first路由、`snapshot(...)` 强制 V2、input report和单次 file expansion/hash；legacy parser/evaluator保持不动。Gate：所有 legacy fixture仍标记 `legacy-v1`。
4. **实现 V2 snapshot compiler。** candidate provenance、exclusion matrix、snapshot capabilities；接入 data typed API。Gate：V2 set-algebra tests通过，legacy resolver tests无变化。
5. **先实现 V4 reader model。** 新类型、flat custom serde、canonical V4 bytes、enum-dispatched store/chain verifier和 facade consumer；writer policy仍为 `legacy-only`。Gate：V1–V3 fixed bytes/hash/read完全不变，V4 reader/unknown-version goldens通过。
6. **实现 V2 timeline compiler。** 使用 pinned catalog/calendar/data-membership evidence生成 logical membership与 kind-specific physical closure；`main/top` 在 acquisition 前 capability-fail。Gate：daily/minute/tick targets、first-available裁剪和预算测试通过。
7. **实现 V4 + V3 projection dual writer。** cache policy/CLI/report、两份 artifact发布、projection equality与 rollback replay。Gate：reader-first、canary、partial-publish孤儿、rollback场景测试通过。
8. **迁移 V2 callers。** data/relay共享 V2 compiler；facade/relay/cache只增加 additive entrypoints；relay compile/file失败保持 last-known-good。Gate：入口 capability matrix全部通过。
9. **同步 public docs/contracts。** 完成 §6.11；检查没有文档把 `main` 与 `continuous`、Universe 与数据流、listing date 与 provider membership混为一谈。
10. **提交前验证。** 运行 §8 全部命令；运行 GitNexus `detect_changes --scope all`，若 `partial` 或 `truncated` 必须重跑，不能视为 clean。暂存区只包含本轮授权文件。

## 8. Test Strategy

### Parser、dispatch、canonicalization

- `crates/tqsdk-data/tests/universe_selector.rs`：保留全部 legacy fixtures；新增 compat dispatch matrix。
- 新建 `crates/tqsdk-data/tests/universe_spec.rs`：
  - 每个合法 view/target；
  - `snapshot(...)`、implicit snapshot、timeline；
  - `cont -> continuous`；
  - illegal view/target、domestic structural parsing、opaque `symbol:`；
  - duplicate dedupe、identical contradiction、mixed-all、top limits；
  - permutation产生相同 canonical JSON/text/hash；
  - exchange case fold、product/contract/symbol case preservation。
- 为 legacy 与 V2 error分别断言稳定 error kind，不依赖完整人类文本。

### Semantic compiler

- snapshot：broad scope过滤 expired，exact contract显式命中 expired；main/top与continuous输出不同 identity。
- exclusion table逐格测试；同一 physical symbol由 contract/main同时产生时，view-only exclusion保留另一 provenance。
- global contract exclusion不移除 continuous/index；global product/exchange exclusion移除所有相应 typed views。
- file：相同内容不同路径 identity相同；顺序/重复不影响；内容变化改变 hash；invalid UTF-8/empty value/读取失败；exclusion作用于展开 symbols；只读一次的 fake loader测试。

### Legacy historical regression

在 `crates/tqsdk-data/tests/historical_fill_universe.rs` 和 `historical_universe_resolution.rs` 增加：

- arbitrary legacy timeline order goldens，而不仅是 `physical:all` equivalence；
- legacy exact physical exclusion继续删除相关 continuous/index；
- 所有 legacy输入继续写 V3、compiler/canonicalization identities不变；
- `physical:all` 保持 legacy V3；与 V2 `timeline(contract:all)` 的 visible membership/targets相等，但 plan version/hash按设计不同。

### V4 wire/artifacts

在 `historical_universe.rs` 与 `historical_universe_artifact.rs` tests固定：

- exact canonical AST JSON bytes与 AST hash；
- exact V4 identity JSON、execution JSON、plan hash preimage/hash；
- exact flat artifact JSON bytes；
- V1/V2/V3既有 bytes/hash fixture逐字节不变；
- V4 round-trip、noncanonical V4 bytes拒绝、unknown version fail-closed；
- 同一 V4重复 publish幂等；相同 plan hash不同 bytes拒绝；
- full acquisition → semantic catalog → V4 plan artifact-chain；
- V3 projection hash固定，V4单向 pin projection；
- V4/projection membership、dependencies、tick/minute/daily targets完全相等；
- old `load_plan` 不误读 V4，新 `load_plan_artifact` 可读 V1–V4。

### Entrypoints、rollout、relay recovery

- `crates/tqsdk-cache/tests/cli.rs`：
  - legacy-only拒绝 V2 timeline且无 acquisition；
  - v4-with-v3-rollback 双发布与 report；
  - snapshot/timeline、kind/market组合；
  - partial second publish只留无害 orphan，不报告成功；
  - rollback hash可被 V3 reader执行；
  - `--universe-file` 单独及与 `--universe` 合用。
- `crates/tqsdk-relay/tests/config.rs`、`universe.rs`、`upstream.rs`：
  - V2 snapshot成功；
  - timeline在 refresh/network前拒绝；
  - parser/compiler/file error保留 last-known-good subscriptions；
  - config/recovery serialisation保持 legacy兼容。
- `crates/tqsdk/tests/facade_contract.rs` 和 S48 contract example：
  - 旧 method签名仍编译；
  - 新 artifact method消费 V4；
  - plan interval mismatch、hash/chain mismatch拒绝；
  - snapshot string/typed API与timeline拒绝。

### Verification commands

```bash
git diff --check
cargo fmt --all --check
cargo test -p tqsdk-data
cargo test -p tqsdk-cache
cargo test -p tqsdk-relay --tests
cargo test -p tqsdk
cargo check --examples
cargo check --no-default-features
cargo check --no-default-features --examples
cargo check --all-features --examples
cargo test --all-features
cargo clippy --examples --all-targets -- -D warnings
cargo clippy -p tqsdk-relay --all-targets -- -D warnings
cargo check -p tqsdk-relay --no-default-features
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features
node .gitnexus/run.cjs detect-changes --scope all --repo .
```

## 9. Risk Impact Analysis

- **HIGH — legacy semantic drift。** 任何把 legacy historical clauses先 normalize再执行的实现都会改变结果。防线：独立 evaluation policy、legacy-first dispatch和两个顺序 golden。
- **HIGH — Rust source compatibility。** 给现有 public struct加字段会破坏下游 literals/destructuring。防线：冻结旧类型，新建 private-field V4类型与 additive methods。
- **HIGH — persisted identity drift。** public serde、map iteration或display text不能作为 V4 hash的隐式契约。防线：private fixed-order wire、domain-separated AST hash、exact-byte goldens。
- **HIGH — downgrade failure。** 旧 binary不识别 V4。防线：reader-first、writer policy、同 execution dual-write V3 projection、报告并保留 rollback hash。
- **HIGH —错误跨视图 exclusion。** final symbol过早去重会丢失 provenance。防线：candidate provenance到 exclusion结束后再 dedupe，逐格 matrix tests。
- **MEDIUM — file TOCTOU/secret leakage。** 重读文件会使 identity与执行不同，日志可能泄露内容。防线：read once、hash bytes、只记录 path/hash/count/error stage。
- **MEDIUM — relay subscription loss。** V2 compile/config错误不能清空当前订阅。防线：compile next state成功后再原子替换，失败保留 last-known-good。
- **MEDIUM — plan体积和编译成本。** provenance与 kind targets可能扩大内存。防线：现有 `UniverseBudget`、sorted unique structures、测试大 catalog预算失败路径；本轮不加第二个缓存。
- **MEDIUM —版本术语混淆。** Language V2与Plan V4必须在类型、常量、report、docs中分开命名。
- **LOW — dual-write orphan。** 第二次publish失败可能留下第一份 immutable artifact；允许保留，由内容寻址GC策略处理，命令不得报告成功或更新引用。

Observability必须包含：input language、evaluation policy、mode、AST hash、compiler/canonicalizer identity、capability identities、plan version/hash、rollback hash、candidate/dependency/target counts、writer policy和失败阶段；不得包含凭证、文件内容或 provider token。

## 10. Files Expected to Change

| File | Symbols/responsibility | Reason |
| --- | --- | --- |
| `crates/tqsdk-data/src/universe_spec/{mod,ast,parser,normalize,source,compiler,compatibility}.rs` (new) | V2 deep module | Typed grammar、canonical bytes、input expansion values、pure compiler |
| `crates/tqsdk-data/src/lib.rs` | curated re-exports | Additive V2/V4 public surface |
| `crates/tqsdk-data/src/universe_expression.rs` | legacy types | 仅补兼容注释/测试辅助；不得改 parse语义 |
| `crates/tqsdk-data/src/universe.rs` | snapshot capability adapter、file loader reuse | Add V2 path，保留 legacy resolver |
| `crates/tqsdk-data/src/historical_fill_universe.rs` | legacy historical parser | 冻结并暴露 dispatch证据 |
| `crates/tqsdk-data/src/historical_universe_resolution.rs` | legacy V3 compiler | 冻结当前 evaluation；必要时抽取不改变结果的共享 materialization |
| `crates/tqsdk-data/src/historical_universe_v4.rs` (new) | V4 types/compiler/projection | 避免修改旧 public plan结构 |
| `crates/tqsdk-data/src/historical_universe.rs` | old type integration only | 保持 V1–V3 bytes/hash；最小 additive bridge |
| `crates/tqsdk-data/src/historical_universe_artifact.rs` | versioned store/chain | V1–V4 enum dispatch与 canonical V4 bytes |
| `crates/tqsdk-data/tests/{universe_selector,universe_spec,historical_fill_universe,historical_universe,historical_universe_resolution,historical_universe_artifact}.rs` | goldens/semantics | §8 coverage |
| `crates/tqsdk-cache/src/main.rs` | CLI dispatch、writer policy、dual publish/report | Cache orchestration |
| `crates/tqsdk-cache/tests/cli.rs` | CLI/rollout/rollback | §8 integration |
| `crates/tqsdk-relay/src/universe_expression.rs` | V2 re-export | Shared data language types |
| `crates/tqsdk-relay/src/universe.rs` | SnapshotCapabilities adapter | V2 current resolution，legacy untouched |
| `crates/tqsdk-relay/src/config.rs` | typed universe/files config | Additive configuration |
| `crates/tqsdk-relay/src/runtime.rs` | compile-before-swap recovery | last-known-good |
| `crates/tqsdk-relay/src/lib.rs` | public builder/re-export | Additive typed entrypoints |
| `crates/tqsdk-relay/tests/{config,universe,upstream}.rs` | parser/compiler/recovery | §8 integration |
| `crates/tqsdk/src/lib.rs` | compat dispatch、typed snapshot、artifact consumer | Preserve old signatures，add V4 |
| `crates/tqsdk/tests/facade_contract.rs` | public API contract | Source compatibility与 capability matrix |
| `crates/tqsdk/examples/api_contract_s48_facade_historical_universe.rs` | scenario contract | 展示 V1–V4 consumption |
| 根及各 crate `README.md` | user-facing syntax/migration | Public API同步 |
| `docs/architecture/universe-language.md` (new) | authoritative DSL contract | §6.11 |
| `docs/architecture/{README,historical-universe-catalog,backtest-tick-cache-cli,backtest-tick-cache-operations,validation}.md` | authority/ops/validation | Architecture update同步 |

不修改 `tqsdk-core` runtime/state tree、session ownership、DIFF protocol或 relay history listener边界。

## 11. Reusable Implementation Context

```json
{
  "implementation_context": {
    "task_summary": "Introduce an additive Universe Language V2 with legacy-first dispatch, deterministic typed semantics, source-compatible Historical Plan V4, reader-first dual-write rollout, and verified snapshot/timeline entrypoint capabilities.",
    "acceptance_criteria": [
      "Every currently valid legacy expression remains on LegacySequentialV1 and preserves results, display, V1-V3 bytes and hashes.",
      "V2 grammar, view/scope/exclusion matrix and canonical AST byte contract are deterministic and golden-tested.",
      "Snapshot contract:all means current eligible physicals; timeline contract:all means provider-data membership intersecting the requested range.",
      "HistoricalUniversePlan remains source- and wire-compatible; V4 uses new private-field types and flat versioned artifact dispatch.",
      "V2 timeline writes canonical V4 only after reader rollout and also publishes an execution-equivalent V3 rollback projection.",
      "Facade and relay reject timeline before acquisition; dynamic backtest accepts only verified interval-matched artifacts.",
      "All focused, all-feature, no-default-feature, clippy, rustdoc and graph-change gates pass."
    ],
    "evidence_provenance": {"schema_version":2,"head_commit":"eaebae7beba7b57e5fc8d158c8606d50d48312a2","generated_plan_path":"docs/plans/2026-08-30-gitnexus-plan-universe-dsl-evolution.md","global_dirty_digest":{"algorithm":"sha256","canonicalization":"gitnexus-evidence-provenance-v2 NUL-framed UTF-8 records","value":"0a9c85780067d9afcd0764f307b60891e3cee927ee11eaeb5ec7826d10fd82cd"},"cited_path_manifest":[{"path":"Cargo.toml","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:f75171675999ca781a5e0da3c7c887ba2362e0b95e9736c8c69c4d420b507b54","index_digest":"sha256:f75171675999ca781a5e0da3c7c887ba2362e0b95e9736c8c69c4d420b507b54","worktree_digest":"sha256:f75171675999ca781a5e0da3c7c887ba2362e0b95e9736c8c69c4d420b507b54","untracked_digest":"absent"},{"path":"README.md","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:376c1a9fb0ab966fa878b8da9d1632ff69c58e0470e282365a1b4dcd25034f2d","index_digest":"sha256:376c1a9fb0ab966fa878b8da9d1632ff69c58e0470e282365a1b4dcd25034f2d","worktree_digest":"sha256:376c1a9fb0ab966fa878b8da9d1632ff69c58e0470e282365a1b4dcd25034f2d","untracked_digest":"absent"},{"path":"crates/tqsdk-cache/README.md","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:d8908e4c555f9e5783fa0b6ad13113a9f90e98104cf716046a058017cc601e17","index_digest":"sha256:d8908e4c555f9e5783fa0b6ad13113a9f90e98104cf716046a058017cc601e17","worktree_digest":"sha256:d8908e4c555f9e5783fa0b6ad13113a9f90e98104cf716046a058017cc601e17","untracked_digest":"absent"},{"path":"crates/tqsdk-cache/src/main.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:4d01ae35ea2b1584b06eda1fa9c9d41facc52eed11840cca4c405ebc9e6a42bb","index_digest":"sha256:4d01ae35ea2b1584b06eda1fa9c9d41facc52eed11840cca4c405ebc9e6a42bb","worktree_digest":"sha256:4d01ae35ea2b1584b06eda1fa9c9d41facc52eed11840cca4c405ebc9e6a42bb","untracked_digest":"absent"},{"path":"crates/tqsdk-cache/tests/cli.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:a1bf9a40b414bdef5507995abeda74bafcc23fff3f355ea8eff2f282779fbe59","index_digest":"sha256:a1bf9a40b414bdef5507995abeda74bafcc23fff3f355ea8eff2f282779fbe59","worktree_digest":"sha256:a1bf9a40b414bdef5507995abeda74bafcc23fff3f355ea8eff2f282779fbe59","untracked_digest":"absent"},{"path":"crates/tqsdk-data/README.md","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:da4a3f1aa4cca1173cf68c422bffaf72b838cab21484415a52eb335574937fdb","index_digest":"sha256:da4a3f1aa4cca1173cf68c422bffaf72b838cab21484415a52eb335574937fdb","worktree_digest":"sha256:da4a3f1aa4cca1173cf68c422bffaf72b838cab21484415a52eb335574937fdb","untracked_digest":"absent"},{"path":"crates/tqsdk-data/src/historical_fill_universe.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:31a9866664d4be17cb91f1856957d4c9cbf2a014185a1f07e2a5d5c4209a2f08","index_digest":"sha256:31a9866664d4be17cb91f1856957d4c9cbf2a014185a1f07e2a5d5c4209a2f08","worktree_digest":"sha256:31a9866664d4be17cb91f1856957d4c9cbf2a014185a1f07e2a5d5c4209a2f08","untracked_digest":"absent"},{"path":"crates/tqsdk-data/src/historical_universe.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:7554c014c57417fafd1a951093748334fb1fdde3c69944b75b9f302b1193bd4f","index_digest":"sha256:7554c014c57417fafd1a951093748334fb1fdde3c69944b75b9f302b1193bd4f","worktree_digest":"sha256:7554c014c57417fafd1a951093748334fb1fdde3c69944b75b9f302b1193bd4f","untracked_digest":"absent"},{"path":"crates/tqsdk-data/src/historical_universe_artifact.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:46c659f09b47de565e0aa7f53500625fb1e6b15981483ec99f88fd538c424098","index_digest":"sha256:46c659f09b47de565e0aa7f53500625fb1e6b15981483ec99f88fd538c424098","worktree_digest":"sha256:46c659f09b47de565e0aa7f53500625fb1e6b15981483ec99f88fd538c424098","untracked_digest":"absent"},{"path":"crates/tqsdk-data/src/historical_universe_resolution.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:d8e2588c92687eddc8f7adb7ef394b8c8918dd2a137144f5fbc5dc7dbfe794f7","index_digest":"sha256:d8e2588c92687eddc8f7adb7ef394b8c8918dd2a137144f5fbc5dc7dbfe794f7","worktree_digest":"sha256:d8e2588c92687eddc8f7adb7ef394b8c8918dd2a137144f5fbc5dc7dbfe794f7","untracked_digest":"absent"},{"path":"crates/tqsdk-data/src/lib.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:17eadac81e6bdb2e29e833ea767712bece62bf014bf6ced736825e98bbd0fcec","index_digest":"sha256:17eadac81e6bdb2e29e833ea767712bece62bf014bf6ced736825e98bbd0fcec","worktree_digest":"sha256:17eadac81e6bdb2e29e833ea767712bece62bf014bf6ced736825e98bbd0fcec","untracked_digest":"absent"},{"path":"crates/tqsdk-data/src/universe.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:c79c42ef536d025cb6141ee15c537b4e801d78f33fa6ac182ffd2adde27c7464","index_digest":"sha256:c79c42ef536d025cb6141ee15c537b4e801d78f33fa6ac182ffd2adde27c7464","worktree_digest":"sha256:c79c42ef536d025cb6141ee15c537b4e801d78f33fa6ac182ffd2adde27c7464","untracked_digest":"absent"},{"path":"crates/tqsdk-data/src/universe_expression.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:042f9722ca1f0e08660682f82d56fdbb5d2fbd8e975bc1a93c5fdeb30649ec87","index_digest":"sha256:042f9722ca1f0e08660682f82d56fdbb5d2fbd8e975bc1a93c5fdeb30649ec87","worktree_digest":"sha256:042f9722ca1f0e08660682f82d56fdbb5d2fbd8e975bc1a93c5fdeb30649ec87","untracked_digest":"absent"},{"path":"crates/tqsdk-data/tests/historical_fill_universe.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:fba1dd4964beb284d9f87e89c1f9c8f7804af83b23f403a30bb4a67b664bcb00","index_digest":"sha256:fba1dd4964beb284d9f87e89c1f9c8f7804af83b23f403a30bb4a67b664bcb00","worktree_digest":"sha256:fba1dd4964beb284d9f87e89c1f9c8f7804af83b23f403a30bb4a67b664bcb00","untracked_digest":"absent"},{"path":"crates/tqsdk-data/tests/historical_universe.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:d910a889c6666b07db3b4e8a2c4193d1a9651e11f9b44227f74cb97daee2813f","index_digest":"sha256:d910a889c6666b07db3b4e8a2c4193d1a9651e11f9b44227f74cb97daee2813f","worktree_digest":"sha256:d910a889c6666b07db3b4e8a2c4193d1a9651e11f9b44227f74cb97daee2813f","untracked_digest":"absent"},{"path":"crates/tqsdk-data/tests/historical_universe_artifact.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:e9ea72a5753257cdfb82516abe52d69674c28ce404f2eaefb5cbda532bfeacc3","index_digest":"sha256:e9ea72a5753257cdfb82516abe52d69674c28ce404f2eaefb5cbda532bfeacc3","worktree_digest":"sha256:e9ea72a5753257cdfb82516abe52d69674c28ce404f2eaefb5cbda532bfeacc3","untracked_digest":"absent"},{"path":"crates/tqsdk-data/tests/historical_universe_resolution.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:94ccdd7a15cedcff28c41788d0fcca4d1993edd91f236350a506a1e46a5cc708","index_digest":"sha256:94ccdd7a15cedcff28c41788d0fcca4d1993edd91f236350a506a1e46a5cc708","worktree_digest":"sha256:94ccdd7a15cedcff28c41788d0fcca4d1993edd91f236350a506a1e46a5cc708","untracked_digest":"absent"},{"path":"crates/tqsdk-data/tests/universe_selector.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:c399169d9c2fd311d28970664173b0a864944b2471152daf381f8e8ba443681c","index_digest":"sha256:c399169d9c2fd311d28970664173b0a864944b2471152daf381f8e8ba443681c","worktree_digest":"sha256:c399169d9c2fd311d28970664173b0a864944b2471152daf381f8e8ba443681c","untracked_digest":"absent"},{"path":"crates/tqsdk-relay/README.md","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:70c002212b18f8dc37e3cf3573f0a05ef3e0965e889cc0a14576b7e91788925e","index_digest":"sha256:70c002212b18f8dc37e3cf3573f0a05ef3e0965e889cc0a14576b7e91788925e","worktree_digest":"sha256:70c002212b18f8dc37e3cf3573f0a05ef3e0965e889cc0a14576b7e91788925e","untracked_digest":"absent"},{"path":"crates/tqsdk-relay/src/config.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:cddc3ccf915d88e9f5d314cf482a30361fb9dc8c4132a88cd6ca80bca8e7e3c8","index_digest":"sha256:cddc3ccf915d88e9f5d314cf482a30361fb9dc8c4132a88cd6ca80bca8e7e3c8","worktree_digest":"sha256:cddc3ccf915d88e9f5d314cf482a30361fb9dc8c4132a88cd6ca80bca8e7e3c8","untracked_digest":"absent"},{"path":"crates/tqsdk-relay/src/lib.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:d378bf001c4186c48c715d8d366c07f7c213391d56e2a437918d79361ba11edf","index_digest":"sha256:d378bf001c4186c48c715d8d366c07f7c213391d56e2a437918d79361ba11edf","worktree_digest":"sha256:d378bf001c4186c48c715d8d366c07f7c213391d56e2a437918d79361ba11edf","untracked_digest":"absent"},{"path":"crates/tqsdk-relay/src/runtime.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:4c23e3e85dda9098f208b31864cd5450c643460d8e5b9bb30d46ef7ae277e943","index_digest":"sha256:4c23e3e85dda9098f208b31864cd5450c643460d8e5b9bb30d46ef7ae277e943","worktree_digest":"sha256:4c23e3e85dda9098f208b31864cd5450c643460d8e5b9bb30d46ef7ae277e943","untracked_digest":"absent"},{"path":"crates/tqsdk-relay/src/universe.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:a6892e8fe3234cbcfa3f045d9f043f573572610e8e82509264a983f51bc9f87a","index_digest":"sha256:a6892e8fe3234cbcfa3f045d9f043f573572610e8e82509264a983f51bc9f87a","worktree_digest":"sha256:a6892e8fe3234cbcfa3f045d9f043f573572610e8e82509264a983f51bc9f87a","untracked_digest":"absent"},{"path":"crates/tqsdk-relay/src/universe_expression.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:169b59c44a4c4aab39d20ff0f44ed553fdc9228a23bbdf0966387a1d9ef8fe87","index_digest":"sha256:169b59c44a4c4aab39d20ff0f44ed553fdc9228a23bbdf0966387a1d9ef8fe87","worktree_digest":"sha256:169b59c44a4c4aab39d20ff0f44ed553fdc9228a23bbdf0966387a1d9ef8fe87","untracked_digest":"absent"},{"path":"crates/tqsdk-relay/tests/config.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:e6353240f99062cb30d2df6361522c9604426720b262f72e41742b9f04e99915","index_digest":"sha256:e6353240f99062cb30d2df6361522c9604426720b262f72e41742b9f04e99915","worktree_digest":"sha256:e6353240f99062cb30d2df6361522c9604426720b262f72e41742b9f04e99915","untracked_digest":"absent"},{"path":"crates/tqsdk-relay/tests/universe.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:933b4a10ad09bbe2b498211dacd55266b267add755611b2e5fc21f24b2111c19","index_digest":"sha256:933b4a10ad09bbe2b498211dacd55266b267add755611b2e5fc21f24b2111c19","worktree_digest":"sha256:933b4a10ad09bbe2b498211dacd55266b267add755611b2e5fc21f24b2111c19","untracked_digest":"absent"},{"path":"crates/tqsdk-relay/tests/upstream.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:0a4176c0bfccd9d6142b26dbba9087e3470372573c8677b0fae05eaf50cc5796","index_digest":"sha256:0a4176c0bfccd9d6142b26dbba9087e3470372573c8677b0fae05eaf50cc5796","worktree_digest":"sha256:0a4176c0bfccd9d6142b26dbba9087e3470372573c8677b0fae05eaf50cc5796","untracked_digest":"absent"},{"path":"crates/tqsdk/README.md","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:aac202a3f99c8b57402bb7e4c268f5a4a5d5ad80e9a1ab1363d81d475a2fef49","index_digest":"sha256:aac202a3f99c8b57402bb7e4c268f5a4a5d5ad80e9a1ab1363d81d475a2fef49","worktree_digest":"sha256:aac202a3f99c8b57402bb7e4c268f5a4a5d5ad80e9a1ab1363d81d475a2fef49","untracked_digest":"absent"},{"path":"crates/tqsdk/examples/api_contract_s48_facade_historical_universe.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:b63172e5d3cf6ab871f56e139fa3a7997d7cf7a9852aa5fcebcf37f7e932e2fa","index_digest":"sha256:b63172e5d3cf6ab871f56e139fa3a7997d7cf7a9852aa5fcebcf37f7e932e2fa","worktree_digest":"sha256:b63172e5d3cf6ab871f56e139fa3a7997d7cf7a9852aa5fcebcf37f7e932e2fa","untracked_digest":"absent"},{"path":"crates/tqsdk/src/lib.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:90defb74a0d88afeac44a8a2e190476ec6d014982570613c3df8c535db48ef90","index_digest":"sha256:90defb74a0d88afeac44a8a2e190476ec6d014982570613c3df8c535db48ef90","worktree_digest":"sha256:90defb74a0d88afeac44a8a2e190476ec6d014982570613c3df8c535db48ef90","untracked_digest":"absent"},{"path":"crates/tqsdk/tests/facade_contract.rs","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:bd52e10067803989cf7c8e92a77660c2deccaedf44b7d54bc7fd6f460d9cc1d4","index_digest":"sha256:bd52e10067803989cf7c8e92a77660c2deccaedf44b7d54bc7fd6f460d9cc1d4","worktree_digest":"sha256:bd52e10067803989cf7c8e92a77660c2deccaedf44b7d54bc7fd6f460d9cc1d4","untracked_digest":"absent"},{"path":"docs/architecture/README.md","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:a5294fb132dfbd20763a1879cdcefa89e8d60561f456660d6c17102374923777","index_digest":"sha256:a5294fb132dfbd20763a1879cdcefa89e8d60561f456660d6c17102374923777","worktree_digest":"sha256:a5294fb132dfbd20763a1879cdcefa89e8d60561f456660d6c17102374923777","untracked_digest":"absent"},{"path":"docs/architecture/backtest-tick-cache-cli.md","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:90869862632f7e1e08d676d4859e9b2e80ceedfdc65c77b041c7b8a159f12402","index_digest":"sha256:90869862632f7e1e08d676d4859e9b2e80ceedfdc65c77b041c7b8a159f12402","worktree_digest":"sha256:90869862632f7e1e08d676d4859e9b2e80ceedfdc65c77b041c7b8a159f12402","untracked_digest":"absent"},{"path":"docs/architecture/backtest-tick-cache-operations.md","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:7e88335e202cd0a45c138ac8454fb47f41a9fae35ac310d565e8774895b27afc","index_digest":"sha256:7e88335e202cd0a45c138ac8454fb47f41a9fae35ac310d565e8774895b27afc","worktree_digest":"sha256:7e88335e202cd0a45c138ac8454fb47f41a9fae35ac310d565e8774895b27afc","untracked_digest":"absent"},{"path":"docs/architecture/crate-boundaries.md","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:07488d245126ebc58489ed44e003f04dfa69f394a8686932ff925a90a75dc112","index_digest":"sha256:07488d245126ebc58489ed44e003f04dfa69f394a8686932ff925a90a75dc112","worktree_digest":"sha256:07488d245126ebc58489ed44e003f04dfa69f394a8686932ff925a90a75dc112","untracked_digest":"absent"},{"path":"docs/architecture/historical-universe-catalog.md","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:e2773ed4d769029214615af908a2eefe8b3eef0170c34fddbb18c24b4af83824","index_digest":"sha256:e2773ed4d769029214615af908a2eefe8b3eef0170c34fddbb18c24b4af83824","worktree_digest":"sha256:e2773ed4d769029214615af908a2eefe8b3eef0170c34fddbb18c24b4af83824","untracked_digest":"absent"},{"path":"docs/architecture/universe-language.md","object_kind":{"head":"absent","index":"absent","worktree":"absent","untracked":"absent"},"state":"absent","rename_from":null,"rename_to":null,"head_digest":"absent","index_digest":"absent","worktree_digest":"absent","untracked_digest":"absent"},{"path":"docs/architecture/validation.md","object_kind":{"head":"regular","index":"regular","worktree":"regular","untracked":"absent"},"state":"clean","rename_from":null,"rename_to":null,"head_digest":"sha256:7368faa8ae5aa2f52cccb1b2741757ef038bbf0917b7f07edd874419633d14b7","index_digest":"sha256:7368faa8ae5aa2f52cccb1b2741757ef038bbf0917b7f07edd874419633d14b7","worktree_digest":"sha256:7368faa8ae5aa2f52cccb1b2741757ef038bbf0917b7f07edd874419633d14b7","untracked_digest":"absent"}]},
    "primary_symbols": [
      {
        "symbol": "UniverseExpression::parse",
        "file": "crates/tqsdk-data/src/universe_expression.rs",
        "lines": "13-29, 141-239",
        "role": "Frozen legacy grammar and dispatch compatibility boundary"
      },
      {
        "symbol": "resolve_selection",
        "file": "crates/tqsdk-data/src/historical_universe_resolution.rs",
        "lines": "159-205",
        "role": "Frozen clause-ordered historical evaluation"
      },
      {
        "symbol": "HistoricalUniversePlan",
        "file": "crates/tqsdk-data/src/historical_universe.rs",
        "lines": "520-804",
        "role": "Frozen V1-V3 Rust/wire/hash contract"
      },
      {
        "symbol": "HistoricalUniverseArtifactStore",
        "file": "crates/tqsdk-data/src/historical_universe_artifact.rs",
        "lines": "762-934",
        "role": "Content-addressed publication and versioned chain verification"
      },
      {
        "symbol": "BacktestBuilder::historical_universe_plan",
        "file": "crates/tqsdk/src/lib.rs",
        "lines": "2899-2926",
        "role": "Existing source-compatible historical consumer"
      }
    ],
    "related_symbols": [
      {
        "symbol": "HistoricalFillUniverseSpec::parse",
        "relationship": "legacy historical dispatch",
        "relevance": "Must win before V2 parsing for every currently valid timeline expression."
      },
      {
        "symbol": "resolve_futures_contracts_with_expression",
        "relationship": "legacy snapshot resolver",
        "relevance": "Remains unchanged while a parallel V2 SnapshotCapabilities path is added."
      },
      {
        "symbol": "tqsdk-relay runtime refresh",
        "relationship": "current subscription consumer",
        "relevance": "Compile next state before replacing last-known-good subscriptions."
      },
      {
        "symbol": "tqsdk-cache fill",
        "relationship": "historical acquisition/writer orchestrator",
        "relevance": "Owns writer policy, V4/V3 dual publish and reports."
      }
    ],
    "execution_path": [
      "Entrypoint classifies physical macro, valid legacy input, forced snapshot V2 or legacy-rejected V2 without rerouting old strings.",
      "V2 input files are read once, hashed and expanded outside the pure compiler.",
      "Normalizer emits explicit-mode typed AST and fixed canonical bytes/hash.",
      "Snapshot or timeline capability adapter supplies only the evidence allowed for that mode.",
      "Compiler retains candidate provenance, unions includes, applies the fixed exclusion matrix, then deduplicates final symbols.",
      "Timeline compiler materializes membership, dependency closure and tick/minute/daily targets.",
      "Writer publishes canonical V4 and execution-equivalent V3 rollback projection; consumers verify the versioned artifact chain."
    ],
    "pdg_constraints": [
      {
        "description": "Historical proof and semantic gates precede selection and plan construction.",
        "affected_statements": [
          "crates/tqsdk-data/src/historical_universe_resolution.rs:67-99"
        ],
        "implementation_consequence": "Unsupported timeline main/top and disabled V4 writer must fail before adapter acquisition or provider I/O."
      },
      {
        "description": "Legacy historical clause order mutates selection state.",
        "affected_statements": [
          "crates/tqsdk-data/src/historical_universe_resolution.rs:159-205"
        ],
        "implementation_consequence": "Do not normalize or reorder legacy clauses; V2 set algebra is a separate policy."
      },
      {
        "description": "Plan hash preimage is selected by plan version and artifact publication serializes after verification.",
        "affected_statements": [
          "crates/tqsdk-data/src/historical_universe.rs:672-804",
          "crates/tqsdk-data/src/historical_universe_artifact.rs:829-934"
        ],
        "implementation_consequence": "Add explicit V4 types/branches and canonical-byte goldens before enabling writers."
      }
    ],
    "architectural_patterns": [
      {
        "pattern": "Data owns pure language/history artifacts; cache owns acquisition; facade/task consume verified plans.",
        "example_location": "docs/architecture/historical-universe-catalog.md:80-117",
        "usage_guidance": "No network, session or filesystem access inside Universe compiler."
      },
      {
        "pattern": "Relay refresh is current-only and preserves last-known-good state on failure.",
        "example_location": "crates/tqsdk-relay/src/runtime.rs",
        "usage_guidance": "Compile and validate replacement subscriptions before swapping runtime state."
      },
      {
        "pattern": "Public contract examples are executable API specifications.",
        "example_location": "crates/tqsdk/examples/api_contract_s48_facade_historical_universe.rs",
        "usage_guidance": "Keep old plan method compiling and demonstrate the additive versioned artifact method."
      }
    ],
    "files_to_modify": [
      {
        "file": "crates/tqsdk-data/src/universe_spec/",
        "symbols": ["UniverseSpec", "UniverseInput", "NormalizedUniverseAst", "SnapshotCapabilities", "TimelineCapabilities"],
        "intended_change": "Add the V2 deep module and pure semantic compiler."
      },
      {
        "file": "crates/tqsdk-data/src/historical_universe_v4.rs",
        "symbols": ["HistoricalUniversePlanV4", "HistoricalUniversePlanArtifact", "HistoricalUniverseResolutionV4"],
        "intended_change": "Add source-compatible V4 types, fixed wire/hash and V3 rollback projection."
      },
      {
        "file": "crates/tqsdk-data/src/historical_universe_artifact.rs",
        "symbols": ["HistoricalUniverseArtifactStore"],
        "intended_change": "Add explicit V1-V4 artifact dispatch while preserving old methods."
      },
      {
        "file": "crates/tqsdk-cache/src/main.rs",
        "symbols": ["fill routing", "HistoricalPlanWritePolicy", "fill report"],
        "intended_change": "Legacy-first dispatch and reader-first V4/V3 dual publication."
      },
      {
        "file": "crates/tqsdk-relay/src/universe.rs",
        "symbols": ["V2 snapshot adapter"],
        "intended_change": "Use shared V2 compiler without altering legacy resolution."
      },
      {
        "file": "crates/tqsdk-relay/src/runtime.rs",
        "symbols": ["universe refresh/recovery"],
        "intended_change": "Preserve last-known-good state when V2 or file expansion fails."
      },
      {
        "file": "crates/tqsdk/src/lib.rs",
        "symbols": ["string universe entrypoints", "BacktestBuilder historical artifact method"],
        "intended_change": "Additive V2 snapshot and V4 consumer APIs with old signatures preserved."
      },
      {
        "file": "docs/architecture/universe-language.md",
        "symbols": ["authoritative grammar and compatibility contract"],
        "intended_change": "Document all resolved semantic, wire and rollout decisions."
      }
    ],
    "tests": [
      {
        "file": "crates/tqsdk-data/tests/universe_spec.rs",
        "scenarios": [
          "grammar and legal view-target matrix",
          "canonical permutations and exact AST bytes/hash",
          "exclusion provenance matrix",
          "file content identity"
        ]
      },
      {
        "file": "crates/tqsdk-data/tests/historical_universe_resolution.rs",
        "scenarios": [
          "legacy clause-order goldens",
          "provider-data timeline contract:all",
          "V4 and V3 projection target equality"
        ]
      },
      {
        "file": "crates/tqsdk-data/tests/historical_universe_artifact.rs",
        "scenarios": [
          "V1-V3 byte preservation",
          "V4 canonical bytes/hash/chain",
          "dual-version coexistence and unknown-version rejection"
        ]
      },
      {
        "file": "crates/tqsdk-cache/tests/cli.rs",
        "scenarios": [
          "reader-only gate",
          "V4/V3 dual publish and rollback report",
          "repeatable universe files",
          "partial publication behavior"
        ]
      },
      {
        "file": "crates/tqsdk-relay/tests/upstream.rs",
        "scenarios": [
          "V2 snapshot refresh",
          "timeline pre-network rejection",
          "last-known-good retention"
        ]
      },
      {
        "file": "crates/tqsdk/tests/facade_contract.rs",
        "scenarios": [
          "old methods remain source-compatible",
          "new V4 artifact consumer verifies hash/chain/range"
        ]
      }
    ],
    "verification_commands": [
      "git diff --check",
      "cargo fmt --all --check",
      "cargo test -p tqsdk-data",
      "cargo test -p tqsdk-cache",
      "cargo test -p tqsdk-relay --tests",
      "cargo test -p tqsdk",
      "cargo check --examples",
      "cargo check --no-default-features",
      "cargo check --no-default-features --examples",
      "cargo check --all-features --examples",
      "cargo test --all-features",
      "cargo clippy --examples --all-targets -- -D warnings",
      "cargo clippy -p tqsdk-relay --all-targets -- -D warnings",
      "cargo check -p tqsdk-relay --no-default-features",
      "RUSTDOCFLAGS=\"-D warnings\" cargo doc --no-deps --all-features",
      "node .gitnexus/run.cjs detect-changes --scope all --repo ."
    ],
    "risks": [
      "Legacy timeline silently changes if clauses are normalized or reordered.",
      "Existing public plan struct cannot receive V4 fields without a source break.",
      "V4 identity is not reproducible unless private fixed wire bytes and exact hash preimages are golden-tested.",
      "Old consumers cannot read V4 without reader-first rollout and retained V3 projection.",
      "Candidate provenance must survive until exclusions finish.",
      "Relay must not clear subscriptions on compile or file-expansion failure."
    ],
    "assumptions": [
      "Re-verify at execution start that HEAD and the schema-2 evidence digest still match this plan.",
      "Re-run the two legacy order examples against the baseline before touching resolver code; captured outputs become immutable goldens.",
      "Confirm all consumers that may read generated plans have deployed V4 reader support before changing writer policy."
    ],
    "open_questions": [],
    "avoid": [
      "Do not add fields to HistoricalUniversePlan or its V3 identity/execution structs.",
      "Do not route a currently valid legacy expression through V2 semantics.",
      "Do not hash Display output or arbitrary public serde structs for V4 identity.",
      "Do not perform file, network, metadata refresh or ranking queries inside the pure compiler.",
      "Do not use current ranking for timeline main/top.",
      "Do not overwrite or delete V1-V4 content-addressed artifacts during rollout.",
      "Do not change tqsdk-core runtime/state tree or DIFF protocol for this feature."
    ]
  }
}
```

## 12. Assumptions, Open Questions, and Deferred Work

### Assumptions to re-verify before execution

- [verified at pinned commit] Existing public plan fields and legacy parser/evaluator behavior match §2; executor must re-anchor if HEAD or dirty digest changes.
- [assumed] Downstream users may construct/destructure `HistoricalUniversePlan` directly even if repository-local graph does not see them; preserve source compatibility accordingly.
- [assumed] Deployment can stage reader and writer policy independently. If packaging cannot do this, writer must remain `legacy-only` until an equivalent operational gate exists.

### Blocking open questions

None. Duplicate policy、cross-view exclusion、`contract` lifecycle、parser dispatch、V4 Rust representation、hash bytes、file ownership and rollback path are all decided in this revision.

### Explicitly deferred follow-ups

- 获取并 hash-pin historical ranking artifact；在此之前 `timeline(main/top)` 保持 capability error。
- 在未来 major release评估移除 legacy `active/physical/cont/exchange/product/file/Auto/~` 和取消 legacy-first dispatch；本轮只新增迁移路径。
- 交易所 authoritative listing reference data不进入本轮；provider-data membership继续是历史选择依据。
- V4稳定运行并完成保留期后，再单独决策何时停止 V3 projection dual-write；本轮不得提前省略。

## 13. Definition Done

- 所有当前有效 legacy 表达式仍由 legacy parser/evaluator执行；两个顺序敏感历史例子和 V1–V3 bytes/hash goldens通过。
- V2 EBNF、合法 view-target、case/alias、duplicate/contradiction、ordering 和完整 exclusion matrix已写入权威文档并由表驱动测试覆盖。
- `contract:all` 的 snapshot/current eligibility 与 timeline/provider-data membership语义分别可验证；exact expired contract规则明确。
- V2 canonical AST JSON bytes、AST hash、V4 identity/execution/plan preimage/hash和flat artifact bytes都有固定 golden。
- 现有 `HistoricalUniversePlan` 与旧 methods保持源码兼容；新 V4 private-field types、artifact enum和consumer API可编译。
- V4 reader先于writer可部署；writer policy、V4/V3 dual publish、rollback hash和旧 reader replay场景通过。
- facade/relay在任何 acquisition前拒绝 timeline；relay失败保留 last-known-good；动态回测只消费已验证且区间匹配的 artifact。
- Universe 与 tick/minute/daily 数据流保持正交；kind-specific targets在 execution closure中验证。
- §8 全部命令成功，public docs/examples同步，GitNexus detect-changes结果完整且所有直接消费者已核对。

