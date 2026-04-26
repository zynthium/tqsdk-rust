# `tqsdk-core`

面向天勤 / TQSDK 官方服务交互的低层异步 runtime substrate。

这是 Rust 重写版 TQSDK 的 V1 核心基座，目标用户不是普通终端研究人员，而是对性能、稳定性和抽象边界有明确要求的上层 SDK / facade / 工具开发者。

> [!IMPORTANT]
> 这是一个纯 async substrate。crate 内部不会创建 Tokio runtime。凡是涉及 auth、HTTP、websocket、重连退避、live session 驱动的路径，调用方都必须自行提供 Tokio runtime。

> [!NOTE]
> 这个 crate 不是 `TqApi`，不是 `wait_update()` SDK，也不是 stream / callback SDK。后续这些高层能力应建立在这个 core 之上，而不是反过来侵入内核。

## 这个 Crate 提供什么

- 覆盖 market diff、trade、replay、query、schema、auth、session、system 的 protocol-complete runtime contract。
- 一套统一命令模型：`RuntimeCommand -> OutboundDispatch -> RuntimeInput -> NormalizedMutation -> CommitResult`。
- 一棵统一的 runtime state tree，用于承载所有上层可见状态。
- 以 `RuntimeReader`、`SnapshotReadGuard`、`CommitReadGuard`、`UpdateCursor` 为核心的 reader-first 消费模型。
- 官方对象与相关 metadata/query 结果的 typed schema contract。
- transport、auth、topology bootstrap、HTTP executor、session orchestration 等底层原语。

## 依赖方式

Cargo 包名是 `tqsdk-core`，代码里的 crate 路径是 `tqsdk_core`。

```toml
[dependencies]
tqsdk-core = { path = "../tqsdk-rust/crates/tqsdk-core" }
tokio = { version = "1", features = ["macros", "rt", "time"] }
```

如果后续单独发布到 crates.io，把这里的 `path` 换成版本号即可。

## 明确不做什么

- 不提供高层用户便利 API。
- 不提供 `wait_update()` facade。
- 不提供 stream / callback facade。
- 不提供策略辅助、任务封装、GUI / 报表、下载器、DataFrame / polars 集成、富视图层。
- 不提供绕过 commit / revision 模型的旁路结果通道。

## 覆盖范围

当前核心层的目标是覆盖天勤官方服务交互所需的全部协议域：

- DIFF 协议对象。
- trade 命令与命令状态。
- replay / feed 推进。
- auth / session / system 生命周期控制。
- GraphQL / HTTP query。
- schema / metadata / bootstrap 交互。

当前已纳入核心的 typed schema 主要包括：

- 市场对象：`Quote`、`Kline`、`Tick`、`Chart`、`ChartInfo`、`TradingTime`。
- 交易对象：`Account`、`Position`、`Order`、`Trade`、`PreInsertOrder`、`Notification`、`SettlementInfo`。
- 风控对象：`RiskManagementRule`、`RiskManagementData`、`SelfTrade`、`FrequentCancellation`、`TradePositionRatio`。
- 证券对象：`SecurityAccount`、`SecurityPosition`、`SecurityOrder`、`SecurityTrade`。
- 查询 / 元数据对象：`TradingStatus`、`SymbolSettlement`、`SymbolRanking`、`TradingCalendarDay`、`EdbIndexData`。

## 核心公开面

| API | 角色 |
| --- | --- |
| `RuntimeHandle` | 写侧入口，负责命令提交、输入摄取、命令状态与 session 状态投影 |
| `RuntimeReader` | 标准读侧入口 |
| `SnapshotReadGuard` / `StateReadView` | revision-bound 的快照读取 |
| `CommitReadGuard` | exact revision 的 commit + state 读面 |
| `UpdateCursor` | 独立推进的 commit 消费游标 |
| `SessionRuntime` | auth / bootstrap / connect / recover / flush / pump 的统一编排器 |
| `AdapterRegistry` | 协议域 adapter 的注册、命令编码、输入解码 |
| `WebSocketTransport` / `DefaultRouteConnector` | 底层 websocket route 连接能力 |

> [!NOTE]
> `tqsdk_core::internal` 是 `#[doc(hidden)]` 的 sibling-crate 桥接层，用于
> `tqsdk-session` 吸收 runtime assembly 细节期间复用底层构件。它不是稳定的
> 用户可见契约。外部用户应优先使用 crate root 导出的 `RuntimeHandle`、
> `RuntimeReader`、`UpdateCursor`、protocol commands、schema types，以及
> transport / session contracts。

## 契约模型

```text
RuntimeCommand
  -> ProtocolAdapter encode
  -> OutboundDispatch
  -> transport / HTTP / replay / internal route
  -> RuntimeInput
  -> ProtocolAdapter decode
  -> NormalizedMutation
  -> CommitResult + Revision
  -> RuntimeReader / UpdateCursor
```

这个内核最重要的约束很简单：所有上层可见状态都必须进入同一棵状态树，所有上层可见变化都必须由同一套 commit / revision / causality 语义解释。

## 快速开始

### 1. 建立核心 runtime

```rust
use tqsdk_core::{AdapterRegistry, Runtime, RuntimeHandle};

fn default_adapters() -> AdapterRegistry {
    let mut registry = AdapterRegistry::new();
    registry.register_default_adapters();
    registry
}

let handle = RuntimeHandle::with_adapters(default_adapters());
let reader = handle.reader();
let cursor = reader.cursor();

assert_eq!(cursor.next_revision().get(), 1);
assert_eq!(reader.head_revision(), None);
```

### 2. 提交底层命令

```rust
use tqsdk_core::{MarketCommand, Runtime, RuntimeCommand, Symbol};

async fn submit_quotes(handle: &impl Runtime) -> tqsdk_core::Result<()> {
    let command_id = handle
        .submit(RuntimeCommand::Market(MarketCommand::SubscribeQuotes {
            symbols: vec![Symbol::new("SHFE.au2602")],
        }))
        .await?;

    println!("submitted command {}", command_id.get());
    let _reader = handle.reader();
    // 命令提交只会生成 outbound work。
    // 真正的状态写入要等 session/runtime 摄取远端输入之后才会可见。
    Ok(())
}
```

### 3. 驱动一个 live session

`tqsdk-core` 只保留底层 runtime、adapter、transport 与 session orchestration contracts。
官方 Tianqin auth、HTTP executor、内置快期模拟账号派生，以及面向用户的 live session
组装入口在 `tqsdk-session` 中维护。

## 环境变量

`EndpointConfig::from_env()` 当前只识别这些 endpoint 覆盖项：

- `TQ_AUTH_URL`
- `TQ_MD_URL`
- `TQ_TD_URL`

`schema` / `replay` 相关 endpoint 不会从环境变量隐式注入，需由调用方显式通过代码传入，例如：

- `EndpointConfig::with_schema_url(...)`
- `EndpointConfig::with_replay_url(...)`

`query` 默认不需要环境变量覆盖。官方 live query 语义会复用市场侧解析出的 websocket 地址并通过 `ins_query` 往返；只有当调用方明确希望把 query 改走自定义 HTTP endpoint 时，才需要显式设置：

- `EndpointConfig::with_query_url(...)`

`TQ_INS_URL` 与 `TQ_CHINESE_HOLIDAY_URL` 虽然在官方 Python SDK 中存在，但它们对应的是更高层的合约信息 / 交易日历数据源语义，不属于当前这个低层 runtime contract 的 `EndpointConfig::from_env()` 责任范围。

live 示例另外会用到：

- `TQ_AUTH_USER`
- `TQ_AUTH_PASS`
- `TQ_TEST_SYMBOL`

## 设计约束

- 所有协议域共享同一棵 runtime state tree。
- 只有一套 revision 推进语义和一套 commit 模型。
- adapter 可以编解码，但没有自行发布 commit 的权限。
- adapter 可以提供重连后的 recovery commands，用于恢复行情订阅或 chart 请求；
  这些命令必须重新进入 `RuntimeHandle::submit()` / outbound dispatch 链路。
- 未来 `wait_update`、stream、callback facade 都应该只消费这个 substrate，而不是重定义内核。
- `StateSnapshot`、`CommitLog` 这类兼容/底层原语仍然保留，但它们不定义主要读模型。

## 验证

建议的回归入口：

```bash
cargo test -p tqsdk-core -q --test runtime_contract_v1_capability
cargo test -p tqsdk-core -q --test runtime_contract_reader_surface --test runtime_contract_surface
cargo test -p tqsdk-core -q
```

仓库级架构说明与验证矩阵位于仓库根目录的 `docs/architecture/`。
