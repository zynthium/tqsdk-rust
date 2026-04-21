# `tqsdk-rust`

这是一个 Cargo workspace，用来承载 Rust 版 TQSDK 的核心层、共享 session 层，以及后续不同风格的 facade 层。

仓库当前已经落地的成员如下：

| Crate | 路径 | 角色 |
| --- | --- | --- |
| `tqsdk-core` | `crates/tqsdk-core` | 面向官方服务交互的低层 async substrate |
| `tqsdk-session` | `crates/tqsdk-session` | mode-agnostic 的共享 session / direct-query thin layer |
| `tqsdk-wait` | `crates/tqsdk-wait` | Python 风格 single-owner wait facade，基于 core/session |
| `tqsdk-stream` | `crates/tqsdk-stream` | Rust async-native multi-consumer commit stream facade，基于 core/session |

后续计划继续在这个 workspace 下补充多种 V2 facade crate，例如：

- `tqsdk-callback`：面向 callback / event handler 风格的 facade。

## 分层原则

- `tqsdk-core` 只负责协议、状态树、commit/revision、session/runtime orchestration。
- facade crate 只消费 `tqsdk-core` 的 substrate，不反向侵入 core。
- core 保持纯 async，不内建 runtime，不附带高层用户便利接口。
- 仓库结构按“稳定底座 + 可替换 facade”组织，优先保证性能、稳定性和长期可维护性。

## 仓库结构

```text
crates/
  tqsdk-core/      # 当前 V1 核心基座
  tqsdk-session/   # 共享 session / direct-query 层
  tqsdk-wait/      # Python 风格 wait facade
  tqsdk-stream/    # Rust 风格 stream facade
docs/
  architecture/    # 架构说明、分层设计与验证矩阵
```

## 文档入口

- core crate 说明见 [crates/tqsdk-core/README.md](crates/tqsdk-core/README.md)
- session crate 说明见 [crates/tqsdk-session/README.md](crates/tqsdk-session/README.md)
- wait crate 说明见 [crates/tqsdk-wait/README.md](crates/tqsdk-wait/README.md)
- stream crate 说明见 [crates/tqsdk-stream/README.md](crates/tqsdk-stream/README.md)
- 仓库级路线图见 [ROADMAP.md](ROADMAP.md)
- 架构总览见 [docs/architecture/README.md](docs/architecture/README.md)
- 验证矩阵见 [docs/architecture/validation.md](docs/architecture/validation.md)

## 常用命令

```bash
cargo test -p tqsdk-core -q
cargo clippy --workspace --all-targets --all-features -- -D warnings -D clippy::perf
cargo test -p tqsdk-core --test runtime_contract_live_smoke -- --ignored --nocapture
```

## 当前状态

- V1 core 已独立为子 crate，可单独发布。
- `tqsdk-session` 已承载共享 session shell、lazy establish、route/pending-route 驱动原语，以及 direct-query/schema 薄层入口。
- `tqsdk-wait` 已具备 market/trade 对象引用、serial window、可工作的 `wait_update()` 驱动链路与 trade 命令包装。
- `tqsdk-stream` 已落地最小 commit-stream facade，当前提供共享 session 驱动、raw commit fan-out、显式 lag/closed/error surface，以及 path/scope 级 commit 过滤；后续继续补对象级 stream 与 trade 可靠事件流。
- workspace 根 README 现在只承载仓库级说明。
- crate 级使用说明和 API 契约已经分别下沉到各子 crate 的 `README.md`。

当前 workspace 里的最小可编译示例：

- `crates/tqsdk-core/examples/live_probe.rs`
- `crates/tqsdk-core/examples/live_market_history.rs`
- `crates/tqsdk-session/examples/query_symbol_info.rs`
- `crates/tqsdk-wait/examples/quote_wait.rs`
- `crates/tqsdk-wait/examples/quote_wait_with_session_query.rs`

## 架构文档

仓库里的 [`docs/architecture`](docs/architecture) 目录给出了完整分层说明：

- [`docs/architecture/README.md`](docs/architecture/README.md)
- [`docs/architecture/runtime-core/overview.md`](docs/architecture/runtime-core/overview.md)
- [`docs/architecture/validation.md`](docs/architecture/validation.md)

一句话总结就是：

- V1 交付的是 protocol-complete runtime contract。
- V2 及以后所有 facade 都应建立在 `RuntimeReader` 和 `UpdateCursor` 之上，而不是反向改写 runtime core。
