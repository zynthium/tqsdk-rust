# `tqsdk-rust`

这是一个 Cargo workspace，用来承载 Rust 版 TQSDK 的核心层与后续不同风格的 facade 层。

仓库当前已经落地的成员只有一个：

| Crate | 路径 | 角色 |
| --- | --- | --- |
| `tqsdk-core` | `crates/tqsdk-core` | 面向官方服务交互的低层 async substrate |

后续计划继续在这个 workspace 下补充多种 V2 facade crate，例如：

- `tqsdk-wait`：贴近 Python `wait_update()` 语义的 facade。
- `tqsdk-stream`：面向 Rust 异步流消费模型的 facade。
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
docs/
  architecture/    # 架构说明、分层设计与验证矩阵
```

## 文档入口

- core crate 说明见 [crates/tqsdk-core/README.md](/Users/joeslee/Projects/GitHub/tqsdk-rust/crates/tqsdk-core/README.md)
- 架构总览见 [docs/architecture/README.md](/Users/joeslee/Projects/GitHub/tqsdk-rust/docs/architecture/README.md)
- 验证矩阵见 [docs/architecture/validation.md](/Users/joeslee/Projects/GitHub/tqsdk-rust/docs/architecture/validation.md)

## 常用命令

```bash
cargo test -p tqsdk-core -q
cargo clippy --workspace --all-targets --all-features -- -D warnings -D clippy::perf
cargo test -p tqsdk-core --test runtime_contract_live_smoke -- --ignored --nocapture
```

## 当前状态

- V1 core 已独立为子 crate，可单独发布。
- workspace 根 README 现在只承载仓库级说明。
- crate 级使用说明和 API 契约已经下沉到 `crates/tqsdk-core/README.md`。

## 架构文档

仓库里的 [`docs/architecture`](docs/architecture) 目录给出了完整分层说明：

- [`docs/architecture/README.md`](docs/architecture/README.md)
- [`docs/architecture/runtime-core/overview.md`](docs/architecture/runtime-core/overview.md)
- [`docs/architecture/validation.md`](docs/architecture/validation.md)

一句话总结就是：

- V1 交付的是 protocol-complete runtime contract。
- V2 及以后所有 facade 都应建立在 `RuntimeReader` 和 `UpdateCursor` 之上，而不是反向改写 runtime core。
