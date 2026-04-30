# Transport Module Split Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split `crates/tqsdk-core/src/transport.rs` into focused internal modules without changing the public `tqsdk-core` transport/session contract.

**Architecture:** This is a source-compatible internal refactor of the core runtime substrate. `crate::transport::*` remains the single private module surface consumed by `crates/tqsdk-core/src/lib.rs`, `session_runtime`, sibling crates through `tqsdk_core::internal`, and tests. The split separates websocket frame I/O, config/targets, topology structs, connected-route runtime, route connectors, frame-to-input mapping, and bootstrap orchestration while preserving the same `RuntimeInput`, `OutboundDispatch`, route matching, and no-internal-Tokio-runtime semantics.

**Tech Stack:** Rust modules, AFIT/RPITIT async trait methods, `yawc` websocket transport, existing runtime contract tests, source-level guardrail tests, `cargo check/test/clippy`.

---

## Scope

In scope:

- Keep `crates/tqsdk-core/src/transport.rs` as the module root.
- Create child modules under `crates/tqsdk-core/src/transport/`.
- Move existing definitions without changing public type names, method names, field visibility, return types, or root re-exports.
- Add a source-level guardrail test to keep `transport.rs` from regressing to one large file.
- Update the existing no-boxing source scan so it reads the new module files.
- Update review/plan documents after verification.

Out of scope:

- Changing `Transport`, `DynTransport`, `SessionRouteConnector`, or `SessionTopologyResolver` signatures.
- Changing route matching precedence or dispatch behavior.
- Changing `EndpointConfig::from_env()` variable names or defaults.
- Making `transport` a public module.
- Moving high-level session/query/wait/stream/task behavior into core transport.
- Removing required boxed object-safety boundaries for `DynTransport`, `DynAuthProvider`, or route connectors.

## File Structure

- Modify: `crates/tqsdk-core/src/transport.rs`
  - Root module only.
  - Declares child modules and re-exports the same items currently visible as `crate::transport::*`.
- Create: `crates/tqsdk-core/src/transport/frame.rs`
  - `RawFrame`
  - `pub(super) map_raw_frame_to_input`
  - private `parse_text_payload`
  - private `parse_binary_payload`
  - existing transport frame/payload unit tests
- Create: `crates/tqsdk-core/src/transport/websocket.rs`
  - `WebSocketConnectOptions`
  - `Transport`
  - `DynTransport`
  - `WebSocketTransport`
  - private `require_tokio_runtime`
- Create: `crates/tqsdk-core/src/transport/config.rs`
  - `EndpointConfig`
  - `HeartbeatPolicy`
  - `ReconnectPolicy`
  - `SessionConfig`
  - `MarketSessionTarget`
  - `TradeSessionTarget`
  - `AuthDerivedTradeTarget`
  - private env helpers and default auth URL
  - existing `MarketSessionTarget` unit test
- Create: `crates/tqsdk-core/src/transport/topology.rs`
  - `SessionTarget`
  - `SessionRouteEndpoint`
  - `SessionRoute`
  - `SessionTopology`
  - `SessionPhase`
  - `BootstrapResult`
  - `SessionTopologyResolver`
- Create: `crates/tqsdk-core/src/transport/connected.rs`
  - `ConnectedSessionRoute`
  - `ConnectedTopology`
  - `DispatchReceipt`
  - private `route_dispatch_match_score`
- Create: `crates/tqsdk-core/src/transport/connector.rs`
  - `DynRouteConnectFuture`
  - `SessionRouteConnector`
  - private `PassiveRouteTransport`
  - `WebSocketRouteConnector`
  - `DefaultRouteConnector`
- Create: `crates/tqsdk-core/src/transport/bootstrap.rs`
  - `SessionBootstrap`
- Modify: `crates/tqsdk-core/tests/runtime_contract_route_connector.rs`
  - Add module split guardrail.
  - Update no-boxing source scan to read `src/transport/connected.rs` and `src/transport/bootstrap.rs`.
- Modify: `docs/reviews/comprehensive-review-2026-04-30.md`
  - Mark the `transport.rs` module-directory split complete after verification.
- Modify: `docs/superpowers/plans/2026-04-29-review-remediation-plan.md`
  - Remove `transport.rs` from remaining module split list after verification.
- Modify: `docs/superpowers/plans/2026-05-01-transport-module-split.md`
  - Check off executed steps and record verification.

## Task 1: Add Transport Split Guardrail Test

**Files:**
- Modify: `crates/tqsdk-core/tests/runtime_contract_route_connector.rs`

- [x] **Step 1: Write the failing structure test**

Add this test near `transport_orchestration_methods_do_not_box_futures`:

```rust
#[test]
fn transport_is_split_into_focused_modules() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let transport_root = root.join("src/transport.rs");
    let transport_dir = root.join("src/transport");

    for module in [
        "frame.rs",
        "websocket.rs",
        "config.rs",
        "topology.rs",
        "connected.rs",
        "connector.rs",
        "bootstrap.rs",
    ] {
        assert!(
            transport_dir.join(module).exists(),
            "transport module {module} should exist under src/transport/"
        );
    }

    let source =
        std::fs::read_to_string(&transport_root).expect("transport root should be readable");
    for module_decl in [
        "mod bootstrap;",
        "mod config;",
        "mod connected;",
        "mod connector;",
        "mod frame;",
        "mod topology;",
        "mod websocket;",
    ] {
        assert!(
            source.contains(module_decl),
            "transport root should declare {module_decl}"
        );
    }

    assert!(
        !source.contains("pub struct WebSocketTransport"),
        "websocket implementation should live in src/transport/websocket.rs"
    );
    assert!(
        !source.contains("pub struct ConnectedTopology"),
        "connected topology runtime should live in src/transport/connected.rs"
    );
    assert!(
        !source.contains("pub struct SessionBootstrap"),
        "bootstrap orchestration should live in src/transport/bootstrap.rs"
    );
}
```

- [x] **Step 2: Run the structure test and verify RED**

Run:

```bash
cargo test -p tqsdk-core --test runtime_contract_route_connector transport_is_split_into_focused_modules
```

Expected before implementation:

```text
FAILED transport_is_split_into_focused_modules
```

The failure should report at least one missing module under `src/transport/`.

Observed RED: failed because `src/transport/frame.rs` did not exist.

- [x] **Step 3: Update the no-boxing source scan**

Replace `transport_orchestration_methods_do_not_box_futures` with this module-aware version:

```rust
#[test]
fn transport_orchestration_methods_do_not_box_futures() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let connected_source =
        std::fs::read_to_string(root.join("src/transport/connected.rs"))
            .expect("connected transport module should be readable");
    let bootstrap_source =
        std::fs::read_to_string(root.join("src/transport/bootstrap.rs"))
            .expect("bootstrap transport module should be readable");
    let blocks = [
        (
            "ConnectedSessionRoute",
            connected_source
                .split("impl ConnectedSessionRoute {")
                .nth(1)
                .and_then(|tail| tail.split("#[derive(Default)]").next()),
        ),
        (
            "ConnectedTopology",
            connected_source
                .split("impl ConnectedTopology {")
                .nth(1)
                .and_then(|tail| tail.split("pub type DynRouteConnectFuture").next()),
        ),
        (
            "SessionBootstrap",
            bootstrap_source
                .split("impl SessionBootstrap {")
                .nth(1),
        ),
    ];

    for (name, block) in blocks {
        let block = block.unwrap_or_else(|| panic!("{name} impl block should be present"));
        assert!(
            !block.contains("Box::pin"),
            "{name} orchestration methods should use native async futures"
        );
    }
}
```

## Task 2: Create Transport Module Root and Re-exports

**Files:**
- Modify: `crates/tqsdk-core/src/transport.rs`
- Create: `crates/tqsdk-core/src/transport/frame.rs`
- Create: `crates/tqsdk-core/src/transport/websocket.rs`
- Create: `crates/tqsdk-core/src/transport/config.rs`
- Create: `crates/tqsdk-core/src/transport/topology.rs`
- Create: `crates/tqsdk-core/src/transport/connected.rs`
- Create: `crates/tqsdk-core/src/transport/connector.rs`
- Create: `crates/tqsdk-core/src/transport/bootstrap.rs`

- [x] **Step 1: Replace root file with module declarations and re-exports**

After moving the definitions in Tasks 3-7, `crates/tqsdk-core/src/transport.rs` should contain:

```rust
mod bootstrap;
mod config;
mod connected;
mod connector;
mod frame;
mod topology;
mod websocket;

pub use bootstrap::SessionBootstrap;
pub use config::{
    AuthDerivedTradeTarget, EndpointConfig, HeartbeatPolicy, MarketSessionTarget, ReconnectPolicy,
    SessionConfig, TradeSessionTarget,
};
pub use connected::{ConnectedTopology, DispatchReceipt};
pub use connector::{
    DefaultRouteConnector, DynRouteConnectFuture, SessionRouteConnector, WebSocketRouteConnector,
};
pub use frame::RawFrame;
pub use topology::{
    BootstrapResult, SessionPhase, SessionRoute, SessionRouteEndpoint, SessionTarget,
    SessionTopology, SessionTopologyResolver,
};
pub use websocket::{DynTransport, Transport, WebSocketConnectOptions, WebSocketTransport};
```

- [x] **Step 2: Keep crate-level public exports unchanged**

Do not edit the `pub use transport::{ ... }` block in `crates/tqsdk-core/src/lib.rs` except if rustfmt reflows it.

Run:

```bash
cargo check -p tqsdk-core
```

Expected after Tasks 3-7 complete:

```text
Finished `dev` profile ...
```

## Task 3: Move Frames and WebSocket Transport

**Files:**
- Create: `crates/tqsdk-core/src/transport/frame.rs`
- Create: `crates/tqsdk-core/src/transport/websocket.rs`
- Modify: `crates/tqsdk-core/src/transport.rs`

- [x] **Step 1: Move frame definitions and payload mapping**

Move these existing definitions from `transport.rs` into `transport/frame.rs`:

- `RawFrame`
- `map_raw_frame_to_input`
- `parse_text_payload`
- `parse_binary_payload`
- unit tests:
  - `parse_text_payload_decodes_json_when_possible`
  - `parse_binary_payload_decodes_json_when_possible`
  - `parse_binary_payload_preserves_non_json_bytes`
  - `map_raw_binary_frame_to_json_io_when_payload_is_json`

Required imports in `frame.rs`:

```rust
use crate::events::{InputPayload, InternalEvent, IoEvent, RuntimeInput};
use crate::ids::ProtocolDomain;
use crate::Result;
use serde_json::{Value, json};

use super::topology::SessionRoute;
```

The moved function must remain internal:

```rust
pub(super) fn map_raw_frame_to_input(
    route: &SessionRoute,
    frame: RawFrame,
) -> Result<Option<RuntimeInput>> {
    /* move the existing match body unchanged */
}
```

- [x] **Step 2: Move websocket transport definitions**

Move these existing definitions from `transport.rs` into `transport/websocket.rs`:

- `WebSocketConnectOptions`
- `Transport`
- `DynTransport`
- `impl<T> DynTransport for T where T: Transport`
- `WebSocketTransport`
- `impl WebSocketTransport`
- `impl Transport for WebSocketTransport`
- private `require_tokio_runtime`

Required imports in `websocket.rs`:

```rust
use std::future::Future;
use std::pin::Pin;

use futures::SinkExt;
use url::Url;
use yawc::frame::{Frame, OpCode};
use yawc::{HttpRequestBuilder, Options, TcpWebSocket, WebSocket};

use crate::commands::OutboundFrame;
use crate::{ContractError, Result};

use super::frame::RawFrame;
```

## Task 4: Move Config and Topology Types

**Files:**
- Create: `crates/tqsdk-core/src/transport/config.rs`
- Create: `crates/tqsdk-core/src/transport/topology.rs`
- Modify: `crates/tqsdk-core/src/transport.rs`

- [x] **Step 1: Move config and target definitions**

Move these existing definitions from `transport.rs` into `transport/config.rs`:

- `DEFAULT_AUTH_URL`
- `read_optional_env`
- `read_env_or_default`
- `EndpointConfig`
- `HeartbeatPolicy`
- `ReconnectPolicy`
- `SessionConfig`
- `MarketSessionTarget`
- `TradeSessionTarget`
- `AuthDerivedTradeTarget`
- unit test `market_session_target_named_constructors_are_explicit`

Required imports in `config.rs`:

```rust
use std::time::Duration;

use crate::ids::{AccountId, ProtocolDomain};
```

- [x] **Step 2: Move topology definitions**

Move these existing definitions from `transport.rs` into `transport/topology.rs`:

- `SessionTarget`
- `SessionRouteEndpoint`
- `SessionRoute`
- `SessionTopology`
- `SessionPhase`
- `BootstrapResult`
- `SessionTopologyResolver`

Required imports in `topology.rs`:

```rust
use std::future::Future;
use std::pin::Pin;

use crate::auth::AuthContext;
use crate::ids::{AccountId, ProtocolDomain, ReplaySessionId};
use crate::Result;

use super::config::SessionConfig;
use super::websocket::WebSocketConnectOptions;
```

## Task 5: Move Connected Runtime and Route Connectors

**Files:**
- Create: `crates/tqsdk-core/src/transport/connected.rs`
- Create: `crates/tqsdk-core/src/transport/connector.rs`
- Modify: `crates/tqsdk-core/src/transport.rs`

- [x] **Step 1: Move connected-route runtime**

Move these existing definitions from `transport.rs` into `transport/connected.rs`:

- `ConnectedSessionRoute`
- `ConnectedTopology`
- `DispatchReceipt`
- `impl ConnectedSessionRoute`
- `impl ConnectedTopology`
- private `route_dispatch_match_score`

Required imports in `connected.rs`:

```rust
use std::collections::VecDeque;

use crate::commands::{OutboundDispatch, OutboundFrame, OutboundRequest};
use crate::events::RuntimeInput;
use crate::ids::{CommandId, ProtocolDomain};
use crate::{ContractError, Result};

use super::frame::map_raw_frame_to_input;
use super::topology::{SessionRoute, SessionRouteEndpoint, SessionTarget};
use super::websocket::DynTransport;
```

- [x] **Step 2: Move route connector definitions**

Move these existing definitions from `transport.rs` into `transport/connector.rs`:

- `DynRouteConnectFuture`
- `SessionRouteConnector`
- private `PassiveRouteTransport`
- `WebSocketRouteConnector`
- `DefaultRouteConnector`
- their impl blocks

Required imports in `connector.rs`:

```rust
use std::future::Future;
use std::pin::Pin;

use crate::commands::OutboundFrame;
use crate::{ContractError, Result};

use super::frame::RawFrame;
use super::topology::{SessionRoute, SessionRouteEndpoint};
use super::websocket::{DynTransport, Transport, WebSocketTransport};
```

## Task 6: Move Bootstrap Orchestration

**Files:**
- Create: `crates/tqsdk-core/src/transport/bootstrap.rs`
- Modify: `crates/tqsdk-core/src/transport.rs`

- [x] **Step 1: Move `SessionBootstrap`**

Move these existing definitions from `transport.rs` into `transport/bootstrap.rs`:

- `SessionBootstrap`
- `impl SessionBootstrap`

Required imports in `bootstrap.rs`:

```rust
use std::collections::VecDeque;

use crate::adapter::AdapterRegistry;
use crate::auth::DynAuthProvider;
use crate::Result;

use super::config::SessionConfig;
use super::connected::{ConnectedSessionRoute, ConnectedTopology};
use super::connector::SessionRouteConnector;
use super::topology::{BootstrapResult, SessionTopology, SessionTopologyResolver};
```

## Task 7: Verify Behavior and Docs

**Files:**
- Modify: `crates/tqsdk-core/tests/runtime_contract_route_connector.rs`
- Modify: `docs/reviews/comprehensive-review-2026-04-30.md`
- Modify: `docs/superpowers/plans/2026-04-29-review-remediation-plan.md`
- Modify: `docs/superpowers/plans/2026-05-01-transport-module-split.md`

- [x] **Step 1: Run focused core checks**

Run:

```bash
cargo test -p tqsdk-core --test runtime_contract_route_connector
cargo test -p tqsdk-core --test runtime_contract_ws_transport
cargo test -p tqsdk-core --test runtime_contract_session_cycle
cargo check -p tqsdk-session
```

Expected:

```text
test result: ok
Finished `dev` profile ...
```

Observed focused verification:

```text
cargo test -p tqsdk-core --test runtime_contract_route_connector
cargo test -p tqsdk-core --test runtime_contract_ws_transport
cargo test -p tqsdk-core --test runtime_contract_session_cycle
cargo check -p tqsdk-session
```

- [x] **Step 2: Update review and umbrella plan docs**

In `docs/reviews/comprehensive-review-2026-04-30.md`:

- Add `transport.rs` to the completed summary as split into `transport/` modules.
- Change the `transport.rs` maintainability table row to mention completion.
- Remove `transport.rs` from the remaining independent plan items.

In `docs/superpowers/plans/2026-04-29-review-remediation-plan.md`:

- Add this child plan to the 2026-05-01 continuation completed list after verification.
- Change the remaining module split sentence from `transport.rs and account_group.rs` to only `account_group.rs`.

- [x] **Step 3: Run full verification**

Run:

```bash
cargo fmt --all --check
cargo check --workspace
cargo test --workspace --tests
cargo clippy --workspace --all-targets -- -D warnings
cargo check --workspace --examples
git diff --check
```

Expected:

```text
Finished `dev` profile ...
test result: ok
```

Observed full verification:

```text
cargo fmt --all --check
cargo check --workspace
cargo test --workspace --tests
cargo clippy --workspace --all-targets -- -D warnings
cargo check --workspace --examples
git diff --check
```

- [x] **Step 4: Commit**

Run:

```bash
git add crates/tqsdk-core/src/transport.rs crates/tqsdk-core/src/transport crates/tqsdk-core/tests/runtime_contract_route_connector.rs docs/reviews/comprehensive-review-2026-04-30.md docs/superpowers/plans/2026-04-29-review-remediation-plan.md docs/superpowers/plans/2026-05-01-transport-module-split.md
git commit -m "refactor: split core transport modules"
```

Expected:

```text
[main <sha>] refactor: split core transport modules
```

## Self-Review

- Spec coverage: Covers the remaining comprehensive-review item `transport.rs` module-directory split and keeps all root exports/source compatibility intact.
- Placeholder scan: No `TBD`, `TODO`, or vague implementation placeholders are present; each task names exact files, moved definitions, and commands.
- Type consistency: Module imports match the existing type names in `transport.rs`; public re-export names match `crates/tqsdk-core/src/lib.rs`.
