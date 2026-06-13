# Relay Universe Expression Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `TQSDK_RELAY_FUTURES_UNIVERSE` so relay startup can compose active, main, index, cont, top-N, symbol, product, and exchange rules with `!` / `~` exclusions.

**Architecture:** Add a focused parser/planner module beside `universe.rs`, keep resolution in the existing pre-subscription universe layer, and feed final symbols into current `UpstreamTickChart` construction. Legacy env vars remain supported but are mutually exclusive with the new expression.

**Tech Stack:** Rust 2024, `tqsdk-relay`, existing `RelayConfig`, `FuturesUniverseResolver`, integration tests under `crates/tqsdk-relay/tests`.

---

### Task 1: Parser and Config Surface

**Files:**
- Create: `crates/tqsdk-relay/src/universe_expression.rs`
- Modify: `crates/tqsdk-relay/src/lib.rs`
- Modify: `crates/tqsdk-relay/src/config.rs`
- Test: `crates/tqsdk-relay/tests/config.rs`

- [ ] **Step 1: Write failing parser/config tests**

Add tests for:

```rust
use tqsdk_relay::{RelayConfig, UniverseExpression};

#[test]
fn config_loads_futures_universe_expression_from_env() {
    let config = RelayConfig::from_env_vars(|key| match key {
        "TQSDK_RELAY_FUTURES_UNIVERSE" => Some("main:all;index:all;!CFFEX".to_string()),
        _ => None,
    })
    .unwrap();

    assert_eq!(
        config
            .futures_universe_expression
            .as_ref()
            .unwrap()
            .to_string(),
        "main:all;index:all;!CFFEX"
    );
}

#[test]
fn config_rejects_new_and_legacy_universe_sources_together() {
    let err = RelayConfig::from_env_vars(|key| match key {
        "TQSDK_RELAY_FUTURES_UNIVERSE" => Some("main:all".to_string()),
        "TQSDK_RELAY_FUTURES_PRODUCTS" => Some("ALL".to_string()),
        _ => None,
    })
    .unwrap_err();

    assert_eq!(
        err.to_string(),
        "invalid relay config: set only one futures universe source"
    );
}

#[test]
fn universe_expression_rejects_invalid_clauses() {
    assert!(UniverseExpression::parse("").is_err());
    assert!(UniverseExpression::parse("main:all;;index:all").is_err());
    assert!(UniverseExpression::parse("unknown:all").is_err());
    assert!(UniverseExpression::parse("top:0:all").is_err());
    assert!(UniverseExpression::parse("main:").is_err());
    assert!(UniverseExpression::parse("main:SHFE.au,,DCE.m").is_err());
    assert!(UniverseExpression::parse("!").is_err());
}
```

- [ ] **Step 2: Run tests to verify RED**

Run: `cargo test -p tqsdk-relay --test config universe_expression`

Expected: compile failures for missing `UniverseExpression` / `futures_universe_expression`.

- [ ] **Step 3: Implement parser/config minimal surface**

Add `UniverseExpression` plus clause/selector types, parse `;` and `,`, support `!` and `~`, validate `top:N`, and add `RelayConfig.futures_universe_expression: Option<UniverseExpression>`.

- [ ] **Step 4: Run tests to verify GREEN**

Run: `cargo test -p tqsdk-relay --test config universe_expression`

Expected: parser/config tests pass.

### Task 2: Universe Resolution Semantics

**Files:**
- Modify: `crates/tqsdk-relay/src/universe_expression.rs`
- Modify: `crates/tqsdk-relay/src/universe.rs`
- Modify: `crates/tqsdk-relay/src/runtime.rs`
- Test: `crates/tqsdk-relay/tests/universe.rs`

- [ ] **Step 1: Write failing resolver tests**

Add tests for:

```rust
#[tokio::test]
async fn expression_resolves_main_and_index_then_excludes_product() {
    let mut resolver = StaticFuturesUniverseResolver::new([
        FuturesContract::new("SHFE.au2602", "SHFE", "au", false).unwrap(),
        FuturesContract::new("DCE.m2609", "DCE", "m", false).unwrap(),
    ])
    .with_main_symbols(["SHFE.au2602", "DCE.m2609"]);

    let expression = UniverseExpression::parse("main:all;index:all;!SHFE.au").unwrap();
    let symbols = resolve_futures_symbols_with_expression(&expression, &mut resolver)
        .await
        .unwrap();

    assert_eq!(symbols, vec!["DCE.m2609", "KQ.i@DCE.m"]);
}

#[tokio::test]
async fn expression_resolves_top_n_and_continuous_symbols() {
    let mut resolver = StaticFuturesUniverseResolver::new([
        FuturesContract::new("SHFE.au2602", "SHFE", "au", false).unwrap(),
        FuturesContract::new("SHFE.au2608", "SHFE", "au", false).unwrap(),
    ])
    .with_main_symbols(["SHFE.au2602"])
    .with_quote_snapshots([
        quote("SHFE.au2602", "SHFE", "au", 90, 10),
        quote("SHFE.au2608", "SHFE", "au", 120, 8),
    ]);

    let expression = UniverseExpression::parse("top:2:all;cont:all").unwrap();
    let symbols = resolve_futures_symbols_with_expression(&expression, &mut resolver)
        .await
        .unwrap();

    assert_eq!(
        symbols,
        vec!["KQ.m@SHFE.au", "SHFE.au2602", "SHFE.au2608"]
    );
}
```

- [ ] **Step 2: Run tests to verify RED**

Run: `cargo test -p tqsdk-relay --test universe expression_`

Expected: compile failures for missing resolver function.

- [ ] **Step 3: Implement expression resolution**

Create `UniverseSymbol` records, resolve include clauses via existing active/main/top logic, generate `KQ.i@EX.product` / `KQ.m@EX.product`, then apply exclude clauses against symbol/product/exchange.

- [ ] **Step 4: Run tests to verify GREEN**

Run: `cargo test -p tqsdk-relay --test universe expression_`

Expected: resolver tests pass.

### Task 3: Diagnostics, Binary Dry-Run, and Docs

**Files:**
- Modify: `crates/tqsdk-relay/src/diagnostics.rs`
- Modify: `crates/tqsdk-relay/tests/observability.rs`
- Modify: `crates/tqsdk-relay/tests/binary_smoke.rs`
- Modify: `crates/tqsdk-relay/README.md`
- Modify: `README.md`
- Modify: `docs/architecture/README.md`

- [ ] **Step 1: Write failing diagnostics tests**

Add tests that startup report for `TQSDK_RELAY_FUTURES_UNIVERSE="symbol:SHFE.au2602,KQ.i@DCE.m"` exposes `futures_universe_expression`, include/exclude counts, final symbol count, and source `universe-expression`.

- [ ] **Step 2: Run tests to verify RED**

Run: `cargo test -p tqsdk-relay --test observability startup_report --test binary_smoke dry_run`

Expected: field assertions fail/compile fail.

- [ ] **Step 3: Implement diagnostics/docs**

Extend `RelayStartupReport`, update binary dry-run assertions, and document the new syntax while marking old env vars as compatible legacy shortcuts.

- [ ] **Step 4: Run tests to verify GREEN**

Run: `cargo test -p tqsdk-relay --test observability startup_report --test binary_smoke dry_run`

Expected: diagnostics and dry-run tests pass.

### Task 4: Full Relay Validation and Commit

**Files:**
- All changed files from Tasks 1-3.

- [ ] **Step 1: Format and run relay tests**

Run:

```bash
cargo fmt --all --check
cargo test -p tqsdk-relay --tests
cargo check -p tqsdk-relay --no-default-features
cargo clippy -p tqsdk-relay --tests -- -D warnings
git diff --check
gitnexus detect-changes --repo tqsdk-rust
```

Expected: all pass. If a transient binary smoke reset appears, rerun that single test once, then rerun the full relay test command.

- [ ] **Step 2: Commit scoped changes**

Run:

```bash
git add crates/tqsdk-relay/src crates/tqsdk-relay/tests crates/tqsdk-relay/README.md README.md docs/architecture/README.md docs/superpowers/plans/2026-06-13-relay-universe-expression.md
git commit -m "feat(relay): add futures universe expression"
```
