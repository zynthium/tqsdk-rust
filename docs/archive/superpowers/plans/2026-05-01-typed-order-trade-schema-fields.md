# Typed Order/Trade Schema Fields Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace futures `Order`/`Trade` schema fields `direction`, `offset`, and `price_type` from raw `String` with typed protocol enums while preserving missing-field tolerance.

**Architecture:** This is a `tqsdk-core` schema contract change only. Existing runtime command enums remain the canonical protocol vocabulary, and wait/stream/task must continue to consume the same runtime state tree without adding local overlays or alternate state machines. Because the old fields were public strings, this is a source-breaking API narrowing and must update docs/reviews/examples where needed.

**Tech Stack:** Rust, Serde, Cargo workspace tests, docs under `docs/architecture/` and `docs/reviews/`.

---

## Scope

This plan intentionally covers futures `tqsdk_core::Order` and `tqsdk_core::Trade` only:

- `Order.direction: Option<TradeDirection>`
- `Order.offset: Option<TradeOffset>`
- `Order.price_type: Option<TradePriceType>`
- `Trade.direction: Option<TradeDirection>`
- `Trade.offset: Option<TradeOffset>`

`PreInsertOrder.direction` and securities `SecurityOrder` / `SecurityTrade` remain raw strings in this batch because they either do not yet have a complete typed schema contract or are not part of the specific review item.

## Task 1: Add Typed Serde Contract Tests

**Files:**
- Modify: `crates/tqsdk-core/tests/runtime_contract_types.rs`

- [x] **Step 1: Write failing tests for typed futures order/trade fields**

Add this test near the existing `trading_and_risk_schema_types_deserialize_nested_payloads` coverage:

```rust
#[test]
fn futures_order_and_trade_decode_typed_side_offset_and_price_type() {
    let order = serde_json::from_value::<Order>(json!({
        "user_id": "simnow",
        "order_id": "order-typed-1",
        "exchange_id": "SHFE",
        "instrument_id": "au2602",
        "direction": "BUY",
        "offset": "OPEN",
        "price_type": "LIMIT",
        "volume_orign": 2,
        "volume_left": 1,
        "status": "ALIVE"
    }))
    .expect("typed order schema should deserialize");

    assert_eq!(order.direction, Some(TradeDirection::Buy));
    assert_eq!(order.offset, Some(TradeOffset::Open));
    assert_eq!(order.price_type, Some(TradePriceType::Limit));

    let trade = serde_json::from_value::<Trade>(json!({
        "user_id": "simnow",
        "trade_id": "trade-typed-1",
        "order_id": "order-typed-1",
        "exchange_id": "SHFE",
        "instrument_id": "au2602",
        "direction": "SELL",
        "offset": "CLOSETODAY",
        "price": 618.5,
        "volume": 1
    }))
    .expect("typed trade schema should deserialize");

    assert_eq!(trade.direction, Some(TradeDirection::Sell));
    assert_eq!(trade.offset, Some(TradeOffset::CloseToday));
}
```

- [x] **Step 2: Write failing tests for missing-field tolerance and unknown-value rejection**

Add this test in the same file:

```rust
#[test]
fn futures_order_and_trade_optional_typed_fields_preserve_missing_field_tolerance() {
    let order = serde_json::from_value::<Order>(json!({
        "user_id": "simnow",
        "order_id": "order-missing-typed-fields"
    }))
    .expect("missing optional typed order fields should deserialize");

    assert_eq!(order.direction, None);
    assert_eq!(order.offset, None);
    assert_eq!(order.price_type, None);

    let unknown_order = serde_json::from_value::<Order>(json!({
        "order_id": "order-unknown-direction",
        "direction": "SIDEWAYS"
    }));
    assert!(unknown_order.is_err());
}
```

- [x] **Step 3: Run the test and verify RED**

Run:

```bash
cargo test -p tqsdk-core --test runtime_contract_types futures_order_and_trade
```

Expected before implementation: compile failure because `Order.direction`, `Order.offset`, `Order.price_type`, `Trade.direction`, and `Trade.offset` are still `String` fields.

## Task 2: Implement Typed Schema Fields

**Files:**
- Modify: `crates/tqsdk-core/src/commands.rs`
- Modify: `crates/tqsdk-core/src/types/trading.rs`
- Modify: affected core tests constructing `Order` values directly

- [x] **Step 1: Add Serde support for trade enums**

Implement `Serialize` and `Deserialize` for `TradeDirection`, `TradeOffset`, and `TradePriceType` using their official protocol strings. Add `from_protocol_str` helpers so schema field deserializers can reuse the same mapping:

```rust
impl TradeDirection {
    pub fn from_protocol_str(value: &str) -> Option<Self> {
        match value {
            "BUY" => Some(Self::Buy),
            "SELL" => Some(Self::Sell),
            _ => None,
        }
    }
}

impl Serialize for TradeDirection {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for TradeDirection {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::from_protocol_str(&value).ok_or_else(|| {
            serde::de::Error::unknown_variant(&value, &["BUY", "SELL"])
        })
    }
}
```

Apply the same pattern to:

```rust
TradeOffset::from_protocol_str("OPEN" | "CLOSE" | "CLOSETODAY")
TradePriceType::from_protocol_str("ANY" | "LIMIT" | "BEST" | "FIVELEVEL")
```

- [x] **Step 2: Change futures schema field types**

In `crates/tqsdk-core/src/types/trading.rs` change:

```rust
pub direction: String,
pub offset: String,
pub price_type: String,
```

to:

```rust
#[serde(default, deserialize_with = "deserialize_optional_trade_direction")]
pub direction: Option<TradeDirection>,
#[serde(default, deserialize_with = "deserialize_optional_trade_offset")]
pub offset: Option<TradeOffset>,
#[serde(default, deserialize_with = "deserialize_optional_trade_price_type")]
pub price_type: Option<TradePriceType>,
```

Apply this to `Order`, and apply `Option<TradeDirection>` / `Option<TradeOffset>` to `Trade`.

Use helper deserializers that preserve missing-field tolerance and also treat empty strings as `None`:

```rust
fn deserialize_optional_trade_direction<'de, D>(
    deserializer: D,
) -> Result<Option<TradeDirection>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    deserialize_optional_protocol_enum(deserializer, TradeDirection::from_protocol_str)
}

fn deserialize_optional_protocol_enum<'de, D, T>(
    deserializer: D,
    parse: impl FnOnce(&str) -> Option<T>,
) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<String>::deserialize(deserializer)?;
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_empty() {
        return Ok(None);
    }
    parse(&value).map(Some).ok_or_else(|| serde::de::Error::custom(format!("unknown protocol enum value {value:?}")))
}
```

- [x] **Step 3: Update direct `Order` struct construction in tests**

Where tests construct `Order { ... }` directly, replace string literals with typed options:

```rust
direction: Some(TradeDirection::Buy),
offset: Some(TradeOffset::Open),
price_type: Some(TradePriceType::Limit),
```

- [x] **Step 4: Run typed schema tests and verify GREEN**

Run:

```bash
cargo test -p tqsdk-core --test runtime_contract_types futures_order_and_trade
```

Expected: both new tests pass.

## Task 3: Migrate Task Consumers

**Files:**
- Modify: `crates/tqsdk-task/src/target_pos/planner.rs`
- Modify: `crates/tqsdk-task/src/target_pos/report.rs`
- Modify: affected task tests

- [x] **Step 1: Update live order matching**

Change `order_can_satisfy_desired` to compare typed options:

```rust
fn order_can_satisfy_desired(order: &Order, desired_order: &DesiredOrder) -> bool {
    order.direction == Some(desired_order.direction)
        && order.offset == Some(desired_order.offset)
        && order.volume_left > 0
        && order.volume_left <= desired_order.volume
        && order.limit_price == desired_order.limit_price
}
```

- [x] **Step 2: Preserve report string output**

In `TargetPosTaskTradeFill::from(&Trade)`, keep the report fields as `String` by converting typed options back to official strings:

```rust
direction: trade.direction.map(TradeDirection::as_str).unwrap_or_default().to_string(),
offset: trade.offset.map(TradeOffset::as_str).unwrap_or_default().to_string(),
```

- [x] **Step 3: Run task tests**

Run:

```bash
cargo test -p tqsdk-task --tests
```

Expected: all task tests pass.

## Task 4: Update Docs and Review State

**Files:**
- Modify: `crates/tqsdk-core/README.md`
- Modify: `docs/architecture/api-layers.md`
- Modify: `docs/architecture/validation.md`
- Modify: `docs/reviews/comprehensive-review-2026-04-30.md`
- Modify: `docs/superpowers/plans/2026-04-29-review-remediation-plan.md`
- Modify: `docs/superpowers/plans/2026-05-01-typed-order-trade-schema-fields.md`

- [x] **Step 1: Document typed schema fields**

Update the core README and architecture docs to state that futures order/trade side, offset, and order price type decode to typed protocol enums while preserving missing-field tolerance via `Option`.

- [x] **Step 2: Mark review item complete**

Update `docs/reviews/comprehensive-review-2026-04-30.md` and the umbrella remediation plan to say futures `Order`/`Trade` string fields have been migrated to typed optional enums.

- [x] **Step 3: Verify docs and workspace**

Run:

```bash
cargo fmt --all --check
cargo check --workspace
cargo test --workspace --tests
git diff --check
```

Expected: all commands pass.

Verification run on 2026-05-01:

- `cargo test -p tqsdk-core --test runtime_contract_types futures_order_and_trade` first failed before implementation because the public fields were still `String`, then passed after the typed schema change.
- `cargo test -p tqsdk-task --tests` passed after task consumers were migrated.
- `cargo fmt --all --check` passed.
- `cargo check --workspace` passed.
- `cargo test --workspace --tests` passed.
- `git diff --check` passed.
- Extra gate: `cargo clippy --workspace --all-targets -- -D warnings` passed.

- [x] **Step 4: Commit**

```bash
git add crates/tqsdk-core/src/commands.rs crates/tqsdk-core/src/types/trading.rs crates/tqsdk-core/tests/runtime_contract_types.rs crates/tqsdk-task/src/target_pos/planner.rs crates/tqsdk-task/src/target_pos/report.rs crates/tqsdk-core/README.md docs/architecture/api-layers.md docs/architecture/validation.md docs/reviews/comprehensive-review-2026-04-30.md docs/superpowers/plans/2026-04-29-review-remediation-plan.md docs/superpowers/plans/2026-05-01-typed-order-trade-schema-fields.md
git commit -m "refactor: type futures order trade schema fields"
```
