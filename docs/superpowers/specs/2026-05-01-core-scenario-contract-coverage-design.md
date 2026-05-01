# Core Scenario Contract Coverage Design

## Purpose

Close the scenario-contract coverage gaps identified in
`docs/reviews/public-api-scenario-review.md` without expanding the SDK's core
capability boundary. The work adds or promotes formal
`api_contract_sXX_*.rs` examples for core SDK workflows that already belong in
`tqsdk-wait`, `tqsdk-session`, `tqsdk-data`, or `tqsdk-task`.

This is documentation and contract coverage work first. Any implementation
change is only allowed when a proposed contract cannot compile against the
current public API and the missing piece is inside the already-approved crate
boundary.

## Non-Goals

- Do not add platform capabilities such as multi-provider aggregation,
  cross-process cache service orchestration, HTTP metrics endpoints, GUI/web
  helpers, production daemon managers, global risk services, or durable audit
  platforms.
- Do not move direct-query helpers into `tqsdk-wait` or `tqsdk-stream`.
- Do not move research/download/Greeks helpers into `tqsdk-session`.
- Do not add a new crate for this batch.
- Do not redesign runtime commit, revision, cursor, or state ownership.

## Design

Use five new scenario groups, numbered after the existing S1-S24 matrix:

### S25: Wait Serial And Trading Status

Target crate: `tqsdk-wait`

Formal example:
`crates/tqsdk-wait/examples/api_contract_s25_wait_serial_trading_status.rs`

This scenario covers the single-strategy wait facade for:

- `TqApi::get_trading_status(...)`
- `TqApi::get_kline_serial(...)`
- `TqApi::get_tick_serial(...)`
- `wait_update(...)`
- `is_changing(...)` / `is_changing_fields(...)`

The example should prove that trading status and serial windows are live,
diff-backed wait objects, not direct-query metadata and not data-layer history
downloads.

### S26: Wait Trade And System Live Refs

Target crate: `tqsdk-wait`

Formal example:
`crates/tqsdk-wait/examples/api_contract_s26_trade_system_refs.rs`

This scenario covers less-visible live refs exposed by the wait facade:

- `NotificationRef`
- `SettlementInfoRef`
- `RiskManagementRuleRef`
- `RiskManagementDataRef`
- `confirm_settlement(...)`
- securities account / position / order / trade refs where a compact example
  can cover them without becoming noisy

If the securities portion makes the example too broad, split it into:

- `api_contract_s26_trade_system_refs.rs`
- `api_contract_s26_security_trade_refs.rs`

Both remain S26 because the user-layer need is the same: live trade/system refs
should be discoverable and contract-tested without creating a new scenario
family.

### S27: Session Metadata And Service Query Pack

Target crate: `tqsdk-session`

Formal example:
`crates/tqsdk-session/examples/api_contract_s27_metadata_service_queries.rs`

This scenario covers one-shot direct-query workflows:

- `query_quotes(...)`
- `query_cont_quotes(...)`
- `query_options(...)`
- `query_atm_options(...)`
- `query_all_level_options(...)`
- `query_all_level_finance_options(...)`
- `get_trading_calendar(...)`
- `query_symbol_settlement(...)`
- `query_symbol_ranking(...)`
- `query_edb_data(...)`

The example should make ownership explicit: these APIs belong in session because
they are request/response metadata or service queries. It must not duplicate
these helpers in wait or stream.

### S28: Data Download, Export, And Greeks

Target crate: `tqsdk-data`

Formal examples:

- `crates/tqsdk-data/examples/api_contract_s28_download_export.rs`
- `crates/tqsdk-data/examples/api_contract_s28_option_greeks.rs`

Split S28 into two examples to keep each example focused:

- download/export covers `query_his_cont_quotes`, Kline/Tick page or download
  flows, progress observation, `collect_remaining`, and CSV export to caller
  supplied async writers.
- Greeks covers `query_option_greeks` as an owned research query that may use
  temporary live quote snapshots internally without exposing a generic snapshot
  API.

Both examples reinforce that research, download, CSV materialization, and Greeks
belong in `tqsdk-data`, not session/wait/stream.

### S29: Target Position Ownership

Target crate: `tqsdk-task`

Formal example:
`crates/tqsdk-task/examples/api_contract_s29_target_pos_ownership.rs`

This scenario promotes the target-position workflow to its own contract instead
of leaving it only inside the simple-strategy scenario. It should cover:

- `TaskHost`
- `TargetPosTask`
- `TargetPosScheduler` where practical
- same-account same-symbol ownership
- wait-driven progression through `TaskHost::wait_update()`
- conservative order planning and cancellation/replanning behavior at the
  contract level, without promising automatic hedge/flatten or cross-account
  TargetPos orchestration

## Documentation Updates

For each scenario group:

- Add the formal example file under the target crate.
- Update the target crate README example list if it lists formal examples.
- Update `docs/reviews/public-api-scenario-review.md` with rows S25-S29.
- Update `docs/scenarios/user-layer-iteration-plan.md` so the user-layer table
  and relevant priority sections mention S25-S29.
- Do not move existing non-core gap sketches unless a separate cleanup task is
  explicitly approved.

## Example Header Contract

Every new formal example must keep the established header fields:

- `Scenario`
- `User goal`
- `API contract`
- `Forbidden`
- `Regression signal`
- `Review questions`

Where useful, also include the newer metadata recommended by
`docs/scenarios/user-layer-iteration-plan.md`:

- `Primary user layer`
- `Intended crate path`
- `Lower-level escape hatch`
- `Non-goal`

## Verification

Each completed batch must pass:

```bash
scripts/check_api_contract_examples.sh
cargo check --workspace --examples
```

For any batch that touches implementation to make a contract compile, also run
the narrow crate tests for the touched crate before committing.

## Batch Order

1. S25 and S27 first: these are mostly contract coverage and should reveal
   whether the public API is already coherent.
2. S28 second: keep download/export and Greeks split to avoid a large mixed
   example.
3. S29 third: target-position ownership is more behavior-heavy and should be
   reviewed after the simpler contract gaps are closed.
4. S26 last: trade/system refs cover several object families and should be split
   if a single example becomes too broad.

## Acceptance Criteria

- S25-S29 formal examples exist and compile.
- Scenario review marks these as core scenario-contract coverage, not new SDK
  capability expansion.
- Direct-query APIs remain in `tqsdk-session`.
- Research/download/Greeks APIs remain in `tqsdk-data`.
- Target-position behavior remains in `tqsdk-task`.
- No non-core platform capability is promoted to a formal SDK contract.
