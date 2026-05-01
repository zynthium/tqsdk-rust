# V1 Core Crate Engine Refactor Design

> Archived on 2026-05-01.
> Current architecture authority lives in `docs/architecture/*`.

**Date:** 2026-04-20
**Status:** Proposed design draft
**Scope:** Refactor the V1 runtime-contract crate into a publishable standalone core crate for high-performance consumers and future V2 facade crates

## 1. Summary

The current V1 crate is protocol-complete and semantically correct, but its implementation strategy is still biased toward convenience rather than hot-path efficiency.

The biggest mismatches with the intended audience are:

- `latest_snapshot() -> StateSnapshot` clones the full snapshot on every read
- core runtime state is guarded by coarse `Arc<Mutex<...>>` shells
- public reads are shaped around owned JSON snapshots instead of revision-bound zero-copy views
- runtime internals mix command routing, state storage, commit publication, and reader concerns in a small number of large modules

This refactor keeps V1 protocol semantics intact while changing the internal engine and part of the public core surface so the crate can be published as:

- a stable low-level runtime crate
- a dependency target for future V2 wait/stream/callback facade crates
- a performance-oriented base for advanced users rather than a research-oriented convenience SDK

## 2. Goals

- Preserve the existing protocol-complete semantic contract:
  - DIFF objects
  - trade commands and state
  - replay/feed progression
  - auth/session/system control
  - GraphQL / HTTP query
  - schema / metadata / bootstrap
- Replace clone-heavy public reads with revision-bound zero-copy read access.
- Separate internal runtime responsibilities into smaller modules with explicit ownership.
- Keep the crate maintainable enough to publish independently and evolve without V2 coupling.
- Preserve a narrow and defensible public surface for downstream facade crates.
- Improve hot-path cost without attempting a full typed-state rewrite in the same phase.

## 3. Non-Goals

This refactor does not:

- add any V2 facade
- add `wait_update`, stream, callback, or `TqApi`
- introduce typed quote/order/account views
- replace all `serde_json::Value` state with a typed arena store
- redesign protocol semantics, revision semantics, or command semantics
- change transport topology, auth semantics, or query/schema wire behavior

The phase is an engine refactor, not a product-surface expansion.

## 4. Chosen Approach

The chosen approach is a boundary-preserving engine refactor with limited public API breakage.

### 4.1 Why This Approach

It keeps the hardest-won part of V1 intact, which is the unified protocol contract, while fixing the parts that would make this crate a poor long-term dependency for higher-performance consumers.

It deliberately avoids a full typed-state rewrite because that would combine:

- semantic risk
- storage redesign
- API redesign
- migration burden

into one oversized phase.

### 4.2 What Changes

- The read surface becomes reader/guard based instead of clone-a-snapshot based.
- Runtime internals are split into smaller modules with clearer responsibilities.
- `StateSnapshot` stops being the primary stable public read artifact.
- `CommitLog` stops being the primary user-facing read entry point.
- Downstream crates read through a runtime reader abstraction bound to revision/cursor semantics.

### 4.3 What Does Not Change

- `RuntimeCommand`
- `RuntimeInput`
