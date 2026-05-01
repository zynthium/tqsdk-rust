# Protocol-Complete Runtime Contract Design

> Archived on 2026-05-01.
> Current architecture authority lives in `docs/architecture/*`.

**Date:** 2026-04-19
**Status:** Approved design draft
**Scope:** V1 foundation layer for a Rust-native TqSdk runtime

## 1. Summary

V1 is not a user-facing `wait_update()` SDK and not a `stream/callback` SDK.
V1 is a protocol-complete runtime contract that unifies all remote interactions under one commit model.

This runtime contract must be sufficient to support, without core redesign:

- a Python-style `wait_update` facade
- a Rust-style `stream/callback` facade

The V1 runtime must cover:

- all DIFF-protocol-backed objects
- trade commands and trade state
- replay/feed commands and replay state
- auth/session/system control
- GraphQL / HTTP query flows
- schema / metadata / bootstrap interactions

V1 must not provide any high-level user facade.

## 2. Goals

- Define one stable runtime contract for all remote protocols and objects.
- Ensure all visible state flows through one `Revision` / `CommitResult` / `ChangeSet` model.
- Make `wait_update` and `stream/callback` future adapters over the same commit log and cursor model.
- Keep protocol-specific complexity inside adapters rather than inside user-facing facades.
- Preserve enough semantic fidelity to later build Python-compatible behavior without rewriting the kernel.

## 3. Non-Goals

V1 explicitly does not provide:

- `TqApi`
- `wait_update()` facade
- stream facade
- callback facade
- high-level quote / kline / tick / order / account views
- `TargetPosTask`
- strategy/task orchestration
- DataFrame / polars / downloader / web helper / GUI / report layers
- Python surface compatibility at the API naming level
- end-user ergonomics for strategy authors

V1 is judged on contract completeness, not end-user convenience.

## 4. Public Boundary

V1 exposes exactly two stable public surfaces.

### 4.1 Runtime Contract

The runtime contract is the only canonical public entry point for V1.

It includes:

- `RuntimeHandle`
- `RuntimeCommand`
- `RuntimeInput`
- `Revision`
- `CommitResult`
- `ChangeSet`
- `StateSnapshot`
- `UpdateCursor`
- command/result identity types such as `CommandId` and `CursorId`

### 4.2 Protocol Adapter Contract

The protocol adapter contract is the only stable extension surface in V1.

It includes:

- `ProtocolAdapter`
- `ProtocolDomain`
