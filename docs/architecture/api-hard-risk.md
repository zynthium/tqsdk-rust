# Durable hard-risk authority

`tqsdk-hard-risk` is an opt-in, single-host SQLite/WAL authority for orders
whose pre-send decision must survive process restart and concurrent local
processes. It is not a default SDK dependency, a broker adapter, or a
multi-node risk service.

## Ownership boundary

`tqsdk-task::RiskEngine` remains process-local advisory/pre-trade logic.
`TradingDeskProfile` remains latency-sensitive and owns no durable writer.
`tqsdk-hard-risk` owns a dedicated database, durable policy activation,
admission reservations, lease fencing, and recovery evidence.

`namespace` must separate live, paper, user, route, and environment
authorities. Sharing a namespace across live and paper is invalid operations.
The stable client order id is not scoped by trading day: reuse on another day
is an identity conflict, never a fresh order.

## Authoritative policy contract

There is one active policy for each `(namespace, account_id)`. Only the
administrative transition below may change it:

```text
install_policy(policy_scope, policy, Absent | Current(revision))
```

The expectation is compare-and-set. The durable policy row contains id,
version, rules, reservation semantics, SHA-256 content fingerprint, activation
time, and append-only activation audit. An admission calls `prepare(request)`,
not `prepare(request, policy)`, so an untrusted worker cannot raise a limit for
one order.

Version one rule ids are fixed:

- `order_attempt`: one counted prepared attempt;
- `open_volume`: requested opening volume.

Daily usage is keyed by stable rule id, not policy version. Changing a version
string cannot reset consumed capacity. A semantic reset requires a new rule id,
explicit operational approval, and a documented schema/API migration.

## Submission lifecycle

1. Build `HardRiskOrderRequest` with scope, stable client id, symbol, side,
   offset, volume, and optional exact IEEE-754 price bits.
2. `prepare(request)` opens a short `BEGIN IMMEDIATE` transaction. It first
   checks identity/fingerprint deduplication, reads active policy, checks daily
   usage, writes `Prepared` and reservations, advances usage, appends audit,
   then commits. Same request returns the existing row; changed request with
   same key fails closed.
3. `begin_submission(identity, owner_id)` commits
   `Prepared -> Submitting`, monotonically increases a lease generation, and
   records an expiry. No database transaction spans network I/O.
4. Prefer `submit_once(permit, |admitted| send(admitted))`. The permit is
   non-cloneable and consumed. The callback receives only the immutable
   admitted request, including stable client id. A success receipt must bind
   that same client id; callback error, invalid receipt, or mismatched receipt
   becomes `Indeterminate`.
5. `record_submitted` and `record_indeterminate` are the lower-level consumed
   permit APIs. An expired lease fences the old permit and changes the row to
   `Indeterminate`; it can never authorize a resend.

```text
Prepared -> Submitting -> Submitted -> Terminal
                |             |
                |             +-- observe_terminal: normal local receipt path
                v
          Indeterminate
                |
                +-- complete exact snapshot + reconcile_terminal -> Terminal
```

`observe_terminal` handles only the local `Submitted` path. It cannot silently
resolve `Submitting` or `Indeterminate`. `reconcile_terminal` records a
distinct `recovered_submission` audit event before terminal evidence, so a
recovered remote submission cannot be mistaken for a locally persisted receipt.
No transition sends network traffic, deletes history, or decreases counted
usage.

## Recovery and unresolved work

`HardRiskCompleteTradeSnapshot` contains one exact
`(namespace, account_id, trading_day)` and consumed runtime revision. It is a
caller-provided completeness assertion: applications may construct it only
after their own authoritative trade snapshot check finishes.

`recover_expired_submissions(CompleteTradeSnapshot(snapshot))` changes only
expired `Submitting` rows in that exact scope and returns the affected records.
An incomplete snapshot fails closed and writes nothing. Operators use
`unresolved_orders(scope, limit)` for bounded, exact-scope listing; there is no
database-wide unscoped recovery sweep. Late or ambiguous broker outcomes must
be reconciled from a matching complete snapshot, never retried automatically.

## Failure, storage, and clock contract

SQLite busy timeout, full disk, corruption, schema mismatch, foreign-key
failure, invalid stored data, and monotonic-clock regression all mean **do not
submit**. Every mutating transition advances a persisted high-water clock; if
wall time moves backwards, the authority fails closed.

Only durable local SQLite file paths are accepted. Empty paths, `:memory:`, and
`file:` URI paths are rejected. The authority uses `WAL`, `synchronous=FULL`,
foreign keys, and short immediate transactions. It is safe only for one host's
cooperating processes; quorum, replication, leader election, and multi-host
fencing require an external authority with equivalent semantics.

Keep SQLite work outside runtime partition locks and latency-critical market
read sections. A caller may use bounded blocking workers, but must not turn
storage pressure into an unbounded queue.

## Schema, rollout, rollback

Version-one initialization accepts only an empty database and sets
`PRAGMA user_version = 1` atomically. Existing unversioned `hard_risk_*`
tables and unknown newer versions are rejected; this crate never rewrites,
drops, or backfills automatically.

Reopen verification checks contract metadata, required tables, required
columns, indexes, foreign keys, check constraints, and `foreign_key_check`.
Schema publication is an explicit contract, not only a user-version number.

Rollout steps:

1. Create a new dedicated local database path and take an operator-managed
   filesystem/snapshot backup.
2. Open authority; require successful WAL/full-sync and schema verification.
3. Install policy through explicit CAS; record namespace/account ownership.
4. Exercise process-crash boundaries before production: before/after
   `Prepared`, send, `Submitted`, lease expiry, complete/incomplete snapshot,
   and client-id mismatch.

Future incompatible schemas require a dedicated migration tool, backup,
old/new compatibility window, idempotent retry behavior, and recovery tests.
Older binaries must refuse a newer schema.

## Required validation

```bash
cargo test -p tqsdk-hard-risk
cargo clippy -p tqsdk-hard-risk --all-targets --no-deps -- -D warnings
cargo check -p tqsdk-hard-risk --examples
```

The crate tests policy CAS/persistence, durable dedup/usage, consumed permit
binding, mismatched receipt fail-closed behavior, lease fencing, scoped
recovery/unresolved listing, monotonic-clock refusal, WAL rollback, schema
contract rejection, restart behavior, and competing local processes. Live
broker tests remain explicit credential and order-side-effect gated.
