# tqsdk-hard-risk

`tqsdk-hard-risk` is an opt-in, local SQLite/WAL authority for durable,
fail-closed order admission. It is intentionally outside `tqsdk-core`,
`tqsdk-session`, and `tqsdk-task`: normal SDK and `TradingDeskProfile` paths
must not acquire a durable writer, journal, or cross-process recovery
dependency.

It owns only durable admission state:

- stable `(namespace, account_id, client_order_id)` deduplication;
- authoritative namespace/account policy installation with explicit CAS;
- transactionally reserved daily usage and fenced submission leases;
- `Prepared`, `Submitting`, `Submitted`, `Indeterminate`, and retained
  `Terminal` evidence;
- SQLite `WAL`, `synchronous=FULL`, foreign keys, monotonic-clock guard, and
  fail-closed schema/storage validation.

It does not send orders, store credentials, or infer broker state.

## Required protocol

1. A privileged operator installs one active `HardRiskPolicy` per
   namespace/account with `install_policy(scope, policy, expectation)`.
   `prepare` never accepts a caller-selected policy.
2. Call `prepare(request)`. It deduplicates the stable client id, applies the
   persisted active policy, reserves usage, writes audit evidence, and commits.
3. Call `begin_submission(identity, owner_id)`. A granted permit is not
   cloneable and is valid for one local send attempt only.
4. Prefer `submit_once(permit, |admitted| ...)`: its callback receives the
   immutable admitted request and the permit is consumed into `Submitted` or
   `Indeterminate`. A receipt must contain the same client order id.
5. Never resend `Indeterminate`. Query `unresolved_orders(scope, limit)` and
   resolve ambiguity only with a complete snapshot for the exact scope and
   runtime revision through `reconcile_terminal`.

`observe_terminal` is for the normal local `Submitted` path. It cannot turn an
ambiguous send into terminal state. Lease expiry, invalid/mismatched dispatch
receipt, cancellation, timeout, or storage uncertainty fail closed.

Only durable local file paths are accepted: SQLite memory and URI paths are
rejected. This is a single-host authority, not a multi-node quorum or leader
service.

```toml
[dependencies]
tqsdk-hard-risk = { git = "https://github.com/zynthium/tqsdk-rust" }
```

See [`docs/architecture/api-hard-risk.md`](../../docs/architecture/api-hard-risk.md)
for lifecycle, recovery, schema, rollout, and validation contracts.
