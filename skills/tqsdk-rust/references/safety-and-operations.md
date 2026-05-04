# Safety and Operations

## Credentials and Permissions

- Use `TQ_AUTH_USER` and `TQ_AUTH_PASS` in examples unless the user provides another auth path.
- Mention that live examples need market data permissions and, for trading, account permissions.
- Use `has_feature(...)` or `check_md_grants(...)` when a workflow depends on specific market access.

## Simulation First

- Prefer `TqKq` / simulation examples for order placement.
- Make real broker integration opt-in and explicit.
- Never hide order submission inside setup helpers or examples that look read-only.

## Order Safety

- Prefer typed order builders, `OrderPrice`, `OrderTicket`, task-layer builders, and stable client intent IDs.
- Use session-scoped intent ledgers or task tickets to avoid duplicate submission on retry.
- Do not parse command status or order status strings when typed helpers exist.
- Do not maintain a private order overlay that can diverge from runtime state.

## Runtime Safety

- One visible state path should flow through runtime commits and readers.
- In hot paths, prefer partition reads such as `read_market_state()`, `read_trade_state()`, or `read_market_trade_state()`.
- Use stream sinks or sidecars for slow logging and persistence instead of blocking trading decisions.

## Testing

- For strategy logic, prefer `tqsdk-task::testing` fake market/fake broker tools.
- Use live smoke tests only when credentials, permissions, endpoints, and side effects are explicitly accepted.
- Keep integration tests gated by environment variables when they require real services.
