# tqsdk-relay

`tqsdk-relay` is an optional market relay and in-memory cache service for
`tqsdk-rust`. It sits between SDK clients and the Tianqin market websocket so
multiple local SDK processes can share one upstream futures tick source instead
of each expanding its own quote and K-line subscriptions.

Use it when a single Tianqin connection can carry the futures tick universe, but
many clients or many K-line periods would push direct SDK connections over
subscription limits.

> [!IMPORTANT]
> The relay is opt-in infrastructure. Existing SDK clients still connect
> directly to Tianqin unless you explicitly point their market endpoint at a
> relay instance with `.market_relay(...)`.

> [!WARNING]
> V1 is market-only and intentionally narrow. It does not proxy trade, query,
> auth, schema, metadata, or direct-query traffic.

## How It Fits

```text
SDK process A ─┐
SDK process B ─┼─ ws://127.0.0.1:7788/market ─ tqsdk-relay ─ Tianqin market websocket
SDK process C ─┘
```

The relay keeps the SDK runtime model unchanged:

- SDK state still flows through the normal `RuntimeHandle -> StateStore ->
  CommitResult -> RuntimeReader/UpdateCursor` path.
- Existing SDK crates do not depend on `tqsdk-relay`.
- Relay adoption is a deployment choice, not a default behavior change.

## Current Capability

| Area | Status |
| --- | --- |
| Downstream websocket server | Accepts SDK market websocket connections on a local address. |
| Downstream command subset | Handles `subscribe_quote`, `set_chart`, and `peek_message`. Unknown market commands fail explicitly. |
| Upstream source | Opens one Tianqin market websocket and sends a duration-`0` `set_chart` for the configured futures universe. |
| Quote fan-out | Projects the latest tick into quote frames and sends them to interested downstream clients. |
| Fixed-duration K-line synthesis | Builds positive-duration K-lines from upstream ticks and emits completed bars to chart subscribers. |
| Cache | Keeps in-memory tick rings and quote snapshots. No disk persistence is enabled in the binary yet. |
| Bootstrap queue | Coalesces/rate-limits chart bootstrap requests internally. Remote K-line backfill and oracle comparison are not implemented yet. |
| Upstream recovery | Keeps the downstream listener up and retries upstream websocket connection failures. |
| Observability structs | Exposes health, metrics, and source-status snapshots in the library API. The `metrics_listen` address is reserved; no HTTP metrics endpoint is served yet. |

Duration-`0` downstream tick chart compatibility is not the primary completed
surface in V1: the relay ingests and caches upstream ticks, but the verified live
downstream fan-out currently focuses on quotes and positive-duration K-lines.

## Quick Start

Create a futures universe file. Use one symbol per line for large universes:

```text
SHFE.au2602
DCE.m2609
CZCE.MA609
```

Run the relay:

```bash
TQSDK_RELAY_FUTURES_SYMBOLS_FILE="./futures-symbols.txt" \
cargo run -p tqsdk-relay
```

By default the process listens for SDK market websocket clients on
`127.0.0.1:7788` and connects upstream to
`wss://openmd.shinnytech.com/t/md/front/mobile`.

Point an SDK client at the relay:

```rust
let mut tq = tqsdk::Tq::futures()
    .auth_env()?
    .market_relay("ws://127.0.0.1:7788/market")
    .connect()
    .await?;
```

Without `.market_relay(...)`, the same SDK client uses the normal direct
Tianqin market endpoint.

For a small smoke test universe, inline symbols are also supported:

```bash
TQSDK_RELAY_FUTURES_SYMBOLS="SHFE.au2602,DCE.m2609" \
cargo run -p tqsdk-relay
```

If neither `TQSDK_RELAY_FUTURES_SYMBOLS` nor
`TQSDK_RELAY_FUTURES_SYMBOLS_FILE` is set, the relay starts only the downstream
service and does not connect upstream. That mode is useful for local protocol
smoke tests, but it will not produce live market data.

## Configuration

The binary reads the following environment variables:

| Variable | Default | Description |
| --- | --- | --- |
| `TQSDK_RELAY_FUTURES_SYMBOLS` | empty | Comma-separated futures symbols for the single upstream tick chart. Mutually exclusive with `TQSDK_RELAY_FUTURES_SYMBOLS_FILE`. |
| `TQSDK_RELAY_FUTURES_SYMBOLS_FILE` | empty | Path to a symbol file. The file may use line breaks or commas as separators. Blank entries are rejected. Recommended for all-futures universes. |
| `TQSDK_RELAY_UPSTREAM_MARKET_URL` | `wss://openmd.shinnytech.com/t/md/front/mobile` | Upstream Tianqin market websocket URL. |
| `TQSDK_RELAY_DOWNSTREAM_LISTEN` | `127.0.0.1:7788` | Downstream SDK websocket listen address. |
| `TQSDK_RELAY_METRICS_LISTEN` | `127.0.0.1:7789` | Reserved metrics listen address. The current binary logs it but does not bind an HTTP metrics server. |

Library users can construct `RelayConfig` directly to tune defaults that are not
currently exposed through environment variables, including:

- `tick_ring_capacity`: default `200_000` rows per symbol.
- `kline_ring_capacity`: default `10_000` rows.
- `bootstrap.max_concurrent_remote_charts`: default `4`.
- `bootstrap.min_remote_request_interval`: default `250ms`.
- `bootstrap.per_series_cooldown`: default `30s`.

## Market Behavior

### Upstream subscription

For a configured futures universe, the relay creates one upstream chart:

```json
{
  "aid": "set_chart",
  "chart_id": "relay-upstream-all-futures-ticks",
  "ins_list": "DCE.m2609,SHFE.au2602",
  "duration": 0,
  "view_width": 10000
}
```

It then sends `{"aid":"peek_message"}` and decodes `rtn_data` tick fragments
from the upstream websocket.

### Downstream command subset

| Command | Relay behavior |
| --- | --- |
| `subscribe_quote` | Registers quote interest for the client and emits quote updates derived from the latest ingested tick. |
| `set_chart` with positive `duration` | Registers K-line chart interest, records a bootstrap request, and emits completed synthetic bars when ticks cross into later windows. |
| `set_chart` with `duration <= 0` | Parsed and registered, but duration-`0` live tick chart fan-out is not complete in the V1 server surface. |
| `peek_message` | Accepted as a no-op compatibility command. |

### K-line synthesis

Synthetic fixed-duration K-lines use `[start, end)` windows. A tick whose
timestamp equals the end boundary belongs to the next bar.

The relay only emits a completed bar after a tick arrives in a later window. It
does not create empty bars for windows with no ticks, and it does not use local
wall-clock time to close bars.

## Operational Notes

- Keep the downstream listener on loopback, a private network, or behind your own
  access control. The relay does not authenticate downstream clients.
- Prefer `TQSDK_RELAY_FUTURES_SYMBOLS_FILE` for large universes so shell command
  lines and process listings do not become unwieldy.
- The relay is designed to reduce subscription string growth by sharing one
  upstream tick chart; it should not be used as a generic Tianqin proxy.
- Upstream connection failures mark the source degraded and are retried. Existing
  downstream connections are not intentionally dropped just because upstream is
  temporarily unavailable.
- Cache is in-memory today. Restarting the relay loses tick, quote, and K-line
  materialization state.

## Development

Useful checks while working on the relay crate:

```bash
cargo test -p tqsdk-relay --tests
cargo clippy -p tqsdk-relay --all-targets -- -D warnings
cargo check -p tqsdk-relay --no-default-features
```

The websocket tests use loopback test servers; they do not require Tianqin
credentials or live market access.
