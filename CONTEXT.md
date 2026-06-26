# tqsdk-rust Context

Domain language for the TQSDK Rust workspace. Terms here name project concepts used across architecture discussion.

## Language

**Backtest Performance Ledger**:
The record of local backtest observations used to derive account performance, return, drawdown, and trade outcome facts.
_Avoid_: metrics helper, summary math, report builder

**Metadata Symbol Decoder**:
The rulebook that turns official metadata GraphQL payloads into symbol info, option lists, ATM option picks, and option level groups.
_Avoid_: metadata helpers, query parser, option utility functions

**Relay Dashboard Read Model**:
The projection that turns detached relay engine observations into dashboard snapshots, compact symbol rows, timeline samples, and timeline history.
_Avoid_: engine dashboard helpers, UI metrics formatter, observability snapshot builder

**Local Backtest Recipe**:
The facade-side plan that turns replay sources, declared quote symbols, price ticks, and instrument specs into a local offline backtest runtime.
_Avoid_: builder fields, backtest helper arguments, local connect options
