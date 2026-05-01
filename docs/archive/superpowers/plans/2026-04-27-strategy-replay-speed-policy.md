## Strategy Replay Speed Policy Plan

**Goal:** Add a minimal public speed policy to `tqsdk-task::StrategyReplay`.

**Architecture:** Keep pacing in `tqsdk-task`, because it belongs to strategy replay runtime
behavior. `tqsdk-data::MarketCacheReplay` remains only an ordered event source. The policy
must not add a second state tree, private revisions, channels, background tasks, or durable
checkpoint storage.

**Public API:**

- Add `StrategyReplaySpeed`.
- Default is `StrategyReplaySpeed::FASTEST`.
- `StrategyReplaySpeed::REAL_TIME` sleeps according to adjacent event-time deltas.
- `StrategyReplaySpeed::scaled(multiplier)` returns `Result<Self>` and rejects non-finite or
  non-positive multipliers.
- Add `StrategyReplayBuilder::speed(...)`.
- Add `StrategyReplay::speed()`.

**Behavior:**

- The first replay event never sleeps.
- Later events sleep before ingesting the next market event.
- Non-increasing event times do not sleep.
- `resume_from(checkpoint)` preserves replay time, so a paced replay can continue from the
  checkpoint's last event time.

**Out of Scope:**

- Durable checkpoint persistence.
- Multi-series replay builder.
- Live/sim/replay environment abstraction.
- Calendar-aware or exchange-session-aware pacing.
