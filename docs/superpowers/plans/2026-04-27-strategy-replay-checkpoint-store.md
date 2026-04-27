## Strategy Replay Durable Checkpoint Store Plan

**Goal:** Add a minimal durable checkpoint persistence foundation for
`tqsdk-task::StrategyReplay`.

**Architecture:** Keep checkpoint persistence in `tqsdk-task`, because the checkpoint is
strategy replay runtime metadata. `tqsdk-data` remains responsible only for cache/history data,
and `tqsdk-core` remains the runtime substrate. The store must not introduce background tasks,
channels, private revisions, or another state tree.

**Public API:**

- Add `StrategyReplayCheckpointStore` as a JSON file-backed checkpoint store.
- Expose `StrategyReplayCheckpointStore::json_file(path)`.
- Expose `load`, `save`, `clear`, and `path`.
- Add `StrategyReplayBuilder::resume_from_store(&store) -> Result<Self>`.

**Behavior:**

- `load` returns `Ok(None)` when the checkpoint file does not exist.
- `save` writes a versioned JSON checkpoint with `next_event_index` and `replay_time_ns`.
- `clear` removes the file and treats missing files as success.
- `resume_from_store` is a no-op when the store is empty.
- Users save checkpoints after their per-event strategy logic completes; `StrategyReplay::next`
  does not auto-save before user logic has run.

**Out of Scope:**

- WAL/fsync crash guarantees.
- Cross-process locking.
- Multi-series replay builder.
- Full live/sim/replay environment abstraction.
