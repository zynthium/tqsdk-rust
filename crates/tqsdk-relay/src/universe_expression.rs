#![cfg_attr(not(test), forbid(unsafe_code))]

pub use tqsdk_data::{
    ExpandedUniverseInput, SnapshotUniverseDispatch, UniverseClause, UniverseExpression,
    UniverseInput, UniverseMode, UniverseSelector, UniverseSelectorKind, UniverseSpec,
    UniverseView, parse_snapshot_universe_compatible,
};
