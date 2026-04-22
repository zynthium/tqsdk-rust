#![cfg_attr(not(test), forbid(unsafe_code))]
//! Research and offline data tooling for `tqsdk-rust`.
//!
//! `tqsdk-data` is intentionally scaffold-only for now. The crate exists to
//! reserve a stable landing zone for history download, offline batch query,
//! local materialization, and optional tabular adapters without widening the
//! public surface of `tqsdk-session`, `tqsdk-wait`, or `tqsdk-stream`.
//!
//! No stable public API is exported yet.
