#![cfg_attr(not(test), forbid(unsafe_code))]
//! Research and offline data tooling for `tqsdk-rust`.
//!
//! `tqsdk-data` hosts research/offline helpers that should not widen the
//! public surface of `tqsdk-session`, `tqsdk-wait`, or `tqsdk-stream`.
//!
//! The first stabilized surface is `DataClient::query_his_cont_quotes`, which
//! provides a thin Rust-native wrapper around historical continuous-contract
//! table lookup without committing to any DataFrame/polars API yet.

mod client;
mod error;

pub use client::{DataClient, HistoricalContQuotesRow};
pub use error::{DataError, Result};
