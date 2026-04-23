#![cfg_attr(not(test), forbid(unsafe_code))]
//! Research and offline data tooling for `tqsdk-rust`.
//!
//! `tqsdk-data` hosts research/offline helpers that should not widen the
//! public surface of `tqsdk-session`, `tqsdk-wait`, or `tqsdk-stream`.
//!
//! Current stabilized surfaces stay intentionally narrow:
//!
//! - `DataClient::new().query_his_cont_quotes(...)`
//! - `DataClient::from_session(...).get_kline_data_series(...)`
//!
//! Both return owned Rust-native data without committing to any DataFrame or
//! polars integration yet.

mod client;
mod error;

pub use client::{DataClient, HistoricalContQuotesRow, KlineDataSeries, KlineDataSeriesRequest};
pub use error::{DataError, Result};
