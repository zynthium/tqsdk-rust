#![cfg_attr(not(test), forbid(unsafe_code))]

/// Embedded single-page market data integrity dashboard.
///
/// The assets live next to this module so the relay remains a self-contained
/// binary without a frontend build step or external runtime dependencies.
pub const DASHBOARD_HTML: &str = include_str!("dashboard/index.html");
pub const DASHBOARD_JS: &str = include_str!("dashboard/app.js");
