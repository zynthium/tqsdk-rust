#![cfg_attr(not(test), forbid(unsafe_code))]
//! Scenario 48: pinned historical universe for cache-backed local replay.

use tqsdk::Tq;
use tqsdk_data::{
    ActiveInterval, CatalogContract, CatalogSnapshot, DynamicUniverseScope, UniverseBudget,
};

fn plan() -> tqsdk_data::Result<tqsdk_data::HistoricalUniversePlan> {
    let scope = DynamicUniverseScope::all();
    CatalogSnapshot::new(
        "exchange-catalog-2026-01",
        "calendar-sha256:2026-01",
        true,
        scope.clone(),
        vec![CatalogContract::new(
            "SHFE.au2606",
            "SHFE",
            "au",
            vec![ActiveInterval::new(0, 60_000_000_000)?],
        )?],
    )?
    .compile_timeline(0, 60_000_000_000, scope, [])?
    .prepare(UniverseBudget::new(10_000, 100_000)?)
}

fn main() -> tqsdk::Result<()> {
    let _backtest = Tq::futures()
        .backtest(0, 60_000_000_000)
        .cache_dir(".tqsdk/backtest_ticks")?
        .cache_only()
        .historical_universe_plan(plan()?)?;

    // `prepare().await?.connect().await?` uses only the pinned plan and cache.
    Ok(())
}
