#![cfg_attr(not(test), forbid(unsafe_code))]
//! Scenario 48: pinned historical universe for cache-backed local replay.

use tqsdk::Tq;
use tqsdk_data::{
    ActiveInterval, CatalogContract, CatalogSnapshot, DynamicUniverseScope,
    HistoricalUniverseArtifactStore, UniverseBudget,
};

const CACHE_ROOT: &str = ".tqsdk/backtest_ticks";
const V4_PLAN_SHA256: &str =
    "sha256:0000000000000000000000000000000000000000000000000000000000000000";

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
    // The source-compatible V1-V3 entry point remains available.
    let _legacy_backtest = Tq::futures()
        .backtest(0, 60_000_000_000)
        .cache_dir(CACHE_ROOT)?
        .cache_only()
        .historical_universe_plan(plan()?)?;

    // A cache fill report supplies the real V4 hash. Loading through the
    // artifact store verifies its canonical bytes; preparation additionally
    // verifies acquisition, semantic-catalog and V3 rollback links.
    if let Ok(artifact) =
        HistoricalUniverseArtifactStore::new(CACHE_ROOT).load_plan_artifact(V4_PLAN_SHA256)
    {
        let _v4_backtest = Tq::futures()
            .backtest(0, 60_000_000_000)
            .cache_dir(CACHE_ROOT)?
            .cache_only()
            .historical_universe_artifact(artifact)?;
    }

    // `prepare().await?.connect().await?` then uses only the pinned chain and cache.
    Ok(())
}
