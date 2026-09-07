//! Offline timeline maintenance; never creates a session or fills missing rows.
use crate::{CliError, CommandOutcome, DaysArgs};
use chrono::{Datelike, NaiveDate, Weekday};
use clap::Args;
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
};
use tqsdk_data::{TradingTimelineBuildRequest, TradingTimelineRuleCatalog, TradingTimelineStore};

#[derive(Debug, Args)]
pub(crate) struct TimelineArgs {
    /// Reviewed historical session catalog, not a version-based calendar guess.
    #[arg(long)]
    pub catalog: PathBuf,
    #[command(flatten)]
    pub days: DaysArgs,
    /// Canonical index to rebuild. Omit to audit every product in the catalog.
    #[arg(long = "symbol")]
    pub symbols: Vec<String>,
    /// Activate only when every requested day has a confirmed unique decision.
    #[arg(long)]
    pub apply: bool,
}

pub(crate) fn rebuild(
    root: &Path,
    catalog_path: &Path,
    start: NaiveDate,
    end: NaiveDate,
    symbols: &[String],
    apply: bool,
) -> Result<Value, CliError> {
    let catalog = TradingTimelineRuleCatalog::from_json_path(catalog_path)?;
    rebuild_with_catalog(root, &catalog, start, end, symbols, apply)
}

fn rebuild_with_catalog(
    root: &Path,
    catalog: &TradingTimelineRuleCatalog,
    start: NaiveDate,
    end: NaiveDate,
    symbols: &[String],
    apply: bool,
) -> Result<Value, CliError> {
    if start > end {
        return Err(CliError::Usage("timeline start day exceeds end day".into()));
    }
    let mut days = Vec::new();
    let mut day = start;
    loop {
        if !matches!(day.weekday(), Weekday::Sat | Weekday::Sun) {
            days.push(day);
        }
        if day == end {
            break;
        }
        day = day
            .succ_opt()
            .ok_or_else(|| CliError::Usage("timeline day overflow".into()))?;
    }
    let products = catalog
        .rules
        .iter()
        .filter(|rule| symbols.is_empty() || symbols.contains(&rule.evidence_symbol))
        .map(|rule| {
            (
                rule.exchange.clone(),
                rule.product.clone(),
                rule.evidence_symbol.clone(),
            )
        })
        .collect::<BTreeSet<_>>();
    if products.is_empty()
        || symbols
            .iter()
            .any(|symbol| !products.iter().any(|p| &p.2 == symbol))
    {
        return Err(CliError::Usage(
            "timeline symbol is absent from catalog".into(),
        ));
    }
    let requests = products
        .into_iter()
        .map(|(exchange, product, symbol)| {
            TradingTimelineBuildRequest::new(
                exchange,
                product,
                symbol,
                "resolved-under-root-gate",
                days.clone(),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let store = if apply {
        TradingTimelineStore::open(root)?
    } else {
        TradingTimelineStore::open_read_only(root)
    };
    let snapshots = store.rebuild_from_cache(catalog, requests, apply)?;
    Ok(json!({
        "command": "timeline",
        "activated": apply,
        "catalog_hash": catalog.catalog_hash(),
        "products": snapshots.iter().map(|snapshot| json!({
            "exchange": snapshot.identity.exchange, "product": snapshot.identity.product,
            "timeline_hash": snapshot.timeline_hash, "evidence_identity": snapshot.identity.evidence_identity,
            "known_ranges": snapshot.known_ranges, "open_intervals": snapshot.open_intervals.len(),
            "decisions": snapshot.decisions,
        })).collect::<Vec<_>>(),
    }))
}

pub(crate) fn run(root: Option<&Path>, args: TimelineArgs) -> Result<CommandOutcome, CliError> {
    let (_, root) = tqsdk_cache::open_read_only_cache(root)?;
    let value = rebuild(
        &root,
        &args.catalog,
        args.days.start_day,
        args.days.end_day,
        &args.symbols,
        args.apply,
    )?;
    Ok(CommandOutcome {
        value,
        exit_code: 0,
    })
}

// Both explicit-minute and historical-universe fills finish here after their
// fill/finalization guards have been dropped. No credentials are accessed.
pub(crate) fn after_fill(
    catalog: &TradingTimelineRuleCatalog,
    outcome: &mut CommandOutcome,
) -> Result<(), CliError> {
    let days = outcome
        .value
        .get("requested_days")
        .or_else(|| {
            outcome
                .value
                .get("report")
                .and_then(|report| report.get("requested_days"))
        })
        .ok_or_else(|| CliError::Usage("fill result missing timeline day range".into()))?;
    let days: tqsdk_cache::TradingDayWindow = serde_json::from_value(days.clone())?;
    let start = NaiveDate::parse_from_str(&days.start_day, "%Y-%m-%d")
        .map_err(|error| CliError::Usage(error.to_string()))?;
    let end = NaiveDate::parse_from_str(&days.end_day, "%Y-%m-%d")
        .map_err(|error| CliError::Usage(error.to_string()))?;
    let root = outcome
        .value
        .get("cache_dir")
        .and_then(Value::as_str)
        .ok_or_else(|| CliError::Usage("fill result missing canonical cache root".into()))?;
    match rebuild_with_catalog(Path::new(root), catalog, start, end, &[], true) {
        Ok(report) => outcome.value["trading_timeline"] = report,
        Err(error) => {
            let uncertain = match &error {
                CliError::Data(tqsdk_data::DataError::Io(io)) => io.get_ref().and_then(|inner| {
                    inner.downcast_ref::<tqsdk_data::TradingTimelineDurabilityUncertain>()
                }),
                _ => None,
            };
            outcome.value["trading_timeline"] =
                json!({"activation_complete": false, "error": error.to_string()});
            if let Some(uncertain) = uncertain {
                outcome.value["trading_timeline"]["activation_state"] =
                    json!(durability_activation_state(&uncertain.path));
                outcome.value["trading_timeline"]["committed_path"] = json!(uncertain.path);
            }
            outcome.exit_code = error.exit_code();
        }
    }
    Ok(())
}

fn durability_activation_state(path: &Path) -> &'static str {
    // V1 committed a separate pointer; V2's sole `timeline.json` is itself
    // the visible generation. A post-rename directory-sync error therefore
    // leaves either path potentially visible and requires a reload.
    if path
        .file_name()
        .is_some_and(|name| name == "active.json" || name == "timeline.json")
    {
        "indeterminate"
    } else {
        "body_durability_uncertain"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        sync::atomic::{AtomicU64, Ordering},
    };
    use tqsdk::advanced::core::Kline;
    use tqsdk_data::{
        BacktestTickCache, MinuteKlineCache, MinuteKlineCacheSnapshot,
        backtest_tick_trading_day_range,
    };

    #[test]
    fn compact_timeline_durability_is_indeterminate_after_rename() {
        assert_eq!(
            durability_activation_state(Path::new(
                "trading-timeline-v1/products/INE/sc/timeline.json"
            )),
            "indeterminate"
        );
        assert_eq!(
            durability_activation_state(Path::new(
                "trading-timeline-v1/products/INE/sc/timeline.json"
            )),
            "indeterminate"
        );
    }

    #[test]
    fn both_fill_report_shapes_maintain_timeline_without_a_remote_session() {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "timeline-cli-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).unwrap();
        let catalog_path = root.join("catalog.json");
        fs::write(&catalog_path, r#"{
            "catalog_version":1,"exception_review_complete":true,"generated_at":"2026-09-06","timezone":"Asia/Shanghai",
            "interval_semantics":"half-open","night_session_semantics":"following trading day",
            "rules":[{"exchange":"INE","product":"sc","evidence_symbol":"KQ.i@INE.sc",
                "rule_id":"fixture-day","candidate_trading_day_start":"2024-01-01",
                "candidate_trading_day_end":"2024-12-31","sessions_cst":[["09:00","09:02"]],
                "validation":{"status":"confirmed","required_symbol":"KQ.i@INE.sc","required_check":"fixture"}}]
        }"#).unwrap();
        let day = NaiveDate::from_ymd_opt(2024, 1, 8).unwrap();
        let range = backtest_tick_trading_day_range(day).unwrap();
        let cache = MinuteKlineCache::open(&root).unwrap();
        cache
            .store_final_range(
                "KQ.i@INE.sc",
                range.start_ns,
                range.end_ns,
                &MinuteKlineCacheSnapshot::cst_v1(),
                &[Kline {
                    id: 1,
                    datetime: range.end_ns - 9 * 3_600_000_000_000,
                    volume: 1,
                    open: 1.,
                    high: 1.,
                    low: 1.,
                    close: 1.,
                    open_oi: 1,
                    close_oi: 1,
                    ..Kline::default()
                }],
            )
            .unwrap();
        let days = tqsdk_cache::TradingDayWindow::from_days(day, day).unwrap();
        for historical in [false, true] {
            let mut outcome = CommandOutcome {
                value: json!({"cache_dir":root,"complete":true}),
                exit_code: 0,
            };
            if historical {
                outcome.value["requested_days"] = json!(days);
            } else {
                outcome.value["report"] = json!({"requested_days":days,"complete":true});
            }
            after_fill(
                &TradingTimelineRuleCatalog::from_json_path(&catalog_path).unwrap(),
                &mut outcome,
            )
            .unwrap();
            assert_eq!(outcome.exit_code, 0);
            assert_eq!(outcome.value["trading_timeline"]["activated"], true);
        }
        let before = TradingTimelineStore::open_read_only(&root)
            .load_active("INE", "sc")
            .unwrap()
            .unwrap();
        let guard = BacktestTickCache::open_read_only(&root)
            .try_acquire_remote_fill_shared_lock()
            .unwrap();
        let mut outcome = CommandOutcome {
            value: json!({"cache_dir":root,"requested_days":days,"complete":true}),
            exit_code: 0,
        };
        after_fill(
            &TradingTimelineRuleCatalog::from_json_path(&catalog_path).unwrap(),
            &mut outcome,
        )
        .unwrap();
        assert_eq!(outcome.exit_code, 0);
        assert_eq!(outcome.value["trading_timeline"]["activated"], true);
        assert_eq!(outcome.value["complete"], true);
        drop(guard);
        let after = TradingTimelineStore::open_read_only(&root)
            .load_active("INE", "sc")
            .unwrap()
            .unwrap();
        assert_eq!(
            before.snapshot().timeline_hash,
            after.snapshot().timeline_hash
        );
        fs::remove_dir_all(root).unwrap();
    }
}
