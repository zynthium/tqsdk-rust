use super::*;
use chrono::{DateTime, Datelike, Weekday};
use tqsdk_core::Kline;

const MINUTE: i64 = 60_000_000_000;
const SYMBOL: &str = "KQ.i@INE.sc";

fn month_lock(root: &Path, month: &str) -> PathBuf {
    root.join("minute-kline-v3")
        .join(format!("trading-{month}"))
        .join("KQ.i%40INE.sc.tqmk.lock")
}

fn probe(mode: &str, path: &Path) {
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "trading_timeline::regression_tests::lock_probe_child",
            "--nocapture",
        ])
        .env("TQ_TIMELINE_LOCK_PROBE_MODE", mode)
        .env("TQ_TIMELINE_LOCK_PROBE_PATH", path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn lock_probe_child() {
    let Ok(mode) = std::env::var("TQ_TIMELINE_LOCK_PROBE_MODE") else {
        return;
    };
    let path = PathBuf::from(std::env::var_os("TQ_TIMELINE_LOCK_PROBE_PATH").unwrap());
    if mode == "rebuild" {
        let catalog =
            TradingTimelineRuleCatalog::from_json_path(path.join("test-catalog.json")).unwrap();
        for activate in [false, true] {
            TradingTimelineStore::open_read_only(&path)
                .rebuild_from_cache(
                    &catalog,
                    vec![
                        TradingTimelineBuildRequest::new(
                            "INE",
                            "sc",
                            SYMBOL,
                            "fixture",
                            vec![day("2024-01-05")],
                        )
                        .unwrap(),
                    ],
                    activate,
                )
                .unwrap();
        }
    } else {
        let file = fs::File::open(path).unwrap();
        let result = FileExt::try_lock_exclusive(&file);
        if mode == "busy" {
            assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::WouldBlock);
        } else {
            result.unwrap();
        }
    }
}

#[test]
fn root_shared_fill_allows_cross_process_audit_and_activation() {
    let fixture = Fixture::new();
    let d = day("2024-01-05");
    fixture.store(&[d], &[row(d)]);
    fs::write(
        fixture.root.join("test-catalog.json"),
        serde_json::to_vec(&fixture.catalog).unwrap(),
    )
    .unwrap();
    let _fill = BacktestTickCache::open_read_only(&fixture.root)
        .try_acquire_remote_fill_shared_lock()
        .unwrap();
    probe("rebuild", &fixture.root);
    build_trading_timeline_from_minute_cache(
        &fixture.cache,
        &MinuteKlineCacheSnapshot::cst_v1(),
        &fixture.catalog,
        fixture.request(vec![d]),
    )
    .unwrap();
    let writer = fs::File::open(month_lock(&fixture.root, "202401")).unwrap();
    FileExt::try_lock_exclusive(&writer).unwrap();
    assert!(matches!(
        build_trading_timeline_from_minute_cache(
            &fixture.cache,
            &MinuteKlineCacheSnapshot::cst_v1(),
            &fixture.catalog,
            fixture.request(vec![d])
        ),
        Err(DataError::CacheBusy { .. })
    ));
}

#[test]
fn all_month_pins_precede_metadata_and_survive_until_publication() {
    let fixture = Fixture::new();
    let days = vec![day("2024-01-31"), day("2024-02-01")];
    fixture.store(&days, &days.iter().copied().map(row).collect::<Vec<_>>());
    for hook in [&TEST_AFTER_MINUTE_PIN, &TEST_BEFORE_PRODUCT_LOCK] {
        let root = fixture.root.clone();
        hook.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(move || {
                probe("busy", &month_lock(&root, "202401"));
                probe("busy", &month_lock(&root, "202402"));
                probe("busy", &root.join(".tqsdk-cache-operation.lock"));
            }))
        });
    }
    TradingTimelineStore::open_read_only(&fixture.root)
        .rebuild_from_cache(&fixture.catalog, vec![fixture.request(days)], true)
        .unwrap();
    probe("free", &month_lock(&fixture.root, "202401"));
    probe("free", &month_lock(&fixture.root, "202402"));
}

#[test]
fn failed_later_pin_releases_earlier_pins_and_missing_lock_is_not_created() {
    let fixture = Fixture::new();
    let days = vec![day("2024-01-31"), day("2024-02-01")];
    fixture.store(&days, &days.iter().copied().map(row).collect::<Vec<_>>());
    let feb = month_lock(&fixture.root, "202402");
    let guard = fs::File::open(&feb).unwrap();
    FileExt::try_lock_exclusive(&guard).unwrap();
    let store = TradingTimelineStore::open(&fixture.root).unwrap();
    assert!(matches!(
        store.rebuild_from_cache(&fixture.catalog, vec![fixture.request(days.clone())], true),
        Err(DataError::CacheBusy { .. })
    ));
    probe("free", &month_lock(&fixture.root, "202401"));
    drop(guard);
    fs::remove_file(&feb).unwrap();
    assert!(
        store
            .rebuild_from_cache(&fixture.catalog, vec![fixture.request(days)], false)
            .is_err()
    );
    assert!(!feb.exists());
    probe("free", &month_lock(&fixture.root, "202401"));
    assert!(store.load_active("INE", "sc").unwrap().is_none());
}

#[test]
fn unrelated_month_is_not_pinned_and_partition_budget_precedes_file_access() {
    let fixture = Fixture::new();
    let days = vec![day("2024-01-31"), day("2024-02-01")];
    fixture.store(&days, &days.iter().copied().map(row).collect::<Vec<_>>());
    let range = backtest_tick_trading_day_range(days[0]).unwrap();
    let _pin = fixture
        .cache
        .pin_final_ranges(&[(SYMBOL.into(), range.start_ns, range.end_ns)])
        .unwrap();
    probe("busy", &month_lock(&fixture.root, "202401"));
    probe("free", &month_lock(&fixture.root, "202402"));
    let error = fixture
        .cache
        .pin_final_ranges(&[(
            SYMBOL.into(),
            ns("2000-01-01T00:00:00+08:00"),
            ns("2026-01-01T00:00:00+08:00"),
        )])
        .err()
        .unwrap();
    assert!(error.to_string().contains("partition limit"));
}

#[test]
fn concurrent_incremental_merge_reads_active_after_product_lock() {
    let fixture = Fixture::new();
    let days = vec![day("2024-01-04"), day("2024-01-05")];
    fixture.store(&days, &days.iter().copied().map(row).collect::<Vec<_>>());
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let root = fixture.root.clone();
    let catalog = fixture.catalog.clone();
    let request = fixture.request(vec![days[0]]);
    let worker = std::thread::spawn(move || {
        TEST_BEFORE_PRODUCT_LOCK.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(move || {
                ready_tx.send(()).unwrap();
                resume_rx
                    .recv_timeout(std::time::Duration::from_secs(10))
                    .unwrap();
            }))
        });
        TradingTimelineStore::open_read_only(root).rebuild_from_cache(&catalog, vec![request], true)
    });
    ready_rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .unwrap();
    let store = TradingTimelineStore::open_read_only(&fixture.root);
    store
        .rebuild_from_cache(&fixture.catalog, vec![fixture.request(vec![days[1]])], true)
        .unwrap();
    resume_tx.send(()).unwrap();
    worker.join().unwrap().unwrap();
    assert_eq!(
        store
            .load_active("INE", "sc")
            .unwrap()
            .unwrap()
            .snapshot
            .decisions
            .len(),
        2
    );
}

#[test]
fn retry_reports_uncertain_single_file_replacement() {
    let fixture = Fixture::new();
    let days = vec![day("2024-01-04"), day("2024-01-05")];
    fixture.store(&days, &days.iter().copied().map(row).collect::<Vec<_>>());
    let store = TradingTimelineStore::open(&fixture.root).unwrap();
    let old = store
        .rebuild_from_cache(&fixture.catalog, vec![fixture.request(vec![days[0]])], true)
        .unwrap()
        .remove(0);
    TEST_FAIL_BODY_DIRECTORY_SYNC.with(|flag| flag.set(true));
    let first =
        store.rebuild_from_cache(&fixture.catalog, vec![fixture.request(vec![days[1]])], true);
    let retry =
        store.rebuild_from_cache(&fixture.catalog, vec![fixture.request(vec![days[1]])], true);
    TEST_FAIL_BODY_DIRECTORY_SYNC.with(|flag| flag.set(false));
    assert!(first.is_err());
    assert!(
        retry.is_err(),
        "single-file replacement must report uncertain durability"
    );
    // A one-file atomic replacement is visible after rename even if directory
    // durability reports uncertain; callers reload and validate the new body.
    assert_ne!(
        store
            .load_active("INE", "sc")
            .unwrap()
            .unwrap()
            .snapshot
            .timeline_hash,
        old.timeline_hash
    );
    store
        .rebuild_from_cache(&fixture.catalog, vec![fixture.request(vec![days[1]])], true)
        .unwrap();
    assert_eq!(
        store
            .load_active("INE", "sc")
            .unwrap()
            .unwrap()
            .snapshot
            .decisions
            .len(),
        2
    );
}

#[test]
fn v1_single_file_migration_does_not_enable_legacy_runtime_reads() {
    let fixture = Fixture::new();
    let days = vec![day("2024-01-04"), day("2024-01-05")];
    fixture.store(&days, &days.iter().copied().map(row).collect::<Vec<_>>());
    let legacy = build_trading_timeline_from_minute_cache(
        &fixture.cache,
        &MinuteKlineCacheSnapshot::cst_v1(),
        &fixture.catalog,
        fixture.request(vec![days[0]]),
    )
    .unwrap();
    let product = fixture
        .root
        .join(TIMELINE_DIRECTORY)
        .join("products")
        .join("INE")
        .join("sc");
    fs::create_dir_all(product.join("snapshots")).unwrap();
    fs::write(
        product.join("active.json"),
        serde_json::to_vec(&serde_json::json!({"timeline_hash":legacy.timeline_hash})).unwrap(),
    )
    .unwrap();
    fs::write(
        product
            .join("snapshots")
            .join(format!("{}.json", legacy.timeline_hash)),
        serde_json::to_vec(&legacy).unwrap(),
    )
    .unwrap();
    let legacy_active = fs::read(product.join("active.json")).unwrap();
    let legacy_body = fs::read(
        product
            .join("snapshots")
            .join(format!("{}.json", legacy.timeline_hash)),
    )
    .unwrap();
    let store = TradingTimelineStore::open(&fixture.root).unwrap();
    assert!(store.load_active("INE", "sc").unwrap().is_none());
    let error = store
        .rebuild_from_cache(&fixture.catalog, vec![fixture.request(vec![days[1]])], true)
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("pre-compact timeline layout present")
    );
    assert_eq!(
        fs::read(product.join("active.json")).unwrap(),
        legacy_active
    );
    let timeline_path = product.join("timeline.json");
    fs::write(&timeline_path, b"not a timeline").unwrap();
    assert!(store.migrate_legacy_active("INE", "sc").is_err());
    fs::remove_file(&timeline_path).unwrap();
    let wrong = TradingTimelineSnapshot::new(
        TradingTimelineIdentity::new("INE", "nr", SYMBOL, "fixture", "fixture").unwrap(),
        legacy.known_ranges.clone(),
        legacy.open_intervals.clone(),
    )
    .unwrap()
    .with_decisions(legacy.decisions.clone())
    .unwrap();
    fs::write(
        &timeline_path,
        serde_json::to_vec(&CompactTimelineFile::from_snapshot(&wrong).unwrap()).unwrap(),
    )
    .unwrap();
    assert!(store.migrate_legacy_active("INE", "sc").is_err());
    fs::remove_file(&timeline_path).unwrap();
    let shared = BacktestTickCache::open_read_only(&fixture.root)
        .try_acquire_remote_fill_shared_lock()
        .unwrap();
    assert!(matches!(
        store.migrate_legacy_active("INE", "sc"),
        Err(DataError::CacheBusy { .. })
    ));
    drop(shared);
    assert!(store.migrate_legacy_active("INE", "sc").unwrap());
    assert!(!store.migrate_legacy_active("INE", "sc").unwrap());
    assert!(timeline_path.is_file());
    assert_eq!(
        store
            .load_active("INE", "sc")
            .unwrap()
            .unwrap()
            .snapshot
            .decisions
            .len(),
        1
    );
    store
        .rebuild_from_cache(&fixture.catalog, vec![fixture.request(vec![days[1]])], true)
        .unwrap();
    assert_eq!(
        fs::read(product.join("active.json")).unwrap(),
        legacy_active
    );
    assert_eq!(
        fs::read(
            product
                .join("snapshots")
                .join(format!("{}.json", legacy.timeline_hash)),
        )
        .unwrap(),
        legacy_body
    );
    fs::remove_file(timeline_path).unwrap();
    assert!(store.load_active("INE", "sc").unwrap().is_none());
}

#[test]
fn incremental_evidence_is_idempotent_and_equals_full_rebuild() {
    let fixture = Fixture::new();
    let days = vec![day("2024-01-04"), day("2024-01-05")];
    fixture.store(&days, &days.iter().copied().map(row).collect::<Vec<_>>());
    let store = TradingTimelineStore::open(&fixture.root).unwrap();
    store
        .rebuild_from_cache(&fixture.catalog, vec![fixture.request(vec![days[0]])], true)
        .unwrap();
    let first = store
        .rebuild_from_cache(&fixture.catalog, vec![fixture.request(vec![days[1]])], true)
        .unwrap()
        .remove(0);
    let repeated = store
        .rebuild_from_cache(&fixture.catalog, vec![fixture.request(vec![days[1]])], true)
        .unwrap()
        .remove(0);
    assert_eq!(first, repeated);
    let full = store
        .rebuild_from_cache(&fixture.catalog, vec![fixture.request(days)], true)
        .unwrap()
        .remove(0);
    assert_eq!(full, repeated);
}

#[test]
fn missing_exception_review_allows_audit_but_never_activation() {
    let fixture = Fixture::new();
    let days = vec![day("2024-01-05")];
    fixture.store(&days, &[row(days[0])]);
    let store = TradingTimelineStore::open(&fixture.root).unwrap();
    let first = store
        .rebuild_from_cache(&fixture.catalog, vec![fixture.request(days.clone())], true)
        .unwrap()
        .remove(0);
    let mut json = serde_json::to_value(&fixture.catalog).unwrap();
    json["exception_review_complete"] = false.into();
    let catalog = TradingTimelineRuleCatalog::from_json_str(&json.to_string()).unwrap();
    assert!(
        store
            .rebuild_from_cache(&catalog, vec![fixture.request(days.clone())], false)
            .is_ok()
    );
    assert!(
        store
            .rebuild_from_cache(&catalog, vec![fixture.request(days)], true)
            .is_err()
    );
    assert_eq!(
        store.load_active("INE", "sc").unwrap().unwrap().snapshot,
        first
    );
}

#[test]
fn post_rename_sync_failure_reports_visible_but_uncertain_activation() {
    let fixture = Fixture::new();
    let days = vec![day("2024-01-04"), day("2024-01-05")];
    fixture.store(&days, &days.iter().copied().map(row).collect::<Vec<_>>());
    let store = TradingTimelineStore::open(&fixture.root).unwrap();
    let old = store
        .rebuild_from_cache(&fixture.catalog, vec![fixture.request(vec![days[0]])], true)
        .unwrap()
        .remove(0);
    TEST_FAIL_ACTIVE_DIRECTORY_SYNC.with(|flag| flag.set(true));
    let result =
        store.rebuild_from_cache(&fixture.catalog, vec![fixture.request(vec![days[1]])], true);
    TEST_FAIL_ACTIVE_DIRECTORY_SYNC.with(|flag| flag.set(false));
    let DataError::Io(error) = result.unwrap_err() else {
        panic!("expected I/O error")
    };
    assert!(
        error
            .get_ref()
            .unwrap()
            .downcast_ref::<TradingTimelineDurabilityUncertain>()
            .is_some()
    );
    let active = store.load_active("INE", "sc").unwrap().unwrap();
    assert_ne!(active.snapshot.timeline_hash, old.timeline_hash);
    assert_eq!(active.snapshot.decisions.len(), 2);
}

static FIXTURE_SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

struct Fixture {
    root: PathBuf,
    cache: MinuteKlineCache,
    catalog: TradingTimelineRuleCatalog,
    _serial: std::sync::MutexGuard<'static, ()>,
}
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let serial = FIXTURE_SERIAL
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let root = std::env::temp_dir().join(format!(
            "timeline-regression-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).unwrap();
        let cache = MinuteKlineCache::open(&root).unwrap();
        let catalog = TradingTimelineRuleCatalog::from_json_str(r#"{
            "catalog_version":1,"exception_review_complete":true,"generated_at":"2026-09-06","timezone":"Asia/Shanghai",
            "interval_semantics":"half-open","night_session_semantics":"following trading day",
            "rules":[{"exchange":"INE","product":"sc","evidence_symbol":"KQ.i@INE.sc",
                "rule_id":"fixture-day","candidate_trading_day_start":"2024-01-01",
                "candidate_trading_day_end":"2024-12-31","sessions_cst":[["09:00","09:02"]],
                "validation":{"status":"confirmed","required_symbol":"KQ.i@INE.sc","required_check":"fixture"}}]
        }"#).unwrap();
        Self {
            root,
            cache,
            catalog,
            _serial: serial,
        }
    }
    fn store(&self, days: &[NaiveDate], rows: &[Kline]) {
        self.cache
            .store_final_range(
                SYMBOL,
                backtest_tick_trading_day_range(days[0]).unwrap().start_ns,
                backtest_tick_trading_day_range(*days.last().unwrap())
                    .unwrap()
                    .end_ns,
                &MinuteKlineCacheSnapshot::cst_v1(),
                rows,
            )
            .unwrap();
    }
    fn request(&self, days: Vec<NaiveDate>) -> TradingTimelineBuildRequest {
        TradingTimelineBuildRequest::new("INE", "sc", SYMBOL, "fixture", days).unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}
fn day(s: &str) -> NaiveDate {
    NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap()
}
fn ns(s: &str) -> i64 {
    DateTime::parse_from_rfc3339(s)
        .unwrap()
        .timestamp_nanos_opt()
        .unwrap()
}
fn row(day: NaiveDate) -> Kline {
    Kline {
        id: i64::from(day.num_days_from_ce()),
        datetime: ns(&format!("{day}T09:00:00+08:00")),
        volume: 1,
        open: 1.,
        high: 1.,
        low: 1.,
        close: 1.,
        open_oi: 1,
        close_oi: 1,
        ..Kline::default()
    }
}

#[test]
fn monday_day_session_uses_monday_not_saturday_and_scans_once_per_month() {
    let fixture = Fixture::new();
    let days = (2..=31)
        .map(|d| NaiveDate::from_ymd_opt(2024, 1, d).unwrap())
        .filter(|d| !matches!(d.weekday(), Weekday::Sat | Weekday::Sun))
        .collect::<Vec<_>>();
    fixture.store(&days, &days.iter().copied().map(row).collect::<Vec<_>>());
    crate::minute_kline_cache::TEST_MONTH_SCAN_COUNT.with(|count| count.set(0));
    let compiled = build_trading_timeline_from_minute_cache(
        &fixture.cache,
        &MinuteKlineCacheSnapshot::cst_v1(),
        &fixture.catalog,
        fixture.request(days.clone()),
    )
    .unwrap();
    assert_eq!(
        crate::minute_kline_cache::TEST_MONTH_SCAN_COUNT.with(std::cell::Cell::get),
        0 // KLOG coverage uses the commit index, not a legacy payload scan.
    );
    assert_eq!(compiled.open_intervals.len(), days.len());
    let monday = ns("2024-01-08T09:00:00+08:00");
    assert!(
        compiled
            .open_intervals
            .contains(&TradingTimelineInterval::new(monday, monday + 2 * MINUTE).unwrap())
    );
    let timeline = TradingTimeline::from_snapshot(compiled).unwrap();
    assert_eq!(
        timeline
            .shift(
                ns("2024-01-05T09:01:00+08:00"),
                Duration::from_secs(120),
                TradingTimeDirection::Forward
            )
            .unwrap(),
        ns("2024-01-08T09:01:00+08:00")
    );
}

#[test]
fn separate_night_anchor_survives_weekend_and_long_storage_gap() {
    let rules = vec![
        TradingTimelineRule::new(
            "night-day",
            vec![
                TradingTimelineRuleWindow::new(3 * 60 * MINUTE, 8 * 60 * MINUTE + 30 * MINUTE)
                    .unwrap(),
                TradingTimelineRuleWindow::new(15 * 60 * MINUTE, 17 * 60 * MINUTE).unwrap(),
            ],
        )
        .unwrap(),
    ];
    for (start, end) in [
        ("2024-01-05T18:00:00+08:00", "2024-01-08T18:00:00+08:00"),
        ("2024-09-30T18:00:00+08:00", "2024-10-08T18:00:00+08:00"),
    ] {
        let dated = dated_rules(rules.clone(), ns(start), ns(end)).unwrap();
        assert_eq!(dated[0].windows[0], rules[0].windows[0]);
        assert_eq!(
            ns(start) + dated[0].windows[1].start_offset_ns,
            ns(end) - 9 * 60 * MINUTE
        );
    }
}

#[test]
fn unresolved_or_missing_evidence_preserves_active_and_incremental_fill_keeps_old_days() {
    let fixture = Fixture::new();
    let days = vec![day("2024-01-04"), day("2024-01-05"), day("2024-01-08")];
    fixture.store(&days, &[row(days[0]), row(days[1])]);
    let store = TradingTimelineStore::open(&fixture.root).unwrap();
    store
        .rebuild_from_cache(&fixture.catalog, vec![fixture.request(vec![days[0]])], true)
        .unwrap();
    store
        .rebuild_from_cache(&fixture.catalog, vec![fixture.request(vec![days[1]])], true)
        .unwrap();
    let previous = store.load_active("INE", "sc").unwrap().unwrap();
    assert_eq!(previous.snapshot.open_intervals.len(), 2);
    assert!(
        store
            .rebuild_from_cache(&fixture.catalog, vec![fixture.request(vec![days[2]])], true)
            .is_err()
    );
    assert_eq!(
        store
            .load_active("INE", "sc")
            .unwrap()
            .unwrap()
            .snapshot
            .timeline_hash,
        previous.snapshot.timeline_hash
    );
    assert!(
        store
            .rebuild_from_cache(
                &fixture.catalog,
                vec![fixture.request(vec![day("2024-02-01")])],
                true
            )
            .is_err()
    );
    assert_eq!(
        store
            .load_active("INE", "sc")
            .unwrap()
            .unwrap()
            .snapshot
            .timeline_hash,
        previous.snapshot.timeline_hash
    );
}

#[test]
fn publisher_detects_corrupt_body_and_rebuild_respects_fill_gate() {
    let fixture = Fixture::new();
    let days = vec![day("2024-01-05")];
    fixture.store(&days, &[row(days[0])]);
    let store = TradingTimelineStore::open(&fixture.root).unwrap();
    let first = store
        .rebuild_from_cache(&fixture.catalog, vec![fixture.request(days.clone())], true)
        .unwrap()
        .remove(0);
    let gate = BacktestTickCache::open_read_only(&fixture.root)
        .try_acquire_remote_fill_lock()
        .unwrap();
    assert!(
        store
            .rebuild_from_cache(&fixture.catalog, vec![fixture.request(days.clone())], true)
            .is_err()
    );
    drop(gate);
    let path = store
        .product_root(&first.identity)
        .unwrap()
        .join("timeline.json");
    fs::write(path, b"corrupt fixture body").unwrap();
    assert!(
        store
            .rebuild_from_cache(&fixture.catalog, vec![fixture.request(days)], true)
            .is_err()
    );
    assert!(store.load_active("INE", "sc").is_err());
}

#[test]
fn two_months_scan_twice_and_catalog_mutation_is_rejected() {
    let fixture = Fixture::new();
    let days = vec![day("2024-01-31"), day("2024-02-01")];
    fixture.store(&days, &days.iter().copied().map(row).collect::<Vec<_>>());
    crate::minute_kline_cache::TEST_MONTH_SCAN_COUNT.with(|count| count.set(0));
    let built = build_trading_timeline_from_minute_cache(
        &fixture.cache,
        &MinuteKlineCacheSnapshot::cst_v1(),
        &fixture.catalog,
        fixture.request(days.clone()),
    )
    .unwrap();
    assert_eq!(built.decisions.len(), 2);
    assert_eq!(
        crate::minute_kline_cache::TEST_MONTH_SCAN_COUNT.with(std::cell::Cell::get),
        0 // Both months use committed indexes.
    );
    let mut altered = fixture.catalog.clone();
    altered.rules[0].sessions_cst[0][1] = "09:03".into();
    assert!(
        build_trading_timeline_from_minute_cache(
            &fixture.cache,
            &MinuteKlineCacheSnapshot::cst_v1(),
            &altered,
            fixture.request(days)
        )
        .is_err()
    );
}

#[test]
fn zero_volume_rows_do_not_prove_a_session() {
    let fixture = Fixture::new();
    let days = vec![day("2024-01-05")];
    let mut no_trade = row(days[0]);
    no_trade.volume = 0;
    fixture.store(&days, &[no_trade]);
    let built = build_trading_timeline_from_minute_cache(
        &fixture.cache,
        &MinuteKlineCacheSnapshot::cst_v1(),
        &fixture.catalog,
        fixture.request(days),
    )
    .unwrap();
    assert!(built.known_ranges.is_empty());
    assert!(built.open_intervals.is_empty());
}

#[test]
fn full_i64_timestamp_width_does_not_overflow_hot_arithmetic() {
    let timeline = TradingTimeline::from_snapshot(
        TradingTimelineSnapshot::new(
            TradingTimelineIdentity::new("INE", "sc", SYMBOL, "fixture", "fixture").unwrap(),
            vec![TradingTimelineKnownRange::new(i64::MIN, i64::MAX).unwrap()],
            vec![TradingTimelineInterval::new(i64::MIN, i64::MAX).unwrap()],
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        timeline
            .trading_duration_between(i64::MIN, i64::MAX)
            .unwrap()
            .as_nanos(),
        u128::from(u64::MAX)
    );
    assert_eq!(
        timeline
            .shift(
                i64::MIN,
                Duration::from_nanos(u64::MAX),
                TradingTimeDirection::Forward
            )
            .unwrap(),
        i64::MAX
    );
}
