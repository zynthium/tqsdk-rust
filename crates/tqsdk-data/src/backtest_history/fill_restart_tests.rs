// Included in fill::tests so these tests exercise the real coordinator seam.
#[derive(Clone, Copy)]
enum RestartStep {
    Complete,
    Empty,
    Fail,
    Retry,
    WrongChart,
    MissingChartTerminal,
    Changed,
    CloseFailure,
}
type RestartRanges = Arc<std::sync::Mutex<Vec<(i64, i64)>>>;

struct RestartFactory {
    steps: std::sync::Mutex<VecDeque<RestartStep>>,
    ranges: Arc<std::sync::Mutex<Vec<(i64, i64)>>>,
    stop_on_open: Option<Arc<AtomicBool>>,
}

impl ServerHistorySourceFactory for RestartFactory {
    fn open<'a>(
        &'a self,
        _: BacktestHistoryCredentials,
        request: ServerBacktestHistoryRequest,
    ) -> OpenServerHistorySourceFuture<'a> {
        Box::pin(async move {
            self.ranges
                .lock()
                .unwrap()
                .push((request.start_ns, request.end_ns));
            if let Some(stop) = &self.stop_on_open {
                stop.store(true, Ordering::Release);
            }
            let step = self
                .steps
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(RestartStep::Complete);
            let chart = &request.charts[0];
            let mut events = VecDeque::new();
            if !matches!(step, RestartStep::Empty) {
                let datetime = request.start_ns + 60_000_000_000;
                let mut row = kline(datetime / 60_000_000_000, datetime);
                if matches!(step, RestartStep::Changed) {
                    row.close += 1.0;
                }
                let rows = vec![row];
                events.push_back(match chart.kind {
                    ServerBacktestHistoryKind::CanonicalDaily => {
                        ServerBacktestHistoryEvent::CanonicalDaily {
                            chart_id: chart.chart_id.clone(),
                            symbol: chart.symbol.clone(),
                            rows,
                        }
                    }
                    _ => ServerBacktestHistoryEvent::CanonicalMinutes {
                        chart_id: chart.chart_id.clone(),
                        symbol: chart.symbol.clone(),
                        rows,
                    },
                });
            }
            if matches!(step, RestartStep::WrongChart)
                && let Some(ServerBacktestHistoryEvent::CanonicalMinutes { chart_id, .. }) =
                    events.front_mut()
            {
                *chart_id = "foreign-attempt".to_string();
            }
            if matches!(step, RestartStep::MissingChartTerminal) {
                events.push_back(ServerBacktestHistoryEvent::StreamCompleted);
            }
            if matches!(
                step,
                RestartStep::Complete
                    | RestartStep::Empty
                    | RestartStep::Changed
                    | RestartStep::CloseFailure
                    | RestartStep::WrongChart
            ) {
                events.push_back(ServerBacktestHistoryEvent::ChartCompleted {
                    chart_id: chart.chart_id.clone(),
                    symbol: chart.symbol.clone(),
                });
                events.push_back(ServerBacktestHistoryEvent::StreamCompleted);
            }
            Ok(Box::new(RestartSource {
                fail_close: matches!(step, RestartStep::CloseFailure),
                events,
                failure: match step {
                    RestartStep::Fail => Some("intentional interruption"),
                    RestartStep::Retry => Some("temporary transport failure"),
                    _ => None,
                },
            }) as Box<dyn ServerHistorySource>)
        })
    }
}

struct RestartSource {
    fail_close: bool,
    events: VecDeque<ServerBacktestHistoryEvent>,
    failure: Option<&'static str>,
}
impl ServerHistorySource for RestartSource {
    fn close<'a>(&'a mut self, _: bool) -> CloseServerHistorySourceFuture<'a> {
        Box::pin(async move {
            if self.fail_close {
                Err(DataError::InvalidResponse(
                    "intentional close failure".into(),
                ))
            } else {
                Ok(())
            }
        })
    }
    fn next_event<'a>(&'a mut self) -> ServerHistorySourceFuture<'a> {
        Box::pin(async move {
            if let Some(event) = self.events.pop_front() {
                return Ok(Some(event));
            }
            if let Some(error) = self.failure.take() {
                return Err(DataError::InvalidResponse(error.into()));
            }
            Ok(None)
        })
    }
}

fn restart_coordinator(
    root: PathBuf,
    steps: Vec<RestartStep>,
) -> (RemoteFillCoordinator, RestartRanges) {
    let ranges = Arc::new(std::sync::Mutex::new(Vec::new()));
    let factory = RestartFactory {
        steps: std::sync::Mutex::new(steps.into()),
        ranges: Arc::clone(&ranges),
        stop_on_open: None,
    };
    (
        coordinator(
            root,
            Arc::new(factory),
            Arc::new(CountingAuth::new(Arc::new(AtomicUsize::new(0)))),
        ),
        ranges,
    )
}

#[tokio::test]
async fn minute_restart_reuses_terminal_prefix_but_not_tentative_rows() {
    use super::super::fill_staging::CHECKPOINT_SPAN_NS;
    let root = temporary_root("minute-restart-staged-prefix");
    let end = closed_range().0;
    let start = end - 4 * CHECKPOINT_SPAN_NS;
    let snapshot = MinuteKlineCacheSnapshot::cst_v1();
    let request = BacktestHistoryFillRequest::canonical_minute(
        "SHFE.au2608",
        (start, end),
        snapshot.clone(),
        None,
        Some(1),
        "SHFE.au2608",
    );
    let (first, _) = restart_coordinator(
        root.clone(),
        vec![
            RestartStep::Complete,
            RestartStep::Complete,
            RestartStep::Fail,
        ],
    );
    assert!(first.ensure_coverage(request.clone()).await.is_err());
    assert!(
        !MinuteKlineCache::open_read_only(&root)
            .coverage("SHFE.au2608", start, end, &snapshot)
            .unwrap()
            .missing_ranges
            .is_empty()
    );
    let (second, ranges) = restart_coordinator(root.clone(), vec![]);
    assert_eq!(
        second
            .ensure_coverage(request.clone())
            .await
            .unwrap()
            .rows_written,
        4
    );
    let requested = ranges.lock().unwrap().clone();
    assert_eq!(requested.len(), 3);
    assert_eq!(requested[0].0, start + CHECKPOINT_SPAN_NS);
    assert!(
        MinuteKlineCache::open_read_only(&root)
            .coverage("SHFE.au2608", start, end, &snapshot)
            .unwrap()
            .missing_ranges
            .is_empty()
    );
    assert!(!second.ensure_coverage(request).await.unwrap().remote_used);
    assert_eq!(ranges.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn failed_attempt_rows_never_leak_into_successful_empty_retry() {
    for daily in [false, true] {
        let root = temporary_root(if daily {
            "daily-retry-empty"
        } else {
            "minute-retry-empty"
        });
        let closed = closed_range();
        let range = (closed.0, closed.0 + 86_400_000_000_000);
        let snapshot = MinuteKlineCacheSnapshot::cst_v1();
        let request = if daily {
            BacktestHistoryFillRequest::canonical_daily(
                "SHFE.au2608",
                range,
                snapshot,
                Some(1),
                "SHFE.au2608",
            )
        } else {
            BacktestHistoryFillRequest::canonical_minute(
                "SHFE.au2608",
                range,
                snapshot,
                None,
                Some(1),
                "SHFE.au2608",
            )
        };
        let (coordinator, ranges) =
            restart_coordinator(root, vec![RestartStep::Retry, RestartStep::Empty]);
        assert_eq!(
            coordinator
                .ensure_coverage(request)
                .await
                .unwrap()
                .rows_written,
            0
        );
        assert_eq!(ranges.lock().unwrap().len(), 2);
    }
}

#[tokio::test]
async fn daily_restart_skips_already_committed_32_day_slice() {
    let root = temporary_root("daily-restart-slices");
    let end = closed_range().0;
    let start = end - 2 * DAILY_FILL_MAX_SPAN_NS;
    let request = BacktestHistoryFillRequest::canonical_daily(
        "SHFE.au2608",
        (start, end),
        MinuteKlineCacheSnapshot::cst_v1(),
        Some(1),
        "SHFE.au2608",
    );
    let (first, _) =
        restart_coordinator(root.clone(), vec![RestartStep::Complete, RestartStep::Fail]);
    assert!(first.ensure_coverage(request.clone()).await.is_err());
    let (second, ranges) = restart_coordinator(root, vec![]);
    assert_eq!(
        second.ensure_coverage(request).await.unwrap().rows_written,
        1
    );
    assert_eq!(
        *ranges.lock().unwrap(),
        vec![(start + DAILY_FILL_MAX_SPAN_NS, end)]
    );
}

#[tokio::test]
async fn preexisting_graceful_stop_does_not_open_a_source() {
    let (coordinator, ranges) = restart_coordinator(temporary_root("preexisting-grace"), vec![]);
    let request = BacktestHistoryFillRequest::canonical_daily(
        "SHFE.au2608",
        closed_range(),
        MinuteKlineCacheSnapshot::cst_v1(),
        Some(1),
        "SHFE.au2608",
    );
    assert!(
        coordinator
            .ensure_coverage_until_cancelled(
                request,
                &AtomicBool::new(false),
                &Arc::new(AtomicBool::new(true)),
                true
            )
            .await
            .is_err()
    );
    assert!(ranges.lock().unwrap().is_empty());
}

#[tokio::test]
async fn graceful_stop_commits_current_daily_slice_without_opening_next() {
    let root = temporary_root("daily-grace-current-window");
    let end = closed_range().0;
    let start = end - 2 * DAILY_FILL_MAX_SPAN_NS;
    let stop = Arc::new(AtomicBool::new(false));
    let ranges = Arc::new(std::sync::Mutex::new(Vec::new()));
    let coordinator = coordinator(
        root.clone(),
        Arc::new(RestartFactory {
            steps: std::sync::Mutex::new(VecDeque::new()),
            ranges: Arc::clone(&ranges),
            stop_on_open: Some(Arc::clone(&stop)),
        }),
        Arc::new(CountingAuth::new(Arc::new(AtomicUsize::new(0)))),
    );
    let snapshot = MinuteKlineCacheSnapshot::cst_v1();
    let request = BacktestHistoryFillRequest::canonical_daily(
        "SHFE.au2608",
        (start, end),
        snapshot.clone(),
        Some(1),
        "SHFE.au2608",
    );
    assert!(
        coordinator
            .ensure_coverage_until_cancelled(request, &AtomicBool::new(false), &stop, true)
            .await
            .is_err()
    );
    assert_eq!(
        *ranges.lock().unwrap(),
        vec![(start, start + DAILY_FILL_MAX_SPAN_NS)]
    );
    let coverage = DailyKlineCache::open_read_only(&root)
        .coverage("SHFE.au2608", start, end, &snapshot)
        .unwrap();
    assert_eq!(
        coverage.missing_ranges,
        vec![(start + DAILY_FILL_MAX_SPAN_NS, end)]
    );
}

#[test]
fn graceful_stop_does_not_stop_another_shared_consumer() {
    let request = BacktestHistoryFillRequest::canonical_daily(
        "SHFE.au2608",
        closed_range(),
        MinuteKlineCacheSnapshot::cst_v1(),
        Some(1),
        "SHFE.au2608",
    );
    let shared = Arc::new(SharedFill::new(request.range, request.compatibility()));
    let signal = Arc::new(AtomicBool::new(true));
    let first = FillSubscription::new(Arc::clone(&shared), Some(&signal));
    let second = FillSubscription::new(Arc::clone(&shared), None);
    assert!(!shared.is_draining());
    drop(second);
    assert!(shared.is_draining());
    drop(first);
    assert!(shared.is_cancelled());
}

#[test]
fn staged_payload_preserves_float_bits_and_rejects_corruption() {
    use super::super::fill_staging::MinuteFillJournal;
    let root = temporary_root("journal-bits");
    let range = closed_range();
    let request = BacktestHistoryFillRequest::canonical_minute(
        "SHFE.au2608",
        range,
        MinuteKlineCacheSnapshot::cst_v1(),
        None,
        Some(1),
        "SHFE.au2608",
    );
    let mut journal = MinuteFillJournal::open(&root, &request).unwrap();
    let mut row = kline(1, range.0 + 60_000_000_000);
    row.open = f64::from_bits(0x7ff8_0000_0000_0042);
    row.close = -0.0;
    journal.rows.insert(row.datetime, row.clone());
    journal.save().unwrap();
    let recovered = MinuteFillJournal::open(&root, &request).unwrap();
    assert_eq!(
        recovered.rows[&row.datetime].open.to_bits(),
        row.open.to_bits()
    );
    assert_eq!(
        recovered.rows[&row.datetime].close.to_bits(),
        row.close.to_bits()
    );
    let path = std::fs::read_dir(root.join(".backtest-history-staging/minute-v1"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let mut bytes = std::fs::read(&path).unwrap();
    let at = bytes.iter().position(|byte| *byte == b'1').unwrap();
    bytes[at] = b'2';
    std::fs::write(&path, bytes).unwrap();
    assert!(MinuteFillJournal::open(&root, &request).is_err());
}

#[tokio::test]
async fn minute_rejects_wrong_chart_missing_terminal_and_failed_close() {
    for step in [
        RestartStep::WrongChart,
        RestartStep::MissingChartTerminal,
        RestartStep::CloseFailure,
    ] {
        let root = temporary_root("minute-invalid-terminal");
        let start = closed_range().0;
        let range = (start, start + 86_400_000_000_000);
        let snapshot = MinuteKlineCacheSnapshot::cst_v1();
        let request = BacktestHistoryFillRequest::canonical_minute(
            "SHFE.au2608",
            range,
            snapshot.clone(),
            None,
            Some(1),
            "SHFE.au2608",
        );
        let (coordinator, _) = restart_coordinator(root.clone(), vec![step]);
        assert!(coordinator.ensure_coverage(request).await.is_err());
        assert_eq!(
            MinuteKlineCache::open_read_only(root)
                .coverage("SHFE.au2608", range.0, range.1, &snapshot)
                .unwrap()
                .missing_ranges,
            vec![range]
        );
    }
}

#[tokio::test]
async fn changed_restart_overlap_discards_only_journal_and_never_commits() {
    let root = temporary_root("minute-changed-overlap");
    let end = closed_range().0;
    let range = (end - 2 * 86_400_000_000_000, end);
    let snapshot = MinuteKlineCacheSnapshot::cst_v1();
    let request = BacktestHistoryFillRequest::canonical_minute(
        "SHFE.au2608",
        range,
        snapshot.clone(),
        None,
        Some(1),
        "SHFE.au2608",
    );
    let (first, _) =
        restart_coordinator(root.clone(), vec![RestartStep::Complete, RestartStep::Fail]);
    assert!(first.ensure_coverage(request.clone()).await.is_err());
    let (second, _) = restart_coordinator(root.clone(), vec![RestartStep::Changed]);
    assert!(
        second
            .ensure_coverage(request.clone())
            .await
            .unwrap_err()
            .to_string()
            .contains("overlap changed")
    );
    assert_eq!(
        MinuteKlineCache::open_read_only(&root)
            .coverage("SHFE.au2608", range.0, range.1, &snapshot)
            .unwrap()
            .missing_ranges,
        vec![range]
    );
    let (third, ranges) = restart_coordinator(root, vec![]);
    third.ensure_coverage(request).await.unwrap();
    assert_eq!(ranges.lock().unwrap()[0].0, range.0);
}

#[tokio::test]
async fn minute_grace_stops_before_next_checkpoint_request() {
    let root = temporary_root("minute-grace-current-window");
    let end = closed_range().0;
    let start = end - 2 * 86_400_000_000_000;
    let stop = Arc::new(AtomicBool::new(false));
    let ranges = Arc::new(std::sync::Mutex::new(Vec::new()));
    let coordinator = coordinator(
        root.clone(),
        Arc::new(RestartFactory {
            steps: std::sync::Mutex::new(VecDeque::new()),
            ranges: Arc::clone(&ranges),
            stop_on_open: Some(Arc::clone(&stop)),
        }),
        Arc::new(CountingAuth::new(Arc::new(AtomicUsize::new(0)))),
    );
    let request = BacktestHistoryFillRequest::canonical_minute(
        "SHFE.au2608",
        (start, end),
        MinuteKlineCacheSnapshot::cst_v1(),
        None,
        Some(1),
        "SHFE.au2608",
    );
    assert!(
        coordinator
            .ensure_coverage_until_cancelled(request.clone(), &AtomicBool::new(false), &stop, true)
            .await
            .is_err()
    );
    assert_eq!(ranges.lock().unwrap().len(), 1);
    let journal = super::super::fill_staging::MinuteFillJournal::open(&root, &request).unwrap();
    assert_eq!(journal.confirmed_end_ns, start + 86_400_000_000_000);
}

#[tokio::test]
async fn stop_only_at_last_commit_preserves_success_and_reports_durability() {
    for daily in [false, true] {
        let root = temporary_root("last-window-stop-success");
        let start = closed_range().0;
        let range = (start, start + 86_400_000_000_000);
        let snapshot = MinuteKlineCacheSnapshot::cst_v1();
        let request = if daily {
            BacktestHistoryFillRequest::canonical_daily(
                "SHFE.au2608",
                range,
                snapshot,
                Some(1),
                "SHFE.au2608",
            )
        } else {
            BacktestHistoryFillRequest::canonical_minute(
                "SHFE.au2608",
                range,
                snapshot,
                None,
                Some(1),
                "SHFE.au2608",
            )
        };
        let (mut coordinator, _) = restart_coordinator(root, vec![]);
        let stop = Arc::new(AtomicBool::new(false));
        let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
        Arc::make_mut(&mut coordinator.config).fill_durability = Some(Arc::new({
            let stop = Arc::clone(&stop);
            let observed = Arc::clone(&observed);
            move |event| {
                if event.progress.final_coverage {
                    stop.store(true, Ordering::Release);
                }
                observed.lock().unwrap().push(event.progress);
            }
        }));
        assert_eq!(
            coordinator
                .ensure_coverage_until_cancelled(request, &AtomicBool::new(false), &stop, true)
                .await
                .unwrap()
                .rows_written,
            1
        );
        assert!(stop.load(Ordering::Acquire));
        let observed = observed.lock().unwrap();
        assert_eq!(observed.last().unwrap().committed_rows, 1);
        assert!(observed.last().unwrap().redownload_range.is_none());
    }
}
