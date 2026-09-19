//! Job-scoped multiplexing. A chart owns a series; the connection owns admission.
use super::*;

type Series = (String, ServerBacktestHistoryKind);

pub(super) struct JobHistorySession {
    credentials: ServerHistorySessionCredentials,
    runtime: tokio::runtime::Id,
    session: tqsdk_session::SessionClient,
    admission: Arc<admission::FillAdmission>,
    established: tokio::sync::Mutex<bool>,
    state: Mutex<JobState>,
    lease: Option<ServerHistorySessionLease>,
}

struct JobState {
    active: Vec<Series>,
    reusable: bool,
    closing: usize,
}

/// A close future owns this guard across cleanup. Dropping an unfinished close
/// poisons the connection before it can admit a new reader.
pub(super) struct JobCloseGuard {
    job: Option<Arc<JobHistorySession>>,
}

impl JobCloseGuard {
    pub(super) fn finish(mut self, clean: bool) {
        if let Some(job) = self.job.take() {
            let mut state = job.state.lock().unwrap_or_else(|p| p.into_inner());
            state.reusable &= clean;
            state.closing -= 1;
        }
    }
}

impl Drop for JobCloseGuard {
    fn drop(&mut self) {
        if let Some(job) = self.job.take() {
            let mut state = job.state.lock().unwrap_or_else(|p| p.into_inner());
            state.reusable = false;
            state.closing -= 1;
        }
    }
}

impl Drop for JobHistorySession {
    fn drop(&mut self) {
        if let Some(mut lease) = self.lease.take()
            && self
                .state
                .get_mut()
                .unwrap_or_else(|p| p.into_inner())
                .reusable
            && server_history_state_fits(&self.session)
        {
            if let Some(entry) = &mut lease.entry {
                entry.established = *self.established.get_mut();
            }
            lease.recycle();
        }
    }
}

pub(super) struct JobChartLease {
    job: Arc<JobHistorySession>,
    series: Series,
    clean: bool,
}

impl Drop for JobChartLease {
    fn drop(&mut self) {
        let mut state = self.job.state.lock().unwrap_or_else(|p| p.into_inner());
        if !self.clean {
            state.reusable = false;
        }
        state.active.retain(|series| series != &self.series);
    }
}

pub(super) enum HistorySourceLease {
    Exclusive(Box<ServerHistorySessionLease>),
    Shared(JobChartLease),
}

impl From<ServerHistorySessionLease> for HistorySourceLease {
    fn from(lease: ServerHistorySessionLease) -> Self {
        Self::Exclusive(Box::new(lease))
    }
}

impl HistorySourceLease {
    pub(super) fn begin_close(&self, reusable: bool) -> JobCloseGuard {
        let job = if let Self::Shared(lease) = self {
            let mut state = lease.job.state.lock().unwrap_or_else(|p| p.into_inner());
            state.closing += 1;
            state.reusable &= reusable;
            Some(Arc::clone(&lease.job))
        } else {
            None
        };
        JobCloseGuard { job }
    }
    pub(super) fn session(&self) -> &tqsdk_session::SessionClient {
        match self {
            Self::Exclusive(lease) => lease.session(),
            Self::Shared(lease) => &lease.job.session,
        }
    }

    pub(super) fn admission(&self) -> &Arc<admission::FillAdmission> {
        match self {
            Self::Exclusive(lease) => &lease.admission,
            Self::Shared(lease) => &lease.job.admission,
        }
    }

    pub(super) async fn establish(&mut self) -> Result<()> {
        match self {
            Self::Exclusive(lease) => {
                let established = lease.entry.as_ref().is_some_and(|entry| entry.established);
                lease.admission.check()?;
                let _guard = if established {
                    None
                } else {
                    Some(lease.admission.enter().await?)
                };
                let result = lease
                    .session()
                    .ensure_established()
                    .await
                    .map_err(DataError::from);
                lease.admission.observe(&result);
                result?;
                if let Some(entry) = &mut lease.entry {
                    entry.established = true;
                }
            }
            Self::Shared(lease) => {
                // Only bootstrap is serialized. Independent chart readers never
                // hold this lock while awaiting data or durable cache writes.
                let mut established = lease.job.established.lock().await;
                lease.job.admission.check()?;
                if !*established {
                    let _guard = lease.job.admission.enter().await?;
                    let result = lease
                        .job
                        .session
                        .ensure_established()
                        .await
                        .map_err(DataError::from);
                    lease.job.admission.observe(&result);
                    result?;
                    *established = true;
                }
            }
        }
        Ok(())
    }

    pub(super) fn mark_pruned(&self) {
        if let Self::Shared(lease) = self {
            lease
                .job
                .state
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .reusable = false;
        }
    }

    pub(super) fn finish(self, reusable: bool) {
        match self {
            Self::Exclusive(lease) if reusable => lease.recycle(),
            Self::Shared(mut lease) => {
                lease.clean = reusable;
            }
            _ => {}
        }
    }
}

impl SessionServerHistorySourceFactory {
    pub(super) async fn acquire_chart(
        &self,
        credentials: BacktestHistoryCredentials,
        chart: &ServerBacktestHistoryChart,
    ) -> Result<HistorySourceLease> {
        let (user, pass) = credentials.into_parts();
        let builder = tqsdk_session::SessionClientBuilder::new(user.clone(), pass.clone());
        let key = ServerHistorySessionCredentials {
            user: user.clone(),
            pass: pass.clone(),
            auth_url: builder.endpoints().auth_url.clone(),
            market_url: builder.endpoints().market_url.clone(),
        };
        let runtime = tokio::runtime::Handle::current().id();
        let series = (chart.symbol.clone(), chart.kind);
        let mut jobs = self.jobs.lock().await;
        jobs.retain(|job| job.strong_count() > 0);
        for job in jobs.iter().filter_map(Weak::upgrade) {
            if job.credentials != key || job.runtime != runtime {
                continue;
            }
            job.admission.check()?;
            let mut state = job.state.lock().unwrap_or_else(|p| p.into_inner());
            if !state.reusable || state.closing != 0 || state.active.contains(&series) {
                continue;
            }
            state.active.push(series.clone());
            drop(state);
            return Ok(HistorySourceLease::Shared(JobChartLease {
                job,
                series,
                clean: false,
            }));
        }
        // acquire is try-only for the global connection permit: never hold a
        // series lease while sleeping for another connection's release.
        let lease = self
            .pool
            .acquire(BacktestHistoryCredentials::new(user, pass))
            .await?;
        let established = lease.entry.as_ref().is_some_and(|entry| entry.established);
        let job = Arc::new(JobHistorySession {
            credentials: key,
            runtime,
            session: lease.session().clone(),
            admission: Arc::clone(&lease.admission),
            established: tokio::sync::Mutex::new(established),
            state: Mutex::new(JobState {
                active: vec![series.clone()],
                reusable: true,
                closing: 0,
            }),
            lease: Some(lease),
        });
        jobs.push(Arc::downgrade(&job));
        Ok(HistorySourceLease::Shared(JobChartLease {
            job,
            series,
            clean: false,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chart(symbol: &str, kind: ServerBacktestHistoryKind) -> ServerBacktestHistoryChart {
        ServerBacktestHistoryChart {
            chart_id: format!("chart-{symbol}-{kind:?}"),
            symbol: symbol.into(),
            kind,
        }
    }

    fn credentials() -> BacktestHistoryCredentials {
        BacktestHistoryCredentials::new("test-only", "test-only")
    }

    #[tokio::test(flavor = "current_thread")]
    async fn attach_rechecks_taint_inside_the_series_critical_section() {
        let factory = Arc::new(SessionServerHistorySourceFactory::new(1));
        let owner = factory
            .acquire_chart(
                credentials(),
                &chart("SHFE.au2602", ServerBacktestHistoryKind::Tick),
            )
            .await
            .unwrap();
        let HistorySourceLease::Shared(owner_ref) = &owner else {
            unreachable!()
        };
        let job = Arc::clone(&owner_ref.job);
        let mut state = job.state.lock().unwrap();
        let runtime = tokio::runtime::Handle::current();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let child_barrier = Arc::clone(&barrier);
        let child_factory = Arc::clone(&factory);
        let waiter = std::thread::spawn(move || {
            let _entered = runtime.enter();
            child_barrier.wait();
            runtime.block_on(child_factory.acquire_chart(
                credentials(),
                &chart("SHFE.au2604", ServerBacktestHistoryKind::Tick),
            ))
        });
        barrier.wait();
        // Wait until attach holds the factory lock and is stopped at job.state.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while factory.jobs.try_lock().is_ok() {
            assert!(
                std::time::Instant::now() < deadline,
                "attach did not enter factory"
            );
            std::thread::yield_now();
        }
        state.reusable = false;
        drop(state);
        assert!(matches!(
            waiter.join().unwrap(),
            Err(DataError::CacheBusy { .. })
        ));
        assert_eq!(job.state.lock().unwrap().active.len(), 1);
        owner.finish(true);
        drop(job);
        assert_eq!(factory.idle_session_count(), 0);
    }

    #[tokio::test]
    async fn independent_serials_share_one_job_connection_but_not_a_series() {
        let factory = SessionServerHistorySourceFactory::new(1);
        let tick = chart("SHFE.au2602", ServerBacktestHistoryKind::Tick);
        let minute = chart("SHFE.au2602", ServerBacktestHistoryKind::CanonicalMinute);
        let first = factory.acquire_chart(credentials(), &tick).await.unwrap();
        let second = factory.acquire_chart(credentials(), &minute).await.unwrap();
        assert_eq!(factory.created_session_count(), 1);
        assert!(matches!(
            factory.acquire_chart(credentials(), &tick).await,
            Err(DataError::CacheBusy { .. })
        ));
        first.finish(true);
        assert_eq!(
            factory.idle_session_count(),
            0,
            "the second reader still owns the connection"
        );
        let third = factory.acquire_chart(credentials(), &tick).await.unwrap();
        assert_eq!(factory.created_session_count(), 1);
        third.finish(true);
        second.finish(true);
        assert_eq!(factory.idle_session_count(), 1);
    }

    #[tokio::test]
    async fn suspended_cleanup_blocks_attach_and_cancelled_cleanup_poisons_job() {
        let factory = SessionServerHistorySourceFactory::new(1);
        let owner = factory
            .acquire_chart(
                credentials(),
                &chart("SHFE.au2602", ServerBacktestHistoryKind::Tick),
            )
            .await
            .unwrap();
        let next = chart("SHFE.au2604", ServerBacktestHistoryKind::Tick);
        let clean = owner.begin_close(true);
        assert!(matches!(
            factory.acquire_chart(credentials(), &next).await,
            Err(DataError::CacheBusy { .. })
        ));
        clean.finish(true);
        let sibling = factory.acquire_chart(credentials(), &next).await.unwrap();
        sibling.finish(true);
        let mut closing = Box::pin(async {
            let guard = owner.begin_close(true);
            std::future::pending::<()>().await;
            guard.finish(true);
        });
        assert!(futures::poll!(closing.as_mut()).is_pending());
        assert!(matches!(
            factory.acquire_chart(credentials(), &next).await,
            Err(DataError::CacheBusy { .. })
        ));
        drop(closing);
        assert!(matches!(
            factory.acquire_chart(credentials(), &next).await,
            Err(DataError::CacheBusy { .. })
        ));
        owner.finish(true);
        assert_eq!(factory.idle_session_count(), 0);
    }

    #[tokio::test]
    async fn every_next_event_error_taints_before_returning() {
        for establish_error in [true, false] {
            let factory = SessionServerHistorySourceFactory::new(1);
            let chart = chart("SHFE.au2602", ServerBacktestHistoryKind::Tick);
            let lease = factory.acquire_chart(credentials(), &chart).await.unwrap();
            let HistorySourceLease::Shared(owner) = &lease else {
                unreachable!()
            };
            let job = Arc::clone(&owner.job);
            let stream = if establish_error {
                job.admission
                    .observe::<()>(&Err(DataError::PermissionDenied("test refusal".into())));
                None
            } else {
                let stream = tqsdk_session::ServerBacktestHistoryStream::open(
                    lease.session().clone(),
                    ServerBacktestHistoryRequest {
                        market_kind: ServerBacktestMarketKind::Futures,
                        start_ns: 1_000,
                        end_ns: 2_000,
                        charts: vec![chart.clone()],
                    },
                )
                .await
                .unwrap();
                lease.session().handle().ingest(
                    tqsdk_core::RuntimeInput::Io(tqsdk_core::IoEvent {
                        route: "market".into(),
                        domains: vec![tqsdk_core::ProtocolDomain::Market],
                        payload: tqsdk_core::InputPayload::Json(serde_json::json!({
                            "aid": "rtn_data", "data": [{
                                "mdhis_more_data": false,
                                "charts": {chart.chart_id.clone(): {
                                    "state": {"ins_list": chart.symbol, "duration": 0,
                                        "view_width": 8964, "focus_datetime": 1000, "focus_position": 8964},
                                    "left_id": -1, "right_id": -1, "ready": true, "more_data": false
                                }},
                                "ticks": {"SHFE.au2602": {"last_id": -1, "data": {}}}
                            }]
                        })),
                    }),
                    Vec::new(),
                    tqsdk_core::CommitScope::RealtimeUpdate,
                ).unwrap();
                Some(stream)
            };
            let mut source = SessionServerHistorySource {
                stream,
                lease: Some(lease),
                // Missing chart kind forces the prune error on ChartCompleted.
                chart_kinds: BTreeMap::new(),
                state_pruned: !establish_error,
                admitted: !establish_error,
            };
            let error = tokio::time::timeout(Duration::from_secs(1), source.next_event())
                .await
                .unwrap()
                .unwrap_err();
            if !establish_error {
                assert!(matches!(
                    error,
                    DataError::InvalidState("completed server-history chart kind was not retained")
                ));
            }
            assert!(!job.state.lock().unwrap().reusable);
            assert!(
                factory
                    .acquire_chart(
                        credentials(),
                        &self::chart("SHFE.au2604", ServerBacktestHistoryKind::Tick)
                    )
                    .await
                    .is_err()
            );
            source.close(false).await.unwrap();
        }
    }

    #[tokio::test]
    async fn pruning_or_cancelling_one_reader_taints_the_whole_connection() {
        for prune in [false, true] {
            let factory = SessionServerHistorySourceFactory::new(1);
            let first = factory
                .acquire_chart(
                    credentials(),
                    &chart("SHFE.au2602", ServerBacktestHistoryKind::Tick),
                )
                .await
                .unwrap();
            let second = factory
                .acquire_chart(
                    credentials(),
                    &chart("SHFE.au2604", ServerBacktestHistoryKind::Tick),
                )
                .await
                .unwrap();
            if prune {
                first.mark_pruned();
                first.finish(true);
            } else {
                drop(first);
            }
            assert_eq!(factory.idle_session_count(), 0);
            assert!(
                factory
                    .acquire_chart(
                        credentials(),
                        &chart("SHFE.au2606", ServerBacktestHistoryKind::Tick)
                    )
                    .await
                    .is_err()
            );
            second.finish(true);
            assert_eq!(factory.idle_session_count(), 0);
            let replacement = factory
                .acquire_chart(
                    credentials(),
                    &chart("SHFE.au2602", ServerBacktestHistoryKind::Tick),
                )
                .await
                .unwrap();
            assert_eq!(factory.created_session_count(), 2);
            replacement.finish(false);
        }
    }
}
