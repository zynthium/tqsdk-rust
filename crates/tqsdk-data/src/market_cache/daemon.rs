#[derive(Debug, Clone)]
pub struct MarketCacheDaemonConfig {
    cache_path: PathBuf,
    queue_path: PathBuf,
    lock_path: PathBuf,
    compaction_staging_path: PathBuf,
    sync_on_enqueue: bool,
    stale_lock_after: Option<Duration>,
    compaction_policy: Option<MarketCacheCompaction>,
}

impl MarketCacheDaemonConfig {
    #[must_use]
    pub fn new(cache_path: impl AsRef<Path>) -> Self {
        let cache_path = cache_path.as_ref().to_path_buf();
        Self {
            queue_path: path_with_suffix(&cache_path, ".queue"),
            lock_path: path_with_suffix(&cache_path, ".lock"),
            compaction_staging_path: path_with_suffix(&cache_path, ".compact"),
            cache_path,
            sync_on_enqueue: false,
            stale_lock_after: None,
            compaction_policy: None,
        }
    }

    #[must_use]
    pub fn cache_path(&self) -> &Path {
        &self.cache_path
    }

    #[must_use]
    pub fn queue_path_ref(&self) -> &Path {
        &self.queue_path
    }

    #[must_use]
    pub fn lock_path_ref(&self) -> &Path {
        &self.lock_path
    }

    #[must_use]
    pub fn compaction_staging_path_ref(&self) -> &Path {
        &self.compaction_staging_path
    }

    #[must_use]
    pub fn with_sync_on_enqueue(mut self, sync_on_enqueue: bool) -> Self {
        self.sync_on_enqueue = sync_on_enqueue;
        self
    }

    #[must_use]
    pub fn queue_path(mut self, queue_path: impl AsRef<Path>) -> Self {
        self.queue_path = queue_path.as_ref().to_path_buf();
        self
    }

    #[must_use]
    pub fn lock_path(mut self, lock_path: impl AsRef<Path>) -> Self {
        self.lock_path = lock_path.as_ref().to_path_buf();
        self
    }

    #[must_use]
    pub fn compaction_staging_path(mut self, compaction_staging_path: impl AsRef<Path>) -> Self {
        self.compaction_staging_path = compaction_staging_path.as_ref().to_path_buf();
        self
    }

    #[must_use]
    pub fn stale_lock_after(mut self, stale_lock_after: Duration) -> Self {
        self.stale_lock_after = Some(stale_lock_after);
        self
    }

    #[must_use]
    pub fn compaction_policy(mut self, compaction_policy: MarketCacheCompaction) -> Self {
        self.compaction_policy = Some(compaction_policy);
        self
    }
}

#[derive(Debug)]
pub struct MarketCacheDaemon {
    config: MarketCacheDaemonConfig,
    queue: MarketCacheQueue,
    lock: MarketCacheLock,
}

impl MarketCacheDaemon {
    pub fn open(config: MarketCacheDaemonConfig) -> Result<Self> {
        let lock = MarketCacheLock::acquire_with_options({
            let mut options = MarketCacheLockOptions::new(&config.lock_path);
            if let Some(stale_after) = config.stale_lock_after {
                options = options.stale_after(stale_after);
            }
            options
        })?;
        let queue = MarketCacheQueue::open(&config.queue_path)?
            .with_sync_on_enqueue(config.sync_on_enqueue);
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(&config.cache_path)?;
        Ok(Self {
            config,
            queue,
            lock,
        })
    }

    #[must_use]
    pub fn cache_path(&self) -> &Path {
        &self.config.cache_path
    }

    #[must_use]
    pub fn queue_path(&self) -> &Path {
        self.queue.path()
    }

    pub fn enqueue_event(&self, event: &MarketCacheEvent) -> Result<()> {
        self.queue.enqueue_event(event)
    }

    pub fn flush_queue(
        &self,
    ) -> std::result::Result<MarketCacheQueueDrainReport, MarketCacheQueueDrainError> {
        let mut writer = MarketCacheWriter::append(&self.config.cache_path).map_err(|error| {
            MarketCacheQueueDrainError {
                report: MarketCacheQueueDrainReport {
                    queue_path: self.config.queue_path.clone(),
                    read_events: 0,
                    written_events: 0,
                },
                error,
            }
        })?;
        self.queue.drain_to_writer_with_report(&mut writer)
    }

    pub fn flush_queue_rotating(
        &self,
        processing_path: impl AsRef<Path>,
    ) -> std::result::Result<MarketCacheQueueDrainReport, MarketCacheQueueDrainError> {
        let mut writer = MarketCacheWriter::append(&self.config.cache_path).map_err(|error| {
            MarketCacheQueueDrainError {
                report: MarketCacheQueueDrainReport {
                    queue_path: self.config.queue_path.clone(),
                    read_events: 0,
                    written_events: 0,
                },
                error,
            }
        })?;
        self.queue
            .drain_to_writer_rotating(&mut writer, processing_path)
    }

    pub fn renew_lock(&mut self) -> Result<()> {
        self.lock.renew()
    }

    pub fn spawn_supervisor(
        self,
        config: MarketCacheSupervisorConfig,
    ) -> Result<MarketCacheSupervisor> {
        config.validate()?;
        let processing_queue_path = config
            .processing_queue_path
            .clone()
            .unwrap_or_else(|| path_with_suffix(&self.config.queue_path, ".processing"));
        if processing_queue_path == self.config.queue_path {
            return Err(DataError::Validation(
                "market cache supervisor processing queue path must differ from queue path".into(),
            ));
        }

        let queue = self.queue.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let handle = thread::spawn(move || {
            run_market_cache_supervisor(self, config, processing_queue_path, thread_stop)
        });

        Ok(MarketCacheSupervisor {
            queue,
            stop,
            handle: Some(handle),
        })
    }

    pub fn shutdown(self) -> Result<MarketCacheDaemonShutdownReport> {
        let flush_report = self.flush_queue().map_err(DataError::from)?;
        let compaction_report = self
            .config
            .compaction_policy
            .as_ref()
            .map(|policy| {
                policy.compact_file_in_place(
                    &self.config.cache_path,
                    &self.config.compaction_staging_path,
                )
            })
            .transpose()?;
        let queue_empty = self.queue.is_empty()?;
        Ok(MarketCacheDaemonShutdownReport {
            flush_report,
            compaction_report,
            queue_empty,
        })
    }
}

#[derive(Debug, Clone)]
pub struct MarketCacheDaemonShutdownReport {
    pub flush_report: MarketCacheQueueDrainReport,
    pub compaction_report: Option<MarketCacheAtomicCompactionReport>,
    pub queue_empty: bool,
}

#[derive(Debug, Clone)]
pub struct MarketCacheSupervisorConfig {
    flush_interval: Duration,
    lease_renew_interval: Duration,
    idle_sleep: Duration,
    processing_queue_path: Option<PathBuf>,
}

impl MarketCacheSupervisorConfig {
    #[must_use]
    pub fn new() -> Self {
        Self {
            flush_interval: Duration::from_secs(1),
            lease_renew_interval: Duration::from_secs(5),
            idle_sleep: Duration::from_millis(10),
            processing_queue_path: None,
        }
    }

    #[must_use]
    pub fn flush_interval(mut self, flush_interval: Duration) -> Self {
        self.flush_interval = flush_interval;
        self
    }

    #[must_use]
    pub fn lease_renew_interval(mut self, lease_renew_interval: Duration) -> Self {
        self.lease_renew_interval = lease_renew_interval;
        self
    }

    #[must_use]
    pub fn idle_sleep(mut self, idle_sleep: Duration) -> Self {
        self.idle_sleep = idle_sleep;
        self
    }

    #[must_use]
    pub fn processing_queue_path(mut self, processing_queue_path: impl AsRef<Path>) -> Self {
        self.processing_queue_path = Some(processing_queue_path.as_ref().to_path_buf());
        self
    }

    fn validate(&self) -> Result<()> {
        if self.flush_interval.is_zero() {
            return Err(DataError::Validation(
                "market cache supervisor flush interval must be positive".into(),
            ));
        }
        if self.lease_renew_interval.is_zero() {
            return Err(DataError::Validation(
                "market cache supervisor lease renew interval must be positive".into(),
            ));
        }
        if self.idle_sleep.is_zero() {
            return Err(DataError::Validation(
                "market cache supervisor idle sleep must be positive".into(),
            ));
        }
        Ok(())
    }
}

impl Default for MarketCacheSupervisorConfig {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub struct MarketCacheSupervisor {
    queue: MarketCacheQueue,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<Result<MarketCacheSupervisorShutdownReport>>>,
}

impl MarketCacheSupervisor {
    pub fn enqueue_event(&self, event: &MarketCacheEvent) -> Result<()> {
        self.queue.enqueue_event(event)
    }

    pub fn shutdown(mut self) -> Result<MarketCacheSupervisorShutdownReport> {
        self.stop.store(true, Ordering::Release);
        let handle = self.handle.take().ok_or(DataError::InvalidState(
            "market cache supervisor is already shut down",
        ))?;
        handle
            .join()
            .map_err(|_| DataError::InvalidState("market cache supervisor thread panicked"))?
    }
}

impl Drop for MarketCacheSupervisor {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

#[derive(Debug, Clone)]
pub struct MarketCacheSupervisorShutdownReport {
    pub periodic_flushes: usize,
    pub lease_renewals: usize,
    pub periodic_errors: usize,
    pub pre_shutdown_flush_report: MarketCacheQueueDrainReport,
    pub shutdown: MarketCacheDaemonShutdownReport,
}

fn run_market_cache_supervisor(
    mut daemon: MarketCacheDaemon,
    config: MarketCacheSupervisorConfig,
    processing_queue_path: PathBuf,
    stop: Arc<AtomicBool>,
) -> Result<MarketCacheSupervisorShutdownReport> {
    let mut periodic_flushes = 0;
    let mut lease_renewals = 0;
    let mut periodic_errors = 0;
    let now = Instant::now();
    let mut last_flush = now - config.flush_interval;
    let mut last_renew = now - config.lease_renew_interval;

    while !stop.load(Ordering::Acquire) {
        let now = Instant::now();
        if now.duration_since(last_flush) >= config.flush_interval {
            match daemon.flush_queue_rotating(&processing_queue_path) {
                Ok(_) => periodic_flushes += 1,
                Err(_) => periodic_errors += 1,
            }
            last_flush = now;
        }
        if now.duration_since(last_renew) >= config.lease_renew_interval {
            match daemon.renew_lock() {
                Ok(()) => lease_renewals += 1,
                Err(_) => periodic_errors += 1,
            }
            last_renew = now;
        }
        thread::sleep(config.idle_sleep);
    }

    let pre_shutdown_flush_report = daemon
        .flush_queue_rotating(&processing_queue_path)
        .map_err(DataError::from)?;
    let shutdown = daemon.shutdown()?;
    Ok(MarketCacheSupervisorShutdownReport {
        periodic_flushes,
        lease_renewals,
        periodic_errors,
        pre_shutdown_flush_report,
        shutdown,
    })
}
