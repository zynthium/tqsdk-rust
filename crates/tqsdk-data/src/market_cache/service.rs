#[derive(Debug, Clone)]
pub struct MarketCacheServiceConfig {
    cache_path: PathBuf,
    queue_path: PathBuf,
    processing_queue_path: PathBuf,
    lock_path: PathBuf,
    reader_manifest_path: PathBuf,
    compaction_staging_path: PathBuf,
    sync_on_enqueue: bool,
    stale_writer_after: Option<Duration>,
    compaction_policy: Option<MarketCacheCompaction>,
}

impl MarketCacheServiceConfig {
    #[must_use]
    pub fn new(cache_path: impl AsRef<Path>) -> Self {
        let cache_path = cache_path.as_ref().to_path_buf();
        let queue_path = path_with_suffix(&cache_path, ".queue");
        Self {
            processing_queue_path: path_with_suffix(&queue_path, ".processing"),
            lock_path: path_with_suffix(&cache_path, ".lock"),
            reader_manifest_path: path_with_suffix(&cache_path, ".readers.json"),
            compaction_staging_path: path_with_suffix(&cache_path, ".compact"),
            cache_path,
            queue_path,
            sync_on_enqueue: false,
            stale_writer_after: None,
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
    pub fn processing_queue_path_ref(&self) -> &Path {
        &self.processing_queue_path
    }

    #[must_use]
    pub fn lock_path_ref(&self) -> &Path {
        &self.lock_path
    }

    #[must_use]
    pub fn reader_manifest_path_ref(&self) -> &Path {
        &self.reader_manifest_path
    }

    #[must_use]
    pub fn compaction_staging_path_ref(&self) -> &Path {
        &self.compaction_staging_path
    }

    #[must_use]
    pub fn queue_path(mut self, queue_path: impl AsRef<Path>) -> Self {
        self.queue_path = queue_path.as_ref().to_path_buf();
        self.processing_queue_path = path_with_suffix(&self.queue_path, ".processing");
        self
    }

    #[must_use]
    pub fn processing_queue_path(mut self, processing_queue_path: impl AsRef<Path>) -> Self {
        self.processing_queue_path = processing_queue_path.as_ref().to_path_buf();
        self
    }

    #[must_use]
    pub fn lock_path(mut self, lock_path: impl AsRef<Path>) -> Self {
        self.lock_path = lock_path.as_ref().to_path_buf();
        self
    }

    #[must_use]
    pub fn reader_manifest_path(mut self, reader_manifest_path: impl AsRef<Path>) -> Self {
        self.reader_manifest_path = reader_manifest_path.as_ref().to_path_buf();
        self
    }

    #[must_use]
    pub fn compaction_staging_path(mut self, compaction_staging_path: impl AsRef<Path>) -> Self {
        self.compaction_staging_path = compaction_staging_path.as_ref().to_path_buf();
        self
    }

    #[must_use]
    pub fn with_sync_on_enqueue(mut self, sync_on_enqueue: bool) -> Self {
        self.sync_on_enqueue = sync_on_enqueue;
        self
    }

    #[must_use]
    pub fn stale_writer_after(mut self, stale_writer_after: Duration) -> Self {
        self.stale_writer_after = Some(stale_writer_after);
        self
    }

    #[must_use]
    pub fn compaction_policy(mut self, compaction_policy: MarketCacheCompaction) -> Self {
        self.compaction_policy = Some(compaction_policy);
        self
    }

    fn validate(&self) -> Result<()> {
        let paths = [
            ("cache", self.cache_path.as_path()),
            ("queue", self.queue_path.as_path()),
            ("processing queue", self.processing_queue_path.as_path()),
            ("lock", self.lock_path.as_path()),
            ("reader manifest", self.reader_manifest_path.as_path()),
            ("compaction staging", self.compaction_staging_path.as_path()),
        ];
        for (left_index, (left_name, left_path)) in paths.iter().enumerate() {
            for (right_name, right_path) in paths.iter().skip(left_index + 1) {
                if *left_path == *right_path {
                    return Err(DataError::Validation(format!(
                        "market cache service {left_name} and {right_name} paths must differ"
                    )));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct MarketCacheService {
    config: MarketCacheServiceConfig,
    queue: MarketCacheQueue,
    reader_manifest: MarketCacheReaderManifest,
    writer_lease: MarketCacheWriterLease,
}

impl MarketCacheService {
    pub fn open(config: MarketCacheServiceConfig) -> Result<MarketCacheServiceOpen> {
        config.validate()?;
        let mut election = MarketCacheWriterElection::new(&config.lock_path);
        if let Some(stale_after) = config.stale_writer_after {
            election = election.stale_after(stale_after);
        }
        let elected = election.elect()?;
        let writer = elected.report().clone();
        let Some(mut writer_lease) = elected.into_lease() else {
            return Ok(MarketCacheServiceOpen {
                report: MarketCacheServiceOpenReport {
                    writer,
                    recovery: None,
                },
                service: None,
            });
        };

        let recovery = MarketCacheRecoveryAction::new(&config.cache_path)
            .queue_path(&config.queue_path)
            .processing_queue_path(&config.processing_queue_path)
            .compaction_staging_path(&config.compaction_staging_path)
            .recover(&mut writer_lease)?;
        let queue = MarketCacheQueue::open(&config.queue_path)?
            .with_sync_on_enqueue(config.sync_on_enqueue);
        let reader_manifest = MarketCacheReaderManifest::open(&config.reader_manifest_path)?;
        Ok(MarketCacheServiceOpen {
            report: MarketCacheServiceOpenReport {
                writer,
                recovery: Some(recovery),
            },
            service: Some(Self {
                config,
                queue,
                reader_manifest,
                writer_lease,
            }),
        })
    }

    pub fn enqueue_event(&self, event: &MarketCacheEvent) -> Result<()> {
        self.queue.enqueue_event(event)
    }

    pub fn record_reader_checkpoint(&self, checkpoint: MarketCacheReaderCheckpoint) -> Result<()> {
        self.reader_manifest.record_checkpoint(checkpoint)
    }

    pub fn reader_lag_report(&self, head_event_time_ns: i64) -> Result<Vec<MarketCacheReaderLag>> {
        self.reader_manifest.reader_lag_report(head_event_time_ns)
    }

    pub fn flush_queue(
        &mut self,
    ) -> std::result::Result<MarketCacheQueueDrainReport, MarketCacheQueueDrainError> {
        self.writer_lease
            .renew()
            .map_err(|error| MarketCacheQueueDrainError {
                report: MarketCacheQueueDrainReport {
                    queue_path: self.config.queue_path.clone(),
                    read_events: 0,
                    written_events: 0,
                },
                error,
            })?;
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
        let report = self
            .queue
            .drain_to_writer_rotating(&mut writer, &self.config.processing_queue_path)?;
        self.writer_lease
            .renew()
            .map_err(|error| MarketCacheQueueDrainError {
                report: report.clone(),
                error,
            })?;
        Ok(report)
    }

    pub fn compact(&mut self) -> Result<Option<MarketCacheCompactionOwnershipReport>> {
        self.config
            .compaction_policy
            .as_ref()
            .map(|policy| {
                MarketCacheCompactionOwnership::new(&self.config.cache_path)
                    .staging_path(&self.config.compaction_staging_path)
                    .reader_manifest_path(&self.config.reader_manifest_path)
                    .policy(policy.clone())
                    .compact(&mut self.writer_lease)
            })
            .transpose()
    }

    pub fn shutdown(mut self) -> Result<MarketCacheServiceShutdownReport> {
        let flush_report = self.flush_queue().map_err(DataError::from)?;
        let compaction_report = self.compact()?;
        let queue_empty = self.queue.is_empty()?;
        Ok(MarketCacheServiceShutdownReport {
            flush_report,
            compaction_report,
            queue_empty,
        })
    }
}

#[derive(Debug, Clone)]
pub struct MarketCacheServiceOpenReport {
    pub writer: MarketCacheWriterElectionReport,
    pub recovery: Option<MarketCacheRecoveryActionReport>,
}

#[derive(Debug)]
pub struct MarketCacheServiceOpen {
    report: MarketCacheServiceOpenReport,
    service: Option<MarketCacheService>,
}

impl MarketCacheServiceOpen {
    #[must_use]
    pub fn report(&self) -> &MarketCacheServiceOpenReport {
        &self.report
    }

    #[must_use]
    pub fn is_open(&self) -> bool {
        self.service.is_some()
    }

    #[must_use]
    pub fn is_busy(&self) -> bool {
        self.service.is_none()
    }

    #[must_use]
    pub fn into_service(self) -> Option<MarketCacheService> {
        self.service
    }
}

#[derive(Debug, Clone)]
pub struct MarketCacheServiceShutdownReport {
    pub flush_report: MarketCacheQueueDrainReport,
    pub compaction_report: Option<MarketCacheCompactionOwnershipReport>,
    pub queue_empty: bool,
}
