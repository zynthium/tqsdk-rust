#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MarketCacheRecoveryFileKind {
    Cache,
    Queue,
    ProcessingQueue,
    CompactionStaging,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketCacheRecoveryFileReport {
    pub kind: MarketCacheRecoveryFileKind,
    pub path: PathBuf,
    pub exists: bool,
    pub bytes: u64,
    pub readable_events: usize,
    pub first_event_time_ns: Option<i64>,
    pub last_event_time_ns: Option<i64>,
    pub read_error: Option<String>,
}

impl MarketCacheRecoveryFileReport {
    #[must_use]
    pub fn has_events(&self) -> bool {
        self.readable_events > 0
    }

    #[must_use]
    pub fn has_bytes(&self) -> bool {
        self.bytes > 0
    }

    #[must_use]
    pub fn is_readable(&self) -> bool {
        self.read_error.is_none()
    }
}

#[derive(Debug, Clone)]
pub struct MarketCacheRecoveryScan {
    cache_path: PathBuf,
    queue_path: PathBuf,
    processing_queue_path: PathBuf,
    compaction_staging_path: PathBuf,
}

impl MarketCacheRecoveryScan {
    #[must_use]
    pub fn new(cache_path: impl AsRef<Path>) -> Self {
        let cache_path = cache_path.as_ref().to_path_buf();
        let queue_path = path_with_suffix(&cache_path, ".queue");
        let processing_queue_path = path_with_suffix(&queue_path, ".processing");
        let compaction_staging_path = path_with_suffix(&cache_path, ".compact");
        Self {
            cache_path,
            queue_path,
            processing_queue_path,
            compaction_staging_path,
        }
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
    pub fn compaction_staging_path(mut self, compaction_staging_path: impl AsRef<Path>) -> Self {
        self.compaction_staging_path = compaction_staging_path.as_ref().to_path_buf();
        self
    }

    pub fn scan(&self) -> Result<MarketCacheRecoveryReport> {
        self.validate()?;
        Ok(MarketCacheRecoveryReport {
            cache: scan_market_cache_recovery_file(
                MarketCacheRecoveryFileKind::Cache,
                &self.cache_path,
            )?,
            queue: scan_market_cache_recovery_file(
                MarketCacheRecoveryFileKind::Queue,
                &self.queue_path,
            )?,
            processing_queue: scan_market_cache_recovery_file(
                MarketCacheRecoveryFileKind::ProcessingQueue,
                &self.processing_queue_path,
            )?,
            compaction_staging: scan_market_cache_recovery_file(
                MarketCacheRecoveryFileKind::CompactionStaging,
                &self.compaction_staging_path,
            )?,
        })
    }

    fn validate(&self) -> Result<()> {
        if self.cache_path == self.queue_path {
            return Err(DataError::Validation(
                "market cache recovery cache and queue paths must differ".into(),
            ));
        }
        if self.queue_path == self.processing_queue_path {
            return Err(DataError::Validation(
                "market cache recovery queue and processing queue paths must differ".into(),
            ));
        }
        if self.cache_path == self.compaction_staging_path {
            return Err(DataError::Validation(
                "market cache recovery cache and compaction staging paths must differ".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketCacheRecoveryReport {
    pub cache: MarketCacheRecoveryFileReport,
    pub queue: MarketCacheRecoveryFileReport,
    pub processing_queue: MarketCacheRecoveryFileReport,
    pub compaction_staging: MarketCacheRecoveryFileReport,
}

impl MarketCacheRecoveryReport {
    #[must_use]
    pub fn has_pending_queue_events(&self) -> bool {
        self.queue.has_events() || self.processing_queue.has_events()
    }

    #[must_use]
    pub fn has_interrupted_drain(&self) -> bool {
        self.processing_queue.has_bytes()
    }

    #[must_use]
    pub fn has_interrupted_compaction(&self) -> bool {
        self.compaction_staging.has_bytes()
    }

    #[must_use]
    pub fn has_read_errors(&self) -> bool {
        self.files().any(|file| !file.is_readable())
    }

    #[must_use]
    pub fn requires_writer_recovery(&self) -> bool {
        self.has_interrupted_drain() || self.has_interrupted_compaction() || self.has_read_errors()
    }

    pub fn files(&self) -> impl Iterator<Item = &MarketCacheRecoveryFileReport> {
        [
            &self.cache,
            &self.queue,
            &self.processing_queue,
            &self.compaction_staging,
        ]
        .into_iter()
    }
}

fn scan_market_cache_recovery_file(
    kind: MarketCacheRecoveryFileKind,
    path: &Path,
) -> Result<MarketCacheRecoveryFileReport> {
    let metadata = match std::fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(MarketCacheRecoveryFileReport {
                kind,
                path: path.to_path_buf(),
                exists: false,
                bytes: 0,
                readable_events: 0,
                first_event_time_ns: None,
                last_event_time_ns: None,
                read_error: None,
            });
        }
        Err(error) => return Err(DataError::Io(error)),
    };
    let mut report = MarketCacheRecoveryFileReport {
        kind,
        path: path.to_path_buf(),
        exists: true,
        bytes: metadata.len(),
        readable_events: 0,
        first_event_time_ns: None,
        last_event_time_ns: None,
        read_error: None,
    };
    if metadata.len() == 0 {
        return Ok(report);
    }

    for event in MarketCacheReader::open(path)? {
        let event = match event {
            Ok(event) => event,
            Err(error) => {
                report.read_error = Some(error.to_string());
                break;
            }
        };
        let event_time_ns = event.event_time_ns();
        report.first_event_time_ns = Some(
            report
                .first_event_time_ns
                .map_or(event_time_ns, |time| time.min(event_time_ns)),
        );
        report.last_event_time_ns = Some(
            report
                .last_event_time_ns
                .map_or(event_time_ns, |time| time.max(event_time_ns)),
        );
        report.readable_events += 1;
    }
    Ok(report)
}
