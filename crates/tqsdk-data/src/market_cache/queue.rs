#[derive(Debug)]
pub struct MarketCacheQueueDrainError {
    pub report: MarketCacheQueueDrainReport,
    pub error: DataError,
}

impl Display for MarketCacheQueueDrainError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "market cache queue drain failed after reading {} event(s) and writing {} event(s): {}",
            self.report.read_events, self.report.written_events, self.error
        )
    }
}

impl std::error::Error for MarketCacheQueueDrainError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

impl From<MarketCacheQueueDrainError> for DataError {
    fn from(error: MarketCacheQueueDrainError) -> Self {
        error.error
    }
}

#[derive(Debug, Clone)]
pub struct MarketCacheQueue {
    path: PathBuf,
    sync_on_enqueue: bool,
    writer: Arc<Mutex<File>>,
}

impl MarketCacheQueue {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let writer = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok(Self {
            path,
            sync_on_enqueue: false,
            writer: Arc::new(Mutex::new(writer)),
        })
    }

    #[must_use]
    pub fn with_sync_on_enqueue(mut self, sync_on_enqueue: bool) -> Self {
        self.sync_on_enqueue = sync_on_enqueue;
        self
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn enqueue_event(&self, event: &MarketCacheEvent) -> Result<()> {
        let mut file = self.writer()?;
        let file = &mut *file;
        write_market_cache_event_line(file, event)?;
        file.flush()?;
        if self.sync_on_enqueue {
            file.sync_data()?;
        }
        Ok(())
    }

    pub fn reader(&self) -> Result<MarketCacheReader<BufReader<File>>> {
        self.flush_writer()?;
        MarketCacheReader::open(&self.path)
    }

    pub fn replay(&self) -> Result<MarketCacheReplay> {
        MarketCacheReplay::from_reader(self.reader()?)
    }

    pub fn is_empty(&self) -> Result<bool> {
        self.flush_writer()?;
        Ok(std::fs::metadata(&self.path)?.len() == 0)
    }

    pub fn clear(&self) -> Result<()> {
        let mut file = self.writer()?;
        file.set_len(0)?;
        file.seek(SeekFrom::Start(0))?;
        Ok(())
    }

    fn writer(&self) -> Result<std::sync::MutexGuard<'_, File>> {
        self.writer
            .lock()
            .map_err(|_| DataError::Validation("market cache queue writer lock poisoned".into()))
    }

    fn flush_writer(&self) -> Result<()> {
        self.writer()?.flush()?;
        Ok(())
    }

    fn reopen_empty_writer(&self) -> Result<()> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        *self.writer()? = file;
        Ok(())
    }

    pub fn drain_to_writer<W: Write>(
        &self,
        writer: &mut MarketCacheWriter<W>,
    ) -> Result<MarketCacheQueueDrainReport> {
        self.drain_to_writer_with_report(writer)
            .map_err(DataError::from)
    }

    pub fn drain_to_writer_with_report<W: Write>(
        &self,
        writer: &mut MarketCacheWriter<W>,
    ) -> std::result::Result<MarketCacheQueueDrainReport, MarketCacheQueueDrainError> {
        let mut report = MarketCacheQueueDrainReport {
            queue_path: self.path.clone(),
            read_events: 0,
            written_events: 0,
        };
        let reader = self.reader().map_err(|error| MarketCacheQueueDrainError {
            report: report.clone(),
            error,
        })?;
        for event in reader {
            let event = event.map_err(|error| MarketCacheQueueDrainError {
                report: report.clone(),
                error,
            })?;
            report.read_events += 1;
            writer
                .write_event(&event)
                .map_err(|error| MarketCacheQueueDrainError {
                    report: report.clone(),
                    error,
                })?;
            report.written_events += 1;
        }
        writer.flush().map_err(|error| MarketCacheQueueDrainError {
            report: report.clone(),
            error,
        })?;
        self.clear().map_err(|error| MarketCacheQueueDrainError {
            report: report.clone(),
            error,
        })?;
        Ok(report)
    }

    pub fn drain_to_writer_rotating<W: Write>(
        &self,
        writer: &mut MarketCacheWriter<W>,
        processing_path: impl AsRef<Path>,
    ) -> std::result::Result<MarketCacheQueueDrainReport, MarketCacheQueueDrainError> {
        let processing_path = processing_path.as_ref();
        let mut report = MarketCacheQueueDrainReport {
            queue_path: self.path.clone(),
            read_events: 0,
            written_events: 0,
        };
        if processing_path == self.path {
            return Err(MarketCacheQueueDrainError {
                report,
                error: DataError::Validation(
                    "market cache processing queue path must differ from queue path".into(),
                ),
            });
        }

        self.drain_processing_file(writer, processing_path, &mut report)?;
        self.flush_writer()
            .map_err(|error| MarketCacheQueueDrainError {
                report: report.clone(),
                error,
            })?;
        if self
            .is_empty()
            .map_err(|error| MarketCacheQueueDrainError {
                report: report.clone(),
                error,
            })?
        {
            writer.flush().map_err(|error| MarketCacheQueueDrainError {
                report: report.clone(),
                error,
            })?;
            return Ok(report);
        }

        std::fs::rename(&self.path, processing_path).map_err(|error| {
            MarketCacheQueueDrainError {
                report: report.clone(),
                error: DataError::Io(error),
            }
        })?;
        self.reopen_empty_writer()
            .map_err(|error| MarketCacheQueueDrainError {
                report: report.clone(),
                error,
            })?;
        self.drain_processing_file(writer, processing_path, &mut report)?;
        writer.flush().map_err(|error| MarketCacheQueueDrainError {
            report: report.clone(),
            error,
        })?;
        Ok(report)
    }

    fn drain_processing_file<W: Write>(
        &self,
        writer: &mut MarketCacheWriter<W>,
        processing_path: &Path,
        report: &mut MarketCacheQueueDrainReport,
    ) -> std::result::Result<(), MarketCacheQueueDrainError> {
        let metadata = match std::fs::metadata(processing_path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => {
                return Err(MarketCacheQueueDrainError {
                    report: report.clone(),
                    error: DataError::Io(error),
                });
            }
        };
        if metadata.len() == 0 {
            std::fs::remove_file(processing_path).map_err(|error| MarketCacheQueueDrainError {
                report: report.clone(),
                error: DataError::Io(error),
            })?;
            return Ok(());
        }

        let reader = MarketCacheReader::open(processing_path).map_err(|error| {
            MarketCacheQueueDrainError {
                report: report.clone(),
                error,
            }
        })?;
        for event in reader {
            let event = event.map_err(|error| MarketCacheQueueDrainError {
                report: report.clone(),
                error,
            })?;
            report.read_events += 1;
            writer
                .write_event(&event)
                .map_err(|error| MarketCacheQueueDrainError {
                    report: report.clone(),
                    error,
                })?;
            report.written_events += 1;
        }
        writer.flush().map_err(|error| MarketCacheQueueDrainError {
            report: report.clone(),
            error,
        })?;
        std::fs::remove_file(processing_path).map_err(|error| MarketCacheQueueDrainError {
            report: report.clone(),
            error: DataError::Io(error),
        })?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketCacheQueueDrainReport {
    pub queue_path: PathBuf,
    pub read_events: usize,
    pub written_events: usize,
}

#[derive(Debug, Clone)]
pub struct MarketCacheRecoveryAction {
    cache_path: PathBuf,
    queue_path: PathBuf,
    processing_queue_path: PathBuf,
    compaction_staging_path: PathBuf,
}

impl MarketCacheRecoveryAction {
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

    pub fn recover(
        &self,
        writer_lease: &mut MarketCacheWriterLease,
    ) -> Result<MarketCacheRecoveryActionReport> {
        writer_lease.renew()?;
        let scan_before = self.scan()?;
        if scan_before.has_read_errors() {
            return Err(DataError::InvalidState(
                "market cache recovery action requires readable cache and queue files",
            ));
        }

        let mut writer = MarketCacheWriter::append(&self.cache_path)?;
        let queue = MarketCacheQueue::open(&self.queue_path)?;
        let queue_drain_report = queue
            .drain_to_writer_rotating(&mut writer, &self.processing_queue_path)
            .map_err(DataError::from)?;
        writer_lease.renew()?;
        let scan_after = self.scan()?;
        Ok(MarketCacheRecoveryActionReport {
            scan_before,
            queue_drain_report,
            scan_after,
        })
    }

    fn scan(&self) -> Result<MarketCacheRecoveryReport> {
        MarketCacheRecoveryScan::new(&self.cache_path)
            .queue_path(&self.queue_path)
            .processing_queue_path(&self.processing_queue_path)
            .compaction_staging_path(&self.compaction_staging_path)
            .scan()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketCacheRecoveryActionReport {
    pub scan_before: MarketCacheRecoveryReport,
    pub queue_drain_report: MarketCacheQueueDrainReport,
    pub scan_after: MarketCacheRecoveryReport,
}

impl MarketCacheRecoveryActionReport {
    #[must_use]
    pub fn recovered_events(&self) -> usize {
        self.queue_drain_report.written_events
    }

    #[must_use]
    pub fn requires_follow_up(&self) -> bool {
        self.scan_after.requires_writer_recovery()
    }
}
