#[derive(Debug, Clone)]
pub struct MarketCacheWriterElection {
    lock_path: PathBuf,
    stale_after: Option<Duration>,
}

impl MarketCacheWriterElection {
    #[must_use]
    pub fn new(lock_path: impl AsRef<Path>) -> Self {
        Self {
            lock_path: lock_path.as_ref().to_path_buf(),
            stale_after: None,
        }
    }

    #[must_use]
    pub fn stale_after(mut self, stale_after: Duration) -> Self {
        self.stale_after = Some(stale_after);
        self
    }

    pub fn elect(&self) -> Result<MarketCacheWriterElectionOutcome> {
        match create_lock_file(&self.lock_path) {
            Ok(file) => self.elected(file, false),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                if !lock_file_is_stale(&self.lock_path, self.stale_after)? {
                    return Ok(MarketCacheWriterElectionOutcome::busy(&self.lock_path));
                }
                match std::fs::remove_file(&self.lock_path) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(DataError::Io(error)),
                }
                match create_lock_file(&self.lock_path) {
                    Ok(file) => self.elected(file, true),
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                        Ok(MarketCacheWriterElectionOutcome::busy(&self.lock_path))
                    }
                    Err(error) => Err(DataError::Io(error)),
                }
            }
            Err(error) => Err(DataError::Io(error)),
        }
    }

    fn elected(
        &self,
        file: File,
        recovered_stale: bool,
    ) -> Result<MarketCacheWriterElectionOutcome> {
        let lease = MarketCacheWriterLease {
            lock: MarketCacheLock::from_file(self.lock_path.clone(), file)?,
            recovered_stale,
        };
        let lease_started_at_ns = lease.lease_started_at_ns();
        Ok(MarketCacheWriterElectionOutcome {
            report: MarketCacheWriterElectionReport {
                lock_path: self.lock_path.clone(),
                status: MarketCacheWriterElectionStatus::Elected,
                recovered_stale,
                lease_started_at_ns: Some(lease_started_at_ns),
            },
            lease: Some(lease),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MarketCacheWriterElectionStatus {
    Elected,
    Busy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketCacheWriterElectionReport {
    pub lock_path: PathBuf,
    pub status: MarketCacheWriterElectionStatus,
    pub recovered_stale: bool,
    pub lease_started_at_ns: Option<i64>,
}

#[derive(Debug)]
pub struct MarketCacheWriterElectionOutcome {
    report: MarketCacheWriterElectionReport,
    lease: Option<MarketCacheWriterLease>,
}

impl MarketCacheWriterElectionOutcome {
    fn busy(lock_path: &Path) -> Self {
        Self {
            report: MarketCacheWriterElectionReport {
                lock_path: lock_path.to_path_buf(),
                status: MarketCacheWriterElectionStatus::Busy,
                recovered_stale: false,
                lease_started_at_ns: None,
            },
            lease: None,
        }
    }

    #[must_use]
    pub fn report(&self) -> &MarketCacheWriterElectionReport {
        &self.report
    }

    #[must_use]
    pub fn is_elected(&self) -> bool {
        self.lease.is_some()
    }

    #[must_use]
    pub fn is_busy(&self) -> bool {
        self.lease.is_none()
    }

    #[must_use]
    pub fn into_lease(self) -> Option<MarketCacheWriterLease> {
        self.lease
    }
}

#[derive(Debug)]
pub struct MarketCacheWriterLease {
    lock: MarketCacheLock,
    recovered_stale: bool,
}

impl MarketCacheWriterLease {
    #[must_use]
    pub fn path(&self) -> &Path {
        self.lock.path()
    }

    #[must_use]
    pub fn recovered_stale(&self) -> bool {
        self.recovered_stale
    }

    #[must_use]
    pub fn lease_started_at_ns(&self) -> i64 {
        self.lock.lease_started_at_ns()
    }

    pub fn renew(&mut self) -> Result<()> {
        self.lock.renew()
    }
}
