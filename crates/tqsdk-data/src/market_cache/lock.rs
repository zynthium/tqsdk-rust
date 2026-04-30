#[derive(Debug, Clone)]
pub struct MarketCacheLockOptions {
    path: PathBuf,
    stale_after: Option<Duration>,
}

impl MarketCacheLockOptions {
    #[must_use]
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            stale_after: None,
        }
    }

    #[must_use]
    pub fn stale_after(mut self, stale_after: Duration) -> Self {
        self.stale_after = Some(stale_after);
        self
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[must_use]
    pub fn stale_after_duration(&self) -> Option<Duration> {
        self.stale_after
    }
}

#[derive(Debug)]
pub struct MarketCacheLock {
    path: PathBuf,
    file: File,
    lease_started_at_ns: i64,
}

impl MarketCacheLock {
    pub fn acquire(path: impl AsRef<Path>) -> Result<Self> {
        Self::acquire_with_options(MarketCacheLockOptions::new(path))
    }

    pub fn acquire_with_options(options: MarketCacheLockOptions) -> Result<Self> {
        let path = options.path.clone();
        match create_lock_file(&path) {
            Ok(file) => Self::from_file(path, file),
            Err(error)
                if error.kind() == std::io::ErrorKind::AlreadyExists
                    && lock_file_is_stale(&path, options.stale_after)? =>
            {
                std::fs::remove_file(&path)?;
                Self::from_file(path.clone(), create_lock_file(&path)?)
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                Err(DataError::InvalidState("market cache lock is already held"))
            }
            Err(error) => Err(DataError::Io(error)),
        }
    }

    pub(super) fn from_file(path: PathBuf, mut file: File) -> Result<Self> {
        let lease_started_at_ns = write_lock_lease(&mut file)?;
        Ok(Self {
            path,
            file,
            lease_started_at_ns,
        })
    }

    pub fn renew(&mut self) -> Result<()> {
        let lease_started_at_ns = write_lock_lease(&mut self.file)?;
        if read_lock_lease_started_at_ns(&self.path)? != Some(lease_started_at_ns) {
            return Err(DataError::InvalidState(
                "market cache lock lease file was replaced",
            ));
        }
        self.lease_started_at_ns = lease_started_at_ns;
        Ok(())
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[must_use]
    pub fn lease_started_at_ns(&self) -> i64 {
        self.lease_started_at_ns
    }
}

pub(super) fn create_lock_file(path: &Path) -> std::io::Result<File> {
    OpenOptions::new().write(true).create_new(true).open(path)
}

fn write_lock_lease(file: &mut File) -> Result<i64> {
    let lease_started_at_ns = system_time_ns()?;
    file.set_len(0)?;
    file.seek(SeekFrom::Start(0))?;
    writeln!(file, "pid={}", std::process::id())?;
    writeln!(file, "lease_started_at_ns={lease_started_at_ns}")?;
    file.flush()?;
    Ok(lease_started_at_ns)
}

pub(super) fn lock_file_is_stale(path: &Path, stale_after: Option<Duration>) -> Result<bool> {
    let Some(stale_after) = stale_after else {
        return Ok(false);
    };
    if stale_after.is_zero() {
        return Ok(true);
    }
    if let Some(lease_started_at_ns) = read_lock_lease_started_at_ns(path)? {
        let now = system_time_ns()?;
        return Ok(now
            .saturating_sub(lease_started_at_ns)
            .try_into()
            .is_ok_and(|age_ns: u128| age_ns >= stale_after.as_nanos()));
    }
    Ok(std::fs::metadata(path)
        .and_then(|metadata| metadata.modified())
        .ok()
        .and_then(|modified| SystemTime::now().duration_since(modified).ok())
        .is_some_and(|age| age >= stale_after))
}

fn read_lock_lease_started_at_ns(path: &Path) -> Result<Option<i64>> {
    let content = match std::fs::read_to_string(path) {
        Ok(content) => content,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(DataError::Io(error)),
    };
    Ok(content.lines().find_map(|line| {
        line.strip_prefix("lease_started_at_ns=")
            .and_then(|value| value.trim().parse::<i64>().ok())
    }))
}

impl Drop for MarketCacheLock {
    fn drop(&mut self) {
        if read_lock_lease_started_at_ns(&self.path)
            .ok()
            .flatten()
            .is_some_and(|lease| lease == self.lease_started_at_ns)
        {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}
