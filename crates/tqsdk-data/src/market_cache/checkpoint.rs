#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketCacheReaderCheckpoint {
    pub reader_id: String,
    pub checkpoint_id: String,
    pub source: String,
    pub symbol: String,
    pub payload_kind: MarketCachePayloadKind,
    pub event_time_ns: i64,
    pub received_at_ns: i64,
}

impl MarketCacheReaderCheckpoint {
    #[must_use]
    pub fn from_event(
        reader_id: impl Into<String>,
        checkpoint_id: impl Into<String>,
        event: &MarketCacheEvent,
    ) -> Self {
        Self {
            reader_id: reader_id.into(),
            checkpoint_id: checkpoint_id.into(),
            source: event.source.clone(),
            symbol: event.symbol.clone(),
            payload_kind: event.payload_kind(),
            event_time_ns: event.event_time_ns(),
            received_at_ns: event.received_at_ns,
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.reader_id.trim().is_empty() {
            return Err(DataError::Validation(
                "market cache reader id must not be empty".into(),
            ));
        }
        if self.checkpoint_id.trim().is_empty() {
            return Err(DataError::Validation(
                "market cache reader checkpoint id must not be empty".into(),
            ));
        }
        if self.source.trim().is_empty() {
            return Err(DataError::Validation(
                "market cache reader checkpoint source must not be empty".into(),
            ));
        }
        if self.symbol.trim().is_empty() {
            return Err(DataError::Validation(
                "market cache reader checkpoint symbol must not be empty".into(),
            ));
        }
        if self.event_time_ns < 0 || self.received_at_ns < 0 {
            return Err(DataError::Validation(
                "market cache reader checkpoint times must be non-negative".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketCacheReaderLag {
    pub reader_id: String,
    pub checkpoint_id: String,
    pub event_time_ns: i64,
    pub lag_event_time_ns: i64,
}

#[derive(Debug, Clone)]
pub struct MarketCacheReaderManifest {
    path: PathBuf,
}

impl MarketCacheReaderManifest {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let manifest = Self {
            path: path.as_ref().to_path_buf(),
        };
        if !manifest.path.exists() || std::fs::metadata(&manifest.path)?.len() == 0 {
            manifest.write_state(&MarketCacheReaderManifestState::default())?;
        } else {
            manifest.read_state()?;
        }
        Ok(manifest)
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn record_checkpoint(&self, checkpoint: MarketCacheReaderCheckpoint) -> Result<()> {
        checkpoint.validate()?;
        let mut state = self.read_state()?;
        state
            .checkpoints
            .insert(checkpoint.reader_id.clone(), checkpoint);
        self.write_state(&state)
    }

    pub fn checkpoint(&self, reader_id: &str) -> Result<Option<MarketCacheReaderCheckpoint>> {
        validate_market_cache_reader_id(reader_id)?;
        Ok(self.read_state()?.checkpoints.get(reader_id).cloned())
    }

    pub fn checkpoints(&self) -> Result<Vec<MarketCacheReaderCheckpoint>> {
        Ok(self.read_state()?.checkpoints.into_values().collect())
    }

    pub fn remove_reader(&self, reader_id: &str) -> Result<bool> {
        validate_market_cache_reader_id(reader_id)?;
        let mut state = self.read_state()?;
        let removed = state.checkpoints.remove(reader_id).is_some();
        if removed {
            self.write_state(&state)?;
        }
        Ok(removed)
    }

    pub fn compaction_floor_event_time_ns(&self) -> Result<Option<i64>> {
        Ok(self
            .read_state()?
            .checkpoints
            .values()
            .map(|checkpoint| checkpoint.event_time_ns)
            .min())
    }

    pub fn reader_lag_report(&self, head_event_time_ns: i64) -> Result<Vec<MarketCacheReaderLag>> {
        if head_event_time_ns < 0 {
            return Err(DataError::Validation(
                "market cache reader lag head event time must be non-negative".into(),
            ));
        }
        let mut report = self
            .read_state()?
            .checkpoints
            .into_values()
            .map(|checkpoint| MarketCacheReaderLag {
                reader_id: checkpoint.reader_id,
                checkpoint_id: checkpoint.checkpoint_id,
                event_time_ns: checkpoint.event_time_ns,
                lag_event_time_ns: head_event_time_ns.saturating_sub(checkpoint.event_time_ns),
            })
            .collect::<Vec<_>>();
        report.sort_by(|left, right| {
            right
                .lag_event_time_ns
                .cmp(&left.lag_event_time_ns)
                .then_with(|| left.reader_id.cmp(&right.reader_id))
        });
        Ok(report)
    }

    fn read_state(&self) -> Result<MarketCacheReaderManifestState> {
        let content = match std::fs::read_to_string(&self.path) {
            Ok(content) => content,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(MarketCacheReaderManifestState::default());
            }
            Err(error) => return Err(DataError::Io(error)),
        };
        if content.trim().is_empty() {
            return Ok(MarketCacheReaderManifestState::default());
        }
        let state: MarketCacheReaderManifestState = serde_json::from_str(&content)?;
        for checkpoint in state.checkpoints.values() {
            checkpoint.validate()?;
        }
        Ok(state)
    }

    fn write_state(&self, state: &MarketCacheReaderManifestState) -> Result<()> {
        let staging_path = path_with_suffix(&self.path, ".tmp");
        {
            let mut file = File::create(&staging_path)?;
            serde_json::to_writer_pretty(&mut file, state)?;
            file.write_all(b"\n")?;
            file.flush()?;
        }
        std::fs::rename(staging_path, &self.path)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct MarketCacheReaderManifestState {
    checkpoints: BTreeMap<String, MarketCacheReaderCheckpoint>,
}

fn validate_market_cache_reader_id(reader_id: &str) -> Result<()> {
    if reader_id.trim().is_empty() {
        return Err(DataError::Validation(
            "market cache reader id must not be empty".into(),
        ));
    }
    Ok(())
}
