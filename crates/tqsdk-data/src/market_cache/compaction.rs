#[derive(Debug, Clone, Default)]
pub struct MarketCacheCompaction {
    min_event_time_ns: Option<i64>,
    max_event_time_ns: Option<i64>,
    symbols: BTreeSet<String>,
    sources: BTreeSet<String>,
    payload_kinds: BTreeSet<MarketCachePayloadKind>,
}

impl MarketCacheCompaction {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn retain_event_time_from(mut self, min_event_time_ns: i64) -> Self {
        self.min_event_time_ns = Some(min_event_time_ns);
        self
    }

    #[must_use]
    pub fn min_event_time_ns(&self) -> Option<i64> {
        self.min_event_time_ns
    }

    #[must_use]
    pub fn retain_event_time_until(mut self, max_event_time_ns: i64) -> Self {
        self.max_event_time_ns = Some(max_event_time_ns);
        self
    }

    #[must_use]
    pub fn retain_symbol(mut self, symbol: impl Into<String>) -> Self {
        self.symbols.insert(symbol.into());
        self
    }

    #[must_use]
    pub fn retain_source(mut self, source: impl Into<String>) -> Self {
        self.sources.insert(source.into());
        self
    }

    #[must_use]
    pub fn retain_payload_kind(mut self, payload_kind: MarketCachePayloadKind) -> Self {
        self.payload_kinds.insert(payload_kind);
        self
    }

    pub fn compact_file(
        &self,
        input_path: impl AsRef<Path>,
        output_path: impl AsRef<Path>,
    ) -> Result<MarketCacheCompactionReport> {
        let input_path = input_path.as_ref();
        let output_path = output_path.as_ref();
        if input_path == output_path {
            return Err(DataError::Validation(
                "market cache compaction input and output paths must differ".into(),
            ));
        }
        let reader = MarketCacheReader::open(input_path)?;
        let mut writer = MarketCacheWriter::create(output_path)?;
        self.compact_reader_to_writer(reader, &mut writer)
    }

    pub fn compact_file_in_place(
        &self,
        cache_path: impl AsRef<Path>,
        staging_path: impl AsRef<Path>,
    ) -> Result<MarketCacheAtomicCompactionReport> {
        let cache_path = cache_path.as_ref().to_path_buf();
        let staging_path = staging_path.as_ref().to_path_buf();
        if cache_path == staging_path {
            return Err(DataError::Validation(
                "market cache compaction cache and staging paths must differ".into(),
            ));
        }
        let compaction = self.compact_file(&cache_path, &staging_path)?;
        std::fs::rename(&staging_path, &cache_path)?;
        Ok(MarketCacheAtomicCompactionReport {
            cache_path,
            staging_path,
            compaction,
        })
    }

    pub fn compact_reader_to_writer<R: BufRead, W: Write>(
        &self,
        reader: MarketCacheReader<R>,
        writer: &mut MarketCacheWriter<W>,
    ) -> Result<MarketCacheCompactionReport> {
        self.validate()?;
        let mut report = MarketCacheCompactionReport {
            read_events: 0,
            written_events: 0,
            dropped_events: 0,
            index: MarketCacheIndex::new(),
        };

        for event in reader {
            let event = event?;
            report.read_events += 1;
            if self.retains(&event) {
                writer.write_event(&event)?;
                report.index.add_event(&event);
                report.written_events += 1;
            } else {
                report.dropped_events += 1;
            }
        }
        writer.flush()?;
        Ok(report)
    }

    fn validate(&self) -> Result<()> {
        if self
            .min_event_time_ns
            .into_iter()
            .chain(self.max_event_time_ns)
            .any(|time| time < 0)
        {
            return Err(DataError::Validation(
                "market cache compaction event time bounds must be non-negative".into(),
            ));
        }
        if matches!(
            (self.min_event_time_ns, self.max_event_time_ns),
            (Some(min), Some(max)) if min > max
        ) {
            return Err(DataError::Validation(
                "market cache compaction min event time exceeds max event time".into(),
            ));
        }
        if self
            .symbols
            .iter()
            .chain(self.sources.iter())
            .any(|value| value.trim().is_empty())
        {
            return Err(DataError::Validation(
                "market cache compaction filters must not be empty".into(),
            ));
        }
        Ok(())
    }

    fn has_partition_filters(&self) -> bool {
        !self.symbols.is_empty() || !self.sources.is_empty() || !self.payload_kinds.is_empty()
    }

    fn with_effective_min_event_time_ns(&self, floor_event_time_ns: Option<i64>) -> Self {
        let mut policy = self.clone();
        if let Some(floor_event_time_ns) = floor_event_time_ns {
            policy.min_event_time_ns = Some(
                policy
                    .min_event_time_ns
                    .map_or(floor_event_time_ns, |min| min.min(floor_event_time_ns)),
            );
        }
        policy
    }

    fn retains(&self, event: &MarketCacheEvent) -> bool {
        if self
            .min_event_time_ns
            .is_some_and(|min| event.event_time_ns() < min)
        {
            return false;
        }
        if self
            .max_event_time_ns
            .is_some_and(|max| event.event_time_ns() > max)
        {
            return false;
        }
        if !self.symbols.is_empty() && !self.symbols.contains(&event.symbol) {
            return false;
        }
        if !self.sources.is_empty() && !self.sources.contains(&event.source) {
            return false;
        }
        if !self.payload_kinds.is_empty() && !self.payload_kinds.contains(&event.payload_kind()) {
            return false;
        }
        true
    }
}

#[derive(Debug, Clone)]
pub struct MarketCacheCompactionReport {
    pub read_events: usize,
    pub written_events: usize,
    pub dropped_events: usize,
    pub index: MarketCacheIndex,
}

#[derive(Debug, Clone)]
pub struct MarketCacheAtomicCompactionReport {
    pub cache_path: PathBuf,
    pub staging_path: PathBuf,
    pub compaction: MarketCacheCompactionReport,
}

#[derive(Debug, Clone)]
pub struct MarketCacheCompactionOwnership {
    cache_path: PathBuf,
    staging_path: PathBuf,
    reader_manifest_path: Option<PathBuf>,
    policy: MarketCacheCompaction,
}

impl MarketCacheCompactionOwnership {
    #[must_use]
    pub fn new(cache_path: impl AsRef<Path>) -> Self {
        let cache_path = cache_path.as_ref().to_path_buf();
        let staging_path = path_with_suffix(&cache_path, ".compact");
        Self {
            cache_path,
            staging_path,
            reader_manifest_path: None,
            policy: MarketCacheCompaction::new(),
        }
    }

    #[must_use]
    pub fn staging_path(mut self, staging_path: impl AsRef<Path>) -> Self {
        self.staging_path = staging_path.as_ref().to_path_buf();
        self
    }

    #[must_use]
    pub fn reader_manifest_path(mut self, reader_manifest_path: impl AsRef<Path>) -> Self {
        self.reader_manifest_path = Some(reader_manifest_path.as_ref().to_path_buf());
        self
    }

    #[must_use]
    pub fn policy(mut self, policy: MarketCacheCompaction) -> Self {
        self.policy = policy;
        self
    }

    pub fn compact(
        &self,
        writer_lease: &mut MarketCacheWriterLease,
    ) -> Result<MarketCacheCompactionOwnershipReport> {
        if self.cache_path == self.staging_path {
            return Err(DataError::Validation(
                "market cache compaction cache and staging paths must differ".into(),
            ));
        }
        if self.reader_manifest_path.is_some() && self.policy.has_partition_filters() {
            return Err(DataError::Validation(
                "reader-protected market cache compaction does not support source, symbol, or payload filters".into(),
            ));
        }

        writer_lease.renew()?;
        let reader_floor_event_time_ns = match &self.reader_manifest_path {
            Some(path) => {
                MarketCacheReaderManifest::open(path)?.compaction_floor_event_time_ns()?
            }
            None => None,
        };
        let effective_policy = self
            .policy
            .with_effective_min_event_time_ns(reader_floor_event_time_ns);
        let effective_min_event_time_ns = effective_policy.min_event_time_ns();
        let compaction =
            effective_policy.compact_file_in_place(&self.cache_path, &self.staging_path)?;
        writer_lease.renew()?;
        Ok(MarketCacheCompactionOwnershipReport {
            reader_floor_event_time_ns,
            effective_min_event_time_ns,
            compaction,
        })
    }
}

#[derive(Debug, Clone)]
pub struct MarketCacheCompactionOwnershipReport {
    pub reader_floor_event_time_ns: Option<i64>,
    pub effective_min_event_time_ns: Option<i64>,
    pub compaction: MarketCacheAtomicCompactionReport,
}

impl MarketCacheCompactionOwnershipReport {
    #[must_use]
    pub fn reader_protected(&self) -> bool {
        self.reader_floor_event_time_ns.is_some()
    }
}
