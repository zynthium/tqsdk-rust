#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct MarketCacheIndexKey {
    pub source: String,
    pub symbol: String,
    pub payload_kind: MarketCachePayloadKind,
}

impl MarketCacheIndexKey {
    #[must_use]
    pub fn new(
        source: impl Into<String>,
        symbol: impl Into<String>,
        payload_kind: MarketCachePayloadKind,
    ) -> Self {
        Self {
            source: source.into(),
            symbol: symbol.into(),
            payload_kind,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketCacheIndexEntry {
    pub events: usize,
    pub min_event_time_ns: i64,
    pub max_event_time_ns: i64,
    pub min_received_at_ns: i64,
    pub max_received_at_ns: i64,
}

impl MarketCacheIndexEntry {
    fn new(event: &MarketCacheEvent) -> Self {
        Self {
            events: 1,
            min_event_time_ns: event.event_time_ns(),
            max_event_time_ns: event.event_time_ns(),
            min_received_at_ns: event.received_at_ns,
            max_received_at_ns: event.received_at_ns,
        }
    }

    fn add_event(&mut self, event: &MarketCacheEvent) {
        self.events += 1;
        self.min_event_time_ns = self.min_event_time_ns.min(event.event_time_ns());
        self.max_event_time_ns = self.max_event_time_ns.max(event.event_time_ns());
        self.min_received_at_ns = self.min_received_at_ns.min(event.received_at_ns);
        self.max_received_at_ns = self.max_received_at_ns.max(event.received_at_ns);
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MarketCacheIndex {
    total_events: usize,
    entries: BTreeMap<MarketCacheIndexKey, MarketCacheIndexEntry>,
}

impl MarketCacheIndex {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn from_events<'a, I>(events: I) -> Self
    where
        I: IntoIterator<Item = &'a MarketCacheEvent>,
    {
        let mut index = Self::new();
        for event in events {
            index.add_event(event);
        }
        index
    }

    pub fn from_reader<R: BufRead>(reader: MarketCacheReader<R>) -> Result<Self> {
        let mut index = Self::new();
        for event in reader {
            index.add_event(&event?);
        }
        Ok(index)
    }

    pub fn add_event(&mut self, event: &MarketCacheEvent) {
        self.total_events += 1;
        let key = MarketCacheIndexKey::new(&event.source, &event.symbol, event.payload_kind());
        self.entries
            .entry(key)
            .and_modify(|entry| entry.add_event(event))
            .or_insert_with(|| MarketCacheIndexEntry::new(event));
    }

    #[must_use]
    pub fn total_events(&self) -> usize {
        self.total_events
    }

    #[must_use]
    pub fn entry(
        &self,
        source: &str,
        symbol: &str,
        payload_kind: MarketCachePayloadKind,
    ) -> Option<&MarketCacheIndexEntry> {
        self.entries
            .get(&MarketCacheIndexKey::new(source, symbol, payload_kind))
    }

    pub fn entries(&self) -> impl Iterator<Item = (&MarketCacheIndexKey, &MarketCacheIndexEntry)> {
        self.entries.iter()
    }
}
