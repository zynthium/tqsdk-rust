use std::collections::BTreeMap;

use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::{
    Result,
    ids::Revision,
    runtime::{RuntimeReader, SnapshotReadGuard},
    state::{SharedCommitResult, UpdateCursor},
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StateSourceId(String);

impl StateSourceId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone)]
pub struct AggregatedRuntimeReader {
    sources: Vec<RuntimeReaderSource>,
}

#[derive(Clone)]
struct RuntimeReaderSource {
    source_id: StateSourceId,
    reader: RuntimeReader,
}

impl AggregatedRuntimeReader {
    pub fn new() -> Self {
        Self {
            sources: Vec::new(),
        }
    }

    pub fn insert_source(&mut self, source_id: StateSourceId, reader: RuntimeReader) {
        if let Some(source) = self
            .sources
            .iter_mut()
            .find(|source| source.source_id == source_id)
        {
            source.reader = reader;
            return;
        }

        self.sources.push(RuntimeReaderSource { source_id, reader });
    }

    pub fn remove_source(&mut self, source_id: &StateSourceId) -> Option<RuntimeReader> {
        let index = self
            .sources
            .iter()
            .position(|source| &source.source_id == source_id)?;
        Some(self.sources.remove(index).reader)
    }

    pub fn source_count(&self) -> usize {
        self.sources.len()
    }

    pub fn sources(&self) -> impl Iterator<Item = &StateSourceId> {
        self.sources.iter().map(|source| &source.source_id)
    }

    pub fn read(&self) -> AggregatedSnapshotReadGuard<'_> {
        AggregatedSnapshotReadGuard {
            snapshots: self
                .sources
                .iter()
                .map(|source| AggregatedSnapshotSource {
                    source_id: source.source_id.clone(),
                    snapshot: source.reader.read(),
                })
                .collect(),
        }
    }

    pub fn cursor(&self) -> AggregatedCursor {
        AggregatedCursor {
            cursors: self
                .sources
                .iter()
                .map(|source| (source.source_id.clone(), source.reader.cursor()))
                .collect(),
        }
    }

    pub fn next(&self, cursor: &mut AggregatedCursor) -> Option<AggregatedCommit> {
        for source in &self.sources {
            let source_cursor = cursor
                .cursors
                .entry(source.source_id.clone())
                .or_insert_with(|| source.reader.cursor());
            if let Some(commit) = source.reader.next(source_cursor) {
                return Some(AggregatedCommit {
                    source_id: source.source_id.clone(),
                    commit,
                });
            }
        }

        None
    }
}

impl Default for AggregatedRuntimeReader {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregatedCommit {
    pub source_id: StateSourceId,
    pub commit: SharedCommitResult,
}

#[derive(Debug, Clone)]
pub struct AggregatedCursor {
    cursors: BTreeMap<StateSourceId, UpdateCursor>,
}

impl AggregatedCursor {
    pub fn source_cursor(&self, source_id: &StateSourceId) -> Option<&UpdateCursor> {
        self.cursors.get(source_id)
    }
}

pub struct AggregatedSnapshotReadGuard<'a> {
    snapshots: Vec<AggregatedSnapshotSource<'a>>,
}

struct AggregatedSnapshotSource<'a> {
    source_id: StateSourceId,
    snapshot: SnapshotReadGuard<'a>,
}

impl AggregatedSnapshotReadGuard<'_> {
    pub fn source_count(&self) -> usize {
        self.snapshots.len()
    }

    pub fn sources(&self) -> impl Iterator<Item = &StateSourceId> {
        self.snapshots.iter().map(|source| &source.source_id)
    }

    pub fn revision(&self, source_id: &StateSourceId) -> Option<Revision> {
        self.source(source_id).map(SnapshotReadGuard::revision)
    }

    pub fn get<I, S>(&self, source_id: &StateSourceId, path: I) -> Option<&Value>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.source(source_id)?.get(path)
    }

    pub fn get_path(&self, source_id: &StateSourceId, path: &[&str]) -> Option<&Value> {
        self.source(source_id)?.get_path(path)
    }

    pub fn decode<T, I, S>(&self, source_id: &StateSourceId, path: I) -> Result<Option<T>>
    where
        T: DeserializeOwned,
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let Some(snapshot) = self.source(source_id) else {
            return Ok(None);
        };
        snapshot.decode(path)
    }

    pub fn decode_path<T>(&self, source_id: &StateSourceId, path: &[&str]) -> Result<Option<T>>
    where
        T: DeserializeOwned,
    {
        let Some(snapshot) = self.source(source_id) else {
            return Ok(None);
        };
        snapshot.decode_path(path)
    }

    fn source(&self, source_id: &StateSourceId) -> Option<&SnapshotReadGuard<'_>> {
        self.snapshots
            .iter()
            .find(|source| &source.source_id == source_id)
            .map(|source| &source.snapshot)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::{
        AdapterRegistry, CommitScope, InputPayload, IoEvent, ProtocolDomain, RuntimeHandle,
        RuntimeInput,
    };

    use super::{AggregatedRuntimeReader, StateSourceId};

    #[test]
    fn aggregated_reader_keeps_two_source_snapshots_and_commits_separate() {
        let primary = runtime_with_default_adapters();
        let backup = runtime_with_default_adapters();

        ingest_quote(&primary, 601.0);
        ingest_quote(&backup, 701.0);

        let mut aggregate = AggregatedRuntimeReader::new();
        let primary_id = StateSourceId::new("primary");
        let backup_id = StateSourceId::new("backup");
        aggregate.insert_source(primary_id.clone(), primary.reader());
        aggregate.insert_source(backup_id.clone(), backup.reader());
        assert_eq!(aggregate.source_count(), 2);
        assert_eq!(
            aggregate
                .sources()
                .map(StateSourceId::as_str)
                .collect::<Vec<_>>(),
            vec!["primary", "backup"]
        );

        let read = aggregate.read();
        assert_eq!(read.source_count(), 2);
        assert_eq!(
            read.sources()
                .map(StateSourceId::as_str)
                .collect::<Vec<_>>(),
            vec!["primary", "backup"]
        );
        assert_eq!(read.revision(&primary_id).unwrap().get(), 1);
        assert_eq!(read.revision(&backup_id).unwrap().get(), 1);
        assert_eq!(
            read.get(&primary_id, ["quotes", "SHFE.au2602", "last_price"]),
            Some(&json!(601.0))
        );
        assert_eq!(
            read.get(&backup_id, ["quotes", "SHFE.au2602", "last_price"]),
            Some(&json!(701.0))
        );
        assert_eq!(
            read.get_path(&primary_id, &["quotes", "SHFE.au2602", "last_price"]),
            Some(&json!(601.0))
        );
        assert_eq!(
            read.decode::<f64, _, _>(&backup_id, ["quotes", "SHFE.au2602", "last_price"])
                .unwrap(),
            Some(701.0)
        );
        assert_eq!(
            read.decode_path::<f64>(&backup_id, &["quotes", "SHFE.au2602", "last_price"])
                .unwrap(),
            Some(701.0)
        );
        drop(read);

        let mut cursor = aggregate.cursor();
        assert!(cursor.source_cursor(&primary_id).is_some());
        assert!(cursor.source_cursor(&backup_id).is_some());
        ingest_quote(&primary, 602.0);
        ingest_quote(&backup, 702.0);

        let first = aggregate
            .next(&mut cursor)
            .expect("primary update should be visible through aggregate cursor");
        let second = aggregate
            .next(&mut cursor)
            .expect("backup update should be visible through aggregate cursor");
        assert_eq!(first.source_id.as_str(), "primary");
        assert_eq!(first.commit.revision.get(), 2);
        assert_eq!(second.source_id.as_str(), "backup");
        assert_eq!(second.commit.revision.get(), 2);
        assert!(
            aggregate.next(&mut cursor).is_none(),
            "aggregate cursor should advance each source independently"
        );
        assert!(aggregate.remove_source(&backup_id).is_some());
        assert_eq!(aggregate.source_count(), 1);
    }

    fn runtime_with_default_adapters() -> RuntimeHandle {
        let mut registry = AdapterRegistry::new();
        registry.register_default_adapters();
        RuntimeHandle::with_adapters(registry)
    }

    fn ingest_quote(handle: &RuntimeHandle, last_price: f64) {
        handle
            .ingest(
                RuntimeInput::Io(IoEvent {
                    route: "market.shared".to_string(),
                    domains: vec![ProtocolDomain::Market],
                    payload: InputPayload::Json(json!({
                        "aid": "rtn_data",
                        "data": [{
                            "quotes": {
                                "SHFE.au2602": {
                                    "last_price": last_price
                                }
                            }
                        }]
                    })),
                }),
                Vec::new(),
                CommitScope::RealtimeUpdate,
            )
            .unwrap()
            .expect("quote update should publish a commit");
    }
}
