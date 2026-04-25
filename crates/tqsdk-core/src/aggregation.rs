use std::collections::BTreeMap;

use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::{
    Result,
    ids::Revision,
    runtime::{RuntimeReader, SnapshotReadGuard},
    state::{CommitResult, UpdateCursor},
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
    pub commit: CommitResult,
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
