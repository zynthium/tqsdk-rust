use std::io::BufRead;
use std::path::Path;

use serde::{Deserialize, Serialize};
use tqsdk_core::{
    ChangeSet, CommandId, CommitResult, CommitScope, ProtocolDomain, Revision, StatePath,
};

use crate::{Result, StreamFacadeError};

use super::runtime::CommitSink;

/// JSONL commit journal used to replay commit metadata into a sink.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StreamCommitJournal {
    after_revision: Option<u64>,
}

/// Stable JSONL record for commit journal replay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamCommitJournalRecord {
    revision: u64,
    scope: StreamCommitJournalScope,
    domains: Vec<StreamCommitJournalDomain>,
    paths: Vec<Vec<String>>,
    caused_by: Vec<u64>,
}

/// Commit scope encoded in the JSONL commit journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamCommitJournalScope {
    InitialReady,
    RealtimeUpdate,
    ResyncRecovery,
    ReplayStep,
    QueryRefresh,
    SessionTransition,
}

/// Protocol domain encoded in the JSONL commit journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamCommitJournalDomain {
    System,
    Market,
    Trade,
    Replay,
    Query,
    Schema,
}

/// Report returned after replaying a JSONL commit journal.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StreamCommitJournalReplayReport {
    replayed_commits: u64,
    last_replayed_revision: Option<u64>,
}

impl StreamCommitJournal {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn after_revision(mut self, revision: u64) -> Self {
        self.after_revision = Some(revision);
        self
    }

    pub fn read_jsonl(self, path: impl AsRef<Path>) -> Result<Vec<StreamCommitJournalRecord>> {
        Ok(read_jsonl_commit_journal(path.as_ref())?
            .into_iter()
            .filter(|record| should_replay_journal_revision(record.revision, self.after_revision))
            .collect())
    }

    pub async fn replay_jsonl<S>(
        self,
        path: impl AsRef<Path>,
        sink: S,
    ) -> Result<StreamCommitJournalReplayReport>
    where
        S: CommitSink,
    {
        replay_jsonl_commit_journal(path.as_ref(), self.after_revision, sink).await
    }
}

impl StreamCommitJournalRecord {
    #[must_use]
    pub fn from_commit(commit: &CommitResult) -> Self {
        Self {
            revision: commit.revision.get(),
            scope: StreamCommitJournalScope::from_core(commit.scope),
            domains: commit
                .domains
                .iter()
                .copied()
                .map(StreamCommitJournalDomain::from_core)
                .collect(),
            paths: commit
                .changes
                .path_hits
                .iter()
                .map(|path| path.segments().to_vec())
                .collect(),
            caused_by: commit.caused_by.iter().map(|id| id.get()).collect(),
        }
    }

    #[must_use]
    pub fn revision(&self) -> u64 {
        self.revision
    }

    #[must_use]
    pub fn scope(&self) -> StreamCommitJournalScope {
        self.scope
    }

    #[must_use]
    pub fn domains(&self) -> &[StreamCommitJournalDomain] {
        &self.domains
    }

    #[must_use]
    pub fn paths(&self) -> &[Vec<String>] {
        &self.paths
    }

    #[must_use]
    pub fn caused_by(&self) -> &[u64] {
        &self.caused_by
    }

    #[must_use]
    pub fn to_commit(&self) -> CommitResult {
        CommitResult::new(
            Revision::new(self.revision),
            self.domains.iter().map(|domain| domain.to_core()).collect(),
            ChangeSet {
                path_hits: self
                    .paths
                    .iter()
                    .map(|segments| StatePath::new(segments.clone()))
                    .collect(),
                object_hits: Vec::new(),
                field_hits: Vec::new(),
            },
            self.caused_by.iter().copied().map(CommandId::new).collect(),
            self.scope.to_core(),
        )
    }
}

impl StreamCommitJournalScope {
    fn from_core(scope: CommitScope) -> Self {
        match scope {
            CommitScope::InitialReady => Self::InitialReady,
            CommitScope::RealtimeUpdate => Self::RealtimeUpdate,
            CommitScope::ResyncRecovery => Self::ResyncRecovery,
            CommitScope::ReplayStep => Self::ReplayStep,
            CommitScope::QueryRefresh => Self::QueryRefresh,
            CommitScope::SessionTransition => Self::SessionTransition,
        }
    }

    fn to_core(self) -> CommitScope {
        match self {
            Self::InitialReady => CommitScope::InitialReady,
            Self::RealtimeUpdate => CommitScope::RealtimeUpdate,
            Self::ResyncRecovery => CommitScope::ResyncRecovery,
            Self::ReplayStep => CommitScope::ReplayStep,
            Self::QueryRefresh => CommitScope::QueryRefresh,
            Self::SessionTransition => CommitScope::SessionTransition,
        }
    }
}

impl StreamCommitJournalDomain {
    fn from_core(domain: ProtocolDomain) -> Self {
        match domain {
            ProtocolDomain::System => Self::System,
            ProtocolDomain::Market => Self::Market,
            ProtocolDomain::Trade => Self::Trade,
            ProtocolDomain::Replay => Self::Replay,
            ProtocolDomain::Query => Self::Query,
            ProtocolDomain::Schema => Self::Schema,
        }
    }

    fn to_core(self) -> ProtocolDomain {
        match self {
            Self::System => ProtocolDomain::System,
            Self::Market => ProtocolDomain::Market,
            Self::Trade => ProtocolDomain::Trade,
            Self::Replay => ProtocolDomain::Replay,
            Self::Query => ProtocolDomain::Query,
            Self::Schema => ProtocolDomain::Schema,
        }
    }
}

impl StreamCommitJournalReplayReport {
    #[must_use]
    pub fn replayed_commits(&self) -> u64 {
        self.replayed_commits
    }

    #[must_use]
    pub fn last_replayed_revision(&self) -> Option<u64> {
        self.last_replayed_revision
    }
}

fn read_jsonl_commit_journal(path: &Path) -> Result<Vec<StreamCommitJournalRecord>> {
    let input = std::fs::File::open(path).map_err(|error| StreamFacadeError::Io {
        operation: "open stream commit journal",
        message: error.to_string(),
    })?;
    let mut records = Vec::new();

    for line in std::io::BufReader::new(input).lines() {
        let line = line.map_err(|error| StreamFacadeError::Io {
            operation: "read stream commit journal",
            message: error.to_string(),
        })?;
        if line.trim().is_empty() {
            continue;
        }
        records.push(
            serde_json::from_str(&line).map_err(|error| StreamFacadeError::Io {
                operation: "parse stream commit journal record",
                message: error.to_string(),
            })?,
        );
    }

    Ok(records)
}

async fn replay_jsonl_commit_journal<S>(
    path: &Path,
    after_revision: Option<u64>,
    mut sink: S,
) -> Result<StreamCommitJournalReplayReport>
where
    S: CommitSink,
{
    let mut replayed_commits = 0;
    let mut last_replayed_revision = None;

    for record in read_jsonl_commit_journal(path)? {
        if !should_replay_journal_revision(record.revision, after_revision) {
            continue;
        }
        let revision = record.revision;
        sink.handle_commit(record.to_commit().into()).await?;
        replayed_commits += 1;
        last_replayed_revision = Some(revision);
    }

    sink.flush().await?;

    Ok(StreamCommitJournalReplayReport {
        replayed_commits,
        last_replayed_revision,
    })
}

fn should_replay_journal_revision(revision: u64, after_revision: Option<u64>) -> bool {
    after_revision.is_none_or(|checkpoint| revision > checkpoint)
}
