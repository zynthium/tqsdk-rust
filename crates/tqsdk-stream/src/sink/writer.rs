use std::io::Write;
use std::path::Path;

use serde::Serialize;
use tqsdk_core::CommitResult;

use crate::{Result, StreamFacadeError};

use super::journal::StreamCommitJournalRecord;
use super::wal::{StreamSinkWalFsyncPolicy, StreamSinkWalRecord};

struct JsonlRecordWriter {
    writer: std::io::BufWriter<std::fs::File>,
    fsync_policy: StreamSinkWalFsyncPolicy,
}

pub(super) struct StreamSinkWalWriter {
    writer: JsonlRecordWriter,
}

pub(super) struct StreamCommitJournalWriter {
    writer: JsonlRecordWriter,
}

impl JsonlRecordWriter {
    fn open(
        path: Option<&Path>,
        fsync_policy: StreamSinkWalFsyncPolicy,
        open_operation: &'static str,
    ) -> Result<Option<Self>> {
        let Some(path) = path else {
            return Ok(None);
        };
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|error| StreamFacadeError::Io {
                operation: open_operation,
                message: error.to_string(),
            })?;
        Ok(Some(Self {
            writer: std::io::BufWriter::new(file),
            fsync_policy,
        }))
    }

    fn write<T>(
        &mut self,
        record: &T,
        serialize_operation: &'static str,
        write_operation: &'static str,
        fsync_operation: &'static str,
    ) -> Result<()>
    where
        T: Serialize,
    {
        serde_json::to_writer(&mut self.writer, record).map_err(|error| StreamFacadeError::Io {
            operation: serialize_operation,
            message: error.to_string(),
        })?;
        self.writer
            .write_all(b"\n")
            .and_then(|()| self.writer.flush())
            .map_err(|error| StreamFacadeError::Io {
                operation: write_operation,
                message: error.to_string(),
            })?;
        if self.fsync_policy == StreamSinkWalFsyncPolicy::EveryRecord {
            self.writer
                .get_ref()
                .sync_data()
                .map_err(|error| StreamFacadeError::Io {
                    operation: fsync_operation,
                    message: error.to_string(),
                })?;
        }
        Ok(())
    }
}

impl StreamSinkWalWriter {
    pub(super) fn open(
        path: Option<&Path>,
        fsync_policy: StreamSinkWalFsyncPolicy,
    ) -> Result<Option<Self>> {
        JsonlRecordWriter::open(path, fsync_policy, "open stream sink jsonl wal")
            .map(|writer| writer.map(|writer| Self { writer }))
    }

    pub(super) fn write(&mut self, record: &StreamSinkWalRecord) -> Result<()> {
        self.writer.write(
            record,
            "serialize stream sink jsonl wal record",
            "write stream sink jsonl wal record",
            "fsync stream sink jsonl wal record",
        )
    }
}

impl StreamCommitJournalWriter {
    pub(super) fn open(
        path: Option<&Path>,
        fsync_policy: StreamSinkWalFsyncPolicy,
    ) -> Result<Option<Self>> {
        JsonlRecordWriter::open(path, fsync_policy, "open stream commit journal")
            .map(|writer| writer.map(|writer| Self { writer }))
    }

    pub(super) fn write(&mut self, commit: &CommitResult) -> Result<()> {
        self.writer.write(
            &StreamCommitJournalRecord::from_commit(commit),
            "serialize stream commit journal record",
            "write stream commit journal record",
            "fsync stream commit journal record",
        )
    }
}
