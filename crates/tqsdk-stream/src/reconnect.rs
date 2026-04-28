#![cfg_attr(not(test), forbid(unsafe_code))]

use std::time::Duration;

use futures::StreamExt;

use crate::{
    CommitStream, Result, SessionReconnectEvent, StreamFacadeError, StreamHealthSnapshot,
    StreamHealthStatus, TqStream,
};

/// Builder for waiting on typed stream reconnect outcomes.
pub struct StreamReconnectMonitor<'a> {
    stream: &'a TqStream,
    timeout: Option<Duration>,
}

/// Typed outcome reported by [`StreamReconnectMonitor`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamReconnectOutcome {
    AlreadyHealthy,
    Recovered,
    Exhausted,
    TimedOut,
    Closed,
}

/// Typed reconnect report for production daemon supervision.
#[derive(Debug, Clone, PartialEq)]
pub struct StreamReconnectReport {
    outcome: StreamReconnectOutcome,
    health: StreamHealthSnapshot,
    last_reconnect: Option<SessionReconnectEvent>,
    observed_commits: u64,
}

impl<'a> StreamReconnectMonitor<'a> {
    pub(crate) fn new(stream: &'a TqStream) -> Self {
        Self {
            stream,
            timeout: None,
        }
    }

    #[must_use]
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub async fn wait(self) -> Result<StreamReconnectReport> {
        let initial = self.stream.health()?;
        if let Some(report) = classify_health(initial, 0) {
            return Ok(report);
        }

        let mut commits = self.stream.commit_stream()?;
        wait_for_reconnect_outcome(self.stream, &mut commits, self.timeout).await
    }
}

impl StreamReconnectReport {
    #[must_use]
    pub fn outcome(&self) -> StreamReconnectOutcome {
        self.outcome
    }

    #[must_use]
    pub fn health(&self) -> &StreamHealthSnapshot {
        &self.health
    }

    #[must_use]
    pub fn last_reconnect(&self) -> Option<&SessionReconnectEvent> {
        self.last_reconnect.as_ref()
    }

    #[must_use]
    pub fn observed_commits(&self) -> u64 {
        self.observed_commits
    }
}

async fn wait_for_reconnect_outcome(
    stream: &TqStream,
    commits: &mut CommitStream,
    timeout: Option<Duration>,
) -> Result<StreamReconnectReport> {
    let deadline = timeout.map(|timeout| tokio::time::Instant::now() + timeout);
    let mut observed_commits = 0;

    loop {
        let item = match deadline {
            Some(deadline) => {
                let Some(remaining) = deadline.checked_duration_since(tokio::time::Instant::now())
                else {
                    return timeout_report(stream, observed_commits);
                };
                match tokio::time::timeout(remaining, commits.next()).await {
                    Ok(item) => item,
                    Err(_) => return timeout_report(stream, observed_commits),
                }
            }
            None => commits.next().await,
        };

        match item {
            Some(Ok(_commit)) => {
                observed_commits += 1;
                let health = stream.health()?;
                if let Some(report) = classify_health(health, observed_commits) {
                    return Ok(report);
                }
            }
            Some(Err(StreamFacadeError::Closed)) | None => {
                let health = stream.health()?;
                return Ok(report(
                    StreamReconnectOutcome::Closed,
                    health,
                    observed_commits,
                ));
            }
            Some(Err(error)) => return Err(error),
        }
    }
}

fn timeout_report(stream: &TqStream, observed_commits: u64) -> Result<StreamReconnectReport> {
    let health = stream.health()?;
    Ok(report(
        StreamReconnectOutcome::TimedOut,
        health,
        observed_commits,
    ))
}

fn classify_health(
    health: StreamHealthSnapshot,
    observed_commits: u64,
) -> Option<StreamReconnectReport> {
    match health.status() {
        StreamHealthStatus::Healthy => Some(report(
            if health.reconnect.is_some() {
                StreamReconnectOutcome::Recovered
            } else {
                StreamReconnectOutcome::AlreadyHealthy
            },
            health,
            observed_commits,
        )),
        StreamHealthStatus::Degraded => Some(report(
            StreamReconnectOutcome::Exhausted,
            health,
            observed_commits,
        )),
        StreamHealthStatus::Closed => Some(report(
            StreamReconnectOutcome::Closed,
            health,
            observed_commits,
        )),
        StreamHealthStatus::Starting | StreamHealthStatus::Recovering => None,
    }
}

fn report(
    outcome: StreamReconnectOutcome,
    health: StreamHealthSnapshot,
    observed_commits: u64,
) -> StreamReconnectReport {
    let last_reconnect = health.reconnect.clone();
    StreamReconnectReport {
        outcome,
        health,
        last_reconnect,
        observed_commits,
    }
}
