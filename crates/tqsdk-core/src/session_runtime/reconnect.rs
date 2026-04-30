use std::time::Duration;

use serde_json::json;

use crate::{
    Result,
    events::{InternalEvent, RuntimeInput},
    ids::CommandId,
    runtime::Runtime,
    state::{CommitScope, SharedCommitResult},
    transport::{SessionConfig, SessionPhase},
};

use super::{SessionRun, SessionRuntime, SessionRuntimeDeps};

pub(super) struct RecoveryOutcome {
    pub(super) run: SessionRun,
    pub(super) commits: Vec<SharedCommitResult>,
}

impl SessionRuntime {
    pub(super) async fn recover_with_policy(
        &self,
        route_label: &str,
        reason: &'static str,
        caused_by: Vec<CommandId>,
        deps: SessionRuntimeDeps<'_>,
    ) -> Result<RecoveryOutcome> {
        let mut commits = Vec::new();
        let max_attempts = deps.config.reconnect.max_attempts.unwrap_or(1).max(1);
        let mut last_error = None;

        for attempt in 1..=max_attempts {
            let scheduled_backoff_ms = reconnect_backoff_ms(deps.config, attempt);
            if let Some(commit) = self.handle.record_session_reconnect(
                attempt,
                scheduled_backoff_ms,
                deps.config.reconnect.max_attempts,
                false,
                Some(json!({
                    "route": route_label,
                    "reason": reason,
                })),
                caused_by.clone(),
            )? {
                commits.push(commit);
            }
            sleep_reconnect_backoff(scheduled_backoff_ms).await?;

            match self.recover_internal(deps, false).await {
                Ok(recovery) => {
                    commits.extend(recovery.commits);
                    return Ok(RecoveryOutcome {
                        run: recovery.run,
                        commits,
                    });
                }
                Err(err) => {
                    if let Some(commit) = self.handle.ingest(
                        RuntimeInput::Internal(InternalEvent {
                            label: "session-recovery-error",
                            payload: Some(json!({
                                "route": route_label,
                                "reason": reason,
                                "attempt": attempt,
                                "message": err.to_string(),
                            })),
                        }),
                        caused_by.clone(),
                        CommitScope::SessionTransition,
                    )? {
                        commits.push(commit);
                    }
                    last_error = Some((attempt, scheduled_backoff_ms, err));
                }
            }
        }

        let Some((attempt, scheduled_backoff_ms, err)) = last_error else {
            return Err(crate::ContractError::validation(
                "reconnect policy exhausted without any recovery attempts",
            ));
        };

        if let Some(commit) = self.handle.record_session_reconnect(
            attempt,
            scheduled_backoff_ms,
            deps.config.reconnect.max_attempts,
            true,
            Some(json!({
                "route": route_label,
                "reason": reason,
                "message": err.to_string(),
            })),
            caused_by.clone(),
        )? {
            commits.push(commit);
        }

        if let Some(commit) = self.handle.record_session_phase(
            SessionPhase::Closed,
            Some(json!({
                "route": route_label,
                "reason": "reconnect-exhausted",
                "attempt": attempt,
                "message": err.to_string(),
            })),
            caused_by,
        )? {
            commits.push(commit);
        }

        Err(err)
    }

    pub(super) async fn recover_internal(
        &self,
        deps: SessionRuntimeDeps<'_>,
        record_reconnecting: bool,
    ) -> Result<RecoveryOutcome> {
        let mut commits = Vec::new();

        if record_reconnecting
            && let Some(commit) =
                self.handle
                    .record_session_phase(SessionPhase::Reconnecting, None, vec![])?
        {
            commits.push(commit);
        }

        let bootstrap = self
            .bootstrap
            .establish_with_resolver(deps.auth, deps.resolver, deps.config, deps.adapters)
            .await?;
        let connected = self
            .connect_if_needed(&bootstrap, deps.connector, None)
            .await?;

        if let Some(commit) =
            self.handle
                .record_session_phase(SessionPhase::Resyncing, None, vec![])?
        {
            commits.push(commit);
        }
        if let Some(commit) = self.handle.record_session_resync(&bootstrap, vec![])? {
            commits.push(commit);
        }

        for command in self.handle.recovery_commands() {
            self.handle.submit(command).await?;
        }

        Ok(RecoveryOutcome {
            run: SessionRun {
                bootstrap,
                connected,
            },
            commits,
        })
    }
}

fn reconnect_backoff_ms(config: &SessionConfig, attempt: u32) -> u64 {
    let base = config.reconnect.initial_backoff.as_millis();
    let cap = config.reconnect.max_backoff.as_millis();
    let shift = attempt.saturating_sub(1).min(63);
    let multiplier = 1u128 << shift;
    let scheduled = base.saturating_mul(multiplier);
    scheduled.min(cap).min(u64::MAX as u128) as u64
}

async fn sleep_reconnect_backoff(scheduled_backoff_ms: u64) -> Result<()> {
    if scheduled_backoff_ms == 0 {
        return Ok(());
    }

    tokio::runtime::Handle::try_current().map_err(|_| {
        crate::ContractError::validation(
            "session reconnect backoff requires an active Tokio runtime",
        )
    })?;
    tokio::time::sleep(Duration::from_millis(scheduled_backoff_ms)).await;
    Ok(())
}
