//! Admission is independent of cache roots and DIFF-session recycling.
use crate::DataError;

pub(super) fn server_refused(error: &DataError) -> bool {
    if let DataError::Session(tqsdk_session::SessionFacadeError::Core(
        tqsdk_core::ContractError::HttpStatus { status, .. },
    )) = error
    {
        return matches!(status, 401 | 403 | 429);
    }
    if matches!(error, DataError::PermissionDenied(_)) {
        return true;
    }
    if let DataError::Session(error) = error
        && error.diagnostic().kind == tqsdk_session::SessionErrorKind::Auth
    {
        return true;
    }
    // HTTP/WS errors currently retain status in their diagnostic string.
    // Match status tokens, never arbitrary occurrences inside a URL or symbol.
    let message = error.to_string().to_ascii_lowercase();
    message
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .any(|word| matches!(word, "401" | "403" | "429"))
        || message.contains("too many requests")
}

#[cfg(all(feature = "live", feature = "services"))]
mod live {
    use crate::{DataError, Result};
    use std::sync::{
        OnceLock,
        atomic::{AtomicBool, Ordering},
    };
    use std::time::Duration;
    use tokio::sync::{Mutex, MutexGuard};
    use tokio::time::Instant;

    #[derive(Default)]
    pub(crate) struct FillAdmission {
        refused: AtomicBool,
        cooldown: std::sync::Mutex<Option<Instant>>,
    }

    impl FillAdmission {
        pub(crate) async fn enter(&self) -> Result<MutexGuard<'static, Instant>> {
            static NEXT_OPEN: OnceLock<Mutex<Instant>> = OnceLock::new();
            self.enter_with(NEXT_OPEN.get_or_init(|| Mutex::new(Instant::now())))
                .await
        }

        async fn enter_with<'a>(
            &self,
            gate: &'a Mutex<Instant>,
        ) -> Result<MutexGuard<'a, Instant>> {
            self.check()?;
            let mut next = gate.lock().await;
            tokio::time::sleep_until(*next).await;
            self.check()?;
            *next = Instant::now() + Duration::from_secs(1);
            Ok(next)
        }

        pub(crate) fn check(&self) -> Result<()> {
            let mut cooldown = self.cooldown.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(deadline) = *cooldown {
                if deadline > Instant::now() {
                    return Err(DataError::PermissionDenied(format!(
                        "remote fill rate limit cooldown: {} seconds remaining",
                        deadline
                            .duration_since(Instant::now())
                            .as_secs()
                            .saturating_add(1),
                    )));
                }
                *cooldown = None;
            }
            if self.refused.load(Ordering::Acquire) {
                return Err(DataError::PermissionDenied(
                    "remote fill stopped after authentication rejection or server rate limit; check credentials and server limits before starting a new fill client".into(),
                ));
            }
            Ok(())
        }

        pub(crate) fn observe<T>(&self, result: &Result<T>) {
            if let Err(DataError::Session(tqsdk_session::SessionFacadeError::Core(
                tqsdk_core::ContractError::HttpStatus {
                    status: 429,
                    retry_after_secs,
                },
            ))) = result
            {
                let delay = Duration::from_secs(retry_after_secs.unwrap_or(60).max(60));
                if let Some(until) = Instant::now().checked_add(delay) {
                    let mut cooldown = self.cooldown.lock().unwrap_or_else(|p| p.into_inner());
                    *cooldown = Some(cooldown.map_or(until, |current| current.max(until)));
                } else {
                    self.refused.store(true, Ordering::Release);
                }
                return;
            }
            if result.as_ref().is_err_and(super::server_refused) {
                self.refused.store(true, Ordering::Release);
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn rate_limit_cooldown_is_shared_and_respects_retry_after() {
            let admission = std::sync::Arc::new(FillAdmission::default());
            let other_client = std::sync::Arc::clone(&admission);
            admission.observe::<()>(&Err(DataError::Session(
                tqsdk_core::ContractError::HttpStatus {
                    status: 429,
                    retry_after_secs: Some(120),
                }
                .into(),
            )));
            assert!(other_client.check().is_err());
            assert!(
                admission
                    .cooldown
                    .lock()
                    .unwrap()
                    .unwrap()
                    .duration_since(Instant::now())
                    > Duration::from_secs(119)
            );
            *admission.cooldown.lock().unwrap() = Some(Instant::now());
            assert!(other_client.check().is_ok());
        }

        #[tokio::test]
        async fn admission_spaces_clients_and_cancelled_wait_does_not_reserve_a_slot() {
            let gate = Mutex::new(Instant::now());
            let first = FillAdmission::default();
            let second = FillAdmission::default();
            let started = Instant::now();
            drop(first.enter_with(&gate).await.unwrap());
            assert!(
                tokio::time::timeout(Duration::from_millis(20), second.enter_with(&gate))
                    .await
                    .is_err()
            );
            drop(second.enter_with(&gate).await.unwrap());
            assert!(started.elapsed() >= Duration::from_secs(1));
            assert!(started.elapsed() < Duration::from_secs(2));
        }

        #[tokio::test]
        async fn refusal_blocks_waiting_and_future_admissions() {
            let admission = FillAdmission::default();
            let gate = Mutex::new(Instant::now() + Duration::from_millis(30));
            let waiting = admission.enter_with(&gate);
            let refusal = async {
                tokio::task::yield_now().await;
                admission.observe::<()>(&Err(DataError::PermissionDenied("denied".into())));
            };
            let (result, ()) = tokio::join!(waiting, refusal);
            assert!(result.is_err());
            assert!(admission.enter_with(&gate).await.is_err());
        }
    }
}

#[cfg(all(feature = "live", feature = "services"))]
pub(super) use live::FillAdmission;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_uses_error_category_and_refuses_authentication_and_limits() {
        use tqsdk_core::ContractError;
        let errors = [
            DataError::Session(ContractError::auth("token request failed").into()),
            DataError::Session(
                ContractError::transport("HTTP error: 429 Too Many Requests").into(),
            ),
            DataError::InvalidResponse("token endpoint timeout in invalid payload".into()),
        ];
        for error in errors {
            assert!(!super::super::is_retryable(&error));
        }
        assert!(super::super::is_retryable(&DataError::Session(
            ContractError::transport("socket reset").into()
        )));
        for attempt in [1, 2] {
            let delay = super::super::retry_delay(attempt);
            let base = std::time::Duration::from_secs(2_u64.pow(attempt as u32));
            assert!(delay >= base && delay <= base + std::time::Duration::from_secs(1));
        }
    }
}
