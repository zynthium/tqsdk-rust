#![cfg_attr(not(test), forbid(unsafe_code))]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::broadcast;

const IDLE_POLL_BACKOFF: Duration = Duration::from_millis(1);
const ROUTE_DRIVE_BUDGET: Duration = Duration::from_millis(1);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DriverEvent {
    Commit(tqsdk_core::CommitResult),
    Error(tqsdk_session::SessionFacadeError),
    Closed,
}

pub(crate) struct StreamDriver {
    pub(crate) session: tqsdk_session::SessionClient,
    pub(crate) reader: tqsdk_core::RuntimeReader,
    pub(crate) sender: broadcast::Sender<DriverEvent>,
    pub(crate) started: AtomicBool,
    pub(crate) closed: Arc<AtomicBool>,
    pub(crate) task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl StreamDriver {
    pub(crate) fn new(
        session: tqsdk_session::SessionClient,
        reader: tqsdk_core::RuntimeReader,
        capacity: usize,
    ) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self {
            session,
            reader,
            sender,
            started: AtomicBool::new(false),
            closed: Arc::new(AtomicBool::new(false)),
            task: Mutex::new(None),
        }
    }

    pub(crate) fn subscribe(&self) -> broadcast::Receiver<DriverEvent> {
        self.sender.subscribe()
    }

    pub(crate) fn ensure_started(&self) -> crate::error::Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(crate::error::StreamFacadeError::Closed);
        }

        if self
            .started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            if self.closed.load(Ordering::Acquire) {
                return Err(crate::error::StreamFacadeError::Closed);
            }
            return Ok(());
        }

        let runtime = match tokio::runtime::Handle::try_current() {
            Ok(runtime) => runtime,
            Err(_) => {
                self.started.store(false, Ordering::Release);
                return Err(crate::error::StreamFacadeError::InvalidState(
                    "commit_stream requires an active Tokio runtime",
                ));
            }
        };

        let session = self.session.clone();
        let reader = self.reader.clone();
        let cursor = reader.cursor();
        let sender = self.sender.clone();
        let closed = Arc::clone(&self.closed);
        self.closed.store(false, Ordering::Release);

        let task = runtime.spawn(async move {
            run_driver(session, reader, cursor, sender, closed).await;
        });

        let mut slot = self.task.lock().expect("stream driver task mutex poisoned");
        *slot = Some(task);

        Ok(())
    }

    pub(crate) fn abort(&self) {
        let mut slot = self.task.lock().expect("stream driver task mutex poisoned");
        if let Some(task) = slot.take() {
            emit_closed_once(&self.sender, self.closed.as_ref());
            task.abort();
        }
    }
}

fn emit_closed_once(sender: &broadcast::Sender<DriverEvent>, closed: &AtomicBool) {
    if closed
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_ok()
    {
        let _ = sender.send(DriverEvent::Closed);
    }
}

async fn run_driver(
    session: tqsdk_session::SessionClient,
    reader: tqsdk_core::RuntimeReader,
    mut cursor: tqsdk_core::UpdateCursor,
    sender: broadcast::Sender<DriverEvent>,
    closed: Arc<AtomicBool>,
) {
    loop {
        if let Some(commit) = reader.next(&mut cursor) {
            let _ = sender.send(DriverEvent::Commit(commit));
            continue;
        }

        let flushed = match session.flush_outbound().await {
            Ok(flushed) => flushed,
            Err(error) => {
                let _ = sender.send(DriverEvent::Error(error));
                break;
            }
        };
        if flushed {
            continue;
        }

        if let Some(commit) = reader.next(&mut cursor) {
            let _ = sender.send(DriverEvent::Commit(commit));
            continue;
        }

        let drove_pending = match session.drive_pending_once().await {
            Ok(drove_pending) => drove_pending,
            Err(error) => {
                let _ = sender.send(DriverEvent::Error(error));
                break;
            }
        };
        if drove_pending {
            continue;
        }

        if let Some(commit) = reader.next(&mut cursor) {
            let _ = sender.send(DriverEvent::Commit(commit));
            continue;
        }

        let drove_route = match session
            .drive_route_once(Some(tokio::time::Instant::now() + ROUTE_DRIVE_BUDGET))
            .await
        {
            Ok(drove_route) => drove_route,
            Err(error) => {
                let _ = sender.send(DriverEvent::Error(error));
                break;
            }
        };
        if drove_route {
            continue;
        }

        tokio::time::sleep(IDLE_POLL_BACKOFF).await;
    }

    emit_closed_once(&sender, closed.as_ref());
}
