#![cfg_attr(not(test), forbid(unsafe_code))]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::broadcast;
use tqsdk_core::{SharedCommitResult, StatePath};

use crate::api::CommitStream;
use crate::driver::{DriverEvent, StreamDriver};
use crate::filter::{PathCommitStream, matches_path_filters};

#[derive(Debug)]
struct PathSubscriber {
    paths: Vec<StatePath>,
    sender: broadcast::Sender<DriverEvent>,
}

pub(crate) struct PathDispatcher {
    capacity: usize,
    subscribers: Arc<Mutex<Vec<PathSubscriber>>>,
    started: AtomicBool,
    closed: Arc<AtomicBool>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl PathDispatcher {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            subscribers: Arc::new(Mutex::new(Vec::new())),
            started: AtomicBool::new(false),
            closed: Arc::new(AtomicBool::new(false)),
            task: Mutex::new(None),
        }
    }

    pub(crate) fn subscribe(
        &self,
        driver: &StreamDriver,
        paths: Vec<StatePath>,
    ) -> crate::error::Result<PathCommitStream> {
        self.ensure_started(driver)?;

        let (sender, receiver) = broadcast::channel(self.capacity);
        let mut subscribers = self
            .subscribers
            .lock()
            .expect("path dispatcher subscribers mutex poisoned");
        subscribers.push(PathSubscriber { paths, sender });

        Ok(PathCommitStream::new(
            CommitStream::new(receiver),
            Vec::new(),
        ))
    }

    pub(crate) fn abort(&self) {
        self.closed.store(true, Ordering::Release);
        let mut task = self
            .task
            .lock()
            .expect("path dispatcher task mutex poisoned");
        if let Some(task) = task.take() {
            task.abort();
        }
        notify_all(&self.subscribers, DriverEvent::Closed);
    }

    fn ensure_started(&self, driver: &StreamDriver) -> crate::error::Result<()> {
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

        let receiver = driver.subscribe();
        if let Err(error) = driver.ensure_started() {
            self.started.store(false, Ordering::Release);
            return Err(error);
        }

        let subscribers = Arc::clone(&self.subscribers);
        let closed = Arc::clone(&self.closed);
        let task = runtime.spawn(async move {
            run_path_dispatcher(receiver, subscribers, closed).await;
        });

        let mut slot = self
            .task
            .lock()
            .expect("path dispatcher task mutex poisoned");
        *slot = Some(task);

        Ok(())
    }
}

async fn run_path_dispatcher(
    mut receiver: broadcast::Receiver<DriverEvent>,
    subscribers: Arc<Mutex<Vec<PathSubscriber>>>,
    closed: Arc<AtomicBool>,
) {
    loop {
        match receiver.recv().await {
            Ok(DriverEvent::Commit(commit)) => {
                notify_matching(&subscribers, commit);
            }
            Ok(DriverEvent::Error(error)) => {
                notify_all(&subscribers, DriverEvent::Error(error));
            }
            Ok(DriverEvent::Lagged(skipped)) => {
                notify_all(&subscribers, DriverEvent::Lagged(skipped));
            }
            Ok(DriverEvent::Closed) => {
                notify_all(&subscribers, DriverEvent::Closed);
                break;
            }
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                notify_all(&subscribers, DriverEvent::Lagged(skipped));
            }
            Err(broadcast::error::RecvError::Closed) => {
                notify_all(&subscribers, DriverEvent::Closed);
                break;
            }
        }
    }

    closed.store(true, Ordering::Release);
}

fn notify_matching(subscribers: &Arc<Mutex<Vec<PathSubscriber>>>, commit: SharedCommitResult) {
    notify(
        subscribers,
        |paths| matches_path_filters(paths, &commit),
        || DriverEvent::Commit(commit.clone()),
    );
}

fn notify_all(subscribers: &Arc<Mutex<Vec<PathSubscriber>>>, event: DriverEvent) {
    notify(subscribers, |_| true, || event.clone());
}

fn notify(
    subscribers: &Arc<Mutex<Vec<PathSubscriber>>>,
    predicate: impl Fn(&[StatePath]) -> bool,
    event: impl Fn() -> DriverEvent,
) {
    let mut subscribers = subscribers
        .lock()
        .expect("path dispatcher subscribers mutex poisoned");
    subscribers.retain(|subscriber| subscriber.sender.receiver_count() > 0);

    for subscriber in subscribers.iter() {
        if predicate(&subscriber.paths) {
            let _ = subscriber.sender.send(event());
        }
    }
}
