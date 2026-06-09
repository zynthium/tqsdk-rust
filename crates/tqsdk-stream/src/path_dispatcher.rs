#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::broadcast;
use tqsdk_core::{SharedCommitResult, StatePath};

use crate::api::CommitStream;
use crate::driver::{DriverEvent, StreamDriver};
use crate::filter::{PathCommitStream, PathMatcher};

#[derive(Debug)]
struct PathSubscriber {
    matcher: PathMatcher,
    sender: broadcast::Sender<DriverEvent>,
}

pub(crate) struct PathDispatcher {
    capacity: usize,
    subscribers: Arc<Mutex<PathSubscriberRegistry>>,
    started: AtomicBool,
    closed: Arc<AtomicBool>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl PathDispatcher {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            subscribers: Arc::new(Mutex::new(PathSubscriberRegistry::default())),
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
        subscribers.push(paths, sender);

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

#[derive(Debug, Default)]
struct PathSubscriberRegistry {
    subscribers: Vec<Option<PathSubscriber>>,
    subscribers_by_root: HashMap<String, Vec<usize>>,
    quote_subscribers_by_symbol: HashMap<String, Vec<usize>>,
    generic_subscribers: Vec<usize>,
}

impl PathSubscriberRegistry {
    fn push(&mut self, paths: Vec<StatePath>, sender: broadcast::Sender<DriverEvent>) {
        let index = PathSubscriberIndex::from_paths(&paths);
        let subscriber_id = self.subscribers.len();
        self.subscribers.push(Some(PathSubscriber {
            matcher: PathMatcher::new(paths),
            sender,
        }));

        if index.match_all {
            self.generic_subscribers.push(subscriber_id);
            return;
        }

        for root in index.roots {
            self.subscribers_by_root
                .entry(root)
                .or_default()
                .push(subscriber_id);
        }
        for symbol in index.quote_symbols {
            self.quote_subscribers_by_symbol
                .entry(symbol)
                .or_default()
                .push(subscriber_id);
        }
    }

    fn notify_matching(&mut self, commit: SharedCommitResult) {
        let mut candidate_ids = self.candidate_ids(&commit);
        candidate_ids.sort_unstable();
        candidate_ids.dedup();
        let mut dead = HashSet::new();

        for subscriber_id in candidate_ids {
            let Some(subscriber) = self.subscribers.get(subscriber_id).and_then(Option::as_ref)
            else {
                continue;
            };
            if subscriber.sender.receiver_count() == 0 {
                dead.insert(subscriber_id);
                continue;
            }
            if subscriber.matcher.is_match(&commit) {
                if subscriber
                    .sender
                    .send(DriverEvent::Commit(commit.clone()))
                    .is_err()
                {
                    dead.insert(subscriber_id);
                }
            }
        }

        self.remove_dead(&dead);
    }

    fn notify_all(&mut self, event: DriverEvent) {
        self.cleanup_dead();
        for subscriber in self.subscribers.iter().filter_map(Option::as_ref) {
            let _ = subscriber.sender.send(event.clone());
        }
    }

    fn candidate_ids(&self, commit: &SharedCommitResult) -> Vec<usize> {
        let mut seen = HashSet::new();
        let mut candidate_ids = Vec::new();

        for subscriber_id in &self.generic_subscribers {
            push_candidate(&mut seen, &mut candidate_ids, *subscriber_id);
        }

        for changed in &commit.changes.path_hits {
            let segments = changed.segments();
            let Some(root) = segments.first() else {
                continue;
            };

            if let Some(subscribers) = self.subscribers_by_root.get(root.as_str()) {
                for subscriber_id in subscribers {
                    push_candidate(&mut seen, &mut candidate_ids, *subscriber_id);
                }
            }

            if root == "quotes"
                && let Some(symbol) = segments.get(1)
                && let Some(subscribers) = self.quote_subscribers_by_symbol.get(symbol.as_str())
            {
                for subscriber_id in subscribers {
                    push_candidate(&mut seen, &mut candidate_ids, *subscriber_id);
                }
            }
        }

        candidate_ids
    }

    fn cleanup_dead(&mut self) {
        let dead = self
            .subscribers
            .iter()
            .enumerate()
            .filter_map(|(subscriber_id, subscriber)| {
                subscriber
                    .as_ref()
                    .filter(|subscriber| subscriber.sender.receiver_count() == 0)
                    .map(|_| subscriber_id)
            })
            .collect::<HashSet<_>>();
        if dead.is_empty() {
            return;
        }

        self.remove_dead(&dead);
    }

    fn remove_dead(&mut self, dead: &HashSet<usize>) {
        if dead.is_empty() {
            return;
        }

        for subscriber_id in dead {
            self.subscribers[*subscriber_id] = None;
        }
        remove_dead_ids(&mut self.generic_subscribers, &dead);
        retain_live_index(&mut self.subscribers_by_root, &dead);
        retain_live_index(&mut self.quote_subscribers_by_symbol, &dead);
    }
}

#[derive(Debug, Default)]
struct PathSubscriberIndex {
    match_all: bool,
    roots: Vec<String>,
    quote_symbols: Vec<String>,
}

impl PathSubscriberIndex {
    fn from_paths(paths: &[StatePath]) -> Self {
        if paths.is_empty() {
            return Self {
                match_all: true,
                ..Self::default()
            };
        }

        let mut index = Self::default();
        for path in paths {
            let segments = path.segments();
            let Some(root) = segments.first() else {
                index.match_all = true;
                continue;
            };

            if root == "quotes" && segments.len() >= 2 {
                push_unique(&mut index.quote_symbols, segments[1].clone());
            } else {
                push_unique(&mut index.roots, root.clone());
            }
        }

        if index.match_all {
            index.roots.clear();
            index.quote_symbols.clear();
        }

        index
    }
}

async fn run_path_dispatcher(
    mut receiver: broadcast::Receiver<DriverEvent>,
    subscribers: Arc<Mutex<PathSubscriberRegistry>>,
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

fn notify_matching(subscribers: &Arc<Mutex<PathSubscriberRegistry>>, commit: SharedCommitResult) {
    let mut subscribers = subscribers
        .lock()
        .expect("path dispatcher subscribers mutex poisoned");
    subscribers.notify_matching(commit);
}

fn notify_all(subscribers: &Arc<Mutex<PathSubscriberRegistry>>, event: DriverEvent) {
    let mut subscribers = subscribers
        .lock()
        .expect("path dispatcher subscribers mutex poisoned");
    subscribers.notify_all(event);
}

fn push_candidate(seen: &mut HashSet<usize>, candidate_ids: &mut Vec<usize>, subscriber_id: usize) {
    if seen.insert(subscriber_id) {
        candidate_ids.push(subscriber_id);
    }
}

fn push_unique(values: &mut Vec<String>, value: String) {
    if !values.contains(&value) {
        values.push(value);
    }
}

fn remove_dead_ids(ids: &mut Vec<usize>, dead: &HashSet<usize>) {
    ids.retain(|subscriber_id| !dead.contains(subscriber_id));
}

fn retain_live_index(index: &mut HashMap<String, Vec<usize>>, dead: &HashSet<usize>) {
    for ids in index.values_mut() {
        remove_dead_ids(ids, dead);
    }
    index.retain(|_, ids| !ids.is_empty());
}
