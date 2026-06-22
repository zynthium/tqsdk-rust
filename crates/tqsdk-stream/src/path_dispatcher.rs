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
    candidate_ids: Vec<usize>,
    candidate_seen: HashSet<usize>,
    dead_ids: Vec<usize>,
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
        self.collect_candidate_ids(&commit);
        self.candidate_ids.sort_unstable();
        self.dead_ids.clear();

        for index in 0..self.candidate_ids.len() {
            let subscriber_id = self.candidate_ids[index];
            let Some(subscriber) = self.subscribers.get(subscriber_id).and_then(Option::as_ref)
            else {
                continue;
            };
            if subscriber.sender.receiver_count() == 0 {
                self.dead_ids.push(subscriber_id);
                continue;
            }
            if subscriber.matcher.is_match(&commit)
                && subscriber
                    .sender
                    .send(DriverEvent::Commit(commit.clone()))
                    .is_err()
            {
                self.dead_ids.push(subscriber_id);
            }
        }

        self.remove_dead_from_scratch();
    }

    fn notify_all(&mut self, event: DriverEvent) {
        self.cleanup_dead();
        for subscriber in self.subscribers.iter().filter_map(Option::as_ref) {
            let _ = subscriber.sender.send(event.clone());
        }
    }

    fn collect_candidate_ids(&mut self, commit: &SharedCommitResult) {
        self.candidate_seen.clear();
        self.candidate_ids.clear();

        let generic_subscribers = &self.generic_subscribers;
        let subscribers_by_root = &self.subscribers_by_root;
        let quote_subscribers_by_symbol = &self.quote_subscribers_by_symbol;
        let candidate_seen = &mut self.candidate_seen;
        let candidate_ids = &mut self.candidate_ids;

        for subscriber_id in generic_subscribers {
            push_candidate(candidate_seen, candidate_ids, *subscriber_id);
        }

        for changed in &commit.changes.path_hits {
            let segments = changed.segments();
            let Some(root) = segments.first() else {
                continue;
            };

            if let Some(subscribers) = subscribers_by_root.get(root.as_str()) {
                for subscriber_id in subscribers {
                    push_candidate(candidate_seen, candidate_ids, *subscriber_id);
                }
            }

            if root == "quotes"
                && let Some(symbol) = segments.get(1)
                && let Some(subscribers) = quote_subscribers_by_symbol.get(symbol.as_str())
            {
                for subscriber_id in subscribers {
                    push_candidate(candidate_seen, candidate_ids, *subscriber_id);
                }
            }
        }
    }

    fn cleanup_dead(&mut self) {
        self.dead_ids.clear();
        for (subscriber_id, subscriber) in self.subscribers.iter().enumerate() {
            if subscriber
                .as_ref()
                .is_some_and(|subscriber| subscriber.sender.receiver_count() == 0)
            {
                self.dead_ids.push(subscriber_id);
            }
        }
        self.remove_dead_from_scratch();
    }

    fn remove_dead_from_scratch(&mut self) {
        if self.dead_ids.is_empty() {
            return;
        }

        self.dead_ids.sort_unstable();
        self.dead_ids.dedup();
        let mut dead_ids = std::mem::take(&mut self.dead_ids);
        self.remove_dead(&dead_ids);
        dead_ids.clear();
        self.dead_ids = dead_ids;
    }

    fn remove_dead(&mut self, dead: &[usize]) {
        if dead.is_empty() {
            return;
        }

        for subscriber_id in dead {
            if let Some(subscriber) = self.subscribers.get_mut(*subscriber_id) {
                *subscriber = None;
            }
        }
        remove_dead_ids(&mut self.generic_subscribers, dead);
        retain_live_index(&mut self.subscribers_by_root, dead);
        retain_live_index(&mut self.quote_subscribers_by_symbol, dead);
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

fn remove_dead_ids(ids: &mut Vec<usize>, dead: &[usize]) {
    ids.retain(|subscriber_id| !contains_dead(dead, *subscriber_id));
}

fn retain_live_index(index: &mut HashMap<String, Vec<usize>>, dead: &[usize]) {
    for ids in index.values_mut() {
        remove_dead_ids(ids, dead);
    }
    index.retain(|_, ids| !ids.is_empty());
}

fn contains_dead(dead: &[usize], subscriber_id: usize) -> bool {
    dead.binary_search(&subscriber_id).is_ok()
}

#[cfg(test)]
mod tests {
    use std::{hint::black_box, sync::Arc, time::Instant};

    use tokio::sync::broadcast;
    use tqsdk_core::{
        ChangeSet, CommitResult, CommitScope, ObjectKey, ProtocolDomain, Revision,
        SharedCommitResult, StatePath, Symbol,
    };

    use super::PathSubscriberRegistry;

    #[test]
    fn path_dispatcher_reuses_candidate_buffers_across_commits() {
        let mut registry = PathSubscriberRegistry::default();
        let symbols = vec!["SHFE.rb2601".to_string(), "DCE.m2601".to_string()];
        let mut receivers = Vec::with_capacity(symbols.len());

        for symbol in &symbols {
            let (sender, receiver) = broadcast::channel(16);
            receivers.push(receiver);
            registry.push(vec![StatePath::new(["quotes", symbol.as_str()])], sender);
        }

        registry.notify_matching(quote_commit(&symbols, 1));
        let candidate_capacity = registry.candidate_ids.capacity();
        let seen_capacity = registry.candidate_seen.capacity();

        registry.notify_matching(quote_commit(&symbols, 2));

        assert!(registry.candidate_ids.capacity() >= candidate_capacity);
        assert!(registry.candidate_seen.capacity() >= seen_capacity);
        assert_eq!(
            registry
                .subscribers
                .iter()
                .filter(|subscriber| subscriber.is_some())
                .count(),
            symbols.len()
        );
        black_box(receivers);
    }

    #[test]
    #[ignore = "benchmark-style fan-out probe; run explicitly with --ignored --nocapture"]
    fn path_dispatcher_quote_fanout_probe_reports_throughput() {
        const SUBSCRIBERS: usize = 512;
        const COMMITS: u64 = 2_000;

        let mut registry = PathSubscriberRegistry::default();
        let symbols = bench_symbols(SUBSCRIBERS);
        let mut receivers = Vec::with_capacity(SUBSCRIBERS);

        for symbol in &symbols {
            let (sender, receiver) = broadcast::channel(1024);
            receivers.push(receiver);
            registry.push(vec![StatePath::new(["quotes", symbol.as_str()])], sender);
        }

        let commits = (0..COMMITS)
            .map(|revision| quote_commit(&symbols, revision + 1))
            .collect::<Vec<_>>();

        let start = Instant::now();
        for commit in commits {
            registry.notify_matching(commit);
        }
        let elapsed = start.elapsed();
        let ns_per_commit = elapsed.as_nanos() as f64 / COMMITS as f64;
        let ns_per_candidate = ns_per_commit / SUBSCRIBERS as f64;

        println!(
            "path_dispatcher_quote_fanout subscribers={SUBSCRIBERS} commits={COMMITS} ns/commit={ns_per_commit:.1} ns/candidate={ns_per_candidate:.1}"
        );
        black_box(registry.candidate_ids.capacity());
        black_box(registry.candidate_seen.capacity());
        black_box(receivers);
    }

    fn quote_commit(symbols: &[String], revision: u64) -> SharedCommitResult {
        let path_hits = symbols
            .iter()
            .map(|symbol| StatePath::new(["quotes", symbol.as_str()]))
            .collect::<Vec<_>>();
        let object_hits = symbols
            .iter()
            .map(|symbol| ObjectKey::Quote {
                symbol: Symbol::new(symbol.clone()),
            })
            .collect::<Vec<_>>();

        Arc::new(CommitResult::new(
            Revision::new(revision),
            vec![ProtocolDomain::Market],
            ChangeSet {
                path_hits,
                object_hits,
                field_hits: Vec::new(),
            },
            Vec::new(),
            CommitScope::RealtimeUpdate,
        ))
    }

    fn bench_symbols(count: usize) -> Vec<String> {
        (0..count)
            .map(|index| format!("SHFE.stream{index:04}"))
            .collect()
    }
}
