#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::HashMap;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use tqsdk_core::{
    CommitResult, CommitScope, ObjectKey, ProtocolDomain, SharedCommitResult, StatePath,
};

use crate::{CommitStream, Result};

/// Commit stream filtered by state-path prefixes.
pub struct PathCommitStream {
    inner: CommitStream,
    matcher: PathMatcher,
}

impl PathCommitStream {
    pub(crate) fn new(inner: CommitStream, paths: Vec<StatePath>) -> Self {
        Self {
            inner,
            matcher: PathMatcher::new(paths),
        }
    }
}

impl Stream for PathCommitStream {
    type Item = Result<SharedCommitResult>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        poll_next_filtered(&mut this.inner, cx, |commit| this.matcher.is_match(commit))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PathMatcher {
    match_all: bool,
    paths_by_root: HashMap<String, Vec<StatePath>>,
}

impl PathMatcher {
    pub(crate) fn new(paths: Vec<StatePath>) -> Self {
        let mut match_all = paths.is_empty();
        let mut paths_by_root = HashMap::new();

        for path in paths {
            let Some(root) = path.segments().first() else {
                match_all = true;
                continue;
            };
            paths_by_root
                .entry(root.clone())
                .or_insert_with(Vec::new)
                .push(path);
        }

        if match_all {
            paths_by_root.clear();
        }

        Self {
            match_all,
            paths_by_root,
        }
    }

    pub(crate) fn is_match(&self, commit: &CommitResult) -> bool {
        self.match_all
            || commit
                .changes
                .path_hits
                .iter()
                .any(|changed| self.matches_changed_path(changed))
    }

    fn matches_changed_path(&self, changed: &StatePath) -> bool {
        let Some(root) = changed.segments().first() else {
            return false;
        };
        self.paths_by_root
            .get(root.as_str())
            .is_some_and(|paths| paths.iter().any(|target| path_matches(target, changed)))
    }
}

/// Commit stream filtered by commit scopes.
pub struct ScopeCommitStream {
    inner: CommitStream,
    scopes: Vec<CommitScope>,
}

impl ScopeCommitStream {
    pub(crate) fn new(inner: CommitStream, scopes: Vec<CommitScope>) -> Self {
        Self { inner, scopes }
    }
}

impl Stream for ScopeCommitStream {
    type Item = Result<SharedCommitResult>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        poll_next_filtered(&mut this.inner, cx, |commit| {
            matches_scope_filters(&this.scopes, commit)
        })
    }
}

/// Commit stream filtered by protocol domains recorded on each commit.
pub struct DomainCommitStream {
    inner: CommitStream,
    domains: Vec<ProtocolDomain>,
}

impl DomainCommitStream {
    pub(crate) fn new(inner: CommitStream, domains: Vec<ProtocolDomain>) -> Self {
        Self { inner, domains }
    }
}

impl Stream for DomainCommitStream {
    type Item = Result<SharedCommitResult>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        poll_next_filtered(&mut this.inner, cx, |commit| {
            matches_domain_filters(&this.domains, commit)
        })
    }
}

/// Commit stream filtered by changed object identities.
pub struct ObjectCommitStream {
    inner: CommitStream,
    objects: Vec<ObjectKey>,
}

impl ObjectCommitStream {
    pub(crate) fn new(inner: CommitStream, objects: Vec<ObjectKey>) -> Self {
        Self { inner, objects }
    }
}

impl Stream for ObjectCommitStream {
    type Item = Result<SharedCommitResult>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        poll_next_filtered(&mut this.inner, cx, |commit| {
            matches_object_filters(&this.objects, commit)
        })
    }
}

/// Commit stream filtered by field hits on a specific object.
pub struct FieldCommitStream {
    inner: CommitStream,
    object: ObjectKey,
    fields: Vec<String>,
}

impl FieldCommitStream {
    pub(crate) fn new(inner: CommitStream, object: ObjectKey, fields: Vec<String>) -> Self {
        Self {
            inner,
            object,
            fields,
        }
    }
}

impl Stream for FieldCommitStream {
    type Item = Result<SharedCommitResult>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        poll_next_filtered(&mut this.inner, cx, |commit| {
            matches_field_filters(&this.object, &this.fields, commit)
        })
    }
}

pub(crate) fn matches_scope_filters(scopes: &[CommitScope], commit: &CommitResult) -> bool {
    scopes.is_empty() || scopes.contains(&commit.scope)
}

pub(crate) fn matches_domain_filters(domains: &[ProtocolDomain], commit: &CommitResult) -> bool {
    domains.is_empty() || commit.domains.iter().any(|hit| domains.contains(hit))
}

pub(crate) fn matches_object_filters(objects: &[ObjectKey], commit: &CommitResult) -> bool {
    objects.is_empty()
        || commit
            .changes
            .object_hits
            .iter()
            .any(|hit| objects.contains(hit))
}

pub(crate) fn matches_field_filters(
    object: &ObjectKey,
    fields: &[String],
    commit: &CommitResult,
) -> bool {
    commit.changes.field_hits.iter().any(|hit| {
        &hit.object == object
            && (fields.is_empty() || fields.iter().any(|field| field == &hit.field))
    })
}

fn poll_next_filtered<F>(
    inner: &mut CommitStream,
    cx: &mut Context<'_>,
    mut predicate: F,
) -> Poll<Option<Result<SharedCommitResult>>>
where
    F: FnMut(&CommitResult) -> bool,
{
    loop {
        match Pin::new(&mut *inner).poll_next(cx) {
            Poll::Ready(Some(Ok(commit))) if predicate(&commit) => {
                return Poll::Ready(Some(Ok(commit)));
            }
            Poll::Ready(Some(Ok(_))) => continue,
            Poll::Ready(Some(Err(error))) => return Poll::Ready(Some(Err(error))),
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Pending => return Poll::Pending,
        }
    }
}

fn path_matches(target: &StatePath, changed: &StatePath) -> bool {
    let target_segments = target.segments();
    let changed_segments = changed.segments();

    target_segments.len() <= changed_segments.len()
        && target_segments
            .iter()
            .zip(changed_segments.iter())
            .all(|(left, right)| left == right)
}
