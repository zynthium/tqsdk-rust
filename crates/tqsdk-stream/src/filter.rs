#![cfg_attr(not(test), forbid(unsafe_code))]

use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use tqsdk_core::{CommitResult, CommitScope, ObjectKey, ProtocolDomain, StatePath};

use crate::{CommitStream, Result};

/// Commit stream filtered by state-path prefixes.
pub struct PathCommitStream {
    inner: CommitStream,
    paths: Vec<StatePath>,
}

impl PathCommitStream {
    pub(crate) fn new(inner: CommitStream, paths: Vec<StatePath>) -> Self {
        Self { inner, paths }
    }
}

impl Stream for PathCommitStream {
    type Item = Result<CommitResult>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        poll_next_filtered(&mut this.inner, cx, |commit| {
            matches_path_filters(&this.paths, commit)
        })
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
    type Item = Result<CommitResult>;

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
    type Item = Result<CommitResult>;

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
    type Item = Result<CommitResult>;

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
    type Item = Result<CommitResult>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        poll_next_filtered(&mut this.inner, cx, |commit| {
            matches_field_filters(&this.object, &this.fields, commit)
        })
    }
}

pub(crate) fn matches_path_filters(paths: &[StatePath], commit: &CommitResult) -> bool {
    paths.is_empty()
        || commit
            .changes
            .path_hits
            .iter()
            .any(|changed| paths.iter().any(|target| path_matches(target, changed)))
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
) -> Poll<Option<Result<CommitResult>>>
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
