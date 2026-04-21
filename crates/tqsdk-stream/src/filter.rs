#![cfg_attr(not(test), forbid(unsafe_code))]

use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use tqsdk_core::{CommitResult, CommitScope, StatePath};

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

        loop {
            match Pin::new(&mut this.inner).poll_next(cx) {
                Poll::Ready(Some(Ok(commit))) if matches_path_filters(&this.paths, &commit) => {
                    return Poll::Ready(Some(Ok(commit)));
                }
                Poll::Ready(Some(Ok(_))) => continue,
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Some(Err(error))),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
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

        loop {
            match Pin::new(&mut this.inner).poll_next(cx) {
                Poll::Ready(Some(Ok(commit))) if matches_scope_filters(&this.scopes, &commit) => {
                    return Poll::Ready(Some(Ok(commit)));
                }
                Poll::Ready(Some(Ok(_))) => continue,
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Some(Err(error))),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
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

fn path_matches(target: &StatePath, changed: &StatePath) -> bool {
    let target_segments = target.segments();
    let changed_segments = changed.segments();

    target_segments.len() <= changed_segments.len()
        && target_segments
            .iter()
            .zip(changed_segments.iter())
            .all(|(left, right)| left == right)
}
