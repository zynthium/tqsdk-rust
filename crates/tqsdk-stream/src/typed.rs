#![cfg_attr(not(test), forbid(unsafe_code))]

use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use serde::de::DeserializeOwned;
use tqsdk_core::{RuntimeReader, SharedCommitResult, StatePath};

use crate::{PathCommitStream, Result, StreamFacadeError};

/// Decoded value paired with the commit that made it visible.
#[derive(Debug, Clone)]
pub struct ValueUpdate<T> {
    pub commit: SharedCommitResult,
    pub value: T,
}

impl<T> ValueUpdate<T> {
    #[must_use]
    pub fn into_parts(self) -> (SharedCommitResult, T) {
        (self.commit, self.value)
    }
}

/// Typed live stream backed by a single filtered state path.
pub struct PathValueStream<T> {
    inner: PathCommitStream,
    reader: RuntimeReader,
    path: StatePath,
    marker: PhantomData<fn() -> T>,
}

impl<T> PathValueStream<T> {
    pub(crate) fn new(inner: PathCommitStream, reader: RuntimeReader, path: StatePath) -> Self {
        Self {
            inner,
            reader,
            path,
            marker: PhantomData,
        }
    }

    #[must_use]
    pub fn path(&self) -> &StatePath {
        &self.path
    }
}

impl<T> Stream for PathValueStream<T>
where
    T: DeserializeOwned,
{
    type Item = Result<ValueUpdate<T>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(commit))) => {
                let snapshot = this.reader.read();
                let value = match snapshot.decode(this.path.segments()) {
                    Ok(Some(value)) => value,
                    Ok(None) => {
                        return Poll::Ready(Some(Err(StreamFacadeError::MissingValue {
                            path: this.path.clone(),
                        })));
                    }
                    Err(error) => {
                        return Poll::Ready(Some(Err(StreamFacadeError::Contract(error))));
                    }
                };
                Poll::Ready(Some(Ok(ValueUpdate { commit, value })))
            }
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(error))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
