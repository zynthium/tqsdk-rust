#![cfg_attr(not(test), forbid(unsafe_code))]

use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

use crate::driver::{DriverEvent, StreamDriver};

const DEFAULT_COMMIT_CHANNEL_CAPACITY: usize = 1024;

/// Shared-session stream facade over [`tqsdk_session::SessionClient`].
///
/// [`TqStream`] lazily starts a single background driver task that advances the
/// underlying session and fans out canonical [`tqsdk_core::CommitResult`]
/// values to multiple async consumers.
pub struct TqStream {
    session: Option<tqsdk_session::SessionClient>,
    reader: tqsdk_core::RuntimeReader,
    driver: StreamDriver,
}

impl TqStream {
    #[must_use]
    pub fn new(session: tqsdk_session::SessionClient) -> Self {
        Self::new_with_capacity(session, DEFAULT_COMMIT_CHANNEL_CAPACITY)
    }

    fn new_with_capacity(session: tqsdk_session::SessionClient, capacity: usize) -> Self {
        let reader = session.reader_clone();
        let driver = StreamDriver::new(session.clone(), reader.clone(), capacity);
        Self {
            session: Some(session),
            reader,
            driver,
        }
    }

    #[must_use]
    pub fn session(&self) -> &tqsdk_session::SessionClient {
        self.session
            .as_ref()
            .expect("tqsdk-stream session missing while facade is alive")
    }

    #[must_use]
    pub fn reader(&self) -> &tqsdk_core::RuntimeReader {
        &self.reader
    }

    #[must_use]
    pub fn into_session(mut self) -> tqsdk_session::SessionClient {
        self.driver.abort();
        self.session
            .take()
            .expect("tqsdk-stream session missing during into_session")
    }

    pub fn commit_stream(&self) -> crate::error::Result<CommitStream> {
        let receiver = self.driver.subscribe();
        self.driver.ensure_started()?;
        Ok(CommitStream::new(receiver))
    }

    #[doc(hidden)]
    #[must_use]
    pub fn new_for_test_with_capacity(
        session: tqsdk_session::SessionClient,
        capacity: usize,
    ) -> Self {
        Self::new_with_capacity(session, capacity)
    }

    #[doc(hidden)]
    pub fn handle_for_test(&self) -> tqsdk_core::RuntimeHandle {
        self.session().handle().clone()
    }
}

impl Drop for TqStream {
    fn drop(&mut self) {
        self.driver.abort();
    }
}

/// Async stream of canonical runtime commits.
pub struct CommitStream {
    inner: BroadcastStream<DriverEvent>,
}

impl CommitStream {
    fn new(receiver: broadcast::Receiver<DriverEvent>) -> Self {
        Self {
            inner: BroadcastStream::new(receiver),
        }
    }
}

impl Stream for CommitStream {
    type Item = crate::error::Result<tqsdk_core::CommitResult>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(DriverEvent::Commit(commit)))) => Poll::Ready(Some(Ok(commit))),
            Poll::Ready(Some(Ok(DriverEvent::Error(error)))) => {
                Poll::Ready(Some(Err(crate::error::StreamFacadeError::Session(error))))
            }
            Poll::Ready(Some(Ok(DriverEvent::Closed))) => {
                Poll::Ready(Some(Err(crate::error::StreamFacadeError::Closed)))
            }
            Poll::Ready(Some(Err(BroadcastStreamRecvError::Lagged(skipped)))) => {
                Poll::Ready(Some(Err(crate::error::StreamFacadeError::Lagged {
                    skipped,
                })))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
