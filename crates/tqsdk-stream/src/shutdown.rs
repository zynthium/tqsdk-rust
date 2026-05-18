#![cfg_attr(not(test), forbid(unsafe_code))]

use crate::{StreamFacadeError, TqStream};

/// Coordinator for explicit stream shutdown.
pub struct StreamGracefulShutdown {
    stream: TqStream,
}

/// Typed shutdown report for a stream facade.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamGracefulShutdownReport {
    outbound_flushed: bool,
    outbound_flush_error: Option<StreamFacadeError>,
    driver_closed: bool,
}

impl StreamGracefulShutdown {
    pub(crate) fn new(stream: TqStream) -> Self {
        Self { stream }
    }

    pub async fn shutdown(self) -> crate::Result<StreamGracefulShutdownReport> {
        let (outbound_flushed, outbound_flush_error) =
            match self.stream.flush_outbound_for_shutdown().await {
                Ok(flushed) => (flushed, None),
                Err(error) => (false, Some(error)),
            };
        self.stream.abort_driver_for_shutdown();

        Ok(StreamGracefulShutdownReport {
            outbound_flushed,
            outbound_flush_error,
            driver_closed: self.stream.driver_closed_for_shutdown(),
        })
    }
}

impl StreamGracefulShutdownReport {
    #[must_use]
    pub fn graceful(&self) -> bool {
        self.driver_closed && self.outbound_flush_error.is_none()
    }

    #[must_use]
    pub fn outbound_flushed(&self) -> bool {
        self.outbound_flushed
    }

    #[must_use]
    pub fn outbound_flush_error(&self) -> Option<&StreamFacadeError> {
        self.outbound_flush_error.as_ref()
    }

    #[must_use]
    pub fn driver_closed(&self) -> bool {
        self.driver_closed
    }
}
