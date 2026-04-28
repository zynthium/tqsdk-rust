#![cfg_attr(not(test), forbid(unsafe_code))]

use crate::{StreamFacadeError, StreamSinkHandle, StreamSinkShutdownReport, TqStream};

/// Coordinator for explicit stream shutdown and managed sink flushing.
pub struct StreamGracefulShutdown {
    stream: TqStream,
    sinks: Vec<StreamSinkHandle>,
}

/// Typed shutdown report for a stream facade and its managed sinks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamGracefulShutdownReport {
    outbound_flushed: bool,
    outbound_flush_error: Option<StreamFacadeError>,
    driver_closed: bool,
    sink_reports: Vec<StreamSinkShutdownReport>,
    sink_errors: Vec<StreamSinkShutdownError>,
}

/// Error captured while shutting down one managed sink.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamShutdownError {
    sink: String,
    error: StreamFacadeError,
}

pub type StreamSinkShutdownError = StreamShutdownError;

impl StreamGracefulShutdown {
    pub(crate) fn new(stream: TqStream) -> Self {
        Self {
            stream,
            sinks: Vec::new(),
        }
    }

    #[must_use]
    pub fn sink(mut self, sink: StreamSinkHandle) -> Self {
        self.sinks.push(sink);
        self
    }

    pub async fn shutdown(self) -> crate::Result<StreamGracefulShutdownReport> {
        let (outbound_flushed, outbound_flush_error) =
            match self.stream.flush_outbound_for_shutdown().await {
                Ok(flushed) => (flushed, None),
                Err(error) => (false, Some(error)),
            };
        self.stream.abort_driver_for_shutdown();

        let mut sink_reports = Vec::with_capacity(self.sinks.len());
        let mut sink_errors = Vec::new();
        for sink in self.sinks {
            let name = sink.name().to_string();
            match sink.shutdown().await {
                Ok(report) => sink_reports.push(report),
                Err(error) => sink_errors.push(StreamSinkShutdownError { sink: name, error }),
            }
        }

        Ok(StreamGracefulShutdownReport {
            outbound_flushed,
            outbound_flush_error,
            driver_closed: self.stream.driver_closed_for_shutdown(),
            sink_reports,
            sink_errors,
        })
    }
}

impl StreamGracefulShutdownReport {
    #[must_use]
    pub fn graceful(&self) -> bool {
        self.driver_closed
            && self.outbound_flush_error.is_none()
            && self.sink_errors.is_empty()
            && self
                .sink_reports
                .iter()
                .all(StreamSinkShutdownReport::flushed)
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

    #[must_use]
    pub fn sink_reports(&self) -> &[StreamSinkShutdownReport] {
        &self.sink_reports
    }

    #[must_use]
    pub fn sink_errors(&self) -> &[StreamSinkShutdownError] {
        &self.sink_errors
    }
}

impl StreamShutdownError {
    #[must_use]
    pub fn sink(&self) -> &str {
        &self.sink
    }

    #[must_use]
    pub fn error(&self) -> &StreamFacadeError {
        &self.error
    }
}
