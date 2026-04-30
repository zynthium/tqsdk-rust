#![cfg_attr(not(test), forbid(unsafe_code))]

use crate::TqStream;

/// Explicit stream fixture driver for deterministic tests.
///
/// This type exposes driver lifecycle and synthetic session events without
/// placing hidden `_for_test` methods on the production [`TqStream`] facade.
pub struct StreamTestDriver<'a> {
    stream: &'a TqStream,
}

impl<'a> StreamTestDriver<'a> {
    /// Create a fixture driver for a stream facade.
    #[must_use]
    pub fn new(stream: &'a TqStream) -> Self {
        Self { stream }
    }

    /// Inject a synthetic session error into stream consumers.
    pub fn emit_session_error(&self, error: tqsdk_session::SessionFacadeError) {
        self.stream.emit_driver_session_error(error);
    }

    /// Emit a synthetic closed event into existing stream consumers.
    pub fn emit_closed(&self) {
        self.stream.emit_driver_closed();
    }

    /// Abort the stream driver to characterize closed-state behavior.
    pub fn close_driver(&self) {
        self.stream.close_driver_for_testing();
    }
}
