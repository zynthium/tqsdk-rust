#![cfg_attr(not(test), forbid(unsafe_code))]

use tqsdk_core::{RuntimeHandle, RuntimeReader, SessionBootstrap, SessionRuntime};

use crate::config::SessionFacadeConfig;

#[derive(Clone)]
pub struct SessionClient {
    handle: RuntimeHandle,
    reader: RuntimeReader,
    runtime: SessionRuntime,
    facade_config: SessionFacadeConfig,
}

impl SessionClient {
    pub(crate) fn new(handle: RuntimeHandle, facade_config: SessionFacadeConfig) -> Self {
        let reader = handle.reader();
        let runtime = SessionRuntime::new(handle.clone(), SessionBootstrap::new());
        Self {
            handle,
            reader,
            runtime,
            facade_config,
        }
    }

    pub fn handle(&self) -> &RuntimeHandle {
        &self.handle
    }

    pub fn reader(&self) -> &RuntimeReader {
        &self.reader
    }

    pub fn runtime(&self) -> &SessionRuntime {
        &self.runtime
    }

    pub fn facade_config(&self) -> &SessionFacadeConfig {
        &self.facade_config
    }
}
