#![cfg_attr(not(test), forbid(unsafe_code))]

use crate::Result;
use crate::registry::{TaskId, TaskRegistry};

/// Single-owner task host built on a wait-style API.
pub struct TaskHost {
    api: tqsdk_wait::TqApi,
    registry: TaskRegistry,
}

impl TaskHost {
    #[must_use]
    pub fn new(api: tqsdk_wait::TqApi) -> Self {
        Self {
            api,
            registry: TaskRegistry::default(),
        }
    }

    #[must_use]
    pub fn api(&self) -> &tqsdk_wait::TqApi {
        &self.api
    }

    #[must_use]
    pub fn api_mut(&mut self) -> &mut tqsdk_wait::TqApi {
        &mut self.api
    }

    #[must_use]
    pub fn into_api(self) -> tqsdk_wait::TqApi {
        self.api
    }

    #[doc(hidden)]
    pub fn register_target_owner_for_test(
        &mut self,
        account_id: impl AsRef<str>,
        symbol: impl AsRef<str>,
    ) -> Result<u64> {
        self.registry
            .register_target_task(account_id, symbol)
            .map(|task| task.id.0)
    }

    #[doc(hidden)]
    pub fn register_scheduler_owner_for_test(
        &mut self,
        account_id: impl AsRef<str>,
        symbol: impl AsRef<str>,
    ) -> Result<u64> {
        self.registry
            .register_scheduler(account_id, symbol)
            .map(|task| task.id.0)
    }

    #[doc(hidden)]
    pub fn check_manual_order_allowed_for_test(
        &self,
        account_id: impl AsRef<str>,
        symbol: impl AsRef<str>,
    ) -> Result<()> {
        self.registry.check_manual_order_allowed(account_id, symbol)
    }

    #[doc(hidden)]
    pub fn unregister_task_for_test(&mut self, task_id: u64) -> bool {
        self.registry.unregister_task(TaskId(task_id))
    }
}
