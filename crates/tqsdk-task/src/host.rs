#![cfg_attr(not(test), forbid(unsafe_code))]

use std::sync::{Arc, Mutex};

use crate::Result;
use crate::registry::{TaskId, TaskRegistry};
use crate::target_pos::{TargetPosBuilder, TargetPosStore};

/// Single-owner task host built on a wait-style API.
pub struct TaskHost {
    api: tqsdk_wait::TqApi,
    registry: Arc<Mutex<TaskRegistry>>,
    target_tasks: Arc<Mutex<TargetPosStore>>,
}

impl TaskHost {
    #[must_use]
    pub fn new(api: tqsdk_wait::TqApi) -> Self {
        Self {
            api,
            registry: Arc::new(Mutex::new(TaskRegistry::default())),
            target_tasks: Arc::new(Mutex::new(TargetPosStore::default())),
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

    pub async fn wait_update(&mut self, deadline: Option<tokio::time::Instant>) -> Result<bool> {
        let updated = self.api.wait_update(deadline).await?;
        if updated {
            self.target_tasks
                .lock()
                .expect("target task store lock poisoned")
                .process_wait_update();
        }
        Ok(updated)
    }

    #[must_use]
    pub fn target_pos(
        &mut self,
        account_id: impl AsRef<str>,
        symbol: impl AsRef<str>,
    ) -> TargetPosBuilder {
        TargetPosBuilder::new(
            Arc::clone(&self.registry),
            Arc::clone(&self.target_tasks),
            account_id.as_ref().to_owned(),
            symbol.as_ref().to_owned(),
        )
    }

    #[doc(hidden)]
    pub fn register_target_owner_for_test(
        &mut self,
        account_id: impl AsRef<str>,
        symbol: impl AsRef<str>,
    ) -> Result<u64> {
        self.registry
            .lock()
            .expect("task registry lock poisoned")
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
            .lock()
            .expect("task registry lock poisoned")
            .register_scheduler(account_id, symbol)
            .map(|task| task.id.0)
    }

    #[doc(hidden)]
    pub fn check_manual_order_allowed_for_test(
        &self,
        account_id: impl AsRef<str>,
        symbol: impl AsRef<str>,
    ) -> Result<()> {
        self.registry
            .lock()
            .expect("task registry lock poisoned")
            .check_manual_order_allowed(account_id, symbol)
    }

    #[doc(hidden)]
    pub fn unregister_task_for_test(&mut self, task_id: u64) -> bool {
        self.registry
            .lock()
            .expect("task registry lock poisoned")
            .unregister_task(TaskId(task_id))
    }
}
