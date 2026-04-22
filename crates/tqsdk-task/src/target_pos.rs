#![cfg_attr(not(test), forbid(unsafe_code))]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::registry::{TaskId, TaskRegistry};
use crate::{Result, TaskError};

/// Builder for a target position task.
pub struct TargetPosBuilder {
    registry: Arc<Mutex<TaskRegistry>>,
    account_id: String,
    symbol: String,
}

/// Minimal target position task shell.
#[derive(Clone)]
pub struct TargetPosTask {
    inner: Arc<TargetPosTaskInner>,
}

struct TargetPosTaskInner {
    registry: Arc<Mutex<TaskRegistry>>,
    task_id: TaskId,
    account_id: String,
    symbol: String,
    target_volume: Mutex<Option<i64>>,
    finished: AtomicBool,
}

impl TargetPosBuilder {
    pub(crate) fn new(
        registry: Arc<Mutex<TaskRegistry>>,
        account_id: String,
        symbol: String,
    ) -> Self {
        Self {
            registry,
            account_id,
            symbol,
        }
    }

    pub fn build(self) -> Result<TargetPosTask> {
        let task = self
            .registry
            .lock()
            .expect("task registry lock poisoned")
            .register_target_task(&self.account_id, &self.symbol)?;

        Ok(TargetPosTask {
            inner: Arc::new(TargetPosTaskInner {
                registry: self.registry,
                task_id: task.id,
                account_id: self.account_id,
                symbol: self.symbol,
                target_volume: Mutex::new(None),
                finished: AtomicBool::new(false),
            }),
        })
    }
}

impl TargetPosTask {
    #[must_use]
    pub fn account_id(&self) -> &str {
        &self.inner.account_id
    }

    #[must_use]
    pub fn symbol(&self) -> &str {
        &self.inner.symbol
    }

    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.inner.finished.load(Ordering::SeqCst)
    }

    #[must_use]
    pub fn current_target_volume(&self) -> Option<i64> {
        *self
            .inner
            .target_volume
            .lock()
            .expect("target volume lock poisoned")
    }

    pub fn set_target_volume(&self, volume: i64) -> Result<()> {
        if self.is_finished() {
            return Err(TaskError::InvalidState(
                "target position task already finished",
            ));
        }

        *self
            .inner
            .target_volume
            .lock()
            .expect("target volume lock poisoned") = Some(volume);
        Ok(())
    }

    pub async fn cancel(&self) -> Result<()> {
        self.inner.finish();
        Ok(())
    }
}

impl TargetPosTaskInner {
    fn finish(&self) {
        if self.finished.swap(true, Ordering::SeqCst) {
            return;
        }

        self.registry
            .lock()
            .expect("task registry lock poisoned")
            .unregister_task(self.task_id);
    }
}

impl Drop for TargetPosTaskInner {
    fn drop(&mut self) {
        if self.finished.swap(true, Ordering::SeqCst) {
            return;
        }

        self.registry
            .lock()
            .expect("task registry lock poisoned")
            .unregister_task(self.task_id);
    }
}
