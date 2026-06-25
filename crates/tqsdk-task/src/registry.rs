#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::HashMap;

use crate::{Result, TaskError, TaskKind};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct TaskId(pub(crate) u64);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TaskKey {
    account_id: String,
    symbol: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RegisteredTask {
    pub(crate) id: TaskId,
    pub(crate) kind: TaskKind,
    pub(crate) account_id: String,
    pub(crate) symbol: String,
}

#[derive(Default)]
pub(crate) struct TaskRegistry {
    next_task_id: u64,
    by_key: HashMap<TaskKey, RegisteredTask>,
    by_id: HashMap<TaskId, TaskKey>,
}

impl TaskRegistry {
    pub(crate) fn allocate_task_id(&mut self) -> TaskId {
        self.next_task_id += 1;
        TaskId(self.next_task_id)
    }

    pub(crate) fn register_target_task(
        &mut self,
        account_id: impl AsRef<str>,
        symbol: impl AsRef<str>,
    ) -> Result<RegisteredTask> {
        self.register(account_id.as_ref(), symbol.as_ref(), TaskKind::TargetPos)
    }

    pub(crate) fn active_task(
        &self,
        account_id: impl AsRef<str>,
        symbol: impl AsRef<str>,
    ) -> Option<RegisteredTask> {
        let key = TaskKey {
            account_id: account_id.as_ref().to_owned(),
            symbol: symbol.as_ref().to_owned(),
        };
        self.by_key.get(&key).cloned()
    }

    pub(crate) fn register_scheduler(
        &mut self,
        account_id: impl AsRef<str>,
        symbol: impl AsRef<str>,
    ) -> Result<RegisteredTask> {
        self.register(account_id.as_ref(), symbol.as_ref(), TaskKind::Scheduler)
    }

    pub(crate) fn check_manual_order_allowed(
        &self,
        account_id: impl AsRef<str>,
        symbol: impl AsRef<str>,
    ) -> Result<()> {
        let key = TaskKey {
            account_id: account_id.as_ref().to_owned(),
            symbol: symbol.as_ref().to_owned(),
        };

        if let Some(active) = self.by_key.get(&key) {
            return Err(TaskError::ManualOrderBlocked {
                account_id: active.account_id.clone(),
                symbol: active.symbol.clone(),
                active_task_kind: active.kind,
            });
        }

        Ok(())
    }

    pub(crate) fn unregister_task(&mut self, task_id: TaskId) -> bool {
        let Some(key) = self.by_id.remove(&task_id) else {
            return false;
        };
        self.by_key.remove(&key).is_some()
    }

    fn register(
        &mut self,
        account_id: &str,
        symbol: &str,
        kind: TaskKind,
    ) -> Result<RegisteredTask> {
        let key = TaskKey {
            account_id: account_id.to_owned(),
            symbol: symbol.to_owned(),
        };

        if let Some(active) = self.by_key.get(&key) {
            return Err(TaskError::OwnershipConflict {
                account_id: active.account_id.clone(),
                symbol: active.symbol.clone(),
                active_task_kind: active.kind,
            });
        }

        let task = RegisteredTask {
            id: self.allocate_task_id(),
            kind,
            account_id: key.account_id.clone(),
            symbol: key.symbol.clone(),
        };

        self.by_key.insert(key.clone(), task.clone());
        self.by_id.insert(task.id, key);

        Ok(task)
    }
}

#[cfg(test)]
mod tests {
    use super::TaskRegistry;
    use crate::{TaskError, TaskKind};

    #[test]
    fn registry_rejects_second_owner_for_same_account_symbol() {
        let mut registry = TaskRegistry::default();

        let first = registry
            .register_target_task("sim", "SHFE.rb2601")
            .expect("first target task should register");
        assert_eq!(first.kind, TaskKind::TargetPos);

        let err = registry
            .register_scheduler("sim", "SHFE.rb2601")
            .expect_err("scheduler should not take ownership while target task is active");
        assert_eq!(
            err,
            TaskError::OwnershipConflict {
                account_id: "sim".to_string(),
                symbol: "SHFE.rb2601".to_string(),
                active_task_kind: TaskKind::TargetPos,
            }
        );
    }

    #[test]
    fn registry_blocks_manual_order_when_symbol_is_owned() {
        let mut registry = TaskRegistry::default();
        let lease = registry
            .register_scheduler("sim", "SHFE.rb2601")
            .expect("scheduler should register");

        let err = registry
            .check_manual_order_allowed("sim", "SHFE.rb2601")
            .expect_err("manual order should be blocked while scheduler owns the symbol");
        assert_eq!(
            err,
            TaskError::ManualOrderBlocked {
                account_id: "sim".to_string(),
                symbol: "SHFE.rb2601".to_string(),
                active_task_kind: TaskKind::Scheduler,
            }
        );

        registry.unregister_task(lease.id);
        registry
            .check_manual_order_allowed("sim", "SHFE.rb2601")
            .expect("manual order should be allowed after ownership is released");
    }
}
