use tqsdk_core::{Notification, NotificationId, ObjectKey, StatePath};

use crate::{change::ChangeTrackedRef, step::WaitReadHandle};

/// Lightweight handle to `system/notify/{notification_id}`.
#[derive(Clone)]
pub struct NotificationRef {
    reader: WaitReadHandle,
    notification_id: NotificationId,
}

impl NotificationRef {
    pub(crate) fn new(reader: WaitReadHandle, notification_id: impl Into<String>) -> Self {
        Self {
            reader,
            notification_id: NotificationId::new(notification_id.into()),
        }
    }

    pub fn load(&self) -> crate::error::Result<Notification> {
        self.snapshot()?
            .ok_or(crate::error::WaitFacadeError::InvalidState(
                "notification not ready",
            ))
    }

    pub fn snapshot(&self) -> crate::error::Result<Option<Notification>> {
        self.reader
            .reader()
            .read()
            .decode_path::<Notification>(&["system", "notify", self.notification_id.as_str()])
            .map_err(Into::into)
    }

    pub fn is_ready(&self) -> crate::error::Result<bool> {
        Ok(self.snapshot()?.is_some())
    }
}

impl ChangeTrackedRef for NotificationRef {
    fn object_key(&self) -> Option<ObjectKey> {
        Some(ObjectKey::Notification {
            notification_id: self.notification_id.clone(),
        })
    }

    fn state_path(&self) -> StatePath {
        StatePath::new(["system", "notify", self.notification_id.as_str()])
    }
}
