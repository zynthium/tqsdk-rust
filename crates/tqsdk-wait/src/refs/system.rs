use tqsdk_core::{Notification, NotificationId, ObjectKey, StatePath};

use crate::{api::TqApi, change::ChangeTrackedRef};

/// Lightweight handle to `system/notify/{notification_id}`.
#[derive(Debug, Clone)]
pub struct NotificationRef {
    notification_id: NotificationId,
}

impl NotificationRef {
    #[must_use]
    pub fn new(notification_id: impl Into<String>) -> Self {
        Self {
            notification_id: NotificationId::new(notification_id.into()),
        }
    }

    pub fn load(&self, api: &TqApi) -> crate::error::Result<Notification> {
        self.snapshot(api)?
            .ok_or(crate::error::WaitFacadeError::InvalidState(
                "notification not ready",
            ))
    }

    pub fn snapshot(&self, api: &TqApi) -> crate::error::Result<Option<Notification>> {
        api.driver
            .reader
            .read()
            .decode_path::<Notification>(&["system", "notify", self.notification_id.as_str()])
            .map_err(Into::into)
    }

    pub fn is_ready(&self, api: &TqApi) -> crate::error::Result<bool> {
        Ok(self.snapshot(api)?.is_some())
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
