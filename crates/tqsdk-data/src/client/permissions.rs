use serde_json::Value;

use crate::error::{DataError, Result};

use super::{DataClient, session_error_into_data};

const HISTORY_DOWNLOAD_PERMISSION_MESSAGE: &str = "history data download requires tq_dl permission; upgrade: https://www.shinnytech.com/tqsdk-buy/";

pub(super) fn has_tq_dl_feature(auth_context: &Value) -> Option<bool> {
    let features = auth_context.get("features").and_then(Value::as_array)?;
    Some(
        features
            .iter()
            .filter_map(Value::as_str)
            .any(|feature| feature == "tq_dl"),
    )
}

impl DataClient {
    pub(crate) fn require_history_download_permission(&self) -> Result<()> {
        let Some(session) = self.session.as_ref() else {
            return Ok(());
        };
        let Some(auth_context) = session.auth_context()? else {
            return Ok(());
        };
        match has_tq_dl_feature(&auth_context) {
            Some(true) | None => Ok(()),
            Some(false) => Err(DataError::PermissionDenied(
                HISTORY_DOWNLOAD_PERMISSION_MESSAGE.to_string(),
            )),
        }
    }

    pub(crate) async fn require_history_download_permission_async(
        &self,
        session: &tqsdk_session::SessionClient,
    ) -> Result<()> {
        if let Some(auth_context) = session.auth_context()? {
            return match has_tq_dl_feature(&auth_context) {
                Some(true) => Ok(()),
                Some(false) | None => Err(DataError::PermissionDenied(
                    HISTORY_DOWNLOAD_PERMISSION_MESSAGE.to_string(),
                )),
            };
        }

        match session.has_feature("tq_dl").await {
            Ok(true) => Ok(()),
            Ok(false) => Err(DataError::PermissionDenied(
                HISTORY_DOWNLOAD_PERMISSION_MESSAGE.to_string(),
            )),
            Err(tqsdk_session::SessionFacadeError::InvalidState(_)) => Ok(()),
            Err(error) => Err(session_error_into_data(error)),
        }
    }
}
