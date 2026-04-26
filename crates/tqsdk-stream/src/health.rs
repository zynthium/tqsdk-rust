#![cfg_attr(not(test), forbid(unsafe_code))]

use tqsdk_core::{Revision, RuntimeReader};

use crate::event::SessionReconnectEvent;
use crate::{Result, StreamFacadeError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamSessionPhase {
    Idle,
    Authenticating,
    Connecting,
    Bootstrapping,
    Running,
    Reconnecting,
    Resyncing,
    Closed,
}

impl StreamSessionPhase {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Authenticating => "authenticating",
            Self::Connecting => "connecting",
            Self::Bootstrapping => "bootstrapping",
            Self::Running => "running",
            Self::Reconnecting => "reconnecting",
            Self::Resyncing => "resyncing",
            Self::Closed => "closed",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "idle" => Ok(Self::Idle),
            "authenticating" => Ok(Self::Authenticating),
            "connecting" => Ok(Self::Connecting),
            "bootstrapping" => Ok(Self::Bootstrapping),
            "running" => Ok(Self::Running),
            "reconnecting" => Ok(Self::Reconnecting),
            "resyncing" => Ok(Self::Resyncing),
            "closed" => Ok(Self::Closed),
            _ => Err(StreamFacadeError::InvalidState("unknown session phase")),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct StreamHealthSnapshot {
    pub revision: Revision,
    pub session_phase: Option<StreamSessionPhase>,
    pub reconnect: Option<SessionReconnectEvent>,
    pub driver_closed: bool,
}

impl StreamHealthSnapshot {
    #[must_use]
    pub fn is_healthy(&self) -> bool {
        matches!(self.session_phase, Some(StreamSessionPhase::Running))
            && !self.driver_closed
            && !self.reconnect_exhausted()
    }

    #[must_use]
    pub fn reconnect_exhausted(&self) -> bool {
        self.reconnect
            .as_ref()
            .is_some_and(|reconnect| reconnect.exhausted)
    }
}

pub(crate) fn read_health(
    reader: &RuntimeReader,
    driver_closed: bool,
) -> Result<StreamHealthSnapshot> {
    let snapshot = reader.read();
    let session_phase = snapshot
        .get_path(&["system", "session", "lifecycle", "phase"])
        .and_then(serde_json::Value::as_str)
        .map(StreamSessionPhase::parse)
        .transpose()?;
    let reconnect =
        snapshot.decode_path::<SessionReconnectEvent>(&["system", "session", "reconnect"])?;

    Ok(StreamHealthSnapshot {
        revision: snapshot.revision(),
        session_phase,
        reconnect,
        driver_closed,
    })
}
