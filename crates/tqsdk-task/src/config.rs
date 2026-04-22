#![cfg_attr(not(test), forbid(unsafe_code))]

use crate::{Result, TaskError};

#[non_exhaustive]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum PriceMode {
    #[default]
    Active,
    Passive,
}

#[non_exhaustive]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum OffsetPriority {
    #[default]
    TodayYesterdayThenOpenWait,
    TodayYesterdayThenOpen,
    YesterdayThenOpen,
    OpenOnly,
}

impl OffsetPriority {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TodayYesterdayThenOpenWait => "今昨,开",
            Self::TodayYesterdayThenOpen => "今昨开",
            Self::YesterdayThenOpen => "昨开",
            Self::OpenOnly => "开",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VolumeSplitPolicy {
    pub min_volume: i64,
    pub max_volume: i64,
}

impl VolumeSplitPolicy {
    pub(crate) fn validate(self) -> Result<()> {
        if self.min_volume <= 0 || self.max_volume <= 0 {
            return Err(TaskError::Unsupported(
                "split policy volumes must be positive",
            ));
        }
        if self.min_volume > self.max_volume {
            return Err(TaskError::Unsupported(
                "split policy min_volume must not exceed max_volume",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TargetPosConfig {
    pub price_mode: PriceMode,
    pub offset_priority: OffsetPriority,
    pub split_policy: Option<VolumeSplitPolicy>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TargetPosSchedulerConfig {
    pub offset_priority: OffsetPriority,
    pub split_policy: Option<VolumeSplitPolicy>,
}
