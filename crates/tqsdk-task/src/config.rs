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
    min_volume: i64,
    max_volume: i64,
}

impl VolumeSplitPolicy {
    #[must_use]
    pub fn new(min_volume: i64, max_volume: i64) -> Result<Self> {
        let policy = Self {
            min_volume,
            max_volume,
        };
        policy.validate()?;
        Ok(policy)
    }

    #[must_use]
    pub fn min_volume(self) -> i64 {
        self.min_volume
    }

    #[must_use]
    pub fn max_volume(self) -> i64 {
        self.max_volume
    }

    fn validate(self) -> Result<()> {
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
    price_mode: PriceMode,
    offset_priority: OffsetPriority,
    split_policy: Option<VolumeSplitPolicy>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TargetPosSchedulerConfig {
    offset_priority: OffsetPriority,
    split_policy: Option<VolumeSplitPolicy>,
}

impl TargetPosConfig {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_price_mode(mut self, mode: PriceMode) -> Self {
        self.price_mode = mode;
        self
    }

    #[must_use]
    pub fn with_offset_priority(mut self, priority: OffsetPriority) -> Self {
        self.offset_priority = priority;
        self
    }

    #[must_use]
    pub fn with_split_policy(mut self, policy: VolumeSplitPolicy) -> Self {
        self.split_policy = Some(policy);
        self
    }

    #[must_use]
    pub fn price_mode(&self) -> PriceMode {
        self.price_mode
    }

    #[must_use]
    pub fn offset_priority(&self) -> OffsetPriority {
        self.offset_priority
    }

    #[must_use]
    pub fn split_policy(&self) -> Option<VolumeSplitPolicy> {
        self.split_policy
    }

    pub(crate) fn set_price_mode(&mut self, mode: PriceMode) {
        self.price_mode = mode;
    }

    pub(crate) fn set_offset_priority(&mut self, priority: OffsetPriority) {
        self.offset_priority = priority;
    }

    pub(crate) fn set_split_policy(&mut self, policy: VolumeSplitPolicy) {
        self.split_policy = Some(policy);
    }
}

impl TargetPosSchedulerConfig {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_offset_priority(mut self, priority: OffsetPriority) -> Self {
        self.offset_priority = priority;
        self
    }

    #[must_use]
    pub fn with_split_policy(mut self, policy: VolumeSplitPolicy) -> Self {
        self.split_policy = Some(policy);
        self
    }

    #[must_use]
    pub fn offset_priority(&self) -> OffsetPriority {
        self.offset_priority
    }

    #[must_use]
    pub fn split_policy(&self) -> Option<VolumeSplitPolicy> {
        self.split_policy
    }

    pub(crate) fn set_offset_priority(&mut self, priority: OffsetPriority) {
        self.offset_priority = priority;
    }

    pub(crate) fn set_split_policy(&mut self, policy: VolumeSplitPolicy) {
        self.split_policy = Some(policy);
    }
}
