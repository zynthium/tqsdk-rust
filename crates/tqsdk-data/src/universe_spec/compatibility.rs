use std::error::Error;
use std::fmt;

use crate::{HistoricalFillUniverseSpec, UniverseExpression};

use super::{UniverseSpec, UniverseSpecError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UniverseLanguage {
    LegacyV1,
    V2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UniverseEvaluationPolicy {
    LegacySequentialV1,
    SetAlgebraV2,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct UniverseDispatchReport {
    language: UniverseLanguage,
    evaluation_policy: UniverseEvaluationPolicy,
    legacy_rejection: Option<String>,
}

impl UniverseDispatchReport {
    #[must_use]
    pub const fn language(&self) -> UniverseLanguage {
        self.language
    }

    #[must_use]
    pub const fn evaluation_policy(&self) -> UniverseEvaluationPolicy {
        self.evaluation_policy
    }

    #[must_use]
    pub fn legacy_rejection(&self) -> Option<&str> {
        self.legacy_rejection.as_deref()
    }

    const fn legacy() -> Self {
        Self {
            language: UniverseLanguage::LegacyV1,
            evaluation_policy: UniverseEvaluationPolicy::LegacySequentialV1,
            legacy_rejection: None,
        }
    }

    fn v2(legacy_rejection: Option<String>) -> Self {
        Self {
            language: UniverseLanguage::V2,
            evaluation_policy: UniverseEvaluationPolicy::SetAlgebraV2,
            legacy_rejection,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SnapshotUniverseDispatch {
    Legacy {
        expression: UniverseExpression,
        report: UniverseDispatchReport,
    },
    V2 {
        spec: UniverseSpec,
        report: UniverseDispatchReport,
    },
}

impl SnapshotUniverseDispatch {
    #[must_use]
    pub const fn report(&self) -> &UniverseDispatchReport {
        match self {
            Self::Legacy { report, .. } | Self::V2 { report, .. } => report,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum HistoricalUniverseDispatch {
    Legacy {
        spec: HistoricalFillUniverseSpec,
        report: UniverseDispatchReport,
    },
    V2 {
        spec: UniverseSpec,
        report: UniverseDispatchReport,
    },
}

impl HistoricalUniverseDispatch {
    #[must_use]
    pub const fn report(&self) -> &UniverseDispatchReport {
        match self {
            Self::Legacy { report, .. } | Self::V2 { report, .. } => report,
        }
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub enum UniverseCompatibilityError {
    TimelineNotAllowed,
    NoCompatibleLanguage {
        legacy_rejection: Option<String>,
        v2_error: UniverseSpecError,
    },
}

impl fmt::Display for UniverseCompatibilityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TimelineNotAllowed => formatter
                .write_str("timeline Universe is not allowed in a snapshot-only entry point"),
            Self::NoCompatibleLanguage {
                legacy_rejection,
                v2_error,
            } => {
                if let Some(legacy_rejection) = legacy_rejection {
                    write!(
                        formatter,
                        "universe is neither valid legacy syntax ({legacy_rejection}) nor V2 ({v2_error})"
                    )
                } else {
                    write!(formatter, "invalid Universe V2 expression: {v2_error}")
                }
            }
        }
    }
}

impl Error for UniverseCompatibilityError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::NoCompatibleLanguage { v2_error, .. } => Some(v2_error),
            Self::TimelineNotAllowed => None,
        }
    }
}

/// Parses a current-only entry point without changing any valid legacy interpretation.
pub fn parse_snapshot_universe_compatible(
    value: &str,
) -> Result<SnapshotUniverseDispatch, UniverseCompatibilityError> {
    let value = value.trim();
    if value.starts_with("timeline(") {
        return Err(UniverseCompatibilityError::TimelineNotAllowed);
    }
    if value.starts_with("snapshot(") {
        return UniverseSpec::parse_v2(value)
            .map(|spec| SnapshotUniverseDispatch::V2 {
                spec,
                report: UniverseDispatchReport::v2(None),
            })
            .map_err(
                |v2_error| UniverseCompatibilityError::NoCompatibleLanguage {
                    legacy_rejection: None,
                    v2_error,
                },
            );
    }
    match UniverseExpression::parse(value) {
        Ok(expression) => Ok(SnapshotUniverseDispatch::Legacy {
            expression,
            report: UniverseDispatchReport::legacy(),
        }),
        Err(error) => {
            let legacy_rejection = error.to_string();
            UniverseSpec::parse_v2(value)
                .map(|spec| SnapshotUniverseDispatch::V2 {
                    spec,
                    report: UniverseDispatchReport::v2(Some(legacy_rejection.clone())),
                })
                .map_err(
                    |v2_error| UniverseCompatibilityError::NoCompatibleLanguage {
                        legacy_rejection: Some(legacy_rejection),
                        v2_error,
                    },
                )
        }
    }
}

/// Parses a historical entry point using the frozen legacy-first dispatch contract.
pub fn parse_historical_universe_compatible(
    value: &str,
) -> Result<HistoricalUniverseDispatch, UniverseCompatibilityError> {
    let value = value.trim();
    if value.starts_with("snapshot(") {
        return UniverseSpec::parse_v2(value)
            .map(|spec| HistoricalUniverseDispatch::V2 {
                spec,
                report: UniverseDispatchReport::v2(None),
            })
            .map_err(
                |v2_error| UniverseCompatibilityError::NoCompatibleLanguage {
                    legacy_rejection: None,
                    v2_error,
                },
            );
    }
    match HistoricalFillUniverseSpec::parse(value) {
        Ok(spec) => Ok(HistoricalUniverseDispatch::Legacy {
            spec,
            report: UniverseDispatchReport::legacy(),
        }),
        Err(error) => {
            let legacy_rejection = error.to_string();
            UniverseSpec::parse_v2(value)
                .map(|spec| HistoricalUniverseDispatch::V2 {
                    spec,
                    report: UniverseDispatchReport::v2(Some(legacy_rejection.clone())),
                })
                .map_err(
                    |v2_error| UniverseCompatibilityError::NoCompatibleLanguage {
                        legacy_rejection: Some(legacy_rejection),
                        v2_error,
                    },
                )
        }
    }
}
