#![cfg_attr(not(test), forbid(unsafe_code))]

use std::fmt;

use crate::{DataError, Result, UniverseExpression, UniverseSelectorKind};

pub const HISTORICAL_FILL_UNIVERSE_CANONICALIZATION: &str = "tqsdk.historical-fill-universe.v1";

/// Historical cache-fill selection, intentionally separate from the shared
/// current/live [`UniverseExpression`] grammar.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HistoricalFillUniverseSpec {
    ObservedPhysicalAll,
    Timeline(UniverseExpression),
}

impl HistoricalFillUniverseSpec {
    pub fn parse(value: &str) -> Result<Self> {
        let value = value.trim();
        if value == "physical:all" {
            return Ok(Self::ObservedPhysicalAll);
        }

        let Some(inner) = value
            .strip_prefix("timeline(")
            .and_then(|value| value.strip_suffix(')'))
        else {
            return Err(invalid_spec(
                "historical universe must be physical:all or timeline(...)",
            ));
        };
        if inner.contains("timeline(") || inner.contains('(') || inner.contains(')') {
            return Err(invalid_spec(
                "historical universe timeline must not be nested",
            ));
        }
        let expression = UniverseExpression::parse(inner)?;
        validate_timeline_expression(&expression)?;
        Ok(Self::Timeline(expression))
    }

    #[must_use]
    pub fn canonicalization_identity(&self) -> &'static str {
        HISTORICAL_FILL_UNIVERSE_CANONICALIZATION
    }

    #[must_use]
    pub fn timeline_expression(&self) -> Option<&UniverseExpression> {
        match self {
            Self::ObservedPhysicalAll => None,
            Self::Timeline(expression) => Some(expression),
        }
    }
}

impl fmt::Display for HistoricalFillUniverseSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ObservedPhysicalAll => f.write_str("physical:all"),
            Self::Timeline(expression) => write!(f, "timeline({expression})"),
        }
    }
}

fn validate_timeline_expression(expression: &UniverseExpression) -> Result<()> {
    let mut has_include = false;
    for clause in expression.clauses() {
        let kind = clause.selector().kind();
        if !clause.exclude() {
            has_include = true;
        }
        match kind {
            UniverseSelectorKind::Active
            | UniverseSelectorKind::Cont
            | UniverseSelectorKind::Index
            | UniverseSelectorKind::Symbol
            | UniverseSelectorKind::Product
            | UniverseSelectorKind::Exchange => {}
            UniverseSelectorKind::Main | UniverseSelectorKind::Top(_) => {
                return Err(invalid_spec(
                    "historical timeline main/top requires authoritative ranking evidence",
                ));
            }
            UniverseSelectorKind::File | UniverseSelectorKind::Auto => {
                return Err(invalid_spec(
                    "historical timeline requires explicit selector kinds",
                ));
            }
        }
    }
    if !has_include {
        return Err(invalid_spec(
            "historical timeline requires at least one include selector",
        ));
    }
    Ok(())
}

fn invalid_spec(message: impl Into<String>) -> DataError {
    DataError::Validation(message.into())
}
