#![cfg_attr(not(test), forbid(unsafe_code))]

use std::fmt;

use crate::error::{RelayError, RelayResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UniverseExpression {
    clauses: Vec<UniverseClause>,
}

impl UniverseExpression {
    pub fn parse(value: &str) -> RelayResult<Self> {
        let value = value.trim();
        if value.is_empty() {
            return Err(RelayError::invalid_config(
                "futures universe expression must not be empty",
            ));
        }
        let clauses = value
            .split(';')
            .map(UniverseClause::parse)
            .collect::<RelayResult<Vec<_>>>()?;
        if clauses.is_empty() {
            return Err(RelayError::invalid_config(
                "futures universe expression must not be empty",
            ));
        }
        Ok(Self { clauses })
    }

    #[must_use]
    pub fn clauses(&self) -> &[UniverseClause] {
        &self.clauses
    }

    #[must_use]
    pub fn include_clause_count(&self) -> usize {
        self.clauses.iter().filter(|clause| !clause.exclude).count()
    }

    #[must_use]
    pub fn exclude_clause_count(&self) -> usize {
        self.clauses.iter().filter(|clause| clause.exclude).count()
    }

    #[must_use]
    pub fn is_static_symbol_only(&self) -> bool {
        self.clauses.iter().all(|clause| {
            let static_values = clause.selector.values.iter().all(|value| {
                value.starts_with("KQ.")
                    || value
                        .split_once('.')
                        .is_some_and(|(_, rhs)| rhs.chars().any(|ch| ch.is_ascii_digit()))
            });
            match clause.selector.kind {
                UniverseSelectorKind::Symbol | UniverseSelectorKind::File => true,
                UniverseSelectorKind::Product | UniverseSelectorKind::Exchange
                    if clause.exclude =>
                {
                    true
                }
                UniverseSelectorKind::Auto => clause.exclude || static_values,
                _ => false,
            }
        })
    }
}

impl fmt::Display for UniverseExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, clause) in self.clauses.iter().enumerate() {
            if index > 0 {
                f.write_str(";")?;
            }
            write!(f, "{clause}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UniverseClause {
    exclude: bool,
    selector: UniverseSelector,
}

impl UniverseClause {
    fn parse(value: &str) -> RelayResult<Self> {
        let value = value.trim();
        if value.is_empty() {
            return Err(RelayError::invalid_config(
                "futures universe expression contains empty clause",
            ));
        }
        let (exclude, selector) = match value.as_bytes()[0] {
            b'!' | b'~' => {
                let selector = value[1..].trim();
                if selector.is_empty() {
                    return Err(RelayError::invalid_config(
                        "futures universe exclude clause must include selector",
                    ));
                }
                (true, selector)
            }
            _ => (false, value),
        };
        Ok(Self {
            exclude,
            selector: UniverseSelector::parse(selector)?,
        })
    }

    #[must_use]
    pub fn exclude(&self) -> bool {
        self.exclude
    }

    #[must_use]
    pub fn selector(&self) -> &UniverseSelector {
        &self.selector
    }
}

impl fmt::Display for UniverseClause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.exclude {
            f.write_str("!")?;
        }
        write!(f, "{}", self.selector)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UniverseSelector {
    kind: UniverseSelectorKind,
    values: Vec<String>,
}

impl UniverseSelector {
    fn parse(value: &str) -> RelayResult<Self> {
        let value = value.trim();
        if value.is_empty() {
            return Err(RelayError::invalid_config(
                "futures universe selector must not be empty",
            ));
        }
        if let Some(rest) = value.strip_prefix("top:") {
            let (limit, values) = rest
                .split_once(':')
                .ok_or_else(|| RelayError::invalid_config("top selector must be top:N:values"))?;
            let limit = limit.trim().parse::<usize>().map_err(|err| {
                RelayError::invalid_config(format!("top selector limit must be positive: {err}"))
            })?;
            if limit == 0 {
                return Err(RelayError::invalid_config(
                    "top selector limit must be greater than zero",
                ));
            }
            return Ok(Self {
                kind: UniverseSelectorKind::Top(limit),
                values: parse_values(values)?,
            });
        }
        if let Some((kind, values)) = value.split_once(':') {
            let kind = match kind.trim() {
                "active" => UniverseSelectorKind::Active,
                "main" => UniverseSelectorKind::Main,
                "index" => UniverseSelectorKind::Index,
                "cont" => UniverseSelectorKind::Cont,
                "symbol" => UniverseSelectorKind::Symbol,
                "file" | "symbol-file" => UniverseSelectorKind::File,
                "product" => UniverseSelectorKind::Product,
                "exchange" => UniverseSelectorKind::Exchange,
                other => {
                    return Err(RelayError::invalid_config(format!(
                        "unknown futures universe selector kind {other}"
                    )));
                }
            };
            return Ok(Self {
                kind,
                values: parse_values(values)?,
            });
        }
        Ok(Self {
            kind: UniverseSelectorKind::Auto,
            values: parse_values(value)?,
        })
    }

    #[must_use]
    pub fn kind(&self) -> UniverseSelectorKind {
        self.kind
    }

    #[must_use]
    pub fn values(&self) -> &[String] {
        &self.values
    }
}

impl fmt::Display for UniverseSelector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind {
            UniverseSelectorKind::Active => f.write_str("active:")?,
            UniverseSelectorKind::Main => f.write_str("main:")?,
            UniverseSelectorKind::Index => f.write_str("index:")?,
            UniverseSelectorKind::Cont => f.write_str("cont:")?,
            UniverseSelectorKind::Symbol => f.write_str("symbol:")?,
            UniverseSelectorKind::File => f.write_str("file:")?,
            UniverseSelectorKind::Product => f.write_str("product:")?,
            UniverseSelectorKind::Exchange => f.write_str("exchange:")?,
            UniverseSelectorKind::Top(limit) => write!(f, "top:{limit}:")?,
            UniverseSelectorKind::Auto => {}
        }
        for (index, value) in self.values.iter().enumerate() {
            if index > 0 {
                f.write_str(",")?;
            }
            f.write_str(value)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UniverseSelectorKind {
    Active,
    Main,
    Index,
    Cont,
    Top(usize),
    Symbol,
    File,
    Product,
    Exchange,
    Auto,
}

fn parse_values(value: &str) -> RelayResult<Vec<String>> {
    let value = value.trim();
    if value.is_empty() {
        return Err(RelayError::invalid_config(
            "futures universe selector values must not be empty",
        ));
    }
    value
        .split(',')
        .map(|part| {
            let part = part.trim();
            if part.is_empty() {
                return Err(RelayError::invalid_config(
                    "futures universe selector values must not contain empty value",
                ));
            }
            Ok(part.to_string())
        })
        .collect()
}
