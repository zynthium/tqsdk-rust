use super::{UniverseMode, UniverseSpecError, UniverseTarget, UniverseView};

const DOMESTIC_EXCHANGES: [&str; 6] = ["CFFEX", "SHFE", "INE", "DCE", "CZCE", "GFEX"];

pub(super) struct RawUniverseSpec {
    pub(super) mode: UniverseMode,
    pub(super) clauses: Vec<RawClause>,
}

pub(super) struct RawClause {
    pub(super) exclude: bool,
    pub(super) view: Option<UniverseView>,
    pub(super) targets: Vec<UniverseTarget>,
}

pub(super) fn parse(value: &str) -> Result<RawUniverseSpec, UniverseSpecError> {
    let value = value.trim();
    if value.is_empty() {
        return Err(UniverseSpecError::Empty);
    }
    let (mode, body) = parse_wrapper(value)?;
    if body.contains("snapshot(") || body.contains("timeline(") {
        return Err(UniverseSpecError::NestedWrapper);
    }
    let clauses = body
        .split(';')
        .map(parse_clause)
        .collect::<Result<_, _>>()?;
    Ok(RawUniverseSpec { mode, clauses })
}

fn parse_wrapper(value: &str) -> Result<(UniverseMode, &str), UniverseSpecError> {
    for (keyword, mode) in [
        ("snapshot", UniverseMode::Snapshot),
        ("timeline", UniverseMode::Timeline),
    ] {
        let prefix = format!("{keyword}(");
        if let Some(rest) = value.strip_prefix(&prefix) {
            let body = rest
                .strip_suffix(')')
                .ok_or_else(|| UniverseSpecError::InvalidWrapper {
                    value: value.to_string(),
                })?;
            return Ok((mode, body));
        }
        if value.starts_with(keyword) {
            return Err(UniverseSpecError::InvalidWrapper {
                value: value.to_string(),
            });
        }
    }
    Ok((UniverseMode::Snapshot, value))
}

fn parse_clause(value: &str) -> Result<RawClause, UniverseSpecError> {
    let value = value.trim();
    if value.is_empty() {
        return Err(UniverseSpecError::EmptyClause);
    }

    if let Some(value) = value.strip_prefix("except(") {
        return parse_except_clause(value);
    }

    let (exclude, selector) = match value.strip_prefix('!') {
        Some(selector) => (true, selector.trim()),
        None => (false, value),
    };
    if selector.is_empty() {
        return Err(UniverseSpecError::EmptyClause);
    }

    if let Some(rest) = selector.strip_prefix("top:") {
        let (limit, values) =
            rest.split_once(':')
                .ok_or_else(|| UniverseSpecError::InvalidTopLimit {
                    value: rest.to_string(),
                })?;
        let limit = limit
            .trim()
            .parse::<u32>()
            .ok()
            .filter(|limit| *limit > 0)
            .ok_or_else(|| UniverseSpecError::InvalidTopLimit {
                value: limit.trim().to_string(),
            })?;
        return parse_view_clause(exclude, UniverseView::Top(limit), values);
    }

    if let Some((keyword, values)) = selector.split_once(':') {
        let view = match keyword.trim() {
            "contract" => UniverseView::Contract,
            "main" => UniverseView::Main,
            "continuous" | "cont" => UniverseView::Continuous,
            "index" => UniverseView::Index,
            "symbol" => UniverseView::Symbol,
            other => {
                return Err(UniverseSpecError::UnknownView {
                    view: other.to_string(),
                });
            }
        };
        return parse_view_clause(exclude, view, values);
    }

    if !exclude {
        return Err(UniverseSpecError::BarePositiveTarget {
            target: selector.to_string(),
        });
    }
    let target = parse_structural_target(selector, false)?;
    Ok(RawClause {
        exclude,
        view: None,
        targets: vec![target],
    })
}

fn parse_except_clause(value: &str) -> Result<RawClause, UniverseSpecError> {
    let selector = value
        .strip_suffix(')')
        .ok_or_else(|| UniverseSpecError::InvalidTarget {
            target: format!("except({value}"),
            reason: "except clause must end with `)`",
        })?
        .trim();
    let (scope, values) =
        selector
            .split_once(':')
            .ok_or_else(|| UniverseSpecError::InvalidTarget {
                target: selector.to_string(),
                reason: "except requires all:<targets> or view:<targets>",
            })?;

    if scope.trim() == "all" {
        let targets = values
            .split(',')
            .map(|value| parse_structural_target(value.trim(), false))
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(RawClause {
            exclude: true,
            view: None,
            targets,
        });
    }

    parse_clause(&format!("!{selector}"))
}

fn parse_view_clause(
    exclude: bool,
    view: UniverseView,
    values: &str,
) -> Result<RawClause, UniverseSpecError> {
    let targets = if view == UniverseView::Symbol {
        parse_symbol_targets(values)?
    } else {
        values
            .split(',')
            .map(|value| parse_structural_target(value.trim(), true))
            .collect::<Result<Vec<_>, _>>()?
    };
    if targets.is_empty() {
        return Err(UniverseSpecError::InvalidTarget {
            target: values.to_string(),
            reason: "target list must not be empty",
        });
    }
    for target in &targets {
        let supported = match view {
            UniverseView::Contract => !matches!(target, UniverseTarget::Symbol { .. }),
            UniverseView::Main
            | UniverseView::Top(_)
            | UniverseView::Continuous
            | UniverseView::Index => matches!(
                target,
                UniverseTarget::All
                    | UniverseTarget::Exchange { .. }
                    | UniverseTarget::Product { .. }
            ),
            UniverseView::Symbol => matches!(target, UniverseTarget::Symbol { .. }),
        };
        if !supported {
            return Err(UniverseSpecError::UnsupportedTarget {
                view,
                target: target.clone(),
            });
        }
    }
    Ok(RawClause {
        exclude,
        view: Some(view),
        targets,
    })
}

fn parse_symbol_targets(values: &str) -> Result<Vec<UniverseTarget>, UniverseSpecError> {
    values
        .split(',')
        .map(str::trim)
        .map(|symbol| {
            if symbol.is_empty() {
                return Err(UniverseSpecError::InvalidTarget {
                    target: symbol.to_string(),
                    reason: "symbol must not be empty",
                });
            }
            if symbol == "all" {
                return Err(UniverseSpecError::InvalidTarget {
                    target: symbol.to_string(),
                    reason: "symbol view requires an explicit provider symbol",
                });
            }
            if symbol.chars().any(char::is_whitespace) {
                return Err(UniverseSpecError::InvalidTarget {
                    target: symbol.to_string(),
                    reason: "symbol must not contain whitespace",
                });
            }
            Ok(UniverseTarget::Symbol {
                symbol: symbol.to_string(),
            })
        })
        .collect()
}

fn parse_structural_target(
    value: &str,
    allow_all: bool,
) -> Result<UniverseTarget, UniverseSpecError> {
    if value.is_empty() {
        return Err(UniverseSpecError::InvalidTarget {
            target: value.to_string(),
            reason: "target must not be empty",
        });
    }
    if value == "all" {
        return if allow_all {
            Ok(UniverseTarget::All)
        } else {
            Err(UniverseSpecError::InvalidTarget {
                target: value.to_string(),
                reason: "all is not a structural global filter",
            })
        };
    }
    if value.chars().any(char::is_whitespace) {
        return Err(UniverseSpecError::InvalidTarget {
            target: value.to_string(),
            reason: "target must not contain whitespace",
        });
    }
    let (exchange, value_part) =
        value
            .split_once('.')
            .ok_or_else(|| UniverseSpecError::InvalidTarget {
                target: value.to_string(),
                reason: "structural target must be EXCHANGE.* or EXCHANGE.value",
            })?;
    if value_part.contains('.') || value_part.is_empty() {
        return Err(UniverseSpecError::InvalidTarget {
            target: value.to_string(),
            reason: "structural target must contain exactly one dot",
        });
    }
    let exchange =
        normalize_exchange(exchange).ok_or_else(|| UniverseSpecError::InvalidTarget {
            target: value.to_string(),
            reason: "unknown domestic futures exchange",
        })?;
    if value_part == "*" {
        return Ok(UniverseTarget::Exchange { exchange });
    }
    if value_part.contains('*') || value_part.contains([':', ';', ',', '(', ')']) {
        return Err(UniverseSpecError::InvalidTarget {
            target: value.to_string(),
            reason: "invalid structural product or contract token",
        });
    }

    let trailing_digits = value_part
        .as_bytes()
        .iter()
        .rev()
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    if trailing_digits == 0 {
        return Ok(UniverseTarget::Product {
            exchange,
            product: value_part.to_string(),
        });
    }
    if trailing_digits == value_part.len() {
        return Err(UniverseSpecError::InvalidTarget {
            target: value.to_string(),
            reason: "structural contract requires a non-empty product prefix",
        });
    }
    Ok(UniverseTarget::Contract {
        exchange,
        contract: value_part.to_string(),
    })
}

fn normalize_exchange(exchange: &str) -> Option<String> {
    DOMESTIC_EXCHANGES
        .iter()
        .find(|candidate| candidate.eq_ignore_ascii_case(exchange))
        .map(|exchange| (*exchange).to_string())
}
