//! Immutable, evidence-backed trading-time timelines.
//!
//! A timeline answers wall-clock/trading-clock conversion.  It deliberately
//! does not infer exchange schedules: callers must construct it from a
//! confirmed historical rule and final canonical-minute evidence.

use std::{
    collections::BTreeMap,
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use chrono::NaiveDate;
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    BacktestTickCache, DataError, MinuteKlineCache, MinuteKlineCacheSnapshot, Result,
    backtest_tick_trading_day_range,
};

const TIMELINE_FORMAT_VERSION: u32 = 1;
const TIMELINE_STORAGE_FORMAT_VERSION: u32 = 1;
const TIMELINE_DIRECTORY: &str = "trading-timeline-v1";
const NANOS_PER_MINUTE: i64 = 60_000_000_000;

#[cfg(test)]
mod regression_tests;

#[cfg(test)]
thread_local! {
    static TEST_AFTER_MINUTE_PIN: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
    static TEST_BEFORE_PRODUCT_LOCK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}

/// Direction used by [`TradingTimeline::shift`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradingTimeDirection {
    Forward,
    Backward,
}

/// A half-open wall-clock range for which schedule knowledge is authoritative.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradingTimelineKnownRange {
    pub start_ns: i64,
    pub end_ns: i64,
}

impl TradingTimelineKnownRange {
    pub fn new(start_ns: i64, end_ns: i64) -> Result<Self> {
        if end_ns <= start_ns {
            return Err(validation("timeline known range must be increasing"));
        }
        Ok(Self { start_ns, end_ns })
    }
}

/// A half-open interval that contributes to trading time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradingTimelineInterval {
    pub start_ns: i64,
    pub end_ns: i64,
}

impl TradingTimelineInterval {
    pub fn new(start_ns: i64, end_ns: i64) -> Result<Self> {
        if end_ns <= start_ns {
            return Err(validation("trading timeline interval must be increasing"));
        }
        Ok(Self { start_ns, end_ns })
    }
}

/// One recurring session window relative to the 18:00 CST trading-day anchor.
/// Rule windows are minute-aligned to avoid encoding a tick-level assumption.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradingTimelineRuleWindow {
    pub start_offset_ns: i64,
    pub end_offset_ns: i64,
}

impl TradingTimelineRuleWindow {
    pub fn new(start_offset_ns: i64, end_offset_ns: i64) -> Result<Self> {
        if end_offset_ns <= start_offset_ns || start_offset_ns < 0 {
            return Err(validation(
                "trading timeline rule window must be increasing",
            ));
        }
        if start_offset_ns % 60_000_000_000 != 0 || end_offset_ns % 60_000_000_000 != 0 {
            return Err(validation(
                "trading timeline rule window must be whole minutes",
            ));
        }
        Ok(Self {
            start_offset_ns,
            end_offset_ns,
        })
    }
}

/// One approved, historically-versioned candidate trading-session rule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradingTimelineRule {
    pub rule_id: String,
    pub windows: Vec<TradingTimelineRuleWindow>,
}

impl TradingTimelineRule {
    pub fn new(
        rule_id: impl Into<String>,
        windows: Vec<TradingTimelineRuleWindow>,
    ) -> Result<Self> {
        let mut rule = Self {
            rule_id: required("rule_id", rule_id.into())?,
            windows,
        };
        rule.normalize()?;
        Ok(rule)
    }

    fn normalize(&mut self) -> Result<()> {
        self.rule_id = required("rule_id", self.rule_id.clone())?;
        self.windows
            .sort_unstable_by_key(|window| (window.start_offset_ns, window.end_offset_ns));
        let mut previous_end = None;
        for window in &self.windows {
            TradingTimelineRuleWindow::new(window.start_offset_ns, window.end_offset_ns)?;
            if previous_end.is_some_and(|end| window.start_offset_ns < end) {
                return Err(validation("trading timeline rule windows overlap"));
            }
            previous_end = Some(window.end_offset_ns);
        }
        Ok(())
    }
}

/// A versioned candidate session epoch loaded from a historical rule catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradingTimelineCatalogRule {
    pub exchange: String,
    pub product: String,
    pub evidence_symbol: String,
    pub rule_id: String,
    pub candidate_trading_day_start: Option<String>,
    pub candidate_trading_day_end: Option<String>,
    pub sessions_cst: Vec<[String; 2]>,
    /// Semantic identifiers parallel to `sessions_cst`, such as `night`.
    /// Required before an exception may remove individual sessions.
    #[serde(default)]
    pub session_ids: Vec<String>,
    #[serde(default)]
    pub authority: Option<TradingTimelineCatalogAuthority>,
    pub validation: TradingTimelineCatalogValidation,
}

/// Validation state supplied by a historical rule catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradingTimelineCatalogValidation {
    pub status: String,
    pub required_symbol: String,
    pub required_check: String,
}

/// Primary-source provenance retained in the catalog and its raw hash.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradingTimelineCatalogAuthority {
    #[serde(default)]
    pub source_type: String,
    pub title: String,
    #[serde(default)]
    pub published_at: Option<String>,
    pub url: String,
    pub confidence: String,
}

/// A dated exchange exception which must prevent normal rule selection until
/// it has a materialized replacement schedule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradingTimelineCatalogException {
    pub exchange: String,
    pub trading_day: String,
    pub kind: String,
    pub products: Vec<String>,
    #[serde(default)]
    pub closed: bool,
    #[serde(default)]
    pub session_ids: Vec<String>,
    #[serde(default)]
    pub replacement_sessions_cst: Option<Vec<[String; 2]>>,
    #[serde(default)]
    pub replacement_rule_id: Option<String>,
    #[serde(default)]
    pub authority: Option<TradingTimelineCatalogAuthority>,
    #[serde(default)]
    pub validation: Option<TradingTimelineCatalogValidation>,
}

/// Immutable historical rule catalog.  Candidate rules can aid evidence
/// matching; only `validation.status = "confirmed"` rules may be activated.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradingTimelineRuleCatalog {
    /// Operator attestation that exceptional session changes have been reviewed
    /// for every rule's entire dated scope. Template authority alone is insufficient.
    /// Missing attestation permits audit, never activation.
    #[serde(default)]
    pub exception_review_complete: bool,
    pub catalog_version: u32,
    pub generated_at: String,
    pub timezone: String,
    pub interval_semantics: String,
    pub night_session_semantics: String,
    #[serde(default)]
    pub scope_note: String,
    pub rules: Vec<TradingTimelineCatalogRule>,
    #[serde(default)]
    pub exceptions: Vec<TradingTimelineCatalogException>,
    #[serde(skip)]
    catalog_hash: String,
}

impl TradingTimelineRuleCatalog {
    pub fn from_json_str(input: &str) -> Result<Self> {
        let mut catalog: Self = serde_json::from_str(input)?;
        catalog.normalize()?;
        // Hash normalized runtime inputs, including rule authorities.
        // Research-only fields do not affect the semantic catalog identity.
        catalog.catalog_hash =
            format!("sha256:{:x}", Sha256::digest(serde_json::to_vec(&catalog)?));
        Ok(catalog)
    }

    pub fn from_json_path(path: impl AsRef<Path>) -> Result<Self> {
        Self::from_json_str(&fs::read_to_string(path)?)
    }

    #[must_use]
    pub fn catalog_hash(&self) -> &str {
        &self.catalog_hash
    }

    /// Returns matching candidate rules for one exchange trading day.
    ///
    /// A dated exception fails closed: it cannot silently inherit a normal
    /// session rule until a catalog revision encodes its replacement schedule.
    pub fn candidate_rules(
        &self,
        exchange: &str,
        product: &str,
        trading_day: NaiveDate,
    ) -> Result<Vec<TradingTimelineRule>> {
        let day = trading_day.format("%F").to_string();
        let exceptions = self
            .exceptions
            .iter()
            .filter(|exception| {
                exception.exchange == exchange
                    && exception.trading_day == day
                    && exception_applies_to(exception, product)
            })
            .collect::<Vec<_>>();
        if exceptions.len() > 1 {
            return Err(validation(
                "multiple trading timeline exceptions apply to one product day",
            ));
        }
        let base_rules = self
            .rules
            .iter()
            .filter(|rule| {
                rule.exchange == exchange
                    && rule.product == product
                    && date_in_epoch(
                        day.as_str(),
                        rule.candidate_trading_day_start.as_deref(),
                        rule.candidate_trading_day_end.as_deref(),
                    )
            })
            .collect::<Vec<_>>();
        match exceptions.as_slice() {
            [] => base_rules
                .into_iter()
                .map(catalog_rule_to_timeline_rule)
                .collect(),
            [exception] => apply_catalog_exception(base_rules, exception),
            _ => unreachable!("length checked"),
        }
    }

    /// Returns only rules approved for active timeline publication.
    pub fn confirmed_rules(
        &self,
        exchange: &str,
        product: &str,
        trading_day: NaiveDate,
    ) -> Result<Vec<TradingTimelineRule>> {
        self.candidate_rules(exchange, product, trading_day)
            .map(|rules| {
                rules
                    .into_iter()
                    .filter(|rule| self.effective_rule_is_confirmed(rule.rule_id.as_str()))
                    .collect()
            })
    }

    fn effective_rule_is_confirmed(&self, rule_id: &str) -> bool {
        if rule_id.starts_with("closed:") || rule_id.starts_with("replacement:") {
            return self.exceptions.iter().any(|exception| {
                let exception_id = format!(
                    "{}:{}:{}",
                    exception.exchange, exception.trading_day, exception.kind
                );
                rule_id.ends_with(exception_id.as_str())
                    && exception
                        .validation
                        .as_ref()
                        .is_some_and(|validation| validation.status == "confirmed")
            });
        }
        let base_rule_id = rule_id.split('#').next().unwrap_or(rule_id);
        self.rules.iter().any(|catalog_rule| {
            catalog_rule.rule_id == base_rule_id && catalog_rule.validation.status == "confirmed"
        })
    }

    fn normalize(&mut self) -> Result<()> {
        if self.catalog_version != 1 {
            return Err(validation("unsupported trading timeline catalog version"));
        }
        if self.timezone != "Asia/Shanghai" {
            return Err(validation(
                "trading timeline catalog must use Asia/Shanghai",
            ));
        }
        for rule in &mut self.rules {
            rule.exchange = component("exchange", rule.exchange.clone())?;
            rule.product = component("product", rule.product.clone())?;
            rule.evidence_symbol = required("evidence_symbol", rule.evidence_symbol.clone())?;
            rule.rule_id = required("rule_id", rule.rule_id.clone())?;
            validate_catalog_date(rule.candidate_trading_day_start.as_deref())?;
            validate_catalog_date(rule.candidate_trading_day_end.as_deref())?;
            if let (Some(start), Some(end)) = (
                rule.candidate_trading_day_start.as_deref(),
                rule.candidate_trading_day_end.as_deref(),
            ) && start > end
            {
                return Err(validation("trading timeline rule epoch is inverted"));
            }
            let _ = catalog_rule_to_timeline_rule(rule)?;
            rule.validation.status = required("validation.status", rule.validation.status.clone())?;
            normalize_authority(rule.authority.as_mut())?;
            if !rule.session_ids.is_empty() && rule.session_ids.len() != rule.sessions_cst.len() {
                return Err(validation(
                    "catalog rule session_ids must parallel sessions_cst",
                ));
            }
        }
        self.rules
            .sort_unstable_by(|left, right| left.rule_id.cmp(&right.rule_id));
        if self
            .rules
            .windows(2)
            .any(|pair| pair[0].rule_id == pair[1].rule_id)
        {
            return Err(validation(
                "trading timeline catalog rule ids must be unique",
            ));
        }
        for exception in &mut self.exceptions {
            exception.exchange = component("exchange", exception.exchange.clone())?;
            exception.trading_day =
                required("exception.trading_day", exception.trading_day.clone())?;
            validate_catalog_date(Some(exception.trading_day.as_str()))?;
            exception.kind = required("exception.kind", exception.kind.clone())?;
            normalize_authority(exception.authority.as_mut())?;
            let operations = usize::from(exception.closed)
                + usize::from(!exception.session_ids.is_empty())
                + usize::from(exception.replacement_sessions_cst.is_some());
            if operations != 1 {
                return Err(validation(
                    "trading timeline exception must declare exactly one schedule operation",
                ));
            }
            if exception.replacement_rule_id.is_some()
                && exception.replacement_sessions_cst.is_none()
            {
                return Err(validation(
                    "trading timeline replacement_rule_id requires replacement_sessions_cst",
                ));
            }
            if let Some(validation) = &mut exception.validation {
                validation.status =
                    required("exception.validation.status", validation.status.clone())?;
            }
            if let Some(sessions) = &exception.replacement_sessions_cst {
                for session in sessions {
                    let _ = cst_offset_from_trading_day_anchor(&session[0])?;
                    let _ = cst_offset_from_trading_day_anchor(&session[1])?;
                }
            }
        }
        self.exceptions.sort_unstable_by(|left, right| {
            (&left.exchange, &left.trading_day, &left.kind).cmp(&(
                &right.exchange,
                &right.trading_day,
                &right.kind,
            ))
        });
        Ok(())
    }
}

fn catalog_rule_to_timeline_rule(rule: &TradingTimelineCatalogRule) -> Result<TradingTimelineRule> {
    TradingTimelineRule::new(rule.rule_id.clone(), catalog_rule_windows(rule)?)
}

fn catalog_rule_windows(
    rule: &TradingTimelineCatalogRule,
) -> Result<Vec<TradingTimelineRuleWindow>> {
    rule.sessions_cst
        .iter()
        .map(|pair| {
            TradingTimelineRuleWindow::new(
                cst_offset_from_trading_day_anchor(&pair[0])?,
                cst_offset_from_trading_day_anchor(&pair[1])?,
            )
        })
        .collect()
}

fn apply_catalog_exception(
    base_rules: Vec<&TradingTimelineCatalogRule>,
    exception: &TradingTimelineCatalogException,
) -> Result<Vec<TradingTimelineRule>> {
    if exception
        .validation
        .as_ref()
        .is_none_or(|validation| validation.status != "confirmed")
    {
        return Err(validation("trading timeline exception is not confirmed"));
    }
    let exception_id = format!(
        "{}:{}:{}",
        exception.exchange, exception.trading_day, exception.kind
    );
    if exception.closed {
        return Ok(vec![TradingTimelineRule::new(
            format!("closed:{exception_id}"),
            Vec::new(),
        )?]);
    }
    if let Some(replacement) = &exception.replacement_sessions_cst {
        let windows = replacement
            .iter()
            .map(|session| {
                TradingTimelineRuleWindow::new(
                    cst_offset_from_trading_day_anchor(&session[0])?,
                    cst_offset_from_trading_day_anchor(&session[1])?,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        return Ok(vec![TradingTimelineRule::new(
            format!("replacement:{exception_id}"),
            windows,
        )?]);
    }
    if exception.session_ids.is_empty() {
        return Err(validation(
            "trading timeline exception has no materialized schedule patch",
        ));
    }
    base_rules
        .into_iter()
        .map(|base| {
            if base.session_ids.len() != base.sessions_cst.len() {
                return Err(validation("base rule has no semantic session identifiers"));
            }
            let windows = catalog_rule_windows(base)?
                .into_iter()
                .zip(base.session_ids.iter())
                .filter_map(|(window, session_id)| {
                    (!exception
                        .session_ids
                        .iter()
                        .any(|removed| removed == session_id))
                    .then_some(window)
                })
                .collect();
            TradingTimelineRule::new(format!("{}#{exception_id}", base.rule_id), windows)
        })
        .collect()
}

fn normalize_authority(authority: Option<&mut TradingTimelineCatalogAuthority>) -> Result<()> {
    let Some(authority) = authority else {
        return Ok(());
    };
    authority.source_type = authority.source_type.trim().to_string();
    authority.title = required("authority.title", authority.title.clone())?;
    authority.published_at = authority
        .published_at
        .take()
        .map(|value| required("authority.published_at", value))
        .transpose()?;
    authority.url = required("authority.url", authority.url.clone())?;
    authority.confidence = required("authority.confidence", authority.confidence.clone())?;
    Ok(())
}

fn cst_offset_from_trading_day_anchor(value: &str) -> Result<i64> {
    let (hours, minutes) = value
        .split_once(':')
        .ok_or_else(|| validation("catalog session time must be HH:MM"))?;
    let hours = hours
        .parse::<i64>()
        .map_err(|_| validation("catalog session hour is invalid"))?;
    let minutes = minutes
        .parse::<i64>()
        .map_err(|_| validation("catalog session minute is invalid"))?;
    if !(0..24).contains(&hours) || !(0..60).contains(&minutes) {
        return Err(validation("catalog session time is invalid"));
    }
    let minutes_since_midnight = hours * 60 + minutes;
    let anchor_minutes = 18 * 60;
    let offset_minutes = if minutes_since_midnight >= anchor_minutes {
        minutes_since_midnight - anchor_minutes
    } else {
        minutes_since_midnight + (24 * 60 - anchor_minutes)
    };
    offset_minutes
        .checked_mul(60_000_000_000)
        .ok_or_else(|| validation("catalog session offset overflow"))
}

fn date_in_epoch(day: &str, start: Option<&str>, end: Option<&str>) -> bool {
    start.is_none_or(|value| value <= day) && end.is_none_or(|value| day <= value)
}

fn exception_applies_to(exception: &TradingTimelineCatalogException, product: &str) -> bool {
    exception.products.iter().any(|scope| {
        scope == product
            || scope == &format!("ALL_{}_FUTURES_OPTIONS", exception.exchange)
            || scope == "ALL_PRODUCTS"
    })
}

fn validate_catalog_date(value: Option<&str>) -> Result<()> {
    if let Some(value) = value {
        NaiveDate::parse_from_str(value, "%F")
            .map_err(|_| validation("catalog trading-day date must be YYYY-MM-DD"))?;
    }
    Ok(())
}

/// Rule-inference result for one trading day.
///
/// A sparse canonical index has no negative evidence: a missing minute never
/// removes a candidate which would have been open during that minute.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum TradingTimelineRuleDecision {
    Unique {
        rule_id: String,
    },
    Ambiguous {
        candidate_rule_ids: Vec<String>,
    },
    /// Final minute coverage exists, but the selected candidate has not passed
    /// the catalog's independent activation gate.
    Unconfirmed {
        rule_id: String,
    },
    NoMatch,
}

/// Persisted evidence decision for one trading-day range.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradingTimelineDecisionRecord {
    pub trading_day_start_ns: i64,
    pub trading_day_end_ns: i64,
    pub decision: TradingTimelineRuleDecision,
    /// Pinned minute metadata and consumed source rows for this day.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence_hash: Option<String>,
}

/// Explicit, fail-closed request to compile a timeline from final minute cache.
///
/// `evidence_identity` is a required compatibility label. Builders derive the
/// resulting evidence identity from the pinned minute snapshot and actual rows;
/// the label is not trusted as proof of source identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradingTimelineBuildRequest {
    pub exchange: String,
    pub product: String,
    pub evidence_symbol: String,
    pub evidence_identity: String,
    pub trading_days: Vec<NaiveDate>,
}

impl TradingTimelineBuildRequest {
    pub fn new(
        exchange: impl Into<String>,
        product: impl Into<String>,
        evidence_symbol: impl Into<String>,
        evidence_identity: impl Into<String>,
        mut trading_days: Vec<NaiveDate>,
    ) -> Result<Self> {
        trading_days.sort_unstable();
        trading_days.dedup();
        if trading_days.is_empty() {
            return Err(validation("trading timeline build requires trading days"));
        }
        Ok(Self {
            exchange: component("exchange", exchange.into())?,
            product: component("product", product.into())?,
            evidence_symbol: required("evidence_symbol", evidence_symbol.into())?,
            evidence_identity: required("evidence_identity", evidence_identity.into())?,
            trading_days,
        })
    }
}

/// Infers one historical rule using only positive final canonical-minute rows.
pub fn infer_trading_timeline_rule(
    trading_day_start_ns: i64,
    rules: &[TradingTimelineRule],
    observed_minute_start_ns: &[i64],
) -> Result<TradingTimelineRuleDecision> {
    if observed_minute_start_ns.is_empty() {
        let mut empty_rule_ids = rules
            .iter()
            .filter(|rule| rule.windows.is_empty())
            .map(|rule| rule.rule_id.clone())
            .collect::<Vec<_>>();
        empty_rule_ids.sort();
        empty_rule_ids.dedup();
        if empty_rule_ids.len() == 1 && empty_rule_ids.len() == rules.len() {
            return Ok(TradingTimelineRuleDecision::Unique {
                rule_id: empty_rule_ids.pop().expect("length checked"),
            });
        }
        let mut candidate_rule_ids = rules
            .iter()
            .map(|rule| rule.rule_id.clone())
            .collect::<Vec<_>>();
        candidate_rule_ids.sort();
        candidate_rule_ids.dedup();
        return Ok(TradingTimelineRuleDecision::Ambiguous { candidate_rule_ids });
    }
    let mut candidates = Vec::new();
    for candidate in rules {
        let mut normalized = candidate.clone();
        normalized.normalize()?;
        let matches = observed_minute_start_ns.iter().all(|timestamp_ns| {
            normalized.windows.iter().any(|window| {
                trading_day_start_ns
                    .checked_add(window.start_offset_ns)
                    .zip(trading_day_start_ns.checked_add(window.end_offset_ns))
                    .is_some_and(|(start_ns, end_ns)| {
                        start_ns <= *timestamp_ns && *timestamp_ns < end_ns
                    })
            })
        });
        if matches {
            candidates.push(normalized.rule_id);
        }
    }
    candidates.sort();
    candidates.dedup();
    Ok(match candidates.len() {
        0 => TradingTimelineRuleDecision::NoMatch,
        1 => TradingTimelineRuleDecision::Unique {
            rule_id: candidates.pop().expect("length checked"),
        },
        _ => TradingTimelineRuleDecision::Ambiguous {
            candidate_rule_ids: candidates,
        },
    })
}

/// Compiles only uniquely proven days into a queryable timeline.
///
/// `Ambiguous` and `NoMatch` records remain in the snapshot for audit, but
/// deliberately contribute neither known wall-clock coverage nor open
/// intervals. Traversal touching such a day fails closed.
/// Rebuild a product timeline using only complete, final canonical-minute cache
/// coverage for the requested complete trading days.
///
/// Candidate matching consumes positive minute rows only. A uniquely matched
/// candidate remains [`TradingTimelineRuleDecision::Unconfirmed`] until its
/// independent catalog validation is confirmed; such a day deliberately does
/// not become queryable timeline coverage.
pub fn build_trading_timeline_from_minute_cache(
    cache: &MinuteKlineCache,
    snapshot: &MinuteKlineCacheSnapshot,
    catalog: &TradingTimelineRuleCatalog,
    request: TradingTimelineBuildRequest,
) -> Result<TradingTimelineSnapshot> {
    let _gate = BacktestTickCache::open_read_only(cache.root_dir())
        .try_acquire_remote_fill_shared_lock()?;
    let start = request
        .trading_days
        .iter()
        .min()
        .ok_or_else(|| validation("timeline rebuild requires trading days"))?;
    let end = request.trading_days.iter().max().expect("nonempty days");
    let _minute_pin = cache.pin_final_ranges(&[(
        request.evidence_symbol.clone(),
        backtest_tick_trading_day_range(*start)?.start_ns,
        backtest_tick_trading_day_range(*end)?.end_ns,
    )])?;
    build_trading_timeline_locked(cache, snapshot, catalog, request)
}

// Separate night and day civil anchors. A Monday storage range starts Friday,
// whereas its day-session template is relative to Sunday 18:00.
fn dated_rules(
    mut rules: Vec<TradingTimelineRule>,
    start_ns: i64,
    end_ns: i64,
) -> Result<Vec<TradingTimelineRule>> {
    const HOUR: i64 = 3_600_000_000_000;
    let day_anchor = end_ns
        .checked_sub(24 * HOUR)
        .ok_or_else(|| validation("timeline day anchor overflow"))?;
    let gap = day_anchor
        .checked_sub(start_ns)
        .ok_or_else(|| validation("timeline day gap overflow"))?;
    for rule in &mut rules {
        for window in &mut rule.windows {
            // Template convention: night/day split at 06:00 CST. This is not
            // an exchange calendar; admissible sessions still come from rules.
            if window.start_offset_ns >= 12 * HOUR {
                window.start_offset_ns = window
                    .start_offset_ns
                    .checked_add(gap)
                    .ok_or_else(|| validation("timeline session overflow"))?;
                window.end_offset_ns = window
                    .end_offset_ns
                    .checked_add(gap)
                    .ok_or_else(|| validation("timeline session overflow"))?;
            } else if window.end_offset_ns > 12 * HOUR {
                return Err(validation("session template straddles night/day boundary"));
            }
        }
    }
    Ok(rules)
}

fn build_trading_timeline_locked(
    cache: &MinuteKlineCache,
    snapshot: &MinuteKlineCacheSnapshot,
    catalog: &TradingTimelineRuleCatalog,
    request: TradingTimelineBuildRequest,
) -> Result<TradingTimelineSnapshot> {
    let validated = TradingTimelineRuleCatalog::from_json_str(&serde_json::to_string(catalog)?)?;
    if validated != *catalog {
        return Err(validation(
            "catalog was modified after validation; reload it before rebuilding",
        ));
    }
    let request = TradingTimelineBuildRequest::new(
        request.exchange,
        request.product,
        request.evidence_symbol,
        request.evidence_identity,
        request.trading_days,
    )?;
    if request.evidence_symbol != format!("KQ.i@{}.{}", request.exchange, request.product) {
        return Err(validation(
            "timeline evidence must be the product's canonical index",
        ));
    }
    let mut months = BTreeMap::<String, Vec<_>>::new();
    for day in &request.trading_days {
        let range = backtest_tick_trading_day_range(*day)?;
        if range.trading_day != *day {
            return Err(validation(
                "timeline trading day must not be a weekend alias",
            ));
        }
        months
            .entry(day.format("%Y%m").to_string())
            .or_default()
            .push((*day, range));
    }
    let mut rules = BTreeMap::new();
    let mut decisions = Vec::with_capacity(request.trading_days.len());
    let snapshot_bytes = serde_json::to_vec(snapshot)?;
    for days in months.into_values() {
        let mut reader = cache.open_reader(
            &request.evidence_symbol,
            days.first().expect("nonempty month").1.start_ns,
            days.last().expect("nonempty month").1.end_ns,
            snapshot,
        )?;
        let mut next = reader.next_kline()?;
        for (trading_day, day_range) in days {
            let mut evidence = Sha256::new();
            evidence.update(&snapshot_bytes);
            let candidates = dated_rules(
                catalog.candidate_rules(&request.exchange, &request.product, trading_day)?,
                day_range.start_ns,
                day_range.end_ns,
            )?;
            let mut observed = Vec::new();
            while next
                .as_ref()
                .is_some_and(|row| row.datetime < day_range.end_ns)
            {
                let row = next.take().expect("checked row");
                if row.datetime >= day_range.start_ns {
                    evidence.update(serde_json::to_vec(&row)?);
                    if row.volume > 0 {
                        observed.push(row.datetime);
                    }
                }
                next = reader.next_kline()?;
            }
            let inferred = infer_trading_timeline_rule(day_range.start_ns, &candidates, &observed)?;
            let decision = match inferred {
                TradingTimelineRuleDecision::Unique { rule_id } => {
                    let confirmed = catalog.confirmed_rules(
                        &request.exchange,
                        &request.product,
                        trading_day,
                    )?;
                    let all_sessions_observed = candidates
                        .iter()
                        .find(|rule| rule.rule_id == rule_id)
                        .is_some_and(|rule| {
                            rule.windows.iter().all(|window| {
                                observed.iter().any(|timestamp| {
                                    day_range
                                        .start_ns
                                        .checked_add(window.start_offset_ns)
                                        .zip(day_range.start_ns.checked_add(window.end_offset_ns))
                                        .is_some_and(|(start, end)| {
                                            start <= *timestamp && *timestamp < end
                                        })
                                })
                            })
                        });
                    if confirmed.iter().any(|rule| rule.rule_id == rule_id) && all_sessions_observed
                    {
                        let dated_id = format!("{rule_id}@{trading_day}");
                        let mut dated = candidates
                            .iter()
                            .find(|rule| rule.rule_id == rule_id)
                            .expect("matched candidate")
                            .clone();
                        dated.rule_id = dated_id.clone();
                        rules.insert(dated_id.clone(), dated);
                        TradingTimelineRuleDecision::Unique { rule_id: dated_id }
                    } else {
                        TradingTimelineRuleDecision::Unconfirmed { rule_id }
                    }
                }
                other => other,
            };
            decisions.push(TradingTimelineDecisionRecord {
                trading_day_start_ns: day_range.start_ns,
                trading_day_end_ns: day_range.end_ns,
                decision,
                evidence_hash: Some(format!("sha256:{:x}", evidence.finalize())),
            });
        }
    }
    let rules = rules.into_values().collect::<Vec<_>>();
    compile_trading_timeline(
        TradingTimelineIdentity::new(
            request.exchange,
            request.product,
            request.evidence_symbol,
            catalog.catalog_hash(),
            decision_evidence_identity(&decisions)?,
        )?,
        &rules,
        decisions,
    )
}

pub fn compile_trading_timeline(
    identity: TradingTimelineIdentity,
    rules: &[TradingTimelineRule],
    decisions: Vec<TradingTimelineDecisionRecord>,
) -> Result<TradingTimelineSnapshot> {
    let mut by_id = std::collections::BTreeMap::new();
    for rule in rules {
        let mut normalized = rule.clone();
        normalized.normalize()?;
        if by_id
            .insert(normalized.rule_id.clone(), normalized)
            .is_some()
        {
            return Err(validation("trading timeline rule ids must be unique"));
        }
    }

    let mut known_ranges = Vec::new();
    let mut open_intervals = Vec::new();
    for record in &decisions {
        if record.trading_day_end_ns <= record.trading_day_start_ns {
            return Err(validation(
                "trading timeline decision range must be increasing",
            ));
        }
        let TradingTimelineRuleDecision::Unique { rule_id } = &record.decision else {
            continue;
        };
        let rule = by_id
            .get(rule_id)
            .ok_or_else(|| validation("unique timeline decision references unknown rule"))?;
        known_ranges.push(TradingTimelineKnownRange::new(
            record.trading_day_start_ns,
            record.trading_day_end_ns,
        )?);
        for window in &rule.windows {
            let start_ns = record
                .trading_day_start_ns
                .checked_add(window.start_offset_ns)
                .ok_or_else(|| validation("trading timeline window overflow"))?;
            let end_ns = record
                .trading_day_start_ns
                .checked_add(window.end_offset_ns)
                .ok_or_else(|| validation("trading timeline window overflow"))?;
            if end_ns > record.trading_day_end_ns {
                return Err(validation(
                    "trading timeline rule window exceeds trading day",
                ));
            }
            open_intervals.push(TradingTimelineInterval::new(start_ns, end_ns)?);
        }
    }
    TradingTimelineSnapshot::new(identity, known_ranges, open_intervals)?.with_decisions(decisions)
}

/// Product-level identities that make a timeline generation reproducible.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradingTimelineIdentity {
    pub exchange: String,
    pub product: String,
    pub evidence_symbol: String,
    pub rule_catalog_hash: String,
    pub evidence_identity: String,
}

impl TradingTimelineIdentity {
    pub fn new(
        exchange: impl Into<String>,
        product: impl Into<String>,
        evidence_symbol: impl Into<String>,
        rule_catalog_hash: impl Into<String>,
        evidence_identity: impl Into<String>,
    ) -> Result<Self> {
        let identity = Self {
            exchange: component("exchange", exchange.into())?,
            product: component("product", product.into())?,
            evidence_symbol: required("evidence_symbol", evidence_symbol.into())?,
            rule_catalog_hash: required("rule_catalog_hash", rule_catalog_hash.into())?,
            evidence_identity: required("evidence_identity", evidence_identity.into())?,
        };
        Ok(identity)
    }
}

/// Canonical expanded in-memory representation of a timeline generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradingTimelineSnapshot {
    pub format_version: u32,
    pub timeline_hash: String,
    pub identity: TradingTimelineIdentity,
    pub known_ranges: Vec<TradingTimelineKnownRange>,
    pub open_intervals: Vec<TradingTimelineInterval>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub decisions: Vec<TradingTimelineDecisionRecord>,
}

impl TradingTimelineSnapshot {
    pub fn new(
        identity: TradingTimelineIdentity,
        known_ranges: Vec<TradingTimelineKnownRange>,
        open_intervals: Vec<TradingTimelineInterval>,
    ) -> Result<Self> {
        let mut snapshot = Self {
            format_version: TIMELINE_FORMAT_VERSION,
            timeline_hash: String::new(),
            identity,
            known_ranges,
            open_intervals,
            decisions: Vec::new(),
        };
        snapshot.normalize()?;
        snapshot.timeline_hash = snapshot_hash(&snapshot)?;
        Ok(snapshot)
    }

    pub fn with_decisions(mut self, decisions: Vec<TradingTimelineDecisionRecord>) -> Result<Self> {
        self.decisions = decisions;
        self.timeline_hash.clear();
        self.normalize()?;
        self.timeline_hash = snapshot_hash(&self)?;
        Ok(self)
    }

    pub fn verify(&self) -> Result<()> {
        let mut normalized = self.clone();
        let claimed = normalized.timeline_hash.clone();
        normalized.timeline_hash.clear();
        normalized.normalize()?;
        if self
            != &(Self {
                timeline_hash: claimed.clone(),
                ..normalized.clone()
            })
        {
            return Err(validation("trading timeline snapshot is not canonical"));
        }
        if claimed != snapshot_hash(&normalized)? {
            return Err(validation("trading timeline snapshot hash mismatch"));
        }
        Ok(())
    }

    fn normalize(&mut self) -> Result<()> {
        if self.format_version != TIMELINE_FORMAT_VERSION {
            return Err(validation("unsupported trading timeline format version"));
        }
        self.identity = TradingTimelineIdentity::new(
            self.identity.exchange.clone(),
            self.identity.product.clone(),
            self.identity.evidence_symbol.clone(),
            self.identity.rule_catalog_hash.clone(),
            self.identity.evidence_identity.clone(),
        )?;
        self.known_ranges = normalize_ranges(&self.known_ranges, "timeline known range")?;
        self.open_intervals = normalize_ranges(&self.open_intervals, "trading timeline interval")?;
        self.decisions.sort_unstable_by_key(|record| {
            (record.trading_day_start_ns, record.trading_day_end_ns)
        });
        for record in &self.decisions {
            if record.trading_day_end_ns <= record.trading_day_start_ns {
                return Err(validation(
                    "trading timeline decision range must be increasing",
                ));
            }
            if matches!(record.decision, TradingTimelineRuleDecision::Unique { .. })
                && !range_contains(
                    &self.known_ranges,
                    record.trading_day_start_ns,
                    record.trading_day_end_ns,
                )
            {
                return Err(validation(
                    "trading timeline decision is outside known coverage",
                ));
            }
        }
        for interval in &self.open_intervals {
            if !range_contains(&self.known_ranges, interval.start_ns, interval.end_ns) {
                return Err(validation(
                    "trading interval is outside known timeline coverage",
                ));
            }
        }
        Ok(())
    }
}

/// Immutable compiled timeline.  Construction performs all normalization;
/// duration and shift queries are allocation-free and use binary search.
#[derive(Debug, Clone)]
pub struct TradingTimeline {
    snapshot: TradingTimelineSnapshot,
    cumulative_end_ns: Vec<u128>,
}

impl TradingTimeline {
    pub fn from_snapshot(snapshot: TradingTimelineSnapshot) -> Result<Self> {
        snapshot.verify()?;
        let mut cumulative_end_ns = Vec::with_capacity(snapshot.open_intervals.len());
        let mut total = 0_u128;
        for interval in &snapshot.open_intervals {
            total = total
                .checked_add((i128::from(interval.end_ns) - i128::from(interval.start_ns)) as u128)
                .ok_or_else(|| validation("trading timeline duration overflow"))?;
            cumulative_end_ns.push(total);
        }
        Ok(Self {
            snapshot,
            cumulative_end_ns,
        })
    }

    #[must_use]
    pub fn snapshot(&self) -> &TradingTimelineSnapshot {
        &self.snapshot
    }

    /// Returns trading duration in a forward, fully known wall-clock range.
    pub fn trading_duration_between(&self, start_ns: i64, end_ns: i64) -> Result<Duration> {
        if end_ns < start_ns {
            return Err(validation("trading duration requires start_ns <= end_ns"));
        }
        self.require_known(start_ns, end_ns)?;
        let nanos = self.trading_offset_at(end_ns)? - self.trading_offset_at(start_ns)?;
        duration_from_nanos(nanos)
    }

    /// Moves an anchor by trading time while requiring a fully known path.
    pub fn shift(
        &self,
        anchor_ns: i64,
        duration: Duration,
        direction: TradingTimeDirection,
    ) -> Result<i64> {
        self.require_known_point(anchor_ns)?;
        let nanos = duration.as_nanos();
        if nanos == 0 {
            return Ok(anchor_ns);
        }
        let target = match direction {
            TradingTimeDirection::Forward => self
                .trading_offset_at(anchor_ns)?
                .checked_add(nanos)
                .ok_or_else(|| validation("trading timeline shift overflow"))?,
            TradingTimeDirection::Backward => self
                .trading_offset_at(anchor_ns)?
                .checked_sub(nanos)
                .ok_or_else(|| validation("trading timeline shift precedes known trading time"))?,
        };
        let result = self.wall_time_at_offset(target, direction)?;
        let (start, end) = match direction {
            TradingTimeDirection::Forward => (anchor_ns, result),
            TradingTimeDirection::Backward => (result, anchor_ns),
        };
        self.require_known(start, end)?;
        Ok(result)
    }

    fn require_known_point(&self, timestamp_ns: i64) -> Result<()> {
        let index = self
            .snapshot
            .known_ranges
            .partition_point(|range| range.end_ns < timestamp_ns);
        if self
            .snapshot
            .known_ranges
            .get(index)
            .is_some_and(|range| range.start_ns <= timestamp_ns && timestamp_ns <= range.end_ns)
        {
            Ok(())
        } else {
            Err(validation("trading timeline does not cover anchor"))
        }
    }

    fn require_known(&self, start_ns: i64, end_ns: i64) -> Result<()> {
        if end_ns < start_ns {
            return Err(validation("timeline coverage requires start_ns <= end_ns"));
        }
        if start_ns == end_ns {
            return self.require_known_point(start_ns);
        }
        if range_contains(&self.snapshot.known_ranges, start_ns, end_ns) {
            Ok(())
        } else {
            Err(validation(
                "trading timeline has unknown wall-clock coverage",
            ))
        }
    }

    fn trading_offset_at(&self, timestamp_ns: i64) -> Result<u128> {
        let intervals = &self.snapshot.open_intervals;
        let index = intervals.partition_point(|interval| interval.end_ns <= timestamp_ns);
        let prior = index
            .checked_sub(1)
            .and_then(|value| self.cumulative_end_ns.get(value).copied())
            .unwrap_or(0);
        let Some(interval) = intervals.get(index) else {
            return Ok(prior);
        };
        if timestamp_ns <= interval.start_ns {
            return Ok(prior);
        }
        let contribution =
            (i128::from(timestamp_ns.min(interval.end_ns)) - i128::from(interval.start_ns)) as u128;
        prior
            .checked_add(contribution)
            .ok_or_else(|| validation("trading timeline duration overflow"))
    }

    fn wall_time_at_offset(&self, offset_ns: u128, direction: TradingTimeDirection) -> Result<i64> {
        if self.cumulative_end_ns.is_empty() {
            return Err(validation("trading timeline contains no open intervals"));
        }
        let index = match direction {
            // At an exact close, a forward traversal lands on that close.
            TradingTimeDirection::Forward => self
                .cumulative_end_ns
                .partition_point(|total| *total < offset_ns),
            // At a break boundary, a backward traversal lands at the next
            // session's open: this keeps `close - session_length` intuitive.
            TradingTimeDirection::Backward => self
                .cumulative_end_ns
                .partition_point(|total| *total <= offset_ns),
        };
        let index = index.min(self.snapshot.open_intervals.len().saturating_sub(1));
        let prior = index
            .checked_sub(1)
            .and_then(|value| self.cumulative_end_ns.get(value).copied())
            .unwrap_or(0);
        let interval = self.snapshot.open_intervals[index];
        let inside = offset_ns
            .checked_sub(prior)
            .ok_or_else(|| validation("invalid timeline offset"))?;
        let width = (i128::from(interval.end_ns) - i128::from(interval.start_ns)) as u128;
        if inside > width {
            return Err(validation(
                "trading timeline shift exceeds known trading time",
            ));
        }
        let point = (interval.start_ns as i128)
            .checked_add(inside as i128)
            .ok_or_else(|| validation("trading timeline wall-clock overflow"))?;
        i64::try_from(point).map_err(|_| validation("trading timeline wall-clock overflow"))
    }
}

/// Filesystem store for product-level timeline generations.
#[derive(Debug, Clone)]
pub struct TradingTimelineStore {
    root: PathBuf,
}

/// Compact, single-file on-disk representation. The public snapshot remains the
/// canonical in-memory/reproducibility model; this private codec only removes
/// repeated JSON keys and stores minute-aligned wall-clock values as minutes.
#[derive(Debug, Serialize, Deserialize)]
struct CompactTimelineFile {
    format_version: u32,
    timeline_hash: String,
    identity: TradingTimelineIdentity,
    known_ranges_minutes: Vec<[i64; 2]>,
    open_intervals_minutes: Vec<[i64; 2]>,
    /// [trading-day start minute, end minute, dated rule id, evidence hash].
    decisions: Vec<(i64, i64, String, Option<String>)>,
}

impl CompactTimelineFile {
    fn from_snapshot(snapshot: &TradingTimelineSnapshot) -> Result<Self> {
        snapshot.verify()?;
        Ok(Self {
            format_version: TIMELINE_STORAGE_FORMAT_VERSION,
            timeline_hash: snapshot.timeline_hash.clone(),
            identity: snapshot.identity.clone(),
            known_ranges_minutes: snapshot
                .known_ranges
                .iter()
                .map(|range| minute_pair(range.start_ns, range.end_ns))
                .collect::<Result<_>>()?,
            open_intervals_minutes: snapshot
                .open_intervals
                .iter()
                .map(|range| minute_pair(range.start_ns, range.end_ns))
                .collect::<Result<_>>()?,
            decisions: snapshot
                .decisions
                .iter()
                .map(|record| match &record.decision {
                    TradingTimelineRuleDecision::Unique { rule_id } => Ok((
                        minute_value(record.trading_day_start_ns)?,
                        minute_value(record.trading_day_end_ns)?,
                        rule_id.clone(),
                        record.evidence_hash.clone(),
                    )),
                    _ => Err(validation(
                        "compact timeline cannot persist unresolved decisions",
                    )),
                })
                .collect::<Result<_>>()?,
        })
    }

    fn into_snapshot(self) -> Result<TradingTimelineSnapshot> {
        if self.format_version != TIMELINE_STORAGE_FORMAT_VERSION {
            return Err(validation("unsupported compact trading timeline format"));
        }
        let known_ranges = self
            .known_ranges_minutes
            .into_iter()
            .map(|pair| TradingTimelineKnownRange::new(minute_ns(pair[0])?, minute_ns(pair[1])?))
            .collect::<Result<_>>()?;
        let open_intervals = self
            .open_intervals_minutes
            .into_iter()
            .map(|pair| TradingTimelineInterval::new(minute_ns(pair[0])?, minute_ns(pair[1])?))
            .collect::<Result<_>>()?;
        let decisions = self
            .decisions
            .into_iter()
            .map(|(start, end, rule_id, evidence_hash)| {
                Ok(TradingTimelineDecisionRecord {
                    trading_day_start_ns: minute_ns(start)?,
                    trading_day_end_ns: minute_ns(end)?,
                    decision: TradingTimelineRuleDecision::Unique { rule_id },
                    evidence_hash,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let snapshot = TradingTimelineSnapshot::new(self.identity, known_ranges, open_intervals)?
            .with_decisions(decisions)?;
        if snapshot.timeline_hash != self.timeline_hash {
            return Err(validation("compact timeline hash mismatch"));
        }
        Ok(snapshot)
    }
}

/// Rename committed the visible timeline file, but directory durability is
/// uncertain. Carried as the inner error of `DataError::Io`; reload it rather
/// than assuming the previous generation remains selected.
#[derive(Debug)]
pub struct TradingTimelineDurabilityUncertain {
    pub path: PathBuf,
    pub message: String,
}

impl std::fmt::Display for TradingTimelineDurabilityUncertain {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "timeline rename committed at {}; durability uncertain: {}",
            self.path.display(),
            self.message
        )
    }
}
impl std::error::Error for TradingTimelineDurabilityUncertain {}

#[cfg(test)]
std::thread_local! {
    static TEST_FAIL_BODY_DIRECTORY_SYNC: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static TEST_FAIL_ACTIVE_DIRECTORY_SYNC: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

impl TradingTimelineStore {
    /// Open an existing root without creating a timeline directory.
    pub fn open_read_only(cache_root: impl AsRef<Path>) -> Self {
        Self {
            root: cache_root.as_ref().join(TIMELINE_DIRECTORY),
        }
    }

    /// Rebuild final closed days under a shared root gate and pinned minute partitions.
    /// With `activate = false`, returns audit candidates without changing active.
    /// Products are independent: all candidates are validated before activation;
    /// filesystem failure during activation may leave different product generations.
    pub fn rebuild_from_cache(
        &self,
        catalog: &TradingTimelineRuleCatalog,
        requests: Vec<TradingTimelineBuildRequest>,
        activate: bool,
    ) -> Result<Vec<TradingTimelineSnapshot>> {
        if activate && !catalog.exception_review_complete {
            return Err(validation(
                "timeline activation refused: exception review incomplete",
            ));
        }
        let root = self
            .root
            .parent()
            .ok_or_else(|| validation("missing cache root"))?;
        let root_cache = BacktestTickCache::open_read_only(root);
        let _gate = if activate {
            root_cache.try_acquire_remote_fill_shared_lock()?
        } else {
            root_cache.try_acquire_live_read_shared_lock()?
        };
        let cache = MinuteKlineCache::open_read_only(root);
        let ranges = requests
            .iter()
            .map(|request| {
                let start = request
                    .trading_days
                    .iter()
                    .min()
                    .ok_or_else(|| validation("timeline rebuild requires trading days"))?;
                let end = request.trading_days.iter().max().expect("nonempty days");
                Ok((
                    request.evidence_symbol.clone(),
                    backtest_tick_trading_day_range(*start)?.start_ns,
                    backtest_tick_trading_day_range(*end)?.end_ns,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        // Pin every partition before metadata resolution or coverage inspection.
        // These guards remain alive through merging and publication.
        let _minute_pin = cache.pin_final_ranges(&ranges)?;
        #[cfg(test)]
        TEST_AFTER_MINUTE_PIN.with(|hook| {
            if let Some(hook) = hook.borrow_mut().take() {
                hook();
            }
        });
        let now = chrono::Utc::now()
            .timestamp_nanos_opt()
            .ok_or_else(|| validation("current timestamp overflow"))?;
        let mut built = Vec::new();
        let mut products = std::collections::BTreeSet::new();
        for request in requests {
            if !products.insert((request.exchange.clone(), request.product.clone())) {
                return Err(validation("duplicate product in timeline rebuild"));
            }
            let start = request
                .trading_days
                .iter()
                .min()
                .ok_or_else(|| validation("timeline rebuild requires trading days"))?;
            let end = request.trading_days.iter().max().expect("nonempty request");
            let start_ns = backtest_tick_trading_day_range(*start)?.start_ns;
            let end_ns = backtest_tick_trading_day_range(*end)?.end_ns;
            if end_ns > now {
                return Err(validation("timeline requires closed final days"));
            }
            let metadata = crate::resolve_minute_cache_metadata_snapshot(
                root,
                &request.evidence_symbol,
                start_ns,
                end_ns,
            )?;
            let snapshot = metadata
                .map(|metadata| {
                    MinuteKlineCacheSnapshot::new(
                        metadata.schema_version,
                        metadata.snapshot_hash,
                        metadata.session.snapshot_hash(),
                    )
                })
                .transpose()?
                .unwrap_or_else(MinuteKlineCacheSnapshot::cst_v1);
            let candidate = build_trading_timeline_locked(&cache, &snapshot, catalog, request)?;
            if activate
                && candidate.decisions.iter().any(|record| {
                    !matches!(record.decision, TradingTimelineRuleDecision::Unique { .. })
                })
            {
                return Err(validation(
                    "timeline activation refused: unresolved session days; audit without activation",
                ));
            }
            built.push(candidate);
        }
        if activate {
            // Validate every product before replacing a visible timeline file.
            // Lock the whole product set in stable order before loading it.
            // Keep all candidates validated before the first replacement.
            #[cfg(test)]
            TEST_BEFORE_PRODUCT_LOCK.with(|hook| {
                if let Some(hook) = hook.borrow_mut().take() {
                    hook();
                }
            });
            let mut publish_locks = BTreeMap::new();
            for candidate in &built {
                publish_locks.insert(self.product_root(&candidate.identity)?, None);
            }
            for (product_root, lock) in &mut publish_locks {
                fs::create_dir_all(product_root)?;
                let file = fs::OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .read(true)
                    .write(true)
                    .open(product_root.join("publish.lock"))?;
                FileExt::try_lock_exclusive(&file).map_err(|error| {
                    if error.kind() == std::io::ErrorKind::WouldBlock {
                        DataError::CacheBusy {
                            cache_dir: product_root.clone(),
                            operation: "timeline product publication",
                        }
                    } else {
                        error.into()
                    }
                })?;
                *lock = Some(file);
            }
            let mut merged = Vec::new();
            for candidate in built {
                let previous =
                    self.load_active(&candidate.identity.exchange, &candidate.identity.product)?;
                if previous.is_none() && self.precompact_active_exists(&candidate.identity)? {
                    return Err(validation(
                        "pre-compact timeline layout present; migrate it before activation",
                    ));
                }
                merged.push(match previous {
                    Some(previous) => merge_timeline(previous.snapshot, candidate)?,
                    None => candidate,
                });
            }
            for candidate in &merged {
                self.publish(candidate, &publish_locks)?;
            }
            for lock in publish_locks.values().flatten() {
                FileExt::unlock(lock)?;
            }
            return Ok(merged);
        }
        Ok(built)
    }

    pub fn open(cache_root: impl AsRef<Path>) -> Result<Self> {
        let root = cache_root.as_ref().join(TIMELINE_DIRECTORY);
        fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    /// Converts one valid pre-compact v1 active generation to the V1 single-file layout.
    ///
    /// The pre-compact files remain an offline recovery source. The exclusive
    /// root gate makes the conversion safe against concurrent maintenance.
    /// Returns `true` when a legacy generation was converted and `false` when
    /// V1 `timeline.json` is already present or no pre-compact active exists.
    pub fn migrate_legacy_active(&self, exchange: &str, product: &str) -> Result<bool> {
        let cache_root = self
            .root
            .parent()
            .ok_or_else(|| validation("missing cache root"))?;
        let _gate = BacktestTickCache::open_read_only(cache_root).try_acquire_remote_fill_lock()?;
        let identity = TradingTimelineIdentity::new(
            exchange,
            product,
            "placeholder",
            "placeholder",
            "placeholder",
        )?;
        let product_root = self.product_root(&identity)?;
        fs::create_dir_all(&product_root)?;
        let lock = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(product_root.join("publish.lock"))?;
        FileExt::try_lock_exclusive(&lock).map_err(|error| {
            if error.kind() == std::io::ErrorKind::WouldBlock {
                DataError::CacheBusy {
                    cache_dir: product_root.clone(),
                    operation: "timeline storage migration",
                }
            } else {
                error.into()
            }
        })?;
        if self.load_active(exchange, product)?.is_some() {
            return Ok(false);
        }
        let Some(snapshot) = self.load_legacy_active(exchange, product)? else {
            return Ok(false);
        };
        let locks = BTreeMap::from([(product_root, Some(lock))]);
        self.publish(&snapshot, &locks)?;
        for lock in locks.values().flatten() {
            FileExt::unlock(lock)?;
        }
        Ok(true)
    }

    fn publish(
        &self,
        snapshot: &TradingTimelineSnapshot,
        locks: &BTreeMap<PathBuf, Option<fs::File>>,
    ) -> Result<PathBuf> {
        snapshot.verify()?;
        if snapshot
            .decisions
            .iter()
            .any(|record| !matches!(record.decision, TradingTimelineRuleDecision::Unique { .. }))
        {
            return Err(validation(
                "timeline activation refused: unresolved session days",
            ));
        }
        let product_root = self.product_root(&snapshot.identity)?;
        if !locks.get(&product_root).is_some_and(Option::is_some) {
            return Err(validation("timeline publication requires product lock"));
        }
        let path = product_root.join("timeline.json");
        let compact = CompactTimelineFile::from_snapshot(snapshot)?;
        atomic_json_write(&path, &compact)?;
        for directory in product_root.ancestors() {
            fs::File::open(directory)?.sync_all()?;
            if Some(directory) == self.root.parent() {
                break;
            }
        }
        Ok(path)
    }

    pub fn load_active(&self, exchange: &str, product: &str) -> Result<Option<TradingTimeline>> {
        let identity = TradingTimelineIdentity::new(
            exchange,
            product,
            "placeholder",
            "placeholder",
            "placeholder",
        )?;
        let product_root = self.product_root(&identity)?;
        let path = product_root.join("timeline.json");
        let snapshot = match fs::read(&path) {
            Ok(bytes) => serde_json::from_slice::<CompactTimelineFile>(&bytes)?.into_snapshot()?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if snapshot.identity.exchange != exchange || snapshot.identity.product != product {
            return Err(validation("trading timeline file product mismatch"));
        }
        Ok(Some(TradingTimeline::from_snapshot(snapshot)?))
    }

    fn product_root(&self, identity: &TradingTimelineIdentity) -> Result<PathBuf> {
        Ok(self
            .root
            .join("products")
            .join(component("exchange", identity.exchange.clone())?)
            .join(component("product", identity.product.clone())?))
    }

    fn precompact_active_exists(&self, identity: &TradingTimelineIdentity) -> Result<bool> {
        let active_path = self.product_root(identity)?.join("active.json");
        match fs::metadata(active_path) {
            Ok(metadata) if metadata.is_file() => Ok(true),
            Ok(_) => Err(validation("pre-compact timeline active path is not a file")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    fn load_legacy_active(
        &self,
        exchange: &str,
        product: &str,
    ) -> Result<Option<TradingTimelineSnapshot>> {
        let root = self
            .root
            .join("products")
            .join(component("exchange", exchange.into())?)
            .join(component("product", product.into())?);
        let active_path = root.join("active.json");
        if !active_path.exists() {
            return Ok(None);
        }
        let active: TradingTimelineActive = serde_json::from_slice(&fs::read(active_path)?)?;
        if !active
            .timeline_hash
            .strip_prefix("sha256:")
            .is_some_and(|hash| {
                hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit())
            })
        {
            return Err(validation("invalid legacy timeline active hash"));
        }
        let path = root
            .join("snapshots")
            .join(format!("{}.json", active.timeline_hash));
        let snapshot: TradingTimelineSnapshot = serde_json::from_slice(&fs::read(path)?)?;
        snapshot.verify()?;
        if snapshot.timeline_hash != active.timeline_hash {
            return Err(validation("legacy timeline active hash mismatch"));
        }
        Ok(Some(snapshot))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TradingTimelineActive {
    timeline_hash: String,
}

fn normalize_ranges<T>(ranges: &[T], name: &str) -> Result<Vec<T>>
where
    T: Copy + Into<TradingTimelineInterval> + From<TradingTimelineInterval>,
{
    let mut normalized = ranges.iter().copied().map(Into::into).collect::<Vec<_>>();
    normalized.sort_unstable_by_key(|range| (range.start_ns, range.end_ns));
    let mut merged = Vec::<TradingTimelineInterval>::with_capacity(normalized.len());
    for range in normalized {
        if range.end_ns <= range.start_ns {
            return Err(validation(format!("{name} must be increasing")));
        }
        match merged.last_mut() {
            Some(previous) if range.start_ns <= previous.end_ns => {
                previous.end_ns = previous.end_ns.max(range.end_ns);
            }
            _ => merged.push(range),
        }
    }
    Ok(merged.into_iter().map(T::from).collect())
}

impl From<TradingTimelineKnownRange> for TradingTimelineInterval {
    fn from(value: TradingTimelineKnownRange) -> Self {
        Self {
            start_ns: value.start_ns,
            end_ns: value.end_ns,
        }
    }
}

impl From<TradingTimelineInterval> for TradingTimelineKnownRange {
    fn from(value: TradingTimelineInterval) -> Self {
        Self {
            start_ns: value.start_ns,
            end_ns: value.end_ns,
        }
    }
}

fn range_contains(ranges: &[TradingTimelineKnownRange], start_ns: i64, end_ns: i64) -> bool {
    let index = ranges.partition_point(|range| range.end_ns < start_ns);
    ranges
        .get(index)
        .is_some_and(|range| range.start_ns <= start_ns && end_ns <= range.end_ns)
}

fn duration_from_nanos(nanos: u128) -> Result<Duration> {
    let secs = nanos / 1_000_000_000;
    let subsec_nanos = (nanos % 1_000_000_000) as u32;
    let secs = u64::try_from(secs).map_err(|_| validation("trading duration overflow"))?;
    Ok(Duration::new(secs, subsec_nanos))
}

fn minute_value(timestamp_ns: i64) -> Result<i64> {
    if timestamp_ns % NANOS_PER_MINUTE != 0 {
        return Err(validation(
            "compact timeline timestamp is not minute-aligned",
        ));
    }
    Ok(timestamp_ns / NANOS_PER_MINUTE)
}

fn minute_ns(minutes: i64) -> Result<i64> {
    minutes
        .checked_mul(NANOS_PER_MINUTE)
        .ok_or_else(|| validation("compact timeline minute timestamp overflow"))
}

fn minute_pair(start_ns: i64, end_ns: i64) -> Result<[i64; 2]> {
    Ok([minute_value(start_ns)?, minute_value(end_ns)?])
}

fn snapshot_hash(snapshot: &TradingTimelineSnapshot) -> Result<String> {
    let bytes = serde_json::to_vec(snapshot)?;
    Ok(format!("sha256:{:x}", Sha256::digest(bytes)))
}

fn atomic_json_write<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let bytes = serde_json::to_vec(value)?;
    static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);
    let (temporary, mut file) = open_temporary_json_file(path, &NEXT_TEMP)?;
    let result = (|| {
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        sync_timeline_parent(path).map_err(|error| {
            std::io::Error::other(TradingTimelineDurabilityUncertain {
                path: path.to_path_buf(),
                message: error.to_string(),
            })
        })?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn open_temporary_json_file(path: &Path, next: &AtomicU64) -> std::io::Result<(PathBuf, fs::File)> {
    // A container restart can reuse a PID and leave a pre-rename temporary
    // behind. Advance the per-process sequence until a fresh name is found
    // instead of wedging every later publication on that stale file.
    for _ in 0..64 {
        let temporary = path.with_extension(format!(
            "json.{}.{}.tmp",
            std::process::id(),
            next.fetch_add(1, Ordering::Relaxed)
        ));
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
        {
            Ok(file) => return Ok((temporary, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        format!("no free temporary timeline path for {}", path.display()),
    ))
}

fn sync_timeline_parent(path: &Path) -> std::io::Result<()> {
    #[cfg(test)]
    if path
        .parent()
        .and_then(Path::file_name)
        .is_some_and(|name| name == "snapshots")
        && TEST_FAIL_BODY_DIRECTORY_SYNC.with(std::cell::Cell::get)
    {
        return Err(std::io::Error::other(
            "injected body directory sync failure",
        ));
    }
    #[cfg(test)]
    if path.file_name().is_some_and(|name| name == "timeline.json")
        && (TEST_FAIL_BODY_DIRECTORY_SYNC.with(std::cell::Cell::get)
            || TEST_FAIL_ACTIVE_DIRECTORY_SYNC.with(std::cell::Cell::get))
    {
        return Err(std::io::Error::other(
            "injected post-rename directory sync failure",
        ));
    }
    let parent = path
        .parent()
        .ok_or_else(|| std::io::Error::other("missing timeline parent"))?;
    fs::File::open(parent)?.sync_all()
}

fn merge_timeline(
    previous: TradingTimelineSnapshot,
    candidate: TradingTimelineSnapshot,
) -> Result<TradingTimelineSnapshot> {
    let replaced = candidate
        .decisions
        .iter()
        .map(|record| TradingTimelineInterval {
            start_ns: record.trading_day_start_ns,
            end_ns: record.trading_day_end_ns,
        })
        .collect::<Vec<_>>();
    let mut decisions = previous
        .decisions
        .iter()
        .filter(|record| {
            !replaced.iter().any(|range| {
                range.start_ns < record.trading_day_end_ns
                    && record.trading_day_start_ns < range.end_ns
            })
        })
        .cloned()
        .collect::<Vec<_>>();
    if decisions.is_empty() {
        return Ok(candidate);
    }
    if previous.identity.rule_catalog_hash != candidate.identity.rule_catalog_hash
        || previous.identity.evidence_symbol != candidate.identity.evidence_symbol
        || decisions
            .iter()
            .any(|record| record.evidence_hash.is_none())
    {
        return Err(validation(
            "catalog changed: rebuild all previously known days before activation",
        ));
    }
    let subtract = |intervals: Vec<TradingTimelineInterval>| {
        let mut remaining = intervals;
        for cut in &replaced {
            remaining = remaining
                .into_iter()
                .flat_map(|range| {
                    if range.end_ns <= cut.start_ns || range.start_ns >= cut.end_ns {
                        return vec![range];
                    }
                    let mut parts = Vec::new();
                    if range.start_ns < cut.start_ns {
                        parts.push(TradingTimelineInterval {
                            start_ns: range.start_ns,
                            end_ns: cut.start_ns,
                        });
                    }
                    if range.end_ns > cut.end_ns {
                        parts.push(TradingTimelineInterval {
                            start_ns: cut.end_ns,
                            end_ns: range.end_ns,
                        });
                    }
                    parts
                })
                .collect();
        }
        remaining
    };
    let mut known = subtract(previous.known_ranges.into_iter().map(Into::into).collect())
        .into_iter()
        .map(Into::into)
        .collect::<Vec<_>>();
    known.extend(candidate.known_ranges);
    let mut open = subtract(previous.open_intervals);
    open.extend(candidate.open_intervals);
    decisions.extend(candidate.decisions);
    decisions.sort_by_key(|record| record.trading_day_start_ns);
    let mut identity = candidate.identity;
    identity.evidence_identity = decision_evidence_identity(&decisions)?;
    TradingTimelineSnapshot::new(identity, known, open)?.with_decisions(decisions)
}

fn component(name: &str, value: String) -> Result<String> {
    let value = required(name, value)?;
    if value
        .chars()
        .any(|character| !character.is_ascii_alphanumeric() && character != '_' && character != '-')
    {
        return Err(validation(format!(
            "{name} contains an unsafe path component"
        )));
    }
    Ok(value)
}

fn decision_evidence_identity(decisions: &[TradingTimelineDecisionRecord]) -> Result<String> {
    let sources = decisions
        .iter()
        .map(|record| {
            (
                record.trading_day_start_ns,
                record.trading_day_end_ns,
                &record.evidence_hash,
            )
        })
        .collect::<Vec<_>>();
    Ok(format!(
        "sha256:{:x}",
        Sha256::digest(serde_json::to_vec(&sources)?)
    ))
}

fn required(name: &str, value: String) -> Result<String> {
    let value = value.trim().to_string();
    if value.is_empty() {
        return Err(validation(format!("{name} must not be empty")));
    }
    Ok(value)
}

fn validation(message: impl Into<String>) -> DataError {
    DataError::Validation(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tqsdk_core::Kline;

    const SECOND: i64 = 1_000_000_000;
    const MINUTE: i64 = 60 * SECOND;

    fn timeline() -> TradingTimeline {
        let snapshot = TradingTimelineSnapshot::new(
            TradingTimelineIdentity::new("SHFE", "au", "KQ.i@SHFE.au", "rules-v1", "minute-v1")
                .unwrap(),
            vec![TradingTimelineKnownRange::new(9 * 60 * MINUTE, 16 * 60 * MINUTE).unwrap()],
            vec![
                TradingTimelineInterval::new(9 * 60 * MINUTE, 10 * 60 * MINUTE + 20 * MINUTE)
                    .unwrap(),
                TradingTimelineInterval::new(
                    10 * 60 * MINUTE + 30 * MINUTE,
                    11 * 60 * MINUTE + 30 * MINUTE,
                )
                .unwrap(),
                TradingTimelineInterval::new(13 * 60 * MINUTE + 30 * MINUTE, 16 * 60 * MINUTE)
                    .unwrap(),
            ],
        )
        .unwrap();
        TradingTimeline::from_snapshot(snapshot).unwrap()
    }

    #[test]
    fn shifts_across_breaks_without_counting_wall_clock_time() {
        let timeline = timeline();
        assert_eq!(
            timeline
                .shift(
                    9 * 60 * MINUTE,
                    Duration::from_secs(75 * 60),
                    TradingTimeDirection::Forward
                )
                .unwrap(),
            10 * 60 * MINUTE + 15 * MINUTE,
        );
        assert_eq!(
            timeline
                .shift(
                    11 * 60 * MINUTE + 30 * MINUTE,
                    Duration::from_secs(60 * 60),
                    TradingTimeDirection::Backward
                )
                .unwrap(),
            10 * 60 * MINUTE + 30 * MINUTE,
        );
        assert_eq!(
            timeline
                .shift(
                    10 * 60 * MINUTE + 20 * MINUTE,
                    Duration::from_secs(60),
                    TradingTimeDirection::Forward
                )
                .unwrap(),
            10 * 60 * MINUTE + 31 * MINUTE,
        );
    }

    #[test]
    fn duration_ignores_breaks_and_unknown_ranges_fail() {
        let timeline = timeline();
        assert_eq!(
            timeline
                .trading_duration_between(
                    10 * 60 * MINUTE + 20 * MINUTE,
                    10 * 60 * MINUTE + 40 * MINUTE
                )
                .unwrap(),
            Duration::from_secs(10 * 60),
        );
        assert!(
            timeline
                .trading_duration_between(8 * 60 * MINUTE, 9 * 60 * MINUTE)
                .is_err()
        );
        assert_eq!(
            timeline
                .shift(
                    10 * 60 * MINUTE + 20 * MINUTE,
                    Duration::ZERO,
                    TradingTimeDirection::Forward
                )
                .unwrap(),
            10 * 60 * MINUTE + 20 * MINUTE,
        );
    }

    #[test]
    fn rule_inference_uses_positive_evidence_only() {
        let short = TradingTimelineRule::new(
            "short",
            vec![TradingTimelineRuleWindow::new(0, 60 * MINUTE).unwrap()],
        )
        .unwrap();
        let long = TradingTimelineRule::new(
            "long",
            vec![TradingTimelineRuleWindow::new(0, 120 * MINUTE).unwrap()],
        )
        .unwrap();

        assert_eq!(
            infer_trading_timeline_rule(0, &[short.clone(), long.clone()], &[30 * MINUTE]).unwrap(),
            TradingTimelineRuleDecision::Ambiguous {
                candidate_rule_ids: vec!["long".to_string(), "short".to_string()],
            },
        );
        assert_eq!(
            infer_trading_timeline_rule(0, &[short, long], &[90 * MINUTE]).unwrap(),
            TradingTimelineRuleDecision::Unique {
                rule_id: "long".to_string(),
            },
        );

        assert!(matches!(
            infer_trading_timeline_rule(
                0,
                &[TradingTimelineRule::new(
                    "only",
                    vec![TradingTimelineRuleWindow::new(0, 60 * MINUTE).unwrap(),]
                )
                .unwrap()],
                &[]
            )
            .unwrap(),
            TradingTimelineRuleDecision::Ambiguous { .. }
        ));
    }

    #[test]
    fn minute_cache_build_requires_final_coverage_and_confirmed_rule() {
        let root =
            std::env::temp_dir().join(format!("tqsdk-timeline-cache-build-{}", std::process::id()));
        let cache = MinuteKlineCache::open(&root).unwrap();
        let store = TradingTimelineStore::open(&root).unwrap();
        let snapshot = MinuteKlineCacheSnapshot::cst_v1();
        let day = NaiveDate::from_ymd_opt(2024, 1, 5).unwrap();
        let day_range = backtest_tick_trading_day_range(day).unwrap();
        let minute = day_range.start_ns + 15 * 60 * MINUTE;
        cache
            .store_final_range(
                "KQ.i@INE.sc",
                day_range.start_ns,
                day_range.end_ns,
                &snapshot,
                &[Kline {
                    id: 1,
                    datetime: minute,
                    open: 1.0,
                    high: 1.0,
                    low: 1.0,
                    close: 1.0,
                    volume: 1,
                    open_oi: 1,
                    close_oi: 1,
                    ..Kline::default()
                }],
            )
            .unwrap();
        let catalog = TradingTimelineRuleCatalog::from_json_str(
            r#"{
                "catalog_version": 1,
                "exception_review_complete": true,
              "generated_at": "2026-09-06",
              "timezone": "Asia/Shanghai",
              "interval_semantics": "half-open",
              "night_session_semantics": "following trading day",
              "rules": [{
                "exchange": "INE", "product": "sc", "evidence_symbol": "KQ.i@INE.sc",
                "rule_id": "sc-day", "candidate_trading_day_start": "2024-01-01",
                "candidate_trading_day_end": null,
                "sessions_cst": [["09:00", "09:02"]],
                "validation": {"status": "confirmed", "required_symbol": "KQ.i@INE.sc", "required_check": "fixture"}
              }]
            }"#,
        )
        .unwrap();
        let compiled = store
            .rebuild_from_cache(
                &catalog,
                vec![
                    TradingTimelineBuildRequest::new(
                        "INE",
                        "sc",
                        "KQ.i@INE.sc",
                        "fixture-minute-cache-v1",
                        vec![day],
                    )
                    .unwrap(),
                ],
                true,
            )
            .unwrap()
            .remove(0);
        assert!(matches!(
            compiled.decisions.as_slice(),
            [TradingTimelineDecisionRecord {
                decision: TradingTimelineRuleDecision::Unique { .. },
                ..
            }]
        ));
        assert_eq!(
            TradingTimeline::from_snapshot(compiled.clone())
                .unwrap()
                .trading_duration_between(minute, minute + 2 * MINUTE)
                .unwrap(),
            Duration::from_secs(2 * 60),
        );
        assert_eq!(
            store
                .load_active("INE", "sc")
                .unwrap()
                .unwrap()
                .snapshot()
                .timeline_hash,
            compiled.timeline_hash,
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn store_round_trips_compact_single_file_generation() {
        let root = std::env::temp_dir().join(format!("tqsdk-timeline-test-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let store = TradingTimelineStore::open(&root).unwrap();
        let snapshot = TradingTimelineSnapshot::new(
            TradingTimelineIdentity::new("SHFE", "au", "KQ.i@SHFE.au", "rules-v1", "minute-v1")
                .unwrap(),
            vec![TradingTimelineKnownRange::new(0, 60 * MINUTE).unwrap()],
            vec![TradingTimelineInterval::new(0, 60 * MINUTE).unwrap()],
        )
        .unwrap();
        let product = store.product_root(&snapshot.identity).unwrap();
        fs::create_dir_all(&product).unwrap();
        let lock = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(product.join("publish.lock"))
            .unwrap();
        FileExt::try_lock_exclusive(&lock).unwrap();
        let locks = BTreeMap::from([(product, Some(lock))]);
        let path = store.publish(&snapshot, &locks).unwrap();
        assert!(path.ends_with("timeline.json"));
        assert_eq!(
            store
                .load_active("SHFE", "au")
                .unwrap()
                .unwrap()
                .snapshot()
                .timeline_hash,
            snapshot.timeline_hash,
        );
        let mut on_disk: CompactTimelineFile =
            serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        on_disk.identity.evidence_identity.push_str("-tampered");
        fs::write(&path, serde_json::to_vec(&on_disk).unwrap()).unwrap();
        let error = store.load_active("SHFE", "au").unwrap_err();
        assert!(error.to_string().contains("compact timeline hash mismatch"));
        on_disk.format_version = TIMELINE_STORAGE_FORMAT_VERSION + 1;
        fs::write(&path, serde_json::to_vec(&on_disk).unwrap()).unwrap();
        let error = store.load_active("SHFE", "au").unwrap_err();
        assert!(
            error
                .to_string()
                .contains("unsupported compact trading timeline format")
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn temporary_writer_skips_a_crash_leftover_with_reused_pid() {
        let root =
            std::env::temp_dir().join(format!("tqsdk-timeline-temp-test-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let path = root.join("timeline.json");
        let stale = path.with_extension(format!("json.{}.0.tmp", std::process::id()));
        fs::write(&stale, b"crash leftover").unwrap();
        let next = AtomicU64::new(0);
        let (temporary, file) = open_temporary_json_file(&path, &next).unwrap();
        assert_ne!(temporary, stale);
        drop(file);
        fs::remove_file(temporary).unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn draft_catalog_is_candidate_only_and_exceptions_fail_closed() {
        let catalog = TradingTimelineRuleCatalog::from_json_str(include_str!(
            "../tests/fixtures/trading-timeline-catalog.draft.json"
        ))
        .unwrap();
        let day = NaiveDate::from_ymd_opt(2024, 9, 27).unwrap();
        let candidates = catalog.candidate_rules("SHFE", "au", day).unwrap();
        assert_eq!(candidates.len(), 1);
        assert!(
            catalog
                .confirmed_rules("SHFE", "au", day)
                .unwrap()
                .is_empty()
        );
        let closure_day = NaiveDate::from_ymd_opt(2024, 10, 1).unwrap();
        assert!(catalog.candidate_rules("INE", "sc", closure_day).is_err());

        // The published notice makes the October 8 daytime-only exception
        // materializable, but the underlying product rule remains candidate
        // evidence until final canonical-minute validation completes.
        let resumed_day = NaiveDate::from_ymd_opt(2024, 10, 8).unwrap();
        let resumed = catalog.candidate_rules("INE", "sc", resumed_day).unwrap();
        assert_eq!(resumed.len(), 1);
        assert_eq!(resumed[0].windows.len(), 3);
        assert!(
            catalog
                .confirmed_rules("INE", "sc", resumed_day)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn confirmed_night_cancellation_removes_only_the_tagged_session() {
        let catalog = TradingTimelineRuleCatalog::from_json_str(
            r#"{
              "catalog_version": 1,
              "generated_at": "2026-09-06",
              "timezone": "Asia/Shanghai",
              "interval_semantics": "half-open",
              "night_session_semantics": "following trading day",
              "rules": [{
                "exchange": "INE", "product": "sc", "evidence_symbol": "KQ.i@INE.sc",
                "rule_id": "sc-normal", "candidate_trading_day_start": "2024-01-01",
                "candidate_trading_day_end": null,
                "sessions_cst": [["21:00", "23:00"], ["09:00", "10:15"]],
                "session_ids": ["night", "morning_1"],
                "validation": {"status": "confirmed", "required_symbol": "KQ.i@INE.sc", "required_check": "fixture"}
              }],
              "exceptions": [{
                "exchange": "INE", "products": ["sc"], "trading_day": "2024-09-30",
                "kind": "night_session_cancelled", "closed": false, "session_ids": ["night"],
                "validation": {"status": "confirmed", "required_symbol": "KQ.i@INE.sc", "required_check": "notice"}
              }]
            }"#,
        )
        .unwrap();
        let rules = catalog
            .candidate_rules("INE", "sc", NaiveDate::from_ymd_opt(2024, 9, 30).unwrap())
            .unwrap();
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].windows.len(), 1);
        assert_eq!(rules[0].windows[0].start_offset_ns, 15 * 60 * MINUTE);
        assert_eq!(
            catalog
                .confirmed_rules("INE", "sc", NaiveDate::from_ymd_opt(2024, 9, 30).unwrap())
                .unwrap()
                .len(),
            1,
        );
    }

    #[test]
    fn catalog_rejects_ambiguous_exception_operations() {
        let result = TradingTimelineRuleCatalog::from_json_str(
            r#"{
              "catalog_version": 1,
              "generated_at": "2026-09-06",
              "timezone": "Asia/Shanghai",
              "interval_semantics": "half-open",
              "night_session_semantics": "following trading day",
              "rules": [],
              "exceptions": [{
                "exchange": "INE", "products": ["sc"], "trading_day": "2024-10-08",
                "kind": "broken", "closed": true, "session_ids": ["night"],
                "validation": {"status": "confirmed", "required_symbol": "KQ.i@INE.sc", "required_check": "fixture"}
              }]
            }"#,
        );
        assert!(result.is_err());
    }

    #[test]
    fn compiler_keeps_ambiguous_days_unknown() {
        let rule = TradingTimelineRule::new(
            "day",
            vec![TradingTimelineRuleWindow::new(0, 60 * MINUTE).unwrap()],
        )
        .unwrap();
        let snapshot = compile_trading_timeline(
            TradingTimelineIdentity::new("SHFE", "au", "KQ.i@SHFE.au", "rules-v1", "minute-v1")
                .unwrap(),
            &[rule],
            vec![
                TradingTimelineDecisionRecord {
                    trading_day_start_ns: 0,
                    evidence_hash: None,
                    trading_day_end_ns: 2 * 60 * MINUTE,
                    decision: TradingTimelineRuleDecision::Unique {
                        rule_id: "day".to_string(),
                    },
                },
                TradingTimelineDecisionRecord {
                    trading_day_start_ns: 2 * 60 * MINUTE,
                    evidence_hash: None,
                    trading_day_end_ns: 4 * 60 * MINUTE,
                    decision: TradingTimelineRuleDecision::Ambiguous {
                        candidate_rule_ids: vec!["day".to_string()],
                    },
                },
            ],
        )
        .unwrap();
        let timeline = TradingTimeline::from_snapshot(snapshot).unwrap();
        assert_eq!(
            timeline.trading_duration_between(0, 60 * MINUTE).unwrap(),
            Duration::from_secs(60 * 60),
        );
        assert!(
            timeline
                .trading_duration_between(2 * 60 * MINUTE, 3 * 60 * MINUTE)
                .is_err()
        );
    }
}
