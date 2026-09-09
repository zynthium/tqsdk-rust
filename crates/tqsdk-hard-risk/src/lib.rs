#![forbid(unsafe_code)]
#![doc = include_str!("../README.md")]

//! Durable, fail-closed order-admission authority.
//!
//! This crate deliberately owns no transport or broker client. A caller first
//! persists a reservation with [`HardRiskAuthority::prepare`], then obtains a
//! fenced [`HardRiskSubmissionPermit`] immediately before it sends the stable
//! client order id to its broker/session. Network submission happens outside a
//! SQLite transaction. Any ambiguous result must become [`HardRiskOrderState::Indeterminate`]
//! and be reconciled by an authoritative complete trade snapshot.

use std::{
    fmt::{Display, Formatter},
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use sha2::{Digest, Sha256};

const SCHEMA_VERSION: i64 = 1;
const SCHEMA_CONTRACT_ID: &str = "tqsdk-hard-risk/schema/v1/2026-09-08";
const ORDER_ATTEMPT_RULE_ID: &str = "order_attempt";
const OPEN_VOLUME_RULE_ID: &str = "open_volume";

/// Result type returned by this crate.
pub type Result<T> = std::result::Result<T, HardRiskError>;

/// Namespace/account/trading-day scope of one durable risk decision.
///
/// `namespace` must separate independent user, environment, and route
/// authorities. A production authority should not share it between paper and
/// live trading.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HardRiskScope {
    namespace: String,
    account_id: String,
    trading_day: String,
}

impl HardRiskScope {
    #[must_use]
    pub fn new(
        namespace: impl Into<String>,
        account_id: impl Into<String>,
        trading_day: impl Into<String>,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            account_id: account_id.into(),
            trading_day: trading_day.into(),
        }
    }

    #[must_use]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    #[must_use]
    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    #[must_use]
    pub fn trading_day(&self) -> &str {
        &self.trading_day
    }

    #[must_use]
    pub fn policy_scope(&self) -> HardRiskPolicyScope {
        HardRiskPolicyScope {
            namespace: self.namespace.clone(),
            account_id: self.account_id.clone(),
        }
    }

    fn validate(&self) -> Result<()> {
        validate_text("namespace", &self.namespace)?;
        validate_text("account_id", &self.account_id)?;
        validate_text("trading_day", &self.trading_day)
    }
}

/// Namespace/account scope of the one active durable hard-risk policy.
/// Trading day intentionally does not participate: a policy transition cannot
/// reset daily usage by selecting a new day-scoped policy key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HardRiskPolicyScope {
    namespace: String,
    account_id: String,
}

impl HardRiskPolicyScope {
    #[must_use]
    pub fn new(namespace: impl Into<String>, account_id: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            account_id: account_id.into(),
        }
    }

    #[must_use]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    #[must_use]
    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    fn validate(&self) -> Result<()> {
        validate_text("namespace", &self.namespace)?;
        validate_text("account_id", &self.account_id)
    }
}

/// Stable durable identity. It intentionally excludes trading day: reusing a
/// client id on another day is an identity conflict, not a fresh order.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HardRiskOrderKey {
    namespace: String,
    account_id: String,
    client_order_id: String,
}

impl HardRiskOrderKey {
    #[must_use]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    #[must_use]
    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    #[must_use]
    pub fn client_order_id(&self) -> &str {
        &self.client_order_id
    }
}

impl Display for HardRiskOrderKey {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "namespace={} account_id={} client_order_id={}",
            self.namespace, self.account_id, self.client_order_id
        )
    }
}

/// Side retained in immutable order evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HardRiskDirection {
    Buy,
    Sell,
}

impl HardRiskDirection {
    fn as_sql(self) -> &'static str {
        match self {
            Self::Buy => "Buy",
            Self::Sell => "Sell",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "Buy" => Ok(Self::Buy),
            "Sell" => Ok(Self::Sell),
            _ => Err(HardRiskError::InvalidStoredData {
                reason: format!("unknown hard-risk direction {value:?}"),
            }),
        }
    }
}

/// Offset retained in immutable order evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HardRiskOffset {
    Open,
    Close,
    CloseToday,
    CloseYesterday,
}

impl HardRiskOffset {
    #[must_use]
    pub fn is_open(self) -> bool {
        matches!(self, Self::Open)
    }

    fn as_sql(self) -> &'static str {
        match self {
            Self::Open => "Open",
            Self::Close => "Close",
            Self::CloseToday => "CloseToday",
            Self::CloseYesterday => "CloseYesterday",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "Open" => Ok(Self::Open),
            "Close" => Ok(Self::Close),
            "CloseToday" => Ok(Self::CloseToday),
            "CloseYesterday" => Ok(Self::CloseYesterday),
            _ => Err(HardRiskError::InvalidStoredData {
                reason: format!("unknown hard-risk offset {value:?}"),
            }),
        }
    }
}

/// Immutable request whose fingerprint is persisted before submission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HardRiskOrderRequest {
    scope: HardRiskScope,
    client_order_id: String,
    symbol: String,
    direction: HardRiskDirection,
    offset: HardRiskOffset,
    volume: i64,
    limit_price_bits: Option<u64>,
}

impl HardRiskOrderRequest {
    #[must_use]
    pub fn new(
        scope: HardRiskScope,
        client_order_id: impl Into<String>,
        symbol: impl Into<String>,
        direction: HardRiskDirection,
        offset: HardRiskOffset,
        volume: i64,
    ) -> Self {
        Self {
            scope,
            client_order_id: client_order_id.into(),
            symbol: symbol.into(),
            direction,
            offset,
            volume,
            limit_price_bits: None,
        }
    }

    /// Retain the exact IEEE-754 representation instead of treating an `f64`
    /// text rendering as identity.
    #[must_use]
    pub fn limit_price_bits(mut self, value: u64) -> Self {
        self.limit_price_bits = Some(value);
        self
    }

    #[must_use]
    pub fn scope(&self) -> &HardRiskScope {
        &self.scope
    }

    #[must_use]
    pub fn key(&self) -> HardRiskOrderKey {
        HardRiskOrderKey {
            namespace: self.scope.namespace.clone(),
            account_id: self.scope.account_id.clone(),
            client_order_id: self.client_order_id.clone(),
        }
    }

    #[must_use]
    pub fn client_order_id(&self) -> &str {
        &self.client_order_id
    }

    #[must_use]
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    #[must_use]
    pub fn direction(&self) -> HardRiskDirection {
        self.direction
    }

    #[must_use]
    pub fn offset(&self) -> HardRiskOffset {
        self.offset
    }

    #[must_use]
    pub fn volume(&self) -> i64 {
        self.volume
    }

    #[must_use]
    pub fn limit_price_bits_value(&self) -> Option<u64> {
        self.limit_price_bits
    }

    fn validate(&self) -> Result<()> {
        self.scope.validate()?;
        validate_text("client_order_id", &self.client_order_id)?;
        validate_text("symbol", &self.symbol)?;
        if self.volume <= 0 {
            return Err(HardRiskError::InvalidInput {
                field: "volume",
                reason: "must be positive".to_owned(),
            });
        }
        Ok(())
    }
}

/// Explicit reservation semantics. Version one intentionally counts every
/// prepared attempt; it never releases risk usage after a caller aborts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HardRiskReservationPolicy {
    CountPreparedAttempt,
}

impl HardRiskReservationPolicy {
    fn as_sql(self) -> &'static str {
        match self {
            Self::CountPreparedAttempt => "CountPreparedAttempt",
        }
    }
}

/// Stable policy identity and limits. Rule ids are fixed (`order_attempt` and
/// `open_volume`) so changing a policy version cannot silently reset usage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HardRiskPolicy {
    policy_id: String,
    policy_version: String,
    max_order_attempts: Option<i64>,
    max_open_volume: Option<i64>,
    reservation_policy: HardRiskReservationPolicy,
}

impl HardRiskPolicy {
    #[must_use]
    pub fn new(policy_id: impl Into<String>, policy_version: impl Into<String>) -> Self {
        Self {
            policy_id: policy_id.into(),
            policy_version: policy_version.into(),
            max_order_attempts: None,
            max_open_volume: None,
            reservation_policy: HardRiskReservationPolicy::CountPreparedAttempt,
        }
    }

    #[must_use]
    pub fn max_order_attempts(mut self, value: i64) -> Self {
        self.max_order_attempts = Some(value);
        self
    }

    #[must_use]
    pub fn max_open_volume(mut self, value: i64) -> Self {
        self.max_open_volume = Some(value);
        self
    }

    #[must_use]
    pub fn policy_id(&self) -> &str {
        &self.policy_id
    }

    #[must_use]
    pub fn policy_version(&self) -> &str {
        &self.policy_version
    }

    #[must_use]
    pub fn reservation_policy(&self) -> HardRiskReservationPolicy {
        self.reservation_policy
    }

    #[must_use]
    pub fn revision(&self) -> HardRiskPolicyRevision {
        HardRiskPolicyRevision {
            policy_id: self.policy_id.clone(),
            policy_version: self.policy_version.clone(),
            policy_fingerprint: policy_fingerprint(self),
        }
    }

    fn validate(&self) -> Result<()> {
        validate_text("policy_id", &self.policy_id)?;
        validate_text("policy_version", &self.policy_version)?;
        if self.max_order_attempts.is_none() && self.max_open_volume.is_none() {
            return Err(HardRiskError::InvalidInput {
                field: "policy",
                reason: "must contain at least one hard limit".to_owned(),
            });
        }
        for (field, value) in [
            ("max_order_attempts", self.max_order_attempts),
            ("max_open_volume", self.max_open_volume),
        ] {
            if value.is_some_and(|value| value <= 0) {
                return Err(HardRiskError::InvalidInput {
                    field,
                    reason: "must be positive when configured".to_owned(),
                });
            }
        }
        Ok(())
    }

    fn rule_requests(&self, request: &HardRiskOrderRequest) -> Vec<RuleReservation> {
        let mut reservations = Vec::with_capacity(2);
        if let Some(limit) = self.max_order_attempts {
            reservations.push(RuleReservation {
                rule_id: ORDER_ATTEMPT_RULE_ID,
                amount: 1,
                limit,
            });
        }
        if request.offset.is_open()
            && let Some(limit) = self.max_open_volume
        {
            reservations.push(RuleReservation {
                rule_id: OPEN_VOLUME_RULE_ID,
                amount: request.volume,
                limit,
            });
        }
        reservations
    }
}

/// Immutable identifier of one installed policy version.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HardRiskPolicyRevision {
    policy_id: String,
    policy_version: String,
    policy_fingerprint: String,
}

impl HardRiskPolicyRevision {
    #[must_use]
    pub fn policy_id(&self) -> &str {
        &self.policy_id
    }

    #[must_use]
    pub fn policy_version(&self) -> &str {
        &self.policy_version
    }

    #[must_use]
    pub fn policy_fingerprint(&self) -> &str {
        &self.policy_fingerprint
    }
}

/// Compare-and-set expectation for changing an active account policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HardRiskPolicyExpectation {
    Absent,
    Current(HardRiskPolicyRevision),
}

/// Durable active-policy record returned by installation and lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HardRiskPolicyRecord {
    scope: HardRiskPolicyScope,
    policy: HardRiskPolicy,
    revision: HardRiskPolicyRevision,
    activated_at_ms: i64,
}

impl HardRiskPolicyRecord {
    #[must_use]
    pub fn scope(&self) -> &HardRiskPolicyScope {
        &self.scope
    }

    #[must_use]
    pub fn policy(&self) -> &HardRiskPolicy {
        &self.policy
    }

    #[must_use]
    pub fn revision(&self) -> &HardRiskPolicyRevision {
        &self.revision
    }

    #[must_use]
    pub fn activated_at_ms(&self) -> i64 {
        self.activated_at_ms
    }
}

#[derive(Debug, Clone, Copy)]
struct RuleReservation {
    rule_id: &'static str,
    amount: i64,
    limit: i64,
}

/// SQLite behavior of an authority instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HardRiskAuthorityConfig {
    busy_timeout: Duration,
    submission_lease: Duration,
}

impl Default for HardRiskAuthorityConfig {
    fn default() -> Self {
        Self {
            busy_timeout: Duration::from_secs(5),
            submission_lease: Duration::from_secs(30),
        }
    }
}

impl HardRiskAuthorityConfig {
    #[must_use]
    pub fn busy_timeout(mut self, value: Duration) -> Self {
        self.busy_timeout = value;
        self
    }

    #[must_use]
    pub fn submission_lease(mut self, value: Duration) -> Self {
        self.submission_lease = value;
        self
    }

    #[must_use]
    pub fn configured_busy_timeout(&self) -> Duration {
        self.busy_timeout
    }

    #[must_use]
    pub fn configured_submission_lease(&self) -> Duration {
        self.submission_lease
    }

    fn validate(&self) -> Result<()> {
        if self.submission_lease.is_zero() {
            return Err(HardRiskError::InvalidInput {
                field: "submission_lease",
                reason: "must be non-zero".to_owned(),
            });
        }
        Ok(())
    }
}

/// Persistent authority. It holds no SQLite connection or async lock; every
/// operation opens a configured connection and uses `BEGIN IMMEDIATE` for its
/// short admission transition.
#[derive(Debug, Clone)]
pub struct HardRiskAuthority {
    database_path: PathBuf,
    config: HardRiskAuthorityConfig,
}

impl HardRiskAuthority {
    /// Open or initialize a version-one SQLite/WAL authority.
    ///
    /// `:memory:` is rejected so a caller cannot mistake an ephemeral database
    /// for durable hard-risk storage.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_config(path, HardRiskAuthorityConfig::default())
    }

    pub fn open_with_config(
        path: impl AsRef<Path>,
        config: HardRiskAuthorityConfig,
    ) -> Result<Self> {
        config.validate()?;
        let database_path = path.as_ref().to_path_buf();
        let database_path_text = database_path.to_string_lossy();
        if database_path.as_os_str().is_empty()
            || database_path == Path::new(":memory:")
            || database_path_text.starts_with("file:")
        {
            return Err(HardRiskError::InvalidInput {
                field: "database_path",
                reason: "must be a durable local SQLite file path; SQLite memory and URI paths are forbidden"
                    .to_owned(),
            });
        }
        let authority = Self {
            database_path,
            config,
        };
        authority.initialize_schema()?;
        Ok(authority)
    }

    #[must_use]
    pub fn database_path(&self) -> &Path {
        &self.database_path
    }

    #[must_use]
    pub fn config(&self) -> &HardRiskAuthorityConfig {
        &self.config
    }

    /// Install or transition the one active policy for a namespace/account
    /// using an explicit compare-and-set expectation. Admissions cannot choose
    /// limits per order.
    pub fn install_policy(
        &self,
        scope: HardRiskPolicyScope,
        policy: HardRiskPolicy,
        expectation: HardRiskPolicyExpectation,
    ) -> Result<HardRiskPolicyRecord> {
        scope.validate()?;
        policy.validate()?;
        let now = unix_epoch_millis()?;
        let revision = policy.revision();
        let mut connection = self.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| storage_error("begin policy transaction", error))?;
        advance_clock(&transaction, now)?;
        let current = read_active_policy(&transaction, &scope)?;
        match (&expectation, &current) {
            (HardRiskPolicyExpectation::Absent, None) => {}
            (HardRiskPolicyExpectation::Current(expected), Some(current))
                if current.revision == *expected => {}
            (_, current) => {
                return Err(HardRiskError::PolicyConflict {
                    scope: Box::new(scope),
                    expected: Box::new(expectation),
                    actual: Box::new(current.as_ref().map(|record| record.revision.clone())),
                });
            }
        }

        if let Some(existing) = read_policy_version(&transaction, &scope, &revision)? {
            if existing.policy != policy {
                return Err(HardRiskError::PolicyConflict {
                    scope: Box::new(scope),
                    expected: Box::new(expectation),
                    actual: Box::new(Some(existing.revision)),
                });
            }
        } else {
            transaction
                .execute(
                    r#"
                        INSERT INTO hard_risk_policies (
                            namespace, account_id, policy_id, policy_version, policy_fingerprint,
                            max_order_attempts, max_open_volume, reservation_policy, created_at_ms
                        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                    "#,
                    params![
                        scope.namespace,
                        scope.account_id,
                        revision.policy_id,
                        revision.policy_version,
                        revision.policy_fingerprint,
                        policy.max_order_attempts,
                        policy.max_open_volume,
                        policy.reservation_policy.as_sql(),
                        now,
                    ],
                )
                .map_err(|error| storage_error("insert immutable policy version", error))?;
        }

        let active_changed = current
            .as_ref()
            .is_none_or(|current| current.revision != revision);
        if active_changed {
            transaction
                .execute(
                    r#"
                        INSERT INTO hard_risk_active_policies (
                            namespace, account_id, policy_id, policy_version, policy_fingerprint, activated_at_ms
                        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                        ON CONFLICT(namespace, account_id) DO UPDATE SET
                            policy_id = excluded.policy_id,
                            policy_version = excluded.policy_version,
                            policy_fingerprint = excluded.policy_fingerprint,
                            activated_at_ms = excluded.activated_at_ms
                    "#,
                    params![
                        scope.namespace,
                        scope.account_id,
                        revision.policy_id,
                        revision.policy_version,
                        revision.policy_fingerprint,
                        now,
                    ],
                )
                .map_err(|error| storage_error("activate hard-risk policy", error))?;
            transaction
                .execute(
                    r#"
                        INSERT INTO hard_risk_policy_events (
                            namespace, account_id, policy_id, policy_version, policy_fingerprint, event_kind, occurred_at_ms
                        ) VALUES (?1, ?2, ?3, ?4, ?5, 'activated', ?6)
                    "#,
                    params![
                        scope.namespace,
                        scope.account_id,
                        revision.policy_id,
                        revision.policy_version,
                        revision.policy_fingerprint,
                        now,
                    ],
                )
                .map_err(|error| storage_error("append policy activation audit", error))?;
        }
        let record = read_active_policy(&transaction, &scope)?.ok_or_else(|| {
            HardRiskError::InvalidStoredData {
                reason: "active policy disappeared before commit".to_owned(),
            }
        })?;
        transaction
            .commit()
            .map_err(|error| storage_error("commit policy transition", error))?;
        Ok(record)
    }

    /// Return the active authoritative policy for one namespace/account.
    pub fn active_policy(
        &self,
        scope: &HardRiskPolicyScope,
    ) -> Result<Option<HardRiskPolicyRecord>> {
        scope.validate()?;
        let connection = self.connect()?;
        read_active_policy(&connection, scope)
    }

    /// Atomically deduplicate the stable client id, check daily limits, and
    /// reserve usage before a network send is possible.
    pub fn prepare(&self, request: HardRiskOrderRequest) -> Result<HardRiskPrepareOutcome> {
        request.validate()?;
        let now = unix_epoch_millis()?;
        let key = request.key();
        let mut connection = self.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| storage_error("begin prepare transaction", error))?;

        if let Some(existing) = read_order(&transaction, &key)? {
            if existing.request != request {
                return Err(HardRiskError::IdentityConflict { key });
            }
            return Ok(HardRiskPrepareOutcome::Existing(existing));
        }

        advance_clock(&transaction, now)?;
        let policy =
            read_active_policy(&transaction, &request.scope.policy_scope())?.ok_or_else(|| {
                HardRiskError::PolicyNotConfigured {
                    scope: request.scope.policy_scope(),
                }
            })?;
        let request_fingerprint = request_fingerprint(&request, policy.policy());

        let reservations = policy.policy.rule_requests(&request);
        for reservation in &reservations {
            let used = read_usage(&transaction, &request.scope, reservation.rule_id)?;
            let projected = used.checked_add(reservation.amount).ok_or_else(|| {
                HardRiskError::InvalidInput {
                    field: "risk usage",
                    reason: "integer overflow".to_owned(),
                }
            })?;
            if projected > reservation.limit {
                return Err(HardRiskError::LimitExceeded {
                    rule_id: reservation.rule_id,
                    used,
                    requested: reservation.amount,
                    limit: reservation.limit,
                });
            }
        }

        transaction
            .execute(
                r#"
                    INSERT INTO hard_risk_orders (
                        namespace, account_id, client_order_id, request_fingerprint, trading_day,
                        policy_id, policy_version, policy_fingerprint, reservation_policy, symbol, direction, offset, volume,
                        limit_price_bits, state, lease_generation, created_at_ms, updated_at_ms
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, 'Prepared', 0, ?15, ?15)
                "#,
                params![
                    request.scope.namespace,
                    request.scope.account_id,
                    request.client_order_id,
                    request_fingerprint,
                    request.scope.trading_day,
                    policy.revision.policy_id,
                    policy.revision.policy_version,
                    policy.revision.policy_fingerprint,
                    policy.policy.reservation_policy.as_sql(),
                    request.symbol,
                    request.direction.as_sql(),
                    request.offset.as_sql(),
                    request.volume,
                    request.limit_price_bits.map(|value| format!("{value:016x}")),
                    now,
                ],
            )
            .map_err(|error| storage_error("insert prepared order", error))?;

        for reservation in &reservations {
            transaction
                .execute(
                    r#"
                        INSERT INTO hard_risk_reservations (
                            namespace, account_id, client_order_id, rule_id, amount, created_at_ms
                        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                    "#,
                    params![
                        key.namespace,
                        key.account_id,
                        key.client_order_id,
                        reservation.rule_id,
                        reservation.amount,
                        now,
                    ],
                )
                .map_err(|error| storage_error("insert hard-risk reservation", error))?;
            transaction
                .execute(
                    r#"
                        INSERT INTO hard_risk_daily_usage (
                            namespace, account_id, trading_day, rule_id, used, updated_at_ms
                        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                        ON CONFLICT(namespace, account_id, trading_day, rule_id)
                        DO UPDATE SET used = hard_risk_daily_usage.used + excluded.used,
                                      updated_at_ms = excluded.updated_at_ms
                    "#,
                    params![
                        request.scope.namespace,
                        request.scope.account_id,
                        request.scope.trading_day,
                        reservation.rule_id,
                        reservation.amount,
                        now,
                    ],
                )
                .map_err(|error| storage_error("advance hard-risk usage", error))?;
        }
        write_event(
            &transaction,
            &key,
            &request_fingerprint,
            "Prepared",
            "prepared",
            "durable admission and usage reservation committed",
            now,
        )?;
        let record =
            read_order(&transaction, &key)?.ok_or_else(|| HardRiskError::InvalidStoredData {
                reason: "prepared order disappeared before commit".to_owned(),
            })?;
        transaction
            .commit()
            .map_err(|error| storage_error("commit prepared order", error))?;
        Ok(HardRiskPrepareOutcome::Prepared(record))
    }

    /// Acquire one fenced submission permit. A permit expires into
    /// `Indeterminate`; no process can reclaim it and resend automatically.
    pub fn begin_submission(
        &self,
        identity: &HardRiskOrderIdentity,
        owner_id: impl AsRef<str>,
    ) -> Result<HardRiskSubmissionOutcome> {
        let owner_id = owner_id.as_ref();
        validate_text("owner_id", owner_id)?;
        let now = unix_epoch_millis()?;
        let expiry = now
            .checked_add(duration_millis(self.config.submission_lease)?)
            .ok_or_else(|| HardRiskError::InvalidInput {
                field: "submission_lease",
                reason: "expiry overflow".to_owned(),
            })?;
        let mut connection = self.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| storage_error("begin submission transaction", error))?;
        advance_clock(&transaction, now)?;
        let Some(existing) = read_order(&transaction, &identity.key)? else {
            return Err(HardRiskError::UnknownOrder {
                key: identity.key.clone(),
            });
        };
        ensure_identity(&existing, identity)?;

        match existing.state {
            HardRiskOrderState::Prepared => {
                transaction
                    .execute(
                        r#"
                            UPDATE hard_risk_orders
                            SET state = 'Submitting', lease_owner = ?1,
                                lease_generation = lease_generation + 1, lease_expires_at_ms = ?2,
                                updated_at_ms = ?3
                            WHERE namespace = ?4 AND account_id = ?5 AND client_order_id = ?6
                              AND state = 'Prepared'
                        "#,
                        params![
                            owner_id,
                            expiry,
                            now,
                            identity.key.namespace,
                            identity.key.account_id,
                            identity.key.client_order_id,
                        ],
                    )
                    .map_err(|error| storage_error("grant submission lease", error))?;
                write_event(
                    &transaction,
                    &identity.key,
                    &identity.request_fingerprint,
                    "Submitting",
                    "submission_lease_granted",
                    "network send may begin outside SQLite transaction",
                    now,
                )?;
                let record = read_order(&transaction, &identity.key)?.ok_or_else(|| {
                    HardRiskError::InvalidStoredData {
                        reason: "submission lease row disappeared before commit".to_owned(),
                    }
                })?;
                let permit = HardRiskSubmissionPermit {
                    request: record.request.clone(),
                    identity: record.identity(),
                    owner_id: owner_id.to_owned(),
                    lease_generation: record.lease_generation,
                };
                transaction
                    .commit()
                    .map_err(|error| storage_error("commit submission lease", error))?;
                Ok(HardRiskSubmissionOutcome::Granted(Box::new(permit)))
            }
            HardRiskOrderState::Submitting
                if existing
                    .lease_expires_at_ms
                    .is_some_and(|expiry| expiry <= now) =>
            {
                mark_indeterminate_in_transaction(
                    &transaction,
                    &existing,
                    "submission lease expired before durable receipt",
                    now,
                )?;
                let record = read_order(&transaction, &identity.key)?.ok_or_else(|| {
                    HardRiskError::InvalidStoredData {
                        reason: "expired submission row disappeared before commit".to_owned(),
                    }
                })?;
                transaction
                    .commit()
                    .map_err(|error| storage_error("commit expired submission", error))?;
                Ok(HardRiskSubmissionOutcome::Existing(Box::new(record)))
            }
            HardRiskOrderState::Submitting
            | HardRiskOrderState::Submitted
            | HardRiskOrderState::Indeterminate
            | HardRiskOrderState::Terminal(_) => {
                Ok(HardRiskSubmissionOutcome::Existing(Box::new(existing)))
            }
        }
    }

    /// Record local dispatch receipt after the network call returns. Receipt
    /// values are diagnostic only; durable identity is client order id.
    pub fn record_submitted(
        &self,
        permit: HardRiskSubmissionPermit,
        receipt: HardRiskRuntimeReceipt,
    ) -> Result<HardRiskOrderRecord> {
        if let Err(error) = receipt.validate() {
            let reason = format!("invalid local dispatch receipt: {error}");
            self.record_indeterminate(permit, reason)?;
            return Err(error);
        }
        if receipt.client_order_id != permit.request.client_order_id {
            let error = HardRiskError::ReceiptIdentityMismatch {
                key: permit.identity.key.clone(),
                receipt_client_order_id: receipt.client_order_id.clone(),
            };
            self.record_indeterminate(permit, error.to_string())?;
            return Err(error);
        }
        let now = unix_epoch_millis()?;
        let mut connection = self.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| storage_error("begin submitted receipt transaction", error))?;
        advance_clock(&transaction, now)?;
        let Some(existing) = read_order(&transaction, &permit.identity.key)? else {
            return Err(HardRiskError::UnknownOrder {
                key: permit.identity.key.clone(),
            });
        };
        ensure_identity(&existing, &permit.identity)?;
        match &existing.state {
            HardRiskOrderState::Submitted => {
                if existing.runtime_receipt.as_ref() == Some(&receipt) {
                    return Ok(existing);
                }
                return Err(HardRiskError::StateConflict {
                    key: existing.key(),
                    expected: "same durable submitted receipt".to_owned(),
                    actual: existing.state,
                });
            }
            HardRiskOrderState::Submitting => {}
            state => {
                return Err(HardRiskError::StateConflict {
                    key: existing.key(),
                    expected: "Submitting".to_owned(),
                    actual: state.clone(),
                });
            }
        }
        if existing.lease_owner.as_deref() != Some(permit.owner_id.as_str())
            || existing.lease_generation != permit.lease_generation
        {
            return Err(HardRiskError::LeaseFenced {
                key: existing.key(),
            });
        }
        if existing
            .lease_expires_at_ms
            .is_some_and(|expiry| expiry <= now)
        {
            mark_indeterminate_in_transaction(
                &transaction,
                &existing,
                "submission lease expired before durable receipt",
                now,
            )?;
            transaction
                .commit()
                .map_err(|error| storage_error("commit expired submitted receipt", error))?;
            return Err(HardRiskError::LeaseExpired {
                key: existing.key(),
            });
        }
        transaction
            .execute(
                r#"
                    UPDATE hard_risk_orders
                    SET state = 'Submitted', runtime_run_id = ?1, command_id = ?2, updated_at_ms = ?3
                    WHERE namespace = ?4 AND account_id = ?5 AND client_order_id = ?6
                      AND state = 'Submitting' AND lease_owner = ?7 AND lease_generation = ?8
                "#,
                params![
                    receipt.runtime_run_id,
                    receipt.command_id,
                    now,
                    permit.identity.key.namespace,
                    permit.identity.key.account_id,
                    permit.identity.key.client_order_id,
                    permit.owner_id,
                    i64::try_from(permit.lease_generation).map_err(|_| {
                        HardRiskError::InvalidInput {
                            field: "lease_generation",
                            reason: "does not fit SQLite integer".to_owned(),
                        }
                    })?,
                ],
            )
            .map_err(|error| storage_error("record submitted receipt", error))?;
        write_event(
            &transaction,
            &permit.identity.key,
            &permit.identity.request_fingerprint,
            "Submitted",
            "submitted",
            "network dispatch receipt persisted",
            now,
        )?;
        let record = read_order(&transaction, &permit.identity.key)?.ok_or_else(|| {
            HardRiskError::InvalidStoredData {
                reason: "submitted row disappeared before commit".to_owned(),
            }
        })?;
        transaction
            .commit()
            .map_err(|error| storage_error("commit submitted receipt", error))?;
        Ok(record)
    }

    /// Persist an ambiguous network outcome. This is intentionally irreversible
    /// without an explicit broker/trade-snapshot reconciliation.
    pub fn record_indeterminate(
        &self,
        permit: HardRiskSubmissionPermit,
        reason: impl AsRef<str>,
    ) -> Result<HardRiskOrderRecord> {
        let reason = reason.as_ref();
        validate_text("indeterminate_reason", reason)?;
        let now = unix_epoch_millis()?;
        let mut connection = self.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| storage_error("begin indeterminate transaction", error))?;
        advance_clock(&transaction, now)?;
        let Some(existing) = read_order(&transaction, &permit.identity.key)? else {
            return Err(HardRiskError::UnknownOrder {
                key: permit.identity.key.clone(),
            });
        };
        ensure_identity(&existing, &permit.identity)?;
        match &existing.state {
            HardRiskOrderState::Indeterminate => return Ok(existing),
            HardRiskOrderState::Submitting => {}
            state => {
                return Err(HardRiskError::StateConflict {
                    key: existing.key(),
                    expected: "Submitting or Indeterminate".to_owned(),
                    actual: state.clone(),
                });
            }
        }
        if existing.lease_owner.as_deref() != Some(permit.owner_id.as_str())
            || existing.lease_generation != permit.lease_generation
        {
            return Err(HardRiskError::LeaseFenced {
                key: existing.key(),
            });
        }
        mark_indeterminate_in_transaction(&transaction, &existing, reason, now)?;
        let record = read_order(&transaction, &permit.identity.key)?.ok_or_else(|| {
            HardRiskError::InvalidStoredData {
                reason: "indeterminate row disappeared before commit".to_owned(),
            }
        })?;
        transaction
            .commit()
            .map_err(|error| storage_error("commit indeterminate transition", error))?;
        Ok(record)
    }

    /// Execute one caller-supplied submission callback against the immutable
    /// admitted request. The permit is consumed, so safe Rust code cannot send
    /// twice with the same authority capability.
    pub fn submit_once<F, E>(
        &self,
        permit: HardRiskSubmissionPermit,
        submit: F,
    ) -> Result<HardRiskSubmitOutcome>
    where
        F: FnOnce(&HardRiskOrderRequest) -> std::result::Result<HardRiskRuntimeReceipt, E>,
        E: Display,
    {
        match submit(permit.request()) {
            Ok(receipt) => self
                .record_submitted(permit, receipt)
                .map(HardRiskSubmitOutcome::Submitted),
            Err(error) => {
                let reason = format!("submission adapter returned error: {error}");
                let record = self.record_indeterminate(permit, &reason)?;
                Ok(HardRiskSubmitOutcome::Indeterminate { record, reason })
            }
        }
    }

    /// Retain a terminal observation for an order with a durable local submit
    /// receipt. Ambiguous sends must use [`Self::reconcile_terminal`] instead.
    pub fn observe_terminal(
        &self,
        identity: &HardRiskOrderIdentity,
        observation: HardRiskTerminalObservation,
    ) -> Result<HardRiskOrderRecord> {
        let now = unix_epoch_millis()?;
        let terminal_revision = observation
            .runtime_revision
            .map(i64::try_from)
            .transpose()
            .map_err(|_| HardRiskError::InvalidInput {
                field: "runtime_revision",
                reason: "does not fit SQLite integer".to_owned(),
            })?;
        let mut connection = self.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| storage_error("begin terminal transaction", error))?;
        advance_clock(&transaction, now)?;
        let Some(existing) = read_order(&transaction, &identity.key)? else {
            return Err(HardRiskError::UnknownOrder {
                key: identity.key.clone(),
            });
        };
        ensure_identity(&existing, identity)?;
        if let HardRiskOrderState::Terminal(current) = existing.state {
            if current == observation.state
                && existing.terminal_revision == observation.runtime_revision
            {
                return Ok(existing);
            }
            return Err(HardRiskError::StateConflict {
                key: existing.key(),
                expected: "same terminal observation".to_owned(),
                actual: existing.state,
            });
        }
        if !matches!(existing.state, HardRiskOrderState::Submitted) {
            return Err(HardRiskError::StateConflict {
                key: existing.key(),
                expected: "Submitted with durable local receipt".to_owned(),
                actual: existing.state,
            });
        }
        transaction
            .execute(
                r#"
                    UPDATE hard_risk_orders
                    SET state = 'Terminal', terminal_state = ?1, terminal_revision = ?2,
                        updated_at_ms = ?3
                    WHERE namespace = ?4 AND account_id = ?5 AND client_order_id = ?6
                "#,
                params![
                    observation.state.as_sql(),
                    terminal_revision,
                    now,
                    identity.key.namespace,
                    identity.key.account_id,
                    identity.key.client_order_id,
                ],
            )
            .map_err(|error| storage_error("record terminal observation", error))?;
        write_event(
            &transaction,
            &identity.key,
            &identity.request_fingerprint,
            "Terminal",
            "terminal",
            observation.state.as_sql(),
            now,
        )?;
        let record = read_order(&transaction, &identity.key)?.ok_or_else(|| {
            HardRiskError::InvalidStoredData {
                reason: "terminal row disappeared before commit".to_owned(),
            }
        })?;
        transaction
            .commit()
            .map_err(|error| storage_error("commit terminal observation", error))?;
        Ok(record)
    }

    /// Reconcile an ambiguous send only from a complete snapshot for this exact
    /// namespace/account/trading-day. The audit chain records a distinct
    /// recovered-submission event before the retained terminal observation.
    pub fn reconcile_terminal(
        &self,
        snapshot: &HardRiskCompleteTradeSnapshot,
        identity: &HardRiskOrderIdentity,
        observation: HardRiskTerminalObservation,
    ) -> Result<HardRiskOrderRecord> {
        snapshot.scope.validate()?;
        if observation
            .runtime_revision
            .is_some_and(|revision| revision != snapshot.runtime_revision)
        {
            return Err(HardRiskError::InvalidInput {
                field: "runtime_revision",
                reason: "terminal observation must match complete snapshot revision".to_owned(),
            });
        }
        let now = unix_epoch_millis()?;
        let snapshot_revision =
            i64::try_from(snapshot.runtime_revision).map_err(|_| HardRiskError::InvalidInput {
                field: "runtime_revision",
                reason: "does not fit SQLite integer".to_owned(),
            })?;
        let mut connection = self.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| storage_error("begin terminal reconciliation transaction", error))?;
        advance_clock(&transaction, now)?;
        let Some(existing) = read_order(&transaction, &identity.key)? else {
            return Err(HardRiskError::UnknownOrder {
                key: identity.key.clone(),
            });
        };
        ensure_identity(&existing, identity)?;
        if existing.request.scope != snapshot.scope {
            return Err(HardRiskError::RecoveryScopeMismatch {
                key: Box::new(existing.key()),
                recovery_scope: Box::new(snapshot.scope.clone()),
            });
        }
        if let HardRiskOrderState::Terminal(current) = existing.state {
            if current == observation.state
                && existing.terminal_revision == Some(snapshot.runtime_revision)
            {
                return Ok(existing);
            }
            return Err(HardRiskError::StateConflict {
                key: existing.key(),
                expected: "same reconciled terminal observation".to_owned(),
                actual: existing.state,
            });
        }
        if !matches!(
            existing.state,
            HardRiskOrderState::Submitting | HardRiskOrderState::Indeterminate
        ) {
            return Err(HardRiskError::StateConflict {
                key: existing.key(),
                expected: "Submitting or Indeterminate".to_owned(),
                actual: existing.state,
            });
        }
        let recovery_detail = format!(
            "complete trade snapshot revision={} reconciled ambiguous submission",
            snapshot.runtime_revision
        );
        write_event(
            &transaction,
            &identity.key,
            &identity.request_fingerprint,
            "ReconciledSubmission",
            "recovered_submission",
            &recovery_detail,
            now,
        )?;
        transaction
            .execute(
                r#"
                    UPDATE hard_risk_orders
                    SET state = 'Terminal', terminal_state = ?1, terminal_revision = ?2,
                        updated_at_ms = ?3
                    WHERE namespace = ?4 AND account_id = ?5 AND client_order_id = ?6
                "#,
                params![
                    observation.state.as_sql(),
                    snapshot_revision,
                    now,
                    identity.key.namespace,
                    identity.key.account_id,
                    identity.key.client_order_id,
                ],
            )
            .map_err(|error| storage_error("record reconciled terminal observation", error))?;
        write_event(
            &transaction,
            &identity.key,
            &identity.request_fingerprint,
            "Terminal",
            "terminal",
            observation.state.as_sql(),
            now,
        )?;
        let record = read_order(&transaction, &identity.key)?.ok_or_else(|| {
            HardRiskError::InvalidStoredData {
                reason: "reconciled terminal row disappeared before commit".to_owned(),
            }
        })?;
        transaction
            .commit()
            .map_err(|error| storage_error("commit terminal reconciliation", error))?;
        Ok(record)
    }

    /// Turn expired in-flight submissions into `Indeterminate` only after the
    /// caller proves its trade snapshot is complete. This method never retries
    /// an order and never opens a network connection itself.
    pub fn recover_expired_submissions(
        &self,
        readiness: HardRiskRecoveryReadiness,
    ) -> Result<Vec<HardRiskOrderRecord>> {
        let snapshot = match readiness {
            HardRiskRecoveryReadiness::CompleteTradeSnapshot(snapshot) => snapshot,
            HardRiskRecoveryReadiness::IncompleteTradeSnapshot { .. } => {
                return Err(HardRiskError::IncompleteTradeSnapshot);
            }
        };
        snapshot.scope.validate()?;
        let now = unix_epoch_millis()?;
        let mut connection = self.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| storage_error("begin recovery transaction", error))?;
        advance_clock(&transaction, now)?;
        let keys = {
            let mut statement = transaction
                .prepare(
                    r#"
                        SELECT namespace, account_id, client_order_id
                        FROM hard_risk_orders
                        WHERE namespace = ?1 AND account_id = ?2 AND trading_day = ?3
                          AND state = 'Submitting' AND lease_expires_at_ms IS NOT NULL
                          AND lease_expires_at_ms <= ?4
                    "#,
                )
                .map_err(|error| storage_error("prepare expired submission query", error))?;
            let rows = statement
                .query_map(
                    params![
                        snapshot.scope.namespace,
                        snapshot.scope.account_id,
                        snapshot.scope.trading_day,
                        now,
                    ],
                    |row| {
                        Ok(HardRiskOrderKey {
                            namespace: row.get(0)?,
                            account_id: row.get(1)?,
                            client_order_id: row.get(2)?,
                        })
                    },
                )
                .map_err(|error| storage_error("query expired submissions", error))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|error| storage_error("read expired submissions", error))?
        };
        let mut recovered = Vec::with_capacity(keys.len());
        for key in &keys {
            let record =
                read_order(&transaction, key)?.ok_or_else(|| HardRiskError::InvalidStoredData {
                    reason: "recovery candidate disappeared before transition".to_owned(),
                })?;
            let detail = format!(
                "submission lease expired during complete trade snapshot recovery revision={}",
                snapshot.runtime_revision
            );
            mark_indeterminate_in_transaction(&transaction, &record, &detail, now)?;
            let updated =
                read_order(&transaction, key)?.ok_or_else(|| HardRiskError::InvalidStoredData {
                    reason: "recovered submission disappeared before commit".to_owned(),
                })?;
            recovered.push(updated);
        }
        transaction
            .commit()
            .map_err(|error| storage_error("commit expired submission recovery", error))?;
        Ok(recovered)
    }

    /// Lookup durable state using stable identity, including after restart.
    pub fn get(&self, key: &HardRiskOrderKey) -> Result<Option<HardRiskOrderRecord>> {
        let connection = self.connect()?;
        read_order(&connection, key)
    }

    /// Return at most `limit` unresolved records for one exact recovery scope.
    /// `Prepared`, `Submitting`, `Submitted`, and `Indeterminate` all remain
    /// discoverable after restart; terminal rows stay available by key/audit.
    pub fn unresolved_orders(
        &self,
        scope: &HardRiskScope,
        limit: usize,
    ) -> Result<Vec<HardRiskOrderRecord>> {
        scope.validate()?;
        let limit = i64::try_from(limit).map_err(|_| HardRiskError::InvalidInput {
            field: "limit",
            reason: "does not fit SQLite integer".to_owned(),
        })?;
        if !(1..=1_000).contains(&limit) {
            return Err(HardRiskError::InvalidInput {
                field: "limit",
                reason: "must be in 1..=1000".to_owned(),
            });
        }
        let connection = self.connect()?;
        let mut statement = connection
            .prepare(
                r#"
                    SELECT namespace, account_id, client_order_id, request_fingerprint, trading_day,
                           policy_id, policy_version, policy_fingerprint, reservation_policy, symbol, direction, offset, volume,
                           limit_price_bits, state, lease_owner, lease_generation, lease_expires_at_ms,
                           runtime_run_id, command_id, terminal_state, terminal_revision, indeterminate_reason,
                           created_at_ms, updated_at_ms
                    FROM hard_risk_orders
                    WHERE namespace = ?1 AND account_id = ?2 AND trading_day = ?3
                      AND state IN ('Prepared', 'Submitting', 'Submitted', 'Indeterminate')
                    ORDER BY created_at_ms, client_order_id
                    LIMIT ?4
                "#,
            )
            .map_err(|error| storage_error("prepare unresolved order query", error))?;
        let rows = statement
            .query_map(
                params![scope.namespace, scope.account_id, scope.trading_day, limit],
                RawOrder::from_row,
            )
            .map_err(|error| storage_error("query unresolved orders", error))?;
        rows.map(|row| {
            row.map_err(|error| storage_error("read unresolved order", error))?
                .into_record()
        })
        .collect()
    }

    /// Read one durable rule total. The fixed rule ids are
    /// `order_attempt` and `open_volume`.
    pub fn daily_usage(&self, scope: &HardRiskScope, rule_id: impl AsRef<str>) -> Result<i64> {
        let connection = self.connect()?;
        read_usage(&connection, scope, rule_id.as_ref())
    }

    /// Return retained audit events for one immutable order identity.
    pub fn audit_events(
        &self,
        identity: &HardRiskOrderIdentity,
    ) -> Result<Vec<HardRiskAuditEvent>> {
        let connection = self.connect()?;
        let mut statement = connection
            .prepare(
                r#"
                    SELECT state, event_kind, detail, occurred_at_ms
                    FROM hard_risk_events
                    WHERE namespace = ?1 AND account_id = ?2 AND client_order_id = ?3
                      AND request_fingerprint = ?4
                    ORDER BY event_id
                "#,
            )
            .map_err(|error| storage_error("prepare audit event query", error))?;
        let rows = statement
            .query_map(
                params![
                    identity.key.namespace,
                    identity.key.account_id,
                    identity.key.client_order_id,
                    identity.request_fingerprint,
                ],
                |row| {
                    Ok(HardRiskAuditEvent {
                        state: row.get(0)?,
                        kind: row.get(1)?,
                        detail: row.get(2)?,
                        occurred_at_ms: row.get(3)?,
                    })
                },
            )
            .map_err(|error| storage_error("query audit events", error))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| storage_error("read audit events", error))
    }

    fn initialize_schema(&self) -> Result<()> {
        let mut connection = self.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| storage_error("begin schema transaction", error))?;
        let version: i64 = transaction
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .map_err(|error| storage_error("read schema version", error))?;
        match version {
            0 => {
                let existing_tables: i64 = transaction
                    .query_row(
                        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name LIKE 'hard_risk_%'",
                        [],
                        |row| row.get(0),
                    )
                    .map_err(|error| {
                        storage_error("inspect unversioned hard-risk schema", error)
                    })?;
                if existing_tables != 0 {
                    return Err(HardRiskError::Schema {
                        reason: "unversioned hard-risk tables found; refuse implicit migration"
                            .to_owned(),
                    });
                }
                create_schema_v1(&transaction)?;
                transaction
                    .pragma_update(None, "user_version", SCHEMA_VERSION)
                    .map_err(|error| storage_error("write schema version", error))?;
            }
            SCHEMA_VERSION => verify_schema_v1(&transaction)?,
            found => {
                return Err(HardRiskError::Schema {
                    reason: format!(
                        "unsupported hard-risk schema version {found}; supported version is {SCHEMA_VERSION}"
                    ),
                });
            }
        }
        transaction
            .commit()
            .map_err(|error| storage_error("commit schema transaction", error))
    }

    fn connect(&self) -> Result<Connection> {
        let connection = Connection::open(&self.database_path)
            .map_err(|error| storage_error("open SQLite authority", error))?;
        connection
            .busy_timeout(self.config.busy_timeout)
            .map_err(|error| storage_error("configure SQLite busy timeout", error))?;
        let journal_mode: String = connection
            .query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))
            .map_err(|error| storage_error("enable SQLite WAL", error))?;
        if !journal_mode.eq_ignore_ascii_case("wal") {
            return Err(HardRiskError::Storage {
                operation: "enable SQLite WAL",
                reason: format!("SQLite returned journal mode {journal_mode:?}, expected WAL"),
            });
        }
        connection
            .execute_batch("PRAGMA synchronous = FULL; PRAGMA foreign_keys = ON;")
            .map_err(|error| storage_error("configure SQLite durability", error))?;
        Ok(connection)
    }
}

/// Idempotent result of [`HardRiskAuthority::prepare`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HardRiskPrepareOutcome {
    Prepared(HardRiskOrderRecord),
    Existing(HardRiskOrderRecord),
}

impl HardRiskPrepareOutcome {
    #[must_use]
    pub fn record(&self) -> &HardRiskOrderRecord {
        match self {
            Self::Prepared(record) | Self::Existing(record) => record,
        }
    }
}

/// Idempotent result of acquiring a submission permit.
#[derive(Debug, PartialEq, Eq)]
pub enum HardRiskSubmissionOutcome {
    Granted(Box<HardRiskSubmissionPermit>),
    Existing(Box<HardRiskOrderRecord>),
}

/// Result of the safe one-shot submission seam.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HardRiskSubmitOutcome {
    Submitted(HardRiskOrderRecord),
    Indeterminate {
        record: HardRiskOrderRecord,
        reason: String,
    },
}

impl HardRiskSubmitOutcome {
    #[must_use]
    pub fn record(&self) -> &HardRiskOrderRecord {
        match self {
            Self::Submitted(record) | Self::Indeterminate { record, .. } => record,
        }
    }
}

/// Fenced, single-send capability. It is persistently checked again before a
/// receipt or indeterminate transition is accepted.
#[derive(Debug, PartialEq, Eq)]
pub struct HardRiskSubmissionPermit {
    request: HardRiskOrderRequest,
    identity: HardRiskOrderIdentity,
    owner_id: String,
    lease_generation: u64,
}

impl HardRiskSubmissionPermit {
    #[must_use]
    pub fn request(&self) -> &HardRiskOrderRequest {
        &self.request
    }

    #[must_use]
    pub fn identity(&self) -> &HardRiskOrderIdentity {
        &self.identity
    }

    #[must_use]
    pub fn owner_id(&self) -> &str {
        &self.owner_id
    }

    #[must_use]
    pub fn lease_generation(&self) -> u64 {
        self.lease_generation
    }
}

/// Durable state retained for every admitted order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HardRiskOrderRecord {
    request: HardRiskOrderRequest,
    identity: HardRiskOrderIdentity,
    policy_id: String,
    policy_version: String,
    policy_fingerprint: String,
    reservation_policy: HardRiskReservationPolicy,
    state: HardRiskOrderState,
    lease_owner: Option<String>,
    lease_generation: u64,
    lease_expires_at_ms: Option<i64>,
    runtime_receipt: Option<HardRiskRuntimeReceipt>,
    terminal_revision: Option<u64>,
    indeterminate_reason: Option<String>,
    created_at_ms: i64,
    updated_at_ms: i64,
}

impl HardRiskOrderRecord {
    #[must_use]
    pub fn request(&self) -> &HardRiskOrderRequest {
        &self.request
    }

    #[must_use]
    pub fn key(&self) -> HardRiskOrderKey {
        self.request.key()
    }

    #[must_use]
    pub fn identity(&self) -> HardRiskOrderIdentity {
        self.identity.clone()
    }

    #[must_use]
    pub fn policy_id(&self) -> &str {
        &self.policy_id
    }

    #[must_use]
    pub fn policy_version(&self) -> &str {
        &self.policy_version
    }

    #[must_use]
    pub fn policy_revision(&self) -> HardRiskPolicyRevision {
        HardRiskPolicyRevision {
            policy_id: self.policy_id.clone(),
            policy_version: self.policy_version.clone(),
            policy_fingerprint: self.policy_fingerprint.clone(),
        }
    }

    #[must_use]
    pub fn reservation_policy(&self) -> HardRiskReservationPolicy {
        self.reservation_policy
    }

    #[must_use]
    pub fn state(&self) -> &HardRiskOrderState {
        &self.state
    }

    #[must_use]
    pub fn lease_owner(&self) -> Option<&str> {
        self.lease_owner.as_deref()
    }

    #[must_use]
    pub fn lease_generation(&self) -> u64 {
        self.lease_generation
    }

    #[must_use]
    pub fn lease_expires_at_ms(&self) -> Option<i64> {
        self.lease_expires_at_ms
    }

    #[must_use]
    pub fn runtime_receipt(&self) -> Option<&HardRiskRuntimeReceipt> {
        self.runtime_receipt.as_ref()
    }

    #[must_use]
    pub fn terminal_revision(&self) -> Option<u64> {
        self.terminal_revision
    }

    #[must_use]
    pub fn indeterminate_reason(&self) -> Option<&str> {
        self.indeterminate_reason.as_deref()
    }

    #[must_use]
    pub fn created_at_ms(&self) -> i64 {
        self.created_at_ms
    }

    #[must_use]
    pub fn updated_at_ms(&self) -> i64 {
        self.updated_at_ms
    }
}

/// Immutable pair used to fence terminal/recovery updates to exactly the
/// request that was admitted.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HardRiskOrderIdentity {
    key: HardRiskOrderKey,
    request_fingerprint: String,
}

impl HardRiskOrderIdentity {
    #[must_use]
    pub fn key(&self) -> &HardRiskOrderKey {
        &self.key
    }

    #[must_use]
    pub fn request_fingerprint(&self) -> &str {
        &self.request_fingerprint
    }
}

/// State of durable order admission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HardRiskOrderState {
    Prepared,
    Submitting,
    Submitted,
    Indeterminate,
    Terminal(HardRiskTerminalState),
}

/// Terminal evidence from an authoritative broker/trade snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HardRiskTerminalState {
    Filled,
    Cancelled,
    Rejected,
    Failed,
}

impl HardRiskTerminalState {
    fn as_sql(self) -> &'static str {
        match self {
            Self::Filled => "Filled",
            Self::Cancelled => "Cancelled",
            Self::Rejected => "Rejected",
            Self::Failed => "Failed",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "Filled" => Ok(Self::Filled),
            "Cancelled" => Ok(Self::Cancelled),
            "Rejected" => Ok(Self::Rejected),
            "Failed" => Ok(Self::Failed),
            _ => Err(HardRiskError::InvalidStoredData {
                reason: format!("unknown terminal state {value:?}"),
            }),
        }
    }
}

/// Runtime-local receipt retained only as diagnostics. It must not be used as
/// a durable order identity because command ids may restart or repeat.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HardRiskRuntimeReceipt {
    client_order_id: String,
    runtime_run_id: String,
    command_id: String,
}

impl HardRiskRuntimeReceipt {
    #[must_use]
    pub fn new(
        client_order_id: impl Into<String>,
        runtime_run_id: impl Into<String>,
        command_id: impl Into<String>,
    ) -> Self {
        Self {
            client_order_id: client_order_id.into(),
            runtime_run_id: runtime_run_id.into(),
            command_id: command_id.into(),
        }
    }

    #[must_use]
    pub fn for_request(
        request: &HardRiskOrderRequest,
        runtime_run_id: impl Into<String>,
        command_id: impl Into<String>,
    ) -> Self {
        Self::new(request.client_order_id(), runtime_run_id, command_id)
    }

    #[must_use]
    pub fn client_order_id(&self) -> &str {
        &self.client_order_id
    }

    #[must_use]
    pub fn runtime_run_id(&self) -> &str {
        &self.runtime_run_id
    }

    #[must_use]
    pub fn command_id(&self) -> &str {
        &self.command_id
    }

    fn validate(&self) -> Result<()> {
        validate_text("receipt_client_order_id", &self.client_order_id)?;
        validate_text("runtime_run_id", &self.runtime_run_id)?;
        validate_text("command_id", &self.command_id)
    }
}

/// Terminal state plus optional runtime revision used to reconcile an order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HardRiskTerminalObservation {
    state: HardRiskTerminalState,
    runtime_revision: Option<u64>,
}

impl HardRiskTerminalObservation {
    #[must_use]
    pub fn new(state: HardRiskTerminalState) -> Self {
        Self {
            state,
            runtime_revision: None,
        }
    }

    #[must_use]
    pub fn runtime_revision(mut self, value: u64) -> Self {
        self.runtime_revision = Some(value);
        self
    }

    #[must_use]
    pub fn state(&self) -> HardRiskTerminalState {
        self.state
    }

    #[must_use]
    pub fn runtime_revision_value(&self) -> Option<u64> {
        self.runtime_revision
    }
}

/// Caller-asserted proof that one exact account/trading-day trade snapshot was
/// fully consumed through `runtime_revision`. The authority cannot fabricate
/// this proof; applications must create it only after their own completeness
/// check succeeds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HardRiskCompleteTradeSnapshot {
    scope: HardRiskScope,
    runtime_revision: u64,
}

impl HardRiskCompleteTradeSnapshot {
    #[must_use]
    pub fn new(scope: HardRiskScope, runtime_revision: u64) -> Self {
        Self {
            scope,
            runtime_revision,
        }
    }

    #[must_use]
    pub fn scope(&self) -> &HardRiskScope {
        &self.scope
    }

    #[must_use]
    pub fn runtime_revision(&self) -> u64 {
        self.runtime_revision
    }
}

/// Whether one exact trade scope is safe to recover. Incomplete state cannot
/// mutate any durable order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HardRiskRecoveryReadiness {
    CompleteTradeSnapshot(HardRiskCompleteTradeSnapshot),
    IncompleteTradeSnapshot { scope: HardRiskScope },
}

/// Retained audit event. Event rows are append-only; terminal state is not a
/// cleanup signal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HardRiskAuditEvent {
    state: String,
    kind: String,
    detail: String,
    occurred_at_ms: i64,
}

impl HardRiskAuditEvent {
    #[must_use]
    pub fn state(&self) -> &str {
        &self.state
    }

    #[must_use]
    pub fn kind(&self) -> &str {
        &self.kind
    }

    #[must_use]
    pub fn detail(&self) -> &str {
        &self.detail
    }

    #[must_use]
    pub fn occurred_at_ms(&self) -> i64 {
        self.occurred_at_ms
    }
}

/// Fail-closed authority errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HardRiskError {
    InvalidInput {
        field: &'static str,
        reason: String,
    },
    Storage {
        operation: &'static str,
        reason: String,
    },
    Schema {
        reason: String,
    },
    InvalidStoredData {
        reason: String,
    },
    IdentityConflict {
        key: HardRiskOrderKey,
    },
    PolicyNotConfigured {
        scope: HardRiskPolicyScope,
    },
    PolicyConflict {
        scope: Box<HardRiskPolicyScope>,
        expected: Box<HardRiskPolicyExpectation>,
        actual: Box<Option<HardRiskPolicyRevision>>,
    },
    UnknownOrder {
        key: HardRiskOrderKey,
    },
    LimitExceeded {
        rule_id: &'static str,
        used: i64,
        requested: i64,
        limit: i64,
    },
    LeaseFenced {
        key: HardRiskOrderKey,
    },
    LeaseExpired {
        key: HardRiskOrderKey,
    },
    ReceiptIdentityMismatch {
        key: HardRiskOrderKey,
        receipt_client_order_id: String,
    },
    ClockRegression {
        observed_at_ms: i64,
        last_persisted_at_ms: i64,
    },
    RecoveryScopeMismatch {
        key: Box<HardRiskOrderKey>,
        recovery_scope: Box<HardRiskScope>,
    },
    StateConflict {
        key: HardRiskOrderKey,
        expected: String,
        actual: HardRiskOrderState,
    },
    IncompleteTradeSnapshot,
}

impl Display for HardRiskError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidInput { field, reason } => {
                write!(formatter, "invalid hard-risk {field}: {reason}")
            }
            Self::Storage { operation, reason } => {
                write!(formatter, "hard-risk SQLite {operation} failed: {reason}")
            }
            Self::Schema { reason } => write!(formatter, "hard-risk schema rejected: {reason}"),
            Self::InvalidStoredData { reason } => {
                write!(formatter, "invalid hard-risk durable data: {reason}")
            }
            Self::IdentityConflict { key } => {
                write!(
                    formatter,
                    "hard-risk client order identity conflicts: {key}"
                )
            }
            Self::PolicyNotConfigured { scope } => write!(
                formatter,
                "no active hard-risk policy for namespace={} account_id={}",
                scope.namespace, scope.account_id
            ),
            Self::PolicyConflict {
                scope,
                expected,
                actual,
            } => write!(
                formatter,
                "hard-risk policy compare-and-set conflict for namespace={} account_id={}: expected {expected:?}, actual {actual:?}",
                scope.namespace, scope.account_id
            ),
            Self::UnknownOrder { key } => write!(formatter, "unknown hard-risk order: {key}"),
            Self::LimitExceeded {
                rule_id,
                used,
                requested,
                limit,
            } => write!(
                formatter,
                "hard-risk rule {rule_id} rejects used={used} requested={requested} limit={limit}"
            ),
            Self::LeaseFenced { key } => {
                write!(formatter, "hard-risk submission lease fenced: {key}")
            }
            Self::LeaseExpired { key } => write!(
                formatter,
                "hard-risk submission lease expired; order is indeterminate: {key}"
            ),
            Self::ReceiptIdentityMismatch {
                key,
                receipt_client_order_id,
            } => write!(
                formatter,
                "hard-risk receipt client id {receipt_client_order_id:?} does not match admitted order: {key}"
            ),
            Self::ClockRegression {
                observed_at_ms,
                last_persisted_at_ms,
            } => write!(
                formatter,
                "hard-risk wall clock regressed observed={observed_at_ms} last_persisted={last_persisted_at_ms}; fail closed"
            ),
            Self::RecoveryScopeMismatch {
                key,
                recovery_scope,
            } => write!(
                formatter,
                "hard-risk recovery scope namespace={} account_id={} trading_day={} cannot reconcile {key}",
                recovery_scope.namespace, recovery_scope.account_id, recovery_scope.trading_day
            ),
            Self::StateConflict {
                key,
                expected,
                actual,
            } => write!(
                formatter,
                "hard-risk state conflict for {key}: expected {expected}, actual {actual:?}"
            ),
            Self::IncompleteTradeSnapshot => write!(
                formatter,
                "hard-risk recovery requires a complete trade snapshot; fail closed"
            ),
        }
    }
}

impl std::error::Error for HardRiskError {}

fn validate_text(field: &'static str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(HardRiskError::InvalidInput {
            field,
            reason: "must be non-empty".to_owned(),
        });
    }
    if value.len() > 4_096 {
        return Err(HardRiskError::InvalidInput {
            field,
            reason: "must not exceed 4096 bytes".to_owned(),
        });
    }
    Ok(())
}

fn unix_epoch_millis() -> Result<i64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| HardRiskError::InvalidInput {
            field: "system clock",
            reason: error.to_string(),
        })?;
    i64::try_from(duration.as_millis()).map_err(|_| HardRiskError::InvalidInput {
        field: "system clock",
        reason: "milliseconds do not fit SQLite integer".to_owned(),
    })
}

fn duration_millis(duration: Duration) -> Result<i64> {
    i64::try_from(duration.as_millis()).map_err(|_| HardRiskError::InvalidInput {
        field: "duration",
        reason: "milliseconds do not fit SQLite integer".to_owned(),
    })
}

fn policy_fingerprint(policy: &HardRiskPolicy) -> String {
    let mut hasher = Sha256::new();
    hash_field(&mut hasher, b"tqsdk-hard-risk-policy-v1");
    hash_field(&mut hasher, policy.policy_id.as_bytes());
    hash_field(&mut hasher, policy.policy_version.as_bytes());
    hash_field(&mut hasher, policy.reservation_policy.as_sql().as_bytes());
    match policy.max_order_attempts {
        Some(value) => {
            hash_field(&mut hasher, b"max_order_attempts:some");
            hash_field(&mut hasher, &value.to_be_bytes());
        }
        None => hash_field(&mut hasher, b"max_order_attempts:none"),
    }
    match policy.max_open_volume {
        Some(value) => {
            hash_field(&mut hasher, b"max_open_volume:some");
            hash_field(&mut hasher, &value.to_be_bytes());
        }
        None => hash_field(&mut hasher, b"max_open_volume:none"),
    }
    hex_encode(&hasher.finalize())
}

fn request_fingerprint(request: &HardRiskOrderRequest, policy: &HardRiskPolicy) -> String {
    let mut hasher = Sha256::new();
    for value in [
        request.scope.namespace.as_bytes(),
        request.scope.account_id.as_bytes(),
        request.scope.trading_day.as_bytes(),
        request.client_order_id.as_bytes(),
        request.symbol.as_bytes(),
        request.direction.as_sql().as_bytes(),
        request.offset.as_sql().as_bytes(),
        request.volume.to_be_bytes().as_slice(),
        policy.policy_id.as_bytes(),
        policy.policy_version.as_bytes(),
        policy.reservation_policy.as_sql().as_bytes(),
        &policy.max_order_attempts.unwrap_or_default().to_be_bytes(),
        &policy.max_open_volume.unwrap_or_default().to_be_bytes(),
    ] {
        hash_field(&mut hasher, value);
    }
    match policy.max_order_attempts {
        Some(_) => hash_field(&mut hasher, b"max_order_attempts:some"),
        None => hash_field(&mut hasher, b"max_order_attempts:none"),
    }
    match policy.max_open_volume {
        Some(_) => hash_field(&mut hasher, b"max_open_volume:some"),
        None => hash_field(&mut hasher, b"max_open_volume:none"),
    }
    match request.limit_price_bits {
        Some(bits) => {
            hash_field(&mut hasher, b"limit_price_bits:some");
            hash_field(&mut hasher, &bits.to_be_bytes());
        }
        None => hash_field(&mut hasher, b"limit_price_bits:none"),
    }
    hex_encode(&hasher.finalize())
}

fn hash_field(hasher: &mut Sha256, value: &[u8]) {
    hasher.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    hasher.update(value);
}

fn hex_encode(value: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(value.len() * 2);
    for byte in value {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn storage_error(operation: &'static str, error: rusqlite::Error) -> HardRiskError {
    HardRiskError::Storage {
        operation,
        reason: error.to_string(),
    }
}

fn ensure_identity(record: &HardRiskOrderRecord, identity: &HardRiskOrderIdentity) -> Result<()> {
    if record.identity != *identity {
        return Err(HardRiskError::IdentityConflict { key: record.key() });
    }
    Ok(())
}

fn read_usage(connection: &Connection, scope: &HardRiskScope, rule_id: &str) -> Result<i64> {
    connection
        .query_row(
            "SELECT used FROM hard_risk_daily_usage WHERE namespace = ?1 AND account_id = ?2 AND trading_day = ?3 AND rule_id = ?4",
            params![
                scope.namespace,
                scope.account_id,
                scope.trading_day,
                rule_id
            ],
            |row| row.get(0),
        )
        .optional()
        .map(|value| value.unwrap_or_default())
        .map_err(|error| storage_error("read hard-risk usage", error))
}

fn advance_clock(transaction: &Transaction<'_>, now: i64) -> Result<()> {
    let last_persisted_at_ms: i64 = transaction
        .query_row(
            "SELECT max_observed_at_ms FROM hard_risk_clock WHERE singleton = 1",
            [],
            |row| row.get(0),
        )
        .map_err(|error| storage_error("read hard-risk monotonic clock", error))?;
    if now < last_persisted_at_ms {
        return Err(HardRiskError::ClockRegression {
            observed_at_ms: now,
            last_persisted_at_ms,
        });
    }
    transaction
        .execute(
            "UPDATE hard_risk_clock SET max_observed_at_ms = ?1 WHERE singleton = 1",
            params![now],
        )
        .map_err(|error| storage_error("advance hard-risk monotonic clock", error))?;
    Ok(())
}

fn read_active_policy(
    connection: &Connection,
    scope: &HardRiskPolicyScope,
) -> Result<Option<HardRiskPolicyRecord>> {
    let raw = connection
        .query_row(
            r#"
            SELECT
                policy.policy_id,
                policy.policy_version,
                policy.policy_fingerprint,
                policy.max_order_attempts,
                policy.max_open_volume,
                policy.reservation_policy,
                active.policy_fingerprint,
                active.activated_at_ms
            FROM hard_risk_active_policies AS active
            JOIN hard_risk_policies AS policy
                ON policy.namespace = active.namespace
                AND policy.account_id = active.account_id
                AND policy.policy_id = active.policy_id
                AND policy.policy_version = active.policy_version
                AND policy.policy_fingerprint = active.policy_fingerprint
            WHERE active.namespace = ?1 AND active.account_id = ?2
            "#,
            params![scope.namespace, scope.account_id],
            RawPolicy::from_row,
        )
        .optional()
        .map_err(|error| storage_error("read active hard-risk policy", error))?;
    raw.map(|raw| raw.into_record(scope.clone())).transpose()
}

fn read_policy_version(
    connection: &Connection,
    scope: &HardRiskPolicyScope,
    revision: &HardRiskPolicyRevision,
) -> Result<Option<HardRiskPolicyRecord>> {
    let raw = connection
        .query_row(
            r#"
            SELECT
                policy_id,
                policy_version,
                policy_fingerprint,
                max_order_attempts,
                max_open_volume,
                reservation_policy,
                policy_fingerprint,
                created_at_ms
            FROM hard_risk_policies
            WHERE namespace = ?1
                AND account_id = ?2
                AND policy_id = ?3
                AND policy_version = ?4
                AND policy_fingerprint = ?5
            "#,
            params![
                scope.namespace,
                scope.account_id,
                revision.policy_id,
                revision.policy_version,
                revision.policy_fingerprint,
            ],
            RawPolicy::from_row,
        )
        .optional()
        .map_err(|error| storage_error("read hard-risk policy version", error))?;
    raw.map(|raw| raw.into_record(scope.clone())).transpose()
}

#[derive(Debug)]
struct RawPolicy {
    policy_id: String,
    policy_version: String,
    policy_fingerprint: String,
    max_order_attempts: Option<i64>,
    max_open_volume: Option<i64>,
    reservation_policy: String,
    active_policy_fingerprint: String,
    activated_at_ms: i64,
}

impl RawPolicy {
    fn from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            policy_id: row.get(0)?,
            policy_version: row.get(1)?,
            policy_fingerprint: row.get(2)?,
            max_order_attempts: row.get(3)?,
            max_open_volume: row.get(4)?,
            reservation_policy: row.get(5)?,
            active_policy_fingerprint: row.get(6)?,
            activated_at_ms: row.get(7)?,
        })
    }

    fn into_record(self, scope: HardRiskPolicyScope) -> Result<HardRiskPolicyRecord> {
        scope
            .validate()
            .map_err(|error| HardRiskError::InvalidStoredData {
                reason: format!("invalid hard-risk policy scope: {error}"),
            })?;
        if self.policy_fingerprint != self.active_policy_fingerprint {
            return Err(HardRiskError::InvalidStoredData {
                reason: "active hard-risk policy fingerprint does not match policy row".to_owned(),
            });
        }
        let reservation_policy = match self.reservation_policy.as_str() {
            "CountPreparedAttempt" => HardRiskReservationPolicy::CountPreparedAttempt,
            value => {
                return Err(HardRiskError::InvalidStoredData {
                    reason: format!("unknown hard-risk reservation policy {value:?}"),
                });
            }
        };
        let policy = HardRiskPolicy {
            policy_id: self.policy_id,
            policy_version: self.policy_version,
            max_order_attempts: self.max_order_attempts,
            max_open_volume: self.max_open_volume,
            reservation_policy,
        };
        policy
            .validate()
            .map_err(|error| HardRiskError::InvalidStoredData {
                reason: format!("invalid stored hard-risk policy: {error}"),
            })?;
        let revision = policy.revision();
        if revision.policy_fingerprint != self.policy_fingerprint {
            return Err(HardRiskError::InvalidStoredData {
                reason: "stored hard-risk policy fingerprint does not match content".to_owned(),
            });
        }
        Ok(HardRiskPolicyRecord {
            scope,
            policy,
            revision,
            activated_at_ms: self.activated_at_ms,
        })
    }
}

fn read_order(
    connection: &Connection,
    key: &HardRiskOrderKey,
) -> Result<Option<HardRiskOrderRecord>> {
    let raw = connection
        .query_row(
            r#"
                SELECT namespace, account_id, client_order_id, request_fingerprint, trading_day,
                       policy_id, policy_version, policy_fingerprint, reservation_policy, symbol, direction, offset, volume,
                       limit_price_bits, state, lease_owner, lease_generation, lease_expires_at_ms,
                       runtime_run_id, command_id, terminal_state, terminal_revision, indeterminate_reason,
                       created_at_ms, updated_at_ms
                FROM hard_risk_orders
                WHERE namespace = ?1 AND account_id = ?2 AND client_order_id = ?3
            "#,
            params![key.namespace, key.account_id, key.client_order_id],
            RawOrder::from_row,
        )
        .optional()
        .map_err(|error| storage_error("read hard-risk order", error))?;
    raw.map(RawOrder::into_record).transpose()
}

fn write_event(
    transaction: &Transaction<'_>,
    key: &HardRiskOrderKey,
    request_fingerprint: &str,
    state: &str,
    kind: &str,
    detail: &str,
    now: i64,
) -> Result<()> {
    transaction
        .execute(
            r#"
                INSERT INTO hard_risk_events (
                    namespace, account_id, client_order_id, request_fingerprint, state, event_kind, detail, occurred_at_ms
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            "#,
            params![
                key.namespace,
                key.account_id,
                key.client_order_id,
                request_fingerprint,
                state,
                kind,
                detail,
                now,
            ],
        )
        .map_err(|error| storage_error("append hard-risk audit event", error))?;
    Ok(())
}

fn mark_indeterminate_in_transaction(
    transaction: &Transaction<'_>,
    existing: &HardRiskOrderRecord,
    reason: &str,
    now: i64,
) -> Result<()> {
    transaction
        .execute(
            r#"
                UPDATE hard_risk_orders
                SET state = 'Indeterminate', indeterminate_reason = ?1, updated_at_ms = ?2
                WHERE namespace = ?3 AND account_id = ?4 AND client_order_id = ?5
                  AND state = 'Submitting'
            "#,
            params![
                reason,
                now,
                existing.request.scope.namespace,
                existing.request.scope.account_id,
                existing.request.client_order_id,
            ],
        )
        .map_err(|error| storage_error("mark submission indeterminate", error))?;
    write_event(
        transaction,
        &existing.key(),
        &existing.identity.request_fingerprint,
        "Indeterminate",
        "indeterminate",
        reason,
        now,
    )
}

fn create_schema_v1(transaction: &Transaction<'_>) -> Result<()> {
    transaction
        .execute_batch(
            "CREATE TABLE hard_risk_schema_metadata (
                metadata_key TEXT PRIMARY KEY,
                metadata_value TEXT NOT NULL
            );
            INSERT INTO hard_risk_schema_metadata (metadata_key, metadata_value)
                VALUES ('contract_id', 'tqsdk-hard-risk/schema/v1/2026-09-08');
            CREATE TABLE hard_risk_clock (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                max_observed_at_ms INTEGER NOT NULL
            );
            INSERT INTO hard_risk_clock (singleton, max_observed_at_ms) VALUES (1, 0);
            CREATE TABLE hard_risk_policies (
                namespace TEXT NOT NULL,
                account_id TEXT NOT NULL,
                policy_id TEXT NOT NULL,
                policy_version TEXT NOT NULL,
                policy_fingerprint TEXT NOT NULL,
                max_order_attempts INTEGER,
                max_open_volume INTEGER,
                reservation_policy TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                PRIMARY KEY (namespace, account_id, policy_id, policy_version),
                UNIQUE (namespace, account_id, policy_id, policy_version, policy_fingerprint)
            );
            CREATE TABLE hard_risk_active_policies (
                namespace TEXT NOT NULL,
                account_id TEXT NOT NULL,
                policy_id TEXT NOT NULL,
                policy_version TEXT NOT NULL,
                policy_fingerprint TEXT NOT NULL,
                activated_at_ms INTEGER NOT NULL,
                PRIMARY KEY (namespace, account_id),
        FOREIGN KEY (namespace, account_id, policy_id, policy_version, policy_fingerprint)
        REFERENCES hard_risk_policies(namespace, account_id, policy_id, policy_version, policy_fingerprint)
                    ON DELETE RESTRICT
            );
            CREATE TABLE hard_risk_policy_events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                namespace TEXT NOT NULL,
                account_id TEXT NOT NULL,
                policy_id TEXT NOT NULL,
                policy_version TEXT NOT NULL,
                policy_fingerprint TEXT NOT NULL,
                event_kind TEXT NOT NULL,
                occurred_at_ms INTEGER NOT NULL
            );
            CREATE TABLE hard_risk_orders (
                namespace TEXT NOT NULL,
                account_id TEXT NOT NULL,
                client_order_id TEXT NOT NULL,
                request_fingerprint TEXT NOT NULL,
                trading_day TEXT NOT NULL,
                policy_id TEXT NOT NULL,
                policy_version TEXT NOT NULL,
                policy_fingerprint TEXT NOT NULL,
                reservation_policy TEXT NOT NULL,
                symbol TEXT NOT NULL,
                direction TEXT NOT NULL,
                offset TEXT NOT NULL,
                volume INTEGER NOT NULL CHECK (volume > 0),
                limit_price_bits TEXT,
                state TEXT NOT NULL CHECK (state IN ('Prepared', 'Submitting', 'Submitted', 'Indeterminate', 'Terminal')),
                lease_owner TEXT,
                lease_generation INTEGER NOT NULL CHECK (lease_generation >= 0),
                lease_expires_at_ms INTEGER,
                runtime_run_id TEXT,
                command_id TEXT,
                terminal_state TEXT,
                terminal_revision INTEGER,
                indeterminate_reason TEXT,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                PRIMARY KEY (namespace, account_id, client_order_id),
        FOREIGN KEY (namespace, account_id, policy_id, policy_version, policy_fingerprint)
        REFERENCES hard_risk_policies(namespace, account_id, policy_id, policy_version, policy_fingerprint)
                    ON DELETE RESTRICT
            );
            CREATE TABLE hard_risk_daily_usage (
                namespace TEXT NOT NULL,
                account_id TEXT NOT NULL,
                trading_day TEXT NOT NULL,
                rule_id TEXT NOT NULL,
                used INTEGER NOT NULL CHECK (used >= 0),
                updated_at_ms INTEGER NOT NULL,
                PRIMARY KEY (namespace, account_id, trading_day, rule_id)
            );
            CREATE TABLE hard_risk_reservations (
                namespace TEXT NOT NULL,
                account_id TEXT NOT NULL,
                client_order_id TEXT NOT NULL,
                rule_id TEXT NOT NULL,
                amount INTEGER NOT NULL CHECK (amount > 0),
                created_at_ms INTEGER NOT NULL,
                PRIMARY KEY (namespace, account_id, client_order_id, rule_id),
                FOREIGN KEY (namespace, account_id, client_order_id)
                    REFERENCES hard_risk_orders(namespace, account_id, client_order_id)
                    ON DELETE RESTRICT
            );
            CREATE TABLE hard_risk_events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                namespace TEXT NOT NULL,
                account_id TEXT NOT NULL,
                client_order_id TEXT NOT NULL,
                request_fingerprint TEXT NOT NULL,
                state TEXT NOT NULL,
                event_kind TEXT NOT NULL,
                detail TEXT NOT NULL,
                occurred_at_ms INTEGER NOT NULL,
                FOREIGN KEY (namespace, account_id, client_order_id)
                    REFERENCES hard_risk_orders(namespace, account_id, client_order_id)
                    ON DELETE RESTRICT
            );
            CREATE INDEX hard_risk_orders_recovery_idx
                ON hard_risk_orders(state, lease_expires_at_ms);
            CREATE INDEX hard_risk_events_order_idx
                ON hard_risk_events(namespace, account_id, client_order_id, event_id);
            CREATE INDEX hard_risk_policy_events_scope_idx
                ON hard_risk_policy_events(namespace, account_id, event_id);",
        )
        .map_err(|error| storage_error("create hard-risk schema v1", error))?;
    Ok(())
}

fn verify_schema_v1(transaction: &Transaction<'_>) -> Result<()> {
    for table in [
        "hard_risk_schema_metadata",
        "hard_risk_clock",
        "hard_risk_policies",
        "hard_risk_active_policies",
        "hard_risk_policy_events",
        "hard_risk_orders",
        "hard_risk_daily_usage",
        "hard_risk_reservations",
        "hard_risk_events",
    ] {
        let exists: i64 = transaction
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
                params![table],
                |row| row.get(0),
            )
            .map_err(|error| storage_error("verify hard-risk schema", error))?;
        if exists != 1 {
            return Err(HardRiskError::Schema {
                reason: format!("schema version {SCHEMA_VERSION} misses table {table}"),
            });
        }
    }

    let contract_id: Option<String> = transaction
        .query_row(
            "SELECT metadata_value FROM hard_risk_schema_metadata WHERE metadata_key = 'contract_id'",
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| storage_error("read hard-risk schema contract", error))?;
    if contract_id.as_deref() != Some(SCHEMA_CONTRACT_ID) {
        return Err(HardRiskError::Schema {
            reason: format!(
                "schema version {SCHEMA_VERSION} has incompatible contract id {contract_id:?}"
            ),
        });
    }

    for (table, required_columns) in [
        (
            "hard_risk_schema_metadata",
            &["metadata_key", "metadata_value"][..],
        ),
        ("hard_risk_clock", &["singleton", "max_observed_at_ms"][..]),
        (
            "hard_risk_policies",
            &[
                "namespace",
                "account_id",
                "policy_id",
                "policy_version",
                "policy_fingerprint",
                "max_order_attempts",
                "max_open_volume",
                "reservation_policy",
                "created_at_ms",
            ][..],
        ),
        (
            "hard_risk_active_policies",
            &[
                "namespace",
                "account_id",
                "policy_id",
                "policy_version",
                "policy_fingerprint",
                "activated_at_ms",
            ][..],
        ),
        (
            "hard_risk_policy_events",
            &[
                "event_id",
                "namespace",
                "account_id",
                "policy_id",
                "policy_version",
                "policy_fingerprint",
                "event_kind",
                "occurred_at_ms",
            ][..],
        ),
        (
            "hard_risk_orders",
            &[
                "namespace",
                "account_id",
                "client_order_id",
                "request_fingerprint",
                "trading_day",
                "policy_id",
                "policy_version",
                "policy_fingerprint",
                "reservation_policy",
                "symbol",
                "direction",
                "offset",
                "volume",
                "limit_price_bits",
                "state",
                "lease_owner",
                "lease_generation",
                "lease_expires_at_ms",
                "runtime_run_id",
                "command_id",
                "terminal_state",
                "terminal_revision",
                "indeterminate_reason",
                "created_at_ms",
                "updated_at_ms",
            ][..],
        ),
        (
            "hard_risk_daily_usage",
            &[
                "namespace",
                "account_id",
                "trading_day",
                "rule_id",
                "used",
                "updated_at_ms",
            ][..],
        ),
        (
            "hard_risk_reservations",
            &[
                "namespace",
                "account_id",
                "client_order_id",
                "rule_id",
                "amount",
                "created_at_ms",
            ][..],
        ),
        (
            "hard_risk_events",
            &[
                "event_id",
                "namespace",
                "account_id",
                "client_order_id",
                "request_fingerprint",
                "state",
                "event_kind",
                "detail",
                "occurred_at_ms",
            ][..],
        ),
    ] {
        verify_schema_columns(transaction, table, required_columns)?;
    }

    for index in [
        "hard_risk_orders_recovery_idx",
        "hard_risk_events_order_idx",
        "hard_risk_policy_events_scope_idx",
    ] {
        let exists: i64 = transaction
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?1",
                params![index],
                |row| row.get(0),
            )
            .map_err(|error| storage_error("verify hard-risk schema index", error))?;
        if exists != 1 {
            return Err(HardRiskError::Schema {
                reason: format!("schema version {SCHEMA_VERSION} misses index {index}"),
            });
        }
    }

    for (table, referenced_table) in [
        ("hard_risk_active_policies", "hard_risk_policies"),
        ("hard_risk_orders", "hard_risk_policies"),
        ("hard_risk_reservations", "hard_risk_orders"),
        ("hard_risk_events", "hard_risk_orders"),
    ] {
        verify_schema_foreign_key(transaction, table, referenced_table)?;
    }

    for (table, required_sql) in [
        ("hard_risk_clock", "check (singleton = 1)"),
        ("hard_risk_orders", "check (state in"),
        ("hard_risk_orders", "check (volume > 0)"),
        ("hard_risk_daily_usage", "check (used >= 0)"),
        ("hard_risk_reservations", "check (amount > 0)"),
    ] {
        let sql: String = transaction
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?1",
                params![table],
                |row| row.get(0),
            )
            .map_err(|error| storage_error("read hard-risk schema SQL", error))?;
        if !sql.to_ascii_lowercase().contains(required_sql) {
            return Err(HardRiskError::Schema {
                reason: format!(
                    "schema version {SCHEMA_VERSION} table {table} misses required constraint {required_sql:?}"
                ),
            });
        }
    }

    let foreign_key_violation: Option<String> = transaction
        .query_row("PRAGMA foreign_key_check", [], |row| row.get(0))
        .optional()
        .map_err(|error| storage_error("verify hard-risk foreign keys", error))?;
    if let Some(table) = foreign_key_violation {
        return Err(HardRiskError::Schema {
            reason: format!("schema version {SCHEMA_VERSION} has foreign-key violation in {table}"),
        });
    }
    Ok(())
}

fn verify_schema_columns(
    transaction: &Transaction<'_>,
    table: &str,
    required_columns: &[&str],
) -> Result<()> {
    let mut statement = transaction
        .prepare(&format!("PRAGMA table_info({table})"))
        .map_err(|error| storage_error("inspect hard-risk schema columns", error))?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(|error| storage_error("read hard-risk schema columns", error))?
        .collect::<std::result::Result<std::collections::BTreeSet<_>, _>>()
        .map_err(|error| storage_error("decode hard-risk schema columns", error))?;
    for column in required_columns {
        if !columns.contains(*column) {
            return Err(HardRiskError::Schema {
                reason: format!(
                    "schema version {SCHEMA_VERSION} table {table} misses column {column}"
                ),
            });
        }
    }
    Ok(())
}

fn verify_schema_foreign_key(
    transaction: &Transaction<'_>,
    table: &str,
    referenced_table: &str,
) -> Result<()> {
    let mut statement = transaction
        .prepare(&format!("PRAGMA foreign_key_list({table})"))
        .map_err(|error| storage_error("inspect hard-risk schema foreign keys", error))?;
    let tables = statement
        .query_map([], |row| row.get::<_, String>(2))
        .map_err(|error| storage_error("read hard-risk schema foreign keys", error))?
        .collect::<std::result::Result<std::collections::BTreeSet<_>, _>>()
        .map_err(|error| storage_error("decode hard-risk schema foreign keys", error))?;
    if !tables.contains(referenced_table) {
        return Err(HardRiskError::Schema {
            reason: format!(
                "schema version {SCHEMA_VERSION} table {table} misses foreign key to {referenced_table}"
            ),
        });
    }
    Ok(())
}

#[derive(Debug)]
struct RawOrder {
    namespace: String,
    account_id: String,
    client_order_id: String,
    request_fingerprint: String,
    trading_day: String,
    policy_id: String,
    policy_version: String,
    policy_fingerprint: String,
    reservation_policy: String,
    symbol: String,
    direction: String,
    offset: String,
    volume: i64,
    limit_price_bits: Option<String>,
    state: String,
    lease_owner: Option<String>,
    lease_generation: i64,
    lease_expires_at_ms: Option<i64>,
    runtime_run_id: Option<String>,
    command_id: Option<String>,
    terminal_state: Option<String>,
    terminal_revision: Option<i64>,
    indeterminate_reason: Option<String>,
    created_at_ms: i64,
    updated_at_ms: i64,
}

impl RawOrder {
    fn from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            namespace: row.get(0)?,
            account_id: row.get(1)?,
            client_order_id: row.get(2)?,
            request_fingerprint: row.get(3)?,
            trading_day: row.get(4)?,
            policy_id: row.get(5)?,
            policy_version: row.get(6)?,
            policy_fingerprint: row.get(7)?,
            reservation_policy: row.get(8)?,
            symbol: row.get(9)?,
            direction: row.get(10)?,
            offset: row.get(11)?,
            volume: row.get(12)?,
            limit_price_bits: row.get(13)?,
            state: row.get(14)?,
            lease_owner: row.get(15)?,
            lease_generation: row.get(16)?,
            lease_expires_at_ms: row.get(17)?,
            runtime_run_id: row.get(18)?,
            command_id: row.get(19)?,
            terminal_state: row.get(20)?,
            terminal_revision: row.get(21)?,
            indeterminate_reason: row.get(22)?,
            created_at_ms: row.get(23)?,
            updated_at_ms: row.get(24)?,
        })
    }

    fn into_record(self) -> Result<HardRiskOrderRecord> {
        let scope = HardRiskScope {
            namespace: self.namespace,
            account_id: self.account_id,
            trading_day: self.trading_day,
        };
        let request = HardRiskOrderRequest {
            scope,
            client_order_id: self.client_order_id,
            symbol: self.symbol,
            direction: HardRiskDirection::parse(&self.direction)?,
            offset: HardRiskOffset::parse(&self.offset)?,
            volume: self.volume,
            limit_price_bits: self
                .limit_price_bits
                .as_deref()
                .map(|value| {
                    u64::from_str_radix(value, 16).map_err(|error| {
                        HardRiskError::InvalidStoredData {
                            reason: format!("invalid limit_price_bits {value:?}: {error}"),
                        }
                    })
                })
                .transpose()?,
        };
        request.validate()?;
        let state = match self.state.as_str() {
            "Prepared" => HardRiskOrderState::Prepared,
            "Submitting" => HardRiskOrderState::Submitting,
            "Submitted" => HardRiskOrderState::Submitted,
            "Indeterminate" => HardRiskOrderState::Indeterminate,
            "Terminal" => {
                let terminal_state = self.terminal_state.as_deref().ok_or_else(|| {
                    HardRiskError::InvalidStoredData {
                        reason: "terminal row has no terminal state".to_owned(),
                    }
                })?;
                HardRiskOrderState::Terminal(HardRiskTerminalState::parse(terminal_state)?)
            }
            value => {
                return Err(HardRiskError::InvalidStoredData {
                    reason: format!("unknown hard-risk state {value:?}"),
                });
            }
        };
        let reservation_policy = match self.reservation_policy.as_str() {
            "CountPreparedAttempt" => HardRiskReservationPolicy::CountPreparedAttempt,
            value => {
                return Err(HardRiskError::InvalidStoredData {
                    reason: format!("unknown hard-risk reservation policy {value:?}"),
                });
            }
        };
        let lease_generation =
            u64::try_from(self.lease_generation).map_err(|_| HardRiskError::InvalidStoredData {
                reason: "negative lease generation".to_owned(),
            })?;
        let terminal_revision = self
            .terminal_revision
            .map(u64::try_from)
            .transpose()
            .map_err(|_| HardRiskError::InvalidStoredData {
                reason: "negative terminal revision".to_owned(),
            })?;
        let runtime_receipt = match (self.runtime_run_id, self.command_id) {
            (Some(runtime_run_id), Some(command_id)) => Some(HardRiskRuntimeReceipt {
                client_order_id: request.client_order_id.clone(),
                runtime_run_id,
                command_id,
            }),
            (None, None) => None,
            _ => {
                return Err(HardRiskError::InvalidStoredData {
                    reason: "partial runtime receipt".to_owned(),
                });
            }
        };
        let identity = HardRiskOrderIdentity {
            key: request.key(),
            request_fingerprint: self.request_fingerprint,
        };
        Ok(HardRiskOrderRecord {
            request,
            identity,
            policy_id: self.policy_id,
            policy_version: self.policy_version,
            policy_fingerprint: self.policy_fingerprint,
            reservation_policy,
            state,
            lease_owner: self.lease_owner,
            lease_generation,
            lease_expires_at_ms: self.lease_expires_at_ms,
            runtime_receipt,
            terminal_revision,
            indeterminate_reason: self.indeterminate_reason,
            created_at_ms: self.created_at_ms,
            updated_at_ms: self.updated_at_ms,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        path::{Path, PathBuf},
        process::{self, Command},
        sync::atomic::{AtomicU64, Ordering},
        time::Duration,
    };

    use super::*;

    static TEST_DATABASE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    struct TestDatabase {
        path: PathBuf,
    }

    impl TestDatabase {
        fn new(label: &str) -> Self {
            let sequence = TEST_DATABASE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let path = env::temp_dir().join(format!(
                "tqsdk-hard-risk-{label}-{}-{sequence}.sqlite",
                process::id()
            ));
            let _ = fs::remove_file(&path);
            let _ = fs::remove_file(format!("{}-wal", path.display()));
            let _ = fs::remove_file(format!("{}-shm", path.display()));
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestDatabase {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.path);
            let _ = fs::remove_file(format!("{}-wal", self.path.display()));
            let _ = fs::remove_file(format!("{}-shm", self.path.display()));
        }
    }

    fn authority(label: &str) -> (HardRiskAuthority, TestDatabase) {
        let database = TestDatabase::new(label);
        let authority = HardRiskAuthority::open(database.path()).unwrap();
        install_test_policy(&authority, 1);
        (authority, database)
    }

    fn test_scope() -> HardRiskScope {
        HardRiskScope::new("live:user-7:route-a", "account-1", "2026-09-08")
    }

    fn request(client_order_id: &str, volume: i64) -> HardRiskOrderRequest {
        HardRiskOrderRequest::new(
            test_scope(),
            client_order_id,
            "SHFE.au2602",
            HardRiskDirection::Buy,
            HardRiskOffset::Open,
            volume,
        )
        .limit_price_bits(4_611_686_018_427_385_856)
    }

    fn policy(max_order_attempts: i64) -> HardRiskPolicy {
        HardRiskPolicy::new("desk-hard-limit", "2026-09-08.1")
            .max_order_attempts(max_order_attempts)
            .max_open_volume(10)
    }

    fn install_test_policy(authority: &HardRiskAuthority, max_order_attempts: i64) {
        authority
            .install_policy(
                test_scope().policy_scope(),
                policy(max_order_attempts),
                HardRiskPolicyExpectation::Absent,
            )
            .unwrap();
    }

    fn prepared(authority: &HardRiskAuthority, client_order_id: &str) -> HardRiskOrderRecord {
        match authority.prepare(request(client_order_id, 1)).unwrap() {
            HardRiskPrepareOutcome::Prepared(record) => record,
            outcome => panic!("expected Prepared, got {outcome:?}"),
        }
    }

    fn granted(
        authority: &HardRiskAuthority,
        record: &HardRiskOrderRecord,
    ) -> HardRiskSubmissionPermit {
        match authority
            .begin_submission(&record.identity(), "process-run-1")
            .unwrap()
        {
            HardRiskSubmissionOutcome::Granted(permit) => *permit,
            outcome => panic!("expected Granted, got {outcome:?}"),
        }
    }

    #[test]
    fn prepare_deduplicates_before_usage_and_rejects_mismatched_identity() {
        let (authority, _database) = authority("deduplicate");
        let first = authority.prepare(request("client-1", 1)).unwrap();
        let first_record = first.record().clone();
        assert!(matches!(first, HardRiskPrepareOutcome::Prepared(_)));

        let duplicate = authority.prepare(request("client-1", 1)).unwrap();
        assert!(matches!(duplicate, HardRiskPrepareOutcome::Existing(_)));
        assert_eq!(
            authority
                .daily_usage(first_record.request().scope(), ORDER_ATTEMPT_RULE_ID)
                .unwrap(),
            1
        );

        let error = authority.prepare(request("client-1", 2)).unwrap_err();
        assert!(matches!(error, HardRiskError::IdentityConflict { .. }));
    }

    #[test]
    fn terminal_evidence_is_retained_and_reservation_is_not_released() {
        let (authority, _database) = authority("terminal-retained");
        let record = match authority.prepare(request("client-1", 1)).unwrap() {
            HardRiskPrepareOutcome::Prepared(record) => record,
            outcome => panic!("expected Prepared, got {outcome:?}"),
        };
        let permit = granted(&authority, &record);
        let submitted = authority
            .record_submitted(
                permit,
                HardRiskRuntimeReceipt::for_request(record.request(), "runtime-a", "1"),
            )
            .unwrap();
        let terminal = authority
            .observe_terminal(
                &submitted.identity(),
                HardRiskTerminalObservation::new(HardRiskTerminalState::Filled)
                    .runtime_revision(42),
            )
            .unwrap();
        assert_eq!(
            terminal.state(),
            &HardRiskOrderState::Terminal(HardRiskTerminalState::Filled)
        );
        assert_eq!(terminal.terminal_revision(), Some(42));
        assert_eq!(
            authority
                .daily_usage(terminal.request().scope(), ORDER_ATTEMPT_RULE_ID)
                .unwrap(),
            1
        );
        let error = authority.prepare(request("client-2", 1)).unwrap_err();
        assert!(matches!(error, HardRiskError::LimitExceeded { .. }));
        let idempotent = authority
            .observe_terminal(
                &terminal.identity(),
                HardRiskTerminalObservation::new(HardRiskTerminalState::Filled)
                    .runtime_revision(42),
            )
            .unwrap();
        assert_eq!(idempotent, terminal);
        let events = authority.audit_events(&terminal.identity()).unwrap();
        assert_eq!(events.len(), 4);
        assert_eq!(events.last().unwrap().kind(), "terminal");
    }

    #[test]
    fn expired_lease_becomes_indeterminate_and_cannot_be_reused() {
        let (authority, _database) = authority("expired-lease");
        let record = prepared(&authority, "client-1");
        let permit = granted(&authority, &record);
        let connection = authority.connect().unwrap();
        connection
            .execute(
                "UPDATE hard_risk_orders SET lease_expires_at_ms = 0 WHERE namespace = ?1 AND account_id = ?2 AND client_order_id = ?3",
                params![
                    record.request().scope().namespace(),
                    record.request().scope().account_id(),
                    record.request().client_order_id(),
                ],
            )
            .unwrap();
        match authority
            .begin_submission(&record.identity(), "process-run-2")
            .unwrap()
        {
            HardRiskSubmissionOutcome::Existing(existing) => {
                assert_eq!(existing.state(), &HardRiskOrderState::Indeterminate);
            }
            outcome => panic!("expected Existing indeterminate, got {outcome:?}"),
        }
        let error = authority
            .record_submitted(
                permit,
                HardRiskRuntimeReceipt::for_request(record.request(), "runtime-a", "1"),
            )
            .unwrap_err();
        assert!(matches!(error, HardRiskError::StateConflict { .. }));
    }

    #[test]
    fn recovery_requires_complete_snapshot_then_marks_expired_submission() {
        let (authority, _database) = authority("recovery");
        let record = prepared(&authority, "client-1");
        let scope = record.request().scope().clone();
        let _permit = granted(&authority, &record);
        let connection = authority.connect().unwrap();
        connection
            .execute(
                "UPDATE hard_risk_orders SET lease_expires_at_ms = 0 WHERE namespace = ?1 AND account_id = ?2 AND client_order_id = ?3",
                params![
                    record.request().scope().namespace(),
                    record.request().scope().account_id(),
                    record.request().client_order_id(),
                ],
            )
            .unwrap();
        let error = authority
            .recover_expired_submissions(HardRiskRecoveryReadiness::IncompleteTradeSnapshot {
                scope: scope.clone(),
            })
            .unwrap_err();
        assert_eq!(error, HardRiskError::IncompleteTradeSnapshot);
        assert_eq!(
            authority
                .recover_expired_submissions(HardRiskRecoveryReadiness::CompleteTradeSnapshot(
                    HardRiskCompleteTradeSnapshot::new(scope, 7),
                ))
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            authority.get(&record.key()).unwrap().unwrap().state(),
            &HardRiskOrderState::Indeterminate
        );
    }

    #[test]
    fn command_id_reuse_after_restart_is_not_durable_identity() {
        let (authority, database) = authority("command-id-reuse");
        let first = prepared(&authority, "client-1");
        let first_permit = granted(&authority, &first);
        let first_submitted = authority
            .record_submitted(
                first_permit,
                HardRiskRuntimeReceipt::for_request(first.request(), "runtime-a", "1"),
            )
            .unwrap();
        drop(authority);

        let restarted = HardRiskAuthority::open(database.path()).unwrap();
        assert_eq!(
            restarted.get(&first.key()).unwrap().unwrap(),
            first_submitted
        );
        let current = restarted
            .active_policy(&test_scope().policy_scope())
            .unwrap()
            .unwrap();
        restarted
            .install_policy(
                test_scope().policy_scope(),
                HardRiskPolicy::new("desk-hard-limit", "2026-09-08.2")
                    .max_order_attempts(2)
                    .max_open_volume(10),
                HardRiskPolicyExpectation::Current(current.revision().clone()),
            )
            .unwrap();
        let second = prepared(&restarted, "client-2");
        let second_permit = granted(&restarted, &second);
        let second_submitted = restarted
            .record_submitted(
                second_permit,
                HardRiskRuntimeReceipt::for_request(second.request(), "runtime-b", "1"),
            )
            .unwrap();
        assert_ne!(first_submitted.key(), second_submitted.key());
        assert_eq!(
            second_submitted.runtime_receipt().unwrap().command_id(),
            "1"
        );
    }

    #[test]
    fn wal_and_uncommitted_usage_write_are_durable_or_absent() {
        let (authority, _database) = authority("wal-rollback");
        let connection = authority.connect().unwrap();
        let journal_mode: String = connection
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .unwrap();
        assert_eq!(journal_mode.to_ascii_lowercase(), "wal");
        drop(connection);

        let mut connection = authority.connect().unwrap();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        transaction
            .execute(
                "INSERT INTO hard_risk_daily_usage (namespace, account_id, trading_day, rule_id, used, updated_at_ms) VALUES ('test', 'account', 'day', 'order_attempt', 1, 1)",
                [],
            )
            .unwrap();
        drop(transaction);
        assert_eq!(
            authority
                .daily_usage(
                    &HardRiskScope::new("test", "account", "day"),
                    ORDER_ATTEMPT_RULE_ID
                )
                .unwrap(),
            0
        );
    }

    #[test]
    fn indeterminate_order_can_only_be_resolved_by_explicit_terminal_evidence() {
        let (authority, _database) = authority("indeterminate-terminal");
        let record = prepared(&authority, "client-1");
        let permit = granted(&authority, &record);
        let indeterminate = authority
            .record_indeterminate(permit, "network timeout after bytes written")
            .unwrap();
        assert_eq!(indeterminate.state(), &HardRiskOrderState::Indeterminate);
        let direct_error = authority
            .observe_terminal(
                &indeterminate.identity(),
                HardRiskTerminalObservation::new(HardRiskTerminalState::Cancelled),
            )
            .unwrap_err();
        assert!(matches!(direct_error, HardRiskError::StateConflict { .. }));
        let snapshot =
            HardRiskCompleteTradeSnapshot::new(indeterminate.request().scope().clone(), 42);
        let terminal = authority
            .reconcile_terminal(
                &snapshot,
                &indeterminate.identity(),
                HardRiskTerminalObservation::new(HardRiskTerminalState::Cancelled)
                    .runtime_revision(42),
            )
            .unwrap();
        assert_eq!(
            terminal.state(),
            &HardRiskOrderState::Terminal(HardRiskTerminalState::Cancelled)
        );
        assert!(
            authority
                .audit_events(&terminal.identity())
                .unwrap()
                .iter()
                .any(|event| event.kind() == "recovered_submission")
        );
    }

    #[test]
    fn unknown_or_newer_schema_is_refused_without_implicit_rewrite() {
        let database = TestDatabase::new("unknown-schema");
        let connection = Connection::open(database.path()).unwrap();
        connection
            .pragma_update(None, "user_version", 99_i64)
            .unwrap();
        drop(connection);
        let error = HardRiskAuthority::open(database.path()).unwrap_err();
        assert!(matches!(error, HardRiskError::Schema { .. }));
    }

    #[test]
    fn multi_process_prepare_child() {
        let Ok(database_path) = env::var("TQSDK_HARD_RISK_CHILD_DB") else {
            return;
        };
        let client_order_id = env::var("TQSDK_HARD_RISK_CHILD_ORDER").unwrap();
        let authority = HardRiskAuthority::open(database_path).unwrap();
        match authority.prepare(request(&client_order_id, 1)) {
            Ok(HardRiskPrepareOutcome::Prepared(_) | HardRiskPrepareOutcome::Existing(_))
            | Err(HardRiskError::LimitExceeded { .. }) => {}
            outcome => panic!("unexpected child prepare result: {outcome:?}"),
        }
    }

    #[test]
    fn multi_process_admission_has_one_daily_limit_winner() {
        let (authority, _database) = authority("multi-process");
        let executable = env::current_exe().unwrap();
        let database_path = authority.database_path().display().to_string();
        let first = Command::new(&executable)
            .args(["--exact", "tests::multi_process_prepare_child"])
            .env("TQSDK_HARD_RISK_CHILD_DB", &database_path)
            .env("TQSDK_HARD_RISK_CHILD_ORDER", "client-child-1")
            .spawn()
            .unwrap();
        let second = Command::new(&executable)
            .args(["--exact", "tests::multi_process_prepare_child"])
            .env("TQSDK_HARD_RISK_CHILD_DB", &database_path)
            .env("TQSDK_HARD_RISK_CHILD_ORDER", "client-child-2")
            .spawn()
            .unwrap();
        assert!(first.wait_with_output().unwrap().status.success());
        assert!(second.wait_with_output().unwrap().status.success());
        assert_eq!(
            authority
                .daily_usage(&test_scope(), ORDER_ATTEMPT_RULE_ID)
                .unwrap(),
            1
        );
    }

    #[test]
    fn zero_submission_lease_is_rejected() {
        let database = TestDatabase::new("zero-lease");
        let error = HardRiskAuthority::open_with_config(
            database.path(),
            HardRiskAuthorityConfig::default().submission_lease(Duration::ZERO),
        )
        .unwrap_err();
        assert!(matches!(error, HardRiskError::InvalidInput { .. }));
    }

    #[test]
    fn policy_is_authoritative_persistent_and_cas_guarded() {
        let database = TestDatabase::new("policy-cas");
        let authority = HardRiskAuthority::open(database.path()).unwrap();
        let scope = test_scope().policy_scope();
        let error = authority.prepare(request("client-1", 1)).unwrap_err();
        assert!(matches!(error, HardRiskError::PolicyNotConfigured { .. }));

        let installed = authority
            .install_policy(scope.clone(), policy(1), HardRiskPolicyExpectation::Absent)
            .unwrap();
        assert!(matches!(
            authority.prepare(request("client-1", 1)).unwrap(),
            HardRiskPrepareOutcome::Prepared(_)
        ));

        let conflict = authority
            .install_policy(
                scope.clone(),
                HardRiskPolicy::new("desk-hard-limit", "2026-09-08.2")
                    .max_order_attempts(2)
                    .max_open_volume(10),
                HardRiskPolicyExpectation::Absent,
            )
            .unwrap_err();
        assert!(matches!(conflict, HardRiskError::PolicyConflict { .. }));

        let updated = authority
            .install_policy(
                scope.clone(),
                HardRiskPolicy::new("desk-hard-limit", "2026-09-08.2")
                    .max_order_attempts(2)
                    .max_open_volume(10),
                HardRiskPolicyExpectation::Current(installed.revision().clone()),
            )
            .unwrap();
        assert_ne!(installed.revision(), updated.revision());
        assert_eq!(authority.active_policy(&scope).unwrap().unwrap(), updated);
        assert!(matches!(
            authority.prepare(request("client-2", 1)).unwrap(),
            HardRiskPrepareOutcome::Prepared(_)
        ));

        drop(authority);
        let restarted = HardRiskAuthority::open(database.path()).unwrap();
        assert_eq!(restarted.active_policy(&scope).unwrap().unwrap(), updated);
    }

    #[test]
    fn submit_once_binds_immutable_admitted_request_and_fails_closed() {
        let (success_authority, _database) = authority("submit-once-success");
        let record = prepared(&success_authority, "client-1");
        let permit = granted(&success_authority, &record);
        let outcome = success_authority
            .submit_once(permit, |admitted| {
                assert_eq!(admitted, record.request());
                Ok::<_, std::convert::Infallible>(HardRiskRuntimeReceipt::for_request(
                    admitted,
                    "runtime-a",
                    "command-1",
                ))
            })
            .unwrap();
        assert_eq!(outcome.record().state(), &HardRiskOrderState::Submitted);

        let (authority, _database) = authority("submit-once-error");
        let record = prepared(&authority, "client-1");
        let permit = granted(&authority, &record);
        let outcome = authority
            .submit_once(permit, |_| {
                Err::<HardRiskRuntimeReceipt, _>("socket timeout")
            })
            .unwrap();
        assert_eq!(outcome.record().state(), &HardRiskOrderState::Indeterminate);
    }

    #[test]
    fn receipt_identity_mismatch_marks_submission_indeterminate() {
        let (authority, _database) = authority("receipt-identity");
        let record = prepared(&authority, "client-1");
        let permit = granted(&authority, &record);
        let error = authority
            .record_submitted(
                permit,
                HardRiskRuntimeReceipt::new("wrong-client-id", "runtime-a", "command-1"),
            )
            .unwrap_err();
        assert!(matches!(
            error,
            HardRiskError::ReceiptIdentityMismatch { .. }
        ));
        assert_eq!(
            authority.get(&record.key()).unwrap().unwrap().state(),
            &HardRiskOrderState::Indeterminate
        );
    }

    #[test]
    fn recovery_and_unresolved_queries_are_exactly_scope_bounded() {
        let database = TestDatabase::new("scoped-recovery");
        let authority = HardRiskAuthority::open(database.path()).unwrap();
        authority
            .install_policy(
                test_scope().policy_scope(),
                policy(10),
                HardRiskPolicyExpectation::Absent,
            )
            .unwrap();
        let first = prepared(&authority, "client-day-one");
        let second_scope = HardRiskScope::new("live:user-7:route-a", "account-1", "2026-09-09");
        let second = match authority
            .prepare(HardRiskOrderRequest::new(
                second_scope.clone(),
                "client-day-two",
                "SHFE.au2602",
                HardRiskDirection::Buy,
                HardRiskOffset::Open,
                1,
            ))
            .unwrap()
        {
            HardRiskPrepareOutcome::Prepared(record) => record,
            outcome => panic!("expected Prepared, got {outcome:?}"),
        };
        let _first_permit = granted(&authority, &first);
        let _second_permit = granted(&authority, &second);
        authority
            .connect()
            .unwrap()
            .execute("UPDATE hard_risk_orders SET lease_expires_at_ms = 0", [])
            .unwrap();

        let recovered = authority
            .recover_expired_submissions(HardRiskRecoveryReadiness::CompleteTradeSnapshot(
                HardRiskCompleteTradeSnapshot::new(first.request().scope().clone(), 88),
            ))
            .unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].key(), first.key());
        assert_eq!(
            authority.get(&second.key()).unwrap().unwrap().state(),
            &HardRiskOrderState::Submitting
        );
        assert_eq!(
            authority
                .unresolved_orders(&second_scope, 10)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn clock_regression_and_non_durable_paths_fail_closed() {
        let (authority, _database) = authority("clock-regression");
        authority
            .connect()
            .unwrap()
            .execute(
                "UPDATE hard_risk_clock SET max_observed_at_ms = ?1 WHERE singleton = 1",
                params![i64::MAX],
            )
            .unwrap();
        let error = authority.prepare(request("client-1", 1)).unwrap_err();
        assert!(matches!(error, HardRiskError::ClockRegression { .. }));

        for path in [":memory:", "file::memory:?cache=shared", ""] {
            let error = HardRiskAuthority::open(path).unwrap_err();
            assert!(matches!(error, HardRiskError::InvalidInput { .. }));
        }
    }

    #[test]
    fn version_one_schema_contract_requires_metadata_columns_and_indexes() {
        let (authority, database) = authority("schema-contract");
        authority
            .connect()
            .unwrap()
            .execute("DROP INDEX hard_risk_orders_recovery_idx", [])
            .unwrap();
        drop(authority);
        let error = HardRiskAuthority::open(database.path()).unwrap_err();
        assert!(matches!(error, HardRiskError::Schema { .. }));
    }
}
