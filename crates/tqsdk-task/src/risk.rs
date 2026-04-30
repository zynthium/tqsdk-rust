#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::time::{Duration, Instant};

use tqsdk_core::{AccountId, Revision, Symbol, TradeDirection, TradeOffset};

use crate::{Result, TaskError, TaskOrderIntent};

/// Revision-bound result of a pre-trade risk check.
#[derive(Debug, Clone, PartialEq)]
pub struct RiskCheckReport {
    revision: Revision,
    decision: RiskDecision,
}

impl RiskCheckReport {
    #[must_use]
    pub fn revision(&self) -> Revision {
        self.revision
    }

    #[must_use]
    pub fn decision(&self) -> &RiskDecision {
        &self.decision
    }

    #[must_use]
    pub fn into_decision(self) -> RiskDecision {
        self.decision
    }
}

/// Revision-bound lightweight what-if projection for one task order intent.
#[derive(Debug, Clone, PartialEq)]
pub struct RiskProjectionReport {
    revision: Revision,
    account_id: String,
    symbol: String,
    current_net: Option<i64>,
    projected_net: Option<i64>,
    price_basis: Option<f64>,
    estimated_price_volume: Option<f64>,
    contract_multiplier: Option<i64>,
    estimated_notional: Option<f64>,
}

impl RiskProjectionReport {
    #[must_use]
    pub fn revision(&self) -> Revision {
        self.revision
    }

    #[must_use]
    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    #[must_use]
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    #[must_use]
    pub fn current_net(&self) -> Option<i64> {
        self.current_net
    }

    #[must_use]
    pub fn projected_net(&self) -> Option<i64> {
        self.projected_net
    }

    #[must_use]
    pub fn price_basis(&self) -> Option<f64> {
        self.price_basis
    }

    #[must_use]
    pub fn estimated_price_volume(&self) -> Option<f64> {
        self.estimated_price_volume
    }

    #[must_use]
    pub fn contract_multiplier(&self) -> Option<i64> {
        self.contract_multiplier
    }

    #[must_use]
    pub fn estimated_notional(&self) -> Option<f64> {
        self.estimated_notional
    }
}

/// Typed result of a pre-trade risk check.
#[derive(Debug, Clone, PartialEq)]
pub enum RiskDecision {
    Accepted,
    Rejected(RiskRejection),
}

impl RiskDecision {
    #[must_use]
    pub fn is_accepted(&self) -> bool {
        matches!(self, Self::Accepted)
    }

    #[must_use]
    pub fn rejection(&self) -> Option<&RiskRejection> {
        match self {
            Self::Accepted => None,
            Self::Rejected(rejection) => Some(rejection),
        }
    }
}

/// Typed rejection reason returned by [`RiskEngine`].
#[derive(Debug, Clone, PartialEq)]
pub enum RiskRejection {
    MaxOrderVolumeExceeded {
        account_id: String,
        symbol: String,
        requested: i64,
        max: i64,
    },
    DailyOpenCountLimitExceeded {
        account_id: String,
        symbol: String,
        current: i64,
        requested: i64,
        max: i64,
    },
    DailyOpenVolumeLimitExceeded {
        account_id: String,
        symbol: String,
        current: i64,
        requested: i64,
        max: i64,
    },
    AccumulatedOpenVolumeLimitExceeded {
        account_id: String,
        symbols: Vec<String>,
        current: i64,
        requested: i64,
        max: i64,
    },
    OrderRateLimitExceeded {
        account_id: String,
        exchange_id: String,
        current: i64,
        requested: i64,
        max: i64,
    },
    MissingAccount {
        account_id: String,
    },
    AvailableBelowMinimum {
        account_id: String,
        available: f64,
        min_available: f64,
    },
    MissingPosition {
        account_id: String,
        symbol: String,
    },
    NetPositionLimitExceeded {
        account_id: String,
        symbol: String,
        current_net: i64,
        projected_net: i64,
        max_abs_net: i64,
    },
    MissingQuote {
        symbol: String,
    },
    PriceDeviationExceeded {
        symbol: String,
        limit_price: f64,
        reference_price: f64,
        max_abs_deviation: f64,
    },
    PriceNotOnTick {
        symbol: String,
        limit_price: f64,
        price_tick: f64,
    },
}

impl Display for RiskRejection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MaxOrderVolumeExceeded {
                account_id,
                symbol,
                requested,
                max,
            } => write!(
                f,
                "max order volume exceeded account={account_id} symbol={symbol} requested={requested} max={max}"
            ),
            Self::DailyOpenCountLimitExceeded {
                account_id,
                symbol,
                current,
                requested,
                max,
            } => write!(
                f,
                "daily open count limit exceeded account={account_id} symbol={symbol} current={current} requested={requested} max={max}"
            ),
            Self::DailyOpenVolumeLimitExceeded {
                account_id,
                symbol,
                current,
                requested,
                max,
            } => write!(
                f,
                "daily open volume limit exceeded account={account_id} symbol={symbol} current={current} requested={requested} max={max}"
            ),
            Self::AccumulatedOpenVolumeLimitExceeded {
                account_id,
                symbols,
                current,
                requested,
                max,
            } => write!(
                f,
                "accumulated open volume limit exceeded account={account_id} symbols={} current={current} requested={requested} max={max}",
                symbols.join(",")
            ),
            Self::OrderRateLimitExceeded {
                account_id,
                exchange_id,
                current,
                requested,
                max,
            } => write!(
                f,
                "order rate limit exceeded account={account_id} exchange_id={exchange_id} current={current} requested={requested} max={max}"
            ),
            Self::MissingAccount { account_id } => {
                write!(f, "missing account account={account_id}")
            }
            Self::AvailableBelowMinimum {
                account_id,
                available,
                min_available,
            } => write!(
                f,
                "available balance below minimum account={account_id} available={available} min_available={min_available}"
            ),
            Self::MissingPosition { account_id, symbol } => {
                write!(f, "missing position account={account_id} symbol={symbol}")
            }
            Self::NetPositionLimitExceeded {
                account_id,
                symbol,
                current_net,
                projected_net,
                max_abs_net,
            } => write!(
                f,
                "net position limit exceeded account={account_id} symbol={symbol} current_net={current_net} projected_net={projected_net} max_abs_net={max_abs_net}"
            ),
            Self::MissingQuote { symbol } => write!(f, "missing quote for symbol={symbol}"),
            Self::PriceDeviationExceeded {
                symbol,
                limit_price,
                reference_price,
                max_abs_deviation,
            } => write!(
                f,
                "price deviation exceeded symbol={symbol} limit_price={limit_price} reference_price={reference_price} max_abs_deviation={max_abs_deviation}"
            ),
            Self::PriceNotOnTick {
                symbol,
                limit_price,
                price_tick,
            } => write!(
                f,
                "price not on tick symbol={symbol} limit_price={limit_price} price_tick={price_tick}"
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct InstrumentRiskRule {
    price_tick: f64,
    volume_multiple: i64,
}

#[derive(Debug, Clone, PartialEq)]
struct StringListRiskRule {
    max: i64,
    values: Vec<String>,
}

impl StringListRiskRule {
    fn applies_to(&self, value: &str) -> bool {
        self.values.iter().any(|candidate| candidate == value)
    }
}

#[derive(Debug, Clone, Default)]
struct RiskDailyUsage {
    open_counts: HashMap<(String, String), i64>,
    open_volumes: HashMap<(String, String), i64>,
    accumulated_open_volumes: HashMap<(String, usize), i64>,
    order_operation_timestamps: HashMap<(String, String), Vec<Instant>>,
}

const ORDER_RATE_WINDOW: Duration = Duration::from_secs(1);

/// Pre-trade risk gate for task-level order entrypoints.
///
/// Snapshot checks are revision-bound. Daily open counters are process-local
/// usage limits recorded by [`TaskHost`](crate::TaskHost) after a submitted
/// order request, matching the core SDK rule shape without adding durable
/// audit or cross-process risk management.
#[derive(Debug, Clone, Default)]
pub struct RiskEngine {
    max_order_volume: Option<i64>,
    min_available: Option<f64>,
    max_abs_net_position: Option<i64>,
    max_abs_price_deviation: Option<f64>,
    daily_open_count_limits: Vec<StringListRiskRule>,
    daily_open_volume_limits: Vec<StringListRiskRule>,
    accumulated_open_volume_limits: Vec<StringListRiskRule>,
    order_rate_limits: Vec<StringListRiskRule>,
    daily_usage: RiskDailyUsage,
    instrument_rules: HashMap<String, InstrumentRiskRule>,
}

impl RiskEngine {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn max_order_volume(mut self, max: i64) -> Self {
        self.max_order_volume = Some(max);
        self
    }

    #[must_use]
    pub fn daily_open_count_limit<I, S>(mut self, max: i64, symbols: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.daily_open_count_limits.push(StringListRiskRule {
            max,
            values: collect_strings(symbols),
        });
        self
    }

    #[must_use]
    pub fn daily_open_volume_limit<I, S>(mut self, max: i64, symbols: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.daily_open_volume_limits.push(StringListRiskRule {
            max,
            values: collect_strings(symbols),
        });
        self
    }

    #[must_use]
    pub fn accumulated_open_volume_limit<I, S>(mut self, max: i64, symbols: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.accumulated_open_volume_limits
            .push(StringListRiskRule {
                max,
                values: collect_strings(symbols),
            });
        self
    }

    #[must_use]
    pub fn order_rate_limit_per_second<I, S>(mut self, max: i64, exchanges: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.order_rate_limits.push(StringListRiskRule {
            max,
            values: collect_strings(exchanges),
        });
        self
    }

    pub fn reset_daily_usage(&mut self) {
        self.daily_usage = RiskDailyUsage::default();
    }

    pub fn record_accepted_order(&mut self, intent: &TaskOrderIntent) -> Result<()> {
        self.validate_usage_limits()?;
        self.record_order_operation_for_symbol(&intent.account_id, &intent.symbol)?;
        if !is_open_intent(intent) {
            return Ok(());
        }

        let account_symbol_key = (intent.account_id.clone(), intent.symbol.clone());
        if self
            .daily_open_count_limits
            .iter()
            .any(|rule| rule.applies_to(&intent.symbol))
        {
            increment_usage(
                &mut self.daily_usage.open_counts,
                account_symbol_key.clone(),
                1,
            );
        }
        if self
            .daily_open_volume_limits
            .iter()
            .any(|rule| rule.applies_to(&intent.symbol))
        {
            increment_usage(
                &mut self.daily_usage.open_volumes,
                account_symbol_key,
                intent.volume,
            );
        }
        for (index, rule) in self.accumulated_open_volume_limits.iter().enumerate() {
            if rule.applies_to(&intent.symbol) {
                increment_usage(
                    &mut self.daily_usage.accumulated_open_volumes,
                    (intent.account_id.clone(), index),
                    intent.volume,
                );
            }
        }
        Ok(())
    }

    pub(crate) fn check_order_operation(
        &self,
        account_id: &str,
        exchange_id: &str,
    ) -> Result<RiskDecision> {
        let now = Instant::now();
        Ok(
            match self.order_rate_rejection(account_id, exchange_id, now)? {
                Some(rejection) => RiskDecision::Rejected(rejection),
                None => RiskDecision::Accepted,
            },
        )
    }

    pub(crate) fn record_order_operation(
        &mut self,
        account_id: &str,
        exchange_id: &str,
    ) -> Result<()> {
        self.record_order_operation_at(account_id, exchange_id, Instant::now())
    }

    #[must_use]
    pub fn min_available(mut self, min_available: f64) -> Self {
        self.min_available = Some(min_available);
        self
    }

    #[must_use]
    pub fn max_net_position(mut self, max_abs_net: i64) -> Self {
        self.max_abs_net_position = Some(max_abs_net);
        self
    }

    #[must_use]
    pub fn max_price_deviation(mut self, max_abs_deviation: f64) -> Self {
        self.max_abs_price_deviation = Some(max_abs_deviation);
        self
    }

    #[must_use]
    pub fn instrument_specs<I>(mut self, specs: I) -> Self
    where
        I: IntoIterator<Item = tqsdk_session::InstrumentSpec>,
    {
        for spec in specs {
            self.instrument_rules.insert(
                spec.symbol.as_str().to_string(),
                InstrumentRiskRule {
                    price_tick: spec.price_tick,
                    volume_multiple: spec.volume_multiple,
                },
            );
        }
        self
    }

    pub fn check(&self, api: &tqsdk_wait::TqApi, intent: &TaskOrderIntent) -> Result<RiskDecision> {
        Ok(self.check_report(api, intent)?.into_decision())
    }

    pub fn project_order(
        &self,
        api: &tqsdk_wait::TqApi,
        intent: &TaskOrderIntent,
    ) -> Result<RiskProjectionReport> {
        let snapshot = api.session().reader().read();
        let revision = snapshot.revision();
        let view = snapshot.view();
        let trade = view.trade_state();
        let market = view.market_state();
        let account_id = AccountId::new(intent.account_id.clone());
        let symbol = Symbol::new(intent.symbol.clone());

        let current_net = trade
            .position(&account_id, &symbol)?
            .map(|position| position.volume_long - position.volume_short);
        let projected_net = current_net.map(|current| project_net_position(current, intent));
        let price_basis = intent.limit_price.or_else(|| {
            market
                .quote(&symbol)
                .ok()
                .flatten()
                .and_then(|quote| quote.last_price.is_finite().then_some(quote.last_price))
        });
        let estimated_price_volume = price_basis.map(|price| price * intent.volume as f64);
        let contract_multiplier = self
            .instrument_rules
            .get(&intent.symbol)
            .map(|rule| {
                validate_instrument_rule(rule)?;
                Ok::<i64, TaskError>(rule.volume_multiple)
            })
            .transpose()?;
        let estimated_notional = price_basis
            .zip(contract_multiplier)
            .map(|(price, multiplier)| price * intent.volume as f64 * multiplier as f64);

        Ok(RiskProjectionReport {
            revision,
            account_id: intent.account_id.clone(),
            symbol: intent.symbol.clone(),
            current_net,
            projected_net,
            price_basis,
            estimated_price_volume,
            contract_multiplier,
            estimated_notional,
        })
    }

    pub fn check_report(
        &self,
        api: &tqsdk_wait::TqApi,
        intent: &TaskOrderIntent,
    ) -> Result<RiskCheckReport> {
        let snapshot = api.session().reader().read();
        let revision = snapshot.revision();
        let view = snapshot.view();
        let trade = view.trade_state();
        let market = view.market_state();
        let account_id = AccountId::new(intent.account_id.clone());
        let symbol = Symbol::new(intent.symbol.clone());

        if let Some(max) = self.max_order_volume {
            if intent.volume > max {
                return Ok(RiskCheckReport {
                    revision,
                    decision: RiskDecision::Rejected(RiskRejection::MaxOrderVolumeExceeded {
                        account_id: intent.account_id.clone(),
                        symbol: intent.symbol.clone(),
                        requested: intent.volume,
                        max,
                    }),
                });
            }
        }

        if !self.order_rate_limits.is_empty() {
            self.validate_usage_limits()?;
            let exchange_id = exchange_id_from_symbol(&intent.symbol).ok_or(
                TaskError::InvalidState("risk order rate requires exchange-prefixed symbol"),
            )?;
            if let Some(rejection) =
                self.order_rate_rejection(&intent.account_id, exchange_id, Instant::now())?
            {
                return Ok(RiskCheckReport {
                    revision,
                    decision: RiskDecision::Rejected(rejection),
                });
            }
        }

        if is_open_intent(intent) {
            self.validate_usage_limits()?;
            for rule in &self.daily_open_count_limits {
                if !rule.applies_to(&intent.symbol) {
                    continue;
                }
                let current = self
                    .daily_usage
                    .open_counts
                    .get(&(intent.account_id.clone(), intent.symbol.clone()))
                    .copied()
                    .unwrap_or(0);
                let requested = 1;
                if current.saturating_add(requested) > rule.max {
                    return Ok(RiskCheckReport {
                        revision,
                        decision: RiskDecision::Rejected(
                            RiskRejection::DailyOpenCountLimitExceeded {
                                account_id: intent.account_id.clone(),
                                symbol: intent.symbol.clone(),
                                current,
                                requested,
                                max: rule.max,
                            },
                        ),
                    });
                }
            }

            for rule in &self.daily_open_volume_limits {
                if !rule.applies_to(&intent.symbol) {
                    continue;
                }
                let current = self
                    .daily_usage
                    .open_volumes
                    .get(&(intent.account_id.clone(), intent.symbol.clone()))
                    .copied()
                    .unwrap_or(0);
                let requested = intent.volume;
                if current.saturating_add(requested) > rule.max {
                    return Ok(RiskCheckReport {
                        revision,
                        decision: RiskDecision::Rejected(
                            RiskRejection::DailyOpenVolumeLimitExceeded {
                                account_id: intent.account_id.clone(),
                                symbol: intent.symbol.clone(),
                                current,
                                requested,
                                max: rule.max,
                            },
                        ),
                    });
                }
            }

            for (index, rule) in self.accumulated_open_volume_limits.iter().enumerate() {
                if !rule.applies_to(&intent.symbol) {
                    continue;
                }
                let current = self
                    .daily_usage
                    .accumulated_open_volumes
                    .get(&(intent.account_id.clone(), index))
                    .copied()
                    .unwrap_or(0);
                let requested = intent.volume;
                if current.saturating_add(requested) > rule.max {
                    return Ok(RiskCheckReport {
                        revision,
                        decision: RiskDecision::Rejected(
                            RiskRejection::AccumulatedOpenVolumeLimitExceeded {
                                account_id: intent.account_id.clone(),
                                symbols: rule.values.clone(),
                                current,
                                requested,
                                max: rule.max,
                            },
                        ),
                    });
                }
            }
        }

        if let Some(min_available) = self.min_available {
            if !min_available.is_finite() {
                return Err(TaskError::InvalidState("risk min available must be finite"));
            }
            let account = trade.account(&account_id)?;
            let Some(account) = account else {
                return Ok(RiskCheckReport {
                    revision,
                    decision: RiskDecision::Rejected(RiskRejection::MissingAccount {
                        account_id: intent.account_id.clone(),
                    }),
                });
            };
            if !account.available.is_finite() {
                return Err(TaskError::InvalidState("account available must be finite"));
            }
            if account.available < min_available {
                return Ok(RiskCheckReport {
                    revision,
                    decision: RiskDecision::Rejected(RiskRejection::AvailableBelowMinimum {
                        account_id: intent.account_id.clone(),
                        available: account.available,
                        min_available,
                    }),
                });
            }
        }

        if let Some(max_abs_net) = self.max_abs_net_position {
            let position = trade.position(&account_id, &symbol)?;
            let Some(position) = position else {
                return Ok(RiskCheckReport {
                    revision,
                    decision: RiskDecision::Rejected(RiskRejection::MissingPosition {
                        account_id: intent.account_id.clone(),
                        symbol: intent.symbol.clone(),
                    }),
                });
            };
            let current_net = position.volume_long - position.volume_short;
            let projected_net = project_net_position(current_net, intent);
            let projected_abs = projected_net.checked_abs().unwrap_or(i64::MAX);
            if projected_abs > max_abs_net {
                return Ok(RiskCheckReport {
                    revision,
                    decision: RiskDecision::Rejected(RiskRejection::NetPositionLimitExceeded {
                        account_id: intent.account_id.clone(),
                        symbol: intent.symbol.clone(),
                        current_net,
                        projected_net,
                        max_abs_net,
                    }),
                });
            }
        }

        if let Some(limit_price) = intent.limit_price {
            if !limit_price.is_finite() {
                return Err(TaskError::InvalidState("limit price must be finite"));
            }
            if let Some(rule) = self.instrument_rules.get(&intent.symbol) {
                validate_instrument_rule(rule)?;
                if !price_is_on_tick(limit_price, rule.price_tick) {
                    return Ok(RiskCheckReport {
                        revision,
                        decision: RiskDecision::Rejected(RiskRejection::PriceNotOnTick {
                            symbol: intent.symbol.clone(),
                            limit_price,
                            price_tick: rule.price_tick,
                        }),
                    });
                }
            }
        }

        if let Some(max_abs_deviation) = self.max_abs_price_deviation {
            if !max_abs_deviation.is_finite() || max_abs_deviation < 0.0 {
                return Err(TaskError::InvalidState(
                    "risk max price deviation must be a non-negative finite value",
                ));
            }
            let Some(limit_price) = intent.limit_price else {
                return Ok(RiskCheckReport {
                    revision,
                    decision: RiskDecision::Accepted,
                });
            };
            if !limit_price.is_finite() {
                return Err(TaskError::InvalidState("limit price must be finite"));
            }
            let quote = market.quote(&symbol)?;
            let Some(quote) = quote else {
                return Ok(RiskCheckReport {
                    revision,
                    decision: RiskDecision::Rejected(RiskRejection::MissingQuote {
                        symbol: intent.symbol.clone(),
                    }),
                });
            };
            if !quote.last_price.is_finite() {
                return Ok(RiskCheckReport {
                    revision,
                    decision: RiskDecision::Rejected(RiskRejection::MissingQuote {
                        symbol: intent.symbol.clone(),
                    }),
                });
            }
            let deviation = (limit_price - quote.last_price).abs();
            if deviation > max_abs_deviation {
                return Ok(RiskCheckReport {
                    revision,
                    decision: RiskDecision::Rejected(RiskRejection::PriceDeviationExceeded {
                        symbol: intent.symbol.clone(),
                        limit_price,
                        reference_price: quote.last_price,
                        max_abs_deviation,
                    }),
                });
            }
        }

        Ok(RiskCheckReport {
            revision,
            decision: RiskDecision::Accepted,
        })
    }

    fn validate_usage_limits(&self) -> Result<()> {
        for rule in self
            .daily_open_count_limits
            .iter()
            .chain(self.daily_open_volume_limits.iter())
            .chain(self.accumulated_open_volume_limits.iter())
        {
            if rule.max < 0 {
                return Err(TaskError::InvalidState(
                    "risk daily open limit must be non-negative",
                ));
            }
        }
        for rule in &self.order_rate_limits {
            if rule.max <= 0 {
                return Err(TaskError::InvalidState(
                    "risk order rate limit must be positive",
                ));
            }
        }
        Ok(())
    }

    fn order_rate_rejection(
        &self,
        account_id: &str,
        exchange_id: &str,
        now: Instant,
    ) -> Result<Option<RiskRejection>> {
        self.validate_usage_limits()?;
        for rule in &self.order_rate_limits {
            if !rule.applies_to(exchange_id) {
                continue;
            }
            let current = self.current_order_operation_count(account_id, exchange_id, now);
            let requested = 1;
            if current.saturating_add(requested) > rule.max {
                return Ok(Some(RiskRejection::OrderRateLimitExceeded {
                    account_id: account_id.to_owned(),
                    exchange_id: exchange_id.to_owned(),
                    current,
                    requested,
                    max: rule.max,
                }));
            }
        }
        Ok(None)
    }

    fn current_order_operation_count(
        &self,
        account_id: &str,
        exchange_id: &str,
        now: Instant,
    ) -> i64 {
        self.daily_usage
            .order_operation_timestamps
            .get(&(account_id.to_owned(), exchange_id.to_owned()))
            .map(|timestamps| {
                timestamps
                    .iter()
                    .filter(|timestamp| {
                        now.saturating_duration_since(**timestamp) < ORDER_RATE_WINDOW
                    })
                    .count() as i64
            })
            .unwrap_or(0)
    }

    fn record_order_operation_for_symbol(&mut self, account_id: &str, symbol: &str) -> Result<()> {
        if self.order_rate_limits.is_empty() {
            return Ok(());
        }
        let exchange_id = exchange_id_from_symbol(symbol).ok_or(TaskError::InvalidState(
            "risk order rate requires exchange-prefixed symbol",
        ))?;
        self.record_order_operation(account_id, exchange_id)
    }

    fn record_order_operation_at(
        &mut self,
        account_id: &str,
        exchange_id: &str,
        now: Instant,
    ) -> Result<()> {
        self.validate_usage_limits()?;
        if !self
            .order_rate_limits
            .iter()
            .any(|rule| rule.applies_to(exchange_id))
        {
            return Ok(());
        }
        let timestamps = self
            .daily_usage
            .order_operation_timestamps
            .entry((account_id.to_owned(), exchange_id.to_owned()))
            .or_default();
        timestamps
            .retain(|timestamp| now.saturating_duration_since(*timestamp) < ORDER_RATE_WINDOW);
        timestamps.push(now);
        Ok(())
    }
}

fn collect_strings<I, S>(values: I) -> Vec<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    values
        .into_iter()
        .map(|value| value.as_ref().to_owned())
        .collect()
}

fn increment_usage<K>(usage: &mut HashMap<K, i64>, key: K, delta: i64)
where
    K: std::hash::Hash + Eq,
{
    usage
        .entry(key)
        .and_modify(|current| *current = current.saturating_add(delta))
        .or_insert(delta);
}

fn validate_instrument_rule(rule: &InstrumentRiskRule) -> Result<()> {
    if !rule.price_tick.is_finite() || rule.price_tick <= 0.0 {
        return Err(TaskError::InvalidState(
            "instrument risk price_tick must be positive",
        ));
    }
    if rule.volume_multiple <= 0 {
        return Err(TaskError::InvalidState(
            "instrument risk volume_multiple must be positive",
        ));
    }
    Ok(())
}

fn price_is_on_tick(price: f64, price_tick: f64) -> bool {
    if !price.is_finite() || !price_tick.is_finite() || price_tick <= 0.0 {
        return false;
    }
    let ticks = (price / price_tick).round();
    (price - ticks * price_tick).abs() <= price_tick * 1e-9
}

fn is_open_intent(intent: &TaskOrderIntent) -> bool {
    matches!(intent.offset, Some(TradeOffset::Open))
}

fn exchange_id_from_symbol(symbol: &str) -> Option<&str> {
    symbol
        .split_once('.')
        .map(|(exchange_id, _)| exchange_id)
        .filter(|exchange_id| !exchange_id.is_empty())
}

fn project_net_position(current_net: i64, intent: &TaskOrderIntent) -> i64 {
    match (intent.direction, intent.offset) {
        (
            TradeDirection::Buy,
            Some(TradeOffset::Open | TradeOffset::Close | TradeOffset::CloseToday),
        ) => current_net.saturating_add(intent.volume),
        (
            TradeDirection::Sell,
            Some(TradeOffset::Open | TradeOffset::Close | TradeOffset::CloseToday),
        ) => current_net.saturating_sub(intent.volume),
        _ => current_net,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn intent(
        direction: TradeDirection,
        offset: Option<TradeOffset>,
        volume: i64,
    ) -> TaskOrderIntent {
        TaskOrderIntent {
            account_id: "sim".to_owned(),
            symbol: "SHFE.rb2601".to_owned(),
            direction,
            offset,
            volume,
            limit_price: Some(3600.0),
        }
    }

    #[test]
    fn price_tick_check_accepts_exact_multiples_across_common_tick_sizes() {
        for price_tick in [0.01, 0.2, 0.5, 1.0, 2.5] {
            for multiple in -10_000..=10_000 {
                let price = multiple as f64 * price_tick;
                assert!(
                    price_is_on_tick(price, price_tick),
                    "price={price} should be accepted for tick={price_tick}"
                );
            }
        }
    }

    #[test]
    fn price_tick_check_rejects_half_tick_offsets_across_common_tick_sizes() {
        for price_tick in [0.01, 0.2, 0.5, 1.0, 2.5] {
            for multiple in -1_000..=1_000 {
                let price = (multiple as f64 * price_tick) + (price_tick / 2.0);
                assert!(
                    !price_is_on_tick(price, price_tick),
                    "price={price} should be rejected for tick={price_tick}"
                );
            }
        }
    }

    #[test]
    fn price_tick_check_rejects_non_finite_prices_and_invalid_ticks() {
        for price in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(!price_is_on_tick(price, 0.2));
        }
        for price_tick in [f64::NAN, f64::INFINITY, 0.0, -0.2] {
            assert!(!price_is_on_tick(3600.0, price_tick));
        }
    }

    #[test]
    fn net_position_projection_matches_directional_signed_volume_properties() {
        for current_net in -1_000..=1_000 {
            for volume in 1..=100 {
                let buy_open = intent(TradeDirection::Buy, Some(TradeOffset::Open), volume);
                let buy_close = intent(TradeDirection::Buy, Some(TradeOffset::Close), volume);
                let sell_open = intent(TradeDirection::Sell, Some(TradeOffset::Open), volume);
                let sell_close = intent(TradeDirection::Sell, Some(TradeOffset::Close), volume);

                assert_eq!(
                    project_net_position(current_net, &buy_open),
                    current_net.saturating_add(volume)
                );
                assert_eq!(
                    project_net_position(current_net, &buy_close),
                    current_net.saturating_add(volume)
                );
                assert_eq!(
                    project_net_position(current_net, &sell_open),
                    current_net.saturating_sub(volume)
                );
                assert_eq!(
                    project_net_position(current_net, &sell_close),
                    current_net.saturating_sub(volume)
                );
            }
        }
    }

    #[test]
    fn net_position_projection_leaves_unspecified_offsets_unchanged() {
        let order_without_offset = intent(TradeDirection::Buy, None, 5);

        for current_net in -1_000..=1_000 {
            assert_eq!(
                project_net_position(current_net, &order_without_offset),
                current_net
            );
        }
    }
}
