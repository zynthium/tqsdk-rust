#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::HashMap;

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

#[derive(Debug, Clone, PartialEq)]
struct InstrumentRiskRule {
    price_tick: f64,
    volume_multiple: i64,
}

/// Stateless pre-trade risk gate for task-level order entrypoints.
#[derive(Debug, Clone, Default)]
pub struct RiskEngine {
    max_order_volume: Option<i64>,
    min_available: Option<f64>,
    max_abs_net_position: Option<i64>,
    max_abs_price_deviation: Option<f64>,
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
