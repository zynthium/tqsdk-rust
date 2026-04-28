#![cfg_attr(not(test), forbid(unsafe_code))]

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
}

/// Stateless pre-trade risk gate for task-level order entrypoints.
#[derive(Debug, Clone, Default)]
pub struct RiskEngine {
    max_order_volume: Option<i64>,
    min_available: Option<f64>,
    max_abs_net_position: Option<i64>,
    max_abs_price_deviation: Option<f64>,
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

    pub fn check(&self, api: &tqsdk_wait::TqApi, intent: &TaskOrderIntent) -> Result<RiskDecision> {
        Ok(self.check_report(api, intent)?.into_decision())
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
