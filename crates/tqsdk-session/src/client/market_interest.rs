use std::collections::BTreeMap;

use tqsdk_core::{MarketChartCommand, MarketCommand, RuntimeCommand, Symbol};

use super::SessionClient;

#[derive(Debug, Default)]
pub(super) struct MarketInterestRegistry {
    quote_counts: BTreeMap<Symbol, usize>,
    trading_status_counts: BTreeMap<Symbol, usize>,
    charts: BTreeMap<String, ChartInterest>,
}

#[derive(Debug, Clone)]
struct ChartInterest {
    command: MarketChartCommand,
    refs: usize,
}

/// Session-owned quote interest lease.
pub struct MarketQuoteLease {
    session: SessionClient,
    symbols: Vec<Symbol>,
    closed: bool,
}

/// Session-owned trading-status interest lease.
pub struct MarketTradingStatusLease {
    session: SessionClient,
    symbols: Vec<Symbol>,
    closed: bool,
}

/// Session-owned chart interest lease.
pub struct MarketChartLease {
    session: SessionClient,
    chart_id: String,
    closed: bool,
}

impl SessionClient {
    pub async fn ensure_quotes<I, S>(&self, symbols: I) -> crate::error::Result<MarketQuoteLease>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let symbols = collect_symbols(symbols)?;
        let mut interests = self.market_interests.lock().await;
        let subscribe = interests.acquire_quotes(&symbols);
        if !subscribe.is_empty()
            && let Err(error) = self
                .submit_market_command(MarketCommand::SubscribeQuotes { symbols: subscribe })
                .await
        {
            interests.release_quotes(&symbols);
            return Err(error);
        }

        Ok(MarketQuoteLease {
            session: self.clone(),
            symbols,
            closed: false,
        })
    }

    pub async fn ensure_trading_status<I, S>(
        &self,
        symbols: I,
    ) -> crate::error::Result<MarketTradingStatusLease>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let symbols = collect_symbols(symbols)?;
        let mut interests = self.market_interests.lock().await;
        let subscribe = interests.acquire_trading_status(&symbols);
        if !subscribe.is_empty()
            && let Err(error) = self
                .submit_market_command(MarketCommand::SubscribeTradingStatus { symbols: subscribe })
                .await
        {
            interests.release_trading_status(&symbols);
            return Err(error);
        }

        Ok(MarketTradingStatusLease {
            session: self.clone(),
            symbols,
            closed: false,
        })
    }

    pub async fn ensure_chart(
        &self,
        command: MarketChartCommand,
    ) -> crate::error::Result<MarketChartLease> {
        let chart_id = command.chart_id.clone();
        let mut interests = self.market_interests.lock().await;
        let should_submit = interests.acquire_chart(command)?;
        if should_submit
            && let Err(error) = self
                .submit_market_command(MarketCommand::SetChart(
                    interests
                        .chart_command(&chart_id)
                        .expect("chart command should exist after acquire")
                        .clone(),
                ))
                .await
        {
            interests.release_chart(&chart_id);
            return Err(error);
        }

        Ok(MarketChartLease {
            session: self.clone(),
            chart_id,
            closed: false,
        })
    }

    async fn release_quotes(&self, symbols: &[Symbol]) -> crate::error::Result<()> {
        let mut interests = self.market_interests.lock().await;
        let unsubscribe = interests.release_quotes(symbols);
        if !unsubscribe.is_empty() {
            self.submit_market_command(MarketCommand::UnsubscribeQuotes {
                symbols: unsubscribe,
            })
            .await?;
        }
        Ok(())
    }

    async fn release_trading_status(&self, symbols: &[Symbol]) -> crate::error::Result<()> {
        let mut interests = self.market_interests.lock().await;
        let unsubscribe = interests.release_trading_status(symbols);
        if !unsubscribe.is_empty() {
            self.submit_market_command(MarketCommand::UnsubscribeTradingStatus {
                symbols: unsubscribe,
            })
            .await?;
        }
        Ok(())
    }

    async fn release_chart(&self, chart_id: &str) -> crate::error::Result<()> {
        let mut interests = self.market_interests.lock().await;
        if interests.release_chart(chart_id) {
            self.submit_market_command(MarketCommand::CancelChart {
                chart_id: chart_id.to_string(),
            })
            .await?;
        }
        Ok(())
    }

    async fn submit_market_command(&self, command: MarketCommand) -> crate::error::Result<()> {
        self.submit(RuntimeCommand::Market(command)).await?;
        Ok(())
    }
}

impl MarketQuoteLease {
    pub async fn release_symbols<I, S>(&mut self, symbols: I) -> crate::error::Result<()>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        if self.closed {
            return Ok(());
        }

        let requested = symbols
            .into_iter()
            .map(|symbol| Symbol::new(symbol.as_ref()))
            .collect::<Vec<_>>();
        let mut release = Vec::new();
        self.symbols.retain(|held| {
            if requested.iter().any(|symbol| symbol == held) {
                release.push(held.clone());
                false
            } else {
                true
            }
        });
        if !release.is_empty() {
            self.session.release_quotes(&release).await?;
        }
        if self.symbols.is_empty() {
            self.closed = true;
        }
        Ok(())
    }

    pub async fn close(mut self) -> crate::error::Result<()> {
        if !self.closed {
            self.session.release_quotes(&self.symbols).await?;
            self.closed = true;
        }
        Ok(())
    }
}

impl MarketTradingStatusLease {
    pub async fn close(mut self) -> crate::error::Result<()> {
        if !self.closed {
            self.session.release_trading_status(&self.symbols).await?;
            self.closed = true;
        }
        Ok(())
    }
}

impl MarketChartLease {
    #[must_use]
    pub fn chart_id(&self) -> &str {
        &self.chart_id
    }

    pub async fn close(mut self) -> crate::error::Result<()> {
        if !self.closed {
            self.session.release_chart(&self.chart_id).await?;
            self.closed = true;
        }
        Ok(())
    }
}

impl MarketInterestRegistry {
    fn acquire_quotes(&mut self, symbols: &[Symbol]) -> Vec<Symbol> {
        acquire_symbols(&mut self.quote_counts, symbols)
    }

    fn release_quotes(&mut self, symbols: &[Symbol]) -> Vec<Symbol> {
        release_symbols(&mut self.quote_counts, symbols)
    }

    fn acquire_trading_status(&mut self, symbols: &[Symbol]) -> Vec<Symbol> {
        acquire_symbols(&mut self.trading_status_counts, symbols)
    }

    fn release_trading_status(&mut self, symbols: &[Symbol]) -> Vec<Symbol> {
        release_symbols(&mut self.trading_status_counts, symbols)
    }

    fn acquire_chart(&mut self, command: MarketChartCommand) -> crate::error::Result<bool> {
        if let Some(existing) = self.charts.get_mut(&command.chart_id) {
            if existing.command != command {
                return Err(crate::error::SessionFacadeError::InvalidState(
                    "chart interest already registered with different parameters",
                ));
            }
            existing.refs += 1;
            return Ok(false);
        }

        self.charts
            .insert(command.chart_id.clone(), ChartInterest { command, refs: 1 });
        Ok(true)
    }

    fn release_chart(&mut self, chart_id: &str) -> bool {
        let Some(existing) = self.charts.get_mut(chart_id) else {
            return false;
        };
        existing.refs = existing.refs.saturating_sub(1);
        if existing.refs > 0 {
            return false;
        }
        self.charts.remove(chart_id);
        true
    }

    fn chart_command(&self, chart_id: &str) -> Option<&MarketChartCommand> {
        self.charts.get(chart_id).map(|interest| &interest.command)
    }
}

fn collect_symbols<I, S>(symbols: I) -> crate::error::Result<Vec<Symbol>>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let symbols = symbols
        .into_iter()
        .map(|symbol| Symbol::new(symbol.as_ref()))
        .collect::<Vec<_>>();
    if symbols.is_empty() {
        return Err(crate::error::SessionFacadeError::InvalidState(
            "market interest requires at least one symbol",
        ));
    }
    Ok(symbols)
}

fn acquire_symbols(counts: &mut BTreeMap<Symbol, usize>, symbols: &[Symbol]) -> Vec<Symbol> {
    let mut changed = Vec::new();
    for symbol in symbols {
        let count = counts.entry(symbol.clone()).or_insert(0);
        if *count == 0 {
            changed.push(symbol.clone());
        }
        *count += 1;
    }
    changed
}

fn release_symbols(counts: &mut BTreeMap<Symbol, usize>, symbols: &[Symbol]) -> Vec<Symbol> {
    let mut changed = Vec::new();
    for symbol in symbols {
        let Some(count) = counts.get_mut(symbol) else {
            continue;
        };
        *count = count.saturating_sub(1);
        if *count == 0 {
            counts.remove(symbol);
            changed.push(symbol.clone());
        }
    }
    changed
}
