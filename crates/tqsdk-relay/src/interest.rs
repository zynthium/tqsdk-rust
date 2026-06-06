#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::{BTreeMap, BTreeSet};

use crate::protocol::SetChartCommand;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ClientId(u64);

impl ClientId {
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SourceKey {
    pub symbols: Vec<String>,
    pub duration_ns: i64,
    pub view_width: usize,
}

#[derive(Debug, Default)]
pub struct InterestRegistry {
    client_quotes: BTreeMap<ClientId, BTreeSet<String>>,
    chart_mappings: BTreeMap<(ClientId, String), SourceKey>,
}

impl InterestRegistry {
    pub fn set_quotes(&mut self, client_id: ClientId, symbols: Vec<String>) {
        self.client_quotes
            .insert(client_id, symbols.into_iter().collect());
    }

    pub fn set_chart(&mut self, client_id: ClientId, command: SetChartCommand) -> SourceKey {
        let mut symbols = command.symbols;
        symbols.sort();
        symbols.dedup();

        let source = SourceKey {
            symbols,
            duration_ns: command.duration_ns,
            view_width: command.view_width,
        };
        self.chart_mappings
            .insert((client_id, command.chart_id), source.clone());
        source
    }

    pub fn remove_client(&mut self, client_id: ClientId) {
        self.client_quotes.remove(&client_id);
        self.chart_mappings
            .retain(|(mapped_client, _), _| *mapped_client != client_id);
    }

    #[must_use]
    pub fn quote_interest_count(&self, symbol: &str) -> usize {
        self.client_quotes
            .values()
            .filter(|symbols| symbols.contains(symbol))
            .count()
    }

    #[must_use]
    pub fn chart_interest_count(&self, source: &SourceKey) -> usize {
        self.chart_mappings
            .values()
            .filter(|mapped_source| *mapped_source == source)
            .count()
    }

    #[must_use]
    pub fn downstream_chart_id(&self, client_id: ClientId, source: &SourceKey) -> Option<&str> {
        self.chart_mappings
            .iter()
            .find_map(|((mapped_client, chart_id), mapped_source)| {
                (*mapped_client == client_id && mapped_source == source)
                    .then_some(chart_id.as_str())
            })
    }

    #[must_use]
    pub fn quote_clients(&self, symbol: &str) -> Vec<ClientId> {
        self.client_quotes
            .iter()
            .filter_map(|(client_id, symbols)| symbols.contains(symbol).then_some(*client_id))
            .collect()
    }

    #[must_use]
    pub fn sources_for_symbol(&self, symbol: &str) -> Vec<SourceKey> {
        let mut sources: Vec<_> = self
            .chart_mappings
            .values()
            .filter(|source| source.symbols.iter().any(|candidate| candidate == symbol))
            .cloned()
            .collect();
        sources.sort();
        sources.dedup();
        sources
    }

    #[must_use]
    pub fn chart_clients(&self, source: &SourceKey) -> Vec<ClientId> {
        self.chart_mappings
            .iter()
            .filter_map(|((client_id, _), mapped_source)| {
                (mapped_source == source).then_some(*client_id)
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    #[must_use]
    pub fn client_count(&self) -> usize {
        let mut clients: BTreeSet<_> = self.client_quotes.keys().copied().collect();
        clients.extend(self.chart_mappings.keys().map(|(client_id, _)| *client_id));
        clients.len()
    }

    #[must_use]
    pub fn total_quote_subscriptions(&self) -> usize {
        self.client_quotes.values().map(BTreeSet::len).sum()
    }

    #[must_use]
    pub fn total_chart_subscriptions(&self) -> usize {
        self.chart_mappings.len()
    }
}
