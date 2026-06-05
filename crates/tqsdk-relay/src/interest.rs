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
}
