#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::{BTreeMap, BTreeSet};

use crate::protocol::SetChartCommand;
use crate::symbol_metrics::SymbolSubscriptionCounts;

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
    quote_clients_by_symbol: BTreeMap<String, BTreeSet<ClientId>>,
    sources_by_symbol: BTreeMap<String, BTreeSet<SourceKey>>,
    chart_clients_by_source: BTreeMap<SourceKey, BTreeSet<ClientId>>,
    chart_ids_by_client_source: BTreeMap<(ClientId, SourceKey), BTreeSet<String>>,
}

impl InterestRegistry {
    pub fn set_quotes(&mut self, client_id: ClientId, symbols: Vec<String>) {
        if let Some(previous) = self.client_quotes.remove(&client_id) {
            for symbol in previous {
                remove_from_index_set(&mut self.quote_clients_by_symbol, &symbol, &client_id);
            }
        }

        let symbols = symbols.into_iter().collect::<BTreeSet<_>>();
        for symbol in &symbols {
            self.quote_clients_by_symbol
                .entry(symbol.clone())
                .or_default()
                .insert(client_id);
        }
        self.client_quotes.insert(client_id, symbols);
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
        let chart_id = command.chart_id;
        if let Some(previous) = self.chart_mappings.remove(&(client_id, chart_id.clone())) {
            self.remove_chart_index(client_id, &chart_id, &previous);
        }

        self.add_chart_index(client_id, &chart_id, &source);
        self.chart_mappings
            .insert((client_id, chart_id), source.clone());
        source
    }

    pub fn remove_client(&mut self, client_id: ClientId) {
        if let Some(symbols) = self.client_quotes.remove(&client_id) {
            for symbol in symbols {
                remove_from_index_set(&mut self.quote_clients_by_symbol, &symbol, &client_id);
            }
        }

        let removed_charts = self
            .chart_mappings
            .iter()
            .filter(|((mapped_client, _), _)| *mapped_client == client_id)
            .map(|((_, chart_id), source)| (chart_id.clone(), source.clone()))
            .collect::<Vec<_>>();
        for (chart_id, source) in removed_charts {
            self.chart_mappings.remove(&(client_id, chart_id.clone()));
            self.remove_chart_index(client_id, &chart_id, &source);
        }
    }

    #[must_use]
    pub fn quote_interest_count(&self, symbol: &str) -> usize {
        self.quote_clients_by_symbol
            .get(symbol)
            .map_or(0, BTreeSet::len)
    }

    #[must_use]
    pub fn chart_interest_count(&self, source: &SourceKey) -> usize {
        self.chart_ids_by_client_source
            .iter()
            .filter(|((_, mapped_source), _)| mapped_source == source)
            .map(|(_, chart_ids)| chart_ids.len())
            .sum()
    }

    #[must_use]
    pub fn downstream_chart_id(&self, client_id: ClientId, source: &SourceKey) -> Option<&str> {
        self.chart_ids_by_client_source
            .get(&(client_id, source.clone()))
            .and_then(|chart_ids| chart_ids.iter().next().map(String::as_str))
    }

    #[must_use]
    pub fn quote_clients(&self, symbol: &str) -> Vec<ClientId> {
        self.quote_clients_by_symbol
            .get(symbol)
            .map(|clients| clients.iter().copied().collect())
            .unwrap_or_default()
    }

    #[must_use]
    pub fn sources_for_symbol(&self, symbol: &str) -> Vec<SourceKey> {
        self.sources_by_symbol
            .get(symbol)
            .map(|sources| sources.iter().cloned().collect())
            .unwrap_or_default()
    }

    #[must_use]
    pub fn chart_clients(&self, source: &SourceKey) -> Vec<ClientId> {
        self.chart_clients_by_source
            .get(source)
            .map(|clients| clients.iter().copied().collect())
            .unwrap_or_default()
    }

    #[must_use]
    pub fn chart_clients_with_ids(&self, source: &SourceKey) -> Vec<(ClientId, String)> {
        let Some(clients) = self.chart_clients_by_source.get(source) else {
            return Vec::new();
        };
        clients
            .iter()
            .filter_map(|client_id| {
                let chart_id = self
                    .chart_ids_by_client_source
                    .get(&(*client_id, source.clone()))?
                    .iter()
                    .next()?
                    .clone();
                Some((*client_id, chart_id))
            })
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

    #[must_use]
    pub fn symbol_subscription_counts(&self) -> BTreeMap<String, SymbolSubscriptionCounts> {
        let mut counts = BTreeMap::new();
        for symbols in self.client_quotes.values() {
            for symbol in symbols {
                counts
                    .entry(symbol.clone())
                    .or_insert_with(SymbolSubscriptionCounts::default)
                    .quote_subscriber_count += 1;
            }
        }
        for source in self.chart_mappings.values() {
            for symbol in &source.symbols {
                counts
                    .entry(symbol.clone())
                    .or_insert_with(SymbolSubscriptionCounts::default)
                    .chart_subscriber_count += 1;
            }
        }
        counts
    }

    #[must_use]
    pub fn subscribed_symbols(&self) -> BTreeSet<String> {
        let mut symbols = BTreeSet::new();
        for quoted in self.client_quotes.values() {
            symbols.extend(quoted.iter().cloned());
        }
        for source in self.chart_mappings.values() {
            symbols.extend(source.symbols.iter().cloned());
        }
        symbols
    }

    fn add_chart_index(&mut self, client_id: ClientId, chart_id: &str, source: &SourceKey) {
        self.chart_clients_by_source
            .entry(source.clone())
            .or_default()
            .insert(client_id);
        self.chart_ids_by_client_source
            .entry((client_id, source.clone()))
            .or_default()
            .insert(chart_id.to_string());
        for symbol in &source.symbols {
            self.sources_by_symbol
                .entry(symbol.clone())
                .or_default()
                .insert(source.clone());
        }
    }

    fn remove_chart_index(&mut self, client_id: ClientId, chart_id: &str, source: &SourceKey) {
        let key = (client_id, source.clone());
        let mut remove_client_from_source = false;
        if let Some(chart_ids) = self.chart_ids_by_client_source.get_mut(&key) {
            chart_ids.remove(chart_id);
            remove_client_from_source = chart_ids.is_empty();
        }
        if remove_client_from_source {
            self.chart_ids_by_client_source.remove(&key);
            remove_from_index_set(&mut self.chart_clients_by_source, source, &client_id);
        }

        if !self.chart_clients_by_source.contains_key(source) {
            for symbol in &source.symbols {
                remove_from_index_set(&mut self.sources_by_symbol, symbol, source);
            }
        }
    }
}

fn remove_from_index_set<K, V>(index: &mut BTreeMap<K, BTreeSet<V>>, key: &K, value: &V)
where
    K: Ord + Clone,
    V: Ord,
{
    let should_remove_key = if let Some(values) = index.get_mut(key) {
        values.remove(value);
        values.is_empty()
    } else {
        false
    };
    if should_remove_key {
        index.remove(key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chart(chart_id: &str, symbols: Vec<&str>) -> SetChartCommand {
        SetChartCommand {
            chart_id: chart_id.to_string(),
            symbols: symbols.into_iter().map(ToString::to_string).collect(),
            duration_ns: 60_000_000_000,
            view_width: 64,
            left_kline_id: None,
            focus_datetime_ns: None,
            focus_position: None,
        }
    }

    #[test]
    fn replacing_quotes_updates_reverse_symbol_index() {
        let mut registry = InterestRegistry::default();
        let client = ClientId::new(1);

        registry.set_quotes(client, vec!["SHFE.au2602".to_string()]);
        registry.set_quotes(client, vec!["DCE.m2609".to_string()]);

        assert!(!registry.quote_clients_by_symbol.contains_key("SHFE.au2602"));
        assert_eq!(
            registry
                .quote_clients_by_symbol
                .get("DCE.m2609")
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .collect::<Vec<_>>(),
            vec![client]
        );
    }

    #[test]
    fn replacing_chart_updates_reverse_source_indexes() {
        let mut registry = InterestRegistry::default();
        let client = ClientId::new(1);

        let old_source = registry.set_chart(client, chart("chart-1", vec!["SHFE.au2602"]));
        let new_source = registry.set_chart(client, chart("chart-1", vec!["DCE.m2609"]));

        assert!(!registry.chart_clients_by_source.contains_key(&old_source));
        assert_eq!(
            registry
                .chart_clients_by_source
                .get(&new_source)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .collect::<Vec<_>>(),
            vec![client]
        );
        assert!(!registry.sources_by_symbol.contains_key("SHFE.au2602"));
        assert_eq!(
            registry
                .sources_by_symbol
                .get("DCE.m2609")
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .collect::<Vec<_>>(),
            vec![new_source]
        );
    }

    #[test]
    fn removing_client_clears_reverse_indexes() {
        let mut registry = InterestRegistry::default();
        let client = ClientId::new(1);

        registry.set_quotes(client, vec!["SHFE.au2602".to_string()]);
        let source = registry.set_chart(client, chart("chart-1", vec!["SHFE.au2602"]));
        registry.remove_client(client);

        assert!(!registry.quote_clients_by_symbol.contains_key("SHFE.au2602"));
        assert!(!registry.sources_by_symbol.contains_key("SHFE.au2602"));
        assert!(!registry.chart_clients_by_source.contains_key(&source));
    }

    #[test]
    fn chart_interest_count_tracks_multiple_chart_ids_for_one_client_source() {
        let mut registry = InterestRegistry::default();
        let client = ClientId::new(1);

        let source = registry.set_chart(client, chart("chart-1", vec!["SHFE.au2602"]));
        registry.set_chart(client, chart("chart-2", vec!["SHFE.au2602"]));

        assert_eq!(registry.chart_clients(&source), vec![client]);
        assert_eq!(registry.chart_interest_count(&source), 2);
        assert_eq!(
            registry.downstream_chart_id(client, &source),
            Some("chart-1")
        );
    }
}
