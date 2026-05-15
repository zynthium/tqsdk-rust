use tqsdk_core::Tick;

/// Owned snapshot of the current tick serial window.
#[derive(Debug, Clone, Default)]
pub struct TickWindow {
    symbol: String,
    view_width: usize,
    chart_id: String,
    rows: Vec<Tick>,
}

impl TickWindow {
    #[must_use]
    pub fn new(symbol: String, view_width: usize, chart_id: String, rows: Vec<Tick>) -> Self {
        Self {
            symbol,
            view_width,
            chart_id,
            rows,
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    #[must_use]
    pub fn last(&self) -> Option<&Tick> {
        self.rows.last()
    }

    #[must_use]
    pub fn first(&self) -> Option<&Tick> {
        self.rows.first()
    }

    #[must_use]
    pub fn rows(&self) -> &[Tick] {
        &self.rows
    }

    #[must_use]
    pub fn into_rows(self) -> Vec<Tick> {
        self.rows
    }

    #[must_use]
    pub fn get(&self, index: usize) -> Option<&Tick> {
        self.rows.get(index)
    }

    pub fn iter(&self) -> impl Iterator<Item = &Tick> {
        self.rows.iter()
    }

    #[must_use]
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    #[must_use]
    pub fn view_width(&self) -> usize {
        self.view_width
    }

    #[must_use]
    pub fn chart_id(&self) -> &str {
        &self.chart_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tick_window_exposes_owned_metadata_and_rows() {
        let rows = vec![
            Tick {
                id: 10,
                last_price: 610.0,
                ..Tick::default()
            },
            Tick {
                id: 11,
                last_price: 611.0,
                ..Tick::default()
            },
        ];
        let window = TickWindow::new(
            "SHFE.au2606".to_string(),
            30,
            "tick-chart-1".to_string(),
            rows,
        );

        assert_eq!(window.symbol(), "SHFE.au2606");
        assert_eq!(window.view_width(), 30);
        assert_eq!(window.chart_id(), "tick-chart-1");
        assert_eq!(window.len(), 2);
        assert!(!window.is_empty());
        assert_eq!(window.get(0).expect("first row should exist").id, 10);
        assert_eq!(
            window.last().expect("last row should exist").last_price,
            611.0
        );
        assert_eq!(window.first().expect("first row should exist").id, 10);
        assert_eq!(window.rows().len(), 2);
        assert_eq!(
            window.iter().map(|row| row.id).collect::<Vec<_>>(),
            vec![10, 11]
        );
        assert_eq!(
            window
                .clone()
                .into_rows()
                .into_iter()
                .map(|row| row.id)
                .collect::<Vec<_>>(),
            vec![10, 11]
        );
    }

    #[test]
    fn tick_window_empty_reports_no_last_row() {
        let window = TickWindow::default();

        assert_eq!(window.len(), 0);
        assert!(window.is_empty());
        assert!(window.last().is_none());
        assert!(window.first().is_none());
        assert!(window.get(0).is_none());
        assert!(window.rows().is_empty());
        assert_eq!(window.iter().count(), 0);
    }
}
