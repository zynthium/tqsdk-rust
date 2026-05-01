use tqsdk_core::Kline;

/// Owned snapshot of the current kline serial window.
#[derive(Debug, Clone, Default)]
pub struct KlineWindow {
    symbol: String,
    duration_ns: i64,
    view_width: usize,
    chart_id: String,
    rows: Vec<Kline>,
}

impl KlineWindow {
    #[must_use]
    pub fn new(
        symbol: String,
        duration_ns: i64,
        view_width: usize,
        chart_id: String,
        rows: Vec<Kline>,
    ) -> Self {
        Self {
            symbol,
            duration_ns,
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
    pub fn last(&self) -> Option<&Kline> {
        self.rows.last()
    }

    #[must_use]
    pub fn get(&self, index: usize) -> Option<&Kline> {
        self.rows.get(index)
    }

    pub fn iter(&self) -> impl Iterator<Item = &Kline> {
        self.rows.iter()
    }

    #[must_use]
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    #[must_use]
    pub fn duration_ns(&self) -> i64 {
        self.duration_ns
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
    fn kline_window_exposes_owned_metadata_and_rows() {
        let rows = vec![
            Kline {
                id: 1,
                close: 610.0,
                ..Kline::default()
            },
            Kline {
                id: 2,
                close: 611.0,
                ..Kline::default()
            },
        ];
        let window = KlineWindow::new(
            "SHFE.au2606".to_string(),
            60_000_000_000,
            20,
            "chart-1".to_string(),
            rows,
        );

        assert_eq!(window.symbol(), "SHFE.au2606");
        assert_eq!(window.duration_ns(), 60_000_000_000);
        assert_eq!(window.view_width(), 20);
        assert_eq!(window.chart_id(), "chart-1");
        assert_eq!(window.len(), 2);
        assert!(!window.is_empty());
        assert_eq!(window.get(0).expect("first row should exist").id, 1);
        assert_eq!(window.last().expect("last row should exist").close, 611.0);
        assert_eq!(
            window.iter().map(|row| row.id).collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[test]
    fn kline_window_empty_reports_no_last_row() {
        let window = KlineWindow::default();

        assert_eq!(window.len(), 0);
        assert!(window.is_empty());
        assert!(window.last().is_none());
        assert!(window.get(0).is_none());
        assert_eq!(window.iter().count(), 0);
    }
}
