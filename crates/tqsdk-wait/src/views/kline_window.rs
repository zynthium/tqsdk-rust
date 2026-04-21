use tqsdk_core::Kline;

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
