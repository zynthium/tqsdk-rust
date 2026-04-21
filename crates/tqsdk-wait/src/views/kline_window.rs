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

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn last(&self) -> Option<&Kline> {
        self.rows.last()
    }

    pub fn get(&self, index: usize) -> Option<&Kline> {
        self.rows.get(index)
    }

    pub fn iter(&self) -> impl Iterator<Item = &Kline> {
        self.rows.iter()
    }

    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    pub fn duration_ns(&self) -> i64 {
        self.duration_ns
    }

    pub fn view_width(&self) -> usize {
        self.view_width
    }

    pub fn chart_id(&self) -> &str {
        &self.chart_id
    }
}
