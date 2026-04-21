use tqsdk_core::Tick;

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
