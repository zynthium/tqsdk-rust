use tqsdk_core::Tick;

#[derive(Debug, Clone, Default)]
pub struct TickWindow {
    symbol: String,
    view_width: usize,
    chart_id: String,
    rows: Vec<Tick>,
}

impl TickWindow {
    pub fn new(symbol: String, view_width: usize, chart_id: String, rows: Vec<Tick>) -> Self {
        Self {
            symbol,
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

    pub fn last(&self) -> Option<&Tick> {
        self.rows.last()
    }

    pub fn get(&self, index: usize) -> Option<&Tick> {
        self.rows.get(index)
    }

    pub fn iter(&self) -> impl Iterator<Item = &Tick> {
        self.rows.iter()
    }

    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    pub fn view_width(&self) -> usize {
        self.view_width
    }

    pub fn chart_id(&self) -> &str {
        &self.chart_id
    }
}
