use tqsdk_core::Kline;

#[derive(Debug, Clone, Default)]
pub struct KlineWindow {
    rows: Vec<Kline>,
}

impl KlineWindow {
    pub fn new(rows: Vec<Kline>) -> Self {
        Self { rows }
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

    pub fn iter(&self) -> impl Iterator<Item = &Kline> {
        self.rows.iter()
    }
}
