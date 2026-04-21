use tqsdk_core::Tick;

#[derive(Debug, Clone, Default)]
pub struct TickWindow {
    rows: Vec<Tick>,
}

impl TickWindow {
    pub fn new(rows: Vec<Tick>) -> Self {
        Self { rows }
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

    pub fn iter(&self) -> impl Iterator<Item = &Tick> {
        self.rows.iter()
    }
}
