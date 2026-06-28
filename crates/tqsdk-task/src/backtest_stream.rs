use std::future::Future;
use std::pin::Pin;

use crate::Result;
use crate::replay::{ReplayMarketEvent, ReplayMarketSource};

pub trait BacktestMarketStream: Send {
    fn next_event<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<ReplayMarketEvent>>> + 'a>>;
}

#[derive(Debug)]
pub struct ReplayMarketStream {
    source: ReplayMarketSource,
}

impl ReplayMarketStream {
    #[must_use]
    pub fn new(source: ReplayMarketSource) -> Self {
        Self { source }
    }
}

impl BacktestMarketStream for ReplayMarketStream {
    fn next_event<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<ReplayMarketEvent>>> + 'a>> {
        Box::pin(async move { Ok(self.source.next_event()) })
    }
}
