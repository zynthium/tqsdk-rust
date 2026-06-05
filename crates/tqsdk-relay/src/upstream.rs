#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::VecDeque;

use crate::protocol::RelayTickRow;

#[derive(Debug, Clone, PartialEq)]
pub struct UpstreamTick {
    pub symbol: String,
    pub row: RelayTickRow,
}

pub trait UpstreamTickSource {
    fn next_tick(&mut self) -> impl std::future::Future<Output = Option<UpstreamTick>> + Send + '_;
}

#[derive(Debug, Default)]
pub struct FakeUpstreamTickSource {
    ticks: VecDeque<UpstreamTick>,
}

impl FakeUpstreamTickSource {
    pub fn push(&mut self, tick: UpstreamTick) {
        self.ticks.push_back(tick);
    }
}

impl UpstreamTickSource for FakeUpstreamTickSource {
    async fn next_tick(&mut self) -> Option<UpstreamTick> {
        self.ticks.pop_front()
    }
}
