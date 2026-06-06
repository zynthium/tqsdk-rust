#![cfg_attr(not(test), forbid(unsafe_code))]

use tqsdk_relay::{RelayConfig, RelayEngine};

fn main() {
    let config = RelayConfig::default();
    if let Err(err) = config.validate() {
        eprintln!("{err}");
        std::process::exit(2);
    }

    let _engine =
        RelayEngine::new_memory_only(config.tick_ring_capacity, config.kline_ring_capacity);
    eprintln!(
        "tqsdk-relay configured: downstream={} metrics={}",
        config.downstream_listen, config.metrics_listen
    );
}
