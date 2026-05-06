#![no_main]

use libfuzzer_sys::fuzz_target;
use serde_json::Value;
use tqsdk_core::{AdapterRegistry, InputPayload, IoEvent, ProtocolDomain, RuntimeInput};

fuzz_target!(|data: &[u8]| {
    let payload = match serde_json::from_slice::<Value>(data) {
        Ok(value) => InputPayload::Json(value),
        Err(_) if data.len() % 2 == 0 => {
            InputPayload::Text(String::from_utf8_lossy(data).into_owned())
        }
        Err(_) => InputPayload::Binary(data.to_vec()),
    };

    let input = RuntimeInput::Io(IoEvent {
        route: "fuzz".to_string(),
        domains: vec![
            ProtocolDomain::System,
            ProtocolDomain::Market,
            ProtocolDomain::Trade,
            ProtocolDomain::Replay,
            ProtocolDomain::Query,
            ProtocolDomain::Schema,
        ],
        payload,
    });

    let mut registry = AdapterRegistry::new();
    registry.register_default_adapters();
    let _ = registry.decode_input(&input);
});
