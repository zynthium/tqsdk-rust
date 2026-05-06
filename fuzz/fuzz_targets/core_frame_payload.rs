#![no_main]

use libfuzzer_sys::fuzz_target;
use tqsdk_core::{ProtocolDomain, RawFrame};

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }

    let frame = match data[0] % 4 {
        0 => RawFrame::Text(String::from_utf8_lossy(&data[1..]).into_owned()),
        1 => RawFrame::Binary(data[1..].to_vec()),
        2 => RawFrame::Ping,
        _ => RawFrame::Pong,
    };

    let _ = tqsdk_core::__fuzz_parse_raw_frame_payload(frame, ProtocolDomain::Market);
});
