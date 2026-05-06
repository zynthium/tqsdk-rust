#![no_main]

use std::io::{BufReader, Cursor};

use libfuzzer_sys::fuzz_target;
use tqsdk_data::MarketCacheReader;

fuzz_target!(|data: &[u8]| {
    let cursor = Cursor::new(data);
    let reader = MarketCacheReader::new(BufReader::new(cursor));

    for item in reader.take(64) {
        let _ = item;
    }
});
