#![no_main]

use std::fs;

use libfuzzer_sys::fuzz_target;
use tempfile::tempdir;

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }

    let split = data[0] as usize % data.len();
    let raw_name = String::from_utf8_lossy(&data[..split]);
    let file_name = tqsdk_data::__fuzz_safe_cache_file_name(&raw_name);

    let Ok(dir) = tempdir() else {
        return;
    };
    let path = dir.path().join(file_name);
    let _ = fs::write(path, &data[split..]);

    let Ok(cache) = tqsdk_data::HistorySeriesCache::open(dir.path()) else {
        return;
    };
    let _ = cache.scan();
});
