use tqsdk_data::{DataError, HistorySeriesCache, HistorySeriesCacheFileStatus};

#[test]
fn tqbn_scan_reports_bad_magic_as_incomplete_write() {
    let dir = temp_dir("bad-magic");
    let path = daily_tick_file(&dir, "19700101", "SHFE.rb2601");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, b"BAD!").unwrap();

    let cache = HistorySeriesCache::open(&dir).unwrap();
    let scan = cache.scan().unwrap();
    assert_eq!(scan.files.len(), 1);
    assert_eq!(
        scan.files[0].status,
        HistorySeriesCacheFileStatus::IncompleteWrite
    );
    assert!(scan.files[0].error.as_deref().unwrap().contains("magic"));
}

#[test]
fn tqbn_read_rejects_truncated_block() {
    let dir = temp_dir("truncated-block");
    let path = daily_tick_file(&dir, "19700101", "SHFE.rb2601");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut bytes = tqbn_valid_tick_prefix("SHFE.rb2601");
    bytes.extend_from_slice(b"TQBB\x02\x00\x00");
    std::fs::write(&path, bytes).unwrap();

    let cache = HistorySeriesCache::open(&dir).unwrap();
    let err = cache
        .read_tick_data_series(tqsdk_data::TickDataSeriesRequest::new("SHFE.rb2601", 0, 1))
        .unwrap_err();
    assert!(
        matches!(err, DataError::InvalidResponse(message) if message.contains("truncated") && message.contains("block"))
    );
}

#[test]
fn tqbn_rejects_checksum_valid_invalid_metadata() {
    let dir = temp_dir("invalid-metadata");
    let path = daily_tick_file(&dir, "19700101", "SHFE.rb2601");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, tqbn_prefix_with_invalid_schema_metadata()).unwrap();

    let cache = HistorySeriesCache::open(&dir).unwrap();
    let scan = cache.scan().unwrap();
    assert_eq!(scan.files.len(), 1);
    assert_eq!(
        scan.files[0].status,
        HistorySeriesCacheFileStatus::IncompleteWrite
    );
    assert!(
        scan.files[0]
            .error
            .as_deref()
            .is_some_and(|message| message.contains("metadata") && message.contains("schema"))
    );

    let err = cache
        .read_tick_data_series(tqsdk_data::TickDataSeriesRequest::new("SHFE.rb2601", 0, 1))
        .unwrap_err();
    assert!(
        matches!(err, DataError::InvalidResponse(message) if message.contains("metadata") && message.contains("schema"))
    );
}

fn tqbn_valid_tick_prefix(symbol: &str) -> Vec<u8> {
    let mut metadata = Vec::new();
    write_string(&mut metadata, "tqsdk-history");
    metadata.push(2);
    write_string(&mut metadata, symbol);
    metadata.extend_from_slice(&0_i64.to_le_bytes());
    metadata.extend_from_slice(&1_000_000_000_i64.to_le_bytes());
    metadata.extend_from_slice(&1_000_000_i64.to_le_bytes());
    metadata.push(5);
    metadata.extend_from_slice(&1_u32.to_le_bytes());
    metadata.extend_from_slice(&1_u32.to_le_bytes());
    write_string(&mut metadata, symbol);
    metadata.extend_from_slice(&i64::MIN.to_le_bytes());
    metadata.extend_from_slice(&i64::MAX.to_le_bytes());
    encode_tqbn_prefix(&metadata)
}

fn tqbn_prefix_with_invalid_schema_metadata() -> Vec<u8> {
    let mut metadata = Vec::new();
    write_string(&mut metadata, "tqsdk-history");
    metadata.push(9);
    write_string(&mut metadata, "SHFE.rb2601");
    metadata.extend_from_slice(&0_i64.to_le_bytes());
    metadata.extend_from_slice(&1_000_000_000_i64.to_le_bytes());
    metadata.extend_from_slice(&1_000_000_i64.to_le_bytes());
    metadata.push(1);
    metadata.extend_from_slice(&0_u32.to_le_bytes());
    encode_tqbn_prefix(&metadata)
}

fn encode_tqbn_prefix(metadata: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"TQBN");
    bytes.push(1);
    bytes.extend_from_slice(&2_u32.to_le_bytes());
    bytes.extend_from_slice(&(metadata.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&checksum64_fnv1a(metadata).to_le_bytes());
    bytes.extend_from_slice(metadata);
    bytes
}

fn write_string(bytes: &mut Vec<u8>, value: &str) {
    bytes.extend_from_slice(&(value.len() as u16).to_le_bytes());
    bytes.extend_from_slice(value.as_bytes());
}

fn checksum64_fnv1a(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn temp_dir(name: &str) -> std::path::PathBuf {
    let mut dir = std::env::temp_dir();
    dir.push(format!(
        "tqsdk-tqbn-corruption-{name}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn daily_tick_file(root: &std::path::Path, day: &str, symbol: &str) -> std::path::PathBuf {
    root.join("series")
        .join(day)
        .join("tick")
        .join(format!("{}.tqbn", symbol.replace('/', "%2F")))
}
