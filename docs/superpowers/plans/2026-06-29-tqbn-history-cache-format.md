# TQBN History Cache Format Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the current `.tqseries` history cache file implementation with a tqsdk-specific DBN-like persistent format (`TQBN`) that keeps the existing public `HistorySeriesCache` / `BacktestTickCache` interface stable while improving self-description, forward compatibility, fixed-width decoding, corruption handling, and long-term storage safety.

**Architecture:** `HistorySeriesCache` remains the external seam. The replacement is an internal `HistorySeriesStore` adapter, so `DataClient`, `BacktestTickCache`, and `HistoryTickReplayStream` continue to consume the same cache interface. TQBN v1 remains one file per `(symbol, tick|duration)` to match current locality, but its metadata includes an instrument mapping so future multi-symbol/day files can be added without changing the public cache interface.

**Tech Stack:** Rust 2024, `tqsdk-data`, internal binary codec modules, fixed little-endian encoding, `#[repr(C)]` layout tests, current `fs2` file locking, existing `HistorySeriesStore` trait, current cargo test/clippy workflow.

---

## Design Decisions

- Keep public models unchanged: `tqsdk_core::Kline` and `tqsdk_core::Tick` remain user-facing structs with `f64` prices. Fixed-point conversion is an implementation detail inside the store adapter.
- Do not depend on the `dbn` crate. DBN's crate-level schema/publisher/venue model is Databento-specific; TQBN only borrows the file-format ideas.
- Do not expose TQBN record structs publicly. They are internal implementation details under `crates/tqsdk-data/src/history_series_cache/tqbn/`.
- Do not keep `.tqseries` compatibility in the new default path. The user direction is "旧缓存直接废弃"; old `.tqseries` files can be ignored by scan or reported as ignored legacy files.
- Use little-endian for all on-disk scalar fields. The current `.tqseries` implementation uses native-endian for rows; TQBN must not.
- Use `i64` fixed-point prices with `FIXED_PRICE_SCALE = 1_000_000_000` and `UNDEF_PRICE = i64::MAX`.
- Use `i64` fixed-point turnover/amount with a metadata-declared `amount_scale`; v1 default is `1_000_000`.
- Use row-level DBN-like headers only where they pay for themselves. V1 row records include a compact common header so unknown record types can be skipped. Blocks carry checksums and optional compression flags.
- Keep zstd out of the first landing unless benchmark data justifies adding the dependency. The binary layout reserves a compression flag; v1 implementation writes uncompressed blocks first.

## Proposed File Layout

```text
crates/tqsdk-data/src/history_series_cache/
  tqbn/
    mod.rs          # TqbnHistoryStore adapter and module wiring
    format.rs       # constants, rtype ids, repr(C) record structs, layout assertions
    fixed.rs        # f64 <-> fixed i64 conversion and sentinel handling
    metadata.rs     # metadata model, symbol/instrument mapping, encode/decode
    codec.rs        # file/block/record encoder and decoder
    index.rs        # parsed file state, coverage/index merge, range lookup
    compaction.rs   # rewrite/compact and size/retention eviction helpers
```

Existing files to modify:

```text
crates/tqsdk-data/src/history_series_cache.rs
crates/tqsdk-data/src/history_series_cache/store.rs
crates/tqsdk-data/src/history_series_cache/series_file_store.rs
crates/tqsdk-data/src/lib.rs
crates/tqsdk-data/tests/history_series_cache.rs
crates/tqsdk-data/tests/history_series_single_file_store.rs
crates/tqsdk-task/tests/history_tick_replay.rs
crates/tqsdk-data/README.md
docs/architecture/api-data.md
docs/architecture/crate-boundaries.md
docs/architecture/validation.md
```

New tests:

```text
crates/tqsdk-data/tests/history_series_tqbn_format.rs
crates/tqsdk-data/tests/history_series_tqbn_store.rs
crates/tqsdk-data/tests/history_series_tqbn_corruption.rs
crates/tqsdk-data/tests/history_series_tqbn_compaction.rs
```

Optional later tests:

```text
crates/tqsdk-data/benches/history_series_tqbn.rs
crates/tqsdk-data/fuzz/fuzz_targets/tqbn_decode.rs
```

## TQBN V1 Binary Contract

### File Path

V1 keeps current series-local path structure and only changes extension:

```text
series/<escaped-symbol>/tick.tqbn
series/<escaped-symbol>/<duration_ns>.tqbn
```

`HistorySeriesCache::tick_series_path(symbol)` and `kline_series_path(symbol, duration_ns)` return `.tqbn` paths after the store switch.

### File Prefix

All files start with a fixed-size prefix:

```rust
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TqbnFilePrefix {
    magic: [u8; 4],        // b"TQBN"
    version: u8,           // 1
    header_len: u8,        // size_of::<TqbnFilePrefix>()
    flags: u16,            // bit 0 = little endian; v1 must set it
    metadata_len: u32,     // bytes following prefix
    metadata_crc32: u32,   // checksum of metadata bytes
}
```

Constants:

```rust
const TQBN_FORMAT_ID: &str = "tqsdk.tqbn.v1";
const TQBN_SCHEMA_VERSION: u32 = 2;
const TQBN_MAGIC: [u8; 4] = *b"TQBN";
const TQBN_VERSION: u8 = 1;
const FIXED_PRICE_SCALE: i64 = 1_000_000_000;
const FIXED_AMOUNT_SCALE: i64 = 1_000_000;
const UNDEF_PRICE: i64 = i64::MAX;
const UNDEF_AMOUNT: i64 = i64::MAX;
```

### Metadata

Metadata is length-prefixed binary, not JSON. V1 metadata fields:

```rust
struct TqbnMetadata {
    dataset: String,            // "tqsdk-history"
    schema: TqbnSchema,          // Kline or Tick
    symbol: String,
    duration_ns: i64,            // 0 for tick
    price_scale: i64,            // 1e9
    amount_scale: i64,           // 1e6
    level_depth: u8,             // 0 for kline, 1 or 5 for tick
    instruments: Vec<TqbnInstrumentMapping>,
}

struct TqbnInstrumentMapping {
    instrument_id: u32,
    symbol: String,
    start_ns: i64,
    end_ns: i64,
}
```

V1 one-series files always contain one mapping:

```text
instrument_id = 1
symbol = metadata.symbol
start_ns = i64::MIN
end_ns = i64::MAX
```

### Common Record Header

Every record begins with a 16-byte DBN-like header:

```rust
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TqbnRecordHeader {
    length_words: u8,    // record length in 32-bit words
    rtype: u8,           // TqbnRType as raw u8
    flags: u16,          // record flags
    instrument_id: u32,  // metadata mapping id
    ts_event: u64,       // row datetime as u64 nanoseconds; i64 conversion checked
}
```

Record type ids:

```rust
#[repr(u8)]
enum TqbnRType {
    Kline = 1,
    Tick1 = 2,
    Tick5 = 3,
    Coverage = 16,
    Index = 17,
    MetadataUpdate = 18,
}
```

Decoder rule:

- Read 16-byte header.
- Reject `length_words * 4 < size_of::<TqbnRecordHeader>()`.
- If `rtype` is unknown, skip `record_size - header_size`.
- If `rtype` is known but `record_size` is shorter than the v1 struct, return typed decode error.
- If `record_size` is longer than the v1 struct, decode known prefix and skip trailing bytes. This is the forward-compatibility mechanism.

### Row Records

TQBN records are internal. Public `Kline` / `Tick` conversion happens at adapter boundaries.

```rust
#[repr(C)]
struct TqbnKlineRecordV1 {
    hd: TqbnRecordHeader,
    row_id: i64,
    open: i64,
    high: i64,
    low: i64,
    close: i64,
    volume: i64,
    open_oi: i64,
    close_oi: i64,
    epoch: i64,          // i64::MIN means None
}
```

```rust
#[repr(C)]
struct TqbnTick1RecordV1 {
    hd: TqbnRecordHeader,
    row_id: i64,
    last_price: i64,
    average: i64,
    highest: i64,
    lowest: i64,
    ask_price1: i64,
    ask_volume1: i64,
    bid_price1: i64,
    bid_volume1: i64,
    volume: i64,
    amount: i64,
    open_interest: i64,
    epoch: i64,
}
```

```rust
#[repr(C)]
struct TqbnTick5RecordV1 {
    hd: TqbnRecordHeader,
    row_id: i64,
    last_price: i64,
    average: i64,
    highest: i64,
    lowest: i64,
    ask_price1: i64,
    ask_volume1: i64,
    bid_price1: i64,
    bid_volume1: i64,
    ask_price2: i64,
    ask_volume2: i64,
    bid_price2: i64,
    bid_volume2: i64,
    ask_price3: i64,
    ask_volume3: i64,
    bid_price3: i64,
    bid_volume3: i64,
    ask_price4: i64,
    ask_volume4: i64,
    bid_price4: i64,
    bid_volume4: i64,
    ask_price5: i64,
    ask_volume5: i64,
    bid_price5: i64,
    bid_volume5: i64,
    volume: i64,
    amount: i64,
    open_interest: i64,
    epoch: i64,
}
```

### Coverage Records

Coverage remains explicit and append-only:

```rust
#[repr(C)]
struct TqbnCoverageRecordV1 {
    hd: TqbnRecordHeader,
    range_start_ns: i64,
    range_end_ns: i64,
    rows: u64,
    id_start: i64,
    id_end: i64,
    has_id_range: u8,
    reserved: [u8; 7],
}
```

### Blocks

Records are grouped into blocks for checksum and future compression:

```rust
#[repr(C)]
struct TqbnBlockHeader {
    magic: [u8; 4],        // b"TQBB"
    header_len: u8,        // size_of::<TqbnBlockHeader>()
    block_type: u8,        // rows, coverage, index, mixed
    flags: u16,            // bit 0 = compressed
    record_count: u32,
    uncompressed_len: u32,
    encoded_len: u32,
    checksum64: u64,
    first_ts_event: u64,
    last_ts_event: u64,
}
```

V1 writes uncompressed blocks. A later task can add zstd by setting `flags & 1` and storing compressed payload.

---

## Task 1: Document Format Contract First

**Files:**

- Create: `docs/architecture/history-cache-format.md`
- Modify: `docs/architecture/api-data.md`
- Modify: `docs/architecture/validation.md`

- [ ] **Step 1: Write the architecture doc**

Create `docs/architecture/history-cache-format.md` with sections:

```markdown
# History Cache Format

## Current Decision

The canonical tqsdk-rust history cache format is TQBN v1, a DBN-like internal
binary format used by `tqsdk-data`.

## Public Interface

The public cache interface remains `HistorySeriesCache` and `BacktestTickCache`.
TQBN record structs, metadata structs, and codec helpers are crate-internal.

## File Identity

- Format id: `tqsdk.tqbn.v1`
- Schema version: `2`
- Extension: `.tqbn`
- Root layout:
  - `series/<escaped-symbol>/tick.tqbn`
  - `series/<escaped-symbol>/<duration_ns>.tqbn`

## Binary Contract

All scalar fields are little-endian. All records begin with `TqbnRecordHeader`.
Unknown records are skipped using `length_words * 4`.

## Price Encoding

Prices are stored as fixed-point `i64` with `FIXED_PRICE_SCALE = 1_000_000_000`.
Unset or non-finite prices are stored as `UNDEF_PRICE = i64::MAX`.

## Compatibility

Readers must decode v1 records by known prefix and skip trailing bytes when the
record length is larger than the v1 struct. Known record types shorter than the
v1 struct are invalid unless an explicit compat module handles that version.
```

- [ ] **Step 2: Update architecture index**

Add a bullet in `docs/architecture/api-data.md` pointing to `history-cache-format.md` and stating that old `.tqseries` is no longer canonical.

- [ ] **Step 3: Update validation doc**

Add these validation commands to `docs/architecture/validation.md` under history cache:

```bash
rtk cargo test -p tqsdk-data --test history_series_tqbn_format
rtk cargo test -p tqsdk-data --test history_series_tqbn_store
rtk cargo test -p tqsdk-data --test history_series_tqbn_corruption
rtk cargo test -p tqsdk-data --test history_series_tqbn_compaction
rtk cargo test -p tqsdk-data
rtk cargo clippy -p tqsdk-data --all-targets -- -D warnings
```

- [ ] **Step 4: Run docs-only verification**

Run:

```bash
rtk git diff --check
```

Expected: exit 0.

---

## Task 2: Add TQBN Module Skeleton and Public-Surface Guards

**Files:**

- Create: `crates/tqsdk-data/src/history_series_cache/tqbn/mod.rs`
- Create: `crates/tqsdk-data/src/history_series_cache/tqbn/format.rs`
- Modify: `crates/tqsdk-data/src/history_series_cache.rs`
- Modify: `crates/tqsdk-data/src/lib.rs`
- Test: `crates/tqsdk-data/src/history_series_cache.rs` doctests

- [ ] **Step 1: Add compile-fail public-surface guards**

Extend the existing `HistorySeriesCache` docs in `crates/tqsdk-data/src/history_series_cache.rs`:

```rust
/// TQBN record and metadata structs are internal storage details.
///
/// ```compile_fail
/// use tqsdk_data::{TqbnRecordHeader, TqbnMetadata};
///
/// let _ = std::mem::size_of::<TqbnRecordHeader>();
/// let _ = std::mem::size_of::<TqbnMetadata>();
/// ```
```

- [ ] **Step 2: Run doctest and verify red**

Run:

```bash
rtk cargo test -p tqsdk-data --doc
```

Expected: failure because `TqbnRecordHeader` and `TqbnMetadata` do not exist yet is acceptable only if the compile-fail block passes. If the test suite passes here, keep it; this guard is for future re-export prevention.

- [ ] **Step 3: Add internal module skeleton**

Create `crates/tqsdk-data/src/history_series_cache/tqbn/mod.rs`:

```rust
mod format;

pub(super) use format::{
    TQBN_FORMAT_ID, TQBN_SCHEMA_VERSION, TqbnRType,
};
```

Create `crates/tqsdk-data/src/history_series_cache/tqbn/format.rs`:

```rust
pub(super) const TQBN_FORMAT_ID: &str = "tqsdk.tqbn.v1";
pub(super) const TQBN_SCHEMA_VERSION: u32 = 2;
pub(super) const TQBN_MAGIC: [u8; 4] = *b"TQBN";
pub(super) const TQBN_BLOCK_MAGIC: [u8; 4] = *b"TQBB";
pub(super) const TQBN_VERSION: u8 = 1;
pub(super) const FIXED_PRICE_SCALE: i64 = 1_000_000_000;
pub(super) const FIXED_AMOUNT_SCALE: i64 = 1_000_000;
pub(super) const UNDEF_PRICE: i64 = i64::MAX;
pub(super) const UNDEF_AMOUNT: i64 = i64::MAX;
pub(super) const NONE_EPOCH: i64 = i64::MIN;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TqbnRType {
    Kline = 1,
    Tick1 = 2,
    Tick5 = 3,
    Coverage = 16,
    Index = 17,
    MetadataUpdate = 18,
}
```

- [ ] **Step 4: Wire module without changing default store**

In `crates/tqsdk-data/src/history_series_cache.rs`, add:

```rust
mod tqbn;
```

Do not switch `HistorySeriesCache::open` yet.

- [ ] **Step 5: Verify green**

Run:

```bash
rtk cargo test -p tqsdk-data --doc
rtk cargo check -p tqsdk-data
```

Expected: doctests pass and crate compiles.

---

## Task 3: Fixed-Point Conversion

**Files:**

- Create: `crates/tqsdk-data/src/history_series_cache/tqbn/fixed.rs`
- Modify: `crates/tqsdk-data/src/history_series_cache/tqbn/mod.rs`
- Test: `crates/tqsdk-data/tests/history_series_tqbn_format.rs`

- [ ] **Step 1: Write failing conversion tests**

Create `crates/tqsdk-data/tests/history_series_tqbn_format.rs` with crate-visible tests through public behavior later. For this task, put unit tests inside `fixed.rs` because helpers are internal:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_price_round_trips_decimal_prices() {
        let encoded = encode_price(618.5).unwrap();
        assert_eq!(encoded, 618_500_000_000);
        assert_eq!(decode_price(encoded), 618.5);
    }

    #[test]
    fn fixed_price_maps_nan_to_sentinel() {
        assert_eq!(encode_price(f64::NAN).unwrap(), UNDEF_PRICE);
        assert!(decode_price(UNDEF_PRICE).is_nan());
    }

    #[test]
    fn fixed_amount_uses_metadata_amount_scale() {
        let encoded = encode_scaled_decimal(1_234.5678, FIXED_AMOUNT_SCALE).unwrap();
        assert_eq!(encoded, 1_234_567_800);
        assert_eq!(decode_scaled_decimal(encoded, FIXED_AMOUNT_SCALE), 1_234.5678);
    }
}
```

- [ ] **Step 2: Run tests and verify red**

Run:

```bash
rtk cargo test -p tqsdk-data fixed_price_round_trips_decimal_prices
```

Expected: compile failure because `fixed.rs` and functions do not exist.

- [ ] **Step 3: Implement fixed conversion**

Create `fixed.rs`:

```rust
use crate::error::{DataError, Result};

use super::format::{FIXED_AMOUNT_SCALE, FIXED_PRICE_SCALE, UNDEF_AMOUNT, UNDEF_PRICE};

pub(super) fn encode_price(value: f64) -> Result<i64> {
    encode_scaled_decimal_or_sentinel(value, FIXED_PRICE_SCALE, UNDEF_PRICE)
}

pub(super) fn decode_price(value: i64) -> f64 {
    if value == UNDEF_PRICE {
        f64::NAN
    } else {
        value as f64 / FIXED_PRICE_SCALE as f64
    }
}

pub(super) fn encode_amount(value: f64) -> Result<i64> {
    encode_scaled_decimal_or_sentinel(value, FIXED_AMOUNT_SCALE, UNDEF_AMOUNT)
}

pub(super) fn decode_amount(value: i64) -> f64 {
    if value == UNDEF_AMOUNT {
        f64::NAN
    } else {
        decode_scaled_decimal(value, FIXED_AMOUNT_SCALE)
    }
}

fn encode_scaled_decimal_or_sentinel(value: f64, scale: i64, sentinel: i64) -> Result<i64> {
    if !value.is_finite() {
        return Ok(sentinel);
    }
    encode_scaled_decimal(value, scale)
}

fn encode_scaled_decimal(value: f64, scale: i64) -> Result<i64> {
    let scaled = (value * scale as f64).round();
    if scaled < i64::MIN as f64 || scaled > i64::MAX as f64 {
        return Err(DataError::InvalidResponse(
            "history cache fixed-point value overflow".to_string(),
        ));
    }
    Ok(scaled as i64)
}

fn decode_scaled_decimal(value: i64, scale: i64) -> f64 {
    value as f64 / scale as f64
}
```

Add in `mod.rs`:

```rust
mod fixed;
```

- [ ] **Step 4: Verify green**

Run:

```bash
rtk cargo test -p tqsdk-data fixed_price_round_trips_decimal_prices
rtk cargo test -p tqsdk-data fixed_price_maps_nan_to_sentinel
rtk cargo test -p tqsdk-data fixed_amount_uses_metadata_amount_scale
```

Expected: all pass.

---

## Task 4: Metadata Encoder/Decoder

**Files:**

- Create: `crates/tqsdk-data/src/history_series_cache/tqbn/metadata.rs`
- Modify: `crates/tqsdk-data/src/history_series_cache/tqbn/mod.rs`
- Test: internal unit tests in `metadata.rs`

- [ ] **Step 1: Write failing metadata tests**

Add tests in `metadata.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_round_trips_tick_symbol_mapping() {
        let metadata = TqbnMetadata::single_series_tick("SHFE.rb2601".to_string(), 5);
        let encoded = encode_metadata(&metadata).unwrap();
        let decoded = decode_metadata(&encoded).unwrap();

        assert_eq!(decoded.dataset, "tqsdk-history");
        assert_eq!(decoded.symbol, "SHFE.rb2601");
        assert_eq!(decoded.duration_ns, 0);
        assert_eq!(decoded.level_depth, 5);
        assert_eq!(decoded.instruments.len(), 1);
        assert_eq!(decoded.instruments[0].instrument_id, 1);
        assert_eq!(decoded.instruments[0].symbol, "SHFE.rb2601");
    }

    #[test]
    fn metadata_round_trips_kline_duration() {
        let metadata = TqbnMetadata::single_series_kline("SHFE.au2608".to_string(), 60_000_000_000);
        let decoded = decode_metadata(&encode_metadata(&metadata).unwrap()).unwrap();

        assert_eq!(decoded.schema, TqbnSchema::Kline);
        assert_eq!(decoded.duration_ns, 60_000_000_000);
        assert_eq!(decoded.level_depth, 0);
    }
}
```

- [ ] **Step 2: Run tests and verify red**

Run:

```bash
rtk cargo test -p tqsdk-data metadata_round_trips_tick_symbol_mapping
```

Expected: compile failure because metadata module is not implemented.

- [ ] **Step 3: Implement metadata model and codec**

Implementation rules:

- Encode strings as `u16 byte_len + UTF-8 bytes`.
- Encode vector counts as `u32`.
- Reject strings longer than `u16::MAX`.
- Reject mapping counts larger than `u32::MAX`.
- Keep fields in this order:
  `dataset`, `schema`, `symbol`, `duration_ns`, `price_scale`, `amount_scale`, `level_depth`, `instrument_count`, mappings.

Create types:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TqbnSchema {
    Kline = 1,
    Tick = 2,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TqbnInstrumentMapping {
    pub instrument_id: u32,
    pub symbol: String,
    pub start_ns: i64,
    pub end_ns: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TqbnMetadata {
    pub dataset: String,
    pub schema: TqbnSchema,
    pub symbol: String,
    pub duration_ns: i64,
    pub price_scale: i64,
    pub amount_scale: i64,
    pub level_depth: u8,
    pub instruments: Vec<TqbnInstrumentMapping>,
}
```

Constructors:

```rust
impl TqbnMetadata {
    pub(super) fn single_series_tick(symbol: String, level_depth: u8) -> Self
    pub(super) fn single_series_kline(symbol: String, duration_ns: i64) -> Self
}
```

- [ ] **Step 4: Verify green**

Run:

```bash
rtk cargo test -p tqsdk-data metadata_round_trips_tick_symbol_mapping
rtk cargo test -p tqsdk-data metadata_round_trips_kline_duration
```

Expected: both pass.

---

## Task 5: Record Layout and Safe Decode Rules

**Files:**

- Modify: `crates/tqsdk-data/src/history_series_cache/tqbn/format.rs`
- Create: `crates/tqsdk-data/src/history_series_cache/tqbn/codec.rs`
- Modify: `crates/tqsdk-data/src/history_series_cache/tqbn/mod.rs`
- Test: internal unit tests in `codec.rs`

- [ ] **Step 1: Write failing record header tests**

Add tests in `codec.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::history_series_cache::tqbn::format::{TqbnRecordHeader, TqbnRType};

    #[test]
    fn record_header_reports_size_from_length_words() {
        let header = TqbnRecordHeader::new::<TqbnCoverageRecordV1>(TqbnRType::Coverage, 1, 123);
        assert_eq!(header.record_size(), std::mem::size_of::<TqbnCoverageRecordV1>());
    }

    #[test]
    fn decoder_skips_unknown_record_by_length() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&[4, 255, 0, 0]);
        bytes.extend_from_slice(&1_u32.to_le_bytes());
        bytes.extend_from_slice(&123_u64.to_le_bytes());

        let decoded = decode_one_record(&bytes).unwrap();
        assert!(matches!(decoded, DecodedTqbnRecord::Unknown { rtype: 255, record_size: 16 }));
    }
}
```

- [ ] **Step 2: Run and verify red**

Run:

```bash
rtk cargo test -p tqsdk-data record_header_reports_size_from_length_words
```

Expected: compile failure because record structs and decoder do not exist.

- [ ] **Step 3: Implement repr(C) record structs**

Add these structs to `format.rs`:

```rust
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct TqbnRecordHeader {
    pub length_words: u8,
    pub rtype: u8,
    pub flags: u16,
    pub instrument_id: u32,
    pub ts_event: u64,
}

impl TqbnRecordHeader {
    pub(super) const LENGTH_MULTIPLIER: usize = 4;

    pub(super) fn new<R>(rtype: TqbnRType, instrument_id: u32, ts_event: u64) -> Self {
        Self {
            length_words: (std::mem::size_of::<R>() / Self::LENGTH_MULTIPLIER) as u8,
            rtype: rtype as u8,
            flags: 0,
            instrument_id,
            ts_event,
        }
    }

    pub(super) fn record_size(self) -> usize {
        self.length_words as usize * Self::LENGTH_MULTIPLIER
    }
}
```

Add `TqbnKlineRecordV1`, `TqbnTick1RecordV1`, `TqbnTick5RecordV1`, and `TqbnCoverageRecordV1` as described in this plan's binary contract.

- [ ] **Step 4: Implement decode-by-header**

`codec.rs` exposes:

```rust
pub(super) enum DecodedTqbnRecord<'a> {
    Kline(&'a [u8]),
    Tick1(&'a [u8]),
    Tick5(&'a [u8]),
    Coverage(&'a [u8]),
    Unknown { rtype: u8, record_size: usize },
}

pub(super) fn decode_one_record(bytes: &[u8]) -> Result<DecodedTqbnRecord<'_>>
```

Do not transmute in this task. Decode only header and return slices after validating record size. Typed row conversion happens in Task 6.

- [ ] **Step 5: Verify green**

Run:

```bash
rtk cargo test -p tqsdk-data record_header_reports_size_from_length_words
rtk cargo test -p tqsdk-data decoder_skips_unknown_record_by_length
```

Expected: both pass.

---

## Task 6: Row Conversion Codec

**Files:**

- Modify: `crates/tqsdk-data/src/history_series_cache/tqbn/codec.rs`
- Modify: `crates/tqsdk-data/src/history_series_cache/tqbn/format.rs`
- Test: `crates/tqsdk-data/tests/history_series_tqbn_format.rs`

- [ ] **Step 1: Write failing public-behavior round-trip tests**

In `crates/tqsdk-data/tests/history_series_tqbn_format.rs`, write tests through a test-only helper only if needed. Prefer internal module tests for raw records and integration tests through `HistorySeriesCache` after Task 8. Add internal tests:

```rust
#[test]
fn kline_record_round_trips_user_row() {
    let row = tqsdk_core::Kline {
        id: 7,
        datetime: 1_000,
        open: 10.1,
        high: 11.2,
        low: 9.9,
        close: 10.8,
        volume: 100,
        open_oi: 200,
        close_oi: 210,
        epoch: Some(42),
    };
    let rec = encode_kline_record(&row).unwrap();
    let decoded = decode_kline_record(&rec).unwrap();
    assert_eq!(decoded.id, row.id);
    assert_eq!(decoded.datetime, row.datetime);
    assert_eq!(decoded.open, row.open);
    assert_eq!(decoded.close, row.close);
    assert_eq!(decoded.epoch, Some(42));
}

#[test]
fn tick5_record_round_trips_depth_five_row() {
    let row = tick_with_five_levels();
    let rec = encode_tick_record(&row, true).unwrap();
    let decoded = decode_tick_record(&rec).unwrap();
    assert_eq!(decoded.id, row.id);
    assert_eq!(decoded.ask_price5, row.ask_price5);
    assert_eq!(decoded.bid_volume5, row.bid_volume5);
}
```

- [ ] **Step 2: Run and verify red**

Run:

```bash
rtk cargo test -p tqsdk-data kline_record_round_trips_user_row
```

Expected: compile failure because row conversion functions do not exist.

- [ ] **Step 3: Implement conversion functions**

Implement:

```rust
pub(super) fn encode_kline_record(row: &Kline) -> Result<TqbnKlineRecordV1>
pub(super) fn decode_kline_record(record: &TqbnKlineRecordV1) -> Result<Kline>
pub(super) fn encode_tick_record(row: &Tick, five_level: bool) -> Result<EncodedTickRecord>
pub(super) fn decode_tick1_record(record: &TqbnTick1RecordV1) -> Result<Tick>
pub(super) fn decode_tick5_record(record: &TqbnTick5RecordV1) -> Result<Tick>
```

Rules:

- `datetime` must be non-negative before converting to `u64 ts_event`; otherwise return `DataError::InvalidResponse`.
- `epoch: None` maps to `NONE_EPOCH`.
- `f64::NAN` price fields map to `UNDEF_PRICE`.
- Tick depth is selected from symbol with existing `tick_rows_use_five_levels(symbol)` when writing store rows.

- [ ] **Step 4: Verify green**

Run:

```bash
rtk cargo test -p tqsdk-data kline_record_round_trips_user_row
rtk cargo test -p tqsdk-data tick5_record_round_trips_depth_five_row
```

Expected: both pass.

---

## Task 7: File Prefix, Block Codec, and Corruption Handling

**Files:**

- Modify: `crates/tqsdk-data/src/history_series_cache/tqbn/codec.rs`
- Test: `crates/tqsdk-data/tests/history_series_tqbn_corruption.rs`

- [ ] **Step 1: Write failing corruption tests**

Create `crates/tqsdk-data/tests/history_series_tqbn_corruption.rs`:

```rust
use tqsdk_data::{DataError, HistorySeriesCache};

#[test]
fn tqbn_scan_reports_bad_magic_as_incomplete_write() {
    let dir = temp_dir("bad-magic");
    let path = dir.join("series").join("SHFE.rb2601").join("tick.tqbn");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, b"BAD!").unwrap();

    let cache = HistorySeriesCache::open(&dir).unwrap();
    let scan = cache.scan().unwrap();
    assert_eq!(scan.files.len(), 1);
    assert!(scan.files[0].error.as_deref().unwrap().contains("magic"));
}

#[test]
fn tqbn_read_rejects_truncated_block() {
    let dir = temp_dir("truncated-block");
    let path = dir.join("series").join("SHFE.rb2601").join("tick.tqbn");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, b"TQBN\x01\x10\x01\x00").unwrap();

    let cache = HistorySeriesCache::open(&dir).unwrap();
    let err = cache
        .read_tick_data_series(tqsdk_data::TickDataSeriesRequest::new("SHFE.rb2601", 0, 1))
        .unwrap_err();
    assert!(matches!(err, DataError::InvalidResponse(message) if message.contains("truncated")));
}

fn temp_dir(name: &str) -> std::path::PathBuf {
    let mut dir = std::env::temp_dir();
    dir.push(format!("tqsdk-tqbn-corruption-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}
```

- [ ] **Step 2: Run and verify red**

Run:

```bash
rtk cargo test -p tqsdk-data --test history_series_tqbn_corruption
```

Expected: failure because `HistorySeriesCache::open` still uses the old store and `.tqbn` is not scanned.

- [ ] **Step 3: Implement prefix and block codec**

Implement:

```rust
pub(super) fn encode_file_prefix(metadata: &[u8]) -> TqbnFilePrefix
pub(super) fn decode_file_prefix(bytes: &[u8]) -> Result<(TqbnFilePrefix, usize)>
pub(super) fn encode_block(block_type: TqbnBlockType, records: &[u8]) -> Vec<u8>
pub(super) fn decode_blocks(bytes: &[u8]) -> Result<Vec<TqbnBlock<'_>>>
```

Checksum:

- Use current FNV-1a `checksum64` initially to avoid adding dependencies.
- Name it `checksum64_fnv1a` so a future stronger checksum swap is localized.

- [ ] **Step 4: Verify codec unit tests**

Run:

```bash
rtk cargo test -p tqsdk-data tqbn
```

Expected: all internal TQBN tests pass. The integration corruption test may still fail until Task 8 wires the store.

---

## Task 8: Implement `TqbnHistoryStore` Without Switching Default

**Files:**

- Modify: `crates/tqsdk-data/src/history_series_cache/tqbn/mod.rs`
- Create: `crates/tqsdk-data/src/history_series_cache/tqbn/index.rs`
- Test: `crates/tqsdk-data/tests/history_series_tqbn_store.rs`

- [ ] **Step 1: Write failing direct-store tests through `HistorySeriesCache` test hook**

If `HistorySeriesCache::from_store` is private, add a crate-internal test constructor under `#[cfg(test)]`:

```rust
#[cfg(test)]
pub(crate) fn open_tqbn_for_test(root_dir: impl AsRef<std::path::Path>) -> Result<HistorySeriesCache> {
    let store = tqbn::TqbnHistoryStore::new(root_dir.as_ref().to_path_buf())?;
    Ok(HistorySeriesCache::from_store(std::sync::Arc::new(store)))
}
```

Create `crates/tqsdk-data/tests/history_series_tqbn_store.rs`:

```rust
use tqsdk_core::{Kline, Tick};
use tqsdk_data::{HistorySeriesCache, KlineDataSeriesRequest, TickDataSeriesRequest};

#[test]
fn tqbn_store_writes_and_reads_kline_range() {
    let dir = temp_dir("kline-range");
    let cache = HistorySeriesCache::open(&dir).unwrap();

    cache.write_kline_range(
        "SHFE.au2608",
        60_000_000_000,
        1_000,
        121_000_000_000,
        &[kline(1, 1_000, 10.1), kline(2, 60_000_000_000, 10.2)],
    ).unwrap();

    let series = cache.read_kline_data_series(
        KlineDataSeriesRequest::new("SHFE.au2608", std::time::Duration::from_secs(60), 1_000, 121_000_000_000)
    ).unwrap();

    assert_eq!(series.len(), 2);
    assert_eq!(series.get(0).unwrap().open, 10.1);
}

#[test]
fn tqbn_store_writes_and_reads_tick_five_level_range() {
    let dir = temp_dir("tick-range");
    let cache = HistorySeriesCache::open(&dir).unwrap();

    cache.write_tick_range("SHFE.rb2601", 1_000, 3_000, &[tick(1, 1_000, 618.5)]).unwrap();

    let series = cache
        .read_tick_data_series(TickDataSeriesRequest::new("SHFE.rb2601", 1_000, 3_000))
        .unwrap();
    assert_eq!(series.len(), 1);
    assert_eq!(series.get(0).unwrap().last_price, 618.5);
    assert_eq!(series.get(0).unwrap().ask_price5, 623.5);
}

fn kline(id: i64, datetime: i64, open: f64) -> Kline {
    Kline { id, datetime, open, high: open, low: open, close: open, ..Kline::default() }
}

fn tick(id: i64, datetime: i64, last_price: f64) -> Tick {
    Tick {
        id,
        datetime,
        last_price,
        ask_price1: last_price + 1.0,
        bid_price1: last_price - 1.0,
        ask_price5: last_price + 5.0,
        bid_price5: last_price - 5.0,
        ..Tick::default()
    }
}

fn temp_dir(name: &str) -> std::path::PathBuf {
    let mut dir = std::env::temp_dir();
    dir.push(format!("tqsdk-tqbn-store-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}
```

- [ ] **Step 2: Run and verify red**

Run:

```bash
rtk cargo test -p tqsdk-data --test history_series_tqbn_store
```

Expected: tests fail until the TQBN store is wired as default or test hook is available.

- [ ] **Step 3: Implement `TqbnHistoryStore`**

`TqbnHistoryStore` implements `HistorySeriesStore`:

```rust
pub(super) struct TqbnHistoryStore {
    root_dir: std::sync::Arc<std::path::PathBuf>,
}

impl TqbnHistoryStore {
    pub(super) fn new(root_dir: std::path::PathBuf) -> Result<Self>
    pub(super) fn series_path(&self, symbol: &str, duration_ns: i64) -> std::path::PathBuf
}
```

Behavior:

- `format_id()` returns `TQBN_FORMAT_ID`.
- `schema_version()` returns `TQBN_SCHEMA_VERSION`.
- `series_path()` returns `.tqbn`.
- `write_segment()` appends a row block and, if `declared_range_ns` exists, a coverage record.
- `commit_coverage()` appends a coverage record only.
- `open_reader()` parses blocks, filters rows by datetime, dedups by row id using last-write-wins.
- `coverage()` reads coverage records and computes missing ranges using existing `rangeset_difference`.

- [ ] **Step 4: Verify direct-store tests**

Run:

```bash
rtk cargo test -p tqsdk-data --test history_series_tqbn_store
```

Expected: TQBN store tests pass.

---

## Task 9: Switch Default Store to TQBN

**Files:**

- Modify: `crates/tqsdk-data/src/history_series_cache.rs`
- Modify: `crates/tqsdk-data/src/history_series_cache/store.rs`
- Modify: `crates/tqsdk-data/tests/history_series_cache.rs`
- Modify: `crates/tqsdk-data/tests/history_series_single_file_store.rs`
- Modify: `crates/tqsdk-task/tests/history_tick_replay.rs`

- [ ] **Step 1: Update existing tests to expect TQBN**

Update expectations:

- `format_id()` becomes `"tqsdk.tqbn.v1"`.
- `schema_version()` becomes `2`.
- tick path becomes `series/<symbol>/tick.tqbn`.
- kline path becomes `series/<symbol>/<duration_ns>.tqbn`.
- scan status remains `Readable`, `IncompleteWrite`, `EmptySegment`, `Ignored`.

- [ ] **Step 2: Run and verify red**

Run:

```bash
rtk cargo test -p tqsdk-data --test history_series_cache
rtk cargo test -p tqsdk-data --test history_series_single_file_store
```

Expected: fail because `HistorySeriesCache::open` still uses `SeriesFileHistoryStore`.

- [ ] **Step 3: Switch default adapter**

In `crates/tqsdk-data/src/history_series_cache.rs`, replace:

```rust
let store = series_file_store::SeriesFileHistoryStore::new(root_dir)?;
```

with:

```rust
let store = tqbn::TqbnHistoryStore::new(root_dir)?;
```

In `store.rs`, replace:

```rust
pub const SERIES_FILE_HISTORY_SERIES_FORMAT_ID: &str = "tqsdk.series-file.v1";
```

with:

```rust
pub const SERIES_FILE_HISTORY_SERIES_FORMAT_ID: &str = "tqsdk.tqbn.v1";
```

Then consider renaming the constant in a later cleanup. Do not rename in the same task unless all call sites are small and tests are green.

- [ ] **Step 4: Verify green**

Run:

```bash
rtk cargo test -p tqsdk-data --test history_series_cache
rtk cargo test -p tqsdk-data --test history_series_single_file_store
rtk cargo test -p tqsdk-task --test history_tick_replay
```

Expected: all pass.

---

## Task 10: Scan, Legacy Ignore, and Maintenance

**Files:**

- Modify: `crates/tqsdk-data/src/history_series_cache/tqbn/mod.rs`
- Create: `crates/tqsdk-data/src/history_series_cache/tqbn/compaction.rs`
- Test: `crates/tqsdk-data/tests/history_series_tqbn_compaction.rs`
- Test: `crates/tqsdk-data/tests/history_series_tqbn_corruption.rs`

- [ ] **Step 1: Write failing scan/maintenance tests**

Add tests:

```rust
#[test]
fn tqbn_scan_ignores_legacy_tqseries_files() {
    let dir = temp_dir("legacy-ignore");
    let legacy = dir.join("series").join("SHFE.rb2601").join("tick.tqseries");
    std::fs::create_dir_all(legacy.parent().unwrap()).unwrap();
    std::fs::write(&legacy, b"TQHSF1\0\0").unwrap();

    let cache = HistorySeriesCache::open(&dir).unwrap();
    let scan = cache.scan().unwrap();

    assert!(scan.files.iter().all(|file| file.path.extension().and_then(|ext| ext.to_str()) != Some("tqseries")));
}

#[test]
fn tqbn_enforce_limits_compacts_duplicate_rows_last_write_wins() {
    let dir = temp_dir("compact-duplicates");
    let cache = HistorySeriesCache::open(&dir).unwrap();

    cache.write_tick_range("SHFE.rb2601", 1_000, 3_000, &[tick(1, 1_000, 100.0)]).unwrap();
    cache.write_tick_range("SHFE.rb2601", 1_000, 3_000, &[tick(1, 1_000, 101.0)]).unwrap();

    let before = std::fs::metadata(cache.tick_series_path("SHFE.rb2601")).unwrap().len();
    let report = cache.enforce_limits(None, None).unwrap();
    let after = std::fs::metadata(cache.tick_series_path("SHFE.rb2601")).unwrap().len();

    assert_eq!(report.removed_files, 0);
    assert!(after < before);

    let series = cache.read_tick_data_series(TickDataSeriesRequest::new("SHFE.rb2601", 1_000, 3_000)).unwrap();
    assert_eq!(series.get(0).unwrap().last_price, 101.0);
}
```

- [ ] **Step 2: Run and verify red**

Run:

```bash
rtk cargo test -p tqsdk-data --test history_series_tqbn_compaction
```

Expected: fail until scan and compaction behavior is implemented.

- [ ] **Step 3: Implement scan**

Scan behavior:

- Recursively scan only `.tqbn` files under `series/`.
- Ignore `.tqseries` files.
- Report `Readable` if prefix, metadata, blocks, and checksums decode.
- Report `IncompleteWrite` if prefix/block/record is truncated or checksum fails.
- Report `EmptySegment` for zero-byte `.tqbn`.
- Fill `symbol`, `duration_ns`, `rows`, `size_bytes`, `schema_version`, and `error`.

- [ ] **Step 4: Implement compaction**

Compaction behavior:

- Parse all readable records.
- Dedup rows by row id, last write wins.
- Merge coverage ranges.
- Rewrite to a temporary `.compact` path.
- `sync_all` temp file.
- Atomically rename temp over original.
- Keep file lock during rewrite.

- [ ] **Step 5: Verify green**

Run:

```bash
rtk cargo test -p tqsdk-data --test history_series_tqbn_corruption
rtk cargo test -p tqsdk-data --test history_series_tqbn_compaction
```

Expected: both pass.

---

## Task 11: Update Data/Backtest Integration Contracts

**Files:**

- Modify: `crates/tqsdk-data/examples/api_contract_s30_history_series_cache.rs`
- Modify: `crates/tqsdk-data/README.md`
- Modify: `docs/architecture/api-data.md`
- Modify: `docs/architecture/crate-boundaries.md`

- [ ] **Step 1: Update example expectations**

In `api_contract_s30_history_series_cache.rs`, assert:

```rust
assert_eq!(cache.format_id(), "tqsdk.tqbn.v1");
assert!(cache.tick_series_path("SHFE.rb2601").ends_with("tick.tqbn"));
```

- [ ] **Step 2: Update README**

Replace `.tqseries` wording with:

```markdown
`HistorySeriesCache::open(root_dir)` uses the canonical TQBN v1 history cache format.
TQBN is a tqsdk-specific DBN-like binary format with fixed-width records,
fixed-point price storage, self-describing metadata, explicit coverage records,
and forward-compatible record lengths.
```

- [ ] **Step 3: Run contract build**

Run:

```bash
rtk cargo check -p tqsdk-data --example api_contract_s30_history_series_cache
```

Expected: exit 0.

---

## Task 12: Full Verification Matrix

**Files:** no new files unless failures require targeted fixes.

- [ ] **Step 1: Run targeted TQBN tests**

Run:

```bash
rtk cargo test -p tqsdk-data --test history_series_tqbn_format
rtk cargo test -p tqsdk-data --test history_series_tqbn_store
rtk cargo test -p tqsdk-data --test history_series_tqbn_corruption
rtk cargo test -p tqsdk-data --test history_series_tqbn_compaction
```

Expected: all pass.

- [ ] **Step 2: Run existing cache/backtest tests**

Run:

```bash
rtk cargo test -p tqsdk-data --test history_series_cache
rtk cargo test -p tqsdk-data --test history_series_single_file_store
rtk cargo test -p tqsdk-task --test history_tick_replay
```

Expected: all pass.

- [ ] **Step 3: Run crate-wide data verification**

Run:

```bash
rtk cargo test -p tqsdk-data
rtk cargo check -p tqsdk-data --no-default-features
rtk cargo clippy -p tqsdk-data --all-targets -- -D warnings
rtk cargo fmt --all --check
rtk git diff --check
```

Expected: all pass.

- [ ] **Step 4: Run impact detection**

Run:

```bash
rtk gitnexus detect-changes -r /Users/joeslee/Projects/GitHub/tqsdk-rust
```

Expected: low or understood medium risk, affected flows limited to history cache/data/backtest paths.

---

## Task 13: Optional Benchmark and Compression Gate

**Files:**

- Create: `crates/tqsdk-data/benches/history_series_tqbn.rs`
- Modify: `crates/tqsdk-data/Cargo.toml` only if adding a benchmark dependency is accepted.

- [ ] **Step 1: Add benchmark scenarios**

Benchmark:

- write 1M tick rows
- read 1 trading-day tick range
- scan 100 symbol files
- compact duplicate appends
- compare disk bytes for current TQBN uncompressed against previous `.tqseries` fixture if available

- [ ] **Step 2: Decide zstd**

Only add zstd if benchmarks show disk size is a material issue and read/write overhead is acceptable. If added, gate it internally first:

```toml
zstd = { version = "0.13", optional = true }
```

Feature:

```toml
tqbn-zstd = ["dep:zstd"]
```

Do not make zstd a public user-facing storage selection API.

---

## Acceptance Criteria

- `HistorySeriesCache::open` uses TQBN by default.
- `format_id()` returns `tqsdk.tqbn.v1`.
- Public `tqsdk_data` does not re-export TQBN record/header/metadata types.
- `DataClientBuilder` cache behavior is unchanged from the caller perspective.
- `BacktestTickCache` continues to be tick-only and does not expose generic store handles.
- `HistoryTickReplayStream` continues to require complete cache coverage.
- `.tqseries` files are ignored or reported as legacy ignored; they are not migrated.
- All scalar on-disk fields are little-endian.
- Prices are fixed-point on disk and round-trip to public `f64` models.
- Unknown record types are skipped by `length_words`.
- Known short records fail with a typed error.
- Known longer records decode known prefix and skip trailing bytes.
- Truncated writes are reported by `scan()` and rejected by readers.
- `enforce_limits(None, None)` compacts duplicate row appends.

## Risk Register

- **Fixed-point overflow:** Price and amount conversion can overflow. Mitigation: explicit overflow errors and tests for large values.
- **Floating round-trip differences:** Public structs use `f64`; fixed conversion can change representation. Mitigation: exact tests for common tick sizes and tolerance tests for non-round values.
- **Header overhead:** Per-record header increases size. Mitigation: benchmark before adding compression; keep one-file-per-series locality for v1.
- **Unsafe layout temptation:** `repr(C)` enables zero-copy but unsafe transmute is risky. Mitigation: first implementation can decode little-endian fields explicitly; only introduce bytemuck/unsafe after layout tests and benchmark evidence.
- **Dirty tree interactions:** Current branch has many unrelated changes. Mitigation: use targeted diffs and never revert unrelated files.
- **Naming churn:** `SERIES_FILE_HISTORY_SERIES_FORMAT_ID` becomes semantically stale. Mitigation: switch value first, then rename constant in a small follow-up once tests are green.

## Execution Notes

- Before editing any Rust symbol, run GitNexus impact for that symbol.
- Use TDD for each task that changes behavior.
- Use `rtk` prefix for all shell commands.
- Use `apply_patch` for manual edits.
- Do not commit unless the user explicitly asks for commits; if committing later, stage only files touched by the current task.
