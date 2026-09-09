use super::*;
use crate::history_container::tests::Fixture;
use std::fs::OpenOptions;

fn sample(datetime: i64) -> Tick {
    Tick {
        id: i64::MIN,
        datetime,
        last_price: -0.0,
        average: f64::from_bits(0x7ff8_0000_0000_0123),
        highest: f64::INFINITY,
        lowest: f64::NEG_INFINITY,
        ask_price1: 1.0,
        ask_volume1: -1,
        bid_price1: -1.0,
        bid_volume1: 2,
        ask_price2: 2.0,
        ask_volume2: -2,
        bid_price2: -2.0,
        bid_volume2: 3,
        ask_price3: 3.0,
        ask_volume3: -3,
        bid_price3: -3.0,
        bid_volume3: 4,
        ask_price4: 4.0,
        ask_volume4: -4,
        bid_price4: -4.0,
        bid_volume4: 5,
        ask_price5: 5.0,
        ask_volume5: -5,
        bid_price5: -5.0,
        bid_volume5: 6,
        volume: i64::MAX,
        amount: f64::from_bits(1),
        open_interest: i64::MIN,
        epoch: Some(i64::MIN),
    }
}

fn index() -> Index<u8> {
    Index::new(
        Identity {
            symbol: "TEST.tick".into(),
            kind: SeriesKind::Tick,
            partition_scheme: 1,
            pack_range: Some((0, 1000)),
            metadata_schema: 1,
        },
        vec![7],
    )
}

fn extent(start_ns: i64, end_ns: i64, slices: Vec<Slice>) -> Extent {
    Extent {
        start_ns,
        end_ns,
        logical_partition: "day-1".into(),
        metadata: 0,
        finality: Finality::Final,
        slices,
    }
}

fn write(fixture: &Fixture, rows: &[Tick]) -> (File, Index<u8>) {
    let block = encode(rows).unwrap();
    let mut index = index();
    index.extents.push(extent(0, 100, vec![block.slice(0)]));
    create(&fixture.path(), index, &[block]).unwrap();
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(fixture.path())
        .unwrap();
    let index = load(&mut file, SeriesKind::Tick, None).unwrap();
    (file, index)
}

#[test]
fn first_row_golden_covers_every_field_and_ieee_bits() {
    let row = sample(12);
    let expected: [u64; WORDS] = [
        0x8000_0000_0000_0000,
        12,
        0x8000_0000_0000_0000,
        0x7ff8_0000_0000_0123,
        0x7ff0_0000_0000_0000,
        0xfff0_0000_0000_0000,
        0x3ff0_0000_0000_0000,
        u64::MAX,
        0xbff0_0000_0000_0000,
        2,
        0x4000_0000_0000_0000,
        u64::MAX - 1,
        0xc000_0000_0000_0000,
        3,
        0x4008_0000_0000_0000,
        u64::MAX - 2,
        0xc008_0000_0000_0000,
        4,
        0x4010_0000_0000_0000,
        u64::MAX - 3,
        0xc010_0000_0000_0000,
        5,
        0x4014_0000_0000_0000,
        u64::MAX - 4,
        0xc014_0000_0000_0000,
        6,
        0x7fff_ffff_ffff_ffff,
        1,
        0x8000_0000_0000_0000,
        1,
        0x8000_0000_0000_0000,
    ];
    let payload = encode_payload(&[row]).unwrap();
    let mut golden = b"TX01\x01\x00\x00\x00".to_vec();
    for word in expected {
        golden.extend_from_slice(&word.to_le_bytes());
    }
    assert_eq!(payload, golden);
    assert_eq!(
        words(&Cursor::new(&golden, 1).unwrap().next().unwrap().unwrap()),
        expected
    );
}

#[test]
fn xor_roundtrip_preserves_every_word_and_optional_epoch() {
    let mut source = Vec::new();
    let mut seed = 0xd4a2_8471_bac3_0139_u64;
    for n in 0..MAX_ROWS {
        let mut w = std::array::from_fn(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        });
        w[29] = u64::from(n % 3 != 0);
        w[30] = if n % 3 == 0 {
            0
        } else if n % 3 == 1 {
            i64::MIN as u64
        } else {
            i64::MAX as u64
        };
        source.push(from_words(&w).unwrap());
    }
    let payload = encode_payload(&source).unwrap();
    let mut cursor = Cursor::new(&payload, source.len()).unwrap();
    for row in &source {
        assert_eq!(words(&cursor.next().unwrap().unwrap()), words(row));
    }
    assert!(cursor.next().unwrap().is_none());
    assert!(valid_length(source.len() as u64, payload.len() as u64));
    source.push(sample(1));
    assert!(encode_payload(&source).is_err());
    assert!(encode_payload(&[]).is_err());
}

#[test]
fn identical_rows_need_only_a_change_mask_and_keep_duplicate_timestamps() {
    let rows = vec![sample(10); MAX_ROWS];
    let payload = encode_payload(&rows).unwrap();
    assert_eq!(payload.len(), FIRST_BYTES + (MAX_ROWS - 1) * 4);
    let fixture = Fixture::new("tick-duplicates");
    let (mut file, index) = write(&fixture, &rows);
    let decoded = read(&mut file, &index, 0, None).unwrap();
    assert_eq!(decoded.len(), MAX_ROWS);
    assert!(decoded.iter().all(|row| words(row) == words(&rows[0])));
    #[cfg(feature = "tqbn-zstd")]
    assert_eq!(index.blocks[0].compression, Compression::Zstd);
}

fn decode_error(payload: &[u8], count: usize) -> bool {
    let Ok(mut cursor) = Cursor::new(payload, count) else {
        return true;
    };
    loop {
        match cursor.next() {
            Ok(Some(_)) => {}
            Ok(None) => return false,
            Err(_) => return true,
        }
    }
}

#[test]
fn corrupt_headers_masks_varints_epoch_and_trailing_bytes_fail_closed() {
    let one = encode_payload(&[sample(10)]).unwrap();
    for end in 0..one.len() {
        assert!(decode_error(&one[..end], 1));
    }
    let mut bad = one.clone();
    bad[0] ^= 1;
    assert!(decode_error(&bad, 1));
    assert!(decode_error(&one, 2));
    let mut bad = one.clone();
    bad[4..8].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(decode_error(&bad, 1));
    for (tag, value) in [(2_u64, 0_u64), (0, 1)] {
        let mut bad = one.clone();
        bad[8 + 29 * 8..8 + 30 * 8].copy_from_slice(&tag.to_le_bytes());
        bad[8 + 30 * 8..8 + 31 * 8].copy_from_slice(&value.to_le_bytes());
        assert!(decode_error(&bad, 1));
    }
    let two = encode_payload(&[sample(10), sample(10)]).unwrap();
    let mut bad = two.clone();
    bad[FIRST_BYTES..FIRST_BYTES + 4].copy_from_slice(&(1_u32 << 31).to_le_bytes());
    assert!(decode_error(&bad, 2));
    for varint in [
        vec![0],
        vec![0x80, 0],
        vec![0xff; 10],
        vec![0x80; 10],
        vec![0x80],
    ] {
        let mut bad = two.clone();
        bad[FIRST_BYTES..FIRST_BYTES + 4].copy_from_slice(&1_u32.to_le_bytes());
        bad.extend_from_slice(&varint);
        assert!(decode_error(&bad, 2));
    }
    let mut trailing = two;
    trailing.push(0);
    let mut cursor = Cursor::new(&trailing, 2).unwrap();
    assert!(cursor.next().unwrap().is_some());
    assert!(cursor.next().is_err());
}

#[test]
fn quota_counts_tick_objects_and_rejects_before_payload_io() {
    let fixture = Fixture::new("tick-quota");
    let rows = vec![sample(10); 128];
    let (mut file, index) = write(&fixture, &rows);
    let block = &index.blocks[0];
    let expected = (block.len + block.decoded_len) as usize
        + rows.len() * std::mem::size_of::<Tick>()
        + block.decoder_workspace_bytes();
    assert_eq!(block.read_allocation_bytes().unwrap(), expected);
    file.seek(SeekFrom::Start(1)).unwrap();
    assert!(matches!(
        read(&mut file, &index, 0, Some(expected - 1)),
        Err(DataError::CollectLimitExceeded { .. })
    ));
    assert_eq!(file.stream_position().unwrap(), 1);
    assert_eq!(
        read(&mut file, &index, 0, Some(expected)).unwrap().len(),
        rows.len()
    );
    let mut invalid_index = index;
    invalid_index.blocks[0].rows = u64::MAX;
    assert!(invalid_index.validate(invalid_index.committed_len).is_err());
    assert!(read(&mut file, &invalid_index, 0, None).is_err());
}

#[test]
fn maximum_row_length_boundaries_are_checked_before_payload_io() {
    let fixture = Fixture::new("tick-length-bounds");
    let (mut file, index) = write(&fixture, &vec![sample(10); MAX_ROWS]);
    let min = FIRST_BYTES as u64 + (MAX_ROWS as u64 - 1) * 4;
    let max = FIRST_BYTES as u64 + (MAX_ROWS as u64 - 1) * (4 + WORDS as u64 * 10);
    for (bytes, valid) in [(min - 1, false), (min, true), (max, true), (max + 1, false)] {
        let mut candidate = index.clone();
        let block = &mut candidate.blocks[0];
        block.len = bytes;
        block.decoded_len = bytes;
        block.compression = Compression::None;
        let index_offset = block.offset + block.len;
        assert_eq!(
            candidate.validate(index_offset).is_ok(),
            valid,
            "length={bytes}"
        );
        file.seek(SeekFrom::Start(1)).unwrap();
        let result = read(&mut file, &candidate, 0, if valid { Some(0) } else { None });
        if valid {
            assert!(matches!(
                result,
                Err(DataError::CollectLimitExceeded { .. })
            ));
        } else {
            assert!(matches!(result, Err(DataError::InvalidResponse(_))));
        }
        assert_eq!(file.stream_position().unwrap(), 1);
    }
}

#[test]
fn delta_epoch_state_is_validated_before_returning_the_row() {
    for xor in [1, 3] {
        let mut payload = encode_payload(&[sample(10), sample(10)]).unwrap();
        // Original epoch is (1, i64::MIN). Changing just its presence word
        // produces either (0, nonzero) or (2, nonzero), both illegal.
        payload[FIRST_BYTES..FIRST_BYTES + 4].copy_from_slice(&(1_u32 << 29).to_le_bytes());
        payload.push(xor);
        let mut cursor = Cursor::new(&payload, 2).unwrap();
        assert!(cursor.next().unwrap().is_some());
        assert!(cursor.next().is_err());
    }
}

#[test]
fn indexed_tick_blocks_append_recover_and_keep_opened_generation() {
    let fixture = Fixture::new("tick-append");
    let original = [sample(10), sample(20)];
    let (mut old_file, old_index) = write(&fixture, &original);
    let mut writer = OpenOptions::new()
        .read(true)
        .write(true)
        .open(fixture.path())
        .unwrap();
    let mut next = load::<u8>(&mut writer, SeriesKind::Tick, None).unwrap();
    let block = encode(&[sample(100)]).unwrap();
    next.extents
        .push(extent(100, 200, vec![block.slice(next.blocks.len())]));
    append(&fixture.path(), &mut writer, next, &[block]).unwrap();
    let next = load::<u8>(&mut writer, SeriesKind::Tick, None).unwrap();
    assert_eq!(old_index.generation + 1, next.generation);
    writer.seek(SeekFrom::End(0)).unwrap();
    writer.write_all(b"uncommitted Tick tail").unwrap();
    writer.sync_all().unwrap();
    assert!(next.require_clean_tail(&writer).is_err());
    recover(&fixture.path(), &mut writer, &next).unwrap();
    next.require_clean_tail(&writer).unwrap();
    assert_eq!(read(&mut writer, &next, 1, None).unwrap()[0].datetime, 100);
    assert_eq!(
        read(&mut old_file, &old_index, 0, None)
            .unwrap()
            .iter()
            .map(words)
            .collect::<Vec<_>>(),
        original.iter().map(words).collect::<Vec<_>>()
    );
    let replacement = encode(&[sample(99)]).unwrap();
    let mut replacement_index = index();
    replacement_index
        .extents
        .push(extent(0, 100, vec![replacement.slice(0)]));
    create(&fixture.path(), replacement_index, &[replacement]).unwrap();
    assert_eq!(
        read(&mut old_file, &old_index, 0, None).unwrap()[0].datetime,
        10
    );
}

#[test]
fn tick_payload_verifies_indexed_id_bounds_and_order_witness() {
    for corrupt_order in [false, true] {
        let fixture = Fixture::new("tick-order-witness");
        let mut block = encode(&[sample(10), sample(20)]).unwrap();
        if corrupt_order {
            block.block.tick_order_strict = Some(true);
        } else {
            block.block.id_bounds = Some((i64::MIN + 1, i64::MIN + 1));
        }
        let mut index = index();
        index.extents.push(extent(0, 100, vec![block.slice(0)]));
        create(&fixture.path(), index, &[block]).unwrap();
        let mut file = File::open(fixture.path()).unwrap();
        let index = load::<u8>(&mut file, SeriesKind::Tick, None).unwrap();
        assert!(read(&mut file, &index, 0, None).is_err());
    }
}

#[test]
fn payload_index_mismatch_and_out_of_order_times_are_rejected() {
    assert!(encode(&[sample(20), sample(10)]).is_err());
    let fixture = Fixture::new("tick-corrupt");
    let (mut file, mut index) = write(&fixture, &[sample(10), sample(20)]);
    index.blocks[0].last_ns = 21;
    assert!(read(&mut file, &index, 0, None).is_err());
    index.blocks[0].last_ns = 20;
    file.seek(SeekFrom::Start(index.blocks[0].offset)).unwrap();
    file.write_all(b"bad!").unwrap();
    file.sync_all().unwrap();
    assert!(read(&mut file, &index, 0, None).is_err());
}
