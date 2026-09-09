//! Two-pass, borrowed index decoding. Count arrays without constructing a JSON
//! tree, check the quota, then reserve each typed Vec exactly once.
//! The quota counts requested buffer/collection capacity, not allocator
//! bookkeeping, page rounding, or thread stacks.

use std::fmt;
use std::mem::size_of;

use serde::Deserialize;
use serde::de::{DeserializeOwned, DeserializeSeed, IgnoredAny, SeqAccess, Visitor};
use serde_json::value::RawValue;

use super::{
    ActiveRowRange, Block, Extent, Finality, Index, MAX_ENTRIES, Slice, check_allocation, invalid,
};
use crate::Result;

/// Implement only for fixed derived records with scalar/string fields and
/// nested fixed records. Allocating custom deserializers, maps and vectors
/// require their own counted decoder; they may not enter through this trait.
pub(crate) trait IndexMetadata: DeserializeOwned {}

impl IndexMetadata for crate::MinuteKlineCacheSnapshot {}
impl IndexMetadata for IgnoredAny {}
#[cfg(test)]
impl IndexMetadata for u8 {}

pub(super) fn base<M>(wire_bytes: usize) -> Result<usize> {
    // Input + owned strings + escaping scratch. RawValue borrows its source;
    // there is no intermediate JSON Value tree or growable reference vector.
    wire_bytes
        .checked_mul(4)
        .and_then(|bytes| bytes.checked_add(size_of::<Index<M>>()))
        .ok_or_else(|| invalid("index allocation overflow"))
}

#[derive(Deserialize)]
struct RawIndex<'a> {
    #[serde(borrow)]
    identity: &'a RawValue,
    #[serde(borrow)]
    metadata: &'a RawValue,
    #[serde(borrow)]
    extents: &'a RawValue,
    #[serde(borrow)]
    blocks: &'a RawValue,
}

#[derive(Deserialize)]
struct RawExtent<'a> {
    start_ns: i64,
    end_ns: i64,
    #[serde(borrow)]
    logical_partition: &'a RawValue,
    metadata: usize,
    finality: Finality,
    #[serde(borrow)]
    slices: &'a RawValue,
}

fn parse<'a, T: Deserialize<'a>>(raw: &'a RawValue) -> Result<T> {
    serde_json::from_str(raw.get()).map_err(|error| invalid(&error.to_string()))
}

struct ArrayScan<F>(F);
impl<'de, F> Visitor<'de> for ArrayScan<F>
where
    F: FnMut(&'de RawValue) -> Result<()>,
{
    type Value = usize;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded index array")
    }

    fn visit_seq<A: SeqAccess<'de>>(
        mut self,
        mut sequence: A,
    ) -> std::result::Result<usize, A::Error> {
        let mut count = 0;
        while let Some(raw) = sequence.next_element::<&'de RawValue>()? {
            count += 1;
            if count > MAX_ENTRIES {
                return Err(serde::de::Error::custom("index array cardinality exceeded"));
            }
            (self.0)(raw).map_err(serde::de::Error::custom)?;
        }
        Ok(count)
    }
}

fn scan<'a>(raw: &'a RawValue, item: impl FnMut(&'a RawValue) -> Result<()>) -> Result<usize> {
    let mut decoder = serde_json::Deserializer::from_str(raw.get());
    serde::Deserializer::deserialize_seq(&mut decoder, ArrayScan(item))
        .map_err(|error| invalid(&error.to_string()))
}

struct ExactArray<T, F> {
    count: usize,
    convert: F,
    marker: std::marker::PhantomData<T>,
}
impl<'de, T, F> DeserializeSeed<'de> for ExactArray<T, F>
where
    F: FnMut(&'de RawValue) -> Result<T>,
{
    type Value = Vec<T>;

    fn deserialize<D: serde::Deserializer<'de>>(
        self,
        decoder: D,
    ) -> std::result::Result<Vec<T>, D::Error> {
        decoder.deserialize_seq(self)
    }
}
impl<'de, T, F> Visitor<'de> for ExactArray<T, F>
where
    F: FnMut(&'de RawValue) -> Result<T>,
{
    type Value = Vec<T>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("an index array with its preflight count")
    }

    fn visit_seq<A: SeqAccess<'de>>(
        mut self,
        mut sequence: A,
    ) -> std::result::Result<Vec<T>, A::Error> {
        let mut values = Vec::new();
        values
            .try_reserve_exact(self.count)
            .map_err(serde::de::Error::custom)?;
        while let Some(raw) = sequence.next_element::<&'de RawValue>()? {
            if values.len() == self.count {
                return Err(serde::de::Error::custom("index changed after preflight"));
            }
            values.push((self.convert)(raw).map_err(serde::de::Error::custom)?);
        }
        if values.len() != self.count {
            return Err(serde::de::Error::custom("index changed after preflight"));
        }
        Ok(values)
    }
}

fn exact<'a, T>(
    raw: &'a RawValue,
    count: usize,
    convert: impl FnMut(&'a RawValue) -> Result<T>,
) -> Result<Vec<T>> {
    let mut decoder = serde_json::Deserializer::from_str(raw.get());
    ExactArray {
        count,
        convert,
        marker: std::marker::PhantomData,
    }
    .deserialize(&mut decoder)
    .map_err(|error| invalid(&error.to_string()))
}

pub(super) fn decode<M: IndexMetadata>(bytes: &[u8], limit: Option<usize>) -> Result<Index<M>> {
    check_allocation(base::<M>(bytes.len())?, limit)?;
    let raw: RawIndex<'_> =
        serde_json::from_slice(bytes).map_err(|error| invalid(&error.to_string()))?;
    let blocks = scan(raw.blocks, |_| Ok(()))?;
    let metadata = scan(raw.metadata, |_| Ok(()))?;
    let mut slices = 0_usize;
    let extents = scan(raw.extents, |raw| {
        let extent: RawExtent<'_> = parse(raw)?;
        slices = slices
            .checked_add(scan(extent.slices, |_| Ok(()))?)
            .filter(|count| *count <= MAX_ENTRIES)
            .ok_or_else(|| invalid("index slice cardinality exceeded"))?;
        Ok(())
    })?;
    let mut allocation_bytes = base::<M>(bytes.len())?;
    for (count, size) in [
        (blocks, size_of::<Block>()),
        (extents, size_of::<Extent>()),
        (slices, size_of::<Slice>()),
        (metadata, size_of::<M>()),
        (slices, size_of::<ActiveRowRange>()),
    ] {
        allocation_bytes = count
            .checked_mul(size)
            .and_then(|bytes| allocation_bytes.checked_add(bytes))
            .ok_or_else(|| invalid("index allocation overflow"))?;
    }
    check_allocation(allocation_bytes, limit)?;
    let identity = parse(raw.identity)?;
    let metadata = exact(raw.metadata, metadata, parse::<M>)?;
    let blocks = exact(raw.blocks, blocks, parse::<Block>)?;
    let extents = exact(raw.extents, extents, |raw| {
        let raw: RawExtent<'_> = parse(raw)?;
        let count = scan(raw.slices, |_| Ok(()))?;
        Ok(Extent {
            start_ns: raw.start_ns,
            end_ns: raw.end_ns,
            logical_partition: parse(raw.logical_partition)?,
            metadata: raw.metadata,
            finality: raw.finality,
            slices: exact(raw.slices, count, parse::<Slice>)?,
        })
    })?;
    Ok(Index {
        identity,
        metadata,
        extents,
        blocks,
        generation: 0,
        committed_len: 0,
        index_bytes: bytes.len(),
        allocation_bytes,
    })
}
