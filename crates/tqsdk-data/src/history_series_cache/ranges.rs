use tqsdk_core::{Kline, Tick};

use crate::error::{DataError, Result};

use super::{IdRange, MergeGroup};

pub(super) fn trim_last_datetime_range(ranges: &mut Vec<(i64, i64)>, width: i64) {
    if let Some(last) = ranges.last_mut() {
        last.1 = last.1.saturating_sub(width).max(last.0);
        if last.0 == last.1 {
            ranges.pop();
        }
    }
}

pub(crate) fn rangeset_difference(
    requested: &[(i64, i64)],
    cached: &[(i64, i64)],
) -> Vec<(i64, i64)> {
    let mut result = Vec::new();
    for &(start, end) in requested {
        if start >= end {
            continue;
        }
        let mut cursor = start;
        for &(cached_start, cached_end) in cached {
            if cached_end <= cursor {
                continue;
            }
            if cached_start >= end {
                break;
            }
            if cursor < cached_start {
                result.push((cursor, cached_start.min(end)));
            }
            cursor = cursor.max(cached_end);
            if cursor >= end {
                break;
            }
        }
        if cursor < end {
            result.push((cursor, end));
        }
    }
    result
}

pub(crate) fn rangeset_intersection(left: &[(i64, i64)], right: &[(i64, i64)]) -> Vec<(i64, i64)> {
    let mut result = Vec::new();
    for &(left_start, left_end) in left {
        for &(right_start, right_end) in right {
            let start = left_start.max(right_start);
            let end = left_end.min(right_end);
            if start < end {
                result.push((start, end));
            }
        }
    }
    result
}

pub(super) fn range_from_ids(ids: impl IntoIterator<Item = i64>) -> Result<(i64, i64)> {
    let mut min_id = None;
    let mut max_id = None;
    for id in ids {
        min_id = Some(min_id.map_or(id, |value: i64| value.min(id)));
        max_id = Some(max_id.map_or(id, |value: i64| value.max(id)));
    }
    let start = min_id
        .ok_or_else(|| DataError::InvalidResponse("history series segment is empty".to_string()))?;
    let end = max_id
        .and_then(|id: i64| id.checked_add(1))
        .ok_or_else(|| {
            DataError::InvalidResponse("history series segment id overflow".to_string())
        })?;
    Ok((start, end))
}

pub(super) fn build_merge_groups(ranges: &[IdRange]) -> Result<Vec<MergeGroup>> {
    if ranges.is_empty() {
        return Ok(Vec::new());
    }
    let mut groups = vec![vec![(ranges[0], ranges[0].1 - ranges[0].0)]];
    for index in 1..ranges.len() {
        let previous = ranges[index - 1];
        let current = ranges[index];
        if current.0 < previous.1 - 1 {
            return Err(DataError::InvalidResponse(
                "history series cache ranges overlap unexpectedly".to_string(),
            ));
        }
        if current.0 == previous.1 {
            groups
                .last_mut()
                .expect("merge group exists")
                .push((current, current.1 - current.0));
        } else if current.0 == previous.1 - 1 {
            if let Some(last_group) = groups.last_mut()
                && let Some(last_entry) = last_group.last_mut()
            {
                last_entry.1 = (previous.1 - 1) - previous.0;
            }
            groups
                .last_mut()
                .expect("merge group exists")
                .push((current, current.1 - current.0));
        } else {
            groups.push(vec![(current, current.1 - current.0)]);
        }
    }
    Ok(groups)
}

pub(super) fn dedup_klines(rows: Vec<Kline>) -> Vec<Kline> {
    let mut by_id = std::collections::BTreeMap::new();
    for row in rows {
        by_id.insert(row.id, row);
    }
    by_id.into_values().collect()
}

pub(super) fn dedup_ticks(rows: Vec<Tick>) -> Vec<Tick> {
    let mut by_id = std::collections::BTreeMap::new();
    for row in rows {
        by_id.insert(row.id, row);
    }
    by_id.into_values().collect()
}
