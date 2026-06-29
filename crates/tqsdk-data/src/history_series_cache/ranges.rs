pub(crate) fn rangeset_difference(base: &[(i64, i64)], covered: &[(i64, i64)]) -> Vec<(i64, i64)> {
    let mut result = Vec::new();
    for &(start, end) in base {
        if start >= end {
            continue;
        }
        let mut cursor = start;
        for &(covered_start, covered_end) in covered {
            if covered_end <= cursor {
                continue;
            }
            if covered_start >= end {
                break;
            }
            if covered_start > cursor {
                result.push((cursor, covered_start.min(end)));
            }
            cursor = cursor.max(covered_end);
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
        if left_start >= left_end {
            continue;
        }
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
