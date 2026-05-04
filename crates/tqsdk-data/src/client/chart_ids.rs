use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_HISTORY_CHART_ID: AtomicU64 = AtomicU64::new(1);

pub(super) fn next_history_chart_sequence() -> u64 {
    NEXT_HISTORY_CHART_ID.fetch_add(1, Ordering::Relaxed)
}

pub(super) fn sanitize_chart_token(raw: &str) -> String {
    raw.chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
}
