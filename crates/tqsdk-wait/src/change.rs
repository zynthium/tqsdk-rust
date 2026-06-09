#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::HashSet;

use tqsdk_core::{ChangeSet, ObjectKey, StatePath};

/// Lightweight handle that can be matched against the latest diff commit.
pub trait ChangeTrackedRef {
    fn object_key(&self) -> Option<ObjectKey>;
    fn state_path(&self) -> StatePath;

    fn visit_extra_state_paths(&self, _visit: &mut dyn FnMut(StatePath)) {}

    fn visit_field_state_paths(&self, _visit: &mut dyn FnMut(StatePath)) {}
}

pub(crate) fn matches_any(changes: &ChangeSet, target: &impl ChangeTrackedRef) -> bool {
    if let Some(key) = target.object_key()
        && changes.object_hits.contains(&key)
    {
        return true;
    }

    let state_path = target.state_path();
    if changes
        .path_hits
        .iter()
        .any(|path| path_matches(&state_path, path))
    {
        return true;
    }

    let mut matched = false;
    target.visit_extra_state_paths(&mut |target_path| {
        if !matched
            && changes
                .path_hits
                .iter()
                .any(|path| path_matches(&target_path, path))
        {
            matched = true;
        }
    });
    matched
}

pub(crate) fn matches_fields(
    changes: &ChangeSet,
    target: &impl ChangeTrackedRef,
    fields: &[&str],
) -> bool {
    let key = target.object_key();

    changes.field_hits.iter().any(|hit| {
        if !fields.iter().any(|field| *field == hit.field) {
            return false;
        }
        if key.as_ref().is_some_and(|key| hit.object == *key) {
            return true;
        }

        let mut matched = false;
        target.visit_field_state_paths(&mut |target_path| {
            if !matched && path_matches(&target_path, &hit.path) {
                matched = true;
            }
        });
        matched
    })
}

pub(crate) fn changed_quote_symbols(changes: &ChangeSet) -> Vec<&str> {
    let mut seen = HashSet::new();
    let mut symbols = Vec::new();

    for path in &changes.path_hits {
        let segments = path.segments();
        if segments.len() >= 2 && segments[0] == "quotes" {
            push_quote_symbol(&mut seen, &mut symbols, segments[1].as_str());
        }
    }

    for object in &changes.object_hits {
        if let ObjectKey::Quote { symbol } = object {
            push_quote_symbol(&mut seen, &mut symbols, symbol.as_str());
        }
    }

    symbols
}

fn push_quote_symbol<'a>(seen: &mut HashSet<&'a str>, symbols: &mut Vec<&'a str>, symbol: &'a str) {
    if seen.insert(symbol) {
        symbols.push(symbol);
    }
}

fn path_matches(target: &StatePath, changed: &StatePath) -> bool {
    let target_segments = target.segments();
    let changed_segments = changed.segments();

    target_segments.len() <= changed_segments.len()
        && target_segments
            .iter()
            .zip(changed_segments.iter())
            .all(|(left, right)| left == right)
}

#[cfg(test)]
mod tests {
    use tqsdk_core::{ChangeHit, Symbol};

    use super::*;

    #[test]
    fn matches_any_prefers_object_hits() {
        let target = tracked_quote();
        let changes = ChangeSet {
            object_hits: vec![target.object_key().expect("tracked quote has object key")],
            path_hits: Vec::new(),
            field_hits: Vec::new(),
        };

        assert!(matches_any(&changes, &target));
    }

    #[test]
    fn matches_any_accepts_changed_child_path() {
        let target = Tracked {
            object: None,
            path: StatePath::new(["quotes", "SHFE.au2606"]),
            extra_paths: Vec::new(),
            field_paths: Vec::new(),
        };
        let changes = ChangeSet {
            path_hits: vec![StatePath::new(["quotes", "SHFE.au2606", "last_price"])],
            object_hits: Vec::new(),
            field_hits: Vec::new(),
        };

        assert!(matches_any(&changes, &target));
    }

    #[test]
    fn matches_any_rejects_unrelated_parent_or_sibling_path() {
        let target = Tracked {
            object: None,
            path: StatePath::new(["quotes", "SHFE.au2606"]),
            extra_paths: Vec::new(),
            field_paths: Vec::new(),
        };
        let parent_change = ChangeSet {
            path_hits: vec![StatePath::new(["quotes"])],
            object_hits: Vec::new(),
            field_hits: Vec::new(),
        };
        let sibling_change = ChangeSet {
            path_hits: vec![StatePath::new(["quotes", "DCE.m2605", "last_price"])],
            object_hits: Vec::new(),
            field_hits: Vec::new(),
        };

        assert!(!matches_any(&parent_change, &target));
        assert!(!matches_any(&sibling_change, &target));
    }

    #[test]
    fn matches_fields_requires_object_key() {
        let target = Tracked {
            object: None,
            path: StatePath::new(["quotes", "SHFE.au2606"]),
            extra_paths: Vec::new(),
            field_paths: Vec::new(),
        };
        let changes = ChangeSet {
            path_hits: Vec::new(),
            object_hits: Vec::new(),
            field_hits: vec![ChangeHit::field(
                StatePath::new(["quotes", "SHFE.au2606"]),
                quote_key(),
                "last_price",
            )],
        };

        assert!(!matches_fields(&changes, &target, &["last_price"]));
    }

    #[test]
    fn matches_fields_accepts_tracked_field_path_without_object_key() {
        let target = Tracked {
            object: None,
            path: StatePath::new(["charts", "chart-1"]),
            extra_paths: vec![StatePath::new([
                "klines",
                "SHFE.au2606",
                "60000000000",
                "data",
            ])],
            field_paths: vec![StatePath::new([
                "klines",
                "SHFE.au2606",
                "60000000000",
                "data",
            ])],
        };
        let changes = ChangeSet {
            path_hits: Vec::new(),
            object_hits: Vec::new(),
            field_hits: vec![ChangeHit::field(
                StatePath::new(["klines", "SHFE.au2606", "60000000000", "data", "101"]),
                ObjectKey::Kline {
                    series: tqsdk_core::SeriesKey {
                        primary: Symbol::new("SHFE.au2606"),
                        secondary: vec![],
                        duration_ns: 60_000_000_000,
                        view_width: 0,
                        right_id: None,
                    },
                    bar_id: 101,
                },
                "close",
            )],
        };

        assert!(!matches_any(&changes, &target));
        assert!(matches_fields(&changes, &target, &["close"]));
        assert!(!matches_fields(&changes, &target, &["open"]));
    }

    #[test]
    fn matches_fields_matches_only_requested_fields() {
        let target = tracked_quote();
        let changes = ChangeSet {
            path_hits: Vec::new(),
            object_hits: Vec::new(),
            field_hits: vec![
                ChangeHit::field(
                    StatePath::new(["quotes", "SHFE.au2606"]),
                    quote_key(),
                    "last_price",
                ),
                ChangeHit::field(
                    StatePath::new(["quotes", "SHFE.au2606"]),
                    quote_key(),
                    "ask_price1",
                ),
            ],
        };

        assert!(matches_fields(&changes, &target, &["ask_price1"]));
        assert!(!matches_fields(&changes, &target, &["bid_price1"]));
    }

    fn tracked_quote() -> Tracked {
        Tracked {
            object: Some(quote_key()),
            path: StatePath::new(["quotes", "SHFE.au2606"]),
            extra_paths: Vec::new(),
            field_paths: Vec::new(),
        }
    }

    fn quote_key() -> ObjectKey {
        ObjectKey::Quote {
            symbol: Symbol::new("SHFE.au2606"),
        }
    }

    struct Tracked {
        object: Option<ObjectKey>,
        path: StatePath,
        extra_paths: Vec<StatePath>,
        field_paths: Vec<StatePath>,
    }

    impl ChangeTrackedRef for Tracked {
        fn object_key(&self) -> Option<ObjectKey> {
            self.object.clone()
        }

        fn state_path(&self) -> StatePath {
            self.path.clone()
        }

        fn visit_extra_state_paths(&self, visit: &mut dyn FnMut(StatePath)) {
            for path in &self.extra_paths {
                visit(path.clone());
            }
        }

        fn visit_field_state_paths(&self, visit: &mut dyn FnMut(StatePath)) {
            for path in &self.field_paths {
                visit(path.clone());
            }
        }
    }
}
