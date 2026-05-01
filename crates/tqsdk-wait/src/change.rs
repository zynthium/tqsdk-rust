#![cfg_attr(not(test), forbid(unsafe_code))]

use tqsdk_core::{ChangeSet, ObjectKey, StatePath};

/// Lightweight handle that can be matched against the latest diff commit.
pub trait ChangeTrackedRef {
    fn object_key(&self) -> Option<ObjectKey>;
    fn state_path(&self) -> StatePath;
}

pub(crate) fn matches_any(changes: &ChangeSet, target: &impl ChangeTrackedRef) -> bool {
    if let Some(key) = target.object_key()
        && changes.object_hits.contains(&key)
    {
        return true;
    }

    changes
        .path_hits
        .iter()
        .any(|path| path_matches(&target.state_path(), path))
}

pub(crate) fn matches_fields(
    changes: &ChangeSet,
    target: &impl ChangeTrackedRef,
    fields: &[&str],
) -> bool {
    let Some(key) = target.object_key() else {
        return false;
    };

    changes
        .field_hits
        .iter()
        .any(|hit| hit.object == key && fields.iter().any(|field| *field == hit.field))
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
    }

    impl ChangeTrackedRef for Tracked {
        fn object_key(&self) -> Option<ObjectKey> {
            self.object.clone()
        }

        fn state_path(&self) -> StatePath {
            self.path.clone()
        }
    }
}
