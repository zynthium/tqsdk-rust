use std::collections::{BTreeMap, BTreeSet};

use super::ast::{UniverseSelectorSpec, UniverseSpec};
use super::parser::RawUniverseSpec;
use super::{UniverseSpecError, UniverseTarget, UniverseView};

pub(super) fn normalize(raw: RawUniverseSpec) -> Result<UniverseSpec, UniverseSpecError> {
    let mut includes = BTreeMap::<UniverseView, BTreeSet<UniverseTarget>>::new();
    let mut excludes = BTreeMap::<UniverseView, BTreeSet<UniverseTarget>>::new();
    let mut global_filters = BTreeSet::<UniverseTarget>::new();

    for clause in raw.clauses {
        match (clause.exclude, clause.view) {
            (false, Some(view)) => includes.entry(view).or_default().extend(clause.targets),
            (true, Some(view)) => excludes.entry(view).or_default().extend(clause.targets),
            (true, None) => global_filters.extend(clause.targets),
            (false, None) => unreachable!("the parser rejects positive global targets"),
        }
    }
    if includes.is_empty() {
        return Err(UniverseSpecError::MissingInclude);
    }

    for (view, targets) in &includes {
        if targets.contains(&UniverseTarget::All) && targets.len() > 1 {
            return Err(UniverseSpecError::MixedAll { view: *view });
        }
        if let Some(excluded_targets) = excludes.get(view) {
            if let Some(target) = targets.intersection(excluded_targets).next() {
                return Err(UniverseSpecError::ContradictorySelector {
                    view: *view,
                    target: target.clone(),
                });
            }
        }
    }

    Ok(UniverseSpec::from_normalized_parts(
        raw.mode,
        selector_specs(includes),
        selector_specs(excludes),
        global_filters.into_iter().collect(),
    ))
}

fn selector_specs(
    selectors: BTreeMap<UniverseView, BTreeSet<UniverseTarget>>,
) -> Vec<UniverseSelectorSpec> {
    selectors
        .into_iter()
        .map(|(view, targets)| UniverseSelectorSpec::new(view, targets.into_iter().collect()))
        .collect()
}
