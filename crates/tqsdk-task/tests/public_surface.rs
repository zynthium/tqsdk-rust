use std::fs;
use std::path::{Path, PathBuf};

#[test]
fn active_docs_and_examples_use_task_family_paths_for_broad_foundations() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let repo_root = manifest_dir
        .parent()
        .and_then(Path::parent)
        .expect("repo root");
    let mut files = Vec::new();

    for path in [
        repo_root.join("README.md"),
        repo_root.join("crates/tqsdk/README.md"),
        repo_root.join("crates/tqsdk-task/README.md"),
    ] {
        files.push(path);
    }
    collect_files(&repo_root.join("docs/architecture"), &mut files);
    collect_files(&repo_root.join("crates/tqsdk/examples"), &mut files);
    collect_files(&repo_root.join("crates/tqsdk-task/examples"), &mut files);

    let forbidden = [
        "tqsdk_task::ReplayMarket",
        "tqsdk_task::StrategyReplay",
        "tqsdk_task::StrategyBacktest",
        "tqsdk_task::TqSim",
        "tqsdk_task::StrategyEnvironment",
        "tqsdk_task::StrategyDeployment",
        "tqsdk_task::StrategySupervisor",
        "tqsdk_task::TradingDesk",
        "tqsdk::advanced::task::ReplayMarket",
        "tqsdk::advanced::task::StrategyReplay",
        "tqsdk::advanced::task::StrategyBacktest",
        "tqsdk::advanced::task::TqSim",
        "use tqsdk_task::{ReplayMarket",
        "use tqsdk_task::{StrategyReplay",
        "use tqsdk_task::{StrategyBacktest",
        "use tqsdk_task::{TqSim",
        "use tqsdk_task::{StrategyEnvironment",
        "use tqsdk_task::{StrategyDeployment",
        "use tqsdk_task::{StrategySupervisor",
        "use tqsdk_task::{TradingDesk",
        "use tqsdk::advanced::task::{ReplayMarket",
        "use tqsdk::advanced::task::{StrategyReplay",
        "use tqsdk::advanced::task::{StrategyBacktest",
    ];

    let mut violations = Vec::new();
    for file in files {
        let source = fs::read_to_string(&file).unwrap_or_else(|err| {
            panic!("read {}: {err}", file.display());
        });
        for pattern in forbidden {
            if source.contains(pattern) {
                violations.push(format!("{} contains `{pattern}`", file.display()));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "broad task foundations should use family paths; root aliases are compatibility only:\n{}",
        violations.join("\n")
    );
}

fn collect_files(root: &Path, files: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(root).unwrap_or_else(|err| panic!("read {}: {err}", root.display())) {
        let entry = entry.expect("read directory entry");
        let path = entry.path();
        if path.is_dir() {
            collect_files(&path, files);
        } else if matches!(
            path.extension().and_then(|extension| extension.to_str()),
            Some("rs" | "md")
        ) {
            files.push(path);
        }
    }
}
