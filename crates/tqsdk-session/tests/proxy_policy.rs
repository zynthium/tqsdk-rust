#![cfg_attr(not(test), forbid(unsafe_code))]

use std::fs;
use std::path::{Path, PathBuf};

#[test]
fn reqwest_clients_use_explicit_http1_proxy_policy() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root should exist");
    let mut rust_files = Vec::new();
    collect_rust_files(&workspace.join("crates"), &mut rust_files);

    let mut violations = Vec::new();
    for path in rust_files {
        if path.ends_with("proxy_policy.rs") {
            continue;
        }
        let source = fs::read_to_string(&path).expect("rust source should be readable");
        let lines = source.lines().collect::<Vec<_>>();
        for (line_index, line) in lines.iter().enumerate() {
            let code = line.split("//").next().unwrap_or_default();
            if code.contains("reqwest::Client::new(") {
                violations.push(format!("{}:{}", path.display(), line_index + 1));
            }
            if code.contains("reqwest::Client::builder(")
                && !builder_chain_uses_http1(&lines[line_index..])
            {
                violations.push(format!("{}:{}", path.display(), line_index + 1));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "reqwest clients must explicitly retain the HTTP/1 transport policy:\n{}",
        violations.join("\n")
    );

    let shared_client =
        fs::read_to_string(workspace.join("crates/tqsdk-session/src/http_client.rs"))
            .expect("shared HTTP client source should be readable");
    assert!(shared_client.contains("TQSDK_HTTP_NO_PROXY"));
    assert!(shared_client.contains("builder = builder.no_proxy()"));
}

fn builder_chain_uses_http1(lines: &[&str]) -> bool {
    let chain = lines
        .iter()
        .take(8)
        .map(|line| line.split("//").next().unwrap_or_default())
        .collect::<Vec<_>>()
        .join("\n");
    let Some(http1_only) = chain.find(".http1_only()") else {
        return false;
    };
    chain
        .find(".build()")
        .is_none_or(|build| http1_only < build)
}

fn collect_rust_files(dir: &Path, output: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).expect("source directory should be readable") {
        let path = entry.expect("directory entry should be readable").path();
        if path.is_dir() {
            collect_rust_files(&path, output);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            output.push(path);
        }
    }
}
