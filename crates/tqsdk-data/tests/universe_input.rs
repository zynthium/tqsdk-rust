use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use tqsdk_data::{UniverseInput, UniverseSourceError, UniverseSpec};

static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

struct TempDirectory(PathBuf);

impl TempDirectory {
    fn new() -> Self {
        let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("tqsdk-universe-input-{}-{id}", std::process::id()));
        fs::create_dir_all(&path).expect("create temporary test directory");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn symbol_files_expand_once_into_sorted_deduplicated_symbols_and_path_free_identity() {
    let first_dir = TempDirectory::new();
    let second_dir = TempDirectory::new();
    let contents = b"SHFE.au2506,DCE.m2509\nKQ.m@SHFE.au\nSHFE.au2506\n";
    let first_path = first_dir.path().join("symbols-a.txt");
    let second_path = second_dir.path().join("renamed.txt");
    fs::write(&first_path, contents).expect("write first symbol file");
    fs::write(&second_path, contents).expect("write second symbol file");

    let first = UniverseInput::new(None)
        .universe_symbol_file(&first_path)
        .expand()
        .expect("expand first file");
    let second = UniverseInput::new(None)
        .universe_symbol_file(&second_path)
        .expand()
        .expect("expand renamed file");

    assert_eq!(
        first.expanded_symbols(),
        &["DCE.m2509", "KQ.m@SHFE.au", "SHFE.au2506"]
    );
    assert_eq!(first.input_sources_sha256(), second.input_sources_sha256());
    assert_ne!(
        first.source_files()[0].path(),
        second.source_files()[0].path()
    );
    assert_eq!(
        first.source_files()[0].raw_content_sha256(),
        second.source_files()[0].raw_content_sha256()
    );
}

#[test]
fn universe_input_combines_an_optional_spec_with_repeatable_symbol_files() {
    let directory = TempDirectory::new();
    let first_path = directory.path().join("one.txt");
    let second_path = directory.path().join("two.txt");
    fs::write(&first_path, "SHFE.au2506\n").expect("write first file");
    fs::write(&second_path, "DCE.m2509\n").expect("write second file");
    let spec =
        UniverseSpec::parse_v2("index:all;!symbol:SHFE.au2506").expect("valid V2 expression");

    let expanded = UniverseInput::new(Some(spec.clone()))
        .universe_symbol_files([&first_path, &second_path])
        .expand()
        .expect("expand universe input");

    assert_eq!(expanded.spec(), Some(&spec));
    assert_eq!(expanded.expanded_symbols(), &["DCE.m2509", "SHFE.au2506"]);
    assert_eq!(expanded.source_files().len(), 2);
}

#[test]
fn invalid_symbol_file_reports_path_and_content_hash_without_contents() {
    let directory = TempDirectory::new();
    let path = directory.path().join("invalid.txt");
    fs::write(&path, "SHFE.au2506,,DCE.m2509").expect("write invalid file");

    let error = UniverseInput::new(None)
        .universe_symbol_file(&path)
        .expand()
        .expect_err("empty configured symbol must fail");

    match error {
        UniverseSourceError::InvalidSymbolFile {
            path: error_path,
            raw_content_sha256,
            ..
        } => {
            assert_eq!(error_path, path);
            assert!(raw_content_sha256.starts_with("sha256:"));
        }
        other => panic!("unexpected source error: {other}"),
    }
}
