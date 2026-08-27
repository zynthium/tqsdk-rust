//! Release-only conformance gate for externally supplied official daily data.
//!
//! The immutable packet is intentionally not checked into this repository.
//! Tag CI verifies `manifest.sha256` closes over every packet artifact before
//! invoking the ignored conformance test. This test verifies each artifact
//! hash again before it is deserialized.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Deserialize;
use sha2::{Digest, Sha256};
use tqsdk_core::Kline;
use tqsdk_data::{
    BacktestHistoryClient, BacktestHistoryEvent, BacktestHistoryMetadataCache,
    BacktestHistoryMetadataSnapshot, BacktestHistoryPolicy, BacktestHistoryRequest,
    BacktestHistoryRows, DailyKlineCache, DailyKlineCacheSnapshot,
};

const PACKET_ENV: &str = "TQSDK_DAILY_KLINE_GOLDEN_PACKET";
const REQUIRED_CATEGORIES: [&str; 3] = ["physical_night_holiday", "main_roll", "index"];
const REQUIRED_PERIODS: [&str; 4] = ["1d", "2d", "5d", "28d"];

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct GoldenManifest {
    schema_version: u32,
    tqsdk_python_commit: String,
    tqsdk_python_version: String,
    cases: Vec<GoldenCase>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct GoldenCase {
    name: String,
    category: String,
    symbol: String,
    start_ns: i64,
    end_ns: i64,
    source_1d: GoldenArtifact,
    expected: BTreeMap<String, GoldenArtifact>,
    metadata: Option<GoldenArtifact>,
    main_roll: Option<MainRollEvidence>,
    physical_night_holiday: Option<PhysicalNightHolidayEvidence>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct GoldenArtifact {
    path: String,
    sha256: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct MainRollEvidence {
    transition_ns: i64,
    from_underlying: String,
    to_underlying: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct PhysicalNightHolidayEvidence {
    night_session_row_datetime_ns: i64,
    holiday_gap_start_ns: i64,
    holiday_gap_end_ns: i64,
    holiday_before_row_datetime_ns: i64,
    holiday_after_row_datetime_ns: i64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct KlineRows {
    rows: Vec<Kline>,
}

#[test]
fn golden_manifest_rejects_category_labels_without_required_symbol_semantics() {
    let mut manifest = manifest_with_required_categories();
    manifest
        .cases
        .iter_mut()
        .find(|case| case.category == "main_roll")
        .expect("main_roll fixture")
        .symbol = "SHFE.au2402".to_string();

    let error =
        validate_manifest(&manifest).expect_err("physical symbol must not pass as main_roll");
    assert!(
        error.contains("main_roll symbol must start with KQ.m@"),
        "{error}"
    );
}

#[test]
fn golden_manifest_requires_an_explicit_main_roll_transition_declaration() {
    let mut manifest = manifest_with_required_categories();
    manifest
        .cases
        .iter_mut()
        .find(|case| case.category == "main_roll")
        .expect("main_roll fixture")
        .main_roll = None;

    let error = validate_manifest(&manifest)
        .expect_err("main_roll category must not pass without transition evidence");
    assert!(
        error.contains("main_roll must declare its underlying transition"),
        "{error}"
    );
}

#[test]
fn golden_artifact_hash_is_checked_before_json_deserialization() {
    let root = temp_dir("artifact-hash");
    std::fs::create_dir_all(&root).expect("create packet root");
    std::fs::write(root.join("source.json"), b"not valid JSON").expect("write packet artifact");
    let artifact = GoldenArtifact {
        path: "source.json".to_string(),
        sha256: "0".repeat(64),
    };

    let error = load_json::<KlineRows>(root.as_path(), &artifact)
        .expect_err("hash mismatch must be reported before JSON parsing");
    assert!(error.contains("sha256 mismatch"), "{error}");
}

#[tokio::test]
#[ignore = "requires immutable official packet in TQSDK_DAILY_KLINE_GOLDEN_PACKET"]
async fn external_official_daily_packet_matches_native_and_local_multi_day_klines() {
    let packet_root = PathBuf::from(
        std::env::var(PACKET_ENV).unwrap_or_else(|_| panic!("{PACKET_ENV} must be set")),
    );
    let manifest_artifact = GoldenArtifact {
        path: "manifest.json".to_string(),
        // Tag CI verifies this file through manifest.sha256 before the test;
        // artifact entries in manifest cannot self-authenticate manifest.json.
        sha256: String::new(),
    };
    let manifest_bytes = read_packet_file(packet_root.as_path(), &manifest_artifact, false)
        .expect("read immutable daily golden manifest");
    let manifest: GoldenManifest =
        serde_json::from_slice(&manifest_bytes).expect("parse immutable daily golden manifest");
    validate_manifest(&manifest).expect("validate immutable daily golden manifest");

    for case in &manifest.cases {
        let metadata = case
            .metadata
            .as_ref()
            .map(|artifact| {
                load_json::<BacktestHistoryMetadataSnapshot>(packet_root.as_path(), artifact)
            })
            .transpose()
            .expect("load hash-verified golden metadata");
        let source = load_json::<KlineRows>(packet_root.as_path(), &case.source_1d)
            .expect("load hash-verified native 1d source");
        validate_case_evidence(case, metadata.as_ref(), source.rows.as_slice())
            .expect("validate golden category evidence");

        let cache_root = temp_dir(case.name.as_str());
        let snapshot = metadata
            .as_ref()
            .map_or_else(DailyKlineCacheSnapshot::cst_v1, |value| {
                DailyKlineCacheSnapshot::new(
                    value.schema_version,
                    value.snapshot_hash.clone(),
                    value.session.snapshot_hash(),
                )
                .expect("golden metadata has valid daily snapshot")
            });
        if let Some(metadata) = metadata {
            BacktestHistoryMetadataCache::open(&cache_root)
                .expect("open golden metadata cache")
                .store_snapshot(metadata)
                .expect("store golden metadata snapshot");
        }
        DailyKlineCache::open(&cache_root)
            .expect("open golden daily cache")
            .store_final_range(
                case.symbol.as_str(),
                case.start_ns,
                case.end_ns,
                &snapshot,
                source.rows.as_slice(),
            )
            .expect("store official native 1d golden data");
        let client = BacktestHistoryClient::builder(cache_root.clone())
            .policy(BacktestHistoryPolicy::CacheOnly)
            .blocking_workers(1)
            .build()
            .expect("build cache-only daily client");

        for period in REQUIRED_PERIODS {
            let expected = load_json::<KlineRows>(
                packet_root.as_path(),
                case.expected
                    .get(period)
                    .expect("validated golden expected period"),
            )
            .expect("load hash-verified golden expected rows");
            let days = period
                .trim_end_matches('d')
                .parse::<u64>()
                .expect("constant daily period");
            let actual = collect_klines(
                &client,
                BacktestHistoryRequest::kline(
                    1,
                    case.symbol.as_str(),
                    Duration::from_secs(days * 86_400),
                    case.start_ns,
                    case.end_ns,
                ),
            )
            .await;
            assert_kline_rows_equal(
                actual.as_slice(),
                expected.rows.as_slice(),
                format!("{} {period}", case.name).as_str(),
            );
        }
    }
}

fn validate_manifest(manifest: &GoldenManifest) -> std::result::Result<(), String> {
    if manifest.schema_version != 2 {
        return Err("golden manifest schema_version must be 2".to_string());
    }
    if !matches!(manifest.tqsdk_python_commit.len(), 40 | 64)
        || !manifest
            .tqsdk_python_commit
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err("golden manifest must pin a tqsdk-python commit".to_string());
    }
    if manifest.tqsdk_python_version.trim().is_empty() {
        return Err("golden manifest must pin a tqsdk-python version".to_string());
    }
    let categories = manifest
        .cases
        .iter()
        .map(|case| case.category.as_str())
        .collect::<BTreeSet<_>>();
    if !REQUIRED_CATEGORIES
        .iter()
        .all(|category| categories.contains(category))
    {
        return Err(
            "golden manifest must cover physical_night_holiday, main_roll, and index".to_string(),
        );
    }
    let required_periods = REQUIRED_PERIODS.into_iter().collect::<BTreeSet<_>>();
    for case in &manifest.cases {
        if case.name.trim().is_empty() || case.start_ns >= case.end_ns {
            return Err(format!(
                "golden case {} has invalid identity or range",
                case.name
            ));
        }
        let periods = case
            .expected
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        if periods != required_periods {
            return Err(format!(
                "golden case {} must contain exactly 1d/2d/5d/28d expected rows",
                case.name
            ));
        }
        validate_artifact(&case.source_1d)?;
        for artifact in case.expected.values() {
            validate_artifact(artifact)?;
        }
        if let Some(metadata) = &case.metadata {
            validate_artifact(metadata)?;
        }
        match case.category.as_str() {
            "main_roll" => {
                if !case.symbol.starts_with("KQ.m@") {
                    return Err(format!(
                        "golden case {} main_roll symbol must start with KQ.m@",
                        case.name
                    ));
                }
                if case.metadata.is_none() {
                    return Err(format!(
                        "golden case {} main_roll must include metadata",
                        case.name
                    ));
                }
                let Some(evidence) = &case.main_roll else {
                    return Err(format!(
                        "golden case {} main_roll must declare its underlying transition",
                        case.name
                    ));
                };
                validate_main_roll_evidence_bounds(case, evidence)?;
                if case.physical_night_holiday.is_some() {
                    return Err(format!(
                        "golden case {} main_roll must not declare physical night/holiday evidence",
                        case.name
                    ));
                }
                if case.main_roll.is_some() {
                    return Err(format!(
                        "golden case {} index must not declare main-roll evidence",
                        case.name
                    ));
                }
            }
            "index" => {
                if !case.symbol.starts_with("KQ.i@") {
                    return Err(format!(
                        "golden case {} index symbol must start with KQ.i@",
                        case.name
                    ));
                }
                if case.physical_night_holiday.is_some() {
                    return Err(format!(
                        "golden case {} index must not declare physical night/holiday evidence",
                        case.name
                    ));
                }
            }
            "physical_night_holiday" => {
                if case.symbol.starts_with("KQ.") || !case.symbol.contains('.') {
                    return Err(format!(
                        "golden case {} physical_night_holiday must use a physical contract",
                        case.name
                    ));
                }
                let Some(evidence) = &case.physical_night_holiday else {
                    return Err(format!(
                        "golden case {} physical_night_holiday must declare boundary evidence",
                        case.name
                    ));
                };
                validate_physical_evidence_bounds(case, evidence)?;
                if case.main_roll.is_some() {
                    return Err(format!(
                        "golden case {} physical_night_holiday must not declare main-roll evidence",
                        case.name
                    ));
                }
            }
            category => {
                return Err(format!(
                    "golden case {} has unsupported category {category}",
                    case.name
                ));
            }
        }
    }
    Ok(())
}

fn validate_case_evidence(
    case: &GoldenCase,
    metadata: Option<&BacktestHistoryMetadataSnapshot>,
    source_rows: &[Kline],
) -> std::result::Result<(), String> {
    if case.category == "main_roll" {
        let metadata = metadata.ok_or_else(|| {
            format!(
                "golden case {} main_roll metadata could not be loaded",
                case.name
            )
        })?;
        if metadata.logical_symbol != case.symbol {
            return Err(format!(
                "golden case {} main_roll metadata symbol does not match case",
                case.name
            ));
        }
        let evidence = case
            .main_roll
            .as_ref()
            .expect("manifest validation requires main-roll evidence");
        let has_transition = metadata.physical_segments.windows(2).any(|pair| {
            let left = &pair[0];
            let right = &pair[1];
            left.end_ns == evidence.transition_ns
                && right.start_ns == evidence.transition_ns
                && left.physical_symbol == evidence.from_underlying
                && right.physical_symbol == evidence.to_underlying
        });
        if !has_transition {
            return Err(format!(
                "golden case {} main_roll metadata must contain an in-range underlying transition",
                case.name
            ));
        }
    }
    if case.category == "physical_night_holiday" {
        let evidence = case
            .physical_night_holiday
            .as_ref()
            .expect("manifest validation requires physical evidence");
        let datetimes = source_rows
            .iter()
            .map(|row| row.datetime)
            .collect::<BTreeSet<_>>();
        if !datetimes.contains(&evidence.night_session_row_datetime_ns)
            || !datetimes.contains(&evidence.holiday_before_row_datetime_ns)
            || !datetimes.contains(&evidence.holiday_after_row_datetime_ns)
            || datetimes.iter().any(|datetime| {
                *datetime >= evidence.holiday_gap_start_ns
                    && *datetime < evidence.holiday_gap_end_ns
            })
        {
            return Err(format!(
                "golden case {} physical_night_holiday source rows do not prove declared boundaries",
                case.name
            ));
        }
    }
    Ok(())
}

fn validate_main_roll_evidence_bounds(
    case: &GoldenCase,
    evidence: &MainRollEvidence,
) -> std::result::Result<(), String> {
    if evidence.transition_ns <= case.start_ns
        || evidence.transition_ns >= case.end_ns
        || evidence.from_underlying.trim().is_empty()
        || evidence.to_underlying.trim().is_empty()
        || evidence.from_underlying == evidence.to_underlying
    {
        return Err(format!(
            "golden case {} main_roll has invalid transition evidence",
            case.name
        ));
    }
    Ok(())
}

fn validate_physical_evidence_bounds(
    case: &GoldenCase,
    evidence: &PhysicalNightHolidayEvidence,
) -> std::result::Result<(), String> {
    if evidence.night_session_row_datetime_ns < case.start_ns
        || evidence.night_session_row_datetime_ns >= case.end_ns
        || evidence.holiday_before_row_datetime_ns < case.start_ns
        || evidence.holiday_after_row_datetime_ns >= case.end_ns
        || !(evidence.holiday_before_row_datetime_ns < evidence.holiday_gap_start_ns
            && evidence.holiday_gap_start_ns < evidence.holiday_gap_end_ns
            && evidence.holiday_gap_end_ns <= evidence.holiday_after_row_datetime_ns)
    {
        return Err(format!(
            "golden case {} physical_night_holiday has invalid boundary evidence",
            case.name
        ));
    }
    Ok(())
}

fn validate_artifact(artifact: &GoldenArtifact) -> std::result::Result<(), String> {
    let path = Path::new(artifact.path.as_str());
    if artifact.path.is_empty()
        || path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, std::path::Component::ParentDir))
        || artifact.path.chars().any(char::is_control)
        || artifact.sha256.len() != 64
        || !artifact.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err("golden artifact must use a relative path and SHA-256 digest".to_string());
    }
    Ok(())
}

async fn collect_klines(
    client: &BacktestHistoryClient,
    request: BacktestHistoryRequest,
) -> Vec<Kline> {
    let mut run = client.query(request).await.expect("open cache-only query");
    let mut rows = Vec::new();
    while let Some(event) = run.next().await {
        match event {
            BacktestHistoryEvent::Chunk(chunk) => match chunk.rows {
                BacktestHistoryRows::Klines {
                    rows: chunk_rows, ..
                } => rows.extend(chunk_rows),
                BacktestHistoryRows::Ticks(_) => panic!("daily query returned Tick rows"),
            },
            BacktestHistoryEvent::RequestCompleted(_) => {}
            BacktestHistoryEvent::RequestFailed(failure) => {
                panic!("daily golden query failed: {failure:?}")
            }
        }
    }
    let report = run.finish().await;
    assert!(report.failed.is_empty(), "daily golden request failed");
    rows
}

fn assert_kline_rows_equal(actual: &[Kline], expected: &[Kline], label: &str) {
    assert_eq!(actual.len(), expected.len(), "{label}: row count");
    for (index, (actual, expected)) in actual.iter().zip(expected).enumerate() {
        assert_eq!(
            actual.datetime, expected.datetime,
            "{label} row {index}: datetime"
        );
        assert_eq!(
            actual.open.to_bits(),
            expected.open.to_bits(),
            "{label} row {index}: open"
        );
        assert_eq!(
            actual.high.to_bits(),
            expected.high.to_bits(),
            "{label} row {index}: high"
        );
        assert_eq!(
            actual.low.to_bits(),
            expected.low.to_bits(),
            "{label} row {index}: low"
        );
        assert_eq!(
            actual.close.to_bits(),
            expected.close.to_bits(),
            "{label} row {index}: close"
        );
        assert_eq!(
            actual.volume, expected.volume,
            "{label} row {index}: volume"
        );
        assert_eq!(
            actual.open_oi, expected.open_oi,
            "{label} row {index}: open_oi"
        );
        assert_eq!(
            actual.close_oi, expected.close_oi,
            "{label} row {index}: close_oi"
        );
    }
}

fn load_json<T: serde::de::DeserializeOwned>(
    root: &Path,
    artifact: &GoldenArtifact,
) -> std::result::Result<T, String> {
    let contents = read_packet_file(root, artifact, true)?;
    serde_json::from_slice(&contents).map_err(|error| {
        format!(
            "golden artifact {} is not valid JSON: {error}",
            artifact.path
        )
    })
}

fn read_packet_file(
    root: &Path,
    artifact: &GoldenArtifact,
    verify_hash: bool,
) -> std::result::Result<Vec<u8>, String> {
    if verify_hash {
        validate_artifact(artifact)?;
    }
    let path = packet_path(root, artifact.path.as_str())?;
    let contents = std::fs::read(&path)
        .map_err(|error| format!("cannot read golden artifact {}: {error}", path.display()))?;
    if verify_hash {
        let actual = format!("{:x}", Sha256::digest(contents.as_slice()));
        if actual != artifact.sha256.to_ascii_lowercase() {
            return Err(format!("golden artifact {} sha256 mismatch", artifact.path));
        }
    }
    Ok(contents)
}

fn packet_path(root: &Path, relative: &str) -> std::result::Result<PathBuf, String> {
    let path = Path::new(relative);
    if path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        return Err("golden packet paths must stay relative".to_string());
    }
    Ok(root.join(path))
}

fn manifest_with_required_categories() -> GoldenManifest {
    GoldenManifest {
        schema_version: 2,
        tqsdk_python_commit: "0".repeat(40),
        tqsdk_python_version: "test".to_string(),
        cases: vec![
            GoldenCase {
                name: "physical".to_string(),
                category: "physical_night_holiday".to_string(),
                symbol: "SHFE.au2402".to_string(),
                start_ns: 0,
                end_ns: 10,
                source_1d: fixture_artifact("physical-source.json"),
                expected: fixture_expected("physical"),
                metadata: None,
                main_roll: None,
                physical_night_holiday: Some(PhysicalNightHolidayEvidence {
                    night_session_row_datetime_ns: 1,
                    holiday_gap_start_ns: 3,
                    holiday_gap_end_ns: 5,
                    holiday_before_row_datetime_ns: 2,
                    holiday_after_row_datetime_ns: 5,
                }),
            },
            GoldenCase {
                name: "main".to_string(),
                category: "main_roll".to_string(),
                symbol: "KQ.m@SHFE.au".to_string(),
                start_ns: 0,
                end_ns: 10,
                source_1d: fixture_artifact("main-source.json"),
                expected: fixture_expected("main"),
                metadata: Some(fixture_artifact("main-metadata.json")),
                main_roll: Some(MainRollEvidence {
                    transition_ns: 5,
                    from_underlying: "SHFE.au2402".to_string(),
                    to_underlying: "SHFE.au2404".to_string(),
                }),
                physical_night_holiday: None,
            },
            GoldenCase {
                name: "index".to_string(),
                category: "index".to_string(),
                symbol: "KQ.i@SHFE.au".to_string(),
                start_ns: 0,
                end_ns: 10,
                source_1d: fixture_artifact("index-source.json"),
                expected: fixture_expected("index"),
                metadata: None,
                main_roll: None,
                physical_night_holiday: None,
            },
        ],
    }
}

fn fixture_expected(prefix: &str) -> BTreeMap<String, GoldenArtifact> {
    REQUIRED_PERIODS
        .into_iter()
        .map(|period| {
            (
                period.to_string(),
                fixture_artifact(format!("{prefix}-{period}.json").as_str()),
            )
        })
        .collect()
}

fn fixture_artifact(path: &str) -> GoldenArtifact {
    GoldenArtifact {
        path: path.to_string(),
        sha256: "0".repeat(64),
    }
}

fn temp_dir(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "tqsdk-data-daily-kline-{name}-{}-{nonce}",
        std::process::id()
    ))
}
