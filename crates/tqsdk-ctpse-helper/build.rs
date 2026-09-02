use std::collections::BTreeSet;
use std::env;
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::{Component, Path, PathBuf};

use serde::Deserialize;
use sha2::{Digest as _, Sha256};
use zip::ZipArchive;

const BUNDLE_VERSION: &str = "1.2.0";

#[derive(Deserialize)]
struct BundleManifest {
    version: String,
    artifacts: std::collections::BTreeMap<String, BundleArtifact>,
}

#[derive(Deserialize)]
struct BundleArtifact {
    wheel: String,
    sha256: String,
    primary_library: String,
    files: Vec<String>,
}

struct ExtractedFile {
    relative_path: String,
    output_path: PathBuf,
    sha256: String,
}

fn main() {
    println!("cargo:rerun-if-env-changed=TQSDK_CTPSE_BUNDLE_DIR");
    println!("cargo:rerun-if-env-changed=TQSDK_CTPSE_REQUIRE_BUNDLE");

    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("Cargo sets OUT_DIR"));
    let generated = out_dir.join("ctpse_bundle.rs");
    let empty_bundle = || write_bundle_source(&generated, None, &[]);

    let bundle_dir = env::var_os("TQSDK_CTPSE_BUNDLE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(default_bundle_dir);
    let manifest_path = bundle_dir.join("manifest.json");
    println!("cargo:rerun-if-changed={}", manifest_path.display());

    if !manifest_path.is_file() {
        if required_bundle() {
            panic!(
                "TQSDK_CTPSE_REQUIRE_BUNDLE requires {}. Run the reviewed vendor workflow after confirming redistribution rights.",
                manifest_path.display()
            );
        }
        println!(
            "cargo:warning=tqsdk-ctpse bundle absent; helper supports only --library and session falls back to the Python collector"
        );
        empty_bundle().expect("write empty tqsdk-ctpse bundle source");
        return;
    }

    let result =
        bundle_manifest(&bundle_dir, &manifest_path, &out_dir).and_then(|(artifact, extracted)| {
            write_bundle_source(&generated, Some(&artifact), &extracted)
        });
    if let Err(error) = result {
        panic!("invalid tqsdk-ctpse bundle: {error}");
    }
}

fn default_bundle_dir() -> PathBuf {
    PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("Cargo sets CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("third_party/tqsdk-ctpse")
        .join(BUNDLE_VERSION)
}

fn required_bundle() -> bool {
    matches!(
        env::var("TQSDK_CTPSE_REQUIRE_BUNDLE")
            .as_deref()
            .map(str::trim)
            .map(str::to_ascii_lowercase),
        Ok(value) if matches!(value.as_str(), "1" | "true" | "yes" | "on")
    )
}

fn bundle_manifest(
    bundle_dir: &Path,
    manifest_path: &Path,
    out_dir: &Path,
) -> io::Result<(BundleArtifact, Vec<ExtractedFile>)> {
    let manifest = serde_json::from_slice::<BundleManifest>(&fs::read(manifest_path)?)
        .map_err(io::Error::other)?;
    if manifest.version != BUNDLE_VERSION {
        return Err(io::Error::other("unexpected tqsdk-ctpse bundle version"));
    }
    let target = env::var("TARGET").map_err(io::Error::other)?;
    let artifact = manifest
        .artifacts
        .get(&target)
        .ok_or_else(|| io::Error::other("bundle has no artifact for the Cargo target"))?;
    validate_artifact(artifact)?;

    let wheel_path = bundle_dir.join(&artifact.wheel);
    println!("cargo:rerun-if-changed={}", wheel_path.display());
    if !wheel_path.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "manifested tqsdk-ctpse wheel is missing",
        ));
    }
    if sha256_file(&wheel_path)? != artifact.sha256 {
        return Err(io::Error::other("wheel SHA-256 does not match manifest"));
    }

    let mut archive = ZipArchive::new(File::open(wheel_path)?).map_err(io::Error::other)?;
    let bundle_out = out_dir.join("ctpse-bundle");
    fs::create_dir_all(&bundle_out)?;
    let mut extracted = Vec::with_capacity(artifact.files.len());
    for entry in &artifact.files {
        let relative_path = bundle_relative_path(entry)?;
        let output_path = bundle_out.join(&relative_path);
        if let Some(parent) = output_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut source = archive.by_name(entry).map_err(io::Error::other)?;
        let mut destination = File::create(&output_path)?;
        io::copy(&mut source, &mut destination)?;
        destination.flush()?;
        extracted.push(ExtractedFile {
            relative_path: relative_path
                .to_str()
                .ok_or_else(|| io::Error::other("bundle path is not UTF-8"))?
                .to_owned(),
            sha256: sha256_file(&output_path)?,
            output_path,
        });
    }
    Ok((
        BundleArtifact {
            wheel: artifact.wheel.clone(),
            sha256: artifact.sha256.clone(),
            primary_library: bundle_relative_path(&artifact.primary_library)?
                .to_str()
                .ok_or_else(|| io::Error::other("primary library path is not UTF-8"))?
                .to_owned(),
            files: artifact.files.clone(),
        },
        extracted,
    ))
}

fn validate_artifact(artifact: &BundleArtifact) -> io::Result<()> {
    if !is_single_relative_file(&artifact.wheel)
        || !is_lowercase_sha256(&artifact.sha256)
        || artifact.files.is_empty()
        || !artifact
            .files
            .iter()
            .any(|file| file == &artifact.primary_library)
    {
        return Err(io::Error::other("manifest artifact has invalid metadata"));
    }
    let mut files = BTreeSet::new();
    for file in &artifact.files {
        bundle_relative_path(file)?;
        if !files.insert(file) {
            return Err(io::Error::other("manifest artifact has duplicate files"));
        }
    }
    Ok(())
}

fn is_single_relative_file(value: &str) -> bool {
    let path = Path::new(value);
    !value.is_empty()
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

fn bundle_relative_path(value: &str) -> io::Result<PathBuf> {
    let path = Path::new(value);
    let mut components = path.components();
    match components.next() {
        Some(Component::Normal(component)) if component == "tqsdk_ctpse" => {}
        Some(Component::Normal(component))
            if component.to_str().is_some_and(|component| {
                component.starts_with("tqsdk_ctpse-") && component.ends_with(".data")
            }) =>
        {
            match (components.next(), components.next()) {
                (Some(Component::Normal(purelib)), Some(Component::Normal(package)))
                    if purelib == "purelib" && package == "tqsdk_ctpse" => {}
                _ => {
                    return Err(io::Error::other(
                        "wheel data payload must be below purelib/tqsdk_ctpse/",
                    ));
                }
            }
        }
        _ => return Err(io::Error::other("bundle file must be below tqsdk_ctpse/")),
    }
    let relative = components.collect::<PathBuf>();
    if relative.as_os_str().is_empty()
        || !relative
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
    {
        return Err(io::Error::other("bundle file path is unsafe"));
    }
    Ok(relative)
}

fn is_lowercase_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn sha256_file(path: &Path) -> io::Result<String> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 8 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn write_bundle_source(
    generated: &Path,
    artifact: Option<&BundleArtifact>,
    extracted: &[ExtractedFile],
) -> io::Result<()> {
    let mut output = File::create(generated)?;
    let version = artifact.map_or(BUNDLE_VERSION, |_| BUNDLE_VERSION);
    writeln!(
        output,
        "pub(crate) const BUNDLE_VERSION: &str = {version:?};"
    )?;
    match artifact {
        Some(artifact) => {
            writeln!(
                output,
                "pub(crate) const PRIMARY_LIBRARY: Option<&str> = Some({:?});",
                artifact.primary_library
            )?;
            writeln!(
                output,
                "pub(crate) const BUNDLED_FILES: &[BundledFile] = &["
            )?;
            for file in extracted {
                writeln!(
                    output,
                    "    BundledFile {{ relative_path: {:?}, expected_sha256: {:?}, bytes: include_bytes!({:?}) }},",
                    file.relative_path, file.sha256, file.output_path
                )?;
            }
            writeln!(output, "];")?;
        }
        None => {
            writeln!(
                output,
                "pub(crate) const PRIMARY_LIBRARY: Option<&str> = None;"
            )?;
            writeln!(
                output,
                "pub(crate) const BUNDLED_FILES: &[BundledFile] = &[];"
            )?;
        }
    }
    Ok(())
}
