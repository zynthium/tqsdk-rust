#![deny(unsafe_op_in_unsafe_fn)]

use std::env;
use std::ffi::{OsString, c_char, c_int};
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Component, Path, PathBuf};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use libloading::{Library, Symbol};
use serde::Serialize;
use sha2::{Digest as _, Sha256};

const MAX_SYSTEM_INFO_BYTES: usize = 344;

struct BundledFile {
    relative_path: &'static str,
    expected_sha256: &'static str,
    bytes: &'static [u8],
}

include!(concat!(env!("OUT_DIR"), "/ctpse_bundle.rs"));

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HelperError {
    InvalidArguments,
    NoBundledLibrary,
    InsecureCache,
    Cache,
    LibraryLoad,
    MissingCollectorSymbol,
    CollectorFailed,
    InvalidCollectorOutput,
    Output,
}

impl std::fmt::Display for HelperError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::InvalidArguments => "invalid arguments",
            Self::NoBundledLibrary => "no embedded official tqsdk-ctpse library is available",
            Self::InsecureCache => "tqsdk-ctpse cache directory is not private",
            Self::Cache => "unable to materialize the tqsdk-ctpse library cache",
            Self::LibraryLoad => "unable to load the official tqsdk-ctpse library",
            Self::MissingCollectorSymbol => {
                "official tqsdk-ctpse library has no system-info collector"
            }
            Self::CollectorFailed => "official tqsdk-ctpse collector failed",
            Self::InvalidCollectorOutput => {
                "official tqsdk-ctpse collector returned invalid output"
            }
            Self::Output => "unable to write collector output",
        })
    }
}

impl std::error::Error for HelperError {}

#[derive(Debug)]
struct Arguments {
    library: Option<PathBuf>,
}

#[derive(Serialize)]
struct HelperOutput<'a> {
    client_system_info: &'a str,
}

/// Runs the helper protocol. The only success output on stdout is one JSON object.
pub fn run_from_env() -> Result<(), HelperError> {
    let Some(arguments) = parse_arguments(env::args_os().skip(1))? else {
        print_help();
        return Ok(());
    };
    let library = match arguments.library {
        Some(path) => canonical_library_path(path)?,
        None => materialize_bundled_library()?,
    };
    let system_info = collect_system_info(&library)?;
    let output = serde_json::to_string(&HelperOutput {
        client_system_info: &system_info,
    })
    .map_err(|_| HelperError::Output)?;
    println!("{output}");
    Ok(())
}

fn canonical_library_path(path: PathBuf) -> Result<PathBuf, HelperError> {
    if !path.is_absolute() {
        return Err(HelperError::InvalidArguments);
    }
    let path = fs::canonicalize(path).map_err(|_| HelperError::LibraryLoad)?;
    path.is_file()
        .then_some(path)
        .ok_or(HelperError::LibraryLoad)
}

fn parse_arguments(
    arguments: impl IntoIterator<Item = OsString>,
) -> Result<Option<Arguments>, HelperError> {
    let mut arguments = arguments.into_iter();
    let mut library = None;
    while let Some(argument) = arguments.next() {
        if argument == "--help" || argument == "-h" {
            if library.is_some() || arguments.next().is_some() {
                return Err(HelperError::InvalidArguments);
            }
            return Ok(None);
        }
        if argument == "--library" {
            let value = arguments.next().ok_or(HelperError::InvalidArguments)?;
            if library.replace(PathBuf::from(value)).is_some() {
                return Err(HelperError::InvalidArguments);
            }
            continue;
        }
        return Err(HelperError::InvalidArguments);
    }
    Ok(Some(Arguments { library }))
}

fn print_help() {
    println!("Usage: tqsdk-ctpse-helper [--library <official-ctpse-library>]");
}

fn collect_system_info(library_path: &Path) -> Result<String, HelperError> {
    let library = unsafe { Library::new(library_path) }.map_err(|_| HelperError::LibraryLoad)?;
    let function = collector_function(&library)?;
    let mut buffer = [0_u8; MAX_SYSTEM_INFO_BYTES];
    let mut length = c_int::try_from(buffer.len()).expect("344 fits in c_int");
    let status = unsafe { function(buffer.as_mut_ptr().cast::<c_char>(), &mut length) };
    if status != 0 {
        return Err(HelperError::CollectorFailed);
    }
    let length = usize::try_from(length).map_err(|_| HelperError::InvalidCollectorOutput)?;
    if !(1..=buffer.len()).contains(&length) {
        return Err(HelperError::InvalidCollectorOutput);
    }
    Ok(STANDARD.encode(&buffer[..length]))
}

type GetSystemInfo = unsafe extern "C" fn(*mut c_char, *mut c_int) -> c_int;

#[cfg(target_os = "linux")]
const SYSTEM_INFO_SYMBOLS: &[&[u8]] = &[b"_Z17CTP_GetSystemInfoPcRi\0", b"CTP_GetSystemInfo\0"];

#[cfg(all(target_os = "windows", target_pointer_width = "64"))]
const SYSTEM_INFO_SYMBOLS: &[&[u8]] = &[
    b"?CTP_GetSystemInfo@@YAHPEADAEAH@Z\0",
    b"CTP_GetSystemInfo\0",
];

#[cfg(all(target_os = "windows", target_pointer_width = "32"))]
const SYSTEM_INFO_SYMBOLS: &[&[u8]] =
    &[b"?CTP_GetSystemInfo@@YAHPADAAH@Z\0", b"CTP_GetSystemInfo\0"];

#[cfg(all(not(target_os = "linux"), not(target_os = "windows")))]
const SYSTEM_INFO_SYMBOLS: &[&[u8]] = &[b"_Z17CTP_GetSystemInfoPcRi\0", b"CTP_GetSystemInfo\0"];

fn collector_function(library: &Library) -> Result<Symbol<'_, GetSystemInfo>, HelperError> {
    for symbol in SYSTEM_INFO_SYMBOLS {
        if let Ok(function) = unsafe { library.get::<GetSystemInfo>(symbol) } {
            return Ok(function);
        }
    }
    Err(HelperError::MissingCollectorSymbol)
}

fn materialize_bundled_library() -> Result<PathBuf, HelperError> {
    let primary = PRIMARY_LIBRARY.ok_or(HelperError::NoBundledLibrary)?;
    if BUNDLED_FILES.is_empty() {
        return Err(HelperError::NoBundledLibrary);
    }
    let root = cache_root()?.join(bundle_fingerprint());
    ensure_private_directory(&root)?;
    for file in BUNDLED_FILES {
        materialize_file(&root, file)?;
    }
    let path = root.join(primary);
    if !path.is_file() {
        return Err(HelperError::Cache);
    }
    Ok(path)
}

fn cache_root() -> Result<PathBuf, HelperError> {
    if let Some(path) = env::var_os("TQSDK_CTPSE_CACHE_DIR") {
        return Ok(PathBuf::from(path)
            .join("tqsdk-rust")
            .join("ctpse")
            .join(BUNDLE_VERSION));
    }
    #[cfg(target_os = "windows")]
    let base = env::var_os("LOCALAPPDATA")
        .map(PathBuf::from)
        .unwrap_or_else(env::temp_dir);
    #[cfg(not(target_os = "windows"))]
    let base = env::var_os("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .or_else(|| env::var_os("HOME").map(|home| PathBuf::from(home).join(".cache")))
        .unwrap_or_else(env::temp_dir);
    Ok(base.join("tqsdk-rust").join("ctpse").join(BUNDLE_VERSION))
}

fn bundle_fingerprint() -> String {
    let mut hasher = Sha256::new();
    for file in BUNDLED_FILES {
        hasher.update(file.relative_path.as_bytes());
        hasher.update(file.expected_sha256.as_bytes());
    }
    format!("{:x}", hasher.finalize())
}

fn materialize_file(root: &Path, file: &BundledFile) -> Result<(), HelperError> {
    let relative = safe_relative_path(file.relative_path)?;
    let destination = root.join(relative);
    let parent = destination.parent().ok_or(HelperError::Cache)?;
    ensure_private_directory(parent)?;

    match fs::symlink_metadata(&destination) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            return Err(HelperError::InsecureCache);
        }
        Ok(_) if file_sha256(&destination)? == file.expected_sha256 => return Ok(()),
        Ok(_) => fs::remove_file(&destination).map_err(|_| HelperError::Cache)?,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(_) => return Err(HelperError::Cache),
    }

    let name = destination
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or(HelperError::Cache)?;
    let temporary = parent.join(format!(".{name}.{}.tmp", std::process::id()));
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .map_err(|_| HelperError::Cache)?;
    output
        .write_all(file.bytes)
        .map_err(|_| HelperError::Cache)?;
    output.sync_all().map_err(|_| HelperError::Cache)?;
    set_private_file_permissions(&temporary)?;
    fs::rename(&temporary, &destination).map_err(|_| HelperError::Cache)?;
    if file_sha256(&destination)? != file.expected_sha256 {
        return Err(HelperError::Cache);
    }
    Ok(())
}

fn safe_relative_path(value: &str) -> Result<PathBuf, HelperError> {
    let path = Path::new(value);
    if path.as_os_str().is_empty()
        || !path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
    {
        return Err(HelperError::Cache);
    }
    Ok(path.to_owned())
}

fn ensure_private_directory(path: &Path) -> Result<(), HelperError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            return Err(HelperError::InsecureCache);
        }
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            fs::create_dir_all(path).map_err(|_| HelperError::Cache)?;
        }
        Err(_) => return Err(HelperError::Cache),
    }
    set_private_directory_permissions(path)
}

#[cfg(unix)]
fn set_private_directory_permissions(path: &Path) -> Result<(), HelperError> {
    use std::os::unix::fs::PermissionsExt as _;

    let metadata = fs::symlink_metadata(path).map_err(|_| HelperError::Cache)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(HelperError::InsecureCache);
    }
    let mut permissions = metadata.permissions();
    if permissions.mode() & 0o077 != 0 {
        permissions.set_mode(0o700);
        fs::set_permissions(path, permissions).map_err(|_| HelperError::Cache)?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn set_private_directory_permissions(_path: &Path) -> Result<(), HelperError> {
    Ok(())
}

#[cfg(unix)]
fn set_private_file_permissions(path: &Path) -> Result<(), HelperError> {
    use std::os::unix::fs::PermissionsExt as _;

    let mut permissions = fs::metadata(path)
        .map_err(|_| HelperError::Cache)?
        .permissions();
    permissions.set_mode(0o500);
    fs::set_permissions(path, permissions).map_err(|_| HelperError::Cache)
}

#[cfg(not(unix))]
fn set_private_file_permissions(_path: &Path) -> Result<(), HelperError> {
    Ok(())
}

fn file_sha256(path: &Path) -> Result<String, HelperError> {
    let bytes = fs::read(path).map_err(|_| HelperError::Cache)?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use super::{HelperError, canonical_library_path, parse_arguments};

    #[test]
    fn parser_accepts_an_explicit_library() {
        let parsed = parse_arguments([OsString::from("--library"), OsString::from("official.so")])
            .expect("arguments should parse")
            .expect("not help");
        assert_eq!(parsed.library, Some("official.so".into()));
    }

    #[test]
    fn parser_rejects_duplicate_or_unknown_arguments() {
        let duplicate = parse_arguments([
            OsString::from("--library"),
            OsString::from("one.so"),
            OsString::from("--library"),
            OsString::from("two.so"),
        ])
        .expect_err("duplicate library must fail");
        assert_eq!(duplicate, HelperError::InvalidArguments);
        let unknown =
            parse_arguments([OsString::from("--unknown")]).expect_err("unknown argument must fail");
        assert_eq!(unknown, HelperError::InvalidArguments);
    }

    #[test]
    fn parser_handles_help_without_loading_anything() {
        let parsed = parse_arguments([OsString::from("--help")]).expect("help should parse");
        assert!(parsed.is_none());
    }

    #[test]
    fn explicit_library_must_be_absolute() {
        let error = canonical_library_path("relative/official.so".into())
            .expect_err("relative library path must be rejected");
        assert_eq!(error, HelperError::InvalidArguments);
    }
}
