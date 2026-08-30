use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::{self, Write as _};
use std::path::{Path, PathBuf};

use serde::Serialize;
use sha2::{Digest, Sha256};

use super::UniverseSpec;

const INPUT_SOURCES_HASH_DOMAIN: &[u8] = b"tqsdk.universe.input-sources.v1\0";

/// One external symbol file attached to a V2 universe input.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct UniverseSymbolFile {
    path: PathBuf,
}

impl UniverseSymbolFile {
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// A V2 expression plus repeatable external exact-symbol sources.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct UniverseInput {
    spec: Option<UniverseSpec>,
    symbol_files: Vec<UniverseSymbolFile>,
}

impl UniverseInput {
    #[must_use]
    pub const fn new(spec: Option<UniverseSpec>) -> Self {
        Self {
            spec,
            symbol_files: Vec::new(),
        }
    }

    #[must_use]
    pub fn from_spec(spec: UniverseSpec) -> Self {
        Self::new(Some(spec))
    }

    #[must_use]
    pub fn spec(&self) -> Option<&UniverseSpec> {
        self.spec.as_ref()
    }

    #[must_use]
    pub fn symbol_files(&self) -> &[UniverseSymbolFile] {
        &self.symbol_files
    }

    #[must_use]
    pub fn universe_symbol_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.symbol_files.push(UniverseSymbolFile::new(path));
        self
    }

    #[must_use]
    pub fn universe_symbol_files<I, P>(mut self, paths: I) -> Self
    where
        I: IntoIterator<Item = P>,
        P: Into<PathBuf>,
    {
        self.symbol_files
            .extend(paths.into_iter().map(UniverseSymbolFile::new));
        self
    }

    /// Reads every configured file exactly once and materializes a pure compiler input.
    pub fn expand(&self) -> Result<ExpandedUniverseInput, UniverseSourceError> {
        if self.spec.is_none() && self.symbol_files.is_empty() {
            return Err(UniverseSourceError::MissingInput);
        }

        let mut expanded_symbols = BTreeSet::new();
        let mut source_files = Vec::with_capacity(self.symbol_files.len());
        for source in &self.symbol_files {
            let configured_path = source.path();
            let bytes =
                std::fs::read(configured_path).map_err(|source| UniverseSourceError::Read {
                    path: absolute_diagnostic_path(configured_path),
                    source,
                })?;
            let raw_content_sha256 = sha256(&bytes);
            let diagnostic_path = canonical_diagnostic_path(configured_path);
            let contents =
                String::from_utf8(bytes).map_err(|_| UniverseSourceError::InvalidUtf8 {
                    path: diagnostic_path.clone(),
                    raw_content_sha256: raw_content_sha256.clone(),
                })?;
            let symbols = parse_symbols(&contents).map_err(|reason| {
                UniverseSourceError::InvalidSymbolFile {
                    path: diagnostic_path.clone(),
                    raw_content_sha256: raw_content_sha256.clone(),
                    reason,
                }
            })?;
            expanded_symbols.extend(symbols.iter().cloned());
            source_files.push(ExpandedUniverseSymbolFile {
                path: diagnostic_path,
                raw_content_sha256,
                symbols,
            });
        }

        let input_sources_sha256 = if source_files.is_empty() {
            None
        } else {
            Some(hash_source_identities(&source_files))
        };
        Ok(ExpandedUniverseInput {
            spec: self.spec.clone(),
            expanded_symbols: expanded_symbols.into_iter().collect(),
            source_files,
            input_sources_sha256,
        })
    }
}

/// Materialized file input. Compilers consume this type without performing I/O.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ExpandedUniverseInput {
    spec: Option<UniverseSpec>,
    expanded_symbols: Vec<String>,
    source_files: Vec<ExpandedUniverseSymbolFile>,
    input_sources_sha256: Option<String>,
}

impl ExpandedUniverseInput {
    #[must_use]
    pub fn spec(&self) -> Option<&UniverseSpec> {
        self.spec.as_ref()
    }

    #[must_use]
    pub fn expanded_symbols(&self) -> &[String] {
        &self.expanded_symbols
    }

    #[must_use]
    pub fn source_files(&self) -> &[ExpandedUniverseSymbolFile] {
        &self.source_files
    }

    #[must_use]
    pub fn input_sources_sha256(&self) -> Option<&str> {
        self.input_sources_sha256.as_deref()
    }
}

/// Diagnostic and identity metadata for one expanded symbol file.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ExpandedUniverseSymbolFile {
    path: PathBuf,
    raw_content_sha256: String,
    symbols: Vec<String>,
}

impl ExpandedUniverseSymbolFile {
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[must_use]
    pub fn raw_content_sha256(&self) -> &str {
        &self.raw_content_sha256
    }

    #[must_use]
    pub fn symbols(&self) -> &[String] {
        &self.symbols
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub enum UniverseSourceError {
    MissingInput,
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    InvalidUtf8 {
        path: PathBuf,
        raw_content_sha256: String,
    },
    InvalidSymbolFile {
        path: PathBuf,
        raw_content_sha256: String,
        reason: &'static str,
    },
}

impl fmt::Display for UniverseSourceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingInput => formatter.write_str(
                "Universe input requires a V2 expression or at least one external symbol file",
            ),
            Self::Read { path, source } => {
                write!(
                    formatter,
                    "failed to read Universe symbol file {}: {source}",
                    path.display()
                )
            }
            Self::InvalidUtf8 {
                path,
                raw_content_sha256,
            } => write!(
                formatter,
                "Universe symbol file {} ({raw_content_sha256}) is not UTF-8",
                path.display()
            ),
            Self::InvalidSymbolFile {
                path,
                raw_content_sha256,
                reason,
            } => write!(
                formatter,
                "invalid Universe symbol file {} ({raw_content_sha256}): {reason}",
                path.display()
            ),
        }
    }
}

impl Error for UniverseSourceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Read { source, .. } => Some(source),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
struct SourceIdentityWire<'a> {
    raw_content_sha256: &'a str,
    symbols: &'a [String],
}

fn parse_symbols(contents: &str) -> Result<Vec<String>, &'static str> {
    let mut symbols = contents
        .lines()
        .flat_map(|line| line.split(','))
        .map(str::trim)
        .map(|symbol| {
            if symbol.is_empty() {
                Err("symbol values must not be empty")
            } else {
                Ok(symbol.to_string())
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    if symbols.is_empty() {
        return Err("symbol file must contain at least one symbol");
    }
    symbols.sort();
    symbols.dedup();
    Ok(symbols)
}

fn hash_source_identities(source_files: &[ExpandedUniverseSymbolFile]) -> String {
    let identities = source_files
        .iter()
        .map(|source| SourceIdentityWire {
            raw_content_sha256: &source.raw_content_sha256,
            symbols: &source.symbols,
        })
        .collect::<BTreeSet<_>>();
    let bytes = serde_json::to_vec(&identities)
        .expect("serializing fixed Universe input source identities cannot fail");
    let mut hasher = Sha256::new();
    hasher.update(INPUT_SOURCES_HASH_DOMAIN);
    hasher.update(bytes);
    format_digest(hasher.finalize())
}

fn sha256(bytes: &[u8]) -> String {
    format_digest(Sha256::digest(bytes))
}

fn format_digest(digest: impl IntoIterator<Item = u8>) -> String {
    let mut output = String::from("sha256:");
    for byte in digest {
        let _ = write!(output, "{byte:02x}");
    }
    output
}

fn canonical_diagnostic_path(path: &Path) -> PathBuf {
    std::fs::canonicalize(path).unwrap_or_else(|_| absolute_diagnostic_path(path))
}

fn absolute_diagnostic_path(path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map(|current| current.join(path))
            .unwrap_or_else(|_| path.to_path_buf())
    }
}
