use std::error::Error;
use std::fmt::{self, Write as _};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const UNIVERSE_LANGUAGE_VERSION: u32 = 2;
pub const UNIVERSE_CANONICALIZER_ID: &str = "tqsdk.universe.canonical.v2";
pub const UNIVERSE_COMPILER_ID: &str = "tqsdk.universe.compiler.v2";

const UNIVERSE_AST_HASH_DOMAIN: &[u8] = b"tqsdk.universe.ast.v2\0";

/// Evaluation mode carried by a normalized V2 universe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum UniverseMode {
    Snapshot,
    Timeline,
}

impl UniverseMode {
    pub(crate) const fn wire_name(self) -> &'static str {
        match self {
            Self::Snapshot => "snapshot",
            Self::Timeline => "timeline",
        }
    }
}

/// Instrument view selected by a normalized V2 clause.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum UniverseView {
    Contract,
    Main,
    Top(u32),
    Continuous,
    Index,
    Symbol,
}

impl UniverseView {
    const fn wire_kind(self) -> &'static str {
        match self {
            Self::Contract => "contract",
            Self::Main => "main",
            Self::Top(_) => "top",
            Self::Continuous => "continuous",
            Self::Index => "index",
            Self::Symbol => "symbol",
        }
    }

    const fn limit(self) -> Option<u32> {
        match self {
            Self::Top(limit) => Some(limit),
            _ => None,
        }
    }
}

impl fmt::Display for UniverseView {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Top(limit) => write!(formatter, "top:{limit}"),
            _ => formatter.write_str(self.wire_kind()),
        }
    }
}

/// A normalized target scope in Universe language V2.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum UniverseTarget {
    All,
    Exchange { exchange: String },
    Product { exchange: String, product: String },
    Contract { exchange: String, contract: String },
    Symbol { symbol: String },
}

impl UniverseTarget {
    const fn wire_kind(&self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Exchange { .. } => "exchange",
            Self::Product { .. } => "product",
            Self::Contract { .. } => "contract",
            Self::Symbol { .. } => "symbol",
        }
    }

    fn exchange(&self) -> Option<&str> {
        match self {
            Self::Exchange { exchange }
            | Self::Product { exchange, .. }
            | Self::Contract { exchange, .. } => Some(exchange),
            Self::All | Self::Symbol { .. } => None,
        }
    }

    fn value(&self) -> Option<&str> {
        match self {
            Self::Product { product, .. } => Some(product),
            Self::Contract { contract, .. } => Some(contract),
            Self::Symbol { symbol } => Some(symbol),
            Self::All | Self::Exchange { .. } => None,
        }
    }
}

impl fmt::Display for UniverseTarget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::All => formatter.write_str("all"),
            Self::Exchange { exchange } => write!(formatter, "{exchange}.*"),
            Self::Product { exchange, product } => write!(formatter, "{exchange}.{product}"),
            Self::Contract { exchange, contract } => {
                write!(formatter, "{exchange}.{contract}")
            }
            Self::Symbol { symbol } => formatter.write_str(symbol),
        }
    }
}

/// One normalized selector. Targets are deduplicated and in canonical order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UniverseSelectorSpec {
    view: UniverseView,
    targets: Vec<UniverseTarget>,
}

impl UniverseSelectorSpec {
    pub(crate) fn new(view: UniverseView, targets: Vec<UniverseTarget>) -> Self {
        Self { view, targets }
    }

    #[must_use]
    pub const fn view(&self) -> UniverseView {
        self.view
    }

    #[must_use]
    pub fn targets(&self) -> &[UniverseTarget] {
        &self.targets
    }
}

/// Parsed and normalized Universe language V2 expression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UniverseSpec {
    mode: UniverseMode,
    includes: Vec<UniverseSelectorSpec>,
    excludes: Vec<UniverseSelectorSpec>,
    global_filters: Vec<UniverseTarget>,
    canonical_text: String,
    canonical_ast_json: Vec<u8>,
    canonical_ast_hash: String,
}

impl UniverseSpec {
    pub(crate) fn from_normalized_parts(
        mode: UniverseMode,
        includes: Vec<UniverseSelectorSpec>,
        excludes: Vec<UniverseSelectorSpec>,
        global_filters: Vec<UniverseTarget>,
    ) -> Self {
        let canonical_text = canonical_text(mode, &includes, &excludes, &global_filters);
        let canonical_ast_json = canonical_ast_json(mode, &includes, &excludes, &global_filters);
        let canonical_ast_hash = hash_ast(&canonical_ast_json);
        Self {
            mode,
            includes,
            excludes,
            global_filters,
            canonical_text,
            canonical_ast_json,
            canonical_ast_hash,
        }
    }

    pub(crate) fn from_canonical_ast_json(bytes: &[u8]) -> Result<Self, UniverseSpecError> {
        let wire: UniverseAstWireOwned = serde_json::from_slice(bytes).map_err(|error| {
            UniverseSpecError::InvalidCanonicalAst {
                reason: error.to_string(),
            }
        })?;
        let expression = wire.expression()?;
        let spec = Self::parse_v2(&expression)?;
        if spec.canonical_ast_json_bytes() != bytes {
            return Err(UniverseSpecError::InvalidCanonicalAst {
                reason: "JSON is not the canonical Universe V2 AST encoding".to_string(),
            });
        }
        Ok(spec)
    }

    #[must_use]
    pub const fn mode(&self) -> UniverseMode {
        self.mode
    }

    #[must_use]
    pub fn includes(&self) -> &[UniverseSelectorSpec] {
        &self.includes
    }

    #[must_use]
    pub fn excludes(&self) -> &[UniverseSelectorSpec] {
        &self.excludes
    }

    #[must_use]
    pub fn global_filters(&self) -> &[UniverseTarget] {
        &self.global_filters
    }

    #[must_use]
    pub fn canonical_text(&self) -> &str {
        &self.canonical_text
    }

    #[must_use]
    pub fn canonical_ast_json_bytes(&self) -> &[u8] {
        &self.canonical_ast_json
    }

    #[must_use]
    pub fn canonical_ast_hash(&self) -> &str {
        &self.canonical_ast_hash
    }
}

impl fmt::Display for UniverseSpec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.canonical_text())
    }
}

/// Parse or normalization error for Universe language V2.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum UniverseSpecError {
    Empty,
    InvalidWrapper {
        value: String,
    },
    NestedWrapper,
    EmptyClause,
    UnknownView {
        view: String,
    },
    BarePositiveTarget {
        target: String,
    },
    InvalidTarget {
        target: String,
        reason: &'static str,
    },
    UnsupportedTarget {
        view: UniverseView,
        target: UniverseTarget,
    },
    InvalidTopLimit {
        value: String,
    },
    MixedAll {
        view: UniverseView,
    },
    ContradictorySelector {
        view: UniverseView,
        target: UniverseTarget,
    },
    InvalidCanonicalAst {
        reason: String,
    },
    MissingInclude,
}

impl fmt::Display for UniverseSpecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("Universe V2 expression must not be empty"),
            Self::InvalidWrapper { value } => {
                write!(formatter, "invalid Universe V2 wrapper: {value}")
            }
            Self::NestedWrapper => {
                formatter.write_str("Universe V2 snapshot/timeline wrappers must not be nested")
            }
            Self::EmptyClause => formatter.write_str("Universe V2 clause must not be empty"),
            Self::UnknownView { view } => write!(formatter, "unknown Universe V2 view {view}"),
            Self::BarePositiveTarget { target } => write!(
                formatter,
                "positive structural target {target} requires an explicit Universe V2 view"
            ),
            Self::InvalidTarget { target, reason } => {
                write!(formatter, "invalid Universe V2 target {target}: {reason}")
            }
            Self::UnsupportedTarget { view, target } => {
                write!(
                    formatter,
                    "Universe V2 view {view} does not support target {target}"
                )
            }
            Self::InvalidTopLimit { value } => {
                write!(formatter, "Universe V2 top limit must be positive: {value}")
            }
            Self::MixedAll { view } => write!(
                formatter,
                "Universe V2 view {view} cannot mix all with narrower include targets"
            ),
            Self::ContradictorySelector { view, target } => write!(
                formatter,
                "Universe V2 selector {view}:{target} is both included and excluded"
            ),
            Self::InvalidCanonicalAst { reason } => {
                write!(formatter, "invalid canonical Universe V2 AST: {reason}")
            }
            Self::MissingInclude => {
                formatter.write_str("Universe V2 expression requires at least one include selector")
            }
        }
    }
}

impl Error for UniverseSpecError {}

#[derive(Serialize)]
struct UniverseAstWire<'a> {
    language_version: u32,
    mode: &'static str,
    includes: Vec<SelectorWire<'a>>,
    excludes: Vec<SelectorWire<'a>>,
    global_filters: Vec<TargetWire<'a>>,
}

#[derive(Serialize)]
struct SelectorWire<'a> {
    view: ViewWire,
    targets: Vec<TargetWire<'a>>,
}

#[derive(Serialize)]
struct ViewWire {
    kind: &'static str,
    limit: Option<u32>,
}

#[derive(Serialize)]
struct TargetWire<'a> {
    kind: &'static str,
    exchange: Option<&'a str>,
    value: Option<&'a str>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UniverseAstWireOwned {
    language_version: u32,
    mode: String,
    includes: Vec<SelectorWireOwned>,
    excludes: Vec<SelectorWireOwned>,
    global_filters: Vec<TargetWireOwned>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SelectorWireOwned {
    view: ViewWireOwned,
    targets: Vec<TargetWireOwned>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ViewWireOwned {
    kind: String,
    limit: Option<u32>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TargetWireOwned {
    kind: String,
    exchange: Option<String>,
    value: Option<String>,
}

impl UniverseAstWireOwned {
    fn expression(&self) -> Result<String, UniverseSpecError> {
        if self.language_version != UNIVERSE_LANGUAGE_VERSION {
            return Err(invalid_canonical_ast(format!(
                "unsupported language_version {}",
                self.language_version
            )));
        }
        let wrapper = match self.mode.as_str() {
            "snapshot" => "snapshot",
            "timeline" => "timeline",
            other => {
                return Err(invalid_canonical_ast(format!(
                    "unsupported Universe mode {other}"
                )));
            }
        };
        let mut clauses = Vec::with_capacity(
            self.includes.len() + self.excludes.len() + self.global_filters.len(),
        );
        for selector in &self.includes {
            clauses.push(selector.expression(false)?);
        }
        for selector in &self.excludes {
            clauses.push(selector.expression(true)?);
        }
        for target in &self.global_filters {
            clauses.push(format!("!{}", target.expression()?));
        }
        Ok(format!("{wrapper}({})", clauses.join(";")))
    }
}

impl SelectorWireOwned {
    fn expression(&self, exclude: bool) -> Result<String, UniverseSpecError> {
        let view = match (self.view.kind.as_str(), self.view.limit) {
            ("contract", None) => "contract".to_string(),
            ("main", None) => "main".to_string(),
            ("top", Some(limit)) if limit > 0 => format!("top:{limit}"),
            ("continuous", None) => "continuous".to_string(),
            ("index", None) => "index".to_string(),
            ("symbol", None) => "symbol".to_string(),
            (kind, limit) => {
                return Err(invalid_canonical_ast(format!(
                    "invalid view/limit combination {kind}:{limit:?}"
                )));
            }
        };
        if self.targets.is_empty() {
            return Err(invalid_canonical_ast("selector targets must not be empty"));
        }
        let targets = self
            .targets
            .iter()
            .map(TargetWireOwned::expression)
            .collect::<Result<Vec<_>, _>>()?
            .join(",");
        let prefix = if exclude { "!" } else { "" };
        Ok(format!("{prefix}{view}:{targets}"))
    }
}

impl TargetWireOwned {
    fn expression(&self) -> Result<String, UniverseSpecError> {
        match (
            self.kind.as_str(),
            self.exchange.as_deref(),
            self.value.as_deref(),
        ) {
            ("all", None, None) => Ok("all".to_string()),
            ("exchange", Some(exchange), None) => Ok(format!("{exchange}.*")),
            ("product" | "contract", Some(exchange), Some(value)) => {
                Ok(format!("{exchange}.{value}"))
            }
            ("symbol", None, Some(symbol)) => Ok(symbol.to_string()),
            (kind, exchange, value) => Err(invalid_canonical_ast(format!(
                "invalid target fields kind={kind} exchange={exchange:?} value={value:?}"
            ))),
        }
    }
}

fn invalid_canonical_ast(reason: impl Into<String>) -> UniverseSpecError {
    UniverseSpecError::InvalidCanonicalAst {
        reason: reason.into(),
    }
}

fn canonical_text(
    mode: UniverseMode,
    includes: &[UniverseSelectorSpec],
    excludes: &[UniverseSelectorSpec],
    global_filters: &[UniverseTarget],
) -> String {
    let mut clauses = Vec::with_capacity(includes.len() + excludes.len() + global_filters.len());
    clauses.extend(includes.iter().map(selector_text));
    clauses.extend(
        excludes
            .iter()
            .map(|selector| format!("!{}", selector_text(selector))),
    );
    clauses.extend(global_filters.iter().map(|target| format!("!{target}")));
    let clause_list = clauses.join(";");
    match mode {
        UniverseMode::Snapshot => clause_list,
        UniverseMode::Timeline => format!("timeline({clause_list})"),
    }
}

fn selector_text(selector: &UniverseSelectorSpec) -> String {
    let targets = selector
        .targets
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",");
    format!("{}:{targets}", selector.view)
}

fn canonical_ast_json(
    mode: UniverseMode,
    includes: &[UniverseSelectorSpec],
    excludes: &[UniverseSelectorSpec],
    global_filters: &[UniverseTarget],
) -> Vec<u8> {
    let wire = UniverseAstWire {
        language_version: UNIVERSE_LANGUAGE_VERSION,
        mode: mode.wire_name(),
        includes: includes.iter().map(selector_wire).collect(),
        excludes: excludes.iter().map(selector_wire).collect(),
        global_filters: global_filters.iter().map(target_wire).collect(),
    };
    serde_json::to_vec(&wire).expect("serializing the fixed Universe V2 wire cannot fail")
}

fn selector_wire(selector: &UniverseSelectorSpec) -> SelectorWire<'_> {
    SelectorWire {
        view: ViewWire {
            kind: selector.view.wire_kind(),
            limit: selector.view.limit(),
        },
        targets: selector.targets.iter().map(target_wire).collect(),
    }
}

fn target_wire(target: &UniverseTarget) -> TargetWire<'_> {
    TargetWire {
        kind: target.wire_kind(),
        exchange: target.exchange(),
        value: target.value(),
    }
}

fn hash_ast(canonical_ast_json: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(UNIVERSE_AST_HASH_DOMAIN);
    hasher.update(canonical_ast_json);
    let digest = hasher.finalize();
    let mut output = String::with_capacity("sha256:".len() + digest.len() * 2);
    output.push_str("sha256:");
    for byte in digest {
        let _ = write!(output, "{byte:02x}");
    }
    output
}
