mod ast;
mod compatibility;
mod compiler;
mod normalize;
mod parser;
mod source;

pub use ast::{
    UNIVERSE_CANONICALIZER_ID, UNIVERSE_COMPILER_ID, UNIVERSE_LANGUAGE_VERSION, UniverseMode,
    UniverseSelectorSpec, UniverseSpec, UniverseSpecError, UniverseTarget, UniverseView,
};
pub use compatibility::{
    HistoricalUniverseDispatch, SnapshotUniverseDispatch, UniverseCompatibilityError,
    UniverseDispatchReport, UniverseEvaluationPolicy, UniverseLanguage,
    parse_historical_universe_compatible, parse_snapshot_universe_compatible,
};
pub use compiler::{
    CompiledUniverse, CompiledUniverseCandidate, CompiledUniverseInstrumentKind,
    SnapshotCapabilities, SnapshotContract, UniverseCompileError, UniverseProduct,
    UniverseSymbolClass, compile_expanded_snapshot_universe, compile_snapshot_universe,
};
pub use source::{
    ExpandedUniverseInput, ExpandedUniverseSymbolFile, UniverseInput, UniverseSourceError,
    UniverseSymbolFile,
};

impl UniverseSpec {
    /// Parses and normalizes a Universe language V2 expression.
    ///
    /// This explicit entry point never falls back to the legacy universe language.
    pub fn parse_v2(value: &str) -> Result<Self, UniverseSpecError> {
        normalize::normalize(parser::parse(value)?)
    }
}
