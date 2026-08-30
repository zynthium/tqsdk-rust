mod ast;
mod normalize;
mod parser;

pub use ast::{
    UNIVERSE_CANONICALIZER_ID, UNIVERSE_COMPILER_ID, UNIVERSE_LANGUAGE_VERSION, UniverseMode,
    UniverseSelectorSpec, UniverseSpec, UniverseSpecError, UniverseTarget, UniverseView,
};

impl UniverseSpec {
    /// Parses and normalizes a Universe language V2 expression.
    ///
    /// This explicit entry point never falls back to the legacy universe language.
    pub fn parse_v2(value: &str) -> Result<Self, UniverseSpecError> {
        normalize::normalize(parser::parse(value)?)
    }
}
