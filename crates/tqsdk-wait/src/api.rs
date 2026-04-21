#![cfg_attr(not(test), forbid(unsafe_code))]

use crate::builder::TqApiBuilder;

/// Placeholder for the Python-style API facade.
#[derive(Debug, Default, Clone, Copy)]
pub struct TqApi;

impl TqApi {
    pub fn builder() -> TqApiBuilder {
        TqApiBuilder::default()
    }
}
