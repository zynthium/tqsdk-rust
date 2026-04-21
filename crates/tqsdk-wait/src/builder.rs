#![cfg_attr(not(test), forbid(unsafe_code))]

use crate::api::TqApi;

/// Placeholder builder for the wait facade.
#[derive(Debug, Default, Clone, Copy)]
pub struct TqApiBuilder;

impl TqApiBuilder {
    pub fn build(self) -> TqApi {
        TqApi
    }
}
