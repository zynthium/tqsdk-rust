#![cfg_attr(not(test), forbid(unsafe_code))]

/// Placeholder error type for the wait facade.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaitFacadeError;

pub type Result<T> = std::result::Result<T, WaitFacadeError>;
