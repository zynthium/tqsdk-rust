#![cfg_attr(not(test), forbid(unsafe_code))]

/// Placeholder error type for the shared session layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionError;
