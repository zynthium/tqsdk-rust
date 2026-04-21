#![cfg_attr(not(test), forbid(unsafe_code))]

/// Session-level facade tuning shared by higher-layer crates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionFacadeConfig {
    pub default_view_width: usize,
}

impl SessionFacadeConfig {
    #[must_use]
    pub fn with_default_view_width(mut self, default_view_width: usize) -> Self {
        self.default_view_width = default_view_width.max(1);
        self
    }
}

impl Default for SessionFacadeConfig {
    fn default() -> Self {
        Self {
            default_view_width: 200,
        }
    }
}
