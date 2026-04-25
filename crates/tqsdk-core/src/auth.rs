use std::future::Future;
use std::pin::Pin;

use crate::Result;
use crate::ids::AuthId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthContext {
    pub access_token: String,
    pub auth_id: Option<AuthId>,
    pub features: Vec<String>,
}

impl AuthContext {
    pub fn new(access_token: impl Into<String>) -> Self {
        Self {
            access_token: access_token.into(),
            auth_id: None,
            features: Vec::new(),
        }
    }

    pub fn access_token(&self) -> &str {
        &self.access_token
    }

    pub fn auth_id(&self) -> Option<&AuthId> {
        self.auth_id.as_ref()
    }

    pub fn features(&self) -> &[String] {
        &self.features
    }

    pub fn with_auth_id(mut self, auth_id: AuthId) -> Self {
        self.auth_id = Some(auth_id);
        self
    }

    pub fn with_feature(mut self, feature: impl Into<String>) -> Self {
        self.features.push(feature.into());
        self
    }
}

pub trait AuthProvider: Send + Sync {
    fn authenticate(&self) -> impl Future<Output = Result<AuthContext>> + Send + '_;
}

#[doc(hidden)]
pub trait DynAuthProvider: Send + Sync {
    fn authenticate_boxed(&self) -> Pin<Box<dyn Future<Output = Result<AuthContext>> + Send + '_>>;
}

impl<T> DynAuthProvider for T
where
    T: AuthProvider,
{
    fn authenticate_boxed(&self) -> Pin<Box<dyn Future<Output = Result<AuthContext>> + Send + '_>> {
        Box::pin(self.authenticate())
    }
}
