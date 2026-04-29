use std::fmt;
use std::future::Future;
use std::pin::Pin;

use crate::Result;
use crate::ids::AuthId;

/// Authentication result returned by [`AuthProvider`].
///
/// Use the constructor and accessor methods as the public contract:
///
/// ```
/// # use tqsdk_core::{AuthContext, AuthId};
/// let auth = AuthContext::new("access-token").with_auth_id(AuthId::new("auth-1"));
/// assert_eq!(auth.access_token(), "access-token");
/// assert_eq!(auth.auth_id().map(AuthId::as_str), Some("auth-1"));
/// ```
///
/// Direct field construction is not part of the public contract:
///
/// ```compile_fail
/// # use tqsdk_core::AuthContext;
/// let _auth = AuthContext {
///     access_token: String::from("access-token"),
///     auth_id: None,
///     features: Vec::new(),
/// };
/// ```
#[derive(Clone, PartialEq, Eq)]
pub struct AuthContext {
    access_token: String,
    auth_id: Option<AuthId>,
    features: Vec<String>,
}

impl fmt::Debug for AuthContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuthContext")
            .field("access_token", &"[REDACTED]")
            .field("auth_id", &self.auth_id)
            .field("features", &self.features)
            .finish()
    }
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
