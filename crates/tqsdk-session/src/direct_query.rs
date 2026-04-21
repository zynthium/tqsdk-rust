#![cfg_attr(not(test), forbid(unsafe_code))]

use serde_json::Value;
use tqsdk_core::CommandId;

#[allow(async_fn_in_trait)]
pub trait SessionDirectQuery {
    async fn query_graphql(
        &self,
        query: &str,
        variables: Option<Value>,
    ) -> crate::error::Result<CommandId>;

    async fn refresh_schema(&self, schema_id: &str, path: &str) -> crate::error::Result<CommandId>;

    async fn query_graphql_value(
        &self,
        query: &str,
        variables: Option<Value>,
    ) -> crate::error::Result<Value>;

    async fn refresh_schema_value(
        &self,
        schema_id: &str,
        path: &str,
    ) -> crate::error::Result<Value>;
}
