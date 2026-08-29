use std::collections::BTreeSet;

use tqsdk_session::{InstrumentClass, SessionClient, SymbolInfo};

use crate::{
    DataError, HistoricalAcquisitionContract, HistoricalCatalogAcquisition, HistoricalCatalogProof,
    Result,
};

pub const PROVIDER_CURRENT_FUTURES_SOURCE_IDENTITY: &str =
    "tq-query:physical-futures:CFFEX,SHFE,DCE,CZCE,INE,GFEX:v2";
pub const PROVIDER_CURRENT_PHYSICAL_FUTURES_EXCHANGES: &[&str] =
    &["CFFEX", "SHFE", "DCE", "CZCE", "INE", "GFEX"];

/// Stable-roster provider-current futures catalog acquisition.
pub struct ProviderCurrentHistoricalCatalogAcquirer {
    client: SessionClient,
    metadata_batch_size: usize,
}

impl ProviderCurrentHistoricalCatalogAcquirer {
    pub fn new(client: SessionClient) -> Self {
        Self {
            client,
            metadata_batch_size: crate::DEFAULT_FUTURES_METADATA_BATCH_SIZE,
        }
    }

    pub fn with_metadata_batch_size(mut self, metadata_batch_size: usize) -> Result<Self> {
        if metadata_batch_size == 0 {
            return Err(validation(
                "historical catalog metadata batch size must be greater than zero",
            ));
        }
        self.metadata_batch_size = metadata_batch_size;
        Ok(self)
    }

    /// Observes the all-futures roster before and after metadata materialization.
    ///
    /// A changed roster is preserved as `complete=false`; callers may persist it for audit but
    /// must retry before using it as a fill manifest.
    pub async fn acquire(
        &self,
        requested_as_of_ns: i64,
        observed_at_ns: i64,
    ) -> Result<HistoricalCatalogAcquisition> {
        let roster_before = self.query_roster().await?;
        let infos_before = self.query_metadata(&roster_before).await?;
        let roster_after = self.query_roster().await?;
        let stable = roster_before == roster_after;
        let infos = if stable {
            infos_before
        } else {
            let union = roster_before
                .iter()
                .chain(&roster_after)
                .cloned()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            self.query_metadata(&union).await?
        };
        let complete = stable && infos.len() == roster_before.len();
        let contracts = infos
            .into_iter()
            .map(contract_from_symbol_info)
            .collect::<Result<Vec<_>>>()?;
        HistoricalCatalogAcquisition::new(
            HistoricalCatalogProof::ProviderCurrentObserved,
            PROVIDER_CURRENT_FUTURES_SOURCE_IDENTITY,
            "physical:all",
            requested_as_of_ns,
            observed_at_ns,
            complete,
            roster_before,
            roster_after,
            contracts,
        )
    }

    async fn query_roster(&self) -> Result<Vec<String>> {
        let mut roster = self
            .client
            .query_quotes(Some("FUTURE"), None, None, None, None)
            .await?;
        roster.retain(|symbol| {
            let Some((exchange, contract)) = symbol.split_once('.') else {
                return false;
            };
            PROVIDER_CURRENT_PHYSICAL_FUTURES_EXCHANGES.contains(&exchange)
                && !contract.contains('@')
        });
        roster.sort();
        roster.dedup();
        Ok(roster)
    }

    async fn query_metadata(&self, symbols: &[String]) -> Result<Vec<SymbolInfo>> {
        let mut infos = Vec::with_capacity(symbols.len());
        for batch in symbols.chunks(self.metadata_batch_size) {
            let requested = batch.iter().map(String::as_str).collect::<Vec<_>>();
            infos.extend(self.client.query_symbol_info(&requested).await?);
        }
        let returned = infos
            .iter()
            .map(|info| info.instrument_id.as_str().to_string())
            .collect::<BTreeSet<_>>();
        let requested = symbols.iter().cloned().collect::<BTreeSet<_>>();
        if returned.len() != infos.len() {
            return Err(validation(
                "provider-current metadata response contains duplicate symbols",
            ));
        }
        if !returned.is_subset(&requested) {
            return Err(validation(
                "provider-current metadata response contains unexpected symbols",
            ));
        }
        Ok(infos)
    }
}

fn contract_from_symbol_info(info: SymbolInfo) -> Result<HistoricalAcquisitionContract> {
    if info.class != InstrumentClass::Future {
        return Err(validation(format!(
            "provider-current futures roster returned non-future {}",
            info.instrument_id.as_str()
        )));
    }
    let expire_datetime_ns = info
        .expire_datetime_secs
        .map(|seconds| {
            seconds.checked_mul(1_000_000_000).ok_or_else(|| {
                validation("historical contract expire_datetime overflows nanoseconds")
            })
        })
        .transpose()?;
    Ok(HistoricalAcquisitionContract {
        symbol: info.instrument_id.as_str().to_string(),
        exchange_id: info.exchange_id,
        product_id: info.product_id,
        expired: info.expired,
        expire_datetime_ns,
        authoritative_lifecycle: Vec::new(),
        first_available_data_ns: Default::default(),
    })
}

fn validation(message: impl Into<String>) -> DataError {
    DataError::Validation(message.into())
}
