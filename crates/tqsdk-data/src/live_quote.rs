#![cfg_attr(not(test), forbid(unsafe_code))]

use std::collections::BTreeMap;
use std::time::Duration;

use tqsdk_core::{MarketCommand, Quote, RuntimeCommand, RuntimeReader, Symbol};

use crate::error::{DataError, Result};

const MARKET_POLL_BUDGET: Duration = Duration::from_millis(250);

pub(crate) async fn await_quote_snapshots(
    session: &tqsdk_session::SessionClient,
    symbols: &[String],
    timeout: Duration,
) -> Result<BTreeMap<String, Quote>> {
    let reader = session.reader_clone();
    let missing_symbols = missing_quote_symbols(&reader, symbols)?;
    if missing_symbols.is_empty() {
        return read_ready_quote_snapshots(&reader, symbols)?
            .ok_or_else(|| DataError::InvalidResponse("ready quote snapshot missing".to_string()));
    }

    let command_id = session
        .submit(RuntimeCommand::Market(MarketCommand::SubscribeQuotes {
            symbols: missing_symbols.into_iter().map(Symbol::new).collect(),
        }))
        .await?;

    // Do not automatically unsubscribe here: quote subscriptions are global on a
    // shared session, and removing them here could break other live consumers.
    wait_for_ready_quotes(session, &reader, symbols, command_id, timeout).await?;
    read_ready_quote_snapshots(&reader, symbols)?
        .ok_or_else(|| DataError::InvalidResponse("ready quote snapshot missing".to_string()))
}

async fn wait_for_ready_quotes(
    session: &tqsdk_session::SessionClient,
    reader: &RuntimeReader,
    symbols: &[String],
    command_id: tqsdk_core::CommandId,
    timeout: Duration,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        if read_ready_quote_snapshots(reader, symbols)?.is_some() {
            return Ok(());
        }

        if let Some(status) = session.command_status(command_id)?
            && matches!(status.as_str(), "rejected" | "failed" | "cancelled")
        {
            return Err(DataError::InvalidResponse(format!(
                "subscribe quote command reached terminal status {status}"
            )));
        }

        if tokio::time::Instant::now() >= deadline {
            return Err(DataError::Timeout(timeout));
        }

        let mut progress = false;
        progress |= session.flush_outbound().await?;
        progress |= session.drive_pending_once().await?;
        progress |= session
            .drive_route_once(Some(
                (tokio::time::Instant::now() + MARKET_POLL_BUDGET).min(deadline),
            ))
            .await?;

        if progress {
            continue;
        }

        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(DataError::Timeout(timeout));
        }

        tokio::time::sleep(remaining.min(Duration::from_millis(1))).await;
    }
}

fn missing_quote_symbols(reader: &RuntimeReader, symbols: &[String]) -> Result<Vec<String>> {
    let snapshot = reader.read();
    let mut missing = Vec::new();
    for symbol in symbols {
        let quote = snapshot
            .decode_path::<Quote>(&["quotes", symbol.as_str()])
            .map_err(contract_error_into_data)?;
        if quote.is_none() {
            missing.push(symbol.clone());
        }
    }
    Ok(missing)
}

fn read_ready_quote_snapshots(
    reader: &RuntimeReader,
    symbols: &[String],
) -> Result<Option<BTreeMap<String, Quote>>> {
    let snapshot = reader.read();
    let mut quotes = BTreeMap::new();

    for symbol in symbols {
        let Some(quote) = snapshot
            .decode_path::<Quote>(&["quotes", symbol.as_str()])
            .map_err(contract_error_into_data)?
        else {
            return Ok(None);
        };
        quotes.insert(symbol.clone(), quote);
    }

    Ok(Some(quotes))
}

fn contract_error_into_data(error: tqsdk_core::ContractError) -> DataError {
    DataError::Session(tqsdk_session::SessionFacadeError::from(error))
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tqsdk_core::{
        AdapterRegistry, CommitScope, InputPayload, IoEvent, ProtocolDomain, RuntimeHandle,
        RuntimeInput,
    };
    use tqsdk_session::SessionClient;

    use super::*;

    #[test]
    fn await_quote_snapshots_uses_existing_snapshot_without_live_io() {
        let mut adapters = AdapterRegistry::new();
        adapters.register_default_adapters();
        let handle = RuntimeHandle::with_adapters(adapters);
        let session = SessionClient::new_for_test_with_handle(handle.clone());

        seed_quote(
            &handle,
            "SHFE.au2606",
            json!({
                "datetime": "2026-04-23 10:00:00.000000",
                "instrument_id": "au2606",
                "last_price": 720.5
            }),
        );

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .unwrap();
        let quotes = runtime
            .block_on(await_quote_snapshots(
                &session,
                &[String::from("SHFE.au2606")],
                Duration::from_millis(10),
            ))
            .unwrap();

        assert_eq!(quotes.len(), 1);
        assert_eq!(
            quotes.get("SHFE.au2606").map(|quote| quote.last_price),
            Some(720.5)
        );
    }

    fn seed_quote(handle: &RuntimeHandle, symbol: &str, quote: serde_json::Value) {
        handle
            .ingest(
                RuntimeInput::Io(IoEvent {
                    route: "market".to_string(),
                    domains: vec![ProtocolDomain::Market],
                    payload: InputPayload::Json(json!({
                        "aid": "rtn_data",
                        "data": [{
                            "quotes": {
                                symbol: quote,
                            }
                        }]
                    })),
                }),
                vec![],
                CommitScope::RealtimeUpdate,
            )
            .unwrap()
            .expect("seed quote should produce a commit");
    }
}
