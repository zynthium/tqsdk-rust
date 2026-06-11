use tqsdk_core::Quote;
use tqsdk_relay::{
    ClientId, DownstreamCommand, FakeUpstreamTickSource, RelayEngine, RelayTickRow, UpstreamQuote,
    UpstreamTick, pump_available, pump_once,
};

fn tick(id: i64, datetime: i64, price: f64) -> RelayTickRow {
    RelayTickRow {
        id,
        datetime,
        last_price: price,
        volume: id * 10,
        open_interest: 1000 + id,
    }
}

fn quote(symbol: &str, datetime: &str, price: f64) -> Quote {
    Quote {
        instrument_id: symbol.to_string(),
        datetime: datetime.to_string(),
        last_price: price,
        volume: 12,
        open_interest: 34,
        ..Quote::default()
    }
}

#[tokio::test]
async fn pump_once_ingests_one_upstream_tick_and_returns_downstream_frames() {
    let mut engine = RelayEngine::new_memory_only(16, 16);
    let client = ClientId::new(1);
    engine
        .handle_command(
            client,
            DownstreamCommand::SubscribeQuote {
                symbols: vec!["SHFE.au2602".to_string()],
            },
        )
        .unwrap();
    let mut upstream = FakeUpstreamTickSource::default();
    upstream.push(UpstreamTick {
        symbol: "SHFE.au2602".to_string(),
        row: tick(1, 1_000, 610.0),
    });

    let frames = pump_once(&mut engine, &mut upstream).await.unwrap();

    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].client_id, client);
    assert_eq!(
        frames[0].payload["data"][0]["quotes"]["SHFE.au2602"]["last_price"],
        610.0
    );
    assert!(
        pump_once(&mut engine, &mut upstream)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn pump_once_ingests_one_upstream_quote_and_returns_downstream_frames() {
    let mut engine = RelayEngine::new_memory_only(16, 16);
    let client = ClientId::new(1);
    engine
        .handle_command(
            client,
            DownstreamCommand::SubscribeQuote {
                symbols: vec!["SHFE.ag2705".to_string()],
            },
        )
        .unwrap();
    let mut upstream = FakeUpstreamTickSource::default();
    upstream.push_quote(UpstreamQuote {
        symbol: "SHFE.ag2705".to_string(),
        quote: quote("SHFE.ag2705", "1780985438500000000", 16666.0),
    });

    let frames = pump_once(&mut engine, &mut upstream).await.unwrap();

    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].client_id, client);
    assert_eq!(
        frames[0].payload["data"][0]["quotes"]["SHFE.ag2705"]["last_price"],
        16666.0
    );
}

#[tokio::test]
async fn pump_available_drains_ready_upstream_ticks() {
    let mut engine = RelayEngine::new_memory_only(16, 16);
    let client = ClientId::new(1);
    engine
        .handle_command(
            client,
            DownstreamCommand::SubscribeQuote {
                symbols: vec!["SHFE.au2602".to_string(), "DCE.m2609".to_string()],
            },
        )
        .unwrap();
    let mut upstream = FakeUpstreamTickSource::default();
    upstream.push(UpstreamTick {
        symbol: "SHFE.au2602".to_string(),
        row: tick(1, 1_000, 610.0),
    });
    upstream.push(UpstreamTick {
        symbol: "DCE.m2609".to_string(),
        row: tick(2, 2_000, 3300.0),
    });

    let frames = pump_available(&mut engine, &mut upstream).await.unwrap();

    assert_eq!(frames.len(), 2);
    assert_eq!(
        frames[0].payload["data"][0]["quotes"]["SHFE.au2602"]["last_price"],
        610.0
    );
    assert_eq!(
        frames[1].payload["data"][0]["quotes"]["DCE.m2609"]["last_price"],
        3300.0
    );
    assert!(
        pump_available(&mut engine, &mut upstream)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn pump_once_records_invalid_tick_rows_even_without_a_tick() {
    let mut engine = RelayEngine::new_memory_only(16, 16);
    engine
        .handle_command(
            ClientId::new(1),
            DownstreamCommand::SubscribeQuote {
                symbols: vec!["SHFE.au2602".to_string()],
            },
        )
        .unwrap();
    let mut invalid_rows_by_symbol = std::collections::BTreeMap::new();
    invalid_rows_by_symbol.insert("SHFE.au2602".to_string(), 1);
    let mut upstream = InvalidOnlySource {
        invalid_rows: 1,
        invalid_rows_by_symbol,
        last_error: Some(
            "SHFE.au2602 row 17: invalid relay protocol: upstream tick row missing last_price"
                .to_string(),
        ),
    };

    let frames = pump_once(&mut engine, &mut upstream).await.unwrap();

    assert!(frames.is_empty());
    let metrics = engine.metrics_snapshot();
    assert_eq!(metrics.upstream_invalid_tick_rows, 1);
    assert_eq!(
        metrics.last_upstream_invalid_tick_row_error.as_deref(),
        Some("SHFE.au2602 row 17: invalid relay protocol: upstream tick row missing last_price")
    );
    let symbol_metrics = engine.symbol_metrics_snapshot_at(1_700_000_010_000, &Default::default());
    assert_eq!(symbol_metrics.symbols[0].symbol, "SHFE.au2602");
    assert_eq!(symbol_metrics.symbols[0].invalid_rows, 1);
    assert_eq!(
        symbol_metrics.symbols[0].last_invalid_row_error.as_deref(),
        Some("SHFE.au2602 row 17: invalid relay protocol: upstream tick row missing last_price")
    );
}

struct InvalidOnlySource {
    invalid_rows: u64,
    invalid_rows_by_symbol: std::collections::BTreeMap<String, u64>,
    last_error: Option<String>,
}

impl tqsdk_relay::UpstreamTickSource for InvalidOnlySource {
    async fn next_tick(&mut self) -> Option<UpstreamTick> {
        None
    }

    fn take_invalid_tick_rows(&mut self) -> u64 {
        std::mem::take(&mut self.invalid_rows)
    }

    fn take_invalid_tick_rows_by_symbol(&mut self) -> std::collections::BTreeMap<String, u64> {
        std::mem::take(&mut self.invalid_rows_by_symbol)
    }

    fn take_last_invalid_tick_row_error(&mut self) -> Option<String> {
        self.last_error.take()
    }
}
