use tqsdk_relay::{
    ClientId, DownstreamCommand, FakeUpstreamTickSource, RelayEngine, RelayTickRow, UpstreamTick,
    pump_available, pump_once,
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
