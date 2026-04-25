use std::future::Future;

mod support;

use support::websocket::{ClientFrame, TestWebSocketServer};
use tqsdk_core::transport::{SessionBootstrap, WebSocketRouteConnector};
use tqsdk_core::{
    AccountId, ProtocolDomain, SessionRoute, SessionRouteEndpoint, SessionTarget, SessionTopology,
    WebSocketConnectOptions,
};

#[test]
fn session_bootstrap_connects_websocket_routes_from_topology() {
    run_on_tokio(async {
        let market_server = TestWebSocketServer::spawn(|mut socket| {
            assert_eq!(
                socket.request().header("authorization"),
                Some("Bearer test-token"),
            );
            match socket.recv().unwrap() {
                ClientFrame::Close => {}
                other => panic!("expected close frame, got {other:?}"),
            }
        })
        .unwrap();

        let trade_server = TestWebSocketServer::spawn(|mut socket| {
            assert_eq!(
                socket.request().header("authorization"),
                Some("Bearer test-token"),
            );
            match socket.recv().unwrap() {
                ClientFrame::Close => {}
                other => panic!("expected close frame, got {other:?}"),
            }
        })
        .unwrap();

        let topology = SessionTopology::default()
            .with_route(SessionRoute {
                label: "market".to_string(),
                target: SessionTarget::Shared,
                domains: vec![ProtocolDomain::System, ProtocolDomain::Market],
                endpoint: SessionRouteEndpoint::WebSocket {
                    url: market_server.url("/md"),
                    connect: WebSocketConnectOptions::default()
                        .with_header("Authorization", "Bearer test-token"),
                },
            })
            .with_route(SessionRoute {
                label: "trade:simnow".to_string(),
                target: SessionTarget::Account(AccountId::new("simnow")),
                domains: vec![ProtocolDomain::Trade],
                endpoint: SessionRouteEndpoint::WebSocket {
                    url: trade_server.url("/trade"),
                    connect: WebSocketConnectOptions::default()
                        .with_header("Authorization", "Bearer test-token"),
                },
            });

        let connector = WebSocketRouteConnector;
        let mut connected = SessionBootstrap::new()
            .connect_topology(&topology, &connector)
            .await
            .unwrap();

        assert_eq!(connected.routes.len(), 2);
        assert_eq!(connected.routes[0].route.label, "market");
        assert_eq!(connected.routes[1].route.label, "trade:simnow");

        connected.close_all().await.unwrap();
        market_server.join();
        trade_server.join();
    });
}

fn run_on_tokio<F>(future: F) -> F::Output
where
    F: Future,
{
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(future)
}
