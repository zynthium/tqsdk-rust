use std::time::Duration;

use serde_json::{Value, json};
use tokio::time::timeout;
use tqsdk_runtime_contract::{
    AuthProvider, EndpointConfig, MarketSessionTarget, OutboundFrame, PasswordCredentials,
    ProtocolDomain, SessionConfig, SessionRouteEndpoint, SessionTopologyResolver, TqAuthProvider,
    Transport, WebSocketConnectOptions, WebSocketTransport,
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let username = read_env("TQ_AUTH_USER")?;
    let password = read_env("TQ_AUTH_PASS")?;
    let symbol = std::env::var("TQ_TEST_SYMBOL").unwrap_or_else(|_| "SHFE.au2602".to_string());

    let provider = TqAuthProvider::new(PasswordCredentials::new(username, password));
    let config = SessionConfig::new(EndpointConfig::new("https://auth.shinnytech.com"))
        .with_market_target(MarketSessionTarget::new(false, false))
        .enable_domain(ProtocolDomain::Market);

    let auth = provider.authenticate().await?;
    println!("auth_id={:?}", auth.auth_id().map(|id| id.as_str()));
    println!("features={:?}", auth.features());

    let topology = provider
        .resolve_topology(&auth, &config, &[ProtocolDomain::Market])
        .await?;

    let (market_url, connect) = topology
        .routes
        .iter()
        .find_map(|route| match &route.endpoint {
            SessionRouteEndpoint::WebSocket { url, connect } if route.label == "market" => {
                Some((url.clone(), connect.clone()))
            }
            _ => None,
        })
        .ok_or("missing market websocket route")?;

    println!("market_url={market_url}");

    probe_sequence(
        "peek-only",
        &market_url,
        &connect,
        vec![json!({"aid": "peek_message"})],
    )
    .await?;
    probe_sequence(
        "quote-subscribe",
        &market_url,
        &connect,
        vec![
            json!({"aid": "subscribe_quote", "ins_list": symbol}),
            json!({"aid": "peek_message"}),
        ],
    )
    .await?;

    Ok(())
}

async fn probe_sequence(
    label: &str,
    market_url: &str,
    connect: &WebSocketConnectOptions,
    frames: Vec<Value>,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("== {label} ==");
    let mut transport =
        WebSocketTransport::new(market_url.to_string()).with_connect_options(connect.clone());
    transport.connect().await?;
    println!("connected");

    for frame in frames {
        println!("send {}", frame);
        transport
            .send(OutboundFrame::Text(frame.to_string()))
            .await?;
    }

    for idx in 1..=5 {
        match timeout(Duration::from_secs(5), transport.recv()).await {
            Ok(Ok(frame)) => {
                println!("recv#{idx} {}", describe_frame(&frame));
                transport
                    .send(OutboundFrame::Text(r#"{"aid":"peek_message"}"#.to_string()))
                    .await?;
            }
            Ok(Err(err)) => {
                println!("recv#{idx}_err {err}");
                break;
            }
            Err(_) => {
                println!("recv#{idx}_timeout");
                break;
            }
        }
    }

    let _ = transport.close().await;
    Ok(())
}
fn describe_frame(frame: &tqsdk_runtime_contract::RawFrame) -> String {
    match frame {
        tqsdk_runtime_contract::RawFrame::Text(text) => {
            let snippet = if text.len() > 240 {
                format!("{}...", &text[..240])
            } else {
                text.clone()
            };
            format!("text:{snippet}")
        }
        tqsdk_runtime_contract::RawFrame::Binary(bytes) => format!("binary:{}bytes", bytes.len()),
        tqsdk_runtime_contract::RawFrame::Ping => "ping".to_string(),
        tqsdk_runtime_contract::RawFrame::Pong => "pong".to_string(),
        tqsdk_runtime_contract::RawFrame::Close => "close".to_string(),
    }
}

fn read_env(name: &str) -> Result<String, Box<dyn std::error::Error>> {
    let value = std::env::var(name)?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(format!("{name} is empty").into());
    }
    Ok(trimmed.to_string())
}
