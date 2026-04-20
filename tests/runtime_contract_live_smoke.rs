use std::future::Future;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tqsdk_runtime_contract::{
    AdapterRegistry, AuthContext, AuthProvider, CommitScope, ContractFuture, DefaultRouteConnector,
    EndpointConfig, InputPayload, IoEvent, MarketCommand, MarketSessionTarget, OutboundFrame,
    PasswordCredentials, ProtocolDomain, ReqwestHttpExecutor, Runtime, RuntimeCommand,
    RuntimeHandle, RuntimeInput, SchemaCommand, SchemaId, SessionBootstrap, SessionConfig,
    SessionRoute, SessionRouteEndpoint, SessionRuntime, SessionTarget, SessionTopology,
    SessionTopologyResolver, TqAuthProvider,
};

#[test]
#[ignore = "live network smoke; requires TQ_AUTH_USER/TQ_AUTH_PASS"]
fn live_auth_market_contract_smoke() {
    run_on_tokio(async {
        let Some(username) = read_env("TQ_AUTH_USER") else {
            return;
        };
        let Some(password) = read_env("TQ_AUTH_PASS") else {
            return;
        };
        let test_symbol = read_env("TQ_TEST_SYMBOL").unwrap_or_else(|| "SHFE.au2602".to_string());

        let provider = TqAuthProvider::new(PasswordCredentials::new(username, password));
        let handle = runtime_with_default_adapters();
        let runtime = SessionRuntime::new(handle.clone(), SessionBootstrap::new());
        let config = SessionConfig::new(EndpointConfig::new("https://auth.shinnytech.com"))
            .with_market_target(MarketSessionTarget::new(false, false))
            .enable_domain(ProtocolDomain::Market);
        let connector = DefaultRouteConnector::default();
        let adapters = adapter_registry();

        let auth = provider
            .authenticate()
            .await
            .expect("live auth should succeed");
        let topology = provider
            .resolve_topology(&auth, &config, &[ProtocolDomain::Market])
            .await
            .expect("live topology resolution should succeed");
        let market_url = topology
            .routes
            .iter()
            .find_map(|route| match &route.endpoint {
                SessionRouteEndpoint::WebSocket { url, .. } if route.label == "market" => {
                    Some(url.clone())
                }
                _ => None,
            })
            .expect("live topology should include market websocket route");

        let mut run = runtime
            .establish(&provider, &provider, &connector, &config, &adapters)
            .await
            .unwrap_or_else(|err| {
                panic!("live auth/topology establish should succeed via {market_url}: {err}")
            });

        run.connected
            .send_route_frame(
                "market",
                OutboundFrame::Text(r#"{"aid":"peek_message"}"#.to_string()),
            )
            .await
            .expect("initial live market peek should succeed");

        assert!(
            handle
                .latest_snapshot()
                .get(["system", "auth", "context", "auth_id"])
                .is_some()
        );
        assert!(
            handle
                .latest_snapshot()
                .get(["system", "session", "topology", "routes"])
                .is_some()
        );

        let market_id = handle
            .submit(RuntimeCommand::Market(MarketCommand::SubscribeQuotes {
                symbols: vec![tqsdk_runtime_contract::Symbol::new(test_symbol.clone())],
            }))
            .await
            .expect("live market subscribe should succeed");

        let market_receipts = runtime
            .flush_outbound(&mut run)
            .await
            .expect("live market dispatch should succeed");
        assert_eq!(market_receipts.len(), 2);

        let market_diagnostics = pump_route_until(
            &handle,
            &mut run,
            "market",
            Duration::from_secs(30),
            vec![market_id],
            CommitScope::RealtimeUpdate,
            |snapshot| snapshot.get(["quotes", test_symbol.as_str(), "last_price"]).cloned(),
        )
        .await
        .unwrap_or_else(|diagnostics| {
            panic!(
                "live market diff did not arrive within timeout for {test_symbol}; observed frames: {:?}",
                diagnostics
            )
        });
        assert!(
            !market_diagnostics.is_empty(),
            "live market smoke should observe at least one market-route input"
        );
        assert!(
            handle
                .latest_snapshot()
                .get(["quotes", test_symbol.as_str(), "last_price"])
                .is_some()
        );
    });
}

#[test]
#[ignore = "live network smoke; uses public HTTP metadata endpoint"]
fn live_schema_http_contract_smoke() {
    run_on_tokio(async {
        let handle = runtime_with_default_adapters();
        let runtime = SessionRuntime::new(handle.clone(), SessionBootstrap::new());
        let resolver = StaticTopologyResolver {
            topology: SessionTopology::default().with_route(SessionRoute {
                label: "schema-live".to_string(),
                target: SessionTarget::Shared,
                domains: vec![ProtocolDomain::Schema],
                endpoint: SessionRouteEndpoint::Http {
                    url: "https://files.shinnytech.com".to_string(),
                },
            }),
        };
        let config = SessionConfig::new(EndpointConfig::new("https://auth.example"))
            .enable_domain(ProtocolDomain::Schema);
        let adapters = adapter_registry();
        let mut run = runtime
            .establish(
                &StaticAuthProvider,
                &resolver,
                &DefaultRouteConnector::default(),
                &config,
                &adapters,
            )
            .await
            .expect("live schema establish should succeed");
        let executor = ReqwestHttpExecutor::new().expect("reqwest http executor should build");

        let schema_id = handle
            .submit(RuntimeCommand::Schema(SchemaCommand::Refresh {
                schema_id: SchemaId::new("symbols-latest"),
                path: "/shinny_chinese_holiday.json".to_string(),
            }))
            .await
            .expect("live schema submit should succeed");

        let receipts = runtime
            .flush_outbound(&mut run)
            .await
            .expect("live schema dispatch should succeed");
        assert_eq!(receipts.len(), 1);

        let outcome = runtime
            .drive_pending_route_once(
                &mut run,
                "schema-live",
                &executor,
                vec![schema_id],
                CommitScope::RealtimeUpdate,
            )
            .await
            .expect("live schema executor should succeed");
        assert_eq!(outcome.commits.len(), 1);

        let schema_segment = schema_id.get().to_string();
        assert_eq!(
            handle.latest_snapshot().get([
                "runtime",
                "commands",
                schema_segment.as_str(),
                "status"
            ]),
            Some(&json!("completed"))
        );
        assert!(
            handle
                .latest_snapshot()
                .get(["schema", "symbols-latest"])
                .is_some()
        );
    });
}

struct StaticAuthProvider;

impl AuthProvider for StaticAuthProvider {
    fn authenticate(&self) -> ContractFuture<'_, AuthContext> {
        Box::pin(async { Ok(AuthContext::new("live-http-smoke")) })
    }
}

struct StaticTopologyResolver {
    topology: SessionTopology,
}

impl SessionTopologyResolver for StaticTopologyResolver {
    fn resolve_topology<'a>(
        &'a self,
        _auth: &'a AuthContext,
        _config: &'a SessionConfig,
        _enabled_domains: &'a [ProtocolDomain],
    ) -> ContractFuture<'a, SessionTopology> {
        let topology = self.topology.clone();
        Box::pin(async move { Ok(topology) })
    }
}

fn runtime_with_default_adapters() -> RuntimeHandle {
    RuntimeHandle::with_adapters(adapter_registry())
}

fn adapter_registry() -> AdapterRegistry {
    let mut registry = AdapterRegistry::new();
    registry.register_default_adapters();
    registry
}

fn read_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

async fn pump_route_until<T, F>(
    handle: &RuntimeHandle,
    run: &mut tqsdk_runtime_contract::SessionRun,
    route_label: &str,
    timeout: Duration,
    caused_by: Vec<tqsdk_runtime_contract::CommandId>,
    scope: CommitScope,
    mut success: F,
) -> Result<Vec<String>, Vec<String>>
where
    F: FnMut(&tqsdk_runtime_contract::StateSnapshot) -> Option<T>,
{
    let deadline = Instant::now() + timeout;
    let mut diagnostics = Vec::new();
    loop {
        if let Some(value) = success(&handle.latest_snapshot()) {
            let _ = value;
            return Ok(diagnostics);
        }
        if Instant::now() >= deadline {
            return Err(diagnostics);
        }

        let Some(input) = run
            .connected
            .recv_route_input(route_label)
            .await
            .expect("live route recv should succeed")
        else {
            continue;
        };
        diagnostics.push(describe_runtime_input(&input));
        let _ = handle
            .ingest(input, caused_by.clone(), scope)
            .expect("live route ingest should succeed");
        if route_label == "market" {
            run.connected
                .send_route_frame(
                    route_label,
                    OutboundFrame::Text(r#"{"aid":"peek_message"}"#.to_string()),
                )
                .await
                .expect("live follow-up market peek should succeed");
        }
    }
}

fn describe_runtime_input(input: &RuntimeInput) -> String {
    match input {
        RuntimeInput::Io(IoEvent {
            route,
            payload: InputPayload::Json(value),
            ..
        }) => {
            let aid = value
                .get("aid")
                .and_then(Value::as_str)
                .unwrap_or("<none>")
                .to_string();
            let keys = value
                .as_object()
                .map(|object| object.keys().cloned().collect::<Vec<_>>())
                .unwrap_or_default();
            format!("io:{route}:aid={aid}:keys={keys:?}")
        }
        RuntimeInput::Io(IoEvent {
            route,
            payload: InputPayload::Text(text),
            ..
        }) => format!("io:{route}:text={text}"),
        RuntimeInput::Io(IoEvent {
            route,
            payload: InputPayload::Binary(bytes),
            ..
        }) => format!("io:{route}:binary={}bytes", bytes.len()),
        RuntimeInput::Internal(event) => format!("internal:{}", event.label),
        RuntimeInput::Auth(event) => format!("auth:{}", event.label),
        RuntimeInput::Replay(event) => format!("replay:{}", event.label),
        RuntimeInput::Timer(event) => format!("timer:{}", event.label),
    }
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
