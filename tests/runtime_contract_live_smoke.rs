use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
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

    let auth = block_on(provider.authenticate()).expect("live auth should succeed");
    let topology =
        block_on(provider.resolve_topology(&auth, &config, &[ProtocolDomain::Market]))
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

    let mut run = block_on(runtime.establish(&provider, &provider, &connector, &config, &adapters))
        .unwrap_or_else(|err| panic!("live auth/topology establish should succeed via {market_url}: {err}"));

    block_on(run.connected.send_route_frame(
        "market",
        OutboundFrame::Text(r#"{"aid":"peek_message"}"#.to_string()),
    ))
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

    let market_id = block_on(handle.submit(RuntimeCommand::Market(
        MarketCommand::SubscribeQuotes {
            symbols: vec![tqsdk_runtime_contract::Symbol::new(test_symbol.clone())],
        },
    )))
    .expect("live market subscribe should succeed");

    let market_receipts = block_on(runtime.flush_outbound(&mut run))
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
}

#[test]
#[ignore = "live network smoke; uses public HTTP metadata endpoint"]
fn live_schema_http_contract_smoke() {
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
    let mut run = block_on(runtime.establish(
        &StaticAuthProvider,
        &resolver,
        &DefaultRouteConnector::default(),
        &config,
        &adapters,
    ))
    .expect("live schema establish should succeed");
    let executor = ReqwestHttpExecutor::default();

    let schema_id = block_on(handle.submit(RuntimeCommand::Schema(SchemaCommand::Refresh {
        schema_id: SchemaId::new("symbols-latest"),
        path: "/shinny_chinese_holiday.json".to_string(),
    })))
    .expect("live schema submit should succeed");

    let receipts = block_on(runtime.flush_outbound(&mut run))
        .expect("live schema dispatch should succeed");
    assert_eq!(receipts.len(), 1);

    let outcome = block_on(runtime.drive_pending_route_once(
        &mut run,
        "schema-live",
        &executor,
        vec![schema_id],
        CommitScope::RealtimeUpdate,
    ))
    .expect("live schema executor should succeed");
    assert_eq!(outcome.commits.len(), 1);

    let schema_segment = schema_id.get().to_string();
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["runtime", "commands", schema_segment.as_str(), "status"]),
        Some(&json!("completed"))
    );
    assert!(
        handle
            .latest_snapshot()
            .get(["schema", "schema-live"])
            .is_some()
    );
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

fn pump_route_until<T, F>(
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

        let Some(input) = block_on(run.connected.recv_route_input(route_label))
            .expect("live route recv should succeed")
        else {
            continue;
        };
        diagnostics.push(describe_runtime_input(&input));
        let _ = handle
            .ingest(input, caused_by.clone(), scope)
            .expect("live route ingest should succeed");
        if route_label == "market" {
            block_on(run.connected.send_route_frame(
                route_label,
                OutboundFrame::Text(r#"{"aid":"peek_message"}"#.to_string()),
            ))
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

fn block_on<F>(future: F) -> F::Output
where
    F: Future,
{
    let mut future = Pin::from(Box::new(future));
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);

    loop {
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

fn noop_waker() -> Waker {
    unsafe { Waker::from_raw(noop_raw_waker()) }
}

fn noop_raw_waker() -> RawWaker {
    RawWaker::new(std::ptr::null(), &NOOP_WAKER_VTABLE)
}

unsafe fn noop_clone(_: *const ()) -> RawWaker {
    noop_raw_waker()
}

unsafe fn noop(_: *const ()) {}

static NOOP_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(noop_clone, noop, noop, noop);
