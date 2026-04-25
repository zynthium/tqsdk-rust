#![allow(clippy::manual_async_fn)]

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use serde_json::json;
use tqsdk_core::{
    AdapterRegistry, AuthContext, AuthEvent, AuthProvider, CommitScope, DynTransport,
    EndpointConfig, IoEvent, OutboundDispatch, ProtocolDomain, QueryCommand, QueryId,
    ReplayCommand, ReplayEvent, ReplaySessionId, Result as CoreResult, RouteRequestExecutor,
    Runtime, RuntimeCommand, RuntimeHandle, RuntimeInput, SchemaCommand, SchemaId,
    SessionBootstrap, SessionConfig, SessionRoute, SessionRouteConnector, SessionRouteEndpoint,
    SessionRuntime, SessionTarget, SessionTopology, SessionTopologyResolver, SystemCommand,
    Transport,
};

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = CoreResult<T>> + Send + 'a>>;

struct TestAuthProvider;

impl AuthProvider for TestAuthProvider {
    fn authenticate(&self) -> impl Future<Output = CoreResult<AuthContext>> + Send + '_ {
        Box::pin(async { Ok(AuthContext::new("test-token")) })
    }
}

struct StaticTopologyResolver {
    topology: SessionTopology,
    expected_domains: Vec<ProtocolDomain>,
}

impl SessionTopologyResolver for StaticTopologyResolver {
    fn resolve_topology<'a>(
        &'a self,
        _auth: &'a AuthContext,
        _config: &'a SessionConfig,
        enabled_domains: &'a [ProtocolDomain],
    ) -> BoxFuture<'a, SessionTopology> {
        let topology = self.topology.clone();
        let expected_domains = self.expected_domains.clone();
        Box::pin(async move {
            assert_eq!(enabled_domains, expected_domains.as_slice());
            Ok(topology)
        })
    }
}

#[derive(Default)]
struct PassiveConnector;

#[derive(Default)]
struct PassiveTransport;

impl Transport for PassiveTransport {
    fn connect(&mut self) -> impl Future<Output = CoreResult<()>> + Send + '_ {
        async { Ok(()) }
    }

    fn recv(&mut self) -> impl Future<Output = CoreResult<tqsdk_core::RawFrame>> + Send + '_ {
        async {
            Err(tqsdk_core::ContractError::validation(
                "passive transport cannot recv",
            ))
        }
    }

    fn send(
        &mut self,
        _frame: tqsdk_core::OutboundFrame,
    ) -> impl Future<Output = CoreResult<()>> + Send + '_ {
        async {
            Err(tqsdk_core::ContractError::validation(
                "passive transport cannot send",
            ))
        }
    }

    fn close(&mut self) -> impl Future<Output = CoreResult<()>> + Send + '_ {
        async { Ok(()) }
    }
}

impl SessionRouteConnector for PassiveConnector {
    fn connect_route<'a>(
        &'a self,
        _route: &'a SessionRoute,
    ) -> BoxFuture<'a, Box<dyn DynTransport>> {
        Box::pin(async { Ok(Box::new(PassiveTransport) as Box<dyn DynTransport>) })
    }
}

type SeenDispatches = Arc<Mutex<Vec<(String, Vec<OutboundDispatch>)>>>;

#[derive(Clone, Default)]
struct RecordingExecutor {
    responses: BTreeMap<String, Vec<RuntimeInput>>,
    seen: SeenDispatches,
    error: Option<String>,
}

impl RecordingExecutor {
    fn with_response(mut self, route_label: impl Into<String>, inputs: Vec<RuntimeInput>) -> Self {
        self.responses.insert(route_label.into(), inputs);
        self
    }

    fn with_error(mut self, message: impl Into<String>) -> Self {
        self.error = Some(message.into());
        self
    }

    fn seen(&self) -> Vec<(String, Vec<OutboundDispatch>)> {
        self.seen.lock().unwrap().clone()
    }
}

impl RouteRequestExecutor for RecordingExecutor {
    fn execute<'a>(
        &'a self,
        route: &'a SessionRoute,
        requests: Vec<OutboundDispatch>,
    ) -> BoxFuture<'a, Vec<RuntimeInput>> {
        let responses = self
            .responses
            .get(&route.label)
            .cloned()
            .unwrap_or_default();
        let error = self.error.clone();
        let seen = Arc::clone(&self.seen);
        let route_label = route.label.clone();
        let recorded_requests = requests.clone();
        Box::pin(async move {
            seen.lock().unwrap().push((route_label, recorded_requests));
            if let Some(message) = error {
                return Err(tqsdk_core::ContractError::auth(message));
            }
            Ok(responses)
        })
    }
}

#[test]
fn session_runtime_executes_pending_http_route_requests_through_executor() {
    let handle = runtime_with_default_adapters();
    let runtime = SessionRuntime::new(handle.clone(), SessionBootstrap::new());
    let route_label = "instrument-schema-route";
    let resolver = StaticTopologyResolver {
        topology: SessionTopology::default().with_route(SessionRoute {
            label: route_label.to_string(),
            target: SessionTarget::Shared,
            domains: vec![ProtocolDomain::Schema],
            endpoint: SessionRouteEndpoint::Http {
                url: "https://schema.example".to_string(),
            },
        }),
        expected_domains: vec![ProtocolDomain::Schema],
    };
    let config = SessionConfig::new(EndpointConfig::new("https://auth.example"))
        .enable_domain(ProtocolDomain::Schema);
    let adapters = adapter_registry();
    let mut run = block_on(runtime.establish(
        &TestAuthProvider,
        &resolver,
        &PassiveConnector,
        &config,
        &adapters,
    ))
    .unwrap();
    let executor = RecordingExecutor::default().with_response(
        route_label,
        vec![RuntimeInput::Io(IoEvent {
            route: route_label.to_string(),
            domains: vec![ProtocolDomain::Schema],
            payload: tqsdk_core::InputPayload::Json(json!({
                "nodes": {
                    "quote": {
                        "fields": ["last_price", "ask_price1"]
                    }
                }
            })),
        })],
    );

    let command_id = block_on(
        handle.submit(RuntimeCommand::Schema(SchemaCommand::Refresh {
            schema_id: SchemaId::new("instrument-schema"),
            path: "/schema/instrument.json".to_string(),
        })),
    )
    .unwrap();
    block_on(runtime.flush_outbound(&mut run)).unwrap();

    let outcome = block_on(runtime.drive_pending_route_once(
        &mut run,
        route_label,
        &executor,
        vec![command_id],
        CommitScope::RealtimeUpdate,
    ))
    .unwrap();

    assert_eq!(outcome.requests.len(), 1);
    assert_eq!(executor.seen().len(), 1);
    assert_eq!(outcome.commits.len(), 1);
    assert_eq!(outcome.commits[0].scope, CommitScope::RealtimeUpdate);
    let command_segment = command_id.get().to_string();
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["runtime", "commands", command_segment.as_str(), "status"]),
        Some(&json!("completed"))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["schema", "instrument-schema", "nodes", "quote", "fields"]),
        Some(&json!(["last_price", "ask_price1"]))
    );
}

#[test]
fn session_runtime_executes_pending_query_route_requests_through_executor() {
    let handle = runtime_with_default_adapters();
    let runtime = SessionRuntime::new(handle.clone(), SessionBootstrap::new());
    let resolver = StaticTopologyResolver {
        topology: SessionTopology::default().with_route(SessionRoute {
            label: "query".to_string(),
            target: SessionTarget::Shared,
            domains: vec![ProtocolDomain::Query],
            endpoint: SessionRouteEndpoint::Http {
                url: "https://query.example/graphql".to_string(),
            },
        }),
        expected_domains: vec![ProtocolDomain::Query],
    };
    let config = SessionConfig::new(EndpointConfig::new("https://auth.example"))
        .enable_domain(ProtocolDomain::Query);
    let adapters = adapter_registry();
    let mut run = block_on(runtime.establish(
        &TestAuthProvider,
        &resolver,
        &PassiveConnector,
        &config,
        &adapters,
    ))
    .unwrap();
    let executor = RecordingExecutor::default().with_response(
        "query",
        vec![RuntimeInput::Io(IoEvent {
            route: "query".to_string(),
            domains: vec![ProtocolDomain::Query],
            payload: tqsdk_core::InputPayload::Json(json!({
                "query_id": "quotes-page-1",
                "data": {
                    "items": [{"instrument_id": "au2602"}],
                    "has_more": false,
                },
                "errors": [],
            })),
        })],
    );

    let command_id = block_on(handle.submit(RuntimeCommand::Query(QueryCommand::Fetch {
        query_id: QueryId::new("quotes-page-1"),
        query: "query Quotes { symbols { instrument_id } }".to_string(),
        variables: None,
    })))
    .unwrap();
    block_on(runtime.flush_outbound(&mut run)).unwrap();

    let outcome = block_on(runtime.drive_pending_route_once(
        &mut run,
        "query",
        &executor,
        vec![command_id],
        CommitScope::QueryRefresh,
    ))
    .unwrap();

    assert_eq!(outcome.requests.len(), 1);
    assert_eq!(executor.seen().len(), 1);
    assert_eq!(outcome.commits.len(), 1);
    assert_eq!(outcome.commits[0].scope, CommitScope::QueryRefresh);
    let command_segment = command_id.get().to_string();
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["runtime", "commands", command_segment.as_str(), "status"]),
        Some(&json!("completed"))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["query", "quotes-page-1", "items"]),
        Some(&json!([{ "instrument_id": "au2602" }]))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["query", "quotes-page-1", "errors"]),
        Some(&json!([]))
    );
}

#[test]
fn session_runtime_executes_pending_replay_route_requests_through_executor() {
    let handle = runtime_with_default_adapters();
    let runtime = SessionRuntime::new(handle.clone(), SessionBootstrap::new());
    let resolver = StaticTopologyResolver {
        topology: SessionTopology::default().with_route(SessionRoute {
            label: "replay:rb-1".to_string(),
            target: SessionTarget::Replay(ReplaySessionId::new("rb-1")),
            domains: vec![ProtocolDomain::Replay],
            endpoint: SessionRouteEndpoint::Replay {
                label: "replay-driver".to_string(),
            },
        }),
        expected_domains: vec![ProtocolDomain::Replay],
    };
    let config = SessionConfig::new(EndpointConfig::new("https://auth.example"))
        .enable_domain(ProtocolDomain::Replay);
    let adapters = adapter_registry();
    let mut run = block_on(runtime.establish(
        &TestAuthProvider,
        &resolver,
        &PassiveConnector,
        &config,
        &adapters,
    ))
    .unwrap();
    let executor = RecordingExecutor::default().with_response(
        "replay:rb-1",
        vec![RuntimeInput::Replay(ReplayEvent {
            label: "step",
            session_id: Some(ReplaySessionId::new("rb-1")),
            payload: Some(json!({
                "cursor": {
                    "seq": 1,
                    "state": "stepped"
                }
            })),
        })],
    );

    let command_id = block_on(handle.submit(RuntimeCommand::Replay(ReplayCommand::Step))).unwrap();
    block_on(runtime.flush_outbound(&mut run)).unwrap();

    let outcome = block_on(runtime.drive_pending_route_once(
        &mut run,
        "replay:rb-1",
        &executor,
        vec![command_id],
        CommitScope::ReplayStep,
    ))
    .unwrap();

    assert_eq!(outcome.requests.len(), 1);
    assert_eq!(executor.seen().len(), 1);
    assert_eq!(outcome.commits.len(), 1);
    assert_eq!(outcome.commits[0].scope, CommitScope::ReplayStep);
    let command_segment = command_id.get().to_string();
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["runtime", "commands", command_segment.as_str(), "status"]),
        Some(&json!("completed"))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["replay", "rb-1", "cursor", "seq"]),
        Some(&json!(1))
    );
}

#[test]
fn session_runtime_executes_pending_internal_route_requests_through_executor() {
    let handle = runtime_with_default_adapters();
    let runtime = SessionRuntime::new(handle.clone(), SessionBootstrap::new());
    let resolver = StaticTopologyResolver {
        topology: SessionTopology::default().with_route(SessionRoute {
            label: "system".to_string(),
            target: SessionTarget::Shared,
            domains: vec![ProtocolDomain::System],
            endpoint: SessionRouteEndpoint::Internal {
                label: "system-driver".to_string(),
            },
        }),
        expected_domains: vec![ProtocolDomain::System],
    };
    let config = SessionConfig::new(EndpointConfig::new("https://auth.example"))
        .enable_domain(ProtocolDomain::System);
    let adapters = adapter_registry();
    let mut run = block_on(runtime.establish(
        &TestAuthProvider,
        &resolver,
        &PassiveConnector,
        &config,
        &adapters,
    ))
    .unwrap();
    let executor = RecordingExecutor::default().with_response(
        "system",
        vec![RuntimeInput::Auth(AuthEvent {
            label: "refreshed",
            payload: Some(json!({
                "auth_id": "auth-2",
                "features": ["trade", "query"],
            })),
        })],
    );

    let command_id =
        block_on(handle.submit(RuntimeCommand::System(SystemCommand::RefreshAuth))).unwrap();
    block_on(runtime.flush_outbound(&mut run)).unwrap();

    let outcome = block_on(runtime.drive_pending_route_once(
        &mut run,
        "system",
        &executor,
        vec![command_id],
        CommitScope::SessionTransition,
    ))
    .unwrap();

    assert_eq!(outcome.requests.len(), 1);
    assert_eq!(executor.seen().len(), 1);
    assert_eq!(outcome.commits.len(), 1);
    assert_eq!(outcome.commits[0].scope, CommitScope::SessionTransition);
    let command_segment = command_id.get().to_string();
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["runtime", "commands", command_segment.as_str(), "status"]),
        Some(&json!("completed"))
    );
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["system", "auth", "refreshed", "auth_id"]),
        Some(&json!("auth-2"))
    );
}

#[test]
fn session_runtime_marks_pending_route_commands_failed_when_executor_errors() {
    let handle = runtime_with_default_adapters();
    let runtime = SessionRuntime::new(handle.clone(), SessionBootstrap::new());
    let resolver = StaticTopologyResolver {
        topology: SessionTopology::default().with_route(SessionRoute {
            label: "instrument-schema".to_string(),
            target: SessionTarget::Shared,
            domains: vec![ProtocolDomain::Schema],
            endpoint: SessionRouteEndpoint::Http {
                url: "https://schema.example".to_string(),
            },
        }),
        expected_domains: vec![ProtocolDomain::Schema],
    };
    let config = SessionConfig::new(EndpointConfig::new("https://auth.example"))
        .enable_domain(ProtocolDomain::Schema);
    let adapters = adapter_registry();
    let mut run = block_on(runtime.establish(
        &TestAuthProvider,
        &resolver,
        &PassiveConnector,
        &config,
        &adapters,
    ))
    .unwrap();
    let executor = RecordingExecutor::default().with_error("schema executor failed");

    let command_id = block_on(
        handle.submit(RuntimeCommand::Schema(SchemaCommand::Refresh {
            schema_id: SchemaId::new("instrument-schema"),
            path: "/schema/instrument.json".to_string(),
        })),
    )
    .unwrap();
    block_on(runtime.flush_outbound(&mut run)).unwrap();

    let err = block_on(runtime.drive_pending_route_once(
        &mut run,
        "instrument-schema",
        &executor,
        vec![command_id],
        CommitScope::RealtimeUpdate,
    ))
    .unwrap_err();

    assert_eq!(err.to_string(), "auth error: schema executor failed");
    let command_segment = command_id.get().to_string();
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["runtime", "commands", command_segment.as_str(), "status"]),
        Some(&json!("failed"))
    );
    assert_eq!(
        handle.latest_snapshot().get([
            "runtime",
            "commands",
            command_segment.as_str(),
            "detail",
            "route"
        ]),
        Some(&json!("instrument-schema"))
    );
}

#[test]
fn session_runtime_marks_failed_pending_route_request_even_without_caused_by_ids() {
    let handle = runtime_with_default_adapters();
    let runtime = SessionRuntime::new(handle.clone(), SessionBootstrap::new());
    let resolver = StaticTopologyResolver {
        topology: SessionTopology::default().with_route(SessionRoute {
            label: "instrument-schema".to_string(),
            target: SessionTarget::Shared,
            domains: vec![ProtocolDomain::Schema],
            endpoint: SessionRouteEndpoint::Http {
                url: "https://schema.example".to_string(),
            },
        }),
        expected_domains: vec![ProtocolDomain::Schema],
    };
    let config = SessionConfig::new(EndpointConfig::new("https://auth.example"))
        .enable_domain(ProtocolDomain::Schema);
    let adapters = adapter_registry();
    let mut run = block_on(runtime.establish(
        &TestAuthProvider,
        &resolver,
        &PassiveConnector,
        &config,
        &adapters,
    ))
    .unwrap();
    let executor = RecordingExecutor::default().with_error("schema executor failed");

    let command_id = block_on(
        handle.submit(RuntimeCommand::Schema(SchemaCommand::Refresh {
            schema_id: SchemaId::new("instrument-schema"),
            path: "/schema/instrument.json".to_string(),
        })),
    )
    .unwrap();
    block_on(runtime.flush_outbound(&mut run)).unwrap();

    let err = block_on(runtime.drive_pending_route_once(
        &mut run,
        "instrument-schema",
        &executor,
        vec![],
        CommitScope::RealtimeUpdate,
    ))
    .unwrap_err();

    assert_eq!(err.to_string(), "auth error: schema executor failed");
    let command_segment = command_id.get().to_string();
    assert_eq!(
        handle
            .latest_snapshot()
            .get(["runtime", "commands", command_segment.as_str(), "status"]),
        Some(&json!("failed"))
    );
}

fn runtime_with_default_adapters() -> RuntimeHandle {
    RuntimeHandle::with_adapters(adapter_registry())
}

fn adapter_registry() -> AdapterRegistry {
    let mut registry = AdapterRegistry::new();
    registry.register_default_adapters();
    registry
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
