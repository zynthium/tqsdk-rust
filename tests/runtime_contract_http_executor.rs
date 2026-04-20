use std::future::Future;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::pin::Pin;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use serde_json::json;
use tqsdk_runtime_contract::{
    CommandId, HttpMethod, HttpRequest, InputPayload, IoEvent, OutboundDispatch, OutboundRequest,
    ProtocolDomain, ReqwestHttpExecutor, RouteRequestExecutor, SessionRoute,
    SessionRouteEndpoint, SessionTarget,
};

#[test]
fn reqwest_http_executor_posts_query_requests_and_wraps_query_id() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let request = read_http_request(&mut stream);
        let normalized = request.to_ascii_lowercase();

        assert!(request.starts_with("POST /graphql HTTP/1.1"), "{request}");
        assert!(normalized.contains("accept: application/json"), "{request}");
        assert!(normalized.contains("user-agent: tqsdk-python 3.8.1"), "{request}");
        assert!(normalized.contains("content-type: application/json"), "{request}");
        assert!(request.contains("\"query_id\":\"quotes-page-1\""), "{request}");
        assert!(request.contains("\"query\":\"query Quotes { symbols { instrument_id } }\""), "{request}");

        write_http_ok(
            &mut stream,
            r#"{"data":{"items":[{"instrument_id":"au2602"}],"has_more":false},"errors":[]}"#,
        );
    });

    let executor = ReqwestHttpExecutor::default();
    let route = SessionRoute {
        label: "query".to_string(),
        target: SessionTarget::Shared,
        domains: vec![ProtocolDomain::Query],
        endpoint: SessionRouteEndpoint::Http {
            url: format!("http://{addr}/graphql"),
        },
    };
    let inputs = block_on(executor.execute(
        &route,
        vec![OutboundDispatch {
            command_id: CommandId::new(1),
            domain: ProtocolDomain::Query,
            request: OutboundRequest::Http(HttpRequest {
                method: HttpMethod::Post,
                path: None,
                body: Some(json!({
                    "aid": "ins_query",
                    "query_id": "quotes-page-1",
                    "query": "query Quotes { symbols { instrument_id } }",
                })),
            }),
        }],
    ))
    .unwrap();

    assert_eq!(
        inputs,
        vec![tqsdk_runtime_contract::RuntimeInput::Io(IoEvent {
            route: "query".to_string(),
            domains: vec![ProtocolDomain::Query],
            payload: InputPayload::Json(json!({
                "query_id": "quotes-page-1",
                "data": {
                    "items": [{"instrument_id": "au2602"}],
                    "has_more": false,
                },
                "errors": [],
            })),
        })]
    );

    server.join().unwrap();
}

#[test]
fn reqwest_http_executor_joins_schema_paths_for_get_requests() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let request = read_http_request(&mut stream);
        let normalized = request.to_ascii_lowercase();

        assert!(request.starts_with("GET /schema/instrument.json HTTP/1.1"), "{request}");
        assert!(normalized.contains("accept: application/json"), "{request}");
        assert!(normalized.contains("user-agent: tqsdk-python 3.8.1"), "{request}");

        write_http_ok(
            &mut stream,
            r#"{"nodes":{"quote":{"fields":["last_price","ask_price1"]}}}"#,
        );
    });

    let executor = ReqwestHttpExecutor::default();
    let route = SessionRoute {
        label: "schema".to_string(),
        target: SessionTarget::Shared,
        domains: vec![ProtocolDomain::Schema],
        endpoint: SessionRouteEndpoint::Http {
            url: format!("http://{addr}"),
        },
    };
    let inputs = block_on(executor.execute(
        &route,
        vec![OutboundDispatch {
            command_id: CommandId::new(2),
            domain: ProtocolDomain::Schema,
            request: OutboundRequest::Http(HttpRequest {
                method: HttpMethod::Get,
                path: Some("/schema/instrument.json".to_string()),
                body: None,
            }),
        }],
    ))
    .unwrap();

    assert_eq!(
        inputs,
        vec![tqsdk_runtime_contract::RuntimeInput::Io(IoEvent {
            route: "schema".to_string(),
            domains: vec![ProtocolDomain::Schema],
            payload: InputPayload::Json(json!({
                "nodes": {
                    "quote": {
                        "fields": ["last_price", "ask_price1"],
                    }
                }
            })),
        })]
    );

    server.join().unwrap();
}

fn read_http_request(stream: &mut std::net::TcpStream) -> String {
    let mut buffer = Vec::new();
    let mut header_end = None;
    let mut expected_body_len = 0usize;

    loop {
        let mut chunk = [0u8; 1024];
        let read = stream.read(&mut chunk).unwrap();
        assert!(read > 0);
        buffer.extend_from_slice(&chunk[..read]);

        if header_end.is_none()
            && let Some(pos) = buffer.windows(4).position(|window| window == b"\r\n\r\n")
        {
            header_end = Some(pos + 4);
            let headers = String::from_utf8_lossy(&buffer[..pos + 4]);
            expected_body_len = headers
                .lines()
                .find_map(|line| {
                    line.strip_prefix("Content-Length: ")
                        .or_else(|| line.strip_prefix("content-length: "))
                })
                .and_then(|value| value.trim().parse::<usize>().ok())
                .unwrap_or(0);
        }

        if let Some(end) = header_end
            && buffer.len() >= end + expected_body_len
        {
            return String::from_utf8(buffer).unwrap();
        }
    }
}

fn write_http_ok(stream: &mut std::net::TcpStream, body: &str) {
    write!(
        stream,
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        body.len(),
        body
    )
    .unwrap();
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

static NOOP_WAKER_VTABLE: RawWakerVTable =
    RawWakerVTable::new(noop_clone, noop, noop, noop);
