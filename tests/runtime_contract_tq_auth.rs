use std::future::Future;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::pin::Pin;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use tqsdk_runtime_contract::{AuthId, AuthProvider, PasswordCredentials, TqAuthProvider};

#[test]
fn tq_auth_provider_authenticates_against_token_endpoint() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut buf = [0_u8; 4096];
        let n = stream.read(&mut buf).unwrap();
        let request = String::from_utf8_lossy(&buf[..n]).to_string();

        assert!(request.starts_with("POST /auth/realms/shinnytech/protocol/openid-connect/token HTTP/1.1"));
        assert!(request.contains("grant_type=password"));
        assert!(request.contains("username=demo"));
        assert!(request.contains("password=secret"));
        assert!(request.contains("client_id=shinny_tq"));
        assert!(request.contains("client_secret=be30b9f4-6862-488a-99ad-21bde0400081"));

        let body = r#"{"access_token":"eyJhbGciOiJub25lIn0.eyJzdWIiOiJ1c2VyLTEiLCJncmFudHMiOnsiZmVhdHVyZXMiOlsiZnV0ciIsInNlYyJdfX0.sig","refresh_token":"refresh-token"}"#;
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        )
        .unwrap();
    });

    let provider = TqAuthProvider::new(PasswordCredentials::new("demo", "secret"))
        .with_auth_url(format!("http://{addr}"));
    let context = block_on(provider.authenticate()).unwrap();

    assert_eq!(context.access_token(), "eyJhbGciOiJub25lIn0.eyJzdWIiOiJ1c2VyLTEiLCJncmFudHMiOnsiZmVhdHVyZXMiOlsiZnV0ciIsInNlYyJdfX0.sig");
    assert_eq!(context.auth_id().map(AuthId::as_str), Some("user-1"));
    assert_eq!(context.features(), &["futr".to_string(), "sec".to_string()]);

    server.join().unwrap();
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
