use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpListener;

use chrono::NaiveDate;
use tqsdk_core::{AdapterRegistry, EndpointConfig, RuntimeHandle, SessionConfig};

use crate::{
    SessionClient, SessionClientBuilder,
    client::SessionClientContext,
    direct_query::{EdbDataAlign, EdbDataFill, SymbolRankingType},
};

use super::SessionServiceEndpoints;

#[test]
fn get_trading_calendar_fetches_holiday_file_and_marks_trading_days() {
    run_on_tokio(async {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let server = std::thread::spawn(move || {
            let (mut holiday_stream, _) = listener.accept().unwrap();
            let holiday_request = read_http_request(&mut holiday_stream);
            let normalized_holiday = holiday_request.to_ascii_lowercase();
            assert!(
                holiday_request.starts_with("GET /holiday.json HTTP/1.1"),
                "{holiday_request}"
            );
            assert!(
                !normalized_holiday.contains("authorization:"),
                "{holiday_request}"
            );
            write_http_ok(
                &mut holiday_stream,
                r#"["2026-05-01","2026-05-02","2026-05-03"]"#,
            );
        });

        let client = test_client(
            format!("http://{addr}"),
            SessionServiceEndpoints {
                holiday_url: format!("http://{addr}/holiday.json"),
                ..SessionServiceEndpoints::default()
            },
        );

        let days = client
            .get_trading_calendar(
                NaiveDate::from_ymd_opt(2026, 4, 30).unwrap(),
                NaiveDate::from_ymd_opt(2026, 5, 4).unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(days.len(), 5);
        assert_eq!(days[0].date, NaiveDate::from_ymd_opt(2026, 4, 30).unwrap());
        assert!(days[0].trading);
        assert_eq!(days[1].date, NaiveDate::from_ymd_opt(2026, 5, 1).unwrap());
        assert!(!days[1].trading);
        assert_eq!(days[2].date, NaiveDate::from_ymd_opt(2026, 5, 2).unwrap());
        assert!(!days[2].trading);
        assert_eq!(days[3].date, NaiveDate::from_ymd_opt(2026, 5, 3).unwrap());
        assert!(!days[3].trading);
        assert_eq!(days[4].date, NaiveDate::from_ymd_opt(2026, 5, 4).unwrap());
        assert!(days[4].trading);

        server.join().unwrap();
    });
}

#[test]
fn get_trading_calendar_rejects_invalid_ranges_before_http() {
    run_on_tokio(async {
        let client = test_client(
            "http://127.0.0.1:1".to_string(),
            SessionServiceEndpoints {
                holiday_url: "http://127.0.0.1:1/holiday.json".to_string(),
                ..SessionServiceEndpoints::default()
            },
        );

        let error = client
            .get_trading_calendar(
                NaiveDate::from_ymd_opt(2026, 5, 2).unwrap(),
                NaiveDate::from_ymd_opt(2026, 5, 1).unwrap(),
            )
            .await
            .expect_err("invalid range should fail");

        assert!(
            error
                .to_string()
                .contains("start_dt must be less than or equal to end_dt")
        );
    });
}

#[test]
fn get_trading_calendar_rejects_empty_holiday_payload() {
    assert_calendar_payload_error("[]", "trading calendar holiday payload is empty");
}

#[test]
fn get_trading_calendar_rejects_non_string_holiday_entries() {
    assert_calendar_payload_error(
        r#"["2026-05-01", 1]"#,
        "trading calendar holiday entry must be a string",
    );
}

#[test]
fn get_trading_calendar_rejects_invalid_holiday_dates() {
    assert_calendar_payload_error(r#"["2026-02-30"]"#, "invalid date string `2026-02-30`");
}

#[test]
fn get_trading_calendar_rejects_ranges_outside_holiday_years() {
    assert_calendar_range_error(
        r#"["2026-05-01"]"#,
        NaiveDate::from_ymd_opt(2027, 1, 1).unwrap(),
        NaiveDate::from_ymd_opt(2027, 1, 2).unwrap(),
        "trading calendar supports 2026-01-01 to 2026-12-31",
    );
}

#[test]
fn get_trading_calendar_caches_holiday_payload_per_client() {
    run_on_tokio(async {
        let (holiday_url, server) =
            spawn_single_holiday_server(r#"["2026-05-01","2026-05-02","2026-05-03"]"#);
        let client = test_client(
            "http://127.0.0.1:1".to_string(),
            SessionServiceEndpoints {
                holiday_url,
                ..SessionServiceEndpoints::default()
            },
        );

        for _ in 0..2 {
            let days = client
                .get_trading_calendar(
                    NaiveDate::from_ymd_opt(2026, 5, 1).unwrap(),
                    NaiveDate::from_ymd_opt(2026, 5, 1).unwrap(),
                )
                .await
                .expect("calendar should use fetched or cached holidays");
            assert_eq!(days.len(), 1);
            assert!(!days[0].trading);
        }

        server.join().unwrap();
    });
}

#[test]
fn session_builder_holiday_url_overrides_default_endpoint() {
    run_on_tokio(async {
        let (holiday_url, server) = spawn_single_holiday_server(r#"["2026-05-01"]"#);
        let client = SessionClientBuilder::new("user", "pass")
            .holiday_url(holiday_url)
            .build()
            .expect("session should build");

        let days = client
            .get_trading_calendar(
                NaiveDate::from_ymd_opt(2026, 5, 1).unwrap(),
                NaiveDate::from_ymd_opt(2026, 5, 1).unwrap(),
            )
            .await
            .expect("calendar should use overridden endpoint");

        assert_eq!(days.len(), 1);
        assert!(!days[0].trading);
        server.join().unwrap();
    });
}

#[test]
fn query_symbol_settlement_sends_expected_query_parameters() {
    run_on_tokio(async {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let server = std::thread::spawn(move || {
            let (mut auth_stream, _) = listener.accept().unwrap();
            let auth_request = read_http_request(&mut auth_stream);
            assert!(
                auth_request.starts_with(
                    "POST /auth/realms/shinnytech/protocol/openid-connect/token HTTP/1.1"
                ),
                "{auth_request}"
            );
            write_http_ok(&mut auth_stream, token_response_body());

            let (mut settlement_stream, _) = listener.accept().unwrap();
            let settlement_request = read_http_request(&mut settlement_stream);
            let normalized = settlement_request.to_ascii_lowercase();
            assert!(
                settlement_request.starts_with("GET /mss?"),
                "{settlement_request}"
            );
            assert!(
                settlement_request.contains("symbols=SHFE.au2602%2CDCE.m2609"),
                "{settlement_request}"
            );
            assert!(
                settlement_request.contains("days=2"),
                "{settlement_request}"
            );
            assert!(
                settlement_request.contains("start_date=20260401"),
                "{settlement_request}"
            );
            assert!(
                normalized.contains("authorization: bearer"),
                "{settlement_request}"
            );
            write_http_ok(
                &mut settlement_stream,
                r#"{
                    "20260402": {"DCE.m2609": 88, "SHFE.au2602": "125.6"},
                    "20260401": {"SHFE.au2602": 123.4, "DCE.m2609": null}
                }"#,
            );
        });

        let client = test_client(
            format!("http://{addr}"),
            SessionServiceEndpoints {
                settlement_url: format!("http://{addr}/mss"),
                ..SessionServiceEndpoints::default()
            },
        );

        let rows = client
            .query_symbol_settlement(
                &["SHFE.au2602", "DCE.m2609"],
                2,
                Some(NaiveDate::from_ymd_opt(2026, 4, 1).unwrap()),
            )
            .await
            .unwrap();

        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].datetime, "20260401");
        assert_eq!(rows[0].symbol, "SHFE.au2602");
        assert_eq!(rows[0].settlement, 123.4);
        assert_eq!(rows[1].datetime, "20260402");
        assert_eq!(rows[1].symbol, "DCE.m2609");
        assert_eq!(rows[1].settlement, 88.0);
        assert_eq!(rows[2].datetime, "20260402");
        assert_eq!(rows[2].symbol, "SHFE.au2602");
        assert_eq!(rows[2].settlement, 125.6);

        server.join().unwrap();
    });
}

#[test]
fn query_symbol_ranking_keeps_requested_ranking_rows_sorted() {
    run_on_tokio(async {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let server = std::thread::spawn(move || {
            let (mut auth_stream, _) = listener.accept().unwrap();
            let auth_request = read_http_request(&mut auth_stream);
            assert!(
                auth_request.starts_with(
                    "POST /auth/realms/shinnytech/protocol/openid-connect/token HTTP/1.1"
                ),
                "{auth_request}"
            );
            write_http_ok(&mut auth_stream, token_response_body());

            let (mut ranking_stream, _) = listener.accept().unwrap();
            let ranking_request = read_http_request(&mut ranking_stream);
            let normalized = ranking_request.to_ascii_lowercase();
            assert!(
                ranking_request.starts_with("GET /srs?"),
                "{ranking_request}"
            );
            assert!(
                ranking_request.contains("symbol=SHFE.cu2606"),
                "{ranking_request}"
            );
            assert!(ranking_request.contains("days=1"), "{ranking_request}");
            assert!(
                ranking_request.contains("broker=DemoBroker"),
                "{ranking_request}"
            );
            assert!(
                normalized.contains("authorization: bearer"),
                "{ranking_request}"
            );
            write_http_ok(
                &mut ranking_stream,
                r#"{
                    "20260401": {
                        "SHFE.cu2606": {
                            "volume_ranking": {
                                "DemoBroker": {"volume": 20, "varvolume": 3, "ranking": 2},
                                "SecondBroker": {"volume": 30, "varvolume": 4, "ranking": 1}
                            },
                            "long_ranking": {
                                "DemoBroker": {"volume": 50, "varvolume": -2, "ranking": 4}
                            }
                        }
                    }
                }"#,
            );
        });

        let client = test_client(
            format!("http://{addr}"),
            SessionServiceEndpoints {
                ranking_url: format!("http://{addr}/srs"),
                ..SessionServiceEndpoints::default()
            },
        );

        let rows = client
            .query_symbol_ranking(
                "SHFE.cu2606",
                SymbolRankingType::Volume,
                1,
                None,
                Some("DemoBroker"),
            )
            .await
            .unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].broker, "SecondBroker");
        assert_eq!(rows[0].volume_ranking, 1.0);
        assert_eq!(rows[1].broker, "DemoBroker");
        assert_eq!(rows[1].volume_ranking, 2.0);
        assert_eq!(rows[1].long_ranking, 4.0);

        server.join().unwrap();
    });
}

#[test]
fn query_edb_data_aligns_and_fills_day_series_locally() {
    run_on_tokio(async {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let server = std::thread::spawn(move || {
            let (mut auth_stream, _) = listener.accept().unwrap();
            let auth_request = read_http_request(&mut auth_stream);
            assert!(
                auth_request.starts_with(
                    "POST /auth/realms/shinnytech/protocol/openid-connect/token HTTP/1.1"
                ),
                "{auth_request}"
            );
            write_http_ok(&mut auth_stream, token_response_body());

            let (mut edb_stream, _) = listener.accept().unwrap();
            let edb_request = read_http_request(&mut edb_stream);
            let normalized = edb_request.to_ascii_lowercase();
            assert!(
                edb_request.starts_with("POST /data/index_data HTTP/1.1"),
                "{edb_request}"
            );
            assert!(
                normalized.contains("authorization: bearer"),
                "{edb_request}"
            );
            assert!(edb_request.contains("\"ids\":[472,497]"), "{edb_request}");
            assert!(
                edb_request.contains("\"start\":\"2026-04-01\""),
                "{edb_request}"
            );
            assert!(
                edb_request.contains("\"end\":\"2026-04-03\""),
                "{edb_request}"
            );
            write_http_ok(
                &mut edb_stream,
                r#"{
                    "error_code": 0,
                    "data": {
                        "ids": [472, 497],
                        "values": {
                            "2026-04-01": [1.0, 10.0],
                            "2026-04-03": [3.0, 30.0]
                        }
                    }
                }"#,
            );
        });

        let client = test_client(
            format!("http://{addr}"),
            SessionServiceEndpoints {
                edb_url: format!("http://{addr}/data/index_data"),
                ..SessionServiceEndpoints::default()
            },
        );

        let rows = client
            .query_edb_data(
                &[472, 497],
                NaiveDate::from_ymd_opt(2026, 4, 1).unwrap(),
                NaiveDate::from_ymd_opt(2026, 4, 3).unwrap(),
                Some(EdbDataAlign::Day),
                Some(EdbDataFill::Forward),
            )
            .await
            .unwrap();

        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].date, "2026-04-01");
        assert_eq!(rows[0].values, HashMap::from([(472, 1.0), (497, 10.0)]));
        assert_eq!(rows[1].date, "2026-04-02");
        assert_eq!(rows[1].values, HashMap::from([(472, 1.0), (497, 10.0)]));
        assert_eq!(rows[2].date, "2026-04-03");
        assert_eq!(rows[2].values, HashMap::from([(472, 3.0), (497, 30.0)]));

        server.join().unwrap();
    });
}

fn assert_calendar_payload_error(body: &'static str, expected: &str) {
    assert_calendar_range_error(
        body,
        NaiveDate::from_ymd_opt(2026, 5, 1).unwrap(),
        NaiveDate::from_ymd_opt(2026, 5, 1).unwrap(),
        expected,
    );
}

fn assert_calendar_range_error(
    body: &'static str,
    start_dt: NaiveDate,
    end_dt: NaiveDate,
    expected: &str,
) {
    run_on_tokio(async {
        let (holiday_url, server) = spawn_single_holiday_server(body);
        let client = test_client(
            "http://127.0.0.1:1".to_string(),
            SessionServiceEndpoints {
                holiday_url,
                ..SessionServiceEndpoints::default()
            },
        );

        let error = client
            .get_trading_calendar(start_dt, end_dt)
            .await
            .expect_err("calendar should reject invalid payload or range");

        assert!(
            error.to_string().contains(expected),
            "expected `{expected}` in `{error}`"
        );
        server.join().unwrap();
    });
}

fn spawn_single_holiday_server(body: &'static str) -> (String, std::thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut holiday_stream, _) = listener.accept().unwrap();
        let holiday_request = read_http_request(&mut holiday_stream);
        let normalized = holiday_request.to_ascii_lowercase();
        assert!(
            holiday_request.starts_with("GET /holiday.json HTTP/1.1"),
            "{holiday_request}"
        );
        assert!(!normalized.contains("authorization:"), "{holiday_request}");
        write_http_ok(&mut holiday_stream, body);
    });

    (format!("http://{addr}/holiday.json"), server)
}

fn test_client(auth_url: String, service_endpoints: SessionServiceEndpoints) -> SessionClient {
    let mut adapters = AdapterRegistry::new();
    adapters.register_default_adapters();
    let handle = RuntimeHandle::with_adapters(adapters);
    let endpoints = EndpointConfig::new(auth_url);
    let context = SessionClientContext::new_with_services(
        "user",
        "pass",
        endpoints.clone(),
        service_endpoints,
    );
    SessionClient::new_live(handle, context, SessionConfig::new(endpoints), Vec::new()).unwrap()
}

fn token_response_body() -> &'static str {
    r#"{"access_token":"eyJhbGciOiJub25lIn0.eyJzdWIiOiJ1c2VyLTEiLCJncmFudHMiOnsiZmVhdHVyZXMiOlsiZnV0ciIsInNlYyJdfX0.sig","refresh_token":"refresh-token"}"#
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

fn run_on_tokio<F>(future: F) -> F::Output
where
    F: std::future::Future,
{
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(future)
}
