use crate::events::{InputPayload, InternalEvent, IoEvent, RuntimeInput};
use crate::ids::ProtocolDomain;
use crate::{ContractError, Result};
use serde_json::{Value, json};

use super::topology::SessionRoute;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RawFrame {
    Text(String),
    Binary(Vec<u8>),
    Ping,
    Pong,
    Close,
}

pub(super) fn map_raw_frame_to_input(
    route: &SessionRoute,
    frame: RawFrame,
) -> Result<Option<RuntimeInput>> {
    match frame {
        RawFrame::Text(text) => Ok(Some(RuntimeInput::Io(IoEvent {
            route: route.label.clone(),
            domains: route.domains.clone(),
            payload: parse_text_payload(text)?,
        }))),
        RawFrame::Binary(bytes) => Ok(Some(RuntimeInput::Io(IoEvent {
            route: route.label.clone(),
            domains: route.domains.clone(),
            payload: parse_binary_payload(bytes)?,
        }))),
        RawFrame::Ping => Ok(Some(RuntimeInput::Internal(InternalEvent {
            label: "transport-ping",
            payload: Some(json!({
                "route": route.label,
                "domains": route.domains.iter().copied().map(ProtocolDomain::as_str).collect::<Vec<_>>(),
            })),
        }))),
        RawFrame::Pong => Ok(Some(RuntimeInput::Internal(InternalEvent {
            label: "transport-pong",
            payload: Some(json!({
                "route": route.label,
                "domains": route.domains.iter().copied().map(ProtocolDomain::as_str).collect::<Vec<_>>(),
            })),
        }))),
        RawFrame::Close => Ok(Some(RuntimeInput::Internal(InternalEvent {
            label: "transport-close",
            payload: Some(json!({
                "route": route.label,
                "domains": route.domains.iter().copied().map(ProtocolDomain::as_str).collect::<Vec<_>>(),
            })),
        }))),
    }
}

fn parse_text_payload(text: String) -> Result<InputPayload> {
    serde_json::from_str::<Value>(&text)
        .map(InputPayload::Json)
        .map_err(|err| {
            ContractError::transport(format!("invalid websocket JSON text frame: {err}"))
        })
}

fn parse_binary_payload(bytes: Vec<u8>) -> Result<InputPayload> {
    match serde_json::from_slice::<Value>(&bytes) {
        Ok(value) => Ok(InputPayload::Json(value)),
        Err(_) => Ok(InputPayload::Binary(bytes)),
    }
}

#[cfg(fuzzing)]
#[doc(hidden)]
pub fn __fuzz_parse_raw_frame_payload(
    frame: RawFrame,
    domain: ProtocolDomain,
) -> Result<Option<RuntimeInput>> {
    let route = SessionRoute {
        label: "fuzz".to_string(),
        target: super::topology::SessionTarget::Shared,
        domains: vec![domain],
        endpoint: super::topology::SessionRouteEndpoint::Internal {
            label: "fuzz".to_string(),
        },
    };
    map_raw_frame_to_input(&route, frame)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        InputPayload, IoEvent, ProtocolDomain, RawFrame, RuntimeInput, map_raw_frame_to_input,
        parse_binary_payload, parse_text_payload,
    };
    use crate::transport::{
        SessionRoute, SessionRouteEndpoint, SessionTarget, WebSocketConnectOptions,
    };

    #[test]
    fn parse_text_payload_decodes_json_when_possible() {
        let payload = parse_text_payload(r#"{"aid":"rtn_data"}"#.to_string()).unwrap();
        assert_eq!(payload, InputPayload::Json(json!({ "aid": "rtn_data" })));
    }

    #[test]
    fn parse_text_payload_rejects_invalid_json() {
        let error = parse_text_payload("{not-json".to_string()).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("invalid websocket JSON text frame")
        );
    }

    #[test]
    fn parse_binary_payload_decodes_json_when_possible() {
        let payload = parse_binary_payload(br#"{"aid":"rtn_data"}"#.to_vec()).unwrap();
        assert_eq!(payload, InputPayload::Json(json!({ "aid": "rtn_data" })));
    }

    #[test]
    fn parse_binary_payload_preserves_non_json_bytes() {
        let payload = parse_binary_payload(vec![0_u8, 1, 2, 3]).unwrap();
        assert_eq!(payload, InputPayload::Binary(vec![0_u8, 1, 2, 3]));
    }

    #[test]
    fn map_raw_binary_frame_to_json_io_when_payload_is_json() {
        let route = SessionRoute {
            label: "market".to_string(),
            target: SessionTarget::Shared,
            domains: vec![ProtocolDomain::Market],
            endpoint: SessionRouteEndpoint::WebSocket {
                url: "wss://market.example".to_string(),
                connect: WebSocketConnectOptions::default(),
            },
        };

        let input = map_raw_frame_to_input(
            &route,
            RawFrame::Binary(
                br#"{"aid":"rtn_data","data":[{"quotes":{"SHFE.au2602":{"last_price":618.5}}}]}"#
                    .to_vec(),
            ),
        )
        .unwrap();

        assert!(matches!(
            input,
            Some(RuntimeInput::Io(IoEvent {
                payload: InputPayload::Json(_),
                ..
            }))
        ));
    }
}
