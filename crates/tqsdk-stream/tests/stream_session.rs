use std::time::Duration;

use futures::StreamExt;
use tqsdk_session::SessionFacadeError;
use tqsdk_stream::{SessionReconnectEvent, TradeSessionEvent, TradeSessionEventUpdate};

mod support;

async fn next_trade_session_event(
    events: &mut tqsdk_stream::TradeSessionEventStream,
) -> TradeSessionEventUpdate {
    tokio::time::timeout(Duration::from_millis(50), events.next())
        .await
        .expect("trade session event stream should not stall")
        .expect("trade session event stream should yield an item")
        .expect("trade session event stream should decode an event")
}

#[tokio::test(flavor = "current_thread")]
async fn trade_session_event_stream_emits_trade_objects_notifications_and_reconnects() {
    let stream = support::core_seed::seeded_stream();
    let mut events = stream.trade_session_event_stream("sim").unwrap();

    support::core_seed::seed_notification_commit_for_user(&stream, "notify-x", "paper");

    let idle = tokio::time::timeout(Duration::from_millis(10), events.next()).await;
    assert!(idle.is_err());

    support::core_seed::seed_trade_snapshot(&stream, "sim", "SHFE.au2602");
    support::core_seed::seed_notification_commit_for_user(&stream, "notify-1", "sim");
    support::core_seed::seed_session_reconnect_commit(&stream, "transport-error");

    let mut saw_account = false;
    let mut saw_position = false;
    let mut saw_order = false;
    let mut saw_trade = false;
    let mut saw_notification = false;
    let mut saw_reconnect = false;

    for _ in 0..6 {
        let update = next_trade_session_event(&mut events).await;
        assert!(update.commit.is_some());

        match update.event {
            TradeSessionEvent::TradeObject(event) => match event {
                tqsdk_stream::TradeObjectEvent::Account(account) => {
                    saw_account = true;
                    assert_eq!(account.user_id, "sim");
                }
                tqsdk_stream::TradeObjectEvent::Position(position) => {
                    saw_position = true;
                    assert_eq!(position.instrument_id, "ao2602");
                }
                tqsdk_stream::TradeObjectEvent::Order(order) => {
                    saw_order = true;
                    assert_eq!(order.order_id, "order-1");
                }
                tqsdk_stream::TradeObjectEvent::Trade(trade) => {
                    saw_trade = true;
                    assert_eq!(trade.trade_id, "trade-1");
                }
                other => panic!("unexpected trade object event variant: {other:?}"),
            },
            TradeSessionEvent::Notification(notification) => {
                saw_notification = true;
                assert_eq!(notification.user_id, "sim");
                assert_eq!(notification.content, "connected");
            }
            TradeSessionEvent::Reconnect(reconnect) => {
                saw_reconnect = true;
                assert_eq!(
                    reconnect,
                    SessionReconnectEvent {
                        attempt: 1,
                        scheduled_backoff_ms: 250,
                        max_attempts: Some(5),
                        exhausted: false,
                        detail: serde_json::json!({
                            "reason": "transport-error",
                        }),
                    }
                );
            }
            TradeSessionEvent::SessionError(error) => {
                panic!("unexpected session error event: {error}");
            }
            _ => panic!("unexpected future trade session event variant"),
        }
    }

    assert!(saw_account);
    assert!(saw_position);
    assert!(saw_order);
    assert!(saw_trade);
    assert!(saw_notification);
    assert!(saw_reconnect);
}

#[tokio::test(flavor = "current_thread")]
async fn trade_session_event_stream_emits_session_error_without_commit() {
    let stream = support::core_seed::seeded_stream();
    let mut events = stream.trade_session_event_stream("sim").unwrap();

    stream.emit_session_error_for_test(SessionFacadeError::InvalidState(
        "synthetic transport failure",
    ));

    let update = next_trade_session_event(&mut events).await;
    assert!(update.commit.is_none());
    match update.event {
        TradeSessionEvent::SessionError(error) => {
            assert_eq!(
                error,
                SessionFacadeError::InvalidState("synthetic transport failure")
            );
        }
        other => panic!("unexpected trade session event variant: {other:?}"),
    }
}
