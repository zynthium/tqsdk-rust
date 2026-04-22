use std::time::Duration;

use serde_json::json;
use tqsdk_core::{
    AdapterRegistry, CommitScope, InputPayload, IoEvent, ProtocolDomain, RuntimeHandle,
    RuntimeInput, TradeDirection, TradeOffset,
};
use tqsdk_session::{SessionClient, SessionFacadeConfig};
use tqsdk_task::{
    OffsetPriority, TargetPosExecutionReport, TargetPosExecutionStep, TargetPosScheduleStep,
    TargetPosScheduler, TargetPosSchedulerConfig, TaskError, TaskHost, TaskKind, VolumeSplitPolicy,
};
use tqsdk_wait::TqApi;

fn seeded_host() -> TaskHost {
    let mut adapters = AdapterRegistry::new();
    adapters.register_default_adapters();
    let handle = RuntimeHandle::with_adapters(adapters);
    let session = SessionClient::new_for_test_with_handle(handle, SessionFacadeConfig::default());
    TaskHost::new(TqApi::new(session))
}

fn seed_quote_commit(host: &TaskHost, symbol: &str, last_price: f64) {
    host.api()
        .handle_for_test()
        .ingest(
            RuntimeInput::Io(IoEvent {
                route: "market".to_string(),
                domains: vec![ProtocolDomain::Market],
                payload: InputPayload::Json(json!({
                    "aid": "rtn_data",
                    "data": [{
                        "quotes": {
                            symbol: {
                                "instrument_id": symbol,
                                "last_price": last_price,
                            }
                        }
                    }]
                })),
            }),
            vec![],
            CommitScope::RealtimeUpdate,
        )
        .unwrap()
        .expect("seed quote commit should produce a commit");
}

#[tokio::test(flavor = "current_thread")]
async fn empty_scheduler_finishes_immediately_and_releases_ownership() {
    let mut host = seeded_host();

    let scheduler: TargetPosScheduler = host
        .target_pos_scheduler("sim", "SHFE.rb2601")
        .steps(Vec::new())
        .build()
        .unwrap();

    scheduler.wait_finished().await.unwrap();
    assert!(scheduler.is_finished());
    assert_eq!(
        scheduler.execution_report(),
        TargetPosExecutionReport {
            applied_steps: vec![]
        }
    );
    host.check_manual_order_allowed_for_test("sim", "SHFE.rb2601")
        .expect("ownership should be released immediately for empty schedulers");
}

#[tokio::test(flavor = "current_thread")]
async fn scheduler_advances_steps_via_host_wait_updates() {
    let mut host = seeded_host();
    let scheduler = host
        .target_pos_scheduler("sim", "SHFE.rb2601")
        .steps(vec![
            TargetPosScheduleStep {
                interval: Duration::from_millis(20),
                target_volume: 3,
            },
            TargetPosScheduleStep {
                interval: Duration::from_millis(20),
                target_volume: 0,
            },
        ])
        .build()
        .unwrap();

    assert_eq!(
        scheduler.execution_report(),
        TargetPosExecutionReport {
            applied_steps: vec![]
        }
    );

    seed_quote_commit(&host, "SHFE.rb2601", 3678.0);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);
    assert_eq!(
        scheduler.execution_report(),
        TargetPosExecutionReport {
            applied_steps: vec![TargetPosExecutionStep {
                step_index: 0,
                target_volume: 3,
            }],
        }
    );
    assert!(!scheduler.is_finished());

    tokio::time::sleep(Duration::from_millis(25)).await;
    seed_quote_commit(&host, "SHFE.rb2601", 3679.0);
    let updated = host.wait_update(None).await.unwrap();
    assert!(updated);

    scheduler.wait_finished().await.unwrap();
    assert_eq!(
        scheduler.execution_report(),
        TargetPosExecutionReport {
            applied_steps: vec![
                TargetPosExecutionStep {
                    step_index: 0,
                    target_volume: 3,
                },
                TargetPosExecutionStep {
                    step_index: 1,
                    target_volume: 0,
                },
            ],
        }
    );
    assert!(scheduler.is_finished());
    host.check_manual_order_allowed_for_test("sim", "SHFE.rb2601")
        .expect("ownership should be released after the last scheduler step");
}

#[tokio::test(flavor = "current_thread")]
async fn scheduler_blocks_guarded_manual_orders_while_active() {
    let mut host = seeded_host();
    let _scheduler = host
        .target_pos_scheduler("sim", "SHFE.rb2601")
        .steps(vec![TargetPosScheduleStep {
            interval: Duration::from_secs(60),
            target_volume: 1,
        }])
        .build()
        .unwrap();

    let err = host
        .insert_order_guarded(
            "sim",
            "SHFE.rb2601",
            TradeDirection::Buy,
            Some(TradeOffset::Open),
            1,
            Some(json!(3678.0)),
        )
        .await
        .expect_err("manual order should be blocked while scheduler owns the symbol");

    assert_eq!(
        err,
        TaskError::ManualOrderBlocked {
            account_id: "sim".to_string(),
            symbol: "SHFE.rb2601".to_string(),
            active_task_kind: TaskKind::Scheduler,
        }
    );
}

#[tokio::test(flavor = "current_thread")]
async fn scheduler_cancel_releases_ownership_and_wait_finished() {
    let mut host = seeded_host();
    let scheduler = host
        .target_pos_scheduler("sim", "SHFE.rb2601")
        .steps(vec![TargetPosScheduleStep {
            interval: Duration::from_secs(60),
            target_volume: 1,
        }])
        .build()
        .unwrap();

    scheduler.cancel().await.unwrap();
    scheduler.wait_finished().await.unwrap();
    assert!(scheduler.is_finished());

    host.check_manual_order_allowed_for_test("sim", "SHFE.rb2601")
        .expect("ownership should be released after scheduler cancellation");
}

#[test]
fn scheduler_builder_preserves_explicit_config() {
    let mut host = seeded_host();
    let scheduler = host
        .target_pos_scheduler("sim", "SHFE.rb2601")
        .offset_priority(OffsetPriority::YesterdayThenOpen)
        .split_policy(VolumeSplitPolicy {
            min_volume: 1,
            max_volume: 4,
        })
        .build()
        .unwrap();

    assert_eq!(
        scheduler.config(),
        &TargetPosSchedulerConfig {
            offset_priority: OffsetPriority::YesterdayThenOpen,
            split_policy: Some(VolumeSplitPolicy {
                min_volume: 1,
                max_volume: 4,
            }),
        }
    );
}
