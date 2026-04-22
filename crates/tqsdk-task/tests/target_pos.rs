use tqsdk_core::{AdapterRegistry, RuntimeHandle};
use tqsdk_session::{SessionClient, SessionFacadeConfig};
use tqsdk_task::{TaskError, TaskHost, TaskKind};
use tqsdk_wait::TqApi;

fn seeded_host() -> TaskHost {
    let mut adapters = AdapterRegistry::new();
    adapters.register_default_adapters();
    let handle = RuntimeHandle::with_adapters(adapters);
    let session = SessionClient::new_for_test_with_handle(handle, SessionFacadeConfig::default());
    TaskHost::new(TqApi::new(session))
}

#[tokio::test(flavor = "current_thread")]
async fn target_pos_task_owns_symbol_until_cancelled() {
    let mut host = seeded_host();
    let task = host.target_pos("sim", "SHFE.rb2601").build().unwrap();

    let err = host
        .register_scheduler_owner_for_test("sim", "SHFE.rb2601")
        .expect_err("scheduler should not take ownership while target task is active");
    assert_eq!(
        err,
        TaskError::OwnershipConflict {
            account_id: "sim".to_string(),
            symbol: "SHFE.rb2601".to_string(),
            active_task_kind: TaskKind::TargetPos,
        }
    );

    task.cancel().await.unwrap();

    host.check_manual_order_allowed_for_test("sim", "SHFE.rb2601")
        .expect("manual order should be allowed after target task cancellation");
}

#[test]
fn target_pos_task_tracks_latest_requested_target_volume() {
    let mut host = seeded_host();
    let task = host.target_pos("sim", "SHFE.rb2601").build().unwrap();

    assert_eq!(task.current_target_volume(), None);

    task.set_target_volume(5).unwrap();
    assert_eq!(task.current_target_volume(), Some(5));

    task.set_target_volume(8).unwrap();
    assert_eq!(task.current_target_volume(), Some(8));
}

#[test]
fn dropping_target_pos_task_releases_ownership() {
    let mut host = seeded_host();

    {
        let _task = host.target_pos("sim", "SHFE.rb2601").build().unwrap();
        assert!(
            host.check_manual_order_allowed_for_test("sim", "SHFE.rb2601")
                .is_err()
        );
    }

    host.check_manual_order_allowed_for_test("sim", "SHFE.rb2601")
        .expect("ownership should be released after the last task handle drops");
}
