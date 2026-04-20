use tqsdk_runtime_contract::{Runtime, RuntimeHandle};

#[test]
fn crate_bootstraps() {
    let handle = RuntimeHandle::default();
    assert_eq!(handle.latest_snapshot().revision().get(), 0);
}
