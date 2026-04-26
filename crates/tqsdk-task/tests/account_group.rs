use tqsdk_task::{AccountGroup, Ratio, TaskError};

#[test]
fn account_group_allocates_ratio_volume_with_largest_remainder() {
    let group = AccountGroup::builder()
        .add("sim-a", Ratio::new(2, 3).unwrap())
        .add("sim-b", Ratio::new(1, 3).unwrap())
        .min_volume_per_account(1)
        .build()
        .unwrap();

    let plan = group.allocate(5).unwrap();

    let allocations: Vec<_> = plan
        .allocations()
        .iter()
        .map(|allocation| (allocation.account_id(), allocation.volume()))
        .collect();
    assert_eq!(allocations, vec![("sim-a", 3), ("sim-b", 2)]);
}

#[test]
fn account_group_rejects_empty_and_duplicate_accounts() {
    let empty = AccountGroup::builder().build().unwrap_err();
    assert_eq!(
        empty,
        TaskError::InvalidState("account group cannot be empty")
    );

    let duplicate = AccountGroup::builder()
        .add("sim-a", Ratio::new(1, 1).unwrap())
        .add("sim-a", Ratio::new(1, 1).unwrap())
        .build()
        .unwrap_err();
    assert_eq!(
        duplicate,
        TaskError::InvalidState("duplicate account id in account group")
    );
}

#[test]
fn account_group_rejects_invalid_ratio_and_impossible_minimum() {
    assert_eq!(
        Ratio::new(0, 10).unwrap_err(),
        TaskError::InvalidState("account allocation ratio numerator must be positive")
    );
    assert_eq!(
        Ratio::new(1, 0).unwrap_err(),
        TaskError::InvalidState("account allocation ratio denominator must be positive")
    );

    let group = AccountGroup::builder()
        .add("sim-a", Ratio::new(1, 2).unwrap())
        .add("sim-b", Ratio::new(1, 2).unwrap())
        .min_volume_per_account(1)
        .build()
        .unwrap();

    assert_eq!(
        group.allocate(1).unwrap_err(),
        TaskError::InvalidState("total volume cannot satisfy account minimum volume")
    );
}
