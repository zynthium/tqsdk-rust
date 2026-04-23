use std::time::Duration;

use tqsdk_core::{AccountId, RuntimeCommand, TradeAccountType, TradeCommand, TradeLoginCommand};
use tqsdk_task::TaskHost;
use tqsdk_wait::TqApiBuilder;

#[tokio::test(flavor = "current_thread")]
#[ignore = "live network smoke; requires TQ_AUTH_USER/TQ_AUTH_PASS and SIMNOW_USER_0/SIMNOW_PASS_0"]
async fn live_task_host_trade_account_ready_smoke() {
    let Some(auth_user) = read_env("TQ_AUTH_USER") else {
        return;
    };
    let Some(auth_pass) = read_env("TQ_AUTH_PASS") else {
        return;
    };
    let Some(account_id) = read_env("SIMNOW_USER_0") else {
        return;
    };
    let Some(trade_password) = read_env("SIMNOW_PASS_0") else {
        return;
    };

    let api = TqApiBuilder::new(auth_user, auth_pass)
        .trade_target("simnow", account_id.clone())
        .build()
        .await
        .expect("live wait api should build");
    let mut host = TaskHost::new(api);

    host.api()
        .session()
        .submit(RuntimeCommand::Trade(TradeCommand::Login(
            TradeLoginCommand {
                account_id: AccountId::new(account_id.clone()),
                broker_id: "simnow".to_string(),
                password: trade_password,
                account_type: TradeAccountType::Future,
                front_broker: None,
                front_url: None,
                client_app_id: None,
                client_system_info: None,
            },
        )))
        .await
        .expect("TradeLoginCommand should submit successfully");

    let account = host.api().get_account(account_id.as_str());
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let now = tokio::time::Instant::now();
        assert!(
            now < deadline,
            "timed out waiting for trade account snapshot"
        );

        let _updated = host
            .wait_update(Some(now + Duration::from_secs(5)))
            .await
            .expect("TaskHost::wait_update should succeed");

        let Some(snapshot) = account
            .snapshot(host.api())
            .expect("account snapshot decode should succeed")
        else {
            continue;
        };

        assert_eq!(snapshot.user_id, account_id);
        assert_eq!(snapshot.currency, "CNY");
        return;
    }
}

fn read_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}
