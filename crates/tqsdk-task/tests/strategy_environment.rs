use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use tqsdk_core::Quote;
use tqsdk_task::deployment::{
    StrategyDeployment, StrategyDeploymentConfig, StrategyEnvironmentProvider, StrategyLifecycle,
    StrategyRetryPolicy, StrategyRunStopReason, StrategyShutdownSignal, StrategySupervisor,
    StrategySupervisorHealthStatus, StrategySupervisorStopReason, StrategyTelemetryEvent,
    StrategyTelemetryEventKind,
};
use tqsdk_task::environment::{
    StrategyEnvironment, StrategyEnvironmentContext, StrategyEnvironmentKind,
};
use tqsdk_task::replay::{ReplayMarketEvent, ReplayMarketSource, StrategyReplay};
use tqsdk_task::testing::{FakeBroker, FakeMarket, StrategyTestHarness};
use tqsdk_task::{RiskEngine, TaskError};

const SYMBOL: &str = "SHFE.au2602";

async fn buy_breakout_once(ctx: &mut StrategyEnvironmentContext<'_>) -> tqsdk_task::Result<()> {
    let quote = ctx.quote(SYMBOL)?;
    let position = ctx.position("sim", SYMBOL)?;
    if quote.last_price > 480.0 && position.pos_long == 0 {
        ctx.orders("sim")
            .buy_open(SYMBOL, 1)
            .limit(quote.last_price)
            .send_once("env-breakout-entry")
            .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn strategy_environment_runs_same_strategy_step_on_test_harness() {
    let harness = StrategyTestHarness::new()
        .market(
            FakeMarket::new()
                .quote(SYMBOL, 481.0)
                .account("sim", 100_000.0)
                .position("sim", SYMBOL, 0),
        )
        .broker(FakeBroker::new().fill_all())
        .build()
        .unwrap();

    let mut environment = StrategyEnvironment::from_test_harness(harness)
        .account("sim")
        .quote(SYMBOL)
        .build()
        .await
        .unwrap();
    assert_eq!(environment.kind(), StrategyEnvironmentKind::TaskHost);

    let mut ctx = environment.next_once().await.unwrap();
    assert!(ctx.replay_event().is_none());
    buy_breakout_once(&mut ctx).await.unwrap();
    let report = ctx.finish_test_step().await.unwrap();

    assert_eq!(report.orders().len(), 1);
    assert_eq!(report.trades().len(), 1);
    assert_eq!(report.position("sim", SYMBOL).unwrap().pos_long, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn strategy_environment_runs_same_strategy_step_on_replay() {
    let replay = ReplayMarketSource::new(vec![
        ReplayMarketEvent::quote(
            "cache",
            SYMBOL,
            1_000,
            Some(900),
            Quote {
                last_price: 481.0,
                ..Quote::default()
            },
        )
        .unwrap(),
    ]);

    let replay_builder = StrategyReplay::builder(replay)
        .market(
            FakeMarket::new()
                .account("sim", 100_000.0)
                .position("sim", SYMBOL, 0),
        )
        .broker(FakeBroker::new().fill_all());
    let mut environment = StrategyEnvironment::from_replay_builder(replay_builder)
        .account("sim")
        .quote(SYMBOL)
        .build()
        .await
        .unwrap();
    assert_eq!(environment.kind(), StrategyEnvironmentKind::Replay);

    let mut ctx = environment.next_once().await.unwrap();
    assert_eq!(ctx.replay_event().unwrap().source(), "cache");
    buy_breakout_once(&mut ctx).await.unwrap();
    let report = ctx.finish_test_step().await.unwrap();

    assert_eq!(report.orders().len(), 1);
    assert_eq!(report.trades().len(), 1);
    assert_eq!(report.position("sim", SYMBOL).unwrap().pos_long, 1);
}

#[test]
fn deployment_config_describes_provider_backed_tqkq_sim_without_protocol_leaks() {
    let config = StrategyDeploymentConfig::tqkq_sim("demo-user", "demo-pass")
        .account_number(7)
        .futures_market()
        .quote(SYMBOL)
        .startup_timeout(Duration::from_secs(5))
        .lifecycle(StrategyLifecycle::new().max_steps(3))
        .risk(RiskEngine::new().max_order_volume(1));

    assert_eq!(config.provider(), StrategyEnvironmentProvider::TqKqSim);
    assert_eq!(config.subscriptions().quote_symbols(), &[SYMBOL.to_owned()]);
    assert_eq!(config.lifecycle_policy().max_steps_limit(), Some(3));
    assert_eq!(config.startup_timeout_value(), Duration::from_secs(5));
    assert!(config.risk_engine().is_some());
}

#[tokio::test(flavor = "current_thread")]
async fn deployment_lifecycle_runs_replay_until_max_steps() {
    let replay = ReplayMarketSource::new(vec![
        ReplayMarketEvent::quote(
            "cache",
            SYMBOL,
            1_000,
            Some(900),
            Quote {
                last_price: 481.0,
                ..Quote::default()
            },
        )
        .unwrap(),
        ReplayMarketEvent::quote(
            "cache",
            SYMBOL,
            2_000,
            Some(1_900),
            Quote {
                last_price: 482.0,
                ..Quote::default()
            },
        )
        .unwrap(),
    ]);
    let replay_builder = StrategyReplay::builder(replay).market(
        FakeMarket::new()
            .account("sim", 100_000.0)
            .position("sim", SYMBOL, 0),
    );
    let environment = StrategyEnvironment::from_replay_builder(replay_builder)
        .account("sim")
        .quote(SYMBOL)
        .build()
        .await
        .unwrap();
    let mut deployment = StrategyDeployment::from_environment(environment)
        .account_id("sim")
        .lifecycle(StrategyLifecycle::new().max_steps(1))
        .build()
        .await
        .unwrap();

    let report = deployment
        .run(|ctx| {
            Box::pin(async move {
                assert_eq!(ctx.kind(), StrategyEnvironmentKind::Replay);
                assert!(ctx.replay_event().is_some());
                Ok(())
            })
        })
        .await
        .unwrap();

    assert_eq!(deployment.account_id(), Some("sim"));
    assert_eq!(report.steps(), 1);
    assert_eq!(report.stop_reason(), StrategyRunStopReason::MaxSteps);
}

#[tokio::test(flavor = "current_thread")]
async fn deployment_runs_async_strategy_step_and_reports_shutdown() {
    let harness = StrategyTestHarness::new()
        .market(
            FakeMarket::new()
                .quote(SYMBOL, 481.0)
                .account("sim", 100_000.0)
                .position("sim", SYMBOL, 0),
        )
        .broker(FakeBroker::new().fill_all())
        .build()
        .unwrap();
    let environment = StrategyEnvironment::from_test_harness(harness)
        .account("sim")
        .quote(SYMBOL)
        .build()
        .await
        .unwrap();
    let mut deployment = StrategyDeployment::from_environment(environment)
        .account_id("sim")
        .lifecycle(StrategyLifecycle::new().max_steps(1))
        .build()
        .await
        .unwrap();

    let run_report = deployment
        .run(|ctx| {
            Box::pin(async move {
                buy_breakout_once(ctx).await?;
                let test_report = ctx.finish_test_step().await?;
                assert_eq!(test_report.trades().len(), 1);
                assert_eq!(test_report.position("sim", SYMBOL).unwrap().pos_long, 1);
                Ok(())
            })
        })
        .await
        .unwrap();
    let shutdown_report = deployment.shutdown().await.unwrap();

    assert_eq!(run_report.steps(), 1);
    assert_eq!(shutdown_report.kind(), StrategyEnvironmentKind::TaskHost);
    assert!(shutdown_report.graceful());
}

#[tokio::test(flavor = "current_thread")]
async fn supervisor_stops_before_run_when_shutdown_signal_is_requested() {
    let deployment = fake_deployment_with_lifecycle(StrategyLifecycle::new().max_steps(1)).await;
    let signal = StrategyShutdownSignal::manual();
    signal.request_shutdown();
    let mut supervisor = StrategySupervisor::new(deployment).shutdown_signal(signal);

    let report = supervisor
        .run(|_| {
            Box::pin(async move {
                panic!("strategy step should not run after shutdown was requested");
            })
        })
        .await
        .unwrap();

    assert_eq!(
        report.stop_reason(),
        StrategySupervisorStopReason::ShutdownRequested
    );
    assert_eq!(report.metrics().steps(), 0);
    assert_eq!(
        supervisor.health().status(),
        StrategySupervisorHealthStatus::Stopped
    );
}

#[tokio::test(flavor = "current_thread")]
async fn supervisor_retries_strategy_step_and_records_metrics() {
    let deployment = replay_deployment_with_events(2, StrategyLifecycle::new().max_steps(1)).await;
    let mut attempts = 0;
    let mut supervisor =
        StrategySupervisor::new(deployment).retry_policy(StrategyRetryPolicy::new().max_retries(1));

    let report = supervisor
        .run(move |_| {
            attempts += 1;
            Box::pin(async move {
                if attempts == 1 {
                    return Err(TaskError::InvalidState("transient strategy failure"));
                }
                Ok(())
            })
        })
        .await
        .unwrap();

    assert_eq!(
        report.stop_reason(),
        StrategySupervisorStopReason::Deployment(StrategyRunStopReason::MaxSteps)
    );
    assert_eq!(report.metrics().steps(), 1);
    assert_eq!(report.metrics().retries(), 1);
    assert_eq!(report.metrics().errors(), 1);
    assert_eq!(
        supervisor.health().status(),
        StrategySupervisorHealthStatus::Stopped
    );
}

#[tokio::test(flavor = "current_thread")]
async fn supervisor_reports_typed_telemetry_events() {
    let deployment = replay_deployment_with_events(1, StrategyLifecycle::new().max_steps(1)).await;
    let events = Rc::new(RefCell::new(Vec::<StrategyTelemetryEvent>::new()));
    let captured_events = Rc::clone(&events);
    let mut supervisor = StrategySupervisor::new(deployment).telemetry_reporter(move |event| {
        captured_events.borrow_mut().push(event);
    });

    let report = supervisor
        .run(|_| Box::pin(async move { Ok(()) }))
        .await
        .unwrap();

    assert_eq!(
        report.stop_reason(),
        StrategySupervisorStopReason::Deployment(StrategyRunStopReason::MaxSteps)
    );

    let events = events.borrow();
    assert!(
        events.iter().any(|event| {
            event.kind() == StrategyTelemetryEventKind::HealthChanged
                && event.health().status() == StrategySupervisorHealthStatus::Running
        }),
        "telemetry should report running health"
    );
    assert!(
        events.iter().any(|event| {
            event.kind() == StrategyTelemetryEventKind::MetricsUpdated
                && event.metrics().steps() == 1
        }),
        "telemetry should report step metrics"
    );
    let stopped = events
        .iter()
        .find(|event| event.kind() == StrategyTelemetryEventKind::RunStopped)
        .expect("telemetry should report final stop");
    assert_eq!(
        stopped.stop_reason(),
        Some(StrategySupervisorStopReason::Deployment(
            StrategyRunStopReason::MaxSteps
        ))
    );
    assert_eq!(stopped.metrics().steps(), 1);
    assert!(stopped.last_error().is_none());
}

async fn fake_deployment_with_lifecycle(lifecycle: StrategyLifecycle) -> StrategyDeployment {
    let harness = StrategyTestHarness::new()
        .market(
            FakeMarket::new()
                .quote(SYMBOL, 481.0)
                .account("sim", 100_000.0)
                .position("sim", SYMBOL, 0),
        )
        .broker(FakeBroker::new().fill_all())
        .build()
        .unwrap();
    let environment = StrategyEnvironment::from_test_harness(harness)
        .account("sim")
        .quote(SYMBOL)
        .build()
        .await
        .unwrap();

    StrategyDeployment::from_environment(environment)
        .account_id("sim")
        .lifecycle(lifecycle)
        .build()
        .await
        .unwrap()
}

async fn replay_deployment_with_events(
    event_count: usize,
    lifecycle: StrategyLifecycle,
) -> StrategyDeployment {
    let events = (0..event_count)
        .map(|idx| {
            ReplayMarketEvent::quote(
                "cache",
                SYMBOL,
                1_000 + idx as i64,
                Some(900 + idx as i64),
                Quote {
                    last_price: 481.0 + idx as f64,
                    ..Quote::default()
                },
            )
            .unwrap()
        })
        .collect();
    let replay = ReplayMarketSource::new(events);
    let replay_builder = StrategyReplay::builder(replay).market(
        FakeMarket::new()
            .account("sim", 100_000.0)
            .position("sim", SYMBOL, 0),
    );
    let environment = StrategyEnvironment::from_replay_builder(replay_builder)
        .account("sim")
        .quote(SYMBOL)
        .build()
        .await
        .unwrap();

    StrategyDeployment::from_environment(environment)
        .account_id("sim")
        .lifecycle(lifecycle)
        .build()
        .await
        .unwrap()
}
