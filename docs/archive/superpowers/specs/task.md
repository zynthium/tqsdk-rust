# 回测 API 收敛 — 任务跟踪

## Phase 1: 服务端回测 TqBuilder::backtest()
- [x] TqBuilder 新增 MarketMode + BacktestConfig
- [x] TqBuilder::backtest() / stock() 方法
- [x] connect() 路由 backtest 分支
- [x] Tq::new() 别名
- [x] api_contract_s37 example
- [x] 验证通过

## Phase 2: 本地离线回测 TqBuilder::local_backtest()
- [x] TqInner enum (Live / LocalBacktest)
- [x] LocalBacktestDriver 包装 StrategyBacktest
- [x] TqBuilder::local_backtest() / quote_symbol() / price_tick()
- [x] connect() 路由 local backtest 分支
- [x] Tq::next() 统一 match dispatch
- [x] api_contract_s38 example
- [x] 验证通过

## Phase 3: Tq 读取面统一
- [x] Tq::quote() match dispatch
- [x] Tq::account() match dispatch
- [x] Tq::position() match dispatch
- [x] 验证 StrategyBacktest 内部 TqApi 可达
- [x] 验证通过

## Phase 4: Prelude + re-export
- [x] advanced::task re-export backtest types
- [x] advanced::data re-export MarketCacheReplay
- [x] crate 根 re-export MarketCacheReplay (via prelude/advanced)
- [x] 验证通过

## Phase 5: API contract examples
- [x] api_contract_s39 same body example
- [x] 验证通过

## Phase 6: 文档更新
- [x] crates/tqsdk/README.md
- [x] docs/architecture/api-task.md
- [x] 验证通过
