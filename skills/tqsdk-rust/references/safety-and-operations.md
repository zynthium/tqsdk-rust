# 安全与运行

## 凭证与权限

- 示例默认使用 `TQ_AUTH_USER` 和 `TQ_AUTH_PASS`，除非用户提供其他 auth path。
- 说明 live 示例需要行情权限；交易示例还需要账户权限。
- 工作流依赖特定行情权限时，使用 `has_feature(...)` 或 `check_md_grants(...)`。
- live smoke test 必须放在显式环境变量后面；普通 unit test 不应依赖官方服务。

## 优先模拟

- 下单示例优先使用 `TqKq` / simulation。
- real broker integration 必须显式 opt-in。
- 不要把下单隐藏在 setup helper 或看起来只读的示例里。
- 明确说明示例是只读、提交模拟订单，还是可能触达实盘账户。

## 订单安全

- 优先使用 typed order builders、`OrderPrice`、`OrderTicket`、task-layer builders 和 stable client intent IDs。
- 用 session-scoped intent ledger 或 task ticket 避免 retry 时重复提交。
- typed helper 存在时，不要解析 command status 或 order status 字符串。
- 不要维护会和 runtime state 分叉的私有 order overlay。
- 不要用字符串匹配或 adapter-local terminal-state 判断绕过 runtime command lifecycle 校验。

## Runtime 安全

- 对外可见状态路径应经过 runtime commits 和 readers。
- hot path 优先使用 `read_market_state()`、`read_trade_state()` 或 `read_market_trade_state()` 这类 partition read。
- 慢日志和持久化使用调用方自有 sidecar，不要阻塞交易决策。
- domain state write 必须经过 runtime mutation path 和 `MutationSource` root-path guard。

## 测试

- 策略逻辑测试优先使用 `tqsdk-task::testing` fake market/fake broker tools。
- 只有凭证、权限、endpoint 和副作用都被明确接受时，才使用 live smoke test。
- 需要真实服务的 integration test 必须用环境变量门控。
