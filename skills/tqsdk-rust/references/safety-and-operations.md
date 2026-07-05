# 安全与运行

## 凭证与权限

- 示例默认使用 `TQ_AUTH_USER` 和 `TQ_AUTH_PASS`，除非用户提供其他 auth path。
- 说明 live 示例需要行情权限；交易示例还需要账户权限。
- `MarketCachePolicy` / `record_ticks(...)` 是只读行情记录，但仍会连接 live/session 并写本地持久缓存；示例必须显式说明需要行情权限、cache 目录和显式 symbol 列表，live smoke 保持环境变量门控。
- `recorded_market_cache_policy()` 只能派生 cache 目录和 symbol 集合，不能携带或复用 live session 明文 auth；后续 `.warmup()` / `.remote_on_miss()` 补洞必须由用户显式重新提供 auth。
- `tqsdk-monitor` 默认只绑定调用方给出的地址，示例优先用 localhost；它是只读观测面板，不应暴露成公网控制面。cache inventory worker 只读本地 cache 目录，不能补数据、compact、删除或写 coverage。
- 工作流依赖特定行情权限时，使用 `has_feature(...)` 或 `check_md_grants(...)`。
- live smoke test 必须放在显式环境变量后面；普通 unit test 不应依赖官方服务。
- HTTP auth / direct-query 默认强制直连，不使用系统 proxy；如果 Rust resolver 对官方域名异常，可用 `TQSDK_DIRECT_RESOLVE_AUTH_SHINNYTECH_COM` / `TQSDK_DIRECT_RESOLVE_API_SHINNYTECH_COM` / `TQSDK_DIRECT_RESOLVE_FILES_SHINNYTECH_COM` 注入当前环境解析到的 IP。

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
- 监控 snapshot 只能从预聚合 registry 读取；cache scan 必须在后台低频 worker 中执行，不能放进 strategy loop、order path 或 HTTP request handler。
- domain state write 必须经过 runtime mutation path 和 `MutationSource` root-path guard。

## 测试

- 策略逻辑测试优先使用 `tqsdk-task::testing` fake market/fake broker tools。
- 只有凭证、权限、endpoint 和副作用都被明确接受时，才使用 live smoke test。
- 需要真实服务的 integration test 必须用环境变量门控。
