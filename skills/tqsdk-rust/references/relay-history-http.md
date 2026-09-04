# Relay History HTTP 与 Docker

当请求涉及 relay 的历史 HTTP 接口、部署、健康检查或从其他程序调用历史服务时使用本文件。它是智能体路由和操作摘要；wire contract 以仓库的
[`history-relay-http.md`](../../../docs/architecture/history-relay-http.md) 为准，Docker 命令与挂载布局以
[`deploy/docker/README.md`](../../../deploy/docker/README.md) 为准。

## 先区分入口

- 普通 Rust 策略和研究代码优先使用 `tqsdk` / `tqsdk-data`，不需要启动 relay。
- `tqsdk-relay` history 是可选、只读、CacheOnly 的 HTTP 服务；它不是远端补洞服务、通用代理或交易接口。
- 默认 live-cache 模式把 `tqsdk-cache fill` 正在维护的 cache root 只读挂载给 relay。每个请求读取最新原子提交的 metadata、coverage 和 rows，因此无需发布快照或重启就能观察实际填充进度；未提交数据不会变成可见 coverage，maintenance gate 期间请求 fail closed。
- `CURRENT` immutable generation 仅是兼容/回滚路径。只有用户明确使用 published 模式时，才引导 `tqsdk-cache snapshot` clone、verify、publish。

## HTTP v1

history listener 只提供：

- `GET /v1/history/schema`
- `GET /v1/history/coverage`
- `GET /v1/history/query`

`query` 和 `coverage` 的通用参数为 `symbol`、`series=tick|kline`、`start`、`end`；Kline 必须带 `period`，Tick 禁止带 `period`。时间是 RFC3339，区间固定为 `[start, end)`。`query` 另可带按请求顺序投影的 `fields`；主连映射需要 `include=provenance`。调用方按 HTTP status 和 `error.code` 判断失败，不解析 `message`。

最小调用示例：

```bash
RELAY_HOST=127.0.0.1
IDENTITY_HEADER=X-Trusted-Identity

curl --fail-with-body --silent --show-error \
  -H "${IDENTITY_HEADER}: local-client" \
  "http://${RELAY_HOST}:7790/v1/history/schema"

curl --fail-with-body --silent --show-error \
  -H "${IDENTITY_HEADER}: local-client" \
  "http://${RELAY_HOST}:7790/v1/history/query?symbol=SHFE.au2612&series=kline&period=1m&start=2026-08-01T00%3A00%3A00%2B08%3A00&end=2026-08-01T01%3A00%3A00%2B08%3A00&fields=t%2Co%2Ch%2Cl%2Cc%2Cv%2Coi"
```

整数 cell 以 JSON 十进制字符串返回；有限浮点是 JSON number；缺失或非有限值是 `null`。`columns` 与每行 `rows` 的位置一一对应。完整覆盖但没有 row 时是 200 和空 `rows`；coverage 缺口通常是 typed 409，不是 partial 200。

请求起点比 relay 服务器时间晚超过 5 秒时，`coverage` 和 `query` 会在 admission 与 cache/source lookup 前返回 `409 coverage_incomplete`，其中 `details.reason=range_starts_in_future`、`retryable=true`。与当前时间重叠但尾部在未来的区间仍执行正常 coverage/finality 校验；不要把它解释成空数据成功。默认 row 上限是 Kline 10,000、Tick 50,000，未压缩响应上限 32 MiB；调用方应缩小时间区间后重试，不要无界重放同一个超限请求。

所有 history 响应默认带 wildcard CORS header；已知 path 的 `OPTIONS` 无需 identity，实际 `GET` 仍要求 trusted identity header。该 header 不进入 CORS allow-headers，浏览器调用应由受控网关注入，不能让前端脚本自行伪造。

## Docker 配置与挂载

Compose 使用 host networking，不配置 `ports:`。常用 listener 是 downstream `7788`、metrics/dashboard `7789`、history `7790`。具体绑定由以下变量决定：

- `TQSDK_RELAY_DOWNSTREAM_LISTEN`
- `TQSDK_RELAY_METRICS_LISTEN`
- `TQSDK_RELAY_HISTORY_LISTEN`
- `TQSDK_RELAY_HISTORY_IDENTITY_HEADER`
- `TQSDK_LIVE_CACHE_ROOT`：Compose 宿主机侧、由 `tqsdk-cache fill` 维护的 active cache root
- `TQSDK_RELAY_HISTORY_CACHE_DIR`：容器内只读 live-cache 路径；Compose 固定为 `/var/lib/tqsdk/history-cache`
- `TQSDK_HISTORY_ROOT` / `TQSDK_RELAY_HISTORY_ROOT`：仅供 optional publisher compatibility profile 使用

将 listener 绑定到 `0.0.0.0` 会让 host network 中的端口面向所有宿主机接口；只有网关、防火墙和访问控制已明确覆盖这些端口时才这样配置。trusted identity header 是网关注入的审计身份，不是服务自身的认证机制：受控网关必须删除客户端自带同名 header、注入可信值并限制并发。不要把裸 history listener 直接暴露到互联网。

启动与验证命令以 `deploy/docker/README.md` 为准。最小检查顺序是：

```bash
docker compose --env-file deploy/docker/.env -f deploy/docker/compose.yaml up -d relay
docker compose --env-file deploy/docker/.env -f deploy/docker/compose.yaml ps

curl --fail-with-body --silent --show-error http://127.0.0.1:7789/health
curl --fail-with-body --silent --show-error http://127.0.0.1:7789/health \
  | jq --exit-status '.history.ready == true'
```

容器 healthcheck 只证明 metrics listener 存活；对外提供 history 前还要验证 `.history.ready == true`，并用真实 coverage 请求确认 `source_mode=live-cache` 和当前已提交覆盖。history 不 ready 时 market relay 可以继续存活，history query/coverage 返回 `503 history_unavailable`。

## 回答和操作边界

- 用户只问“如何调用”时，给 endpoint、参数、identity header、URL 编码、typed error 和 readiness 检查；live-cache 不需要发布 snapshot。
- 用户要求部署或切换数据时，先核对 `TQSDK_LIVE_CACHE_ROOT`、容器只读挂载、operation lock、listener 暴露范围和真实 coverage 请求。
- snapshot clone、publish、recover、rollback、scrub、GC 只属于显式 compatibility/rollback 运维；遵守 `tqsdk-cache snapshot` 的 dry-run、verify 和显式 apply 门。
- Docker 改善镜像、权限、重启和发布控制，但不改变 market 与 history 同进程的 failure domain，也不提供 CPU、SMT、LLC、内存带宽或 IRQ 的硬隔离。
