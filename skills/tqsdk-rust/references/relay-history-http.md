# Relay History HTTP 与 Docker

当请求涉及 relay 的历史 HTTP 接口、部署、健康检查或从其他程序调用历史服务时使用本文件。它是智能体路由和操作摘要；wire contract 以仓库的
[`history-relay-http.md`](../../../docs/architecture/history-relay-http.md) 为准，Docker 命令与挂载布局以
[`deploy/docker/README.md`](../../../deploy/docker/README.md) 为准。

## 先区分入口

- 普通 Rust 策略和研究代码优先使用 `tqsdk` / `tqsdk-data`，不需要启动 relay。
- `tqsdk-relay` history 是可选、只读、CacheOnly 的 HTTP 服务，只读取 `CURRENT` 指向的 immutable published snapshot；它不是远端补洞服务、通用代理或交易接口。
- 原始可写回测 cache 与 published history root 是两个角色。先用 `tqsdk-cache snapshot` clone、verify、publish，再让 relay 只读挂载 published root。不能把普通 cache 目录仅靠 bind mount 就视为可服务 snapshot。

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

## Docker 配置与挂载

Compose 使用 host networking，不配置 `ports:`。常用 listener 是 downstream `7788`、metrics/dashboard `7789`、history `7790`。具体绑定由以下变量决定：

- `TQSDK_RELAY_DOWNSTREAM_LISTEN`
- `TQSDK_RELAY_METRICS_LISTEN`
- `TQSDK_RELAY_HISTORY_LISTEN`
- `TQSDK_RELAY_HISTORY_IDENTITY_HEADER`
- `TQSDK_HISTORY_ROOT`：Compose 宿主机侧的 published root bind source
- `TQSDK_RELAY_HISTORY_ROOT`：容器内只读 published root，由 Compose 固定映射
- `TQSDK_WRITABLE_CACHE_ROOT`：publisher 读取的宿主机可写 cache root

将 listener 绑定到 `0.0.0.0` 会让 host network 中的端口面向所有宿主机接口；只有网关、防火墙和访问控制已明确覆盖这些端口时才这样配置。trusted identity header 是网关注入的审计身份，不是服务自身的认证机制：受控网关必须删除客户端自带同名 header、注入可信值并限制并发。不要把裸 history listener 直接暴露到互联网。

启动与验证命令以 `deploy/docker/README.md` 为准。最小检查顺序是：

```bash
docker compose --env-file deploy/docker/.env -f deploy/docker/compose.yaml up -d relay
docker compose --env-file deploy/docker/.env -f deploy/docker/compose.yaml ps

curl --fail-with-body --silent --show-error http://127.0.0.1:7789/health
curl --fail-with-body --silent --show-error http://127.0.0.1:7789/health \
  | jq --exit-status '.history.ready == true and (.history.snapshot_id | length > 0)'
```

容器 healthcheck 只证明 metrics listener 存活；对外提供 history 前还要验证 `.history.ready == true` 并记录当前 `snapshot_id`。history 不 ready 时 market relay 可以继续存活，history query/coverage 返回 `503 history_unavailable`。

## 回答和操作边界

- 用户只问“如何调用”时，给 endpoint、参数、identity header、URL 编码和 readiness 检查；无需让其重新发布 snapshot。
- 用户要求部署或切换数据时，先核对宿主机 source/published root、`CURRENT`、snapshot verify 结果和 listener 暴露范围。
- 修改 snapshot 的 clone、publish、recover、rollback、scrub、GC 都是运维动作；遵守 `tqsdk-cache snapshot` 的 dry-run、verify 和显式 apply 门。
- Docker 改善镜像、权限、重启和发布控制，但不改变 market 与 history 同进程的 failure domain，也不提供 CPU、SMT、LLC、内存带宽或 IRQ 的硬隔离。
