# Relay 内置只读历史查询 ADR

## 状态

- 状态：Accepted
- 日期：2026-08-29
- 适用范围：`tqsdk-data`、`tqsdk-cache`、`tqsdk-relay`

HTTP wire contract 见 [history-relay-http.md](history-relay-http.md)，snapshot 发布与兼容合同见
[history-snapshot-manifest.md](history-snapshot-manifest.md)。

## 背景

部署侧需要通过受控网关查询已经预热并验收的历史行情，同时不能让历史查询改变 SDK 默认直连路径，
也不能让大查询占用 relay 的实时行情锁、runtime 或 CPU worker。发布新历史数据时，正在执行的旧查询
必须继续读取原 generation，不能因为切换 cache root 被中断。

现有边界已经提供三个可复用角色：

- `tqsdk-data::BacktestHistoryClient` 拥有查询规划、聚合、coverage、finality、metadata 和主连映射；
- `tqsdk-cache` 是可选的 operator CLI；
- `tqsdk-relay` 是可选 market service，但不在 Cargo default-members，也不进入 SDK 默认依赖路径。

## 决策

### 同进程、独立兄弟模块

不新增 `tqsdk-historyd`。历史查询作为同一个 `tqsdk-relay` 进程内的私有兄弟模块部署，但必须满足：

- 不进入 `RelayEngine` 或 `RelayServer`；
- 不接收、持有或获取 market `Arc<Mutex<RelayEngine>>`；
- 使用独立 listener、Tokio runtime、OS threads、blocking decoder pool 和 gzip pool；
- history readiness 不参与 market readiness；
- market 与 history 配置为非空、互斥且实际应用成功的 CPU set 后，才允许 gzip；
- 同进程 OOM/abort 仍是接受的 failure domain；该方案不宣称进程级、LLC 或内存带宽隔离。

如果以后不能接受同进程 OOM/abort，另行设计 relay 管理的内部 worker process；本 ADR 不预留通用
multi-process framework。

### 唯一查询 owner

`tqsdk-data` 提供一个深模块接口，统一拥有：

- field schema、alias、canonical order 和 typed cell；
- 参数与周期校验；
- concrete、index、main-continuous 规划；
- metadata snapshot、physical segment 和 authoritative symbol catalog；
- coverage、missing ranges、finality；
- CacheOnly source inspection、查询和聚合；
- snapshot manifest 校验、只读打开和 generation lease。

`tqsdk-cache query` 与 relay HTTP 是这个接口的两个 adapter。两者不得复制 planner、manifest parser、
cache reader、字段表或通过错误字符串推断状态。relay 不直接解析 `.tqbn`、`.tqmk`、`.tqdk`。

### Publisher 与 reader 分权

`tqsdk-cache` 独占以下可写职责：

- 从稳定的现有 cache root 导入；
- snapshot clone、prewarm、CacheOnly verify、实际 query smoke；
- manifest 生成、publish、recover、rollback、scrub、retention 和 GC。

relay 只执行：

- HTTP/1 parsing、可信身份头校验、admission；
- 调用 data 的 schema/inspect/query；
- field projection、JSON、ETag、gzip；
- audit、readiness 和低基数 metrics；
- CURRENT 轮询、校验和内存 `Arc<Snapshot>` 原子替换。

relay 永不写 snapshot、永不 GC、永不 RemoteOnMiss、永不读取历史远端凭证。

### 零读中断 snapshot

history root 使用不可变 generation：

```text
history-root/
├── CURRENT
├── snapshots/<snapshot_id>/
│   ├── manifest.json
│   ├── lease.lock
│   └── cache/
└── staging/
```

publisher 只在 staging 写入和验证，完成持久化后原子发布 generation，再原子切换 CURRENT。relay 每
5 秒轮询 CURRENT，先取得新 generation 的 shared lease，再校验并复核 pointer，最后替换当前
`Arc<Snapshot>`。

旧请求持有旧 snapshot Arc；data 查询把 shared lease 延长到 detached coordinator 和 blocking scan
真正结束。请求断开或超时只能触发取消，不能提前释放 lease。无有效 generation 时只有 history 返回
503；新 generation 无效时保留上一有效 generation。

### 资源与响应边界

history 的默认硬边界：

| 资源 | 默认值 |
| --- | ---: |
| active request | 8 |
| admission queue wait | 100 ms |
| total request timeout | 10 s |
| Kline rows | 10,000 |
| Tick rows | 50,000 |
| uncompressed response | 32 MiB |
| daemon-global history buffers | 512 MiB |
| gzip workers | 2 |
| gzip threshold | 64 KiB |
| gzip level | 1 |

512 MiB 是 relay 拥有的跨请求、跨 generation history buffer policy，至少覆盖 scan chunks、JSON 和
compression buffers；它不是整个进程 RSS 上限。data 只提供
`BacktestHistorySnapshotResourceBudget` / opaque reservation seam；relay 负责配置总额，并通过
`BacktestHistorySnapshotQueryResources` 把同一 budget 与 per-request active pin 带入 coordinator、
shared scan 和 blocking reader。per-run/per-symbol semaphore 只能作为其下级限制。

relay 增量消费 `BacktestHistoryRun::next()`，但必须先缓存在有界私有内存中，只有 terminal report
证明 coverage、finality、metadata hash 和 snapshot identity 全部一致后才发送完整 body。任何失败都
丢弃缓存，禁止 partial body、分页或流式成功。

### Feature 与迁移

- relay 增加默认启用但可关闭的 `history` feature；
- 该 feature 只能传播 `tqsdk-data/tqbn-zstd` 等本地 reader 能力；
- 禁止传播 `live`、`services`、reqwest 或认证依赖；
- `cargo check -p tqsdk-relay --no-default-features` 与
  `--no-default-features --features history` 都必须成立；
- snapshot manifest 声明 cache format 和 minimum reader compatibility；
- rollout 顺序固定为 reader-expand、publisher-write、verify、切 CURRENT；
- rollback 只把 CURRENT 切回保留的已验证兼容 generation，不原地重写已发布文件；
- minute v4 到 v5 继续使用现有带外 backup 的显式迁移；v3 保持 fail closed；
- 现有 `tqsdk-cache --cache-dir` 不获得隐式 CURRENT 语义，snapshot 命令必须显式
  `--history-root`。

## 安全与运维

- 服务只允许部署在受控网关后，由配置的可信 peer 提供 identity header 和 client quota；
- 默认不返回 CORS header，不提供 browser credential flow；
- audit 至少记录 request id、trusted identity、endpoint、snapshot id、series、period、range、
  projected field count、rows、bytes、duration、status/error code；不得记录 auth secret；
- metrics 不得使用 symbol 作为 label；
- 配置了 history 但 listener、root、limits 或 affinity 非法时进程启动 fail fast；
- snapshot 缺失/损坏只使 history unready，不使 market unready；
- active generation 的 relay-local atomic health state 第一次从 healthy 转为 unhealthy 时返回
  500；同 generation 的其他并发和后续请求返回 503。该状态不写入 immutable snapshot，
  market 继续服务。

## 容量证据与发布口径

本阶段按受控网关后的低并发 CacheOnly 查询完成验收。默认 active request、buffer 和 gzip worker
仍是安全上限，不代表经过验证的吞吐量或 p99 SLO。部署侧必须在网关设置符合实际业务的低并发
配额；当前架构不声明一个经过验证的安全并发数字。

以下非 live 干扰测试保留为容量特征项：

- 8 个并发 history query；
- 512 MiB history buffer budget；
- 2 个 gzip worker；
- market 无丢失、乱序或异常断连；
- 观察 market p99 latency 增量是否不超过 `max(1 ms, 10%)`。

2026-08-29 在当前生产宿主机运行该测试时，上述 p99 目标未通过。该端到端 gate 同时包含
loopback、peek 闭环、scheduler/softirq、fixture 与客户端解码，因此只能证明 history 负载期间观察到
market 尾延迟恶化，不能把原因归结为 relay 内部 CPU 核共享。它不阻塞本阶段低并发功能版本收尾，
也不得被报告为已经通过。

CPU affinity 只能减少部分 scheduler 竞争，不能替代容量测量。若以后需要高并发或明确的 market
p99 SLO，应先做分段、分因素基准，再评估 relay 私有 `HistoryExecutor` worker process；worker process
本身也不能隔离共享 LLC、内存带宽、page cache、磁盘或 IRQ/softirq。

## 明确不做

- 新 daemon、通用代理或多 provider aggregation；
- RemoteOnMiss、远端历史认证、relay cache 写入；
- batch POST、多 symbol、分页、cursor、streaming body；
- CSV、Arrow、Parquet、gRPC；
- provisional success；
- 默认 CORS；
- generation pin query parameter；
- relay 侧 snapshot GC。

### Affinity 与压缩配置

二进制使用 `TQSDK_RELAY_MARKET_CPU_SET` 和 `TQSDK_RELAY_HISTORY_CPU_SET` 配置 CPU
集合。两者均缺失时不启用 affinity/gzip；任一单边、空值、无效、重复、不可用、重叠或
实际绑定失败都必须 fail-fast。启动握手必须确认 market current thread、history
supervisor、每个 history Tokio worker 以及两个专用 gzip worker 均已成功绑定。

gzip 使用两个专用 worker、level 1 和 64 KiB threshold。提交采用有界、非阻塞 try
admission；池满立即返回 identity。响应协商带 `Vary: Accept-Encoding`，ETag 在选定的
identity/gzip 精确 bytes 上分别计算为 strong ETag，304 保留所选 representation 的
headers。10 s total timeout 包含压缩，512 MiB daemon budget 包含 scan、JSON 与压缩
buffer。生产同规格 gate 作为非阻塞容量特征项保留，CPU affinity 不是该测量的替代品。
