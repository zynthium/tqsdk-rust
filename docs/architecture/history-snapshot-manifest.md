# History Snapshot Manifest v1

## 目的

本文固定 history snapshot root、manifest、lease、发布、恢复、回滚和 GC 合同。它不改变
`BacktestHistoryClient` 现有 cache-root 内部布局，也不定义新的行情文件格式。

publisher 是 `tqsdk-cache`；validator/reader primitives 位于 `tqsdk-data`；relay 只能通过这些
primitives 只读打开 generation。

## Root layout

```text
history-root/
├── CURRENT
├── snapshots/
│   └── <snapshot_id>/
│       ├── manifest.json
│       ├── lease.lock
│       └── cache/
└── staging/
```

- CURRENT 是 UTF-8 文本，只包含一个 `snapshot_id` 和结尾换行；
- `snapshot_id` 必须与目录 basename 和 manifest 字段完全一致；
- published generation 不可修改；
- staging 不是 reader namespace，relay 不遍历或修复 staging；
- history root、snapshots、staging 和 CURRENT 必须位于同一个受支持的本地 filesystem。

拒绝 NFS/object-store 语义、symlink parent、跨 filesystem publish、非原子 rename 或无法可靠 fsync/
advisory-lock 的部署。

## Manifest v1

`manifest.json` 至少包含：

```json
{
  "manifest_version": 1,
  "snapshot_id": "s-20260829-8d19c4af",
  "identity_sha256": "sha256:...",
  "created_at": "2026-08-29T12:00:00Z",
  "minimum_reader": "0.1.0",
  "cache_formats": [
    {"family": "tick", "format_id": "tqsdk.tqbn.daily.v3", "schema_version": 3},
    {"family": "minute", "format_id": "tqsdk.minute-kline.monthly.v5", "schema_version": 5},
    {"family": "daily", "format_id": "tqsdk.daily-kline.single-file.v1", "schema_version": 1}
  ],
  "metadata_snapshot_hash": "sha256:...",
  "catalog": {
    "complete": true,
    "symbols": []
  },
  "coverage_summary": [],
  "files": [
    {
      "path": "cache/series/20260829/tick/SHFE.au2612.tqbn",
      "role": "tqbn_mutable_layout",
      "size": 1234,
      "sha256": "sha256:..."
    }
  ]
}
```

实际 schema 可以增加 optional additive 字段，但 reader 遇到未知 required feature、未知 role 或不兼容
format 必须 fail closed。

### Canonical identity

- canonical identity payload 是 manifest 去掉 `snapshot_id` 和 `identity_sha256` 后的 JSON value；
- object keys 按 UTF-8 byte order 排序，无额外 whitespace，string/number 使用唯一 canonical encoding；
- `identity_sha256` 是 canonical payload 的完整 SHA-256；
- `snapshot_id` 是 `s-<created_at UTC YYYYMMDD>-<identity_sha256 前 8 个 hex>`；
- validator 重算 full hash，并同时校验 id、目录名和 manifest；8-hex id 只用于人类可读定位，不替代
  full hash 完整性；
- file list 按 normalized relative path 排序；catalog symbols、formats 和 coverage summaries 使用
  schema 定义的 canonical ordering。

manifest 不把自身 bytes 或 snapshot_id 放入被 hash 的 payload，避免 self-reference。

### Authoritative catalog

`catalog.complete=true` 表示 publisher 使用同一个 metadata snapshot 枚举并验证了本 generation
显式声明服务的完整 symbol universe；它不声称覆盖天勤全局所有合约。只有该声明和 data strict
inspection 同时成立时，该服务 universe 内的 symbol absence 才能映射 HTTP 404。

`complete=false` 或 catalog 缺失时，reader 可以查询已列出的 symbol，但不能从 absence 推断不存在。
coverage_summary 是审计/运维摘要，不是 query 或 `/coverage` 的 authority；每个请求仍由 planner 和
实际 cache source inspection 重新证明 coverage/finality。

## File roles 与 clone policy

分类按 manifest role 和 validator allowlist，不按未知扩展名猜测：

| role | 典型文件 | 允许 clone |
| --- | --- | --- |
| `tqbn_mutable_layout` | `.tqbn` | reflink，失败后普通 copy；禁止 hardlink |
| `tqmk_immutable_generation` | `.tqmk` | reflink、hardlink、copy |
| `tqdk_immutable_generation` | `.tqdk` | reflink、hardlink、copy |
| `metadata_content_addressed` | immutable metadata snapshot | reflink、hardlink、copy |
| `pointer_copy` | `active.json` 等 pointer | 独立 copy 或 rebuild |

必须排除并重建：

- `.tqbn.lock`、`.tqmk.lock`、`.tqdk.lock`、`.metadata.lock`；
- cache-root operation lock 和 `lease.lock`；
- temp、partial、recovery 或 writer sidecar。

symlink、device、FIFO、socket、absolute path、`..` escape、重复 normalized path、大小/hash 不符、
未知 role 全部 fail closed。禁止未经分类的 `cp -al`。

`.tqbn` 可能 append、checkpoint truncate 或 recovery truncate，因此永不 hardlink。当前
`.tqmk`/`.tqdk` writer 使用临时文件加原子 pathname replace；只有这个不原地修改合同仍成立时才能
跨 generation hardlink。任何未来改为原地写的格式，必须先收窄 role policy。

Daily 保持一个 logical symbol 一个 `.tqdk`，minute 保持按月，Tick 保持按交易日；snapshot 层不改变
这些格式。

## Publisher state machine

从 writable cache root 导入时，publisher 必须先取得该 root 的 exclusive
`.tqsdk-cache-operation.lock` stable-view gate。所有受支持 writer 都必须通过同一 gate 协调；
不受该 gate 约束的外部 writer 与在线导入不兼容。publisher 在 gate 内完成文件枚举、role
classification、clone/copy 和 source identity 复核；任一 source 在捕获窗口变化就丢弃 staging。
完成稳定 clone 后才能释放 source gate，后续 prewarm 只写 staging。

1. 取得 history-root publisher operation lock。
2. 解析当前有效 generation；若从 writable source root 导入，取得 exclusive stable-view gate。
3. 在 stable-view gate 内创建 staging clone 并复核 source identity，然后释放 source gate。
4. 重建所有 lock/temp/pointer，预热只写 staging。
5. 用 data strict inspection 对计划 publish 的 catalog/range 做 CacheOnly verify。
6. 至少执行一个覆盖每类实际数据源的真实 query smoke，并验证 terminal coverage、finality 和
   metadata hash。
7. 枚举并 hash files，生成 manifest 和 identity。
8. 重新只读打开 staging，按 manifest 验证文件 role、size、hash、catalog、format 和查询。
9. 按 durability order 发布 generation。
10. 原子切 CURRENT 并完成 history-root fsync。
11. 发布事务成功后，在独占 lease 条件下执行 best-effort retention/GC。

CURRENT rename 是 publish commit point。commit point 之前的任何失败不得改变 CURRENT。rename
成功但 history-root fsync 失败时，结果是 indeterminate：publisher 不回切、不删除任一候选
generation，也不自动重试覆盖；operator 必须运行 recover，重新解析并验证 CURRENT 后补足目录
durability。GC 不属于 publish transaction；GC 失败只报告 maintenance failure，不能把已经成功
发布的 CURRENT 判为回滚。

### Durability order

固定顺序：

1. flush/sync staging data；
2. 写入并 sync manifest；
3. sync `cache/` 和 staging generation directory；
4. 原子 rename completed generation 到 `snapshots/<snapshot_id>`；
5. sync `snapshots/`；
6. 写 `CURRENT.tmp`、flush/sync；
7. 原子 rename 为 CURRENT；
8. sync history root。

只有第 8 步成功后 publish 才报告 committed。第 7 步完成但第 8 步失败按上述 indeterminate
状态处理，不声称旧 CURRENT 仍然有效，也不尝试通过第二次 rename 猜测回滚。

同一 snapshot id 已存在时，只有完整 revalidation 证明 byte-identical 才可把操作视为 idempotent；
否则 identity collision/corruption fail closed。

## Reader open 与 hot swap

relay 的 reload 顺序：

1. 读取 CURRENT；
2. 打开对应 generation directory 和 manifest；
3. 取得 `lease.lock` shared lease；
4. 重读 CURRENT；若 pointer 已变化，释放并重试；
5. 验证 manifest identity、compatibility、paths、roles、sizes、hashes、catalog 和 metadata；
6. 创建 data-owned read-only CacheOnly snapshot handle；
7. 再次确认 generation 未被标记 unhealthy；
8. 原子替换当前 `Arc<Snapshot>`。

每 5 秒轮询一次。新 generation 无效时记录 bounded alert 并继续旧 generation；没有旧 generation 时
history unready/503。

snapshot query 必须把 shared lease 放入 data coordinator 所有权，而不只是 HTTP handler Arc。Drop/
disconnect/timeout 发出取消后，lease 直到 coordinator 与已启动 blocking scan 全部退出才释放。

runtime unhealthy 是 relay-owned、generation-local 的内存状态，不写回 immutable snapshot。
同一 loaded generation 的所有请求共享一个原子 `healthy -> unhealthy` 转换；检测 corruption 的
唯一 winning request 返回首次 500，其他并发和后续请求返回 503。轮询到同一 snapshot id 不得
清除状态；只有切换到不同、完整验证成功的 snapshot id，或进程重启后重新完成严格验证，才创建
新的 health state。publisher 只通过独立 scrub 报告持久 corruption，不消费 relay 内存状态。

## Recover、rollback、scrub

- recover 清理或隔离未完成 staging/CURRENT temp，但不猜测或发布缺 manifest/未 sync 的 generation；
- CURRENT 缺失或损坏时，只能从完整、兼容、重新验证成功的 retained generation 显式恢复；
- rollback 是 publisher command：验证目标、取得必要 lease、原子切 CURRENT；不修改目标 generation；
- scrub 重算 manifest identity 与全部 file hashes，并执行 data strict inspect/query smoke；
- 已发布 generation 的任何 file mismatch 都标记该 generation corrupt，不原地 repair；
- active generation 首次运行时 corruption 由 relay 返回一次 500 并标记 unhealthy，之后返回 503。

recover/rollback/scrub 都必须可 dry-run，并输出计划动作而不写入。

## Lease 与 GC

- relay/reader 持 generation `lease.lock` shared lease；
- publisher GC 只有取得同一文件 exclusive lease 后才能删除 generation；
- relay 永不 GC；
- retention 默认保留 CURRENT 加前两个兼容 generation；
- 被 shared lease pin 的 generation 不计为可立即删除；GC 跳过并在下次重试；
- 删除前再次确认目标不是 CURRENT，并在取得 exclusive lease 后重读 CURRENT；
- 删除完整 generation directory 后 sync `snapshots/`。

lease 是 generation-local coordinator lifetime contract，不是 HTTP request lifetime contract。

## Dry-run 与容量报告

`tqsdk-cache snapshot ... --dry-run` 必须保持 source、staging、CURRENT 和 snapshots 全部不变，并报告：

- reflink 是否实际支持；
- 每个 role 的 reflink/hardlink/copy 文件与字节；
- 预计新增 allocated bytes；
- publish 后 retained generations 和磁盘占用；
- 会被 GC 但当前 leased 的 generations；
- 不兼容格式、未知 role 或空间不足。

dry-run 不得为探测而创建持久目录或锁；临时探测只能位于操作专用临时位置并在返回前清理。

## 格式兼容与 rollout

- reader 先支持 manifest/format，publisher 后输出；
- manifest minimum reader 和每个 cache format 必须同时满足；
- relay history feature 只启用本地 reader codec；
- minute v4 必须先走现有 `tqsdk-cache --kind minute migrate --apply --backup-dir`；
- minute v3 和未知格式保持 `LegacyUnsupported`/fail closed；
- 新旧 writer 不得同时写同一 staging/source root；
- rollback 只切 pointer 到已验证兼容 generation，不能依赖旧 binary 读取新格式。
