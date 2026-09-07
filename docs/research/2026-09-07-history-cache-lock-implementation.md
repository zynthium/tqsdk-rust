# Timeline 锁粒度实施与验证

2026-09-07。实现了[锁粒度研究](2026-09-07-history-cache-lock-granularity.md)推荐的最小方案。

- data 私有 `MinuteKlineReadPin`：复用 `.tqmk.lock`，稳定排序/去重，最多 256 分区。
- `TradingTimelineStore::rebuild_from_cache` 和公开直接 builder 均使用 root shared + 月 pin。
- pin 先于 metadata/coverage，保持至构建、合并和发布完成；失败通过 RAII 释放。
- 产品发布锁覆盖读取 active、merge 与 durable publish，保持多产品预验证语义。
- 没有修改 Tick fill、缓存文件格式、公共 API 签名或策略热路径。
- audit 不创建锁；activate 沿用既有共享 fill gate 的根锁初始化能力。

独立只读架构审查已完成，无剩余 correctness 阻断。实施过程中补齐了公开直接
builder 的同样锁语义，避免 CLI 与 SDK 分歧。旧 writer 兼容仍以遵守现有锁协议为前提。

## 验证结果

- data：266 项单元测试通过，其中 26 项 Timeline 测试。
- cache CLI：61 项单元测试通过，其中 2 项 Timeline/hook 测试。
- workspace examples check、data/cache all-targets clippy（`-D warnings`）、
  data/cache rustdoc（`-D warnings`）、fmt check、diff check 通过。
- data 五项本地 HTTP fixture 测试最初被沙箱禁止绑定回环端口；允许本地端口后全部通过。
  没有使用真实账号、远端补数或交易操作。

新增确定性测试包括跨进程 root-shared 共存、两个月锁在 metadata 前至发布前均排他于
writer、后续 pin 失败时释放前序 pin、缺锁不创建、不同月份不阻塞、FD 预算、
同产品两个交错增量重建保留并集；原测试继续覆盖根独占维护和发布 fsync 故障。

## 真实缓存只读验证

原缓存 `/root/.tqsdk/data_series_1`，Tick fill 进程 295575 仍持 READ 根锁时，
使用新编译 CLI 对 IM/CF 的 2024-01-01～2026-09-04 进行完整只读 audit：成功。

| 产品 | unique 日桶 | open intervals | timeline hash |
| --- | ---: | ---: | --- |
| IM | 700 | 1298 | `sha256:86fc99d361145dbc84fdda82d9c0a6d3ed2d343209cae9ecaac6dda855d39a95` |
| CF | 700 | 2578 | `sha256:1abb72ad54c7398db20386c07fa25a19d147b3de52741832377e4a7b247f136d` |

catalog hash：`sha256:df4af6466dd2dc46a23587b1058c37dec6fe45c9872670955c3672ee57e37963`。
命令未带 `--apply`，返回 `activated:false`。本轮未停止 Tick 任务、未写生产 active、
未提交 git、未部署 Docker。生产激活可继续通过正常带 pin 的 `timeline --apply` 流程，
不再需要等待无关 Tick fill 释放根共享锁；仍需处理真正重叠的月写锁或独占维护。
