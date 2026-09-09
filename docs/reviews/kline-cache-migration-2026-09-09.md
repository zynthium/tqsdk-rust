# 本地 canonical Kline 迁移执行记录

状态：完成。迁移目录为 `/root/.tqsdk/data_series_1`。
用户授权向持锁 `tqsdk-cache` 发送一次 SIGINT；进程正常退出，未强杀、未删除锁。
未重新启动 fill，未中断其他服务，未访问远端行情或账号。

## 数据保全结果

| 项目 | 数量 |
| --- | ---: |
| 全部日线文件 | 5,393 |
| 全部分钟线文件 | 63,277 |
| 总校验文件 | 68,670 |
| 原有 KLOG 文件 | 827 |
| 本次 raw → KLOG | 67,843 |
| 最终遗留 raw | 0 |
| 迁移前 canonical 文件字节 | 2,708,716,220 |
| 迁移后 canonical 文件字节 | 2,740,843,703 |

每个转换文件在原子替换前保存原件，并逐字节证明新片段保留原始 payload、用新 reader 深验。
转换后完整复验通过；运行时兼容收缩后的最终 release 工具再次深验，同样是 68,670 文件、0 raw。
Tick、provisional minute、metadata、universe、Timeline 和其他历史文件未转换或删除。

## 回滚材料与部署

备份：`/root/.tqsdk/kline-migration-backup-20260909/`。

- 按原相对路径保存迁移前 canonical 文件。
- `migration-report-1788918787054307937.json`：正式 apply 的结果。
- `tqsdk-cache-installed-before-migration`：安装目录中原始 CLI。
- `tqsdk-cache-pre-contraction`：收缩前兼容 CLI。
- `migrate-kline-cache-current`：最终显式迁移工具；先前工具也保留。

最终 CLI 已用临时文件、fsync 和原子替换更新到 `/root/.cargo/bin/tqsdk-cache`。
替换前确认安装文件仍等于已保存的原件；替换后确认其与验证过的 release artifact 完全相同。
程序 crate 版本仍为 0.1.0，不能仅凭版本字符串判断是否支持当前布局。

回滚必须遵守 [停写、排他锁和备份恢复合同](../architecture/kline-cache-migration.md)。
未删除本次备份或以前的 `.kline-append-backups/`。

## 验证与架构

- `cargo test -p tqsdk-data -p tqsdk-cache --offline --quiet`：通过。
- `cargo test -p tqsdk-relay --tests --offline --quiet`：通过。
- `cargo test -p tqsdk-data --no-default-features --test kline_cache_migration --test kline_append_recovery --offline --quiet`：通过。
- `cargo clippy -p tqsdk-data -p tqsdk-cache --all-targets --offline --quiet -- -D warnings`：通过。
- `cargo check --examples --offline --quiet`：通过。
- `git diff --check`：通过。

mock socket／子进程信号测试在允许相关系统调用的环境中执行，未运行 ignored/live 测试。
这是持久化兼容合同收缩；架构格式、snapshot、API、边界、迁移、验证文档和 README 已同步。
普通 reader/fill/writer 必须 KLOG；旧 standalone 解码仅留在显式迁移路径。

独立只读 reviewer：1 个。两项审查返工已修复：追加后重跑迁移的备份幂等；v4 迁移的同根排他门禁。
最终无代码安全阻塞项；文案矛盾已同步。审查耗时未单独记录，token usage 不可用，不作估算。
代码未提交；保留工作树中用户原有修改。
