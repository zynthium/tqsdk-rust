# fill 中断恢复优化执行记录

日期：2026-09-09。

## 交付范围

1. 日线保留已有的最多 32 个日历日分段提交，补齐中断后跳过已提交分段的回归测试；优雅停止后不再开启下一分段。不是新增或缩小原有日线分段。
2. 分钟线在原有最多 10,000 分钟的父窗口内，以最多 24 小时的子请求保存私有恢复日志。日志不计入正式 coverage；重启复用已确认前缀，重取并逐字段验证最后一个已确认子区间作为重叠。未确认尾部可能重下。父窗口完整成功后一次提交正式缓存。
3. 首次 Ctrl+C 请求停止新工作，给当前工作 5 秒收尾；超时请求强制取消，第二次信号立即退出。5 秒不是文件系统阻塞或进程退出的硬实时上限。
4. 进度区分已接收、正式提交、暂存以及需要重新下载的范围。未知进度显示 n/a 或 null，不伪装成零；统计范围是本次观察到的填充窗口，不是全库总量。

同时修复失败重试残留行混入成功结果、未完整确认终止事件即提交、共享消费者停止影响其他消费者、最后提交成功被迟到停止信号覆盖等相关边界。

旧的公开 telemetry 结构体字面量和 snapshot disposition 穷尽匹配保持源码兼容；新增独立、可选的 durability observer。恢复日志不会进入发布快照。

权威契约见 [history-fill-recovery.md](../architecture/history-fill-recovery.md)。本次涉及持久化恢复、取消和新增公共 API，属于架构更新，相关架构文档与 crate README 已同步。

## 安全与取舍

- 按 migration 技能采用独立私有日志、校验和、原子替换和可回退部署；正式缓存格式不变，本轮无需再次迁移。
- 日志损坏或身份不符明确报错；重叠数据不一致时仅丢弃未发布日志并报错，不污染正式缓存。
- 分钟子请求会增加连接/请求开销；恢复仍需重下重叠子区间及未确认尾部，不承诺零重复下载。
- 未执行真实账号、联网下载或 live smoke；未启动或重启 fill，未修改现有正式缓存。

## 验证

以下最终代码检查均退出 0：

```bash
cargo test -p tqsdk-data -p tqsdk-cache --offline --quiet
cargo clippy -p tqsdk-data -p tqsdk-cache --all-targets --offline --quiet -- -D warnings
cargo test -p tqsdk --lib backtest_remote --offline --quiet
cargo check --examples --offline --quiet
cargo test -p tqsdk-data --no-default-features --lib backtest_history::fill --offline --quiet
cargo test -p tqsdk-relay --tests --offline --quiet
cargo build -p tqsdk-cache --release --offline --quiet
git diff --check
```

无默认特性 fill 测试 29 项通过；facade backtest_remote 测试 8 项通过。新增测试覆盖暂存续跑、日线分段续跑、重试清理、重叠不一致、错误/缺失终止事件、关闭失败、停止与最终提交竞态、源码兼容、快照排除私有日志。

使用 1 位只读 architecture reviewer；初审指出的停止语义和公开结构体兼容性问题均已修复，最终定点复核无剩余阻断。未独立计量代理时间或 token。

## 本机部署

- 已原子替换 `/root/.cargo/bin/tqsdk-cache`，与本次 release 构建逐字节一致；版本仍为 `0.1.0`。
- 旧程序保存在 `/root/.tqsdk/fill-recovery-binary-backup-20260909-mICq7V/tqsdk-cache`，部署前已同步落盘。
- 安装后 `fill --help` 成功；移除认证环境变量后运行默认目录的 `doctor --kind daily --output-format json`，返回 `status: success`、`exit_code: 0`、`error: null`。本轮未重新执行全量分钟线 doctor。
- 回退时可使用上述备份二进制；旧程序的普通 fill 忽略新私有日志，不能利用其中的恢复进度。正式已提交缓存不依赖私有日志。
- 未提交 Git；保留工作区已有改动。
