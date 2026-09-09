# Canonical Kline 离线格式收缩

## 当前契约

工作区 P1 过渡尚未部署：canonical 日线和分钟线使用 `TQHIST01` 统一容器，
Minute 公开格式为 monthly.v6；真实默认缓存与已安装 P0 binary 尚未切换。
新建、重叠更新、snapshot identity 变更和压实都输出 common 容器；普通 fill 只恢复提交尾部，
不再转换旧 raw/KLOG 文件。query、coverage、doctor 均不把旧格式作为当前格式读取。

旧 standalone/KLOG 解码只供显式离线迁移；旧写入器仅作为测试夹具编译。
Tick、provisional minute、metadata、universe 和 TradingTimeline 的格式不在本次收缩范围。

## 显式迁移

停止同一 cache root 的写入程序。不可删除 lock 文件或强行绕过锁。
以下工具不需要账号，不访问网络：

```bash
cargo run -p tqsdk-data --release --example migrate_kline_cache -- ROOT BACKUP
cargo run -p tqsdk-data --release --example migrate_kline_cache -- ROOT BACKUP --apply
cargo run -p tqsdk-data --release --example migrate_kline_cache -- ROOT BACKUP
```

第一、三条是只读预演／深度复验，BACKUP 可以尚不存在，但父目录必须存在。
最后一次必须满足 `legacy_files=0`，且 `verified_files` 与完整迁移前 inventory 一致。
旧 minute v4 需要先通过其原有显式备份迁移入口转换；普通 reader 不接收 v4。

`tqsdk-data::migrate_kline_cache` 拥有转换与数据保全逻辑，example 只解析参数、输出报告。
工具持有现存 root operation lock 的排他锁，再持目标分区排他锁。锁忙立即失败。
拒绝 symlink/non-regular namespace entry；拒绝在 published generation 内迁移。

每个文件依次执行：

1. 深度验证旧 payload、path identity、coverage 和行；KLOG/common 必须尾部干净。
2. 在 ROOT 外保存原文件，优先 hardlink，跨文件系统时临时复制后原子发布；同步备份与目录。
3. 在独占创建的候选文件中重新编码为 common 容器，不更改原文件。
4. 用新 reader 深度读取候选，逐字段比较行情（含浮点位模式）、coverage、metadata；确认等价。
5. 原子替换 canonical 文件，再同步父目录。

迁移不是跨文件事务。中断后保留已完成文件，使用相同 ROOT/BACKUP 重跑；将被替换的旧文件
若已有备份，必须与当前文件逐字节相同，否则拒绝覆盖。已有 common 深验后直接跳过，不重写
或重新要求备份匹配，因此正常追加后也可重跑同一迁移。
每次成功 apply 保存独立 JSON 报告，不覆盖旧报告。残留临时文件不属于 cache coverage。
迁移期间、复验前不得重新启动 writer；不得将部分迁移误报为全量完成。

## 回滚与部署

不自动删除本次 BACKUP 或以前的 `.kline-append-backups/`。备份不属于 live cache、
不参与 snapshot 发布。已有备份若共享 inode，后续 writer 的 COW 必须保护它。

回滚先停写、获得 root 排他门禁，再将本次 BACKUP 内对应相对路径的文件用临时复制和
原子替换恢复；不可在 writer 运行时直接覆盖。使用迁移前保留的兼容 binary 审计。
回滚只恢复备份时点，不包含之后追加的数据；若已有新数据，先另存当前完整状态。

运行时兼容收缩后，所有访问该 root 的程序必须重新构建／升级，不支持旧版本滚动混跑。
旧 published snapshot 不可原地改写，应对私有 writable clone 做迁移并重新发布 generation；
新 manifest 必须声明 `history-container-v1`。

旧 v4 public 迁移入口同样在发布前读回候选并逐字段验证；额外在
`ROOT/.kline-append-backups/minute-v4-to-common/` 保留原始文件。该内部恢复备份不属于
可发布数据，不能替代运维层的外部 BACKUP；CLI 的外部备份继续保留。

## 验收

```bash
cargo test -p tqsdk-data --test kline_cache_migration
cargo test -p tqsdk-data --test kline_append_recovery
cargo test -p tqsdk-data -p tqsdk-cache
cargo clippy -p tqsdk-data -p tqsdk-cache --all-targets -- -D warnings
```

验证根门禁、预演零写入、原始字节保全、备份保全、重跑幂等，以及普通 reader/fill
拒绝旧 raw/KLOG、新建和覆盖写均输出 common 容器。格式细节见 [history-cache-format.md](history-cache-format.md)。
