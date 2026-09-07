# 每日历史缓存与 Timeline 调度建议

状态：配置设计，未安装 cron、未创建调度脚本、未使用凭据执行 fill。

## 当前可用与缺口

`tqsdk-cache fill` 支持 tick/minute/daily、闭合交易日解析、final 门禁和缺口补齐。
`--start-day` 配合省略 `--end-day` 可由 CLI 决定最近已结束交易日；
`--last-trading-days` 可做有界重叠检查，但固定回看 N 日不能覆盖停机超过 N 日的缺口。
fill 的 family 要分别调用，不把 `--kind all` 当作批量 fill。

现有 `--trading-timeline-catalog` hook 只负责填充后的 rebuild/activate，
不会自动维护候选规则有效期或推断、确认新增例外。
生产 IM/CF catalog 当前截至 2026-09-04；直接挂 hook 不能覆盖以后日期。
而且 catalog hash 变化、旧日期仍需保留时，`merge_timeline` 要求完整重建旧范围，
不能把新 catalog 的单日候选直接合并到旧 generation。

因此不能声称仅安装几条 cron 即可保证 Timeline 每日自动前进。

## 最小调度设计

建议两条队列：minute → Timeline → daily 为轻量队列，Tick 独立队列。
目录审查失败不得阻止 minute/daily/Tick 数据正常更新；失败须可见并保持旧 Timeline active。
每日 18:40 运行轻量队列，20:10 运行 Tick；22:40、次日 06:10 分别补一次可重入重试。
时间均指 Asia/Shanghai。每日运行包括周末，休市判定交给 CLI，不在 cron 推算交易日。
这些时间是启动策略，不是供应商 final 数据就绪保证；失败重试不能改用 provisional。

各队列使用独立、外层 `flock -n` 防重入锁，绝不手动锁 `.tqsdk-cache-operation.lock`。
CLI 的 root/月分区锁继续提供数据一致性；外层锁只防同一计划重复启动。
全量历史补数结束后再启用相同范围的日常任务，避免重复等待和资源竞争。

每个 family 与 symbol scope 各有成功水位，初值必须来自该 scope 的完整 final 审计。
从最后成功交易日开始包含式重试，只在 CLI 成功且最终报告确认 complete/final 后推进水位。
失败/超时不推进；配置 scope 改变后单独 bootstrap，不复用其他 scope 的水位。
选择终点由 CLI 日历负责，成功水位取报告 requested_days.end_day，不直接用 date 的自然日。
时间范围过大可显式分段追赶，不为每天新建通用任务平台。

精确 symbol 清单适合稳定小范围；全期货使用已审定、按 kind 区分的 universe 表达式，
避免遗漏新合约或给 Tick/Daily 套用不适用的 minute scope。minute scope 必须显式包含
`KQ.i@CFFEX.IM` 与 `KQ.i@CZCE.CF`，不能因为 broad scope 排除了 CFFEX 而把证据指数漏掉。

认证由权限 0600 的独立本机 env 文件载入，不放仓库、crontab 或命令行；不启用 set -x。
固定已验证的二进制绝对路径，不让 cron 自动 cargo build/update；配置日志轮转与失败告警。
分钟缓存、Tick、Daily、Timeline 分别记录成功末端，不能只监控 cron 进程退出。

## Timeline 每日维护尚需补齐

需要一个受限的维护步骤：在 final 指数证据完整后，对新增闭合交易日进行候选匹配；
在操作者已批准的 cache_inference 策略内确认休市/取消夜盘，保存带 evidence hash 的例外。
缺 coverage、provisional、冲突/歧义仍失败，保留旧 active，不能 sed 延长日期或无条件 confirmed。

优先预审一个有限未来规则范围（例如当年末），避免每天只因日期字段更新 catalog hash。
这不是把未来例外提前确认为事实；首次扩大范围需要按新 catalog 完整重建已有日期。
无 catalog 变化时正常增量；新例外导致 catalog hash 变化时，第一版沿用受控全范围重建。
IM/CF 目前约 66 月分区，在 256 pin 预算内；只在 catalog 真正变化时重建，不能每天全量重扫。
不在 cron 脚本中另造会话推断算法，不取消现有 hash/coverage 门禁。

## cron 模板（维护脚本实现后安装）

以下 `/usr/local/sbin/tqsdk-cache-daily` 是待实现的维护脚本，不是现有仓库命令。
使用 root 的 `crontab -e` 时不加 username；如果写 `/etc/cron.d/` 则须加 root 列。

```cron
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
40 18,22 * * * /usr/bin/flock -n /run/lock/tqsdk-kline-daily.lock /usr/local/sbin/tqsdk-cache-daily kline >> /var/log/tqsdk-kline-daily.log 2>&1
10 6,20 * * * /usr/bin/flock -n /run/lock/tqsdk-tick-daily.lock /usr/local/sbin/tqsdk-cache-daily tick >> /var/log/tqsdk-tick-daily.log 2>&1
```

本机为 Debian 13 cron；其调度按守护进程系统时区，不应依赖 Cronie 的 CRON_TZ。
设置 TZ 环境变量只能影响命令内时间转换，不保证改变 cron 调度时区。
来源：[Debian trixie crontab(5)](https://manpages.debian.org/trixie/cron/crontab.5.en.html)。
外层非阻塞协作锁语义：[flock(1)](https://man7.org/linux/man-pages/man1/flock.1.html)。

本研究仅记录配置建议；在每日 Timeline 目录维护与水位脚本实现前，不应宣称全链路无人值守。
