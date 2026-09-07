# TradingTimeline

2026-09-07 生产重建完成：75 个目标品种均有 active Timeline，已知范围不同。
67 个原有品种覆盖 2024-01-01～2026-09-04。八个新上市品种已定向重建并发布至
2026-09-04；首个已确认交易日为 PL 2025-07-23、PR 2024-09-02、bz 2025-07-09、
pd/pt 2025-11-28、ps 2024-12-27、ad 2025-06-11、op 2025-09-11。
其上市首日缺少指数证据，未推定为休市；不能声称这些品种从 2024 年或上市首日即全部覆盖。
准确墙钟边界见验收摘要的 known_ranges；既有消费者须重新 load_active 使用新 generation。
详见[生产重建记录](../research/2026-09-07-trading-timeline-production-rebuild.md)。
在真实 Tick fill 持根共享锁时，IM/CF 的 2024-01-01～2026-09-04 原缓存审计通过，
各 700 个日桶已决；随后已在原缓存完成生产 active 激活、增量一致性与计算回读验证。
旧审计中的 Tick 占锁阻断已解除，详见
[生产激活闭环](../research/2026-09-07-trading-timeline-production-activation.md)。
详见 [锁粒度验证记录](../research/2026-09-07-history-cache-lock-implementation.md)。

## 2026-09-07：受限范围的缓存确认口径

经操作者明确接受，完整、final 的规范指数分钟缓存可以作为例外确认依据：
候选日全部无正成交量证据时确认休市；仅候选夜盘无正成交量、保留日盘各段
均有正成交量时确认取消夜盘。记录为 `cache_inference`，不是交易所认证。
这项策略仅用于操作者维护的范围明确 catalog：
CFFEX.IM 为 2024-01-01 至 2024-12-31，CZCE.CF 为 2024-02-06 至 2024-12-31。
其 `exception_review_complete=true` 表示操作者接受该范围的缓存推断审查，
不表示已经逐项找到官方公告。下文“exception-complete”的要求按此口径解释；
原有 draft/reviewed catalog 保持不变。

不能因此推断缺失 coverage、provisional、范围外或其他品种也已确认。
现有重建与发布门禁不变；新增正成交量若与休市例外冲突，必须拒绝激活并复核。
缓存中的供应方遗漏、段内稀疏成交导致的延迟开盘/提前收盘无法被此策略保证识别；
本目录明确接受这些风险，不再将“没有公告原文”单独作为该范围的激活阻断项。
证据摘要用于追溯，不替代每次 rebuild 对当前缓存的检查。

验证与操作命令见 [缓存确认闭环](../research/2026-09-07-trading-timeline-cache-confirmed.md)。

`tqsdk-data` owns trading-time arithmetic, rule matching, minute-cache evidence and
immutable product generations. `tqsdk-cache` owns offline maintenance and the fill
hook. Relay stays range-only; it does not infer calendars or expand row windows.

## Semantics

Intervals are half-open. `trading_duration_between` measures only known open
intervals; `shift` advances through their cumulative duration. Both operate on an
immutable in-memory prefix index using binary search, without disk access, locks
or allocation. Unknown coverage is an error, never an assumed closure.

Session templates use minute-aligned offsets from 18:00, with an explicit 06:00
night/day split. Night windows use the storage day's starting civil date; day
windows use the actual trading-day date. This avoids mapping Monday's 09:00 to
Saturday merely because the TQBN storage partition starts Friday at 18:00.
This convention does not define exchange holidays: exceptional closures and
removed/replaced sessions still require dated catalog evidence.

Only positive-volume, final canonical 60-second index rows prove tradability.
Every session in the selected rule needs positive evidence; every positive row
must fit it. Sparse rows are not counted as elapsed seconds. A missing session,
unknown rule, ambiguous match or incomplete coverage cannot activate a generation.

## Maintenance and resource bounds

`TradingTimelineStore::rebuild_from_cache` acquires one shared root lifecycle gate and
pins every requested `(evidence_symbol, trading_month)` through existing `.tqmk.lock`
shared locks before resolving metadata or inspecting coverage. All pins survive
through publication; unrelated Tick/Daily fills and other minute partitions can proceed.
The request rejects more than 256 distinct partitions before opening partition locks;
missing sidecars fail closed and audit never creates them. Busy partition errors name
the resource and release previously acquired guards. Root-exclusive maintenance stays excluded.
Product publication locks are acquired in stable order before loading active generations;
they cover merge, validation and durable publication, preventing lost incremental updates.
The immutable metadata snapshot is then fixed for each product, and the request
resolves each index's minute metadata identity, validates final coverage, builds
and verifies all requested products, and activates independent product snapshots.
Do not invoke rebuild while holding a target minute write lock or product publication
lock. A shared root fill gate is compatible. Audit mode opens the existing
root lock read-only and writes no timeline files; a busy root fails retryably.

Evidence requests are grouped by trading month. One reader per group performs
the cache's existing full validation pass and one streaming pass; rows are retained
only for the current day. This is O(selected months), not one full-month scan per
requested day. A sparse/non-contiguous request may validate intervening coverage
within the selected month; no remote fill or calendar guessing is attempted.
Evidence identity hashes the pinned minute metadata and consumed rows. Catalog
identity hashes normalized runtime fields including authorities; mutation after
validation is rejected.

Activation rejects any unresolved requested day. Same-catalog incremental updates
replace requested days and retain other known days; changing catalog requires a
rebuild covering the prior generation. The current store is one compact,
atomically replaced file per product:
`trading-timeline-v1/products/<exchange>/<product>/timeline.json`. It stores
minute-aligned interval pairs and compact day-decision tuples, while
`timeline_hash` continues to identify the canonical expanded in-memory snapshot,
not its storage bytes. File and parent directories are fsynced around the rename.
Product publication is serialized. Products are independent: this is **not a
cross-product transaction**; an I/O failure during activation can leave
different products on different valid generations. Callers needing repeatability
retain the immutable snapshot/hash they loaded.

## CLI

Activation has one public entry point, `TradingTimelineStore::rebuild_from_cache`.
Raw publication is private. Catalogs must explicitly attest
`exception_review_complete: true` for **every rule's entire dated scope** before
activation. This is an operator-reviewed authority statement, not a conclusion
derived from sparse minute rows. Without it, audit remains available but `--apply`
and the fill hook refuse activation. The supplied reviewed-2024 subset deliberately
does **not** carry this attestation: template sources do not prove an exhaustive
exception history, including delayed opening or early closing within a session.

Evidence digests are stored per trading day. Incremental replacement drops the
replaced days' old evidence and computes the generation identity in canonical day
order; repeating an unchanged rebuild does not create a new hash.

After a rename, a parent-directory sync failure is reported as
`TradingTimelineDurabilityUncertain` inside `DataError::Io`. The new
`timeline.json` may already be visible: callers must reload it, not assume the
previous generation remains active. Fill diagnostics mark
`activation_state: indeterminate` and include the committed path. The fill hook
pins its parsed catalog before filling; it does not reopen a potentially changed
catalog.

### Pre-compact layout conversion

The current and only runtime layout is the v1 `timeline.json` file. Runtime
loading never falls back to the former v1 `active.json` plus
`snapshots/<hash>.json` layout. `TradingTimelineStore::migrate_legacy_active`
is a one-time, root-exclusive conversion helper; it validates the old active
hash before writing `timeline.json`. The pre-compact files remain an offline
recovery source until a later explicit cleanup with a per-product old/new hash
manifest; recovery after deletion requires rebuilding from final minute cache
and the pinned catalog. There is no mixed-layout runtime contract.

Audit existing minute caches without credentials or remote data:

```bash
tqsdk-cache timeline --cache-dir /path/to/cache \
 --catalog /path/to/operator-catalog.json \
  --symbol KQ.i@CFFEX.IM --start-day 2024-01-02 --end-day 2024-01-31
```

Add `--apply` only with an exception-complete catalog. Omitted `--symbol` selects all products in the catalog;
select a catalog matching the intended fill scope and date range.

Both ordinary and historical-universe minute fill paths accept
`--trading-timeline-catalog PATH` together with `--require-final`. After successful
fill/finalization they release their fill gates and run maintenance under a fresh
exclusive gate. No option means unchanged fill behavior. Dry-run performs no
maintenance. Provisional/open-day mode is rejected for this hook. Failure changes
the command exit status and adds `trading_timeline` diagnostics without deleting
the successfully filled market data. The fill report describes the data stage;
the command result describes timeline maintenance separately. Required index data
must already be final in cache; the hook never downloads extra evidence symbols.

## Authority coverage and limits

The reviewed 2024 catalog contains only CF (from 2024-02-06) and IF/IH/IC/IM
templates supported by the linked exchange documents. It is not a complete
holiday calendar or certification of all 2024 dates. The broader 15-template
draft remains research input, not an activation catalog. Unsupported historical
epochs and missing holiday exceptions remain unresolved. See
[authority audit](../research/2026-09-06-trading-timeline-2024-authority.md).

No new context endpoint, batch API, public cursor, streaming HTTP body, calendar
daemon, reverse TQBN index or source-cache write path is introduced. Published
relay snapshot manifests do not yet carry timeline artifacts: load/retain a
product generation explicitly, then query the existing history range interface.

## Verification

2026-09-07 release performance validation: 75 production timelines, 270 million timed successful
queries without heap allocation; separate 6-million-query trace had no file/read/futex calls inside
the measured hot region. Mixed-product duration ~113 ns, shifts ~184 ns on Ryzen 9 5900X;
these are amortized batch timings, not portable latency guarantees. Load once outside the replay loop.
See [method, limitations and CPU budget](../research/2026-09-07-trading-timeline-performance.md).

Final offline gates on 2026-09-06: data/cache/relay `--tests` completed with
956 passed, zero failed, six ignored. The 20 timeline unit/regression tests include
idempotent incremental evidence and both publication durability failure paths.
Workspace examples, the S40 arithmetic example, formatter, three-crate clippy
with `-D warnings`, and three-crate rustdoc with `-D warnings` passed.
The relay build script warned that npm/dashboard building was unavailable;
these results do not certify a dashboard rebuild. No credentials, remote fill,
production timeline activation, commit or deployment were performed.

Unit regressions cover Monday anchors, sparse positive evidence, one monthly
validation scan for many days, incremental retention, failed activation, corrupted
bodies and fill-gate contention. The cache CLI test exercises both fill-report
shapes without authentication. The public arithmetic contract is
`api_contract_s40_trading_timeline`.

2026-09-06 local frozen-copy audit: IM January 2024 matched 22 days/44 intervals;
CF March matched 21 days/84 intervals. Original cache was busy, so only selected
month files and their immutable metadata were copied; original cache was not
modified. `strace` on the 171,757-byte IM month showed 3 opens and 66 reads for
the entire 22-day request (header/validation/streaming), about 130 ms in a debug
build. These are logical syscalls on a warm filesystem, **not physical disk IOPS
or a cold-cache benchmark**. No full-cache/year-wide audit is claimed.
