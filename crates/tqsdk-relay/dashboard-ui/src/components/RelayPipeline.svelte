<script lang="ts">
  import { formatDuration, formatNumber } from '../lib/format';
  import type { IntegrityModel, TimelineSeverity } from '../lib/types';

  let { model }: { model: IntegrityModel } = $props();

  type Node = {
    name: string;
    icon: string;
    state: string;
    meta: string;
    severity: TimelineSeverity;
  };

  const liveNodeClass =
    'node live box-border grid h-[60px] min-w-0 grid-cols-[30px_1fr_8px] items-center gap-[7px] rounded-[9px] border border-[color:var(--relay-line-soft)] bg-[#061a2be6] px-[10px] py-[5px] shadow-[inset_0_0_20px_#20d8ff0b,0_0_18px_#20d8ff0d]';
  const liveStateClass = 'state truncate text-[13px] leading-[1.2] font-[850] text-[color:var(--relay-live)]';
  const liveDotClass =
    'status-dot live rounded-full bg-[color:var(--relay-live)] shadow-[0_0_10px_color-mix(in_srgb,var(--relay-live)_70%,transparent)]';

  const nodeClass = {
    live: liveNodeClass,
    auction: liveNodeClass,
    warn:
      'node warn box-border grid h-[60px] min-w-0 grid-cols-[30px_1fr_8px] items-center gap-[7px] rounded-[9px] border border-[#ffc44766] bg-[#061a2be6] px-[10px] py-[5px] shadow-[inset_0_0_20px_#20d8ff0b,0_0_18px_#20d8ff0d]',
    bad:
      'node bad box-border grid h-[60px] min-w-0 grid-cols-[30px_1fr_8px] items-center gap-[7px] rounded-[9px] border border-[#ff536a66] bg-[#061a2be6] px-[10px] py-[5px] shadow-[inset_0_0_20px_#20d8ff0b,0_0_18px_#20d8ff0d]',
    closed:
      'node closed box-border grid h-[60px] min-w-0 grid-cols-[30px_1fr_8px] items-center gap-[7px] rounded-[9px] border border-[#58758a66] bg-[#061a2be6] px-[10px] py-[5px] shadow-[inset_0_0_20px_#20d8ff0b,0_0_18px_#20d8ff0d]',
    unknown:
      'node unknown box-border grid h-[60px] min-w-0 grid-cols-[30px_1fr_8px] items-center gap-[7px] rounded-[9px] border border-[#4d789066] bg-[#061a2be6] px-[10px] py-[5px] shadow-[inset_0_0_20px_#20d8ff0b,0_0_18px_#20d8ff0d]',
    no_sample:
      'node no_sample box-border grid h-[60px] min-w-0 grid-cols-[30px_1fr_8px] items-center gap-[7px] rounded-[9px] border border-[#4d789066] bg-[#061a2be6] px-[10px] py-[5px] shadow-[inset_0_0_20px_#20d8ff0b,0_0_18px_#20d8ff0d]',
  } satisfies Record<TimelineSeverity, string>;

  const stateClass = {
    live: liveStateClass,
    auction: liveStateClass,
    warn: 'state truncate text-[13px] leading-[1.2] font-[850] text-[color:var(--relay-warn)]',
    bad: 'state truncate text-[13px] leading-[1.2] font-[850] text-[color:var(--relay-bad)]',
    closed: 'state truncate text-[13px] leading-[1.2] font-[850] text-[color:var(--relay-closed)]',
    unknown: 'state truncate text-[13px] leading-[1.2] font-[850] text-[color:var(--relay-muted)]',
    no_sample: 'state truncate text-[13px] leading-[1.2] font-[850] text-[color:var(--relay-muted)]',
  } satisfies Record<TimelineSeverity, string>;

  const severityDotClass = {
    live: liveDotClass,
    auction: liveDotClass,
    warn: 'status-dot warn rounded-full bg-[color:var(--relay-warn)] shadow-[0_0_10px_color-mix(in_srgb,var(--relay-warn)_70%,transparent)]',
    bad: 'status-dot bad rounded-full bg-[color:var(--relay-bad)] shadow-[0_0_10px_color-mix(in_srgb,var(--relay-bad)_70%,transparent)]',
    closed: 'status-dot closed rounded-full bg-[color:var(--relay-closed)] shadow-none',
    unknown: 'status-dot unknown rounded-full bg-[color:var(--relay-muted)] shadow-none',
    no_sample: 'status-dot no_sample rounded-full bg-[color:var(--relay-muted)] shadow-none',
  } satisfies Record<TimelineSeverity, string>;

  function sourceState(model: IntegrityModel): string {
    return {
      connecting: '连接中',
      subscribing: '订阅中',
      backfilling: '补历史',
      live: '在线',
      degraded: '降级',
      down: '断开',
    }[model.metrics.upstream_stage];
  }

  function sourceSeverity(model: IntegrityModel): TimelineSeverity {
    if (model.metrics.upstream_stage === 'down' || model.metrics.upstream_stage === 'degraded') return 'bad';
    if (model.metrics.upstream_stage === 'live') return 'live';
    return 'no_sample';
  }

  function universeMeta(model: IntegrityModel): string {
    if (model.cacheHealth === 'subscribing') return '等待订阅';
    if (model.cacheHealth === 'backfilling') {
      const initializing = Number(model.global.initializing || 0);
      return initializing > 0 ? `初始化 ${formatNumber(initializing)}` : '等待样本';
    }
    return `覆盖 ${formatNumber(model.observedUniverse)}`;
  }

  function universeSeverity(model: IntegrityModel): TimelineSeverity {
    if (model.cacheHealth === 'subscribing' || model.cacheHealth === 'backfilling') return 'no_sample';
    if (model.coverageRatio >= 0.98) return 'live';
    if (model.coverageRatio >= 0.9) return 'warn';
    return 'bad';
  }

  function cacheState(model: IntegrityModel): string {
    return {
      active: '活跃',
      attention: '需关注',
      interrupted: '链路异常',
      subscribing: '订阅中',
      backfilling: '补历史',
      closed: '休盘中',
    }[model.cacheHealth];
  }

  function cacheMeta(model: IntegrityModel): string {
    if (model.cacheHealth === 'closed') return '帧 -- / 事件 --';
    return `帧 ${formatDuration(model.upstreamIdleMs)} / 事件 ${formatDuration(model.eventIdleMs)}`;
  }

  function cacheSeverity(model: IntegrityModel): TimelineSeverity {
    return model.cacheSeverity;
  }

  let nodes = $derived<Node[]>([
    {
      name: '上游连接',
      icon: '☁',
      state: sourceState(model),
      meta: `frame ${formatNumber(model.metrics.upstream_frames_received)}`,
      severity: sourceSeverity(model),
    },
    {
      name: '合约集合',
      icon: '▦',
      state: `${formatNumber(model.totalUniverse)} 合约`,
      meta: universeMeta(model),
      severity: universeSeverity(model),
    },
    {
      name: '数据解码',
      icon: '⌘',
      state: model.decodeHealth === 'degraded' ? '解析诊断' : '正常',
      meta: `${formatNumber(model.metrics.recent_invalid_rows_1m)} 近期 / ${formatNumber(model.invalidRowCount)} 累计`,
      severity: model.decodeHealth === 'degraded' ? 'warn' : 'live',
    },
    {
      name: '行情缓存',
      icon: '◫',
      state: cacheState(model),
      meta: cacheMeta(model),
      severity: cacheSeverity(model),
    },
    {
      name: '下游服务',
      icon: '▤',
      state: model.subscribedProblemCount > 0 ? '影响订阅' : '正常',
      meta: `${formatNumber(model.metrics.downstream_clients)} 客户端`,
      severity: model.subscribedProblemCount > 0 ? 'bad' : 'live',
    },
  ]);
</script>

<section
  class="panel-shell grid min-h-[68px] items-center gap-1 px-[3%] py-1.5 [grid-template-columns:1fr_28px_1fr_28px_1fr_28px_1fr_28px_1fr]"
  data-testid="relay-pipeline"
>
  {#each nodes as node, index}
    <div class={nodeClass[node.severity]}>
      <div
        class="grid size-7 place-items-center rounded-full border border-[#2ad0ffaa] text-[13px] text-[#86ebff]"
      >
        {node.icon}
      </div>
      <div class="grid min-w-0 gap-[3px]">
        <div class="truncate text-[10px] leading-[1.1] text-[#c6dbe5]">{node.name}</div>
        <div class={stateClass[node.severity]}>{node.state}</div>
        <div class="truncate text-[9px] leading-[1.1] text-[color:color-mix(in_srgb,var(--relay-muted)_78%,transparent)]">
          {node.meta}
        </div>
      </div>
      <span aria-hidden="true" class={`size-2 ${severityDotClass[node.severity]}`}></span>
    </div>
    {#if index < nodes.length - 1}
      <div class="arrow"></div>
    {/if}
  {/each}
</section>

<style>
  .arrow {
    position: relative;
    height: 2px;
    background: linear-gradient(90deg, var(--relay-line-soft), var(--relay-info));
    box-shadow: 0 0 9px var(--relay-info);
  }

  .arrow::after {
    content: "";
    position: absolute;
    top: -3px;
    right: -1px;
    width: 7px;
    height: 7px;
    border-top: 2px solid var(--relay-info);
    border-right: 2px solid var(--relay-info);
    transform: rotate(45deg);
  }
</style>
