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

  let nodes = $derived<Node[]>([
    {
      name: '上游连接',
      icon: '☁',
      state: model.metrics.upstream_stage,
      meta: `frame ${formatNumber(model.metrics.upstream_frames_received)}`,
      severity: model.metrics.upstream_stage === 'live' ? 'live' : model.metrics.upstream_stage === 'down' || model.metrics.upstream_stage === 'degraded' ? 'bad' : 'warn',
    },
    {
      name: '合约集合',
      icon: '▦',
      state: `${formatNumber(model.totalUniverse)} 合约`,
      meta: `覆盖 ${formatNumber(model.observedUniverse)}`,
      severity: model.coverageRatio >= 0.98 ? 'live' : model.coverageRatio >= 0.9 ? 'warn' : 'bad',
    },
    {
      name: '数据解码',
      icon: '⌘',
      state: model.invalidRowCount > 0 ? '坏行' : '正常',
      meta: `${formatNumber(model.invalidRowCount)} 行`,
      severity: model.invalidRowCount > 0 ? 'bad' : 'live',
    },
    {
      name: '行情缓存',
      icon: '◫',
      state: model.problems.length > 0 ? '需关注' : '活跃',
      meta: `静默 ${formatDuration(model.upstreamIdleMs)}`,
      severity: model.issueCount > 0 ? 'warn' : 'live',
    },
    {
      name: '下游服务',
      icon: '▤',
      state: model.subscribedProblems.length > 0 ? '影响订阅' : '正常',
      meta: `${formatNumber(model.metrics.downstream_clients)} 客户端`,
      severity: model.subscribedProblems.length > 0 ? 'bad' : 'live',
    },
  ]);
</script>

<section class="panel pipeline" data-testid="relay-pipeline">
  {#each nodes as node, index}
    <div class="node">
      <div class="node-icon">{node.icon}</div>
      <div class="node-copy">
        <div class="name">{node.name}</div>
        <div class={`state ${node.severity}`}>{node.state}</div>
        <div class="meta">{node.meta}</div>
      </div>
      <span class={`status-dot ${node.severity}`}></span>
    </div>
    {#if index < nodes.length - 1}
      <div class="arrow"></div>
    {/if}
  {/each}
</section>

<style>
  .pipeline {
    min-height: 72px;
    display: grid;
    grid-template-columns: repeat(9, minmax(0, 1fr));
    align-items: center;
    gap: 6px;
    padding: 6px 5%;
  }

  .node {
    box-sizing: border-box;
    height: 64px;
    min-width: 0;
    display: grid;
    grid-template-columns: 34px 1fr 8px;
    align-items: center;
    gap: 9px;
    border: 1px solid var(--relay-line-soft);
    border-radius: 10px;
    padding: 6px 12px;
    background: #061a2be6;
    box-shadow:
      inset 0 0 20px #20d8ff0b,
      0 0 18px #20d8ff0d;
  }

  .node-icon {
    width: 32px;
    height: 32px;
    display: grid;
    place-items: center;
    border: 1px solid #2ad0ffaa;
    border-radius: 50%;
    color: #86ebff;
    font-size: 15px;
  }

  .node-copy {
    min-width: 0;
    display: grid;
    gap: 2px;
  }

  .name {
    overflow: hidden;
    color: #c6dbe5;
    font-size: 12px;
    line-height: 1.05;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .state {
    overflow: hidden;
    color: var(--relay-live);
    font-size: 14px;
    font-weight: 850;
    line-height: 1.12;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .state.warn {
    color: var(--relay-warn);
  }

  .state.bad {
    color: var(--relay-bad);
  }

  .meta {
    overflow: hidden;
    color: color-mix(in srgb, var(--relay-muted) 78%, transparent);
    font-size: 10px;
    line-height: 1.05;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

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
