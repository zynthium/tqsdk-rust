<script lang="ts">
  import { formatTime } from '../lib/format';
  import type { LocalIncident, TimelineSeverity } from '../lib/types';

  let { incidents }: { incidents: LocalIncident[] } = $props();
  const itemToneClass: Record<TimelineSeverity, string> = {
    live: 'border-[#45ff9a44]',
    closed: 'border-[#58758a44]',
    warn: 'border-[#ffc44744]',
    bad: 'border-[#ff536a44]',
    unknown: 'border-[#4d789044]',
    no_sample: 'border-[#4d789044]',
  };
  const severityBadgeClass: Record<TimelineSeverity, string> = {
    live: 'border-[#45ff9a55] bg-[#45ff9a0d] text-[color:var(--relay-live)]',
    closed: 'border-[#58758a66] bg-[#58758a14] text-[#9eb9ce]',
    warn: 'border-[#ffc44766] bg-[#ffc44712] text-[color:var(--relay-warn)]',
    bad: 'border-[#ff536a66] bg-[#ff536a12] text-[color:var(--relay-bad)]',
    unknown: 'border-[#4d789066] bg-[#4d78900f] text-[color:var(--relay-muted)]',
    no_sample: 'border-[#4d789066] bg-[#4d78900f] text-[color:var(--relay-muted)]',
  };
</script>

<section class="panel panel-shell flex min-h-0 flex-col overflow-hidden px-3 py-2.5" data-testid="incident-table">
  <div class="head flex items-center justify-between gap-1.5">
    <div class="panel-title">状态变化事件</div>
    {#if incidents.length > 0}
      <span class="count-chip bg-[#42a7ff22] text-[color:var(--relay-blue)]">{incidents.length}</span>
    {/if}
  </div>
  {#if incidents.length === 0}
    <div class="list-panel-empty flex flex-1 items-center justify-center px-2 text-center text-[11px] text-[color:var(--relay-muted)]">
      本页尚未观测到状态变化
    </div>
  {:else}
    <div class="list-panel-list relative z-[1] mt-2 grid min-h-0 flex-1 content-start gap-[5px] overflow-y-auto pr-1">
      {#each incidents.slice(0, 12) as incident}
        <div class={`rounded-[6px] border bg-[#071929] px-2 py-1.5 text-[10px] leading-[1.4] ${itemToneClass[incident.severity]}`}>
          <div class="flex items-center gap-1.5">
            <span class="whitespace-nowrap text-[10px] text-[color:var(--relay-muted)]">{formatTime(incident.at)}</span>
            <span class={`severity-badge ${severityBadgeClass[incident.severity]}`}>{incident.type}</span>
            <span class="ml-auto shrink-0 whitespace-nowrap text-[9px] text-[#6e94a8]">{incident.impact}</span>
          </div>
          <div class="mt-0.5 truncate font-bold text-[#c6dbe5]" title={incident.scope_symbol}>{incident.scope}</div>
          <div class="truncate text-[color:var(--relay-muted)]" title={incident.detail}>{incident.detail}</div>
        </div>
      {/each}
    </div>
  {/if}
</section>
