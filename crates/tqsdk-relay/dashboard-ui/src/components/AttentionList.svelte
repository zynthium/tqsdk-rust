<script lang="ts">
  import { formatDuration } from '../lib/format';
  import { statusLabel } from '../lib/integrity-model';
  import type { ProblemSeverity, SymbolRow } from '../lib/types';

  let { rows }: { rows: SymbolRow[] } = $props();
  const itemToneClass: Record<ProblemSeverity, string> = {
    live: 'border-[#45ff9a55] text-[#9cffc5]',
    warn: 'border-[#ffc44780] text-[#ffe08c] [box-shadow:inset_3px_0_#ffc447]',
    bad: 'border-[#ff536a80] text-[#ffadb8] [box-shadow:inset_3px_0_#ff536a]',
    closed: 'border-[#2a93c55e] text-[#91dcff]',
    initializing: 'border-[#4d789066] text-[#b8c8d3]',
  };
  let ordered = $derived(
    [...rows]
      .sort((left, right) => {
        const leftRank = (left.subscribed ? 0 : 10) + (left.problem_severity === 'bad' ? 0 : 1);
        const rightRank = (right.subscribed ? 0 : 10) + (right.problem_severity === 'bad' ? 0 : 1);
        return leftRank - rightRank || (right.receive_gap_ms ?? -1) - (left.receive_gap_ms ?? -1);
      })
      .slice(0, 24),
  );
</script>

<aside class="panel panel-shell flex min-h-0 flex-col overflow-hidden px-3 py-2.5" data-testid="attention-list">
  <div class="head flex items-center justify-between gap-1.5">
    <div class="panel-title">当前关注 · 问题合约</div>
    {#if rows.length > 0}
      <span class="count-chip bg-[#ff536a22] text-[color:var(--relay-bad)]">{rows.length}</span>
    {/if}
  </div>
  <div class="list-panel-list relative z-[1] mt-2 grid min-h-0 flex-1 content-start gap-[5px] overflow-y-auto pr-1">
    {#if ordered.length === 0}
      <div class="list-panel-empty flex flex-1 items-center justify-center px-1 text-center text-[11px] text-[color:var(--relay-muted)]">
        当前无活动异常
      </div>
    {:else}
      {#each ordered as row}
        <article
          class={`rounded-[7px] border bg-[#071929] px-[9px] py-[7px] text-[11px] leading-[1.35] ${itemToneClass[row.problem_severity]}`}
        >
          <div class="flex items-center justify-between gap-1">
            <span class="truncate text-[12px] font-extrabold text-inherit" title={row.symbol}>
              {row.instrument_name ?? row.symbol}
            </span>
            <span class="shrink-0 text-[9px] font-bold text-[#6e94a8]">{row.subscribed ? '订阅中' : ''}</span>
          </div>
          <div class="mt-0.5 text-[11px] text-inherit opacity-82">
            {statusLabel(row.status)} · {formatDuration(row.receive_gap_ms)}
          </div>
        </article>
      {/each}
    {/if}
  </div>
</aside>
