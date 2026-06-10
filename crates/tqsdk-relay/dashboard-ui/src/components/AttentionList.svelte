<script lang="ts">
  import { formatDuration } from '../lib/format';
  import { statusLabel } from '../lib/integrity-model';
  import type { SymbolRow } from '../lib/types';

  let { rows }: { rows: SymbolRow[] } = $props();
  let ordered = $derived(
    [...rows]
      .sort((left, right) => {
        const leftRank = (left.subscribed ? 0 : 10) + (left.problem_severity === 'bad' ? 0 : 1);
        const rightRank = (right.subscribed ? 0 : 10) + (right.problem_severity === 'bad' ? 0 : 1);
        return leftRank - rightRank || (right.receive_gap_ms ?? -1) - (left.receive_gap_ms ?? -1);
      })
      .slice(0, 8),
  );
</script>

<aside class="panel attention" data-testid="attention-list">
  <div class="panel-title">当前关注 · 问题合约</div>
  <div class="list">
    {#if ordered.length === 0}
      <div class="empty">当前无活动异常</div>
    {:else}
      {#each ordered as row}
        <article class={`item ${row.problem_severity}`}>
          <div class="symbol">{row.symbol}</div>
          <div class="desc">
            {row.instrument_name ?? '未命名'} · {statusLabel(row.status)} · {formatDuration(row.receive_gap_ms)}
          </div>
          <div class="foot">{row.subscribed ? '下游正在使用' : '未订阅'}</div>
        </article>
      {/each}
    {/if}
  </div>
</aside>

<style>
  .attention {
    padding: 10px 12px;
  }

  .list {
    margin-top: 9px;
    display: grid;
    gap: 7px;
  }

  .item {
    border: 1px solid var(--relay-line-soft);
    border-left: 3px solid var(--relay-warn);
    border-radius: 7px;
    padding: 8px 9px;
    background: rgb(255 255 255 / 3%);
  }

  .item.bad {
    border-left-color: var(--relay-bad);
  }

  .symbol {
    color: var(--relay-text);
    font-size: 12px;
    font-weight: 850;
  }

  .desc,
  .foot,
  .empty {
    color: var(--relay-muted);
    font-size: 11px;
  }

  .desc {
    margin-top: 3px;
  }

  .foot {
    margin-top: 4px;
    text-align: right;
  }

  .empty {
    padding: 26px 4px;
    text-align: center;
  }
</style>
