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
      .slice(0, 24),
  );
</script>

<aside class="panel attention" data-testid="attention-list">
  <div class="head">
    <div class="panel-title">当前关注 · 问题合约</div>
    {#if rows.length > 0}
      <span class="count">{rows.length}</span>
    {/if}
  </div>
  <div class="list">
    {#if ordered.length === 0}
      <div class="empty">当前无活动异常</div>
    {:else}
      {#each ordered as row}
        <article class={`item ${row.problem_severity}`}>
          <div class="item-top">
            <span class="symbol" title={row.symbol}>{row.instrument_name ?? row.symbol}</span>
            <span class="sub-tag">{row.subscribed ? '订阅中' : ''}</span>
          </div>
          <div class="desc">
            {statusLabel(row.status)} · {formatDuration(row.receive_gap_ms)}
          </div>
        </article>
      {/each}
    {/if}
  </div>
</aside>

<style>
  .attention {
    padding: 10px 12px;
  }

  .head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 6px;
  }

  .count {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    min-width: 22px;
    height: 18px;
    border-radius: 9px;
    padding: 0 6px;
    background: #ff536a22;
    color: var(--relay-bad);
    font-size: 11px;
    font-weight: 850;
  }

  .list {
    position: relative;
    z-index: 1;
    margin-top: 8px;
    display: grid;
    gap: 5px;
  }

  .item {
    position: relative;
    border: 1px solid #2a93c55e;
    border-radius: 7px;
    padding: 7px 9px;
    background: #071929;
    color: #d5eaf3;
    font-size: 11px;
    line-height: 1.35;
  }

  .item.warn {
    border-color: #ffc44780;
    box-shadow: inset 3px 0 #ffc447;
    color: #ffe08c;
  }

  .item.bad {
    border-color: #ff536a80;
    box-shadow: inset 3px 0 #ff536a;
    color: #ffadb8;
  }

  .item.live {
    border-color: #45ff9a55;
    color: #9cffc5;
  }

  .item.closed {
    color: #91dcff;
  }

  .item-top {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 4px;
  }

  .symbol {
    overflow: hidden;
    color: inherit;
    font-size: 12px;
    font-weight: 850;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .sub-tag {
    flex-shrink: 0;
    color: #6e94a8;
    font-size: 9px;
    font-weight: 700;
  }

  .desc {
    margin-top: 2px;
    color: inherit;
    opacity: 0.82;
    font-size: 11px;
  }

  .empty {
    padding: 22px 4px;
    color: inherit;
    opacity: 0.82;
    font-size: 11px;
    text-align: center;
  }
</style>
