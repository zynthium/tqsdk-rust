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
    position: relative;
    z-index: 1;
    margin-top: 9px;
    display: grid;
    gap: 7px;
  }

  .item {
    position: relative;
    border: 1px solid #2a93c55e;
    border-radius: 8px;
    padding: 10px 9px 10px 34px;
    background: #071929;
    color: #d5eaf3;
    font-size: 11px;
    line-height: 1.45;
  }

  .item::before {
    content: "i";
    position: absolute;
    top: 50%;
    left: 9px;
    width: 17px;
    height: 17px;
    display: grid;
    place-items: center;
    transform: translateY(-50%);
    border: 1px solid currentColor;
    border-radius: 50%;
    font-weight: 900;
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

  .symbol {
    color: inherit;
    font-size: 12px;
    font-weight: 850;
  }

  .desc,
  .foot,
  .empty {
    color: inherit;
    opacity: 0.82;
    font-size: 11px;
  }

  .desc {
    margin-top: 3px;
  }

  .foot {
    margin-top: 4px;
    color: #6e94a8;
    text-align: right;
  }

  .empty {
    padding: 26px 4px;
    text-align: center;
  }
</style>
