<script lang="ts">
  import { EXCHANGES, exchangeOf } from '../lib/timeline';
  import type { SymbolRow, TimelineSample, TimelineSeverity } from '../lib/types';

  let { buckets, rows }: { buckets: Array<TimelineSample | null>; rows: SymbolRow[] } = $props();
  let exchangeRows = $derived(
    EXCHANGES.filter((exchange) => rows.some((row) => exchangeOf(row.symbol) === exchange)).slice(0, 4),
  );
  let definitions = $derived([
    { label: '全局', severity: (sample: TimelineSample) => sample.globalSeverity },
    { label: '订阅', severity: (sample: TimelineSample) => sample.subscribedSeverity },
    ...exchangeRows.map((exchange) => ({
      label: exchange,
      severity: (sample: TimelineSample) => sample.exchangeSeverity[exchange] ?? 'closed',
    })),
  ]);

  function cellClass(sample: TimelineSample | null, accessor: (sample: TimelineSample) => TimelineSeverity) {
    return sample ? accessor(sample) : 'closed';
  }
</script>

<section class="panel timeline-panel" data-testid="continuity-timeline">
  <div class="head">
    <div class="panel-title">最近 5 分钟连续性</div>
    <div class="legend">
      <span><i class="live"></i>正常</span>
      <span><i class="warn"></i>静默</span>
      <span><i class="bad"></i>异常</span>
      <span><i class="closed"></i>休盘</span>
    </div>
  </div>
  <div class="timeline" style={`--bucket-count:${buckets.length}`}>
    {#each definitions as definition}
      <div class="row-label">{definition.label}</div>
      {#each buckets as bucket}
        <span class={`cell ${cellClass(bucket, definition.severity)}`}></span>
      {/each}
    {/each}
    <div class="axis"><span>-5m</span><span>now</span></div>
  </div>
</section>

<style>
  .timeline-panel {
    padding: 10px 12px;
  }

  .head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
  }

  .legend {
    display: flex;
    gap: 10px;
    color: var(--relay-muted);
    font-size: 10px;
  }

  .legend span {
    display: inline-flex;
    align-items: center;
    gap: 5px;
  }

  .legend i {
    width: 12px;
    height: 7px;
    border-radius: 2px;
  }

  .timeline {
    position: relative;
    z-index: 1;
    margin-top: 12px;
    display: grid;
    grid-template-columns: 72px repeat(var(--bucket-count), minmax(3px, 1fr));
    grid-auto-rows: 27px;
    align-items: center;
    column-gap: 2px;
  }

  .row-label {
    color: #c3dbe6;
    font-size: 11px;
  }

  .cell {
    height: 9px;
    border-radius: 1px;
    background: #1b3343;
  }

  .live {
    background: linear-gradient(180deg, #52ffae, #23d786);
    box-shadow: 0 0 5px #45ff9a44;
  }

  .warn {
    background: var(--relay-warn);
    box-shadow: 0 0 8px #ffc44780;
  }

  .bad {
    background: var(--relay-bad);
    box-shadow: 0 0 8px #ff536a8c;
  }

  .closed {
    background: #354d60;
  }

  .axis {
    grid-column: 2 / -1;
    display: flex;
    justify-content: space-between;
    padding-top: 2px;
    color: #66889a;
    font-size: 9px;
  }
</style>
