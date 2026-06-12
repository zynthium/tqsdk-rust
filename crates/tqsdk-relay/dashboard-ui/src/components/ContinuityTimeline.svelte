<script lang="ts">
  import { EXCHANGES, exchangeOf } from '../lib/timeline';
  import { formatDuration, formatNumber } from '../lib/format';
  import { statusLabel } from '../lib/integrity-model';
  import type { SymbolRow, TimelineSample, TimelineSeverity } from '../lib/types';

  let { buckets, rows }: {
    buckets: Array<TimelineSample | null>;
    rows: SymbolRow[];
  } = $props();
  let expandedExchanges = $state<string[]>([]);
  let viewMode = $state<'blocks' | 'sparkline'>('blocks');

  let hoveredSymbol = $state<string | null>(null);
  let hoveredSymbolRow = $derived(rows.find((row) => row.symbol === hoveredSymbol) ?? null);
  let tooltipX = $state(0);
  let tooltipY = $state(0);

  function handleHover(definition: TimelineDefinition, e: MouseEvent) {
    if (definition.kind === 'symbol') {
      hoveredSymbol = definition.symbol;
      tooltipX = e.clientX + 15;
      tooltipY = Math.min(e.clientY + 15, window.innerHeight - 160);
    }
  }

  function clearHover() {
    hoveredSymbol = null;
  }

  type TimelineDefinition =
    | {
        kind: 'summary';
        key: string;
        label: string;
        emptySeverity?: TimelineSeverity;
        severity: (sample: TimelineSample) => TimelineSeverity;
        latency: (sample: TimelineSample) => number;
      }
    | {
        kind: 'exchange';
        key: string;
        label: string;
        exchange: string;
        issueCount: number;
        totalCount: number;
        expanded: boolean;
        emptySeverity?: TimelineSeverity;
        severity: (sample: TimelineSample) => TimelineSeverity;
        latency: (sample: TimelineSample) => number;
      }
    | {
        kind: 'symbol';
        key: string;
        label: string;
        detail: string;
        symbol: string;
        row: SymbolRow;
        emptySeverity?: TimelineSeverity;
        severity: (sample: TimelineSample) => TimelineSeverity;
        latency: (sample: TimelineSample) => number;
      };

  let exchangeRows = $derived(EXCHANGES.filter((exchange) => rows.some((row) => exchangeOf(row.symbol) === exchange)));
  let definitions = $derived<TimelineDefinition[]>([
    { kind: 'summary', key: 'global', label: '全局', severity: (sample: TimelineSample) => sample.globalSeverity, latency: (sample: TimelineSample) => sample.globalLatency },
    { kind: 'summary', key: 'subscribed', label: '订阅', severity: (sample: TimelineSample) => sample.subscribedSeverity, latency: (sample: TimelineSample) => sample.subscribedLatency },
    ...exchangeRows.flatMap((exchange) => {
      const exchangeSymbols = rows.filter((row) => exchangeOf(row.symbol) === exchange);
      const issueCount = exchangeSymbols.filter((row) => row.problem_severity === 'bad' || row.problem_severity === 'warn').length;
      const expanded = expandedExchanges.includes(exchange);
      const exchangeDefinition: TimelineDefinition = {
        kind: 'exchange',
        key: `exchange:${exchange}`,
        label: exchange,
        exchange,
        issueCount,
        totalCount: exchangeSymbols.length,
        expanded,
        emptySeverity: exchangeSymbols.every((row) => row.session === 'closed') ? 'closed' : 'no_sample',
        severity: (sample: TimelineSample) => sample.exchangeSeverity[exchange] ?? 'closed',
        latency: (sample: TimelineSample) => sample.exchangeLatency[exchange] ?? 0,
      };
      if (!expanded) return [exchangeDefinition];
      return [
        exchangeDefinition,
        ...orderedSymbolRows(exchangeSymbols).map((row): TimelineDefinition => ({
          kind: 'symbol',
          key: `symbol:${row.symbol}`,
          label: row.instrument_name ?? row.symbol,
          detail: row.subscribed ? '订阅' : '',
          symbol: row.symbol,
          row,
          emptySeverity: row.session === 'closed' ? 'closed' : 'no_sample',
          severity: (sample: TimelineSample) => sample.symbolSeverity[row.symbol] ?? 'closed',
          latency: (sample: TimelineSample) => sample.symbolLatency[row.symbol] ?? 0,
        })),
      ];
    }),
  ]);

  function cellClass(definition: TimelineDefinition, sample: TimelineSample | null) {
    const severity = sample ? definition.severity(sample) : (definition.emptySeverity ?? 'no_sample');
    return severity === 'closed' ? 'unknown' : severity;
  }

  function sparklinePath(definition: TimelineDefinition, buckets: Array<TimelineSample | null>): string {
    const coords: string[] = [];
    buckets.forEach((bucket, i) => {
      const x = i * 10;
      let y = 100;
      if (bucket) {
        const severity = definition.severity(bucket);
        if (severity === 'bad') y = 10;
        else if (severity === 'warn') y = 40;
        else {
          const lat = definition.latency(bucket);
          y = Math.max(70, 100 - (lat / 1000) * 30);
        }
      } else {
        const emptySev = definition.emptySeverity ?? 'no_sample';
        if (emptySev === 'closed') y = 100;
      }
      coords.push(`${x},${y}`);
    });
    return coords.length ? `M ${coords.join(' L ')}` : '';
  }

  function sparklineColor(definition: TimelineDefinition, buckets: Array<TimelineSample | null>): string {
    let lastActive = buckets[buckets.length - 1];
    for (let i = buckets.length - 1; i >= 0; i--) {
      if (buckets[i]) { lastActive = buckets[i]; break; }
    }
    const severity = lastActive ? definition.severity(lastActive) : (definition.emptySeverity ?? 'no_sample');
    if (severity === 'bad') return 'var(--relay-bad)';
    if (severity === 'warn') return 'var(--relay-warn)';
    if (severity === 'closed') return '#566170'; // same as unknown
    return 'var(--relay-live)';
  }

  function orderedSymbolRows(exchangeSymbols: SymbolRow[]): SymbolRow[] {
    return [...exchangeSymbols]
      .sort((left, right) => severityRank(left) - severityRank(right) || (right.receive_gap_ms ?? -1) - (left.receive_gap_ms ?? -1))
      .slice(0, 30);
  }

  function severityRank(row: SymbolRow): number {
    if (row.problem_severity === 'bad') return 0;
    if (row.problem_severity === 'warn') return 1;
    if (row.subscribed) return 2;
    if (row.problem_severity === 'closed') return 4;
    return 3;
  }

  function subscriberCount(row: SymbolRow): number {
    return row.quote_subscriber_count + row.chart_subscriber_count;
  }

  function toggleExchange(exchange: string) {
    expandedExchanges = expandedExchanges.includes(exchange)
      ? expandedExchanges.filter((item) => item !== exchange)
      : [...expandedExchanges, exchange];
  }
</script>

<section class="panel timeline-panel" data-testid="continuity-timeline">
  <div class="head">
    <div class="panel-title">最近 5 分钟连续性</div>
    <div class="view-toggle">
      <button class:active={viewMode === 'blocks'} onclick={() => (viewMode = 'blocks')}>Blocks</button>
      <button class:active={viewMode === 'sparkline'} onclick={() => (viewMode = 'sparkline')}>Sparkline</button>
    </div>
    <div class="legend">
      <span><i class="live"></i>正常</span>
      <span><i class="warn"></i>静默</span>
      <span><i class="bad"></i>异常</span>
      <span><i class="unknown"></i>休盘</span>
      <span><i class="no_sample"></i>无样本</span>
    </div>
  </div>
  <div class="timeline" style={`--bucket-count:${buckets.length}`}>
    {#each definitions as definition}
      {#if definition.kind === 'exchange'}
        <button
          type="button"
          class="row-label exchange-row"
          aria-expanded={definition.expanded}
          aria-label={`${definition.label} ${definition.issueCount}/${definition.totalCount} 异常`}
          onclick={() => toggleExchange(definition.exchange)}
        >
          <span class="caret">{definition.expanded ? '−' : '+'}</span>
          <span>{definition.label}</span>
          <em>{definition.issueCount}/{definition.totalCount}</em>
        </button>
      {:else if definition.kind === 'symbol'}
        <div
          data-testid="timeline-symbol-row"
          class="row-label symbol-row"
          title={definition.symbol}
          aria-label={`${definition.label} ${statusLabel(definition.row.status)} ${formatDuration(definition.row.receive_gap_ms)} ${definition.row.problem_severity}`}
        >
          <span class="symbol-name">{definition.label}</span>
          <span class={`badge ${definition.row.status}`}>{statusLabel(definition.row.status)}</span>
          <em>Tick {formatNumber(definition.row.ticks_ingested)}</em>
          <em>订阅 {formatNumber(subscriberCount(definition.row))}</em>
          <span class={`risk ${definition.row.problem_severity}`}>{definition.row.problem_severity}</span>
        </div>
      {:else}
        <div class="row-label">{definition.label}</div>
      {/if}
      {#if viewMode === 'blocks'}
        {#each buckets as bucket}
          <!-- svelte-ignore a11y_no_static_element_interactions -->
          <span class={`cell ${cellClass(definition, bucket)}`} onmousemove={(e) => handleHover(definition, e)} onmouseleave={clearHover}></span>
        {/each}
      {:else}
        <!-- svelte-ignore a11y_no_static_element_interactions -->
        <div class="sparkline-container" style="grid-column: 2 / -1;" onmousemove={(e) => handleHover(definition, e)} onmouseleave={clearHover}>
          <svg viewBox={`0 0 ${buckets.length * 10} 100`} preserveAspectRatio="none">
            <path d={sparklinePath(definition, buckets)} stroke={sparklineColor(definition, buckets)} stroke-width="2" fill="none" vector-effect="non-scaling-stroke"/>
          </svg>
        </div>
      {/if}
    {/each}
    <div class="axis"><span>-5m</span><span>now</span></div>
  </div>
  {#if hoveredSymbolRow}
    <div class="health-tooltip" style={`left: ${tooltipX}px; top: ${tooltipY}px;`}>
      <div class="tooltip-title">{hoveredSymbolRow.instrument_name ?? hoveredSymbolRow.symbol}</div>
      <div class="tooltip-body">
        <div><span>接收延迟</span><b>{formatDuration(hoveredSymbolRow.receive_gap_ms)}</b></div>
        <div><span>行情延时</span><b>{formatDuration(hoveredSymbolRow.market_time_lag_ms)}</b></div>

        <div><span>异常记录数</span><b>{formatNumber(hoveredSymbolRow.invalid_rows)}</b></div>
        {#if hoveredSymbolRow.last_invalid_row_error}
          <div class="error" title={hoveredSymbolRow.last_invalid_row_error}>{hoveredSymbolRow.last_invalid_row_error}</div>
        {/if}
      </div>
    </div>
  {/if}
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

  .view-toggle {
    display: flex;
    gap: 4px;
    background: #0f2130;
    padding: 3px;
    border-radius: 6px;
    border: 1px solid var(--relay-line-soft);
  }

  .view-toggle button {
    background: transparent;
    border: none;
    color: var(--relay-muted);
    font-size: 10px;
    padding: 3px 8px;
    border-radius: 4px;
    cursor: pointer;
    font-weight: 600;
    transition: all 0.2s;
  }

  .view-toggle button:hover {
    color: var(--relay-text);
  }

  .view-toggle button.active {
    background: #1d3648;
    color: var(--relay-info);
    box-shadow: 0 0 5px rgba(50, 163, 230, 0.2);
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

  .sparkline-container {
    display: flex;
    align-items: center;
    height: 100%;
    width: 100%;
  }

  .sparkline-container svg {
    height: 20px;
    width: 100%;
    filter: drop-shadow(0 0 3px currentColor);
    transition: filter 0.15s ease;
  }

  .sparkline-container:hover svg {
    filter: drop-shadow(0 0 6px currentColor) brightness(1.4) saturate(1.5);
  }

  .timeline {
    --timeline-label-width: clamp(320px, 22vw, 380px);

    position: relative;
    z-index: 1;
    margin-top: 12px;
    display: grid;
    grid-template-columns: var(--timeline-label-width) repeat(var(--bucket-count), minmax(3px, 1fr));
    grid-auto-rows: 27px;
    align-items: center;
    column-gap: 2px;
    flex: 1;
    min-height: 0;
    overflow-y: auto;
    padding-right: 4px;
  }

  .row-label {
    min-width: 0;
    overflow: hidden;
    border: 0;
    padding: 0;
    background: transparent;
    color: #c3dbe6;
    font-size: 11px;
    text-align: left;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  button.row-label {
    cursor: pointer;
    font: inherit;
  }

  button.row-label:hover {
    color: var(--relay-info);
  }

  .exchange-row,
  .symbol-row {
    display: grid;
    align-items: center;
    gap: 4px;
  }

  .exchange-row {
    grid-template-columns: auto 1fr auto;
  }

  .symbol-row {
    grid-template-columns: minmax(72px, 1fr) 46px 80px 56px 50px;
    padding-left: 12px;
    color: #9fc4d5;
  }

  .caret {
    width: 12px;
    color: var(--relay-info);
    font-weight: 900;
  }

  .row-label em {
    overflow: hidden;
    color: var(--relay-muted);
    font-size: 11px;
    font-style: normal;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .exchange-row span:nth-child(2),
  .symbol-name {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .symbol-row.selected {
    color: var(--relay-info);
  }

  .badge {
    justify-self: start;
    border: 1px solid var(--relay-line-soft);
    border-radius: 4px;
    padding: 1px 5px;
    color: var(--relay-live);
    font-size: 11px;
    line-height: 1.2;
  }

  .badge.closed {
    color: var(--relay-closed);
  }

  .badge.stale,
  .badge.missing {
    color: var(--relay-warn);
  }

  .badge.inactive {
    color: var(--relay-muted);
  }

  .risk {
    color: var(--relay-live);
    font-size: 11px;
    font-weight: 850;
  }

  .risk.warn {
    color: var(--relay-warn);
  }

  .risk.bad {
    color: var(--relay-bad);
  }

  .risk.closed {
    color: var(--relay-closed);
  }

  .cell {
    height: 9px;
    border-radius: 1px;
    background: #1b3343;
    transition: filter 0.15s ease;
  }

  .cell:hover {
    filter: brightness(1.5) saturate(1.5) contrast(1.2);
  }

  .cell.live, .legend .live {
    background: linear-gradient(180deg, #52ffae, #23d786);
    box-shadow: 0 0 5px #45ff9a44;
  }

  .cell.warn, .legend .warn {
    background: var(--relay-warn);
    box-shadow: 0 0 8px #ffc44780;
  }

  .cell.bad, .legend .bad {
    background: var(--relay-bad);
    box-shadow: 0 0 8px #ff536a8c;
  }

  .cell.closed_unmarked, .legend .closed_unmarked {
    background: transparent;
    box-shadow: none;
  }

  .cell.unknown, .legend .unknown {
    background: #566170;
  }

  .cell.no_sample, .legend .no_sample {
    background: #172532;
  }

  .axis {
    grid-column: 2 / -1;
    display: flex;
    justify-content: space-between;
    padding-top: 2px;
    color: #66889a;
    font-size: 9px;
  }

  .health-tooltip {
    position: fixed;
    z-index: 10000;
    pointer-events: none;
    background: rgba(15, 33, 48, 0.95);
    backdrop-filter: blur(8px);
    border: 1px solid var(--relay-line-soft);
    border-radius: 8px;
    padding: 10px 14px;
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.5);
    color: var(--relay-text);
    font-size: 11px;
    min-width: 200px;
    display: flex;
    flex-direction: column;
    gap: 6px;
  }

  .tooltip-title {
    font-weight: bold;
    color: var(--relay-info);
    border-bottom: 1px solid rgba(255, 255, 255, 0.1);
    padding-bottom: 6px;
    margin-bottom: 2px;
  }

  .tooltip-body div {
    display: flex;
    justify-content: space-between;
    gap: 16px;
  }

  .tooltip-body span {
    color: var(--relay-muted);
  }

  .tooltip-body b {
    color: var(--relay-text);
  }

  .tooltip-body .error {
    margin-top: 4px;
    color: var(--relay-bad);
    max-width: 250px;
    white-space: normal;
    word-break: break-all;
  }

  @media (max-width: 900px) {
    .timeline {
      --timeline-label-width: 260px;
    }

    .symbol-row {
      grid-template-columns: minmax(72px, 1fr) 44px 58px 58px;
    }

    .symbol-row em:nth-of-type(n + 3),
    .symbol-row .risk {
      display: none;
    }
  }
</style>
