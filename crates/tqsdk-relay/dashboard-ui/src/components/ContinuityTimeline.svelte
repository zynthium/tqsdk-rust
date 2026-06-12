<script lang="ts">
  import { EXCHANGES, exchangeOf } from '../lib/timeline';
  import { formatDuration, formatNumber } from '../lib/format';
  import { statusLabel } from '../lib/integrity-model';
  import type { DashboardTimelineScope, SymbolRow, TimelineSample, TimelineSeverity } from '../lib/types';

  let { buckets, rows = [] }: {
    buckets: Array<TimelineSample | null>;
    rows?: SymbolRow[];
  } = $props();
  let viewMode = $state<'blocks' | 'sparkline'>('blocks');
  let expandedExchanges = $state<string[]>([]);
  let hoveredSymbol = $state<string | null>(null);
  let tooltipX = $state(0);
  let tooltipY = $state(0);
  let hoveredSymbolRow = $derived(rows.find((row) => row.symbol === hoveredSymbol) ?? null);

  type TimelineDefinition = {
    kind: 'summary' | 'exchange' | 'symbol';
    key: string;
    label: string;
    summary: string;
    row?: SymbolRow;
    exchange?: string;
    expanded?: boolean;
    severity: (sample: TimelineSample) => TimelineSeverity | undefined;
    latency: (sample: TimelineSample) => number | null | undefined;
    averageLatency: (sample: TimelineSample) => number | null | undefined;
    emptySeverity?: TimelineSeverity;
  };

  function latestTimelineSample(buckets: Array<TimelineSample | null>): TimelineSample | null {
    for (let index = buckets.length - 1; index >= 0; index -= 1) {
      if (buckets[index]) return buckets[index];
    }
    return null;
  }

  function exchangeRank(exchange: string): number {
    const index = EXCHANGES.indexOf(exchange);
    return index === -1 ? EXCHANGES.length : index;
  }

  let latestSample = $derived(latestTimelineSample(buckets));
  let exchangeRows = $derived(
    Object.keys(latestSample?.sample.exchanges ?? {}).sort(
      (left, right) => exchangeRank(left) - exchangeRank(right) || left.localeCompare(right),
    ),
  );
  let definitions = $derived<TimelineDefinition[]>([
    {
      kind: 'summary',
      key: 'global',
      label: '全局',
      summary: scopeSummary(latestSample?.sample.global),
      severity: (sample) => sample.sample.global.severity,
      latency: (sample) => sample.sample.global.receive_gap_ms,
      averageLatency: (sample) => sample.sample.global.avg_receive_gap_ms,
    },
    {
      kind: 'summary',
      key: 'subscribed',
      label: '订阅',
      summary: scopeSummary(latestSample?.sample.subscribed),
      severity: (sample) => sample.sample.subscribed.severity,
      latency: (sample) => sample.sample.subscribed.receive_gap_ms,
      averageLatency: (sample) => sample.sample.subscribed.avg_receive_gap_ms,
    },
    ...exchangeRows.flatMap((exchange) => {
      const scope = latestSample?.sample.exchanges[exchange];
      const exchangeSymbols = orderedSymbolRows(rows.filter((row) => exchangeOf(row.symbol) === exchange));
      const expanded = expandedExchanges.includes(exchange);
      const exchangeDefinition: TimelineDefinition = {
        kind: 'exchange',
        key: `exchange:${exchange}`,
        label: exchange,
        summary: scopeSummary(scope),
        exchange,
        expanded,
        severity: (sample: TimelineSample) => sample.sample.exchanges[exchange]?.severity,
        latency: (sample: TimelineSample) => sample.sample.exchanges[exchange]?.receive_gap_ms,
        averageLatency: (sample: TimelineSample) => sample.sample.exchanges[exchange]?.avg_receive_gap_ms,
        emptySeverity: scope?.severity ?? 'no_sample',
      };
      if (!expanded) return [exchangeDefinition];
      return [
        exchangeDefinition,
        ...exchangeSymbols.map((row): TimelineDefinition => ({
          kind: 'symbol',
          key: `symbol:${row.symbol}`,
          label: row.instrument_name ?? row.symbol,
          summary: row.subscribed ? '订阅' : '',
          row,
          severity: (sample: TimelineSample) => sample.symbols[row.symbol]?.severity,
          latency: (sample: TimelineSample) => sample.symbols[row.symbol]?.receive_gap_ms,
          averageLatency: (sample: TimelineSample) => sample.symbols[row.symbol]?.avg_receive_gap_ms,
          emptySeverity: row.session === 'closed' ? 'closed' : 'no_sample',
        })),
      ];
    }),
  ]);
  let pageRowCount = $derived(rows.length);

  function scopeSummary(scope: DashboardTimelineScope | undefined): string {
    if (!scope) return '0/0';
    return `${formatNumber(scope.problem)}/${formatNumber(scope.total)}`;
  }

  function cellSeverity(definition: TimelineDefinition, sample: TimelineSample | null): TimelineSeverity {
    return sample ? (definition.severity(sample) ?? 'unknown') : (definition.emptySeverity ?? 'no_sample');
  }

  function cellClass(definition: TimelineDefinition, sample: TimelineSample | null) {
    const severity = cellSeverity(definition, sample);
    return severity === 'closed' ? 'unknown' : severity;
  }

  function latency(definition: TimelineDefinition, sample: TimelineSample): number {
    return definition.latency(sample) ?? 0;
  }

  function averageLatencyLabel(definition: TimelineDefinition): string {
    const average = latestSample ? definition.averageLatency(latestSample) : null;
    return average == null ? '⌁ --' : `⌁ ${formatDuration(average)}`;
  }

  function sparklinePath(definition: TimelineDefinition, buckets: Array<TimelineSample | null>): string {
    const coords: string[] = [];
    buckets.forEach((bucket, i) => {
      const x = i * 10;
      let y = 100;
      if (bucket) {
        const severity = cellSeverity(definition, bucket);
        if (severity === 'bad') y = 10;
        else if (severity === 'warn') y = 40;
        else {
          const lag = latency(definition, bucket);
          y = Math.max(70, 100 - (lag / 1000) * 30);
        }
      } else if ((definition.emptySeverity ?? 'no_sample') === 'closed') {
        y = 100;
      }
      coords.push(`${x},${y}`);
    });
    return coords.length ? `M ${coords.join(' L ')}` : '';
  }

  function sparklineColor(definition: TimelineDefinition, buckets: Array<TimelineSample | null>): string {
    const lastActive = latestTimelineSample(buckets);
    const severity = lastActive ? cellSeverity(definition, lastActive) : (definition.emptySeverity ?? 'no_sample');
    if (severity === 'bad') return 'var(--relay-bad)';
    if (severity === 'warn') return 'var(--relay-warn)';
    if (severity === 'closed') return '#566170';
    return 'var(--relay-live)';
  }

  function cellTitle(definition: TimelineDefinition, sample: TimelineSample | null): string {
    if (!sample) return `${definition.label} 无样本`;
    return `${definition.label} ${cellSeverity(definition, sample)} ${definition.summary} ${formatDuration(latency(definition, sample))}`;
  }

  function handleHover(definition: TimelineDefinition, event: MouseEvent) {
    if (definition.kind !== 'symbol' || !definition.row) return;
    hoveredSymbol = definition.row.symbol;
    tooltipX = event.clientX + 15;
    tooltipY = Math.min(event.clientY + 15, window.innerHeight - 160);
  }

  function clearHover() {
    hoveredSymbol = null;
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
  <div class="timeline-meta">当前页 {formatNumber(pageRowCount)} 行</div>
  <div class="timeline" style={`--bucket-count:${buckets.length}`}>
    {#each definitions as definition (definition.key)}
      {#if definition.kind === 'exchange' && definition.exchange}
        <button
          type="button"
          class="row-label exchange-row"
          aria-expanded={definition.expanded}
          aria-label={`${definition.label} ${definition.summary} 异常`}
          title={definition.label}
          onclick={() => definition.exchange && toggleExchange(definition.exchange)}
        >
          <span class="caret">{definition.expanded ? '-' : '+'}</span>
          <span>{definition.label}</span>
          <em>{definition.summary}</em>
          <strong>{averageLatencyLabel(definition)}</strong>
        </button>
      {:else if definition.kind === 'symbol' && definition.row}
        <div
          data-testid="timeline-symbol-row"
          class="row-label symbol-row"
          title={definition.row.symbol}
          aria-label={`${definition.label} ${statusLabel(definition.row.status)} ${formatDuration(definition.row.receive_gap_ms)} ${definition.row.problem_severity}`}
        >
          <span class="symbol-name">{definition.label}</span>
          <span class={`badge ${definition.row.status}`}>{statusLabel(definition.row.status)}</span>
          <em class="avg-latency">{averageLatencyLabel(definition)}</em>
          <em>Tick {formatNumber(definition.row.ticks_ingested)}</em>
          <em>订阅 {formatNumber(subscriberCount(definition.row))}</em>
          <span class={`risk ${definition.row.problem_severity}`}>{definition.row.problem_severity}</span>
        </div>
      {:else}
        <div class="row-label" title={definition.label}>
          <span>{definition.label}</span>
          <em>{definition.summary}</em>
          <strong>{averageLatencyLabel(definition)}</strong>
        </div>
      {/if}
      {#if viewMode === 'blocks'}
        {#each buckets as bucket, index (`${definition.key}:${index}`)}
          <!-- svelte-ignore a11y_no_static_element_interactions -->
          <span class={`cell ${cellClass(definition, bucket)}`} title={cellTitle(definition, bucket)} onmousemove={(event) => handleHover(definition, event)} onmouseleave={clearHover}></span>
        {/each}
      {:else}
        <!-- svelte-ignore a11y_no_static_element_interactions -->
        <div class="sparkline-container" style="grid-column: 2 / -1;" onmousemove={(event) => handleHover(definition, event)} onmouseleave={clearHover}>
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
        <div><span>平均接收</span><b>{formatDuration(hoveredSymbolRow.avg_receive_gap_ms)}</b></div>
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
    border: 1px solid var(--relay-line-soft);
    border-radius: 6px;
    background: #0f2130;
    padding: 3px;
  }

  .view-toggle button {
    border: 0;
    border-radius: 4px;
    background: transparent;
    color: var(--relay-muted);
    cursor: pointer;
    font-size: 10px;
    font-weight: 600;
    padding: 3px 8px;
  }

  .view-toggle button:hover {
    color: var(--relay-text);
  }

  .view-toggle button.active {
    background: #1d3648;
    color: var(--relay-info);
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

  .timeline-meta {
    margin-top: 8px;
    color: var(--relay-muted);
    font-size: 10px;
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
  }

  .timeline {
    --timeline-label-width: clamp(320px, 22vw, 380px);

    position: relative;
    z-index: 1;
    margin-top: 10px;
    display: grid;
    grid-template-columns: var(--timeline-label-width) repeat(var(--bucket-count), minmax(3px, 1fr));
    grid-auto-rows: 24px;
    align-items: center;
    column-gap: 2px;
    flex: 1;
    min-height: 0;
    overflow-y: auto;
    padding-right: 4px;
  }

  .row-label {
    min-width: 0;
    display: grid;
    grid-template-columns: minmax(0, 1fr) auto auto;
    align-items: center;
    gap: 8px;
    overflow: hidden;
    color: #c3dbe6;
    font-size: 11px;
  }

  button.row-label {
    border: 0;
    background: transparent;
    cursor: pointer;
    font: inherit;
    text-align: left;
  }

  button.row-label:hover {
    color: var(--relay-info);
  }

  .exchange-row {
    grid-template-columns: auto minmax(0, 1fr) auto auto;
  }

  .symbol-row {
    grid-template-columns: minmax(72px, 1fr) 44px 62px 66px 52px 42px;
    padding-left: 12px;
    color: #9fc4d5;
  }

  .caret {
    width: 12px;
    color: var(--relay-info);
    font-weight: 900;
  }

  .row-label span {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .row-label em {
    color: var(--relay-muted);
    font-size: 10px;
    font-style: normal;
    white-space: nowrap;
  }

  .row-label strong {
    color: var(--relay-info);
    font-size: 10px;
    font-weight: 800;
    white-space: nowrap;
  }

  .symbol-name {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
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
  }

  .cell.live, .legend .live {
    background: #23d786;
  }

  .cell.warn, .legend .warn {
    background: var(--relay-warn);
  }

  .cell.bad, .legend .bad {
    background: var(--relay-bad);
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
    display: flex;
    min-width: 200px;
    flex-direction: column;
    gap: 6px;
    border: 1px solid var(--relay-line-soft);
    border-radius: 8px;
    background: rgba(15, 33, 48, 0.95);
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.5);
    color: var(--relay-text);
    font-size: 11px;
    padding: 10px 14px;
  }

  .tooltip-title {
    margin-bottom: 2px;
    border-bottom: 1px solid rgba(255, 255, 255, 0.1);
    color: var(--relay-info);
    font-weight: 700;
    padding-bottom: 6px;
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
    max-width: 250px;
    margin-top: 4px;
    color: var(--relay-bad);
    white-space: normal;
    word-break: break-all;
  }

  @media (max-width: 900px) {
    .timeline {
      --timeline-label-width: 320px;
    }

    .symbol-row {
      grid-template-columns: minmax(72px, 1fr) 44px 64px;
    }

    .symbol-row em:nth-of-type(n + 2),
    .symbol-row .risk {
      display: none;
    }

    .legend {
      display: none;
    }
  }
</style>
