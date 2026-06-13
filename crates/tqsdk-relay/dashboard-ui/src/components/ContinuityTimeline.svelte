<script lang="ts">
  import { EXCHANGES, exchangeOf } from '../lib/timeline';
  import { formatDuration, formatNumber } from '../lib/format';
  import { statusLabel } from '../lib/integrity-model';
  import type { DashboardTimelineScope, SymbolRow, TimelineSample, TimelineSeverity } from '../lib/types';

  const PANEL_PREFS_KEY = 'tqsdk-relay.dashboard.continuity-panel';
  type ViewMode = 'blocks' | 'sparkline';
  type PanelPrefs = {
    search?: string;
    openOnly?: boolean;
    viewMode?: ViewMode;
    flatSymbols?: boolean;
  };

  let { buckets, rows = [] }: {
    buckets: Array<TimelineSample | null>;
    rows?: SymbolRow[];
  } = $props();
  const initialPrefs = readPanelPrefs();
  let viewMode = $state<ViewMode>(initialPrefs.viewMode ?? 'blocks');
  let search = $state(initialPrefs.search ?? '');
  let openOnly = $state(initialPrefs.openOnly ?? false);
  let flatSymbols = $state(initialPrefs.flatSymbols ?? false);
  let expandedExchanges = $state<string[]>([]);
  let hoveredSymbol = $state<string | null>(null);
  let tooltipX = $state(0);
  let tooltipY = $state(0);
  let searchNeedle = $derived(search.trim().toLowerCase());
  let visibleRows = $derived(
    rows.filter((row) => (!openOnly || row.session === 'open') && (!searchNeedle || matchesSymbolSearch(row, searchNeedle))),
  );
  let hoveredSymbolRow = $derived(visibleRows.find((row) => row.symbol === hoveredSymbol) ?? null);

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
    Object.keys(latestSample?.sample.exchanges ?? {})
      .filter((exchange) => visibleRows.some((row) => exchangeOf(row.symbol) === exchange))
      .sort((left, right) => exchangeRank(left) - exchangeRank(right) || left.localeCompare(right)),
  );
  let flatRows = $derived(orderedSymbolRows(visibleRows, Number.POSITIVE_INFINITY));
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
    ...(flatSymbols
      ? flatRows.map((row): TimelineDefinition => symbolDefinition(row))
      : exchangeRows.flatMap((exchange) => {
          const scope = latestSample?.sample.exchanges[exchange];
          const exchangeSymbols = orderedSymbolRows(visibleRows.filter((row) => exchangeOf(row.symbol) === exchange));
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
            ...exchangeSymbols.map((row): TimelineDefinition => symbolDefinition(row)),
          ];
        })),
  ]);
  let pageRowCount = $derived(rows.length);
  let visibleRowCount = $derived(visibleRows.length);
  let isFiltered = $derived(openOnly || searchNeedle.length > 0);

  $effect(() => {
    writePanelPrefs({ search, openOnly, viewMode, flatSymbols });
  });

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

  function readPanelPrefs(): PanelPrefs {
    try {
      const raw = localStorage.getItem(PANEL_PREFS_KEY);
      if (!raw) return {};
      const parsed = JSON.parse(raw) as PanelPrefs;
      return {
        search: typeof parsed.search === 'string' ? parsed.search : undefined,
        openOnly: typeof parsed.openOnly === 'boolean' ? parsed.openOnly : undefined,
        viewMode: parsed.viewMode === 'sparkline' ? 'sparkline' : parsed.viewMode === 'blocks' ? 'blocks' : undefined,
        flatSymbols: typeof parsed.flatSymbols === 'boolean' ? parsed.flatSymbols : undefined,
      };
    } catch {
      return {};
    }
  }

  function writePanelPrefs(prefs: PanelPrefs) {
    try {
      localStorage.setItem(PANEL_PREFS_KEY, JSON.stringify(prefs));
    } catch {
      // Ignore storage failures; the dashboard should remain usable in private or locked-down contexts.
    }
  }

  function matchesSymbolSearch(row: SymbolRow, needle: string): boolean {
    return row.symbol.toLowerCase().includes(needle)
      || row.instrument_name?.toLowerCase().includes(needle)
      || false;
  }

  function orderedSymbolRows(exchangeSymbols: SymbolRow[], limit = 30): SymbolRow[] {
    const sorted = [...exchangeSymbols]
      .sort((left, right) => severityRank(left) - severityRank(right) || (right.receive_gap_ms ?? -1) - (left.receive_gap_ms ?? -1));
    return limit === Number.POSITIVE_INFINITY ? sorted : sorted.slice(0, limit);
  }

  function symbolDefinition(row: SymbolRow): TimelineDefinition {
    return {
      kind: 'symbol',
      key: `symbol:${row.symbol}`,
      label: row.instrument_name ?? row.symbol,
      summary: row.subscribed ? '订阅' : '',
      row,
      severity: (sample: TimelineSample) => sample.symbols[row.symbol]?.severity,
      latency: (sample: TimelineSample) => sample.symbols[row.symbol]?.receive_gap_ms,
      averageLatency: (sample: TimelineSample) => sample.symbols[row.symbol]?.avg_receive_gap_ms,
      emptySeverity: row.session === 'closed' ? 'closed' : 'no_sample',
    };
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
    <div class="panel-tools">
      <label class="open-only-toggle">
        <input type="checkbox" bind:checked={openOnly} aria-label="只看开盘中品种" />
        <span>开盘中</span>
      </label>
      <label class="open-only-toggle">
        <input type="checkbox" bind:checked={flatSymbols} aria-label="不分交易所" />
        <span>不分交易所</span>
      </label>
      <input class="panel-search" bind:value={search} placeholder="搜索合约或中文名" aria-label="搜索合约或中文名" />
      <div class="view-toggle">
        <button class:active={viewMode === 'blocks'} onclick={() => (viewMode = 'blocks')}>Blocks</button>
        <button class:active={viewMode === 'sparkline'} onclick={() => (viewMode = 'sparkline')}>Sparkline</button>
      </div>
    </div>
    <div class="legend">
      <span><i class="live"></i>正常</span>
      <span><i class="warn"></i>静默</span>
      <span><i class="bad"></i>异常</span>
      <span><i class="unknown"></i>休盘</span>
      <span><i class="no_sample"></i>无样本</span>
    </div>
  </div>
  <div class="timeline-meta">
    当前页 {formatNumber(pageRowCount)} 行{#if isFiltered} · 显示 {formatNumber(visibleRowCount)} 行{/if}
  </div>
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
          <span class="name">{definition.label}</span>
          <em class="summary">{definition.summary}</em>
          <strong class="latency">{averageLatencyLabel(definition)}</strong>
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
          <em class="tick">Tick {formatNumber(definition.row.ticks_ingested)}</em>
          <em class="sub">订阅 {formatNumber(subscriberCount(definition.row))}</em>
          <span class={`risk ${definition.row.problem_severity}`}>{definition.row.problem_severity}</span>
          <strong class="latency">{averageLatencyLabel(definition)}</strong>
        </div>
      {:else}
        <div class="row-label summary-row" title={definition.label}>
          <span class="name">{definition.label}</span>
          <em class="summary">{definition.summary}</em>
          <strong class="latency">{averageLatencyLabel(definition)}</strong>
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

  .panel-tools {
    display: flex;
    align-items: center;
    gap: 8px;
    margin-left: auto;
  }

  .panel-search {
    width: clamp(150px, 14vw, 220px);
    height: 26px;
    min-width: 0;
    border: 1px solid var(--relay-line-soft);
    border-radius: 6px;
    background: #071929;
    color: var(--relay-text);
    font-size: 11px;
    padding: 0 9px;
  }

  .panel-search::placeholder {
    color: var(--relay-muted);
  }

  .open-only-toggle {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    height: 26px;
    border: 1px solid var(--relay-line-soft);
    border-radius: 6px;
    background: #071929;
    color: var(--relay-muted);
    font-size: 11px;
    padding: 0 8px;
    white-space: nowrap;
  }

  .open-only-toggle input {
    width: 13px;
    height: 13px;
    margin: 0;
    accent-color: var(--relay-live);
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
    /* 1: caret, 2: name, 3: badge, 4: tick, 5: sub, 6: risk, 7: latency */
    grid-template-columns: 12px minmax(0, 1fr) 42px 64px 50px 38px 46px;
    align-items: center;
    gap: 6px;
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

  .summary-row .name { grid-column: 1 / 3; }
  .summary-row .summary,
  .exchange-row .summary { grid-column: 3 / 7; text-align: right; }
  
  .exchange-row .caret { grid-column: 1; }
  .exchange-row .name { grid-column: 2; }
  
  .symbol-row {
    color: #9fc4d5;
  }
  
  .symbol-row .symbol-name { grid-column: 2; }
  .symbol-row .badge { grid-column: 3; }
  .symbol-row .tick { grid-column: 4; text-align: right; }
  .symbol-row .sub { grid-column: 5; text-align: right; }
  .symbol-row .risk { grid-column: 6; text-align: center; }
  
  .row-label .latency { grid-column: 7; text-align: right; }

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

  .badge.initializing {
    color: var(--relay-muted);
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

  .risk.initializing {
    color: var(--relay-muted);
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

    .row-label {
      /* 1: caret, 2: name, 3: badge, 4: tick, 5: latency */
      grid-template-columns: 12px minmax(0, 1fr) 42px 64px 46px;
    }

    .summary-row .summary,
    .exchange-row .summary {
      grid-column: 3 / 5;
    }

    .row-label .latency {
      grid-column: 5;
    }

    .symbol-row .sub,
    .symbol-row .risk {
      display: none;
    }

    .legend {
      display: none;
    }
  }
</style>
