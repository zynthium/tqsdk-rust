<script lang="ts">
  import { EXCHANGES, exchangeOf, productGroupOf } from '../lib/timeline';
  import { formatDuration, formatNumber, formatTime } from '../lib/format';
  import { statusLabel } from '../lib/integrity-model';
  import type {
    DashboardTimelineScope,
    ProblemSeverity,
    SymbolTradingPhase,
    SymbolRow,
    SymbolStatus,
    TimelineSample,
    TimelineSeverity,
  } from '../lib/types';

  const PANEL_PREFS_KEY = 'tqsdk-relay.dashboard.continuity-panel';
  const NAME_COLLATOR = new Intl.Collator('zh-Hans-CN', { numeric: true, sensitivity: 'base' });
  type ViewMode = 'blocks' | 'sparkline';
  type PanelPrefs = {
    openOnly?: boolean;
    viewMode?: ViewMode;
    expandAll?: boolean;
  };

  const statusBadgeClass = {
    live: 'badge live severity-badge justify-self-start text-[color:var(--relay-live)]',
    closed: 'badge closed severity-badge justify-self-start text-[color:var(--relay-closed)]',
    initializing: 'badge initializing severity-badge justify-self-start text-[color:var(--relay-muted)]',
    stale: 'badge stale severity-badge justify-self-start text-[color:var(--relay-warn)]',
    missing: 'badge missing severity-badge justify-self-start text-[color:var(--relay-warn)]',
    inactive: 'badge inactive severity-badge justify-self-start text-[color:var(--relay-muted)]',
  } satisfies Record<SymbolStatus, string>;

  const riskClass = {
    live: 'risk live text-[11px] font-[850] text-[color:var(--relay-live)]',
    closed: 'risk closed text-[11px] font-[850] text-[color:var(--relay-closed)]',
    initializing: 'risk initializing text-[11px] font-[850] text-[color:var(--relay-muted)]',
    warn: 'risk warn text-[11px] font-[850] text-[color:var(--relay-warn)]',
    bad: 'risk bad text-[11px] font-[850] text-[color:var(--relay-bad)]',
  } satisfies Record<ProblemSeverity, string>;

  const cellToneClass = {
    live: 'live bg-[#23d786]',
    auction: 'auction',
    warn: 'warn bg-[color:var(--relay-warn)]',
    bad: 'bad bg-[color:var(--relay-bad)]',
    unknown: 'unknown bg-[#566170]',
    no_sample: 'no_sample bg-[#172532]',
  } as const;

  let { buckets, rows = [] }: {
    buckets: Array<TimelineSample | null>;
    rows?: SymbolRow[];
  } = $props();
  const initialPrefs = readPanelPrefs();
  let viewMode = $state<ViewMode>(initialPrefs.viewMode ?? 'blocks');
  let search = $state('');
  let openOnly = $state(initialPrefs.openOnly ?? false);
  let expandAll = $state(initialPrefs.expandAll ?? false);
  let expandedExchanges = $state<string[]>([]);
  let hoveredCell = $state<HoveredCell | null>(null);
  let tooltipX = $state(0);
  let tooltipY = $state(0);
  let searchNeedle = $derived(search.trim().toLowerCase());
  let visibleRows = $derived(
    rows.filter((row) => (!openOnly || row.session === 'open') && (!searchNeedle || matchesSymbolSearch(row, searchNeedle))),
  );

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
  type HoveredCell = {
    definition: TimelineDefinition;
    sample: TimelineSample | null;
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
    Array.from(new Set(visibleRows.map((row) => exchangeOf(row.symbol))))
      .sort((left, right) => exchangeRank(left) - exchangeRank(right) || left.localeCompare(right)),
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
    ...(expandAll
      ? orderedSymbolRows(visibleRows).map((row): TimelineDefinition => symbolDefinition(row))
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

  $effect(() => {
    writePanelPrefs({ openOnly, viewMode, expandAll });
  });

  function scopeSummary(scope: DashboardTimelineScope | undefined): string {
    if (!scope) return '0/0';
    return `${formatNumber(scope.problem)}/${formatNumber(scope.total)}`;
  }

  function cellSeverity(definition: TimelineDefinition, sample: TimelineSample | null): TimelineSeverity {
    return sample ? (definition.severity(sample) ?? 'unknown') : (definition.emptySeverity ?? 'no_sample');
  }

  function cellClass(definition: TimelineDefinition, sample: TimelineSample | null): keyof typeof cellToneClass {
    const severity = cellSeverity(definition, sample);
    return severity === 'closed' ? 'unknown' : severity;
  }

  function isQuietSeverity(severity: TimelineSeverity): boolean {
    return severity === 'closed' || severity === 'auction';
  }

  function latency(definition: TimelineDefinition, sample: TimelineSample): number {
    return definition.latency(sample) ?? 0;
  }

  function averageLatencyLabel(definition: TimelineDefinition): string {
    if (latestSample && isQuietSeverity(cellSeverity(definition, latestSample))) return '⌁ --';
    const delay = latestSample
      ? (definition.averageLatency(latestSample) ?? definition.latency(latestSample))
      : null;
    return delay == null ? '⌁ --' : `⌁ ${formatDuration(delay)}`;
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
        else if (severity === 'auction') y = 85;
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
    if (severity === 'auction') return '#38bdf8';
    if (severity === 'closed') return '#566170';
    return 'var(--relay-live)';
  }

  function cellTitle(definition: TimelineDefinition, sample: TimelineSample | null): string {
    if (!sample) return `${definition.label} 无样本`;
    const severity = cellSeverity(definition, sample);
    const delay = isQuietSeverity(severity) ? '--' : formatDuration(latency(definition, sample));
    return `${definition.label} ${severity} ${definition.summary} ${delay}`;
  }

  function handleHover(definition: TimelineDefinition, sample: TimelineSample | null, event: MouseEvent) {
    hoveredCell = { definition, sample };
    tooltipX = event.clientX + 15;
    tooltipY = Math.min(event.clientY + 15, window.innerHeight - 160);
  }

  function clearHover() {
    hoveredCell = null;
  }

  function scopeFor(definition: TimelineDefinition, sample: TimelineSample): DashboardTimelineScope | undefined {
    if (definition.kind === 'summary') return definition.key === 'global' ? sample.sample.global : sample.sample.subscribed;
    if (definition.kind === 'exchange' && definition.exchange) return sample.sample.exchanges[definition.exchange];
    return undefined;
  }

  function cellSummary(definition: TimelineDefinition, sample: TimelineSample | null): string {
    return sample ? scopeSummary(scopeFor(definition, sample)) : definition.summary;
  }

  function cellLatencyLabel(definition: TimelineDefinition, sample: TimelineSample | null): string {
    if (!sample || isQuietSeverity(cellSeverity(definition, sample))) return '--';
    return formatDuration(definition.latency(sample));
  }

  function cellAverageLatencyLabel(definition: TimelineDefinition, sample: TimelineSample | null): string {
    if (!sample || isQuietSeverity(cellSeverity(definition, sample))) return '--';
    return formatDuration(definition.averageLatency(sample) ?? definition.latency(sample));
  }

  function rowLatencyLabel(definition: TimelineDefinition, sample: TimelineSample | null): string {
    if (!sample || !definition.row) return '--';
    if (isQuietSeverity(cellSeverity(definition, sample))) return '--';
    return rowDelayLabel(definition.row, definition.row.market_time_lag_ms);
  }

  function sparklineBucket(event: MouseEvent): number {
    const target = event.currentTarget as HTMLElement | null;
    const bucketsLength = buckets.length;
    if (bucketsLength < 1 || !target) return 0;

    const typedEvent = event as MouseEvent & { offsetX?: number };
    const offsetX = typedEvent.offsetX;
    const rect = target.getBoundingClientRect();
    const width = target.clientWidth || rect.width;
    const fallbackWidth = bucketsLength * 10;
    const effectiveWidth = width > 0 ? width : fallbackWidth;

    const pointerX = typeof offsetX === 'number' ? offsetX : event.clientX - rect.left;
    const rawIndex = Math.floor((pointerX / effectiveWidth) * bucketsLength);
    return Math.max(0, Math.min(bucketsLength - 1, rawIndex));
  }

  function handleSparklineHover(definition: TimelineDefinition, event: MouseEvent) {
    hoveredCell = { definition, sample: buckets[sparklineBucket(event)] };
    tooltipX = event.clientX + 15;
    tooltipY = Math.min(event.clientY + 15, window.innerHeight - 160);
  }

  function readPanelPrefs(): PanelPrefs {
    try {
      const raw = localStorage.getItem(PANEL_PREFS_KEY);
      if (!raw) return {};
      const parsed = JSON.parse(raw) as PanelPrefs;
      return {
        openOnly: typeof parsed.openOnly === 'boolean' ? parsed.openOnly : undefined,
        viewMode: parsed.viewMode === 'sparkline' ? 'sparkline' : parsed.viewMode === 'blocks' ? 'blocks' : undefined,
        expandAll: typeof parsed.expandAll === 'boolean' ? parsed.expandAll : undefined,
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

  function orderedSymbolRows(exchangeSymbols: SymbolRow[]): SymbolRow[] {
    const productNames = productNamesByGroup(exchangeSymbols);
    return [...exchangeSymbols].sort((left, right) => {
      const leftGroup = productGroupOf(left.symbol);
      const rightGroup = productGroupOf(right.symbol);
      return compareText(productNames.get(leftGroup) ?? leftGroup, productNames.get(rightGroup) ?? rightGroup)
        || leftGroup.localeCompare(rightGroup)
        || compareText(contractName(left), contractName(right))
        || left.symbol.localeCompare(right.symbol);
    });
  }

  function productNamesByGroup(rows: SymbolRow[]): Map<string, string> {
    const names = new Map<string, string>();
    for (const row of rows) {
      const group = productGroupOf(row.symbol);
      const name = productName(row);
      const current = names.get(group);
      if (!current || compareText(name, current) < 0) {
        names.set(group, name);
      }
    }
    return names;
  }

  function productName(row: SymbolRow): string {
    return contractName(row)
      .replace(/(?:主连|加权|指数)$/u, '')
      .replace(/\d+$/u, '')
      .trim() || productGroupOf(row.symbol);
  }

  function contractName(row: SymbolRow): string {
    return row.instrument_name?.trim() || row.symbol;
  }

  function compareText(left: string, right: string): number {
    return NAME_COLLATOR.compare(left, right) || left.localeCompare(right);
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
      emptySeverity: row.session === 'closed' ? 'closed' : isAuctionPhase(row.phase) ? 'auction' : 'no_sample',
    };
  }

  function subscriberCount(row: SymbolRow): number {
    return row.quote_subscriber_count + row.chart_subscriber_count;
  }

  function rowDelayLabel(row: SymbolRow, value: number | null | undefined): string {
    return row.session === 'closed' || isAuctionPhase(row.phase) ? '--' : formatDuration(value);
  }

  function toggleExchange(exchange: string) {
    expandedExchanges = expandedExchanges.includes(exchange)
      ? expandedExchanges.filter((item) => item !== exchange)
      : [...expandedExchanges, exchange];
  }

  function isAuctionPhase(phase: SymbolTradingPhase): boolean {
    return phase.startsWith('auction');
  }

  function phaseLabel(row: SymbolRow): string | null {
    if (row.phase === 'auction_ordering') return '集合竞价';
    if (row.phase === 'auction_balance') return '竞价平衡';
    if (row.phase === 'auction_match') return '竞价撮合';
    if (row.phase === 'pre_close') return '临近收盘';
    return null;
  }
</script>

<section class="panel-shell flex min-h-0 flex-col px-3 py-2.5" data-testid="continuity-timeline">
  <div class="flex items-center justify-between gap-3">
    <div class="panel-title">最近 5 分钟连续性</div>
    <div class="ml-auto flex items-center gap-2">
      <label class="inline-flex h-[26px] items-center gap-[5px] rounded-md border border-[color:var(--relay-line-soft)] bg-[#071929] px-2 text-[11px] whitespace-nowrap text-[color:var(--relay-muted)]">
        <input class="m-0 size-[13px] accent-[color:var(--relay-live)]" type="checkbox" bind:checked={openOnly} aria-label="只看开盘中品种" />
        <span>开盘中</span>
      </label>
      <label class="inline-flex h-[26px] items-center gap-[5px] rounded-md border border-[color:var(--relay-line-soft)] bg-[#071929] px-2 text-[11px] whitespace-nowrap text-[color:var(--relay-muted)]">
        <input class="m-0 size-[13px] accent-[color:var(--relay-live)]" type="checkbox" bind:checked={expandAll} aria-label="展开" />
        <span>展开</span>
      </label>
      <input
        class="h-[26px] min-w-0 rounded-md border border-[color:var(--relay-line-soft)] bg-[#071929] px-[9px] text-[10px] text-[color:var(--relay-text)] outline-none placeholder:text-[10px] placeholder:text-[color:var(--relay-muted)] w-[clamp(150px,14vw,220px)]"
        bind:value={search}
        placeholder="搜索合约或中文名"
        aria-label="搜索合约或中文名"
      />
      <div class="view-toggle flex gap-1 rounded-md border border-[color:var(--relay-line-soft)] bg-[#0f2130] p-[3px]">
        <button class:active={viewMode === 'blocks'} onclick={() => (viewMode = 'blocks')}>Blocks</button>
        <button class:active={viewMode === 'sparkline'} onclick={() => (viewMode = 'sparkline')}>Sparkline</button>
      </div>
    </div>
    <div class="legend flex gap-2.5 text-[10px] text-[color:var(--relay-muted)]">
      <span class="inline-flex items-center gap-[5px]"><i class="live"></i>正常</span>
      <span class="inline-flex items-center gap-[5px]"><i class="auction"></i>竞价</span>
      <span class="inline-flex items-center gap-[5px]"><i class="warn"></i>静默</span>
      <span class="inline-flex items-center gap-[5px]"><i class="bad"></i>异常</span>
      <span class="inline-flex items-center gap-[5px]"><i class="unknown"></i>休盘</span>
      <span class="inline-flex items-center gap-[5px]"><i class="no_sample"></i>无样本</span>
    </div>
  </div>
  <div class="timeline" style={`--bucket-count:${buckets.length}`}>
    {#each definitions as definition (definition.key)}
      {#if definition.kind === 'exchange' && definition.exchange}
        <button
          type="button"
          class="row-label exchange-row border-0 bg-transparent cursor-pointer font-[inherit] text-left hover:text-[color:var(--relay-info)]"
          aria-expanded={definition.expanded}
          aria-label={`${definition.label} ${definition.summary} 异常`}
          title={definition.label}
          onclick={() => definition.exchange && toggleExchange(definition.exchange)}
        >
          <span class="w-3 text-[color:var(--relay-info)] font-black">{definition.expanded ? '-' : '+'}</span>
          <span class="name">{definition.label}</span>
          <em class="summary">{definition.summary}</em>
          <strong class="latency">{averageLatencyLabel(definition)}</strong>
        </button>
      {:else if definition.kind === 'symbol' && definition.row}
        <div
          data-testid="timeline-symbol-row"
          class="row-label symbol-row"
          title={definition.row.symbol}
          aria-label={`${definition.label} ${phaseLabel(definition.row) ?? ''} ${statusLabel(definition.row.status)} ${rowDelayLabel(definition.row, definition.row.receive_gap_ms)} ${definition.row.problem_severity}`}
        >
          <span class="symbol-name overflow-hidden text-ellipsis whitespace-nowrap">{definition.label}</span>
          <span class={statusBadgeClass[definition.row.status]}>
            {phaseLabel(definition.row) ?? statusLabel(definition.row.status)}
          </span>
          <em class="tick">Tick {formatNumber(definition.row.ticks_ingested)}</em>
          <em class="sub">订阅 {formatNumber(subscriberCount(definition.row))}</em>
          <span class={riskClass[definition.row.problem_severity]}>{definition.row.problem_severity}</span>
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
          <span class={`cell ${cellToneClass[cellClass(definition, bucket)]}`} title={cellTitle(definition, bucket)} onmousemove={(event) => handleHover(definition, bucket, event)} onmouseleave={clearHover}></span>
        {/each}
      {:else}
        <!-- svelte-ignore a11y_no_static_element_interactions -->
        <div class="flex items-center h-full w-full" style="grid-column: 2 / -1;" onmousemove={(event) => handleSparklineHover(definition, event)} onmouseleave={clearHover}>
          <svg class="h-[20px] w-full" viewBox={`0 0 ${buckets.length * 10} 100`} preserveAspectRatio="none">
            <path d={sparklinePath(definition, buckets)} stroke={sparklineColor(definition, buckets)} stroke-width="1" fill="none" vector-effect="non-scaling-stroke"/>
          </svg>
        </div>
      {/if}
    {/each}
    <div class="axis"><span>-5m</span><span>now</span></div>
  </div>
  {#if hoveredCell}
    <div
      class="health-tooltip fixed z-[10000] flex min-w-[200px] pointer-events-none flex-col gap-1.5 rounded-lg border border-[color:var(--relay-line-soft)] bg-[rgba(15,33,48,0.95)] px-[14px] py-[10px] text-[11px] text-[color:var(--relay-text)] shadow-[0_4px_12px_rgba(0,0,0,0.5)]"
      style={`left: ${tooltipX}px; top: ${tooltipY}px;`}
    >
      <div class="mb-0.5 border-b border-[rgba(255,255,255,0.1)] pb-1.5 font-bold text-[color:var(--relay-info)]">{hoveredCell.definition.label}</div>
      <div class="tooltip-body">
        <div><span>样本时间</span><b>{hoveredCell.sample ? formatTime(hoveredCell.sample.sampledAt) : '无样本'}</b></div>
        <div><span>状态</span><b>{cellSeverity(hoveredCell.definition, hoveredCell.sample)}</b></div>
        <div><span>接收延迟</span><b>{cellLatencyLabel(hoveredCell.definition, hoveredCell.sample)}</b></div>
        <div><span>平均接收</span><b>{cellAverageLatencyLabel(hoveredCell.definition, hoveredCell.sample)}</b></div>
        {#if hoveredCell.definition.row}
          <div><span>市场延迟</span><b>{rowLatencyLabel(hoveredCell.definition, hoveredCell.sample)}</b></div>
        {/if}
        {#if hoveredCell.definition.kind !== 'symbol'}
          <div><span>异常/总数</span><b>{cellSummary(hoveredCell.definition, hoveredCell.sample)}</b></div>
        {/if}
        {#if hoveredCell.definition.row}
          <div><span>异常记录数</span><b>{formatNumber(hoveredCell.definition.row.invalid_rows)}</b></div>
          {#if hoveredCell.definition.row.last_invalid_row_error}
            <div class="error" title={hoveredCell.definition.row.last_invalid_row_error}>{hoveredCell.definition.row.last_invalid_row_error}</div>
          {/if}
        {/if}
      </div>
    </div>
  {/if}
</section>

<style>
  .view-toggle button {
    border: 0;
    border-radius: 4px;
    background: transparent;
    color: var(--relay-muted);
    cursor: pointer;
    font-size: 10px;
    font-weight: 600;
    padding: 3px 8px;
    line-height: 14px;
  }

  .view-toggle button:hover {
    color: var(--relay-text);
  }

  .view-toggle button.active {
    background: #1d3648;
    color: var(--relay-info);
  }

  .legend i {
    width: 12px;
    height: 7px;
    border-radius: 2px;
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

  .cell {
    height: 9px;
    border-radius: 1px;
    background: #1b3343;
  }

  .cell.live, .legend .live {
    background: #23d786;
  }

  .cell.auction, .legend .auction {
    background: #38bdf8;
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
