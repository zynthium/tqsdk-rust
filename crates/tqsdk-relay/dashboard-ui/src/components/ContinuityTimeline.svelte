<script lang="ts">
  import { EXCHANGES, exchangeOf } from '../lib/timeline';
  import { formatDuration, formatNumber } from '../lib/format';
  import { statusLabel } from '../lib/integrity-model';
  import type { SymbolRow, TimelineSample, TimelineSeverity } from '../lib/types';

  let { buckets, rows, selectedSymbol = $bindable(null) }: {
    buckets: Array<TimelineSample | null>;
    rows: SymbolRow[];
    selectedSymbol?: string | null;
  } = $props();
  let expandedExchanges = $state<string[]>([]);

  type TimelineDefinition =
    | {
        kind: 'summary';
        key: string;
        label: string;
        emptySeverity?: TimelineSeverity;
        severity: (sample: TimelineSample) => TimelineSeverity;
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
      };

  let exchangeRows = $derived(EXCHANGES.filter((exchange) => rows.some((row) => exchangeOf(row.symbol) === exchange)));
  let selected = $derived(rows.find((row) => row.symbol === selectedSymbol) ?? null);
  let definitions = $derived<TimelineDefinition[]>([
    { kind: 'summary', key: 'global', label: '全局', severity: (sample: TimelineSample) => sample.globalSeverity },
    { kind: 'summary', key: 'subscribed', label: '订阅', severity: (sample: TimelineSample) => sample.subscribedSeverity },
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
        })),
      ];
    }),
  ]);

  function cellClass(definition: TimelineDefinition, sample: TimelineSample | null) {
    const severity = sample ? definition.severity(sample) : (definition.emptySeverity ?? 'no_sample');
    return severity === 'closed' ? 'closed_unmarked' : severity;
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
    <div class="legend">
      <span><i class="live"></i>正常</span>
      <span><i class="warn"></i>静默</span>
      <span><i class="bad"></i>异常</span>
      <span><i class="closed_unmarked"></i>休盘</span>
      <span><i class="unknown"></i>未知</span>
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
        <button
          type="button"
          data-testid="timeline-symbol-row"
          class:selected={definition.symbol === selectedSymbol}
          class="row-label symbol-row"
          title={definition.symbol}
          aria-label={`${definition.label} ${statusLabel(definition.row.status)} ${formatDuration(definition.row.receive_gap_ms)} ${definition.row.problem_severity}`}
          onclick={() => (selectedSymbol = definition.symbol)}
        >
          <span class="symbol-name">{definition.label}</span>
          <span class={`badge ${definition.row.status}`}>{statusLabel(definition.row.status)}</span>
          <em>距 {formatDuration(definition.row.receive_gap_ms)}</em>
          <em>延 {formatDuration(definition.row.market_time_lag_ms)}</em>
          <em>Tick {formatNumber(definition.row.ticks_ingested)}</em>
          <em>订阅 {formatNumber(subscriberCount(definition.row))}</em>
          <span class={`risk ${definition.row.problem_severity}`}>{definition.row.problem_severity}</span>
        </button>
      {:else}
        <div class="row-label">{definition.label}</div>
      {/if}
      {#each buckets as bucket}
        <span class={`cell ${cellClass(definition, bucket)}`}></span>
      {/each}
    {/each}
    <div class="axis"><span>-5m</span><span>now</span></div>
  </div>
  {#if selected}
    <div class="health-detail">
      <span title={selected.symbol}>{selected.instrument_name ?? selected.symbol}</span>
      <span>last price <b>{formatNumber(selected.last_price)}</b></span>
      <span>volume <b>{formatNumber(selected.last_volume)}</b></span>
      <span>open interest <b>{formatNumber(selected.last_open_interest)}</b></span>
      <span>invalid rows <b>{formatNumber(selected.invalid_rows)}</b></span>
      {#if selected.last_invalid_row_error}
        <span class="error" title={selected.last_invalid_row_error}>{selected.last_invalid_row_error}</span>
      {/if}
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
    --timeline-label-width: clamp(280px, 31vw, 520px);

    position: relative;
    z-index: 1;
    margin-top: 12px;
    display: grid;
    grid-template-columns: var(--timeline-label-width) repeat(var(--bucket-count), minmax(3px, 1fr));
    grid-auto-rows: 27px;
    align-items: center;
    column-gap: 2px;
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
    grid-template-columns: minmax(72px, 1fr) 44px 58px 58px 70px 54px 50px;
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
    font-size: 9px;
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
    font-size: 9px;
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
    font-size: 9px;
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

  .closed_unmarked {
    background: transparent;
    box-shadow: none;
  }

  .unknown {
    background: #566170;
  }

  .no_sample {
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

  .health-detail {
    margin-top: 10px;
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    color: var(--relay-muted);
    font-size: 11px;
  }

  .health-detail span {
    min-width: 0;
    overflow: hidden;
    border: 1px solid var(--relay-line-soft);
    border-radius: 4px;
    padding: 5px 7px;
    background: #071929;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .health-detail b {
    color: var(--relay-text);
  }

  .health-detail .error {
    max-width: 300px;
    color: var(--relay-bad);
  }

  @media (max-width: 900px) {
    .timeline {
      --timeline-label-width: 260px;
    }

    .symbol-row {
      grid-template-columns: minmax(72px, 1fr) 42px 52px 52px;
    }

    .symbol-row em:nth-of-type(n + 3),
    .symbol-row .risk {
      display: none;
    }
  }
</style>
