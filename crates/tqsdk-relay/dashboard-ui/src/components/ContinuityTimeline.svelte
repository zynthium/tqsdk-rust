<script lang="ts">
  import { EXCHANGES } from '../lib/timeline';
  import { formatDuration, formatNumber } from '../lib/format';
  import type { DashboardTimelineScope, SymbolRow, TimelineSample, TimelineSeverity } from '../lib/types';

  let { buckets, rows = [] }: {
    buckets: Array<TimelineSample | null>;
    rows?: SymbolRow[];
  } = $props();
  let viewMode = $state<'blocks' | 'sparkline'>('blocks');

  type TimelineDefinition = {
    key: string;
    label: string;
    scope: (sample: TimelineSample) => DashboardTimelineScope | undefined;
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
    { key: 'global', label: '全局', scope: (sample) => sample.sample.global },
    { key: 'subscribed', label: '订阅', scope: (sample) => sample.sample.subscribed },
    ...exchangeRows.map((exchange) => ({
      key: `exchange:${exchange}`,
      label: exchange,
      scope: (sample: TimelineSample) => sample.sample.exchanges[exchange],
      emptySeverity: latestSample?.sample.exchanges[exchange]?.severity ?? 'no_sample',
    })),
  ]);
  let pageRowCount = $derived(rows.length);

  function currentScope(definition: TimelineDefinition): DashboardTimelineScope | undefined {
    return latestSample ? definition.scope(latestSample) : undefined;
  }

  function scopeSummary(scope: DashboardTimelineScope | undefined): string {
    if (!scope) return '0/0';
    return `${formatNumber(scope.problem)}/${formatNumber(scope.total)}`;
  }

  function cellSeverity(definition: TimelineDefinition, sample: TimelineSample | null): TimelineSeverity {
    return sample ? (definition.scope(sample)?.severity ?? 'unknown') : (definition.emptySeverity ?? 'no_sample');
  }

  function cellClass(definition: TimelineDefinition, sample: TimelineSample | null) {
    const severity = cellSeverity(definition, sample);
    return severity === 'closed' ? 'unknown' : severity;
  }

  function latency(definition: TimelineDefinition, sample: TimelineSample): number {
    return definition.scope(sample)?.receive_gap_ms ?? 0;
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
    const scope = definition.scope(sample);
    if (!scope) return `${definition.label} 未知`;
    return `${definition.label} ${scope.severity} ${scopeSummary(scope)} ${formatDuration(scope.receive_gap_ms)}`;
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
      <div class="row-label" title={definition.label}>
        <span>{definition.label}</span>
        <em>{scopeSummary(currentScope(definition))}</em>
      </div>
      {#if viewMode === 'blocks'}
        {#each buckets as bucket, index (`${definition.key}:${index}`)}
          <span class={`cell ${cellClass(definition, bucket)}`} title={cellTitle(definition, bucket)}></span>
        {/each}
      {:else}
        <div class="sparkline-container" style="grid-column: 2 / -1;">
          <svg viewBox={`0 0 ${buckets.length * 10} 100`} preserveAspectRatio="none">
            <path d={sparklinePath(definition, buckets)} stroke={sparklineColor(definition, buckets)} stroke-width="2" fill="none" vector-effect="non-scaling-stroke"/>
          </svg>
        </div>
      {/if}
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
    --timeline-label-width: clamp(180px, 16vw, 240px);

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
    grid-template-columns: minmax(0, 1fr) auto;
    align-items: center;
    gap: 8px;
    overflow: hidden;
    color: #c3dbe6;
    font-size: 11px;
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

  @media (max-width: 900px) {
    .timeline {
      --timeline-label-width: 120px;
    }

    .legend {
      display: none;
    }
  }
</style>
