<script lang="ts">
  import { formatDuration, formatNumber, formatPercent } from '../lib/format';
  import type { IntegrityModel } from '../lib/types';

  let { model }: { model: IntegrityModel } = $props();
  let title = $derived(
    model.overall === 'critical'
      ? '订阅受影响'
      : model.overall === 'warning'
        ? '需要关注'
        : model.overall === 'warming'
          ? '启动观测中'
          : '行情完整',
  );
  let tone = $derived(model.overall === 'critical' ? 'bad' : model.overall === 'warning' ? 'warn' : 'live');
  let subtitle = $derived(
    `${formatNumber(model.observedUniverse)}/${formatNumber(model.totalUniverse)} 合约有接收记录，覆盖 ${formatPercent(model.coverageRatio * 100)}%，上游静默 ${formatDuration(model.upstreamIdleMs)}`,
  );
</script>

<section class={`panel hero ${tone}`} data-testid="integrity-hero">
  <div>
    <span class={`status-dot ${tone}`}></span>
    <h2>{title}</h2>
    <p>{subtitle}</p>
  </div>
  <div class="summary">
    <span><b>{formatNumber(model.issueCount)}</b>异常</span>
    <span><b>{formatNumber(model.subscribedProblems.length)}</b>影响订阅</span>
    <span><b>{formatNumber(model.snapshot.summary.closed)}</b>休盘</span>
  </div>
</section>

<style>
  .hero {
    min-height: 92px;
    display: grid;
    grid-template-columns: 1fr auto;
    align-items: center;
    padding: 14px 20px;
    border-color: color-mix(in srgb, var(--relay-live) 60%, var(--relay-line));
  }

  .hero.warn {
    border-color: color-mix(in srgb, var(--relay-warn) 72%, var(--relay-line));
  }

  .hero.bad {
    border-color: color-mix(in srgb, var(--relay-bad) 72%, var(--relay-line));
  }

  h2 {
    display: inline-flex;
    margin: 0 0 6px 8px;
    color: var(--relay-text);
    font-size: clamp(24px, 2vw, 34px);
    letter-spacing: 0;
  }

  p {
    margin: 0;
    color: var(--relay-muted);
    font-size: 13px;
  }

  .summary {
    display: flex;
    gap: 12px;
  }

  .summary span {
    min-width: 92px;
    border: 1px solid var(--relay-line-soft);
    border-radius: 8px;
    padding: 8px 10px;
    background: rgb(255 255 255 / 4%);
    color: var(--relay-muted);
    text-align: center;
    font-size: 12px;
  }

  .summary b {
    display: block;
    color: var(--relay-text);
    font-size: 23px;
    line-height: 1.15;
  }
</style>
