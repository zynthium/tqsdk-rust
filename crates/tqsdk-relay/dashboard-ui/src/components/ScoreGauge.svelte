<script lang="ts">
  import { formatNumber } from '../lib/format';

  let { score, compact = false }: { score: number; compact?: boolean } = $props();
  let clamped = $derived(Math.max(0, Math.min(100, score)));
  let tone = $derived(clamped < 60 ? 'bad' : clamped < 85 ? 'warn' : 'live');
</script>

<div class={`gauge ${tone} ${compact ? 'compact' : ''}`} style={`--angle:${clamped * 3.6}deg`} data-testid="score-gauge">
  <div class="inner">
    <span>连续性</span>
    <b>{formatNumber(Math.round(clamped))}</b>
    <em>{tone === 'bad' ? '告警' : tone === 'warn' ? '关注' : '连续'}</em>
  </div>
</div>

<style>
  .gauge {
    width: 130px;
    height: 130px;
    display: grid;
    place-items: center;
    border-radius: 50%;
    background: conic-gradient(var(--relay-live) var(--angle), #143346 0);
    box-shadow: 0 0 24px #45ff9a2e;
  }

  .gauge.warn {
    background: conic-gradient(var(--relay-warn) var(--angle), #143346 0);
  }

  .gauge.bad {
    background: conic-gradient(var(--relay-bad) var(--angle), #143346 0);
  }

  .gauge.compact {
    width: 96px;
    height: 96px;
  }

  .inner {
    width: 102px;
    height: 102px;
    display: grid;
    place-content: center;
    border-radius: 50%;
    background: #061523;
    box-shadow: inset 0 0 20px #0008;
    text-align: center;
  }

  .compact .inner {
    width: 74px;
    height: 74px;
  }

  span {
    color: var(--relay-muted);
    font-size: 10px;
  }

  b {
    color: var(--relay-text);
    font-size: 24px;
    line-height: 1.1;
  }

  .compact b {
    font-size: 20px;
  }

  em {
    color: var(--relay-live);
    font-size: 11px;
    font-style: normal;
    font-weight: 800;
  }

  .gauge.warn em {
    color: var(--relay-warn);
  }

  .gauge.bad em {
    color: var(--relay-bad);
  }
</style>
