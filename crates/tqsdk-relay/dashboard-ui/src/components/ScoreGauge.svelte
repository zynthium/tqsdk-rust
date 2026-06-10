<script lang="ts">
  import { formatNumber } from '../lib/format';

  let { score }: { score: number } = $props();
  let clamped = $derived(Math.max(0, Math.min(100, score)));
  let tone = $derived(clamped < 60 ? 'bad' : clamped < 85 ? 'warn' : 'live');
</script>

<div class={`gauge ${tone}`} style={`--angle:${clamped * 3.6}deg`} data-testid="score-gauge">
  <div class="inner">
    <span>连续性评分</span>
    <b>{formatNumber(Math.round(clamped))}</b>
  </div>
</div>

<style>
  .gauge {
    width: 136px;
    height: 136px;
    display: grid;
    place-items: center;
    border-radius: 50%;
    background: conic-gradient(var(--relay-live) var(--angle), rgb(255 255 255 / 8%) 0);
  }

  .gauge.warn {
    background: conic-gradient(var(--relay-warn) var(--angle), rgb(255 255 255 / 8%) 0);
  }

  .gauge.bad {
    background: conic-gradient(var(--relay-bad) var(--angle), rgb(255 255 255 / 8%) 0);
  }

  .inner {
    width: 108px;
    height: 108px;
    display: grid;
    place-content: center;
    border-radius: 50%;
    background: var(--relay-panel-soft);
    text-align: center;
  }

  span {
    color: var(--relay-muted);
    font-size: 11px;
  }

  b {
    color: var(--relay-text);
    font-size: 28px;
    line-height: 1.1;
  }
</style>
