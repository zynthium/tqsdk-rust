<script lang="ts">
  import { sparkPoints } from '../lib/history';
  import type { IntegrityModel, RuntimeHistory } from '../lib/types';
  import ScoreGauge from './ScoreGauge.svelte';

  let { history, model }: { history: RuntimeHistory; model: IntegrityModel } = $props();
  let recent = $derived(history.samples.slice(-60));
  let framePoints = $derived(sparkPoints(recent.map((sample) => sample.frameRate), 800, 145));
  let eventPoints = $derived(sparkPoints(recent.map((sample) => sample.eventRate), 800, 145));
  let scorePoints = $derived(sparkPoints(recent.map((sample) => sample.continuityScore), 800, 145));
  let hasTrend = $derived(recent.length >= 2);
  let average = $derived(
    recent.length === 0
      ? model.continuityScore
      : recent.reduce((sum, sample) => sum + sample.continuityScore, 0) / recent.length,
  );
</script>

<section class="panel trend" data-testid="integrity-trend">
  <div class="panel-title">完整性趋势</div>
  <div class="body">
    <div class="chart">
      <svg viewBox="0 0 800 145" preserveAspectRatio="none" aria-label="integrity trend">
        <polyline class="frame" points={framePoints}></polyline>
        <polyline class="event" points={eventPoints}></polyline>
        <polyline class="score" points={scorePoints}></polyline>
      </svg>
      {#if !hasTrend}
        <div class="empty">积累实时采样后显示趋势</div>
      {/if}
    </div>
    <div class="score-box">
      <ScoreGauge score={model.continuityScore} />
      <div class="average">本页平均 <b>{Math.round(average)}</b></div>
    </div>
  </div>
</section>

<style>
  .trend {
    padding: 10px 12px;
  }

  .body {
    margin-top: 9px;
    display: grid;
    grid-template-columns: 1fr 160px;
    gap: 12px;
    align-items: center;
  }

  .chart {
    position: relative;
    min-height: 165px;
    border-left: 1px solid var(--relay-line-soft);
    border-bottom: 1px solid var(--relay-line-soft);
    background:
      linear-gradient(rgb(255 255 255 / 5%) 1px, transparent 1px),
      linear-gradient(90deg, rgb(255 255 255 / 5%) 1px, transparent 1px);
    background-size: 100% 25%, 12.5% 100%;
  }

  svg {
    position: absolute;
    inset: 8px;
    width: calc(100% - 16px);
    height: calc(100% - 16px);
  }

  polyline {
    fill: none;
    stroke-width: 2;
  }

  .frame {
    stroke: var(--relay-info);
  }

  .event {
    stroke: var(--relay-accent);
  }

  .score {
    stroke: var(--relay-live);
  }

  .empty {
    position: absolute;
    inset: 0;
    display: grid;
    place-items: center;
    color: var(--relay-muted);
    font-size: 12px;
  }

  .score-box {
    display: grid;
    justify-items: center;
    gap: 8px;
  }

  .average {
    color: var(--relay-muted);
    font-size: 11px;
  }

  .average b {
    color: var(--relay-text);
    font-size: 16px;
  }
</style>
