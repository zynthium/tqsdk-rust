<script lang="ts">
  import { sparkPoints } from '../lib/history';
  import type { IntegrityModel, RuntimeHistory } from '../lib/types';
  import ScoreGauge from './ScoreGauge.svelte';

  let { history, model }: { history: RuntimeHistory; model: IntegrityModel } = $props();
  let recent = $derived(history.samples.slice(-60));
  let framePoints = $derived(sparkPoints(recent.map((sample) => sample.frameRate), 600, 100));
  let eventPoints = $derived(sparkPoints(recent.map((sample) => sample.eventRate), 600, 100));
  let scorePoints = $derived(sparkPoints(recent.map((sample) => sample.continuityScore), 600, 100));
  let hasTrend = $derived(recent.length >= 2);
  let average = $derived(
    recent.length === 0
      ? model.continuityScore
      : recent.reduce((sum, sample) => sum + sample.continuityScore, 0) / recent.length,
  );
</script>

<section class="panel trend" data-testid="integrity-trend">
  <div class="head">
    <div class="panel-title">完整性趋势</div>
    <div class="legend">
      <span><i class="ln-frame"></i>帧流</span>
      <span><i class="ln-event"></i>事件</span>
      <span><i class="ln-score"></i>评分</span>
    </div>
  </div>
  <div class="body">
    <div class="chart">
      <svg viewBox="0 0 600 100" preserveAspectRatio="none" aria-label="integrity trend">
        <polyline class="frame" points={framePoints}></polyline>
        <polyline class="event" points={eventPoints}></polyline>
        <polyline class="score" points={scorePoints}></polyline>
      </svg>
      {#if !hasTrend}
        <div class="empty">积累实时采样后显示趋势</div>
      {/if}
    </div>
    <div class="score-box">
      <ScoreGauge score={model.continuityScore} compact />
      <div class="average">页均 <b>{Math.round(average)}</b></div>
    </div>
  </div>
</section>

<style>
  .trend {
    padding: 10px 12px;
  }

  .head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 8px;
  }

  .legend {
    display: flex;
    gap: 8px;
    color: var(--relay-muted);
    font-size: 9px;
  }

  .legend span {
    display: inline-flex;
    align-items: center;
    gap: 3px;
  }

  .legend i {
    width: 14px;
    height: 2px;
    border-radius: 1px;
  }

  .ln-frame {
    background: var(--relay-blue);
  }

  .ln-event {
    background: var(--relay-accent);
  }

  .ln-score {
    background: var(--relay-live);
  }

  .body {
    position: relative;
    z-index: 1;
    height: 120px;
    margin-top: 5px;
    display: grid;
    grid-template-columns: 1fr 100px;
    gap: 8px;
    align-items: center;
  }

  .chart {
    position: relative;
    height: 100%;
    border-left: 1px solid #30556d66;
    border-bottom: 1px solid #30556d66;
    background:
      linear-gradient(#1c496123 1px, transparent 1px),
      linear-gradient(90deg, #1c496123 1px, transparent 1px);
    background-size: 100% 25%, 12.5% 100%;
  }

  svg {
    position: absolute;
    inset: 6px;
    width: calc(100% - 12px);
    height: calc(100% - 12px);
  }

  polyline {
    fill: none;
    stroke-width: 1.5;
  }

  .frame {
    stroke: var(--relay-blue);
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
    color: #66899b;
    font-size: 10px;
  }

  .score-box {
    display: grid;
    justify-items: center;
    gap: 4px;
  }

  .average {
    color: #7e9eae;
    font-size: 10px;
  }

  .average b {
    color: var(--relay-live);
    font-size: 14px;
  }
</style>
