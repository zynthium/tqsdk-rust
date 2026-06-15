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

<section class="panel-shell flex flex-col px-3 py-2.5" data-testid="integrity-trend">
  <div class="flex items-center justify-between gap-2">
    <div class="panel-title">完整性趋势</div>
    <div class="flex gap-2 text-[9px] text-[color:var(--relay-muted)]">
      <span class="inline-flex items-center gap-[3px]"><i class="ln-frame"></i>帧流</span>
      <span class="inline-flex items-center gap-[3px]"><i class="ln-event"></i>事件</span>
      <span class="inline-flex items-center gap-[3px]"><i class="ln-score"></i>评分</span>
    </div>
  </div>
  <div class="relative z-[1] mt-[5px] grid h-[120px] items-center gap-2 [grid-template-columns:minmax(0,1fr)_100px]">
    <div class="trend-chart relative h-full border-l border-b border-[#30556d66]">
      <svg viewBox="0 0 600 100" preserveAspectRatio="none" aria-label="integrity trend">
        <polyline class="frame" points={framePoints}></polyline>
        <polyline class="event" points={eventPoints}></polyline>
        <polyline class="score" points={scorePoints}></polyline>
      </svg>
      {#if !hasTrend}
        <div class="empty">积累实时采样后显示趋势</div>
      {/if}
    </div>
    <div class="grid justify-items-center gap-1">
      <ScoreGauge score={model.continuityScore} state={model.overall} compact />
      <div class="text-[10px] text-[#7e9eae]">页均 <b class="text-[14px] text-[color:var(--relay-live)]">{Math.round(average)}</b></div>
    </div>
  </div>
</section>

<style>
  i {
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

  .trend-chart {
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
</style>
