<script lang="ts">
  import { formatDuration, formatNumber, formatPercent } from '../lib/format';
  import type { IntegrityModel } from '../lib/types';

  let { model }: { model: IntegrityModel } = $props();

  function heroTitle(model: IntegrityModel): string {
    if (model.overall === 'critical') return '订阅链路告警';
    if (model.overall === 'warning') return '行情静默预警';
    if (model.overall === 'warming') return '启动观测中';
    if (model.overall === 'closed') return '全市场休盘';
    return '行情链路连续';
  }

  let title = $derived(heroTitle(model));
  let tone = $derived(
    model.overall === 'critical'
      ? 'error'
      : model.overall === 'warning'
        ? 'warning'
        : model.overall === 'warming'
          ? 'standby'
          : model.overall === 'closed'
            ? 'closed'
            : 'live',
  );
  let icon = $derived(
    model.overall === 'critical' ? '!' : model.overall === 'warning' ? '!' : model.overall === 'warming' ? '…' : model.overall === 'closed' ? '☾' : '✓',
  );

  const chipValueToneClass = {
    live: 'text-[color:var(--relay-text)]',
    warn: 'text-[color:var(--relay-warn)]',
    critical: 'text-[color:var(--relay-bad)]',
    no_sample: 'text-[color:var(--relay-text)]',
  } as const;

  function idleValue(model: IntegrityModel, idleMs: number | null): string {
    if (model.idleDisplayState === 'closed') return '--';
    if (model.idleDisplayState === 'subscribing') return '订阅中';
    if (model.idleDisplayState === 'backfilling') return '补历史';
    return formatDuration(idleMs);
  }

  function coverageSub(model: IntegrityModel): string {
    if (model.idleDisplayState === 'subscribing') return '订阅中';
    if (model.idleDisplayState === 'backfilling') {
      const initializing = Number(model.global.initializing || 0);
      return initializing > 0 ? `初始化 ${formatNumber(initializing)}` : '补历史';
    }
    return `${formatPercent(model.coverageRatio * 100)}%`;
  }
</script>

<section
  class={`hero panel-shell grid items-center gap-4 px-[22px] py-3 [grid-template-columns:auto_minmax(0,1fr)_minmax(140px,180px)] ${tone}`}
  data-testid="integrity-hero"
>
  <div class="flex shrink-0 items-center gap-[14px]">
    <div class="orb"><span class="shield">{icon}</span></div>
    <div class="min-w-0">
      <h2>{title}</h2>
      <p class="issue-count"><b>{formatNumber(model.issueCount)}</b> 个异常</p>
    </div>
  </div>
  <div class="flex min-w-0 flex-wrap items-stretch gap-1.5">
    <div class="flex min-w-20 flex-col gap-px rounded-[7px] border border-[#2ad0ff22] bg-[#071a2b99] px-[10px] py-[5px]">
      <span class="text-[10px] font-semibold tracking-[0.3px] whitespace-nowrap text-[#7ea8bc]">合约覆盖</span>
      <span class={`text-[16px] leading-[1.15] font-[850] whitespace-nowrap ${chipValueToneClass.live}`}>
        {formatNumber(model.observedUniverse)}<em class="ml-[0.28em] text-[11px] font-semibold not-italic text-[color:var(--relay-muted)]">/{formatNumber(model.totalUniverse)}</em>
      </span>
      <span class="text-[11px] font-[750] text-[color:var(--relay-live)]">{coverageSub(model)}</span>
    </div>
    <div class="flex min-w-20 flex-col gap-px rounded-[7px] border border-[#2ad0ff22] bg-[#071a2b99] px-[10px] py-[5px]">
      <span class="text-[10px] font-semibold tracking-[0.3px] whitespace-nowrap text-[#7ea8bc]">帧静默</span>
      <span class={`text-[16px] leading-[1.15] font-[850] whitespace-nowrap ${chipValueToneClass[model.effectiveFrameFlowHealth]}`}>{idleValue(model, model.upstreamIdleMs)}</span>
    </div>
    <div class="flex min-w-20 flex-col gap-px rounded-[7px] border border-[#2ad0ff22] bg-[#071a2b99] px-[10px] py-[5px]">
      <span class="text-[10px] font-semibold tracking-[0.3px] whitespace-nowrap text-[#7ea8bc]">事件静默</span>
      <span class={`text-[16px] leading-[1.15] font-[850] whitespace-nowrap ${chipValueToneClass[model.effectiveEventFlowHealth]}`}>{idleValue(model, model.eventIdleMs)}</span>
    </div>
    <div class="flex min-w-20 flex-col gap-px rounded-[7px] border border-[#2ad0ff22] bg-[#071a2b99] px-[10px] py-[5px]">
      <span class="text-[10px] font-semibold tracking-[0.3px] whitespace-nowrap text-[#7ea8bc]">Diff行号</span>
      <span class={`text-[16px] leading-[1.15] font-[850] whitespace-nowrap ${chipValueToneClass.live}`}>{formatNumber(model.diffRowDiscontinuityCount)}</span>
    </div>
    <div class="flex min-w-20 flex-col gap-px rounded-[7px] border border-[#2ad0ff22] bg-[#071a2b99] px-[10px] py-[5px]">
      <span class="text-[10px] font-semibold tracking-[0.3px] whitespace-nowrap text-[#7ea8bc]">估算缺失</span>
      <span class={`text-[16px] leading-[1.15] font-[850] whitespace-nowrap ${chipValueToneClass.live}`}>
        {formatNumber(model.estimatedMissingRows)}<em class="ml-[0.28em] text-[11px] font-semibold not-italic text-[color:var(--relay-muted)]"> 行</em>
      </span>
    </div>
    <div class="flex min-w-20 flex-col gap-px rounded-[7px] border border-[#2ad0ff22] bg-[#071a2b99] px-[10px] py-[5px]">
      <span class="text-[10px] font-semibold tracking-[0.3px] whitespace-nowrap text-[#7ea8bc]">倒序</span>
      <span class={`text-[16px] leading-[1.15] font-[850] whitespace-nowrap ${chipValueToneClass.live}`}>
        {formatNumber(model.outOfOrderRowCount)}<em class="ml-[0.28em] text-[11px] font-semibold not-italic text-[color:var(--relay-muted)]"> 行</em>
      </span>
    </div>
  </div>
  <div class="ecg" aria-hidden="true">
    <svg viewBox="0 0 190 58">
      <polyline points="0,31 72,31 82,24 90,38 98,8 106,50 115,20 124,31 190,31"></polyline>
    </svg>
  </div>
</section>

<style>
  .hero {
    width: 100%;
    justify-self: stretch;
    border-color: #45ff9a8c;
    background: linear-gradient(90deg, #062d26f5, #061e1df5);
    box-shadow:
      0 0 0 1px #45ff9a1a,
      0 0 28px #45ff9a29,
      inset 0 0 44px #45ff9a14;
  }

  .hero.warning {
    border-color: #ffc447ad;
    background: linear-gradient(90deg, #372708f5, #1f1b08f5);
  }

  .hero.error {
    border-color: #ff536ab8;
    background: linear-gradient(90deg, #390b16f5, #1f0a13f5);
  }

  .hero.standby {
    border-color: #42a7ff94;
    background: linear-gradient(90deg, #071e36f5, #071527f5);
  }

  .hero.closed {
    border-color: #58758a80;
    background: linear-gradient(90deg, #0d1e2df5, #071522f5);
  }

  .orb {
    position: relative;
    width: 62px;
    height: 62px;
    display: grid;
    flex-shrink: 0;
    place-items: center;
    border: 1px solid #45ff9a73;
    border-radius: 50%;
    background: radial-gradient(circle, #45ff9a2b, transparent 64%);
    box-shadow: 0 0 28px #45ff9a3d;
  }

  .orb::before,
  .orb::after {
    content: "";
    position: absolute;
    border: 1px dashed #45ff9a70;
    border-radius: 50%;
    animation: orbit 10s linear infinite;
  }

  .orb::before {
    inset: 6px;
  }

  .orb::after {
    inset: 16px;
    animation-direction: reverse;
    animation-duration: 7s;
  }

  @keyframes orbit {
    to {
      transform: rotate(360deg);
    }
  }

  .shield {
    color: var(--relay-live);
    font-size: 26px;
    filter: drop-shadow(0 0 8px currentColor);
  }

  .hero.warning .shield {
    color: var(--relay-warn);
  }

  .hero.error .shield {
    color: var(--relay-bad);
  }

  .hero.standby .shield {
    color: var(--relay-blue);
  }

  .hero.closed .shield {
    color: var(--relay-closed);
    font-size: 32px;
  }

  h2 {
    margin: 0;
    color: #9cff7b;
    font-size: clamp(18px, 1.6vw, 26px);
    font-weight: 850;
    letter-spacing: 1px;
    text-shadow: 0 0 16px #6aff825c;
    white-space: nowrap;
  }

  .hero.warning h2 {
    color: #ffd874;
  }

  .hero.error h2 {
    color: #ff8192;
  }

  .hero.standby h2 {
    color: #83c8ff;
  }

  .hero.closed h2 {
    color: #9eb9ce;
  }

  .issue-count {
    margin: 4px 0 0;
    color: #c9e9d8;
    font-size: 12px;
    white-space: nowrap;
  }

  .issue-count b {
    color: #8dff82;
    font-size: 15px;
  }

  .ecg svg {
    width: 100%;
    height: 48px;
    filter: drop-shadow(0 0 8px #45ff9a99);
  }

  .ecg polyline {
    fill: none;
    stroke: #45ff9a;
    stroke-width: 2;
  }

  .hero.warning .ecg polyline {
    stroke: var(--relay-warn);
  }

  .hero.error .ecg polyline {
    stroke: var(--relay-bad);
  }

  .hero.standby .ecg polyline {
    stroke: var(--relay-blue);
  }

  .hero.closed .ecg polyline {
    stroke: var(--relay-closed);
  }
</style>
