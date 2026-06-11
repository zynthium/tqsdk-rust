<script lang="ts">
  import { formatDuration, formatNumber, formatPercent } from '../lib/format';
  import type { IntegrityModel } from '../lib/types';
  import ScoreGauge from './ScoreGauge.svelte';

  let { model }: { model: IntegrityModel } = $props();
  let title = $derived(
    model.overall === 'critical'
      ? '订阅链路告警'
      : model.overall === 'warning'
        ? '行情静默预警'
        : model.overall === 'warming'
          ? '启动观测中'
          : '行情链路连续',
  );
  let tone = $derived(
    model.overall === 'critical'
      ? 'error'
      : model.overall === 'warning'
        ? 'warning'
        : model.overall === 'warming'
          ? 'standby'
          : 'live',
  );
  let icon = $derived(model.overall === 'critical' ? '!' : model.overall === 'warning' ? '!' : model.overall === 'warming' ? '…' : '✓');
  let subtitle = $derived(
    `${formatNumber(model.observedUniverse)}/${formatNumber(model.totalUniverse)} 合约有接收记录，覆盖 ${formatPercent(model.coverageRatio * 100)}%，上游静默 ${formatDuration(model.upstreamIdleMs)}`,
  );
</script>

<section class={`panel hero ${tone}`} data-testid="integrity-hero">
  <div class="orb"><span class="shield">{icon}</span></div>
  <div class="copy">
    <h2>{title}</h2>
    <p><b>{formatNumber(model.issueCount)}</b> 个异常 · {subtitle}</p>
  </div>
  <div class="ecg" aria-hidden="true">
    <svg viewBox="0 0 190 58">
      <polyline points="0,31 72,31 82,24 90,38 98,8 106,50 115,20 124,31 190,31"></polyline>
    </svg>
  </div>
  <div class="score">
    <ScoreGauge score={model.continuityScore} compact />
  </div>
</section>

<style>
  .hero {
    width: 100%;
    min-height: 132px;
    justify-self: stretch;
    display: grid;
    grid-template-columns: 112px minmax(0, 1fr) minmax(170px, 220px) 128px;
    align-items: center;
    gap: 18px;
    padding: 10px 22px;
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

  .orb {
    position: relative;
    width: 78px;
    height: 78px;
    display: grid;
    place-items: center;
    margin: auto;
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
    inset: 8px;
  }

  .orb::after {
    inset: 20px;
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
    font-size: 31px;
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

  h2 {
    margin: 0;
    color: #9cff7b;
    font-size: clamp(24px, 2vw, 34px);
    letter-spacing: 2px;
    text-shadow: 0 0 16px #6aff825c;
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

  p {
    margin: 8px 0 0;
    overflow: hidden;
    color: #c9e9d8;
    font-size: 13px;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  p b {
    color: #8dff82;
    font-size: 16px;
  }

  .ecg svg {
    width: 100%;
    height: 58px;
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

  .score {
    position: relative;
    z-index: 1;
    display: grid;
    place-items: center;
  }
</style>
