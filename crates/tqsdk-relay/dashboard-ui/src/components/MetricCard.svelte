<script lang="ts">
  import { formatDuration, formatNumber, formatPercent, formatRate } from '../lib/format';

  type Tone = 'live' | 'warn' | 'bad' | 'info' | 'accent';
  type Format = 'number' | 'rate' | 'duration' | 'percent';

  let {
    label,
    value,
    unit = '',
    tone = 'info',
    format = 'number',
  }: {
    label: string;
    value: number | null;
    unit?: string;
    tone?: Tone;
    format?: Format;
  } = $props();

  let display = $derived(
    format === 'duration'
      ? formatDuration(value)
      : format === 'rate'
        ? formatRate(value)
        : format === 'percent'
          ? formatPercent(value)
          : formatNumber(value),
  );
</script>

<article class={`panel metric ${tone}`}>
  <div class="label">{label}</div>
  <div class="value">{display}<span>{unit}</span></div>
</article>

<style>
  .metric {
    min-height: 78px;
    display: grid;
    align-content: center;
    gap: 5px;
    padding: 11px 12px;
  }

  .metric::before {
    content: "";
    position: absolute;
    inset: 0 auto 0 0;
    width: 3px;
    background: var(--relay-info);
  }

  .metric.live::before {
    background: var(--relay-live);
  }

  .metric.warn::before {
    background: var(--relay-warn);
  }

  .metric.bad::before {
    background: var(--relay-bad);
  }

  .metric.accent::before {
    background: var(--relay-accent);
  }

  .label {
    color: var(--relay-muted);
    font-size: 12px;
  }

  .value {
    color: var(--relay-text);
    font-size: clamp(20px, 2vw, 28px);
    font-weight: 850;
    line-height: 1;
  }

  .value span {
    margin-left: 3px;
    color: var(--relay-muted);
    font-size: 12px;
  }
</style>
