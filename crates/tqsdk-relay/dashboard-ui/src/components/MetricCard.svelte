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
    icon,
  }: {
    label: string;
    value: number | null;
    unit?: string;
    tone?: Tone;
    format?: Format;
    icon: string;
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
  <div class="icon">{icon}</div>
  <div class="body">
    <div class="label">{label}</div>
    <div class="value">{display}<span>{unit}</span></div>
  </div>
</article>

<style>
  .metric {
    min-width: 120px;
    display: grid;
    grid-template-columns: 34px 1fr;
    align-items: center;
    gap: 8px;
    padding: 8px 11px;
  }

  .icon {
    width: 32px;
    height: 32px;
    display: grid;
    place-items: center;
    border: 1px solid currentColor;
    border-radius: 50%;
    background: #20d8ff0d;
    box-shadow: 0 0 14px currentColor;
    color: var(--relay-info);
    font-size: 15px;
  }

  .metric.live .icon {
    color: var(--relay-live);
  }

  .metric.warn .icon {
    color: var(--relay-warn);
  }

  .metric.bad .icon,
  .metric.accent .icon {
    color: var(--relay-accent);
  }

  .label {
    color: #a7c0ce;
    font-size: 11px;
    white-space: nowrap;
  }

  .value {
    color: var(--relay-text);
    font-size: 20px;
    font-weight: 850;
    line-height: 1;
    white-space: nowrap;
  }

  .value span {
    margin-left: 3px;
    color: var(--relay-muted);
    font-size: 11px;
  }
</style>
