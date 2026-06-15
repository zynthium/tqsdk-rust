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
  let iconToneClass = $derived(
    tone === 'live'
      ? 'text-[var(--relay-live)]'
      : tone === 'warn'
        ? 'text-[var(--relay-warn)]'
        : tone === 'bad' || tone === 'accent'
          ? 'text-[var(--relay-accent)]'
          : 'text-[var(--relay-info)]'
  );
</script>

<article class={`panel grid min-w-[120px] grid-cols-[34px_1fr] items-center gap-2 px-[11px] py-2 ${tone}`}>
  <div
    class={`grid h-8 w-8 place-items-center rounded-full border border-current bg-[#20d8ff0d] text-[15px] shadow-[0_0_14px_currentColor] ${iconToneClass}`}
  >
    {icon}
  </div>
  <div class="min-w-0">
    <div class="whitespace-nowrap text-[11px] text-[#a7c0ce]">{label}</div>
    <div class="whitespace-nowrap text-[20px] leading-none font-black text-[var(--relay-text)]">
      {display}<span class="ml-[3px] text-[11px] text-[var(--relay-muted)]">{unit}</span>
    </div>
  </div>
</article>
