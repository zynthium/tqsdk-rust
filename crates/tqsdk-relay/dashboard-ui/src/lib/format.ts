const numberFormatter = new Intl.NumberFormat('zh-CN');
const rateFormatter = new Intl.NumberFormat('zh-CN', { maximumFractionDigits: 1 });
const timeFormatter = new Intl.DateTimeFormat('zh-CN', {
  hour12: false,
  timeZone: 'Asia/Shanghai',
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
});

export function formatNumber(value: number | null | undefined): string {
  return value == null || !Number.isFinite(value) ? '--' : numberFormatter.format(value);
}

export function formatRate(value: number | null | undefined): string {
  return value == null || !Number.isFinite(value) ? '--' : rateFormatter.format(value);
}

export function formatPercent(value: number | null | undefined): string {
  return value == null || !Number.isFinite(value) ? '--' : rateFormatter.format(value);
}

export function formatDuration(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return '--';
  const ms = Math.max(0, value);
  if (ms < 1_000) return `${Math.round(ms)}ms`;
  if (ms < 60_000) return `${(ms / 1_000).toFixed(1)}s`;
  return `${Math.floor(ms / 60_000)}m${Math.floor((ms % 60_000) / 1_000)}s`;
}

export function formatTime(unixMillis: number | null | undefined): string {
  if (unixMillis == null || !Number.isFinite(unixMillis)) return '--';
  return timeFormatter.format(new Date(unixMillis));
}
