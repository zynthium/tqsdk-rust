<script lang="ts">
  import { formatTime } from '../lib/format';
  import type { IntegrityModel } from '../lib/types';

  type Props = {
    model: IntegrityModel | null;
    error: string | null;
    paused: boolean;
    fullscreen: boolean;
  };

  let { model, error, paused = $bindable(false), fullscreen = $bindable(false) }: Props = $props();
  let now = $state(Date.now());
  let stateLabel = $derived(paused ? '已暂停' : error ? '读取异常' : '实时监控中');
  let stateClass = $derived(error ? 'bad' : paused ? 'closed' : 'live');

  $effect(() => {
    const timer = window.setInterval(() => {
      now = Date.now();
    }, 1_000);
    return () => window.clearInterval(timer);
  });
</script>

<header class="panel header" data-testid="monitor-header">
  <div class="left">
    <span class="env">RELAY</span>
    <span>Asia/Shanghai</span>
    <span>{formatTime(now)}</span>
  </div>
  <h1>tqsdk-relay 行情完整性监控中心</h1>
  <div class="right">
    {#if model}
      <span class="muted">采样 {formatTime(model.sampledAt)}</span>
    {/if}
    <span class={`status-dot ${stateClass}`}></span>
    <span>{stateLabel}</span>
    <button type="button" onclick={() => (paused = !paused)}>{paused ? '继续' : '暂停'}</button>
    <button type="button" onclick={() => (fullscreen = !fullscreen)}>{fullscreen ? '退出全屏' : '全屏'}</button>
  </div>
</header>

{#if error}
  <div class="panel error-banner">{error}</div>
{/if}

<style>
  .header {
    min-height: 46px;
    display: grid;
    grid-template-columns: 1fr auto 1fr;
    align-items: center;
    gap: 12px;
    padding: 8px 12px;
  }

  h1 {
    margin: 0;
    font-size: clamp(20px, 1.7vw, 28px);
    font-weight: 850;
    letter-spacing: 0;
  }

  .left,
  .right {
    display: flex;
    align-items: center;
    gap: 10px;
    color: var(--relay-muted);
    font-size: 12px;
    white-space: nowrap;
  }

  .right {
    justify-content: end;
  }

  .env,
  button {
    border: 1px solid var(--relay-line);
    border-radius: 6px;
    background: rgb(255 255 255 / 4%);
    color: var(--relay-text);
    font-weight: 750;
  }

  .env {
    padding: 4px 8px;
    color: var(--relay-live);
  }

  button {
    min-width: 58px;
    padding: 5px 9px;
    cursor: pointer;
  }

  button:hover {
    border-color: var(--relay-info);
  }

  .muted {
    color: color-mix(in srgb, var(--relay-muted) 75%, transparent);
  }

  .error-banner {
    margin-top: -2px;
    padding: 8px 12px;
    border-color: color-mix(in srgb, var(--relay-bad) 70%, transparent);
    color: var(--relay-bad);
    font-size: 13px;
  }
</style>
