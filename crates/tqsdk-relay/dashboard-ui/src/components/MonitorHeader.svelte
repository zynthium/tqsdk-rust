<script lang="ts">
  import { onMount } from 'svelte';
  import { formatTime } from '../lib/format';
  import type { IntegrityModel } from '../lib/types';
  import type { Snippet } from 'svelte';

  type Props = {
    model: IntegrityModel | null;
    error: string | null;
    paused: boolean;
    fullscreen: boolean;
    children?: Snippet;
  };

  let { model, error, paused = $bindable(false), fullscreen = $bindable(false), children }: Props = $props();
  let now = $state(Date.now());
  let fullscreenSupported = $state(true);
  let stateLabel = $derived(paused ? '已暂停' : error ? '读取异常' : model?.overall === 'closed' ? '休盘中' : '实时监控中');
  let stateClass = $derived(error ? 'bad' : paused ? 'closed' : model?.overall === 'closed' ? 'closed' : 'live');

  onMount(() => {
    fullscreenSupported = typeof document.documentElement.requestFullscreen === 'function';
    const syncFullscreen = () => {
      fullscreen = document.fullscreenElement != null;
    };
    document.addEventListener('fullscreenchange', syncFullscreen);
    syncFullscreen();
    return () => document.removeEventListener('fullscreenchange', syncFullscreen);
  });

  $effect(() => {
    const timer = window.setInterval(() => {
      now = Date.now();
    }, 1_000);
    return () => window.clearInterval(timer);
  });

  async function toggleFullscreen() {
    if (!fullscreenSupported) return;
    try {
      if (document.fullscreenElement) {
        await document.exitFullscreen();
      } else {
        await document.documentElement.requestFullscreen();
      }
      fullscreen = document.fullscreenElement != null;
    } catch {
      fullscreen = document.fullscreenElement != null;
    }
  }
</script>

<header class="header" data-testid="monitor-header">
  <div class="left">
    <span class="env">RELAY</span>
    <span>◉ Asia/Shanghai</span>
    <span>{formatTime(now)}</span>
  </div>
  <h1 class="brand">tqsdk-relay 行情完整性监控中心</h1>
  <div class="right">
    {#if model}
      <span class="muted">采样 {formatTime(model.sampledAt)}</span>
    {/if}
    <span class={`live-chip ${stateClass === 'bad' ? 'offline' : stateClass === 'closed' ? 'sleeping' : ''}`}>
      <span class={`status-dot ${stateClass}`}></span>
      <span>{stateLabel}</span>
    </span>
    {@render children?.()}
    <button type="button" onclick={() => (paused = !paused)}>{paused ? '继续' : '暂停'}</button>
    <button type="button" disabled={!fullscreenSupported} title={fullscreenSupported ? '切换全屏' : '当前浏览器不支持全屏'} onclick={toggleFullscreen}>
      {fullscreen ? '退出' : '全屏'}
    </button>
  </div>
</header>

{#if error}
  <div class="panel error-banner">{error}</div>
{/if}

<style>
  .header {
    position: relative;
    min-height: 44px;
    display: grid;
    grid-template-columns: 1fr auto 1fr;
    align-items: center;
    gap: 12px;
  }

  .header::after {
    content: "";
    position: absolute;
    right: 20%;
    bottom: 0;
    left: 20%;
    height: 1px;
    background: linear-gradient(90deg, transparent, var(--relay-info), transparent);
    box-shadow: 0 0 10px var(--relay-info);
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
    gap: 12px;
    color: var(--relay-muted);
    font-size: 12px;
    white-space: nowrap;
  }

  .right {
    justify-content: end;
  }

  .brand {
    text-shadow: 0 0 20px #50beff7a;
    white-space: nowrap;
  }

  .env,
  button {
    border: 1px solid var(--relay-line);
    border-radius: 7px;
    background: rgb(255 255 255 / 4%);
    color: var(--relay-text);
    font-weight: 750;
  }

  .env {
    padding: 5px 9px;
    border-color: #25ffbc80;
    background: #00ffb40d;
    color: #35ffc6;
    font-weight: 800;
  }

  .live-chip {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    padding: 7px 12px;
    border: 1px solid #45ff9a66;
    border-radius: 999px;
    background: #45ff9a0f;
    color: #ccffe1;
  }

  .live-chip.offline {
    border-color: #ff536a80;
    background: #ff536a14;
    color: #ffd2d8;
  }

  .live-chip.sleeping {
    border-color: #58758a80;
    background: #58758a14;
    color: #9eb9ce;
  }

  button {
    min-width: 58px;
    padding: 5px 9px;
    border-color: #2ad0ff55;
    background: #061a2b99;
    color: #aeeaff;
    cursor: pointer;
  }

  button:hover {
    border-color: var(--relay-info);
  }

  button:disabled {
    cursor: not-allowed;
    opacity: 0.48;
  }

  .muted {
    color: color-mix(in srgb, var(--relay-muted) 75%, transparent);
  }

  .error-banner {
    position: fixed;
    z-index: 20;
    top: 56px;
    left: 50%;
    width: min(760px, calc(100vw - 40px));
    transform: translateX(-50%);
    margin-top: 0;
    padding: 8px 12px;
    border-color: color-mix(in srgb, var(--relay-bad) 70%, transparent);
    background: #290812f5;
    color: var(--relay-bad);
    font-size: 13px;
  }
</style>
