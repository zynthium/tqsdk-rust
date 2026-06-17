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
  function monitorStateLabel(model: IntegrityModel | null): string {
    if (!model) return '读取中';
    if (model.cacheHealth === 'interrupted') return '链路异常';
    if (model.idleDisplayState === 'closed') return '休盘中';
    if (model.idleDisplayState === 'subscribing') return '订阅中';
    if (model.idleDisplayState === 'backfilling') return '补历史';
    if (model.overall === 'warming') return '启动观测中';
    return '实时监控中';
  }

  function monitorStateClass(model: IntegrityModel | null): string {
    if (!model) return 'no_sample';
    if (model.cacheHealth === 'interrupted') return 'bad';
    if (model.idleDisplayState === 'closed') return 'closed';
    if (model.idleDisplayState === 'subscribing' || model.idleDisplayState === 'backfilling' || model.overall === 'warming') {
      return 'no_sample';
    }
    return 'live';
  }

  let stateLabel = $derived(paused ? '已暂停' : error ? '读取异常' : monitorStateLabel(model));
  let stateClass = $derived(error ? 'bad' : paused ? 'closed' : monitorStateClass(model));
  let liveChipClass = $derived(
    stateClass === 'bad'
      ? 'border-[#ff536a80] bg-[#ff536a14] text-[#ffd2d8]'
      : stateClass === 'closed'
        ? 'border-[#58758a80] bg-[#58758a14] text-[#9eb9ce]'
        : stateClass === 'no_sample'
          ? 'border-[#4d789080] bg-[#4d789014] text-[#b8c8d3]'
          : 'border-[#45ff9a66] bg-[#45ff9a0f] text-[#ccffe1]'
  );

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

<header class="relative grid min-h-11 grid-cols-[1fr_auto_1fr] items-center gap-3" data-testid="monitor-header">
  <div class="flex items-center gap-3 whitespace-nowrap text-xs text-[var(--relay-muted)]">
    <span class="rounded-[7px] border border-[#25ffbc80] bg-[#00ffb40d] px-[9px] py-[5px] font-extrabold text-[#35ffc6]">RELAY</span>
    <span>◉ Asia/Shanghai</span>
    <span>{formatTime(now)}</span>
  </div>
  <h1 class="m-0 whitespace-nowrap text-[clamp(20px,1.7vw,28px)] font-black tracking-normal text-shadow-[0_0_20px_#50beff7a]">
    中继行情监控中心
  </h1>
  <div class="flex items-center justify-end gap-3 whitespace-nowrap text-xs text-[var(--relay-muted)]">
    {#if model}
      <span class="text-[color:color-mix(in_srgb,var(--relay-muted)_75%,transparent)]">
        采样 {formatTime(model.sampledAt)}
      </span>
    {/if}
    <span class={`inline-flex items-center gap-2 rounded-full border px-3 py-[7px] ${liveChipClass}`}>
      <span class={`status-dot ${stateClass}`}></span>
      <span>{stateLabel}</span>
    </span>
    {@render children?.()}
    <button
      type="button"
      class="min-w-[58px] cursor-pointer rounded-[7px] border border-[#2ad0ff55] bg-[#061a2b99] px-[9px] py-[5px] font-bold text-[#aeeaff] hover:border-[var(--relay-info)] disabled:cursor-not-allowed disabled:opacity-48"
      onclick={() => (paused = !paused)}
    >
      {paused ? '继续' : '暂停'}
    </button>
    <button
      type="button"
      class="min-w-[58px] cursor-pointer rounded-[7px] border border-[#2ad0ff55] bg-[#061a2b99] px-[9px] py-[5px] font-bold text-[#aeeaff] hover:border-[var(--relay-info)] disabled:cursor-not-allowed disabled:opacity-48"
      disabled={!fullscreenSupported}
      title={fullscreenSupported ? '切换全屏' : '当前浏览器不支持全屏'}
      onclick={toggleFullscreen}
    >
      {fullscreen ? '退出' : '全屏'}
    </button>
  </div>
  <div
    class="pointer-events-none absolute right-[20%] bottom-0 left-[20%] h-px bg-[linear-gradient(90deg,transparent,var(--relay-info),transparent)] shadow-[0_0_10px_var(--relay-info)]"
    aria-hidden="true"
  ></div>
</header>

{#if error}
  <div
    class="panel fixed top-14 left-1/2 z-20 mt-0 w-[min(760px,calc(100vw-40px))] -translate-x-1/2 border-[color:color-mix(in_srgb,var(--relay-bad)_70%,transparent)] bg-[#290812f5] px-3 py-2 text-[13px] text-[var(--relay-bad)]"
  >
    {error}
  </div>
{/if}
