<script lang="ts">
  import AttentionList from './components/AttentionList.svelte';
  import ContinuityTimeline from './components/ContinuityTimeline.svelte';
  import IncidentTable from './components/IncidentTable.svelte';
  import IntegrityHero from './components/IntegrityHero.svelte';
  import IntegrityTrend from './components/IntegrityTrend.svelte';
  import MetricCard from './components/MetricCard.svelte';
  import MonitorHeader from './components/MonitorHeader.svelte';
  import RelayPipeline from './components/RelayPipeline.svelte';
  import { untrack } from 'svelte';
  import { fetchRelaySnapshot } from './lib/api';
  import { createIncidentLedger, updateIncidentLedger } from './lib/incident-ledger';
  import { createHistory, pushHistorySample } from './lib/history';
  import { deriveIntegrity } from './lib/integrity-model';
  import { createTimelineHistory, pushTimelineSample, timelineBuckets, timelineRowsForSnapshot } from './lib/timeline';
  import type { IntegrityModel, RelaySnapshot, SymbolRow } from './lib/types';

  const POLL_INTERVAL_MS = 2_000;

  let snapshot = $state<RelaySnapshot | null>(null);
  let model = $state<IntegrityModel | null>(null);
  let timelineRows = $state<SymbolRow[]>([]);
  let timeline = $state(createTimelineHistory());
  let history = $state(createHistory());
  let incidents = $state(createIncidentLedger());
  let error = $state<string | null>(null);
  let sequence = $state(0);
  let timelineHistoryLoaded = $state(false);
  let view = $state({
    paused: false,
    fullscreen: false,
  });

  let buckets = $derived(timelineBuckets(timeline, snapshot?.receivedAt ?? Date.now(), 60));

  async function load(signal?: AbortSignal) {
    const requestId = sequence + 1;
    sequence = requestId;
    const includeTimelineHistory = !timelineHistoryLoaded;
    const next = await fetchRelaySnapshot(signal, { includeTimelineHistory });
    if (requestId !== sequence) return;
    snapshot = next;
    const nextTimelineRows = timelineRowsForSnapshot(next);
    const nextModel = deriveIntegrity(next.metrics, next.page, next.receivedAt, model, next.global);
    if (includeTimelineHistory) {
      timelineHistoryLoaded = true;
    }
    if (next.timelineHistory) {
      timeline = next.timelineHistory;
    } else {
      pushTimelineSample(timeline, next.timeline, next.receivedAt, nextTimelineRows);
    }
    timelineRows = nextTimelineRows;
    pushHistorySample(history, nextModel);
    updateIncidentLedger(incidents, nextModel);
    model = nextModel;
    error = null;
  }

  $effect(() => {
    if (view.paused) return;
    const controller = new AbortController();
    let disposed = false;
    let timer: number | undefined;
    async function poll() {
      try {
        await untrack(() => load(controller.signal));
      } catch (reason) {
        if (!controller.signal.aborted) error = reason instanceof Error ? reason.message : String(reason);
      }
      if (!disposed && !controller.signal.aborted) {
        timer = window.setTimeout(poll, POLL_INTERVAL_MS);
      }
    }
    void poll();
    return () => {
      disposed = true;
      controller.abort();
      if (timer != null) window.clearTimeout(timer);
    };
  });
</script>

<main class="dashboard-shell" data-fullscreen={view.fullscreen}>
  <MonitorHeader {model} {error} bind:paused={view.paused} bind:fullscreen={view.fullscreen} />

  {#if model}
    <IntegrityHero {model} />
    <section class="pipeline-kpi-row">
      <div class="kpi-strip">
        <MetricCard label="上游帧流" value={model.frameRate} unit="/s" tone="info" format="rate" icon="⌁" />
        <MetricCard label="有效事件" value={model.eventRate} unit="/s" tone="accent" format="rate" icon="▥" />
        <MetricCard label="下游客户端" value={model.metrics.downstream_clients} tone="info" icon="▤" />
      </div>
      <RelayPipeline {model} />
    </section>
    <section class="dashboard-main">
      <div class="main-left">
        <ContinuityTimeline {buckets} rows={timelineRows} />
      </div>
      <div class="main-right">
        <AttentionList rows={model.problems} />
        <IntegrityTrend {history} {model} />
        <IncidentTable incidents={incidents.incidents} />
      </div>
    </section>
  {:else}
    <section class="panel grid min-h-[280px] place-content-center text-center text-[var(--relay-muted)]">
      正在读取 relay 观测数据
    </section>
  {/if}
</main>
