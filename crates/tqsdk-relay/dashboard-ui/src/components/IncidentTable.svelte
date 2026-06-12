<script lang="ts">
  import { formatTime } from '../lib/format';
  import type { LocalIncident } from '../lib/types';

  let { incidents }: { incidents: LocalIncident[] } = $props();
</script>

<section class="panel incidents" data-testid="incident-table">
  <div class="head">
    <div class="panel-title">状态变化事件</div>
    {#if incidents.length > 0}
      <span class="count">{incidents.length}</span>
    {/if}
  </div>
  {#if incidents.length === 0}
    <div class="empty-state">本页尚未观测到状态变化</div>
  {:else}
    <div class="event-list">
      {#each incidents.slice(0, 12) as incident}
        <div class={`event-item ${incident.severity}`}>
          <div class="event-top">
            <span class="event-time">{formatTime(incident.at)}</span>
            <span class={`badge ${incident.severity}`}>{incident.type}</span>
            <span class="event-impact">{incident.impact}</span>
          </div>
          <div class="event-scope" title={incident.scope_symbol}>{incident.scope}</div>
          <div class="event-detail" title={incident.detail}>{incident.detail}</div>
        </div>
      {/each}
    </div>
  {/if}
</section>

<style>
  .incidents {
    padding: 10px 12px;
  }

  .head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 6px;
  }

  .count {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    min-width: 22px;
    height: 18px;
    border-radius: 9px;
    padding: 0 6px;
    background: #42a7ff22;
    color: var(--relay-blue);
    font-size: 11px;
    font-weight: 850;
  }

  .empty-state {
    position: relative;
    z-index: 1;
    padding: 20px 8px;
    color: var(--relay-muted);
    font-size: 11px;
    text-align: center;
    flex: 1;
  }

  .event-list {
    position: relative;
    z-index: 1;
    margin-top: 8px;
    display: grid;
    gap: 5px;
    flex: 1;
    min-height: 0;
    overflow-y: auto;
    padding-right: 4px;
    scrollbar-width: thin;
    scrollbar-color: color-mix(in srgb, var(--relay-line) 80%, transparent) transparent;
  }

  .event-item {
    border: 1px solid #24445a66;
    border-radius: 6px;
    padding: 6px 8px;
    background: #071929;
    font-size: 10px;
    line-height: 1.4;
  }

  .event-item.bad {
    border-color: #ff536a44;
  }

  .event-item.warn {
    border-color: #ffc44744;
  }

  .event-item.live {
    border-color: #45ff9a44;
  }

  .event-top {
    display: flex;
    align-items: center;
    gap: 6px;
  }

  .event-time {
    color: var(--relay-muted);
    font-size: 10px;
    font-variant-numeric: tabular-nums;
    white-space: nowrap;
  }

  .event-impact {
    margin-left: auto;
    flex-shrink: 0;
    color: #6e94a8;
    font-size: 9px;
    white-space: nowrap;
  }

  .event-scope {
    margin-top: 2px;
    overflow: hidden;
    color: #c6dbe5;
    font-weight: 700;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .event-detail {
    overflow: hidden;
    color: var(--relay-muted);
    text-overflow: ellipsis;
    white-space: nowrap;
  }
</style>
