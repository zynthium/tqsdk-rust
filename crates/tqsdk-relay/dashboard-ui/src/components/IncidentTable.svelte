<script lang="ts">
  import { formatTime } from '../lib/format';
  import type { LocalIncident } from '../lib/types';

  let { incidents }: { incidents: LocalIncident[] } = $props();
</script>

<section class="panel incidents" data-testid="incident-table">
  <div class="panel-title">断流 / 覆盖事件</div>
  <table class="table">
    <thead>
      <tr>
        <th>时间</th>
        <th>范围</th>
        <th>类型</th>
        <th>详情</th>
        <th>影响</th>
      </tr>
    </thead>
    <tbody>
      {#if incidents.length === 0}
        <tr><td colspan="5" class="empty-cell">本页尚未观测到状态变化</td></tr>
      {:else}
        {#each incidents.slice(0, 8) as incident}
          <tr>
            <td>{formatTime(incident.at)}</td>
            <td title={incident.scope_symbol}>{incident.scope}</td>
            <td><span class={`badge ${incident.severity}`}>{incident.type}</span></td>
            <td title={incident.detail}>{incident.detail}</td>
            <td>{incident.impact}</td>
          </tr>
        {/each}
      {/if}
    </tbody>
  </table>
</section>

<style>
  .incidents {
    padding: 10px 12px;
  }

  th:nth-child(1) {
    width: 18%;
  }

  th:nth-child(2) {
    width: 22%;
  }

  th:nth-child(3) {
    width: 16%;
  }

  th:nth-child(5) {
    width: 16%;
  }

  .empty-cell {
    padding: 30px 8px;
    color: var(--relay-muted);
    text-align: center;
  }
</style>
