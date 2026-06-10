<script lang="ts">
  import type { DashboardFilters, SymbolSort, SymbolStatus } from '../lib/types';

  type Props = {
    filters: DashboardFilters;
    disabled: boolean;
    onrefresh: () => void | Promise<void>;
  };

  const statuses: Array<{ value: SymbolStatus; label: string }> = [
    { value: 'live', label: '正常' },
    { value: 'closed', label: '休盘' },
    { value: 'stale', label: '静默' },
    { value: 'missing', label: '未收到' },
    { value: 'inactive', label: '未纳入' },
  ];

  const sorts: Array<{ value: SymbolSort; label: string }> = [
    { value: 'receive_gap_ms_desc', label: '接收延迟' },
    { value: 'market_time_lag_ms_desc', label: '行情时间延迟' },
    { value: 'ticks_ingested_desc', label: 'Tick 累计' },
    { value: 'status_asc', label: '状态' },
    { value: 'symbol_asc', label: '合约' },
  ];

  let { filters = $bindable(), disabled, onrefresh }: Props = $props();

  function toggleStatus(status: SymbolStatus, checked: boolean) {
    filters.statuses = checked
      ? [...new Set([...filters.statuses, status])]
      : filters.statuses.filter((item) => item !== status);
  }

  function checkboxValue(event: Event): boolean {
    return (event.currentTarget as HTMLInputElement).checked;
  }
</script>

<section class="panel controls" data-testid="dashboard-controls">
  <div class="status-set" aria-label="status filters">
    {#each statuses as status}
      <label>
        <input
          type="checkbox"
          checked={filters.statuses.includes(status.value)}
          disabled={disabled}
          onchange={(event) => toggleStatus(status.value, checkboxValue(event))}
        />
        <span>{status.label}</span>
      </label>
    {/each}
  </div>
  <label class="toggle">
    <input type="checkbox" bind:checked={filters.subscribedOnly} disabled={disabled} />
    <span>只看订阅</span>
  </label>
  <input class="search" bind:value={filters.q} disabled={disabled} placeholder="搜索合约或中文名" />
  <select bind:value={filters.sort} disabled={disabled}>
    {#each sorts as sort}
      <option value={sort.value}>{sort.label}</option>
    {/each}
  </select>
  <select bind:value={filters.limit} disabled={disabled}>
    {#each [50, 100, 200, 500] as limit}
      <option value={limit}>{limit}</option>
    {/each}
  </select>
  <button type="button" disabled={disabled} onclick={() => onrefresh()}>刷新</button>
</section>

<style>
  .controls {
    min-height: 42px;
    display: grid;
    grid-template-columns: auto auto minmax(180px, 1fr) 150px 90px 72px;
    align-items: center;
    gap: 8px;
    padding: 7px 10px;
  }

  .status-set {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
  }

  label {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    color: var(--relay-muted);
    font-size: 12px;
    white-space: nowrap;
  }

  input,
  select,
  button {
    height: 28px;
    border: 1px solid var(--relay-line-soft);
    border-radius: 6px;
    background: rgb(255 255 255 / 4%);
    color: var(--relay-text);
  }

  .search {
    min-width: 0;
    padding: 0 9px;
  }

  select,
  button {
    padding: 0 8px;
  }

  button {
    cursor: pointer;
    font-weight: 750;
  }

  button:disabled,
  input:disabled,
  select:disabled {
    cursor: not-allowed;
    opacity: 0.55;
  }
</style>
