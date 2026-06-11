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

<details class="panel controls" data-testid="dashboard-controls">
  <summary>筛选</summary>
  <div class="control-grid">
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
  </div>
</details>

<style>
  .controls {
    position: absolute;
    top: 52px;
    right: 12px;
    z-index: 6;
    width: 84px;
    min-height: 34px;
    overflow: visible;
    border-color: #45ff9a66;
    border-radius: 999px;
    background: #061a2be6;
  }

  .controls[open] {
    width: min(760px, calc(100vw - 24px));
    border-radius: 10px;
    background: linear-gradient(180deg, #081b2df7, #040f1bf7);
  }

  summary {
    position: relative;
    z-index: 1;
    display: flex;
    align-items: center;
    justify-content: center;
    min-height: 32px;
    cursor: pointer;
    color: #ccffe1;
    font-size: 12px;
    font-weight: 800;
    list-style: none;
  }

  summary::-webkit-details-marker {
    display: none;
  }

  summary::before {
    content: "";
    width: 8px;
    height: 8px;
    margin-right: 6px;
    display: inline-block;
    border-radius: 50%;
    background: var(--relay-live);
    box-shadow: 0 0 12px var(--relay-live);
  }

  .control-grid {
    position: relative;
    z-index: 1;
    display: grid;
    grid-template-columns: auto auto minmax(180px, 1fr) 150px 90px 72px;
    align-items: center;
    gap: 8px;
    padding: 0 10px 9px;
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
    color: #a7c0ce;
    font-size: 12px;
    white-space: nowrap;
  }

  input,
  select,
  button {
    height: 28px;
    border: 1px solid #2ad0ff6b;
    border-radius: 6px;
    background: #071929;
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
