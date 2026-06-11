<script lang="ts">
  import { formatDuration, formatNumber } from '../lib/format';
  import { statusLabel } from '../lib/integrity-model';
  import type { SymbolRow } from '../lib/types';

  let { rows, selectedSymbol = $bindable(null) }: {
    rows: SymbolRow[];
    selectedSymbol: string | null;
  } = $props();
  let ordered = $derived(
    [...rows]
      .sort((left, right) => {
        const leftRank = severityRank(left);
        const rightRank = severityRank(right);
        return leftRank - rightRank || (right.receive_gap_ms ?? -1) - (left.receive_gap_ms ?? -1);
      })
      .slice(0, 80),
  );
  let selected = $derived(rows.find((row) => row.symbol === selectedSymbol) ?? ordered[0] ?? null);

  function severityRank(row: SymbolRow): number {
    if (row.problem_severity === 'bad') return 0;
    if (row.problem_severity === 'warn') return 1;
    if (row.subscribed) return 2;
    if (row.problem_severity === 'closed') return 4;
    return 3;
  }
</script>

<section class="panel table-panel" data-testid="symbol-health-table">
  <div class="head">
    <div class="panel-title">活跃合约健康排行</div>
    {#if selected}
      <div class="selected" title={selected.symbol}>{selected.instrument_name ?? selected.symbol}</div>
    {/if}
  </div>
  <table class="table">
    <thead>
      <tr>
        <th>状态</th>
        <th>名称</th>
        <th>距上次更新</th>
        <th>行情延迟</th>
        <th>Tick</th>
        <th>订阅</th>
        <th>风险</th>
      </tr>
    </thead>
    <tbody>
      {#if ordered.length === 0}
        <tr><td colspan="7" class="empty-cell">等待合约数据</td></tr>
      {:else}
        {#each ordered as row}
          <tr class:selected={row.symbol === selectedSymbol} onclick={() => (selectedSymbol = row.symbol)}>
            <td><span class={`badge ${row.status}`}>{statusLabel(row.status)}</span></td>
            <td title={row.symbol}>{row.instrument_name ?? row.symbol}</td>
            <td>{formatDuration(row.receive_gap_ms)}</td>
            <td>{formatDuration(row.market_time_lag_ms)}</td>
            <td>{formatNumber(row.ticks_ingested)}</td>
            <td>{formatNumber(row.quote_subscriber_count + row.chart_subscriber_count)}</td>
            <td><span class={`risk ${row.problem_severity}`}><i></i>{row.problem_severity}</span></td>
          </tr>
        {/each}
      {/if}
    </tbody>
  </table>
  {#if selected}
    <div class="detail">
      <span>last price <b>{formatNumber(selected.last_price)}</b></span>
      <span>volume <b>{formatNumber(selected.last_volume)}</b></span>
      <span>open interest <b>{formatNumber(selected.last_open_interest)}</b></span>
      <span>invalid rows <b>{formatNumber(selected.invalid_rows)}</b></span>
      {#if selected.last_invalid_row_error}
        <span class="error" title={selected.last_invalid_row_error}>{selected.last_invalid_row_error}</span>
      {/if}
    </div>
  {/if}
</section>

<style>
  .table-panel {
    padding: 10px 12px;
  }

  .head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
  }

  .selected {
    color: var(--relay-muted);
    font-size: 12px;
  }

  th:nth-child(1) {
    width: 10%;
  }

  th:nth-child(2) {
    width: 25%;
  }

  th:nth-child(6),
  th:nth-child(7),
  th:nth-child(8) {
    width: 9%;
  }

  tr {
    cursor: pointer;
  }

  tr.selected td {
    background: rgb(93 184 215 / 8%);
  }

  .risk {
    display: flex;
    align-items: center;
    gap: 5px;
    color: var(--relay-live);
    font-weight: 800;
  }

  .risk i {
    width: 6px;
    height: 6px;
    border-radius: 50%;
    background: var(--relay-live);
    box-shadow: 0 0 7px var(--relay-live);
  }

  .risk.warn {
    color: var(--relay-warn);
  }

  .risk.warn i {
    background: var(--relay-warn);
    box-shadow: 0 0 7px var(--relay-warn);
  }

  .risk.bad {
    color: var(--relay-bad);
  }

  .risk.bad i {
    background: var(--relay-bad);
    box-shadow: 0 0 7px var(--relay-bad);
  }

  .risk.closed {
    color: var(--relay-closed);
  }

  .risk.closed i {
    background: var(--relay-closed);
    box-shadow: none;
  }

  .empty-cell {
    padding: 30px 8px;
    color: var(--relay-muted);
    text-align: center;
  }

  .detail {
    margin-top: 10px;
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    color: var(--relay-muted);
    font-size: 11px;
  }

  .detail span {
    border: 1px solid var(--relay-line-soft);
    border-radius: 4px;
    padding: 5px 7px;
    background: #071929;
  }

  .detail b {
    color: var(--relay-text);
  }

  .detail .error {
    max-width: 300px;
    overflow: hidden;
    color: var(--relay-bad);
    text-overflow: ellipsis;
    white-space: nowrap;
  }
</style>
