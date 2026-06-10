#![cfg_attr(not(test), forbid(unsafe_code))]

pub const DASHBOARD_HTML: &str = r#"<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>行情中继合约监控</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f5f6f8;
      --panel: #ffffff;
      --panel-soft: #f0f3f7;
      --border: #d8dee8;
      --text: #172033;
      --muted: #667085;
      --live: #067647;
      --closed: #4e5ba6;
      --stale: #b54708;
      --missing: #b42318;
      --inactive: #667085;
      --accent: #2454a6;
      --warning-bg: #fffaeb;
      --error-bg: #fef3f2;
      --ok-bg: #ecfdf3;
    }

    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-width: 980px;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      color: var(--text);
      background: var(--bg);
      letter-spacing: 0;
    }

    header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 14px 20px;
      background: #182230;
      color: #ffffff;
      border-bottom: 1px solid #101828;
    }

    h1 {
      margin: 0;
      font-size: 20px;
      line-height: 1.2;
      font-weight: 700;
    }

    .timestamp {
      color: #cbd5e1;
      font-size: 13px;
      white-space: nowrap;
    }

    main { padding: 16px 20px 24px; }

    .alert {
      display: none;
      margin-bottom: 12px;
      padding: 10px 12px;
      border: 1px solid #fecdca;
      border-radius: 6px;
      background: var(--error-bg);
      color: var(--missing);
      font-size: 13px;
      font-weight: 700;
    }

    .health {
      display: grid;
      grid-template-columns: minmax(240px, 1.3fr) repeat(4, minmax(120px, 1fr));
      gap: 10px;
      margin-bottom: 12px;
    }

    .health-main,
    .tile,
    .detail {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 6px;
    }

    .health-main {
      min-height: 88px;
      padding: 12px 14px;
      border-left: 5px solid var(--inactive);
    }

    .health-main.live { border-left-color: var(--live); background: var(--ok-bg); }
    .health-main.warning { border-left-color: var(--stale); background: var(--warning-bg); }
    .health-main.error { border-left-color: var(--missing); background: var(--error-bg); }

    .health-title {
      font-size: 22px;
      line-height: 1.2;
      font-weight: 800;
    }

    .health-subtitle {
      margin-top: 6px;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.4;
    }

    .diagnostics,
    .summary {
      display: grid;
      grid-template-columns: repeat(4, minmax(160px, 1fr));
      gap: 10px;
      margin-bottom: 12px;
    }

    .tile {
      min-height: 76px;
      padding: 10px 12px;
    }

    .label {
      color: var(--muted);
      font-size: 12px;
    }

    .value {
      margin-top: 5px;
      font-size: 22px;
      line-height: 1.15;
      font-weight: 800;
    }

    .meta {
      margin-top: 5px;
      color: var(--muted);
      font-size: 12px;
      line-height: 1.35;
    }

    .status-overview {
      margin-bottom: 12px;
      padding: 10px 12px;
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 6px;
    }

    .status-bar {
      display: flex;
      height: 10px;
      overflow: hidden;
      border-radius: 999px;
      background: #edf0f5;
    }

    .segment.live { background: var(--live); }
    .segment.closed { background: var(--closed); }
    .segment.stale { background: var(--stale); }
    .segment.missing { background: var(--missing); }
    .segment.inactive { background: var(--inactive); }

    .legend {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-top: 9px;
      color: var(--muted);
      font-size: 12px;
    }

    .legend-item {
      display: inline-flex;
      align-items: center;
      gap: 5px;
      white-space: nowrap;
    }

    .dot {
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background: currentColor;
    }

    .toolbar {
      display: grid;
      grid-template-columns: minmax(220px, 1fr) repeat(4, max-content) max-content;
      gap: 10px;
      align-items: center;
      margin-bottom: 12px;
    }

    input, select, button {
      height: 36px;
      border: 1px solid #c7d0df;
      border-radius: 6px;
      padding: 0 10px;
      background: #ffffff;
      color: var(--text);
      font: inherit;
      font-size: 13px;
    }

    input { width: 100%; min-width: 180px; }
    button {
      min-width: 88px;
      color: #ffffff;
      background: var(--accent);
      border-color: var(--accent);
      cursor: pointer;
    }

    .workbench {
      display: grid;
      grid-template-columns: minmax(0, 1fr) 330px;
      gap: 12px;
      align-items: start;
    }

    .table-wrap {
      overflow: auto;
      max-height: calc(100vh - 330px);
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 6px;
    }

    table {
      width: 100%;
      border-collapse: collapse;
      min-width: 980px;
    }

    th, td {
      padding: 8px 10px;
      border-bottom: 1px solid #edf0f5;
      text-align: left;
      font-size: 13px;
      line-height: 1.35;
      white-space: nowrap;
    }

    th {
      position: sticky;
      top: 0;
      z-index: 1;
      background: var(--panel-soft);
      color: #475467;
      font-weight: 700;
    }

    tbody tr { cursor: pointer; }
    tbody tr:hover, tbody tr.selected { background: #f8fafc; }
    .status { font-weight: 800; }
    .live { color: var(--live); }
    .closed { color: var(--closed); }
    .stale { color: var(--stale); }
    .missing { color: var(--missing); }
    .inactive { color: var(--inactive); }
    .numeric { text-align: right; font-variant-numeric: tabular-nums; }
    .empty { color: var(--muted); text-align: center; padding: 28px 10px; }

    .pill {
      display: inline-flex;
      align-items: center;
      min-width: 42px;
      justify-content: center;
      padding: 2px 7px;
      border-radius: 999px;
      background: #edf0f5;
      color: #344054;
      font-size: 12px;
      font-weight: 700;
    }

    .detail {
      position: sticky;
      top: 12px;
      min-height: 220px;
      padding: 12px;
    }

    .detail h2 {
      margin: 0 0 10px;
      font-size: 16px;
      line-height: 1.2;
    }

    .detail-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 8px;
    }

    .detail-item {
      min-height: 50px;
      padding: 8px;
      border: 1px solid #edf0f5;
      border-radius: 6px;
      background: #fbfcfe;
    }

    .detail-wide { grid-column: 1 / -1; }

    @media (max-width: 1100px) {
      body { min-width: 0; }
      header { align-items: flex-start; flex-direction: column; gap: 6px; }
      .health,
      .diagnostics,
      .summary { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .toolbar { grid-template-columns: 1fr 1fr; }
      .toolbar input { grid-column: 1 / -1; }
      button { width: 100%; }
      .workbench { grid-template-columns: 1fr; }
      .table-wrap { max-height: none; }
      .detail { position: static; }
    }
  </style>
</head>
<body>
  <header>
    <h1>行情中继合约监控</h1>
    <div class="timestamp" id="timestamp"></div>
  </header>
  <main>
    <div class="alert" id="alert"></div>
    <section class="health" id="health"></section>
    <section class="diagnostics" id="diagnostics"></section>
    <section class="status-overview">
      <div class="status-bar" id="statusBar"></div>
      <div class="legend" id="statusLegend"></div>
    </section>
    <section class="summary" id="summary"></section>
    <section class="toolbar">
      <input id="query" aria-label="搜索合约" placeholder="搜索合约代码或中文名称">
      <select id="status" aria-label="状态筛选">
        <option value="stale,missing,inactive" selected>问题合约</option>
        <option value="">全部状态</option>
        <option value="live">实时</option>
        <option value="closed">休盘</option>
        <option value="stale">过期</option>
        <option value="missing">缺失</option>
        <option value="inactive">未激活</option>
      </select>
      <select id="subscribed" aria-label="订阅筛选">
        <option value="">全部合约</option>
        <option value="1">仅有订阅</option>
      </select>
      <select id="sort" aria-label="排序">
        <option value="status_asc" selected>问题优先</option>
        <option value="receive_gap_ms_desc">接收延迟</option>
        <option value="market_time_lag_ms_desc">行情延迟</option>
        <option value="symbol_asc">合约代码</option>
        <option value="ticks_ingested_desc">Tick 数</option>
      </select>
      <select id="limit" aria-label="行数限制">
        <option value="200">200 行</option>
        <option value="500">500 行</option>
        <option value="1000">1000 行</option>
      </select>
      <button id="refresh" type="button">刷新</button>
    </section>
    <section class="workbench">
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>状态</th>
              <th>合约</th>
              <th>中文名称</th>
              <th class="numeric">接收延迟</th>
              <th class="numeric">行情延迟</th>
              <th>订阅</th>
              <th class="numeric">最新价</th>
              <th>异常</th>
            </tr>
          </thead>
          <tbody id="symbols"></tbody>
        </table>
      </div>
      <aside class="detail" id="detail"></aside>
    </section>
  </main>
  <script src="/dashboard/app.js"></script>
</body>
</html>
"#;

pub const DASHBOARD_JS: &str = r#"
const alertBox = document.getElementById('alert');
const health = document.getElementById('health');
const diagnostics = document.getElementById('diagnostics');
const summary = document.getElementById('summary');
const symbols = document.getElementById('symbols');
const detail = document.getElementById('detail');
const statusBar = document.getElementById('statusBar');
const statusLegend = document.getElementById('statusLegend');
const timestamp = document.getElementById('timestamp');
const controls = ['query', 'status', 'subscribed', 'sort', 'limit']
  .map((id) => document.getElementById(id));
const STATUS_LABELS = {
  live: '实时',
  closed: '休盘',
  stale: '过期',
  missing: '缺失',
  inactive: '未激活'
};
const STAGE_LABELS = {
  connecting: '连接中',
  subscribing: '订阅中',
  backfilling: '回填中',
  live: '实时',
  degraded: '降级',
  down: '断开'
};

let selectedSymbol = null;
let currentRows = [];
let activeController = null;
let loadSequence = 0;

function params() {
  const query = new URLSearchParams();
  const q = document.getElementById('query').value.trim();
  const status = document.getElementById('status').value;
  const subscribed = document.getElementById('subscribed').value;
  const sort = document.getElementById('sort').value;
  const limit = document.getElementById('limit').value;
  if (q) query.set('q', q);
  if (status) query.set('status', status);
  if (subscribed) query.set('subscribed', subscribed);
  if (sort) query.set('sort', sort);
  if (limit) query.set('limit', limit);
  return query.toString();
}

function escapeHtml(value) {
  return String(value)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function statusLabel(status) {
  return STATUS_LABELS[status] || status || '--';
}

function stageLabel(stage) {
  return STAGE_LABELS[stage] || stage || '--';
}

function boolLabel(value) {
  return value ? '是' : '否';
}

function issueSummary(summary) {
  return `${summary.stale} 过期 | ${summary.missing} 缺失 | ${summary.inactive} 未激活`;
}

function closedCount(summary) {
  return summary.closed ?? 0;
}

function fmtMs(value) {
  if (value === null || value === undefined) return '--';
  if (value < 1000) return `${value}毫秒`;
  if (value < 60000) return `${(value / 1000).toFixed(1)}秒`;
  return fmtSeconds(Math.floor(value / 1000));
}

function fmtTime(value) {
  if (!value) return '--';
  return new Date(value).toLocaleString();
}

function fmtPrice(value) {
  if (value === null || value === undefined) return '--';
  return Number(value).toString();
}

function fmtSeconds(value) {
  if (value === null || value === undefined) return '--';
  if (value < 60) return `${value}秒`;
  if (value < 3600) return `${Math.floor(value / 60)}分 ${value % 60}秒`;
  return `${Math.floor(value / 3600)}时 ${Math.floor((value % 3600) / 60)}分`;
}

function metricAge(nowUnixMillis, unixSecs) {
  if (!unixSecs) return null;
  return Math.max(0, Math.floor(nowUnixMillis / 1000) - unixSecs);
}

function backfillProgress(metrics, nowUnixMillis) {
  if (!metrics || metrics.upstream_stage !== 'backfilling') return '--';
  const nowSecs = Math.floor(nowUnixMillis / 1000);
  const started = metrics.upstream_stage_started_unix_secs;
  const elapsed = started ? Math.max(0, nowSecs - started) : null;
  const lastFrame = metrics.last_upstream_frame_unix_secs;
  const idle = lastFrame ? Math.max(0, nowSecs - lastFrame) : null;
  const frameRate = elapsed && elapsed > 0
    ? (metrics.upstream_frames_received / elapsed).toFixed(2)
    : null;
  const parts = [];
  if (elapsed !== null) parts.push(`已持续 ${fmtSeconds(elapsed)}`);
  parts.push(`${metrics.upstream_frames_received ?? 0} 帧`);
  parts.push(`${metrics.upstream_events_decoded ?? 0} 条事件`);
  if (frameRate !== null) parts.push(`${frameRate} 帧/秒`);
  if (idle !== null) parts.push(`空闲 ${fmtSeconds(idle)}`);
  if ((metrics.upstream_events_decoded ?? 0) === 0) parts.push('等待首个事件');
  return parts.join(' | ');
}

function deriveHealth(data, metrics) {
  const stage = metrics.upstream_stage || 'connecting';
  const s = data.summary;
  const issues = s.stale + s.missing + s.inactive;
  const closed = closedCount(s);
  const lastTickAge = metricAge(data.now_unix_millis, metrics.last_tick_unix_secs);
  const staleWindowSecs = Math.floor(data.data_stale_after_millis / 1000);
  const dataFresh = lastTickAge !== null && lastTickAge <= staleWindowSecs;

  if (metrics.last_universe_refresh_error) {
    return {
      className: 'error',
      title: '合约集合刷新失败',
      subtitle: metrics.last_universe_refresh_error,
      issues,
      lastTickAge,
      dataFresh
    };
  }
  if (stage === 'live' && issues === 0 && s.total > 0 && closed === s.total) {
    return {
      className: 'live',
      title: '当前休盘',
      subtitle: `${closed} 个合约按各自交易时段处于休盘，未计入问题合约`,
      issues,
      lastTickAge,
      dataFresh
    };
  }
  if (stage === 'live' && dataFresh && issues === 0) {
    return {
      className: 'live',
      title: '行情实时',
      subtitle: `${s.live} 个合约在 ${staleWindowSecs} 秒新鲜度窗口内，${closed} 个休盘`,
      issues,
      lastTickAge,
      dataFresh
    };
  }
  if (stage === 'live' && dataFresh) {
    return {
      className: 'warning',
      title: `${issues} 个合约需要关注`,
      subtitle: issueSummary(s),
      issues,
      lastTickAge,
      dataFresh
    };
  }
  if (stage === 'backfilling') {
    return {
      className: 'warning',
      title: '正在回填上游行情',
      subtitle: backfillProgress(metrics, data.now_unix_millis),
      issues,
      lastTickAge,
      dataFresh
    };
  }
  return {
    className: stage === 'degraded' || stage === 'down' ? 'error' : 'warning',
    title: `上游${stageLabel(stage)}`,
    subtitle: dataFresh ? '最近数据仍新鲜' : '等待新鲜行情数据',
    issues,
    lastTickAge,
    dataFresh
  };
}

function renderHealth(data, metrics) {
  const h = deriveHealth(data, metrics);
  const lastFrameAge = metricAge(data.now_unix_millis, metrics.last_upstream_frame_unix_secs);
  const staleWindow = Math.floor(data.data_stale_after_millis / 1000);
  health.innerHTML = [
    `<div class="health-main ${escapeHtml(h.className)}">
      <div class="health-title">${escapeHtml(h.title)}</div>
      <div class="health-subtitle">${escapeHtml(h.subtitle)}</div>
    </div>`,
    tile('最近 Tick', fmtSeconds(h.lastTickAge), `新鲜度窗口 ${staleWindow} 秒`),
    tile('上游空闲', fmtSeconds(lastFrameAge), `${metrics.upstream_frames_received ?? 0} 帧`),
    tile('问题合约', h.issues, `${issueSummary(data.summary)} | ${closedCount(data.summary)} 休盘`),
    tile('客户端', metrics.downstream_clients ?? 0, `${metrics.quote_subscriptions ?? 0} 报价 | ${metrics.chart_subscriptions ?? 0} 图表`)
  ].join('');
}

function renderDiagnostics(data, metrics) {
  diagnostics.innerHTML = [
    tile('上游阶段', stageLabel(metrics.upstream_stage),
      `${metrics.upstream_transport_connected ? '连接已建立' : '等待连接'} | ${metrics.upstream_subscription_sent ? '订阅已发送' : '等待订阅'}`),
    tile('合约集合', metrics.upstream_symbols ?? 0,
      metrics.last_universe_refresh_error || `最近刷新 ${fmtTime(metrics.last_universe_refresh_unix_secs ? metrics.last_universe_refresh_unix_secs * 1000 : null)}`),
    tile('新鲜度', fmtMs(data.summary.p95_receive_gap_ms),
      `P95 接收延迟 | 阈值 ${data.data_stale_after_millis / 1000} 秒`),
    tile('启动回放', `${metrics.bootstrap_pending ?? 0}/${metrics.bootstrap_inflight ?? 0}`,
      `${metrics.upstream_events_decoded ?? 0} 条事件 | ${metrics.upstream_invalid_tick_rows ?? 0} 行异常`)
  ].join('');
}

function renderStatusOverview(data) {
  const s = data.summary;
  const total = Math.max(1, s.total);
  const parts = [
    ['live', s.live],
    ['closed', closedCount(s)],
    ['stale', s.stale],
    ['missing', s.missing],
    ['inactive', s.inactive]
  ];
  statusBar.innerHTML = parts.map(([status, count]) => (
    `<div class="segment ${status}" style="width: ${(count / total) * 100}%"></div>`
  )).join('');
  statusLegend.innerHTML = parts.map(([status, count]) => (
    `<span class="legend-item ${status}"><span class="dot"></span>${escapeHtml(statusLabel(status))} ${count}</span>`
  )).join('');
}

function tile(label, value, meta = '') {
  return `<div class="tile">
    <div class="label">${escapeHtml(label)}</div>
    <div class="value">${escapeHtml(value)}</div>
    <div class="meta">${escapeHtml(meta)}</div>
  </div>`;
}

function demand(row) {
  const quote = row.quote_subscriber_count ?? 0;
  const chart = row.chart_subscriber_count ?? 0;
  if (quote === 0 && chart === 0) return '<span class="pill">无</span>';
  return `<span class="pill">报${quote} / 图${chart}</span>`;
}

function rowError(row) {
  if ((row.invalid_rows ?? 0) === 0) return '--';
  const title = escapeHtml(row.last_invalid_row_error || '');
  return `<span class="missing" title="${title}">${row.invalid_rows} 行异常</span>`;
}

function render(data, metrics) {
  alertBox.style.display = 'none';
  renderHealth(data, metrics);
  renderDiagnostics(data, metrics);
  renderStatusOverview(data);

  const s = data.summary;
  timestamp.textContent = `更新时间 ${fmtTime(data.now_unix_millis)}`;
  summary.innerHTML = [
    tile('当前显示', data.symbols.length, `共 ${s.total} 个合约`),
    tile('问题合约', s.stale + s.missing + s.inactive, issueSummary(s)),
    tile('休盘', closedCount(s), '按合约交易时段排除告警'),
    tile('有订阅', s.subscribed, '报价或图表需求'),
    tile('P95 延迟', fmtMs(s.p95_receive_gap_ms), '有数据的合约')
  ].join('');

  currentRows = data.symbols;
  if (data.symbols.length === 0) {
    symbols.innerHTML = '<tr><td class="empty" colspan="8">没有匹配合约</td></tr>';
    selectedSymbol = null;
    renderDetail(null);
    return;
  }

  if (!selectedSymbol || !data.symbols.some((row) => row.symbol === selectedSymbol)) {
    selectedSymbol = data.symbols[0].symbol;
  }

  symbols.innerHTML = data.symbols.map((row) => {
    const selected = row.symbol === selectedSymbol ? ' selected' : '';
    return `
      <tr class="${selected}" data-symbol="${escapeHtml(row.symbol)}">
        <td class="status ${escapeHtml(row.status)}">${escapeHtml(statusLabel(row.status))}</td>
        <td>${escapeHtml(row.symbol)}</td>
        <td>${escapeHtml(row.instrument_name || '--')}</td>
        <td class="numeric">${fmtMs(row.receive_gap_ms)}</td>
        <td class="numeric">${fmtMs(row.market_time_lag_ms)}</td>
        <td>${demand(row)}</td>
        <td class="numeric">${escapeHtml(fmtPrice(row.last_price))}</td>
        <td>${rowError(row)}</td>
      </tr>
    `;
  }).join('');
  renderDetail(data.symbols.find((row) => row.symbol === selectedSymbol));
}

function renderDetail(row) {
  if (!row) {
    detail.innerHTML = '<div class="empty">请选择合约</div>';
    return;
  }
  const lastTickMs = row.last_tick_datetime_ns
    ? Math.floor(row.last_tick_datetime_ns / 1000000)
    : null;
  detail.innerHTML = `
    <h2>${escapeHtml(row.symbol)}</h2>
    <div class="detail-grid">
      ${detailItem('中文名称', row.instrument_name || '--', '', true)}
      ${detailItem('状态', statusLabel(row.status), row.status)}
      ${detailItem('在合约集合', boolLabel(row.in_universe))}
      ${detailItem('接收延迟', fmtMs(row.receive_gap_ms))}
      ${detailItem('行情延迟', fmtMs(row.market_time_lag_ms))}
      ${detailItem('最近接收', fmtTime(row.last_receive_unix_millis), '', true)}
      ${detailItem('最近 Tick', fmtTime(lastTickMs), '', true)}
      ${detailItem('Tick 数', row.ticks_ingested)}
      ${detailItem('最新价', fmtPrice(row.last_price))}
      ${detailItem('成交量', row.last_volume ?? '--')}
      ${detailItem('持仓量', row.last_open_interest ?? '--')}
      ${detailItem('订阅', `报价 ${row.quote_subscriber_count} / 图表 ${row.chart_subscriber_count}`)}
      ${detailItem('异常行', row.invalid_rows)}
      ${detailItem('最近错误', row.last_invalid_row_error || '--', '', true)}
    </div>
  `;
}

function detailItem(label, value, className = '', wide = false) {
  return `<div class="detail-item ${wide ? 'detail-wide' : ''}">
    <div class="label">${escapeHtml(label)}</div>
    <div class="value ${escapeHtml(className)}">${escapeHtml(value)}</div>
  </div>`;
}

function showError(error) {
  alertBox.textContent = `监控面板刷新失败：${error.message || error}`;
  alertBox.style.display = 'block';
}

async function load() {
  const sequence = ++loadSequence;
  if (activeController) activeController.abort();
  activeController = new AbortController();
  const suffix = params();
  try {
    const [symbolResponse, metricsResponse] = await Promise.all([
      fetch(`/symbol-metrics${suffix ? `?${suffix}` : ''}`, {
        signal: activeController.signal,
        cache: 'no-store'
      }),
      fetch('/metrics', {
        signal: activeController.signal,
        cache: 'no-store'
      })
    ]);
    if (!symbolResponse.ok) throw new Error(`/symbol-metrics 返回 ${symbolResponse.status}`);
    if (!metricsResponse.ok) throw new Error(`/metrics 返回 ${metricsResponse.status}`);
    const [data, metrics] = await Promise.all([
      symbolResponse.json(),
      metricsResponse.json()
    ]);
    if (sequence !== loadSequence) return;
    render(data, metrics);
  } catch (error) {
    if (error && error.name === 'AbortError') return;
    if (sequence === loadSequence) showError(error);
  }
}

document.getElementById('refresh').addEventListener('click', load);
for (const control of controls) {
  control.addEventListener('change', load);
}
document.getElementById('query').addEventListener('input', () => {
  clearTimeout(window.relayDashboardSearchTimer);
  window.relayDashboardSearchTimer = setTimeout(load, 250);
});
symbols.addEventListener('click', (event) => {
  const row = event.target.closest('tr[data-symbol]');
  if (!row) return;
  selectedSymbol = row.getAttribute('data-symbol');
  renderDetail(currentRows.find((item) => item.symbol === selectedSymbol));
  for (const tr of symbols.querySelectorAll('tr')) tr.classList.remove('selected');
  row.classList.add('selected');
});
document.addEventListener('visibilitychange', () => {
  if (!document.hidden) load();
});

load();
setInterval(() => {
  if (!document.hidden) load();
}, 2000);
"#;
