#![cfg_attr(not(test), forbid(unsafe_code))]

pub const DASHBOARD_HTML: &str = r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Relay Symbol Dashboard</title>
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
      --stale: #b54708;
      --missing: #b42318;
      --inactive: #667085;
      --accent: #2454a6;
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

    .summary {
      display: grid;
      grid-template-columns: repeat(6, minmax(120px, 1fr));
      gap: 10px;
      margin-bottom: 14px;
    }

    .upstream {
      display: grid;
      grid-template-columns: repeat(6, minmax(120px, 1fr));
      gap: 10px;
      margin-bottom: 14px;
    }

    .tile {
      min-height: 72px;
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 6px;
      padding: 10px 12px;
    }

    .label {
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
    }

    .value {
      margin-top: 5px;
      font-size: 24px;
      line-height: 1.1;
      font-weight: 700;
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

    .table-wrap {
      overflow: auto;
      max-height: calc(100vh - 200px);
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 6px;
    }

    table {
      width: 100%;
      border-collapse: collapse;
      min-width: 1180px;
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

    tbody tr:hover { background: #f8fafc; }
    .status { font-weight: 700; text-transform: uppercase; }
    .live { color: var(--live); }
    .stale { color: var(--stale); }
    .missing { color: var(--missing); }
    .inactive { color: var(--inactive); }
    .numeric { text-align: right; font-variant-numeric: tabular-nums; }
    .empty { color: var(--muted); text-align: center; padding: 28px 10px; }

    @media (max-width: 1100px) {
      body { min-width: 0; }
      header { align-items: flex-start; flex-direction: column; gap: 6px; }
      .summary, .upstream { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .toolbar { grid-template-columns: 1fr 1fr; }
      .toolbar input { grid-column: 1 / -1; }
      button { width: 100%; }
      .table-wrap { max-height: none; }
    }
  </style>
</head>
<body>
  <header>
    <h1>Relay Symbol Dashboard</h1>
    <div class="timestamp" id="timestamp"></div>
  </header>
  <main>
    <section class="upstream" id="upstream"></section>
    <section class="summary" id="summary"></section>
    <section class="toolbar">
      <input id="query" aria-label="Search symbol" placeholder="Search symbol">
      <select id="status" aria-label="Status filter">
        <option value="">All statuses</option>
        <option value="live">Live</option>
        <option value="stale">Stale</option>
        <option value="missing">Missing</option>
        <option value="inactive">Inactive</option>
      </select>
      <select id="subscribed" aria-label="Subscription filter">
        <option value="">All symbols</option>
        <option value="1">Subscribed only</option>
      </select>
      <select id="sort" aria-label="Sort">
        <option value="receive_gap_ms_desc">Receive gap</option>
        <option value="market_time_lag_ms_desc">Market lag</option>
        <option value="symbol_asc">Symbol</option>
        <option value="status_asc">Status</option>
        <option value="ticks_ingested_desc">Tick count</option>
      </select>
      <select id="limit" aria-label="Row limit">
        <option value="200">200 rows</option>
        <option value="500">500 rows</option>
        <option value="1000">1000 rows</option>
      </select>
      <button id="refresh" type="button">Refresh</button>
    </section>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Status</th>
            <th>Symbol</th>
            <th>Subscribed</th>
            <th class="numeric">Receive gap</th>
            <th class="numeric">Market lag</th>
            <th>Last receive</th>
            <th>Last tick</th>
            <th class="numeric">Ticks</th>
            <th class="numeric">Last price</th>
            <th class="numeric">Quote subs</th>
            <th class="numeric">Chart subs</th>
            <th class="numeric">Invalid rows</th>
          </tr>
        </thead>
        <tbody id="symbols"></tbody>
      </table>
    </div>
  </main>
  <script src="/dashboard/app.js"></script>
</body>
</html>
"#;

pub const DASHBOARD_JS: &str = r#"
const upstream = document.getElementById('upstream');
const summary = document.getElementById('summary');
const symbols = document.getElementById('symbols');
const timestamp = document.getElementById('timestamp');
const controls = ['query', 'status', 'subscribed', 'sort', 'limit']
  .map((id) => document.getElementById(id));

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

function fmtMs(value) {
  if (value === null || value === undefined) return '--';
  if (value < 1000) return `${value}ms`;
  return `${(value / 1000).toFixed(1)}s`;
}

function fmtTime(value) {
  if (!value) return '--';
  return new Date(value).toLocaleTimeString();
}

function fmtPrice(value) {
  if (value === null || value === undefined) return '--';
  return Number(value).toString();
}

function fmtSeconds(value) {
  if (value === null || value === undefined) return '--';
  if (value < 60) return `${value}s`;
  if (value < 3600) return `${Math.floor(value / 60)}m ${value % 60}s`;
  return `${Math.floor(value / 3600)}h ${Math.floor((value % 3600) / 60)}m`;
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
  if (elapsed !== null) parts.push(`elapsed ${fmtSeconds(elapsed)}`);
  parts.push(`${metrics.upstream_frames_received ?? 0} frames`);
  parts.push(`${metrics.upstream_events_decoded ?? 0} decoded`);
  if (frameRate !== null) parts.push(`${frameRate}/s`);
  if (idle !== null) parts.push(`idle ${fmtSeconds(idle)}`);
  if ((metrics.upstream_events_decoded ?? 0) === 0) parts.push('waiting first event');
  return parts.join(' · ');
}

function renderUpstream(metrics, nowUnixMillis) {
  if (!metrics) {
    upstream.innerHTML = '';
    return;
  }
  upstream.innerHTML = [
    ['stage', metrics.upstream_stage || '--'],
    ['transport', metrics.upstream_transport_connected ? 'connected' : 'waiting'],
    ['subscription', metrics.upstream_subscription_sent ? 'sent' : 'pending'],
    ['frames', metrics.upstream_frames_received ?? 0],
    ['decoded', metrics.upstream_events_decoded ?? 0],
    ['backfill', backfillProgress(metrics, nowUnixMillis)],
    ['last frame', fmtTime(metrics.last_upstream_frame_unix_secs ? metrics.last_upstream_frame_unix_secs * 1000 : null)]
  ].map(([label, value]) => (
    `<div class="tile"><div class="label">${escapeHtml(label)}</div><div class="value">${escapeHtml(value)}</div></div>`
  )).join('');
}

function render(data, metrics) {
  renderUpstream(metrics, data.now_unix_millis);
  const s = data.summary;
  timestamp.textContent = `Updated ${fmtTime(data.now_unix_millis)}`;
  summary.innerHTML = [
    ['live', s.live],
    ['stale', s.stale],
    ['missing', s.missing],
    ['inactive', s.inactive],
    ['subscribed', s.subscribed],
    ['p95 gap', fmtMs(s.p95_receive_gap_ms)]
  ].map(([label, value]) => (
    `<div class="tile"><div class="label">${escapeHtml(label)}</div><div class="value">${escapeHtml(value)}</div></div>`
  )).join('');

  if (data.symbols.length === 0) {
    symbols.innerHTML = '<tr><td class="empty" colspan="12">No symbols</td></tr>';
    return;
  }

  symbols.innerHTML = data.symbols.map((row) => {
    const lastTickMs = row.last_tick_datetime_ns
      ? Math.floor(row.last_tick_datetime_ns / 1000000)
      : null;
    return `
      <tr>
        <td class="status ${escapeHtml(row.status)}">${escapeHtml(row.status)}</td>
        <td>${escapeHtml(row.symbol)}</td>
        <td>${row.subscribed ? 'yes' : 'no'}</td>
        <td class="numeric">${fmtMs(row.receive_gap_ms)}</td>
        <td class="numeric">${fmtMs(row.market_time_lag_ms)}</td>
        <td>${fmtTime(row.last_receive_unix_millis)}</td>
        <td>${fmtTime(lastTickMs)}</td>
        <td class="numeric">${row.ticks_ingested}</td>
        <td class="numeric">${escapeHtml(fmtPrice(row.last_price))}</td>
        <td class="numeric">${row.quote_subscriber_count}</td>
        <td class="numeric">${row.chart_subscriber_count}</td>
        <td class="numeric" title="${escapeHtml(row.last_invalid_row_error || '')}">${row.invalid_rows}</td>
      </tr>
    `;
  }).join('');
}

async function load() {
  const suffix = params();
  const [symbolResponse, metricsResponse] = await Promise.all([
    fetch(`/symbol-metrics${suffix ? `?${suffix}` : ''}`),
    fetch('/metrics')
  ]);
  render(await symbolResponse.json(), await metricsResponse.json());
}

document.getElementById('refresh').addEventListener('click', load);
for (const control of controls) {
  control.addEventListener('change', load);
}
document.getElementById('query').addEventListener('input', () => {
  clearTimeout(window.relayDashboardSearchTimer);
  window.relayDashboardSearchTimer = setTimeout(load, 250);
});

load();
setInterval(load, 2000);
"#;
