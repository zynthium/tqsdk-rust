'use strict';

const POLL_INTERVAL_MS = 2000;
const HISTORY_LIMIT = 150;
const TIMELINE_BUCKETS = 60;
const EXCHANGES = ['SHFE', 'DCE', 'CZCE', 'INE', 'GFEX', 'CFFEX'];
const PROBLEM_STATUSES = new Set(['stale', 'missing', 'inactive']);
const state = {
  controller: null,
  sequence: 0,
  timer: null,
  previous: null,
  samples: [],
  timeline: [],
  events: [],
  knownStatuses: new Map(),
  lastGlobalFlow: null,
  lastStage: null,
  clockTimer: null,
};

const $ = (id) => document.getElementById(id);
const fmtNumber = new Intl.NumberFormat('zh-CN');
const shanghaiClock = new Intl.DateTimeFormat('zh-CN', {
  timeZone: 'Asia/Shanghai',
  year: 'numeric', month: '2-digit', day: '2-digit',
  hour: '2-digit', minute: '2-digit', second: '2-digit',
  hour12: false,
});
const shanghaiTime = new Intl.DateTimeFormat('zh-CN', {
  timeZone: 'Asia/Shanghai', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false,
});
const shanghaiMinute = new Intl.DateTimeFormat('zh-CN', {
  timeZone: 'Asia/Shanghai', hour: '2-digit', minute: '2-digit', hour12: false,
});

function escapeHtml(value) {
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function formatDuration(value) {
  if (value === null || value === undefined || !Number.isFinite(Number(value))) return '--';
  const ms = Math.max(0, Number(value));
  if (ms < 1000) return `${Math.round(ms)}ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(ms < 10_000 ? 1 : 0)}s`;
  if (ms < 3_600_000) return `${Math.floor(ms / 60_000)}m${Math.floor((ms % 60_000) / 1000)}s`;
  return `${Math.floor(ms / 3_600_000)}h${Math.floor((ms % 3_600_000) / 60_000)}m`;
}

function formatRate(value) {
  if (value === null || value === undefined || !Number.isFinite(value)) return '--';
  if (value >= 1000) return fmtNumber.format(Math.round(value));
  if (value >= 10) return value.toFixed(0);
  return value.toFixed(1);
}

function formatTime(unixMillis) {
  if (!unixMillis) return '--';
  return shanghaiTime.format(new Date(unixMillis));
}

function statusLabel(status) {
  return ({ live: '正常', closed: '休盘', stale: '静默', missing: '未覆盖', inactive: '未激活' })[status] || status || '未知';
}

function severityForRow(row) {
  if (row.invalid_rows > 0 || row.status === 'missing' || row.status === 'inactive') return 'bad';
  if (row.status === 'stale') return 'warn';
  if (row.status === 'closed') return 'closed';
  return 'live';
}

function exchangeOf(symbol) {
  const exchange = String(symbol || '').split('.')[0].toUpperCase();
  return EXCHANGES.includes(exchange) ? exchange : 'OTHER';
}

function frameIdleMs(metrics, nowMillis) {
  return metrics.last_upstream_frame_unix_secs == null
    ? null
    : Math.max(0, nowMillis - Number(metrics.last_upstream_frame_unix_secs) * 1000);
}

function backfillProgress(metrics) {
  const stage = metrics.upstream_stage;
  if (stage !== 'backfilling') return null;
  const started = metrics.upstream_stage_started_unix_secs;
  if (started == null) return '正在初始化';
  return `已持续 ${formatDuration(Date.now() - Number(started) * 1000)}`;
}

function calculateRates(metrics, sampledAt) {
  const previous = state.previous;
  const result = { frameRate: null, eventRate: null };
  if (previous) {
    const elapsed = (sampledAt - previous.sampledAt) / 1000;
    if (elapsed > 0) {
      result.frameRate = Math.max(0, (metrics.upstream_frames_received - previous.metrics.upstream_frames_received) / elapsed);
      result.eventRate = Math.max(0, (metrics.upstream_events_decoded - previous.metrics.upstream_events_decoded) / elapsed);
    }
  }
  state.previous = { metrics, sampledAt };
  return result;
}

function deriveModel(metrics, data, sampledAt) {
  const rows = Array.isArray(data.symbols) ? data.symbols : [];
  const universeRows = rows.filter((row) => row.in_universe);
  const observed = universeRows.filter((row) => row.last_receive_unix_millis != null).length;
  const totalUniverse = universeRows.length || Number(metrics.upstream_symbols || data.summary.total || 0);
  const coverage = totalUniverse > 0 ? observed / totalUniverse * 100 : 0;
  const problems = rows.filter((row) => PROBLEM_STATUSES.has(row.status) || row.invalid_rows > 0);
  const subscribedProblems = problems.filter((row) => row.subscribed);
  const invalidRows = Number(metrics.upstream_invalid_tick_rows || 0);
  const idleMs = frameIdleMs(metrics, sampledAt);
  const rates = calculateRates(metrics, sampledAt);
  const flowThreshold = Number(data.data_stale_after_millis || 30_000);
  const sourceDown = ['down', 'degraded'].includes(metrics.upstream_stage);
  const sourceWarming = ['connecting', 'subscribing', 'backfilling'].includes(metrics.upstream_stage);
  const globalFlow = sourceDown || (idleMs != null && idleMs > flowThreshold)
    ? 'bad'
    : sourceWarming || (idleMs != null && idleMs > Math.min(5000, flowThreshold / 3))
      ? 'warn'
      : 'live';
  const issueCount = problems.length + invalidRows;
  const scorePenalty = Math.min(100,
    subscribedProblems.length * 8
      + problems.filter((row) => !row.subscribed).length * 0.5
      + invalidRows * 2
      + (globalFlow === 'bad' ? 35 : globalFlow === 'warn' ? 8 : 0));
  const score = Math.max(0, 100 - scorePenalty);
  return {
    metrics, data, rows, universeRows, problems, subscribedProblems, observed, totalUniverse,
    coverage, invalidRows, idleMs, rates, globalFlow, sourceDown, sourceWarming, issueCount, score,
    sampledAt,
  };
}

function setDot(id, severity) {
  const element = $(id);
  element.className = `dot ${severity === 'bad' ? 'bad' : severity === 'warn' ? 'warn' : severity === 'live' ? '' : 'idle'}`;
}

function setNodeState(id, severity) {
  const element = $(id);
  element.className = `node-state ${severity === 'bad' ? 'error' : severity === 'warn' ? 'warning' : ''}`;
}

function renderHealth(model) {
  const hero = $('health');
  let severity = model.globalFlow;
  let title = '行情链路连续';
  let icon = '✓';
  if (model.sourceWarming) {
    severity = 'standby';
    title = model.metrics.upstream_stage === 'backfilling' ? '行情初始化中' : '等待上游行情';
    icon = '↻';
  } else if (model.globalFlow === 'bad') {
    title = '疑似全局断流';
    icon = '!';
  } else if (model.subscribedProblems.length > 0) {
    severity = 'error';
    title = `${model.subscribedProblems.length} 个订阅合约异常`;
    icon = '!';
  } else if (model.problems.length > 0 || model.invalidRows > 0) {
    severity = 'warning';
    title = '行情链路正常，存在局部异常';
    icon = '!';
  }
  hero.className = `panel hero ${severity}`;
  $('healthIcon').textContent = icon;
  $('healthTitle').textContent = title;
  const idle = model.idleMs == null ? '尚未收到上游帧' : `最近上游帧 ${formatDuration(model.idleMs)} 前`;
  const coverage = model.totalUniverse ? `${model.observed}/${model.totalUniverse}` : '--';
  const issue = model.issueCount === 0 ? '当前无完整性异常' : `当前关注 ${model.issueCount} 项`;
  $('healthSubtitle').innerHTML = `${idle} · 已观测合约 <b>${coverage}</b> · ${issue}`;
  const healthy = severity === 'live' || severity === '';
  $('liveChip').className = healthy ? 'live-chip' : 'live-chip offline';
  $('liveLabel').textContent = model.sourceWarming ? '初始化监控中' : model.globalFlow === 'bad' ? '断流告警' : '实时监控中';
  setDot('liveDot', healthy ? 'live' : severity === 'warning' || severity === 'standby' ? 'warn' : 'bad');
}

function renderDiagnostics(model) {
  const stageMap = {
    connecting: '连接中', subscribing: '订阅中', backfilling: '初始化', live: '已连接', degraded: '降级', down: '已断开',
  };
  const upstreamSeverity = model.globalFlow;
  $('upstreamState').textContent = stageMap[model.metrics.upstream_stage] || model.metrics.upstream_stage || '--';
  $('upstreamMeta').textContent = model.idleMs == null ? '尚无 frame' : `静默 ${formatDuration(model.idleMs)}`;
  setNodeState('upstreamState', upstreamSeverity); setDot('upstreamDot', upstreamSeverity);

  const universeSeverity = model.metrics.upstream_symbols > 0 ? (model.coverage >= 99 ? 'live' : model.coverage >= 90 ? 'warn' : 'bad') : 'warn';
  $('universeState').textContent = model.totalUniverse ? `${model.observed}/${model.totalUniverse}` : '--';
  $('universeMeta').textContent = backfillProgress(model.metrics) || `覆盖 ${model.coverage.toFixed(1)}%`;
  setNodeState('universeState', universeSeverity); setDot('universeDot', universeSeverity);

  const decoderSeverity = model.invalidRows > 0 ? 'bad' : 'live';
  $('decoderState').textContent = model.invalidRows > 0 ? `${fmtNumber.format(model.invalidRows)} 坏行` : '正常';
  $('decoderMeta').textContent = `${formatRate(model.rates.eventRate)} events/s`;
  setNodeState('decoderState', decoderSeverity); setDot('decoderDot', decoderSeverity);

  const cacheSeverity = model.problems.length > 0 ? 'warn' : model.rows.length > 0 ? 'live' : 'warn';
  $('cacheState').textContent = model.rows.length > 0 ? '持续更新' : '等待数据';
  $('cacheMeta').textContent = `${fmtNumber.format(model.rows.length)} 合约遥测`;
  setNodeState('cacheState', cacheSeverity); setDot('cacheDot', cacheSeverity);

  const downstreamSeverity = model.subscribedProblems.length > 0 ? 'bad' : 'live';
  $('downstreamState').textContent = `${fmtNumber.format(model.metrics.downstream_clients || 0)} 客户端`;
  $('downstreamMeta').textContent = `${fmtNumber.format((model.metrics.quote_subscriptions || 0) + (model.metrics.chart_subscriptions || 0))} 订阅`;
  setNodeState('downstreamState', downstreamSeverity); setDot('downstreamDot', downstreamSeverity);
}

function renderKpis(model) {
  $('frameRate').textContent = formatRate(model.rates.frameRate);
  $('eventRate').textContent = formatRate(model.rates.eventRate);
  $('coverage').textContent = model.totalUniverse ? model.coverage.toFixed(model.coverage >= 99.95 ? 0 : 1) : '--';
  $('issueCount').textContent = fmtNumber.format(model.issueCount);
  $('frameIdle').textContent = model.idleMs == null ? '--' : formatDuration(model.idleMs);
  $('decodeErrors').textContent = fmtNumber.format(model.invalidRows);
}

function attentionMessage(row) {
  if (row.invalid_rows > 0) return `${row.symbol} 已记录 ${row.invalid_rows} 条解码异常`;
  if (row.status === 'missing') return `${row.symbol} 尚未收到任何行情数据`;
  if (row.status === 'inactive') return `${row.symbol} 当前未被上游合约集合覆盖`;
  if (row.status === 'stale') return `${row.symbol} 已静默 ${formatDuration(row.receive_gap_ms)}`;
  return `${row.symbol} ${statusLabel(row.status)}`;
}

function renderAttention(model) {
  const container = $('attentionList');
  const rows = [...model.problems]
    .sort((a, b) => Number(b.subscribed) - Number(a.subscribed)
      || (b.invalid_rows || 0) - (a.invalid_rows || 0)
      || (b.receive_gap_ms || 0) - (a.receive_gap_ms || 0))
    .slice(0, 5);
  if (rows.length === 0) {
    container.innerHTML = '<div class="alert-card ok">当前没有需要关注的合约<span class="alert-time">实时采样</span></div>';
    return;
  }
  container.innerHTML = rows.map((row) => {
    const severity = severityForRow(row);
    const name = row.instrument_name ? ` · ${escapeHtml(row.instrument_name)}` : '';
    const sub = row.subscribed ? ' · 下游正在使用' : '';
    return `<div class="alert-card ${severity}">${escapeHtml(attentionMessage(row))}${name}${sub}<span class="alert-time">最近 ${formatTime(row.last_receive_unix_millis)}</span></div>`;
  }).join('');
}

function groupSeverity(rows) {
  if (rows.length === 0) return 'closed';
  if (rows.some((row) => row.invalid_rows > 0 || row.status === 'missing' || row.status === 'inactive')) return 'bad';
  if (rows.some((row) => row.status === 'stale')) return 'warn';
  if (rows.every((row) => row.status === 'closed')) return 'closed';
  return 'live';
}

function recordTimeline(model) {
  const exchangeStates = {};
  for (const exchange of EXCHANGES) {
    exchangeStates[exchange] = groupSeverity(model.rows.filter((row) => exchangeOf(row.symbol) === exchange));
  }
  const subscribed = model.rows.filter((row) => row.subscribed);
  state.timeline.push({ at: model.sampledAt, global: model.globalFlow, exchanges: exchangeStates, subscribed: groupSeverity(subscribed) });
  state.timeline = state.timeline.filter((sample) => model.sampledAt - sample.at <= 300_000).slice(-HISTORY_LIMIT);
}

function timelineSamples(now) {
  const bucketMs = 300_000 / TIMELINE_BUCKETS;
  const result = [];
  for (let index = 0; index < TIMELINE_BUCKETS; index += 1) {
    const start = now - 300_000 + index * bucketMs;
    const end = start + bucketMs;
    result.push(state.timeline.filter((sample) => sample.at >= start && sample.at < end).at(-1) || null);
  }
  return result;
}

function renderTimeline(model) {
  const samples = timelineSamples(model.sampledAt);
  const populatedExchanges = EXCHANGES.filter((exchange) => model.rows.some((row) => exchangeOf(row.symbol) === exchange)).slice(0, 4);
  const definitions = [
    ['全局', (sample) => sample.global],
    ...populatedExchanges.map((exchange) => [exchange, (sample) => sample.exchanges[exchange]]),
    ['下游订阅', (sample) => sample.subscribed],
  ];
  while (definitions.length < 6) definitions.splice(definitions.length - 1, 0, [`市场 ${definitions.length}`, () => 'closed']);
  const html = [];
  for (const [label, accessor] of definitions.slice(0, 6)) {
    html.push(`<div class="row-label">${escapeHtml(label)}</div>`);
    for (const sample of samples) html.push(`<div class="cell ${sample ? accessor(sample) : 'closed'}"></div>`);
  }
  const start = model.sampledAt - 300_000;
  const marks = [0, 1, 2, 3, 4, 5].map((n) => shanghaiMinute.format(new Date(start + n * 60_000)));
  html.push(`<div class="axis">${marks.map((mark) => `<span>${mark}</span>`).join('')}</div>`);
  $('timeline').innerHTML = html.join('');
}

function addEvent(type, scope, detail, impact, severity, at = Date.now()) {
  const key = `${type}|${scope}|${detail}`;
  if (state.events[0]?.key === key && at - state.events[0].at < 5000) return;
  state.events.unshift({ key, type, scope, detail, impact, severity, at });
  state.events = state.events.slice(0, 30);
}

function detectEvents(model) {
  if (state.lastStage !== null && state.lastStage !== model.metrics.upstream_stage) {
    addEvent('阶段切换', '上游', `${state.lastStage} → ${model.metrics.upstream_stage}`, '--', model.globalFlow === 'bad' ? 'bad' : 'blue', model.sampledAt);
  }
  state.lastStage = model.metrics.upstream_stage;
  if (state.lastGlobalFlow !== null && state.lastGlobalFlow !== model.globalFlow) {
    if (model.globalFlow === 'bad') addEvent('疑似断流', '全局', `上游 frame 静默 ${formatDuration(model.idleMs)}`, `${model.rows.length} 合约`, 'bad', model.sampledAt);
    else if (state.lastGlobalFlow === 'bad') addEvent('恢复', '全局', '上游行情流恢复', `${model.rows.length} 合约`, 'blue', model.sampledAt);
  }
  state.lastGlobalFlow = model.globalFlow;

  const current = new Map(model.rows.map((row) => [row.symbol, row.status]));
  if (state.knownStatuses.size > 0) {
    for (const row of model.rows) {
      const before = state.knownStatuses.get(row.symbol);
      if (before && before !== row.status) {
        if (PROBLEM_STATUSES.has(row.status)) {
          addEvent(statusLabel(row.status), row.symbol, `${statusLabel(before)} → ${statusLabel(row.status)}`, row.subscribed ? '影响订阅' : '未订阅', severityForRow(row), model.sampledAt);
        } else if (PROBLEM_STATUSES.has(before) && row.status === 'live') {
          addEvent('恢复', row.symbol, `${statusLabel(before)} → 正常`, '--', 'blue', model.sampledAt);
        }
      }
    }
  }
  state.knownStatuses = current;
}

function renderEvents() {
  const body = $('eventRows');
  if (state.events.length === 0) {
    body.innerHTML = '<tr><td class="empty-cell" colspan="5">本页尚未观测到状态变化</td></tr>';
    return;
  }
  body.innerHTML = state.events.slice(0, 6).map((event) => `<tr>
    <td>${formatTime(event.at)}</td><td title="${escapeHtml(event.scope)}">${escapeHtml(event.scope)}</td>
    <td><span class="badge ${event.severity}">${escapeHtml(event.type)}</span></td>
    <td title="${escapeHtml(event.detail)}">${escapeHtml(event.detail)}</td><td>${escapeHtml(event.impact)}</td>
  </tr>`).join('');
}

function renderRanking(model) {
  const body = $('rankingRows');
  const rows = [...model.rows]
    .sort((a, b) => Number(b.subscribed) - Number(a.subscribed)
      || Number(PROBLEM_STATUSES.has(b.status)) - Number(PROBLEM_STATUSES.has(a.status))
      || (b.ticks_ingested || 0) - (a.ticks_ingested || 0))
    .slice(0, 6);
  if (rows.length === 0) {
    body.innerHTML = '<tr><td class="empty-cell" colspan="7">等待合约数据</td></tr>';
    return;
  }
  body.innerHTML = rows.map((row) => {
    const severity = severityForRow(row);
    const risk = severity === 'bad' ? ['high', '高'] : severity === 'warn' ? ['mid', '中'] : ['', '低'];
    const subscriptions = (row.quote_subscriber_count || 0) + (row.chart_subscriber_count || 0);
    return `<tr>
      <td title="${escapeHtml(row.symbol)}">${escapeHtml(row.symbol)}</td>
      <td title="${escapeHtml(row.instrument_name || '--')}">${escapeHtml(row.instrument_name || '--')}</td>
      <td><span class="badge ${escapeHtml(row.status)}">${escapeHtml(statusLabel(row.status))}</span></td>
      <td>${formatDuration(row.receive_gap_ms)}</td><td>${fmtNumber.format(row.ticks_ingested || 0)}</td>
      <td>${subscriptions}</td><td><span class="risk ${risk[0]}"><i></i>${risk[1]}</span></td>
    </tr>`;
  }).join('');
}

function pushSample(model) {
  state.samples.push({
    at: model.sampledAt,
    frameRate: model.rates.frameRate,
    eventRate: model.rates.eventRate,
    score: model.score,
    coverage: model.coverage,
    issueCount: model.issueCount,
    idleMs: model.idleMs,
    invalidRows: model.invalidRows,
  });
  state.samples = state.samples.slice(-HISTORY_LIMIT);
}

function polyline(values, width, height, minValue, maxValue) {
  const valid = values.filter((value) => value !== null && Number.isFinite(value));
  if (valid.length < 2) return '';
  const min = minValue ?? Math.min(...valid);
  const max = maxValue ?? Math.max(...valid);
  const range = Math.max(0.0001, max - min);
  return values.map((value, index) => {
    const safe = value === null || !Number.isFinite(value) ? min : value;
    const x = values.length === 1 ? 0 : index / (values.length - 1) * width;
    const y = height - (safe - min) / range * height;
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(' ');
}

function sparkPoints(values) {
  const recent = values.slice(-30);
  if (recent.length < 2) return '0,10 160,10';
  return polyline(recent, 160, 18);
}

function renderSparks() {
  $('frameSpark').setAttribute('points', sparkPoints(state.samples.map((sample) => sample.frameRate)));
  $('eventSpark').setAttribute('points', sparkPoints(state.samples.map((sample) => sample.eventRate)));
  $('coverageSpark').setAttribute('points', sparkPoints(state.samples.map((sample) => sample.coverage)));
  $('issueSpark').setAttribute('points', sparkPoints(state.samples.map((sample) => sample.issueCount)));
  $('idleSpark').setAttribute('points', sparkPoints(state.samples.map((sample) => sample.idleMs)));
  $('errorSpark').setAttribute('points', sparkPoints(state.samples.map((sample) => sample.invalidRows)));
}

function renderTrend(model) {
  const recent = state.samples.slice(-60);
  const frameValues = recent.map((sample) => sample.frameRate);
  const eventValues = recent.map((sample) => sample.eventRate);
  const scoreValues = recent.map((sample) => sample.score);
  const rateMax = Math.max(1, ...frameValues.filter(Number.isFinite), ...eventValues.filter(Number.isFinite));
  $('frameTrend').setAttribute('points', polyline(frameValues, 800, 145, 0, rateMax));
  $('eventTrend').setAttribute('points', polyline(eventValues, 800, 145, 0, rateMax));
  $('scoreTrend').setAttribute('points', polyline(scoreValues, 800, 145, 0, 100));
  $('chartEmpty').style.display = recent.length >= 2 ? 'none' : 'grid';
  const ring = $('scoreRing');
  ring.style.setProperty('--angle', `${Math.max(0, Math.min(100, model.score)) * 3.6}deg`);
  ring.className = `ring ${model.score < 80 ? 'error' : model.score < 98 ? 'warning' : ''}`;
  $('scoreValue').textContent = `${model.score.toFixed(model.score >= 99.95 ? 0 : 1)}%`;
  $('scoreLabel').textContent = model.score >= 99.9 ? '极佳' : model.score >= 98 ? '稳定' : model.score >= 80 ? '关注' : '异常';
  const average = recent.reduce((sum, sample) => sum + sample.score, 0) / Math.max(1, recent.length);
  $('scoreAverage').textContent = `${average.toFixed(1)}%`;
}

function render(model) {
  renderHealth(model);
  renderDiagnostics(model);
  renderKpis(model);
  renderAttention(model);
  recordTimeline(model);
  renderTimeline(model);
  detectEvents(model);
  renderEvents();
  renderRanking(model);
  pushSample(model);
  renderSparks();
  renderTrend(model);
}

function params() {
  return 'sort=receive_gap_ms_desc&limit=5000';
}

async function fetchJson(path, signal) {
  const response = await fetch(path, { cache: 'no-store', signal });
  const body = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(body.error || `${path} 返回 HTTP ${response.status}`);
  return body;
}

async function load() {
  const requestSequence = ++state.sequence;
  state.controller?.abort();
  state.controller = new AbortController();
  try {
    const [metrics, data] = await Promise.all([
      fetchJson('/metrics', state.controller.signal),
      fetchJson(`/symbol-metrics?${params()}`, state.controller.signal),
    ]);
    if (requestSequence !== state.sequence) return;
    $('alert').style.display = 'none';
    render(deriveModel(metrics, data, Date.now()));
  } catch (error) {
    if (error.name === 'AbortError') return;
    $('alert').textContent = `监控数据读取失败：${error.message}`;
    $('alert').style.display = 'block';
    $('liveChip').className = 'live-chip offline';
    $('liveLabel').textContent = '监控连接中断';
    setDot('liveDot', 'bad');
  }
}

function updateClock() {
  $('clock').textContent = shanghaiClock.format(new Date()).replaceAll('/', '-');
}

function startPolling() {
  if (state.timer !== null) return;
  load();
  state.timer = window.setInterval(load, POLL_INTERVAL_MS);
}

function stopPolling() {
  if (state.timer !== null) window.clearInterval(state.timer);
  state.timer = null;
  state.controller?.abort();
}

document.addEventListener('visibilitychange', () => {
  if (document.hidden) stopPolling();
  else startPolling();
});

updateClock();
state.clockTimer = window.setInterval(updateClock, 1000);
startPolling();
