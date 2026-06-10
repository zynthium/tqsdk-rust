import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';

const source = fs.readFileSync('crates/tqsdk-relay/src/dashboard/app.js', 'utf8')
  .replace(/\nupdateClock\(\);\nstate\.clockTimer = window\.setInterval\(updateClock, 1000\);\nstartPolling\(\);\n?$/, '\n');

const context = {
  AbortController: class {
    abort() {}
  },
  Date,
  Intl,
  Math,
  Number,
  Set,
  Map,
  String,
  console,
  document: {
    addEventListener() {},
    getElementById() {
      return {
        className: '',
        innerHTML: '',
        style: { display: '', setProperty() {} },
        textContent: '',
        setAttribute() {},
      };
    },
  },
  fetch: async () => ({ ok: true, json: async () => ({}) }),
  window: {
    clearInterval() {},
    setInterval() { return 0; },
  },
};

vm.createContext(context);
vm.runInContext(source, context, { filename: 'app.js' });

const closedWithHistoricalDecode = {
  symbol: 'SHFE.au2602',
  instrument_name: '沪金2602',
  status: 'closed',
  in_universe: true,
  subscribed: true,
  quote_subscriber_count: 1,
  chart_subscriber_count: 0,
  ticks_ingested: 0,
  receive_gap_ms: 90_000,
  market_time_lag_ms: 90_000,
  last_receive_unix_millis: 1_700_000_000_000,
  invalid_rows: 7,
};

const activeStale = {
  symbol: 'DCE.m2609',
  instrument_name: '豆粕2609',
  status: 'stale',
  in_universe: true,
  subscribed: false,
  quote_subscriber_count: 0,
  chart_subscriber_count: 0,
  ticks_ingested: 1,
  receive_gap_ms: 90_000,
  market_time_lag_ms: 90_000,
  last_receive_unix_millis: 1_700_000_000_000,
  invalid_rows: 0,
};

const model = context.deriveModel(
  {
    upstream_stage: 'live',
    last_upstream_frame_unix_secs: 1_700_000_100,
    upstream_frames_received: 10,
    upstream_events_decoded: 20,
    upstream_invalid_tick_rows: 7,
    upstream_symbols: 2,
  },
  {
    data_stale_after_millis: 30_000,
    summary: { total: 2 },
    symbols: [closedWithHistoricalDecode, activeStale],
  },
  1_700_000_100_000,
);

assert.equal(context.severityForRow(closedWithHistoricalDecode), 'closed');
assert.equal(context.groupSeverity([closedWithHistoricalDecode]), 'closed');
assert.deepEqual(model.problems.map((row) => row.symbol), ['DCE.m2609']);
assert.equal(model.subscribedProblems.length, 0);
assert.equal(model.issueCount, 1);
