import { describe, expect, it } from 'vitest';
import { createIncidentLedger, updateIncidentLedger } from './incident-ledger';
import { deriveIntegrity } from './integrity-model';
import { metrics, NOW, row, symbolSnapshot } from '../test/fixtures';

describe('incident-ledger', () => {
  it('records status transitions once per symbol transition', () => {
    const ledger = createIncidentLedger(10);
    const live = deriveIntegrity(
      metrics(),
      symbolSnapshot([row({ symbol: 'DCE.m2609', instrument_name: '豆粕2609', status: 'live' })]),
      NOW,
    );
    const stale = deriveIntegrity(
      metrics(),
      symbolSnapshot([
        row({
          symbol: 'DCE.m2609',
          instrument_name: '豆粕2609',
          status: 'stale',
          problem: true,
          problem_severity: 'warn',
        }),
      ]),
      NOW + 2_000,
    );

    updateIncidentLedger(ledger, live);
    updateIncidentLedger(ledger, stale);
    updateIncidentLedger(ledger, stale);

    expect(ledger.incidents).toHaveLength(1);
    expect(ledger.incidents[0]).toMatchObject({
      scope: '豆粕2609',
      scope_symbol: 'DCE.m2609',
      type: '静默',
      impact: '未订阅',
      severity: 'warn',
    });
  });

  it('does not record incidents for diff row id diagnostics', () => {
    const ledger = createIncidentLedger(10);
    const clean = deriveIntegrity(
      metrics(),
      symbolSnapshot([row({ symbol: 'SHFE.au2602', gap_event_count: 0 })]),
      NOW,
    );
    const gapped = deriveIntegrity(
      metrics(),
      symbolSnapshot([
        row({
          symbol: 'SHFE.au2602',
          gap_event_count: 1,
          estimated_missing_rows: 2,
        }),
      ]),
      NOW + 2_000,
    );

    updateIncidentLedger(ledger, clean);
    updateIncidentLedger(ledger, gapped);
    updateIncidentLedger(ledger, gapped);

    expect(ledger.incidents).toHaveLength(0);
  });
});
