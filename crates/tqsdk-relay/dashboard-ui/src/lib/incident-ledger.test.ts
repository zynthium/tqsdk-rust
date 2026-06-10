import { describe, expect, it } from 'vitest';
import { createIncidentLedger, updateIncidentLedger } from './incident-ledger';
import { deriveIntegrity } from './integrity-model';
import { metrics, NOW, row, symbolSnapshot } from '../test/fixtures';

describe('incident-ledger', () => {
  it('records status transitions once per symbol transition', () => {
    const ledger = createIncidentLedger(10);
    const live = deriveIntegrity(metrics(), symbolSnapshot([row({ symbol: 'DCE.m2609', status: 'live' })]), NOW);
    const stale = deriveIntegrity(
      metrics(),
      symbolSnapshot([
        row({ symbol: 'DCE.m2609', status: 'stale', problem: true, problem_severity: 'warn' }),
      ]),
      NOW + 2_000,
    );

    updateIncidentLedger(ledger, live);
    updateIncidentLedger(ledger, stale);
    updateIncidentLedger(ledger, stale);

    expect(ledger.incidents).toHaveLength(1);
    expect(ledger.incidents[0]).toMatchObject({
      scope: 'DCE.m2609',
      type: '静默',
      impact: '未订阅',
      severity: 'warn',
    });
  });
});
