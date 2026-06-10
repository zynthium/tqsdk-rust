import { statusLabel } from './integrity-model';
import type { IncidentLedger, IntegrityModel, LocalIncident, SymbolRow, TimelineSeverity } from './types';

export function createIncidentLedger(limit = 80): IncidentLedger {
  return { limit, knownStatuses: new Map(), incidents: [] };
}

function severityForIncident(row: SymbolRow): TimelineSeverity {
  if (row.problem_severity === 'bad') return 'bad';
  if (row.problem_severity === 'warn') return 'warn';
  if (row.problem_severity === 'closed') return 'closed';
  return 'live';
}

export function updateIncidentLedger(ledger: IncidentLedger, model: IntegrityModel): IncidentLedger {
  for (const row of model.rows) {
    const before = ledger.knownStatuses.get(row.symbol);
    if (before && before !== row.status) {
      const incident: LocalIncident = {
        id: `${model.sampledAt}:${row.symbol}:${before}:${row.status}`,
        at: model.sampledAt,
        scope: row.symbol,
        type: statusLabel(row.status),
        detail: `${statusLabel(before)} -> ${statusLabel(row.status)}`,
        impact: row.subscribed ? '影响订阅' : '未订阅',
        severity: severityForIncident(row),
      };
      if (!ledger.incidents.some((item) => item.id === incident.id)) {
        ledger.incidents.unshift(incident);
      }
    }
    ledger.knownStatuses.set(row.symbol, row.status);
  }
  if (ledger.incidents.length > ledger.limit) {
    ledger.incidents.splice(ledger.limit);
  }
  return ledger;
}
