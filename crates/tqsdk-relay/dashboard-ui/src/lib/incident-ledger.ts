import { statusLabel } from './integrity-model';
import type { IncidentLedger, IntegrityModel, LocalIncident, SymbolRow, TimelineSeverity } from './types';

export function createIncidentLedger(limit = 80): IncidentLedger {
  return { limit, knownStatuses: new Map(), knownContinuity: new Map(), incidents: [] };
}

function severityForIncident(row: SymbolRow): TimelineSeverity {
  if (row.problem_severity === 'bad') return 'bad';
  if (row.problem_severity === 'warn') return 'warn';
  if (row.problem_severity === 'closed') return 'closed';
  return 'live';
}

export function updateIncidentLedger(ledger: IncidentLedger, model: IntegrityModel): IncidentLedger {
  for (const row of model.globalRows) {
    const continuityEvents =
      Number(row.gap_event_count || 0) +
      Number(row.duplicate_rows || 0) +
      Number(row.out_of_order_rows || 0);
    const knownContinuity = ledger.knownContinuity.get(row.symbol);
    if (continuityEvents > (knownContinuity ?? 0)) {
      const incident: LocalIncident = {
        id: `${model.sampledAt}:${row.symbol}:SymbolGapDetected:${continuityEvents}`,
        at: model.sampledAt,
        scope: row.instrument_name ?? row.symbol,
        scope_symbol: row.symbol,
        type: 'SymbolGapDetected',
        detail: `gap ${row.gap_event_count} / duplicate ${row.duplicate_rows} / out-of-order ${row.out_of_order_rows}`,
        impact: row.subscribed ? '影响订阅' : '未订阅',
        severity: 'bad',
      };
      if (!ledger.incidents.some((item) => item.id === incident.id)) {
        ledger.incidents.unshift(incident);
      }
    }
    ledger.knownContinuity.set(row.symbol, continuityEvents);

    const before = ledger.knownStatuses.get(row.symbol);
    if (before && before !== row.status) {
      const incident: LocalIncident = {
        id: `${model.sampledAt}:${row.symbol}:${before}:${row.status}`,
        at: model.sampledAt,
        scope: row.instrument_name ?? row.symbol,
        scope_symbol: row.symbol,
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
