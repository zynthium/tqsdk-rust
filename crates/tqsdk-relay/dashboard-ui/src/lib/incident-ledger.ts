import { statusLabel } from './integrity-model';
import type { IncidentLedger, IntegrityModel, LocalIncident, RelayEvent, SymbolRow, TimelineSeverity } from './types';

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
  for (const row of model.globalRows) {
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

function severityForRelayEvent(event: RelayEvent): TimelineSeverity {
  if (event.kind === 'universe_refresh_failed' || event.kind === 'decode_incident') return 'bad';
  if (event.kind === 'flow_incident') return 'warn';
  return 'live';
}

function typeForRelayEvent(event: RelayEvent): string {
  return {
    universe_refreshed: 'Universe',
    universe_refresh_failed: 'Universe失败',
    flow_incident: '流状态',
    decode_incident: '解码',
  }[event.kind];
}

export function relayEventsToIncidents(events: RelayEvent[]): LocalIncident[] {
  return [...events]
    .sort((left, right) => right.sequence - left.sequence)
    .map((event) => ({
      id: `relay:${event.sequence}`,
      at: event.at_unix_secs * 1_000,
      scope: typeForRelayEvent(event),
      scope_symbol: event.kind,
      type: typeForRelayEvent(event),
      detail: event.detail,
      impact: '后端事件',
      severity: severityForRelayEvent(event),
    }));
}
