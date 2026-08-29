//! Relay-private history listener observability.
//!
//! The state here is deliberately a read model.  It never owns a snapshot or
//! participates in generation selection; `SnapshotSlot` publishes updates only
//! after its existing swap decision has completed.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use serde::Serialize;
use serde_json::{Value, to_value};

#[derive(Clone, Debug, Serialize)]
pub(super) struct HistoryObservabilitySnapshot {
    pub configured: bool,
    pub listener: bool,
    pub snapshot_id: Option<String>,
    pub ready: bool,
    pub healthy: bool,
    pub degraded: bool,
    pub active: usize,
    pub queued: usize,
    pub query_total: u64,
    pub query_duration_ms_total: u64,
    pub query_by_endpoint: BTreeMap<&'static str, u64>,
    pub query_by_status_class: BTreeMap<&'static str, u64>,
    pub query_by_error_code: BTreeMap<&'static str, u64>,
    pub buffer_used_bytes: usize,
    pub buffer_limit_bytes: usize,
    pub buffer_high_water_bytes: usize,
    pub compression_enabled: bool,
    pub compression_queued: usize,
    pub compression_active: usize,
    pub compression_success_total: u64,
    pub compression_fallback_total: u64,
    pub compression_failure_total: u64,
    pub reload_attempt_total: u64,
    pub reload_success_total: u64,
    pub reload_failure_total: u64,
    pub reload_last_code: &'static str,
}

#[derive(Debug)]
struct State {
    snapshot: HistoryObservabilitySnapshot,
}

/// A binary-private, single-source history read model.
#[derive(Clone, Debug)]
pub(super) struct HistoryObservability {
    state: Arc<Mutex<State>>,
    audit: Arc<dyn HistoryAuditSink>,
}

impl HistoryObservability {
    pub(super) fn enabled(compression_enabled: bool) -> Self {
        Self::new(true, compression_enabled, Arc::new(JsonLineAuditSink))
    }

    #[cfg(test)]
    pub(super) fn with_audit(compression_enabled: bool, audit: Arc<dyn HistoryAuditSink>) -> Self {
        Self::new(true, compression_enabled, audit)
    }

    fn new(configured: bool, compression_enabled: bool, audit: Arc<dyn HistoryAuditSink>) -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                snapshot: HistoryObservabilitySnapshot {
                    configured,
                    listener: false,
                    snapshot_id: None,
                    ready: false,
                    healthy: false,
                    degraded: false,
                    active: 0,
                    queued: 0,
                    query_total: 0,
                    query_duration_ms_total: 0,
                    query_by_endpoint: BTreeMap::new(),
                    query_by_status_class: BTreeMap::new(),
                    query_by_error_code: BTreeMap::new(),
                    buffer_used_bytes: 0,
                    buffer_limit_bytes: 0,
                    buffer_high_water_bytes: 0,
                    compression_enabled,
                    compression_queued: 0,
                    compression_active: 0,
                    compression_success_total: 0,
                    compression_fallback_total: 0,
                    compression_failure_total: 0,
                    reload_attempt_total: 0,
                    reload_success_total: 0,
                    reload_failure_total: 0,
                    reload_last_code: "none",
                },
            })),
            audit,
        }
    }

    pub(super) fn snapshot(&self) -> HistoryObservabilitySnapshot {
        self.state
            .lock()
            .expect("history observability lock poisoned")
            .snapshot
            .clone()
    }

    pub(super) fn json_snapshot(&self) -> Value {
        to_value(self.snapshot()).expect("history observability snapshot is serializable")
    }

    pub(super) fn listener_started(&self) {
        self.with_state(|snapshot| snapshot.listener = true);
    }

    pub(super) fn listener_stopped(&self) {
        self.with_state(|snapshot| {
            snapshot.listener = false;
            snapshot.ready = false;
        });
    }

    pub(super) fn note_reload_attempt(&self) {
        self.with_state(|s| s.reload_attempt_total += 1);
    }

    pub(super) fn note_reload_success(&self, snapshot_id: &str) {
        let snapshot_id = snapshot_id.to_owned();
        self.with_state(|s| {
            s.reload_success_total += 1;
            s.reload_last_code = "ok";
            s.snapshot_id = Some(snapshot_id);
            s.healthy = true;
            s.ready = s.listener;
            s.degraded = false;
        });
    }

    pub(super) fn note_reload_unchanged(&self, snapshot_id: &str) {
        let snapshot_id = snapshot_id.to_owned();
        self.with_state(|s| {
            s.reload_success_total += 1;
            s.reload_last_code = "ok";
            s.snapshot_id = Some(snapshot_id);
            s.ready = s.listener && s.healthy;
            s.degraded = !s.healthy;
        });
    }

    pub(super) fn note_reload_failure(&self, code: &'static str) {
        self.with_state(|s| {
            s.reload_failure_total += 1;
            s.reload_last_code = code;
            // A failed replacement must not erase last-good readiness.
            s.degraded = s.snapshot_id.is_some();
        });
    }

    pub(super) fn note_corrupt(&self, snapshot_id: &str) {
        let snapshot_id = snapshot_id.to_owned();
        self.with_state(|s| {
            if s.snapshot_id.as_deref() == Some(snapshot_id.as_str()) {
                s.healthy = false;
                s.ready = false;
                s.degraded = true;
            }
        });
    }

    pub(super) fn note_buffers(&self, used: usize, limit: usize, high_water: usize) {
        self.with_state(|s| {
            s.buffer_used_bytes = used;
            s.buffer_limit_bytes = limit;
            s.buffer_high_water_bytes = high_water;
        });
    }

    pub(super) fn request_queued(&self) -> Gauge {
        self.gauge(GaugeKind::Queued)
    }
    pub(super) fn request_active(&self) -> Gauge {
        self.gauge(GaugeKind::Active)
    }
    pub(super) fn compression_queued(&self) -> Gauge {
        self.gauge(GaugeKind::CompressionQueued)
    }
    pub(super) fn compression_active(&self) -> Gauge {
        self.gauge(GaugeKind::CompressionActive)
    }
    pub(super) fn compression_success(&self) {
        self.with_state(|s| s.compression_success_total += 1);
    }
    pub(super) fn compression_failure(&self) {
        self.with_state(|s| s.compression_failure_total += 1);
    }

    pub(super) fn compression_fallback(&self) {
        self.with_state(|s| s.compression_fallback_total += 1);
    }

    pub(super) fn begin_request(&self, request_id: String) -> RequestAudit {
        RequestAudit {
            observability: self.clone(),
            request_id,
            started: Instant::now(),
            endpoint: "unknown",
            trusted_identity: None,
            snapshot_id: None,
            symbol: None,
            series: None,
            period: None,
            range: None,
            projected_fields: Vec::new(),
            projected_field_count: None,
            rows: None,
            status: None,
            complete: false,
        }
    }

    fn gauge(&self, kind: GaugeKind) -> Gauge {
        self.with_state(|s| match kind {
            GaugeKind::Queued => s.queued += 1,
            GaugeKind::Active => s.active += 1,
            GaugeKind::CompressionQueued => s.compression_queued += 1,
            GaugeKind::CompressionActive => s.compression_active += 1,
        });
        Gauge {
            observability: self.clone(),
            kind: Some(kind),
        }
    }

    fn with_state(&self, update: impl FnOnce(&mut HistoryObservabilitySnapshot)) {
        update(
            &mut self
                .state
                .lock()
                .expect("history observability lock poisoned")
                .snapshot,
        );
    }

    fn emit(&self, record: HistoryAuditRecord) {
        self.audit.emit(record);
    }

    fn record_query(
        &self,
        endpoint: &'static str,
        status: u16,
        error_code: Option<&'static str>,
        duration_ms: u64,
    ) {
        let status_class = match status {
            499 => "cancelled",
            200..=399 => "success",
            400..=499 => "client_error",
            _ => "server_error",
        };
        self.with_state(|snapshot| {
            snapshot.query_total = snapshot.query_total.saturating_add(1);
            snapshot.query_duration_ms_total =
                snapshot.query_duration_ms_total.saturating_add(duration_ms);
            *snapshot.query_by_endpoint.entry(endpoint).or_default() += 1;
            *snapshot
                .query_by_status_class
                .entry(status_class)
                .or_default() += 1;
            if let Some(error_code) = error_code {
                *snapshot.query_by_error_code.entry(error_code).or_default() += 1;
            }
        });
    }
}

#[derive(Clone, Copy)]
enum GaugeKind {
    Queued,
    Active,
    CompressionQueued,
    CompressionActive,
}

pub(super) struct Gauge {
    observability: HistoryObservability,
    kind: Option<GaugeKind>,
}
impl Drop for Gauge {
    fn drop(&mut self) {
        let Some(kind) = self.kind.take() else {
            return;
        };
        self.observability.with_state(|s| match kind {
            GaugeKind::Queued => s.queued = s.queued.saturating_sub(1),
            GaugeKind::Active => s.active = s.active.saturating_sub(1),
            GaugeKind::CompressionQueued => {
                s.compression_queued = s.compression_queued.saturating_sub(1)
            }
            GaugeKind::CompressionActive => {
                s.compression_active = s.compression_active.saturating_sub(1)
            }
        });
    }
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct HistoryAuditRecord {
    pub(super) request_id: String,
    pub(super) trusted_identity: Option<String>,
    pub(super) endpoint: &'static str,
    pub(super) snapshot_id: Option<String>,
    pub(super) symbol: Option<String>,
    pub(super) series: Option<String>,
    pub(super) period: Option<String>,
    pub(super) range: Option<(String, String)>,
    pub(super) projected_fields: Vec<&'static str>,
    pub(super) projected_field_count: Option<usize>,
    pub(super) rows: Option<usize>,
    pub(super) selected_representation_bytes: usize,
    pub(super) duration_ms: u64,
    pub(super) status: u16,
    pub(super) error_code: Option<&'static str>,
}

pub(super) trait HistoryAuditSink: Send + Sync + std::fmt::Debug {
    fn emit(&self, record: HistoryAuditRecord);
}

#[derive(Debug)]
struct JsonLineAuditSink;
impl HistoryAuditSink for JsonLineAuditSink {
    fn emit(&self, record: HistoryAuditRecord) {
        // Never serialize untrusted request headers or error messages.
        if let Ok(line) = serde_json::to_string(&record) {
            eprintln!("history_audit={line}");
        }
    }
}

#[cfg(test)]
#[derive(Debug, Default)]
pub(super) struct MemoryAuditSink(Mutex<Vec<HistoryAuditRecord>>);
#[cfg(test)]
impl HistoryAuditSink for MemoryAuditSink {
    fn emit(&self, record: HistoryAuditRecord) {
        self.0.lock().unwrap().push(record);
    }
}
#[cfg(test)]
impl MemoryAuditSink {
    pub(super) fn len(&self) -> usize {
        self.0.lock().unwrap().len()
    }

    pub(super) fn records(&self) -> Vec<HistoryAuditRecord> {
        self.0.lock().unwrap().clone()
    }
}

pub(super) struct RequestAudit {
    observability: HistoryObservability,
    request_id: String,
    started: Instant,
    endpoint: &'static str,
    trusted_identity: Option<String>,
    snapshot_id: Option<String>,
    symbol: Option<String>,
    series: Option<String>,
    period: Option<String>,
    range: Option<(String, String)>,
    projected_fields: Vec<&'static str>,
    projected_field_count: Option<usize>,
    rows: Option<usize>,
    status: Option<u16>,
    complete: bool,
}
impl RequestAudit {
    pub(super) fn endpoint(&mut self, endpoint: &'static str) {
        self.endpoint = endpoint;
    }

    pub(super) fn identity(&mut self, identity: &str) {
        self.trusted_identity = Some(identity.to_owned());
    }

    pub(super) fn query(
        &mut self,
        snapshot_id: Option<&str>,
        symbol: &str,
        series: &str,
        period: Option<&str>,
        range: (&str, &str),
        projected_fields: Vec<&'static str>,
    ) {
        self.snapshot_id = snapshot_id.map(ToOwned::to_owned);
        self.symbol = Some(symbol.to_owned());
        self.series = Some(series.to_owned());
        self.period = period.map(ToOwned::to_owned);
        self.range = Some((range.0.to_owned(), range.1.to_owned()));
        self.projected_field_count = Some(projected_fields.len());
        self.projected_fields = projected_fields;
    }

    pub(super) fn rows(&mut self, rows: usize) {
        self.rows = Some(rows);
    }
    pub(super) fn finish(mut self, status: u16, error_code: Option<&'static str>, bytes: usize) {
        self.status = Some(status);
        self.complete = true;
        let duration_ms = self.started.elapsed().as_millis() as u64;
        self.observability
            .record_query(self.endpoint, status, error_code, duration_ms);
        self.observability.emit(HistoryAuditRecord {
            request_id: self.request_id.clone(),
            trusted_identity: self.trusted_identity.clone(),
            endpoint: self.endpoint,
            snapshot_id: self.snapshot_id.clone(),
            symbol: self.symbol.clone(),
            series: self.series.clone(),
            period: self.period.clone(),
            range: self.range.clone(),
            projected_fields: self.projected_fields.clone(),
            projected_field_count: self.projected_field_count,
            rows: self.rows,
            selected_representation_bytes: bytes,
            duration_ms,
            status,
            error_code,
        });
    }
}
impl Drop for RequestAudit {
    fn drop(&mut self) {
        if self.complete {
            return;
        }
        let status = self.status.unwrap_or(499);
        let duration_ms = self.started.elapsed().as_millis() as u64;
        self.observability.record_query(
            self.endpoint,
            status,
            Some("request_cancelled"),
            duration_ms,
        );
        self.observability.emit(HistoryAuditRecord {
            request_id: self.request_id.clone(),
            trusted_identity: self.trusted_identity.clone(),
            endpoint: self.endpoint,
            snapshot_id: self.snapshot_id.clone(),
            symbol: self.symbol.clone(),
            series: self.series.clone(),
            period: self.period.clone(),
            range: self.range.clone(),
            projected_fields: self.projected_fields.clone(),
            projected_field_count: self.projected_field_count,
            rows: self.rows,
            selected_representation_bytes: 0,
            duration_ms,
            status,
            error_code: Some("request_cancelled"),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::{HistoryObservability, MemoryAuditSink};
    use std::sync::Arc;

    #[test]
    fn readiness_tracks_listener_generation_health_and_recovery() {
        let sink = Arc::new(MemoryAuditSink::default());
        let observability = HistoryObservability::with_audit(false, sink);
        assert!(!observability.snapshot().ready);

        observability.listener_started();
        observability.note_reload_failure("snapshot_open_failed");
        let initial_failure = observability.snapshot();
        assert!(!initial_failure.ready);
        assert!(!initial_failure.degraded);

        observability.note_reload_success("snapshot-a");
        assert!(observability.snapshot().ready);
        observability.note_reload_failure("snapshot_open_failed");
        let retained = observability.snapshot();
        assert!(retained.ready);
        assert!(retained.degraded);

        observability.note_corrupt("snapshot-other");
        assert!(observability.snapshot().ready);
        observability.note_corrupt("snapshot-a");
        assert!(!observability.snapshot().ready);
        observability.note_reload_unchanged("snapshot-a");
        assert!(!observability.snapshot().ready);

        observability.note_reload_success("snapshot-b");
        assert!(observability.snapshot().ready);
        observability.listener_stopped();
        assert!(!observability.snapshot().ready);
    }

    #[test]
    fn gauges_follow_raii_and_audit_finishes_once() {
        let sink = Arc::new(MemoryAuditSink::default());
        let observability = HistoryObservability::with_audit(false, sink.clone());
        observability.listener_started();
        let queue = observability.request_queued();
        let active = observability.request_active();
        assert_eq!(observability.snapshot().queued, 1);
        assert_eq!(observability.snapshot().active, 1);
        drop(queue);
        drop(active);
        let mut audit = observability.begin_request("r-1".to_string());
        audit.endpoint("query");
        audit.identity("desk-a");
        audit.query(
            Some("snapshot-a"),
            "SHFE.au2612",
            "tick",
            None,
            ("2026-08-01T09:00:00+08:00", "2026-08-01T10:00:00+08:00"),
            vec!["t", "id", "lp"],
        );
        audit.rows(2);
        audit.finish(200, None, 17);
        let mut cancelled = observability.begin_request("r-2".to_string());
        cancelled.endpoint("coverage");
        cancelled.identity("desk-b");
        cancelled.query(
            Some("snapshot-b"),
            "KQ.m@SHFE.au",
            "kline",
            Some("1m"),
            ("2026-08-01T09:00:00+08:00", "2026-08-01T10:00:00+08:00"),
            vec!["t", "o", "h", "l", "c", "v", "oi"],
        );
        drop(cancelled);
        let records = sink.records();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].trusted_identity.as_deref(), Some("desk-a"));
        assert_eq!(records[0].snapshot_id.as_deref(), Some("snapshot-a"));
        assert_eq!(records[0].symbol.as_deref(), Some("SHFE.au2612"));
        assert_eq!(records[0].projected_fields, vec!["t", "id", "lp"]);
        assert_eq!(records[0].rows, Some(2));
        assert_eq!(records[0].selected_representation_bytes, 17);
        assert_eq!(records[0].error_code, None);
        assert_eq!(records[1].trusted_identity.as_deref(), Some("desk-b"));
        assert_eq!(records[1].snapshot_id.as_deref(), Some("snapshot-b"));
        assert_eq!(records[1].period.as_deref(), Some("1m"));
        assert_eq!(records[1].status, 499);
        assert_eq!(records[1].error_code, Some("request_cancelled"));
        let metrics = observability.snapshot();
        assert_eq!(metrics.query_total, 2);
        assert_eq!(metrics.query_by_endpoint.get("query"), Some(&1));
        assert_eq!(metrics.query_by_endpoint.get("coverage"), Some(&1));
        assert_eq!(metrics.query_by_status_class.get("success"), Some(&1));
        assert_eq!(metrics.query_by_status_class.get("cancelled"), Some(&1));
        assert_eq!(
            metrics.query_by_error_code.get("request_cancelled"),
            Some(&1)
        );
        assert_eq!(observability.snapshot().active, 0);
    }
}
