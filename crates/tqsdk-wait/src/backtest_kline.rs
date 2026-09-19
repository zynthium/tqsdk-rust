//! Bounded Kline consumption over the shared official chart pager.
use std::collections::{BTreeMap, VecDeque};

use serde_json::{Value, json};
use tqsdk_core::{Chart, RuntimeReader, Symbol};
use tqsdk_session::{BacktestChartPager, MarketChartLease, SessionClient};

use super::TqBacktest;
use crate::error::{Result, WaitFacadeError};

#[derive(Debug, Default)]
pub(super) struct KlinePump {
    serials: BTreeMap<(Vec<String>, i64), Serial>,
}

#[derive(Debug)]
struct Serial {
    symbols: Vec<String>,
    duration: i64,
    handles: BTreeMap<String, usize>,
    pager: BacktestChartPager,
    leases: BTreeMap<String, MarketChartLease>,
    pending: bool,
    current: VecDeque<Row>,
    next: Option<Page>,
    close_phase: bool,
    last_loaded: Option<i64>,
    source_last_id: Option<i64>,
    terminal: bool,
    empty_ready: bool,
    done: bool,
}

#[derive(Debug)]
struct Page {
    rows: VecDeque<Row>,
    right: i64,
    terminal: bool,
}

#[derive(Debug)]
struct Row {
    id: i64,
    values: Vec<(String, i64, Value)>,
}

pub(super) struct Candidate {
    pub(super) datetime: i64,
}

impl KlinePump {
    pub(super) async fn subscribe(
        &mut self,
        session: &SessionClient,
        backtest: &TqBacktest,
        symbols: Vec<String>,
        duration: i64,
        width: usize,
        chart_id: String,
    ) -> Result<()> {
        let key = (symbols.clone(), duration);
        if let Some(serial) = self.serials.get_mut(&key) {
            serial.handles.insert(chart_id, width);
            return Ok(());
        }
        let pager = BacktestChartPager::new(
            format!("{chart_id}--source"),
            symbols.iter().map(Symbol::new).collect(),
            duration,
            backtest.start_datetime_ns(),
        );
        let mut serial = Serial {
            symbols,
            duration,
            handles: BTreeMap::from([(chart_id, width)]),
            pager,
            leases: BTreeMap::new(),
            pending: false,
            current: VecDeque::new(),
            next: None,
            close_phase: false,
            last_loaded: None,
            source_last_id: None,
            terminal: false,
            empty_ready: false,
            done: false,
        };
        serial.send(session).await?;
        self.serials.insert(key, serial);
        Ok(())
    }

    pub(super) fn touches(&self, commit: &tqsdk_core::CommitResult) -> bool {
        commit.changes.path_hits.iter().any(|p| {
            let parts = p.segments();
            self.serials.values().any(|s| match parts {
                [root, symbol, duration, ..] if root == "klines" => {
                    s.symbols.contains(symbol) && duration == &s.duration.to_string()
                }
                [root, id, ..] if root == "charts" => s.pager.chart_ids().contains(id),
                [root] if root == "mdhis_more_data" => s.pending,
                _ => false,
            })
        })
    }

    /// `false` means an unresolved serial could still precede every candidate.
    pub(super) async fn poll(
        &mut self,
        session: &SessionClient,
        reader: &RuntimeReader,
        backtest: &TqBacktest,
    ) -> Result<(bool, Option<Candidate>)> {
        let mut best: Option<Candidate> = None;
        let mut ready = true;
        for serial in self.serials.values_mut() {
            serial.load(reader)?;
            if serial.empty_ready {
                best = Some(Candidate {
                    datetime: backtest.start_datetime_ns(),
                });
                continue;
            }
            loop {
                if serial.done {
                    break;
                }
                if serial.current.is_empty() {
                    if serial.terminal {
                        serial.finish().await?;
                        break;
                    }
                    if let Some(page) = serial.next.take() {
                        serial.current = page.rows;
                        serial.terminal = page.terminal;
                        serial.pager.advance(page.right);
                        // Match the official generator: prefetch even the final
                        // nonempty page before inspecting timestamps.
                        serial.send(session).await?;
                    } else if serial.pending {
                        ready = false;
                        break;
                    } else {
                        serial.finish().await?;
                        break;
                    }
                }
                let Some(row) = serial.current.front() else {
                    continue;
                };
                let datetime = event_time(row, serial.duration, serial.close_phase)?;
                if datetime >= backtest.end_datetime_ns() {
                    serial.finish().await?;
                    break;
                }
                let datetime = datetime.max(backtest.start_datetime_ns());
                if best.as_ref().is_none_or(|b| datetime < b.datetime) {
                    best = Some(Candidate { datetime });
                }
                break;
            }
        }
        Ok((ready, best))
    }

    pub(super) fn take_at(&mut self, datetime: i64) -> Result<Vec<Value>> {
        let mut diffs = Vec::new();
        for serial in self.serials.values_mut() {
            if serial.empty_ready {
                serial.empty_ready = false;
                let charts = serial.handles.keys().map(|id| {
                    (id.clone(), json!({
                        "state": {"ins_list": serial.symbols.join(","), "duration": serial.duration},
                        "left_id": -1, "right_id": -1, "ready": true, "more_data": false
                    }))
                }).collect::<serde_json::Map<_, _>>();
                diffs.push(json!({"charts": charts}));
            }
            while let Some(row) = serial.current.front() {
                if event_time(row, serial.duration, serial.close_phase)? > datetime {
                    break;
                }
                diffs.push(serial.take_diff());
            }
        }
        Ok(diffs)
    }
}

impl Serial {
    fn take_diff(&mut self) -> Value {
        let serial = self;
        let row = serial.current.front().expect("candidate retains its row");
        let mut charts = serde_json::Map::new();
        for (id, width) in &serial.handles {
            charts.insert(id.clone(), json!({"state": {"ins_list": serial.symbols.join(","), "duration": serial.duration},
                "left_id": row.id.saturating_sub(width.saturating_sub(1) as i64).max(0), "right_id": row.id,
                "ready": true, "more_data": false}));
        }
        let mut klines = serde_json::Map::new();
        let mut binding = serde_json::Map::new();
        for (symbol, id, original) in &row.values {
            let mut value = original.clone();
            if !serial.close_phase {
                for field in ["high", "low", "close"] {
                    value[field] = original["open"].clone();
                }
                value["volume"] = json!(0);
                value["close_oi"] = original["open_oi"].clone();
            }
            klines.insert(symbol.clone(), json!({serial.duration.to_string(): {"last_id": id, "data": {id.to_string(): value}}}));
            if symbol != &serial.symbols[0] {
                binding.insert(symbol.clone(), json!({row.id.to_string(): id.to_string()}));
            }
        }
        klines.get_mut(&serial.symbols[0]).unwrap()[serial.duration.to_string()]["binding"] =
            Value::Object(binding);
        let diff = json!({"charts": charts, "klines": klines});
        serial.consume();
        diff
    }
}

impl Serial {
    async fn send(&mut self, session: &SessionClient) -> Result<()> {
        let command = self.pager.command().clone();
        if let Some(lease) = self.leases.get_mut(&command.chart_id) {
            lease.update(command).await?;
        } else {
            self.leases.insert(
                command.chart_id.clone(),
                session.ensure_chart(command).await?,
            );
        }
        self.pending = true;
        Ok(())
    }

    fn load(&mut self, reader: &RuntimeReader) -> Result<()> {
        if !self.pending || self.done {
            return Ok(());
        }
        let market = reader.read_market_state();
        let Some(chart) =
            market.decode_path::<Chart>(&["charts", &self.pager.command().chart_id])?
        else {
            return Ok(());
        };
        if !chart.ready
            || chart.more_data
            || !self.pager.matches(&chart)
            || market
                .get_path(&["mdhis_more_data"])
                .and_then(Value::as_bool)
                != Some(false)
        {
            return Ok(());
        }
        let duration = self.duration.to_string();
        let Some(main) = market.get_path(&["klines", &self.symbols[0], &duration]) else {
            return Ok(());
        };
        if let Some(last) = main["last_id"].as_i64() {
            self.source_last_id = Some(self.source_last_id.unwrap_or(-1).max(last));
        }
        if chart.left_id == -1
            && chart.right_id == -1
            && main["last_id"] == -1
            && main["data"].as_object().is_some_and(|v| v.is_empty())
        {
            self.pending = false;
            self.empty_ready = self.last_loaded.is_none();
            return Ok(());
        }
        let Some(data) = main["data"].as_object() else {
            return Ok(());
        };
        if chart.left_id < 0 || chart.right_id < chart.left_id {
            return Ok(());
        }
        // A header can precede row DIFFs. Never turn an incomplete window into
        // a terminal, especially after the synthetic last_id has rewound.
        let Some(source_last_id) = self.source_last_id else {
            return Ok(());
        };
        let right = chart.right_id.min(source_last_id);
        if chart.left_id <= right
            && (!data.contains_key(&chart.left_id.to_string())
                || !data.contains_key(&right.to_string()))
        {
            return Ok(());
        }
        let mut rows = Vec::new();
        for (id, value) in data {
            let Ok(id) = id.parse::<i64>() else {
                continue;
            };
            if id < chart.left_id || id > right || self.last_loaded.is_some_and(|last| id <= last) {
                continue;
            }
            let mut values = vec![(self.symbols[0].clone(), id, value.clone())];
            for symbol in self.symbols.iter().skip(1) {
                let binding = &main["binding"][symbol][id.to_string()];
                let other_id = binding
                    .as_i64()
                    .or_else(|| binding.as_str().and_then(|s| s.parse().ok()))
                    .unwrap_or(-1);
                if other_id < 0 {
                    continue;
                }
                let Some(other) =
                    market.get_path(&["klines", symbol, &duration, "data", &other_id.to_string()])
                else {
                    return Ok(());
                };
                values.push((symbol.clone(), other_id, other.clone()));
            }
            rows.push(Row { id, values });
        }
        rows.sort_by_key(|row| row.id);
        if rows.len() > tqsdk_session::BACKTEST_PAGE_WIDTH {
            return Err(WaitFacadeError::InvalidState(
                "backtest Kline page exceeds official width",
            ));
        }
        self.pending = false;
        if let Some(last) = rows.last() {
            self.last_loaded = Some(last.id);
        }
        self.next = (!rows.is_empty()).then(|| Page {
            rows: rows.into(),
            right: chart.right_id,
            terminal: source_last_id < chart.right_id,
        });
        Ok(())
    }

    fn consume(&mut self) {
        if self.close_phase {
            self.current.pop_front();
        }
        self.close_phase = !self.close_phase;
    }

    async fn finish(&mut self) -> Result<()> {
        let mut error = None;
        for lease in std::mem::take(&mut self.leases).into_values() {
            if let Err(failure) = lease.close().await {
                error.get_or_insert(failure);
            }
        }
        self.current.clear();
        self.next = None;
        self.done = true;
        error.map_or(Ok(()), |error| Err(error.into()))
    }
}

impl Drop for KlinePump {
    fn drop(&mut self) {
        let leases = self
            .serials
            .values_mut()
            .flat_map(|s| std::mem::take(&mut s.leases).into_values())
            .collect::<Vec<_>>();
        if !leases.is_empty()
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            runtime.spawn(async move {
                for lease in leases {
                    let _ = lease.close().await;
                }
            });
        }
    }
}

fn event_time(row: &Row, duration: i64, close: bool) -> Result<i64> {
    let datetime = row.values[0].2["datetime"]
        .as_i64()
        .ok_or(WaitFacadeError::InvalidState(
            "backtest Kline missing datetime",
        ))?;
    let time = if close {
        datetime.checked_add(duration)
    } else {
        Some(datetime)
    }
    .ok_or(WaitFacadeError::InvalidState(
        "backtest Kline time overflow",
    ))?;
    let time = if duration >= 86_400_000_000_000 {
        // Python _get_trading_day_start_time: previous evening, skipping weekends.
        let start = time
            .checked_sub(21_600_000_000_000)
            .ok_or(WaitFacadeError::InvalidState("backtest day overflow"))?;
        let weekday = (i128::from(start) - 631_123_200_000_000_000_i128)
            .div_euclid(86_400_000_000_000)
            .rem_euclid(7);
        start.checked_sub(if weekday >= 5 {
            (weekday as i64 - 4) * 86_400_000_000_000
        } else {
            0
        })
    } else {
        Some(time)
    }
    .ok_or(WaitFacadeError::InvalidState("backtest day overflow"))?;
    if close {
        time.checked_sub(1000)
            .ok_or(WaitFacadeError::InvalidState("backtest close overflow"))
    } else {
        Ok(time)
    }
}
