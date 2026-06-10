#![cfg_attr(not(test), forbid(unsafe_code))]

use std::time::Duration;

use chrono::{FixedOffset, NaiveDateTime, TimeZone, Utc};
use tqsdk_core::Quote;
use tqsdk_session::{InstrumentClass, SymbolInfo};

use crate::error::{DataError, Result};

const DEFAULT_OPTION_GREEKS_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_IMPLIED_VOLATILITY_SEED: f64 = 0.3;
const IMPLIED_VOLATILITY_EPSILON: f64 = 1e-8;
const IMPLIED_VOLATILITY_MIN_VEGA: f64 = 1e-8;
const IMPLIED_VOLATILITY_MAX_ITERATIONS: usize = 128;
const CST_OFFSET_SECONDS: i32 = 8 * 60 * 60;
const QUOTE_DATETIME_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.f";

/// Request for a one-shot owned option greeks query.
#[derive(Debug, Clone)]
pub struct OptionGreeksRequest {
    symbols: Vec<String>,
    volatilities: Option<Vec<f64>>,
    risk_free_rate: f64,
    timeout: Duration,
}

impl OptionGreeksRequest {
    #[must_use]
    pub fn new<I, S>(symbols: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            symbols: symbols.into_iter().map(Into::into).collect(),
            volatilities: None,
            risk_free_rate: 0.025,
            timeout: DEFAULT_OPTION_GREEKS_TIMEOUT,
        }
    }

    #[must_use]
    pub fn with_volatilities(mut self, volatilities: impl Into<Vec<f64>>) -> Self {
        self.volatilities = Some(volatilities.into());
        self
    }

    #[must_use]
    pub fn with_risk_free_rate(mut self, risk_free_rate: f64) -> Self {
        self.risk_free_rate = risk_free_rate;
        self
    }

    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    #[must_use]
    pub fn symbols(&self) -> &[String] {
        &self.symbols
    }

    #[must_use]
    pub fn volatilities(&self) -> Option<&[f64]> {
        self.volatilities.as_deref()
    }

    #[must_use]
    pub fn risk_free_rate(&self) -> f64 {
        self.risk_free_rate
    }

    #[must_use]
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    pub(crate) fn validate(&self) -> Result<OptionGreeksSpec> {
        if self.symbols.is_empty() {
            return Err(DataError::Validation(
                "symbols must not be empty".to_string(),
            ));
        }
        if self.symbols.iter().any(|symbol| symbol.is_empty()) {
            return Err(DataError::Validation(
                "symbols must not contain empty entries".to_string(),
            ));
        }
        if !self.risk_free_rate.is_finite() {
            return Err(DataError::Validation(
                "risk_free_rate must be finite".to_string(),
            ));
        }
        if let Some(volatilities) = self.volatilities.as_ref() {
            if volatilities.len() != self.symbols.len() {
                return Err(DataError::Validation(
                    "volatilities length must match symbols length".to_string(),
                ));
            }
            if volatilities
                .iter()
                .any(|volatility| !volatility.is_finite() || *volatility < 0.0)
            {
                return Err(DataError::Validation(
                    "volatilities must be finite and greater than or equal to zero".to_string(),
                ));
            }
        }

        Ok(OptionGreeksSpec {
            symbols: self.symbols.clone(),
            volatilities: self.volatilities.clone(),
            risk_free_rate: self.risk_free_rate,
            timeout: self.timeout,
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct OptionGreeksSpec {
    pub(crate) symbols: Vec<String>,
    pub(crate) volatilities: Option<Vec<f64>>,
    pub(crate) risk_free_rate: f64,
    pub(crate) timeout: Duration,
}

/// Owned result of a one-shot option greeks query.
#[derive(Debug, Clone, Default)]
pub struct OptionGreeksResult {
    rows: Vec<OptionGreeksRow>,
}

impl OptionGreeksResult {
    pub(crate) fn new(rows: Vec<OptionGreeksRow>) -> Self {
        Self { rows }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    #[must_use]
    pub fn get(&self, index: usize) -> Option<&OptionGreeksRow> {
        self.rows.get(index)
    }

    #[must_use]
    pub fn last(&self) -> Option<&OptionGreeksRow> {
        self.rows.last()
    }

    pub fn iter(&self) -> impl ExactSizeIterator<Item = &OptionGreeksRow> + DoubleEndedIterator {
        self.rows.iter()
    }

    #[must_use]
    pub fn rows(&self) -> &[OptionGreeksRow] {
        &self.rows
    }

    #[must_use]
    pub fn into_rows(self) -> Vec<OptionGreeksRow> {
        self.rows
    }
}

/// Owned greeks row for one option contract.
#[derive(Debug, Clone)]
pub struct OptionGreeksRow {
    pub symbol: String,
    pub instrument_id: String,
    pub instrument_name: String,
    pub quote_datetime: String,
    pub option_class: String,
    pub expire_rest_days: Option<i64>,
    pub expire_datetime: Option<i64>,
    pub underlying_symbol: String,
    pub strike_price: f64,
    pub option_last_price: f64,
    pub underlying_last_price: f64,
    pub volatility: f64,
    pub delta: f64,
    pub gamma: f64,
    pub theta: f64,
    pub vega: f64,
    pub rho: f64,
}

#[derive(Debug, Clone, Copy)]
struct GreeksMetrics {
    volatility: f64,
    delta: f64,
    gamma: f64,
    theta: f64,
    vega: f64,
    rho: f64,
}

impl GreeksMetrics {
    fn nan() -> Self {
        Self {
            volatility: f64::NAN,
            delta: f64::NAN,
            gamma: f64::NAN,
            theta: f64::NAN,
            vega: f64::NAN,
            rho: f64::NAN,
        }
    }
}

pub(crate) fn validate_option_metadata(symbol: &str, metadata: &SymbolInfo) -> Result<()> {
    if !metadata.ins_class.ends_with("OPTION") && metadata.class != InstrumentClass::Option {
        return Err(DataError::Validation(format!(
            "symbol {symbol} is not an option contract"
        )));
    }
    if metadata.underlying_symbol.is_none() {
        return Err(DataError::InvalidResponse(format!(
            "option metadata for {symbol} missing underlying_symbol"
        )));
    }
    if metadata.expire_datetime_secs.is_none() {
        return Err(DataError::InvalidResponse(format!(
            "option metadata for {symbol} missing expire_datetime"
        )));
    }
    if !metadata
        .strike_price
        .is_some_and(|strike_price| strike_price.is_finite() && strike_price > 0.0)
    {
        return Err(DataError::InvalidResponse(format!(
            "option metadata for {symbol} has invalid strike_price"
        )));
    }
    let option_class = metadata.option_class.as_deref().unwrap_or_default();
    if option_class_sign(option_class).is_none() {
        return Err(DataError::InvalidResponse(format!(
            "option metadata for {symbol} has unsupported option_class {}",
            option_class
        )));
    }
    Ok(())
}

pub(crate) fn build_option_greeks_row(
    symbol: &str,
    metadata: &SymbolInfo,
    option_quote: &Quote,
    underlying_quote: &Quote,
    explicit_volatility: Option<f64>,
    risk_free_rate: f64,
) -> Result<OptionGreeksRow> {
    validate_option_metadata(symbol, metadata)?;

    let expire_datetime = metadata.expire_datetime_secs.ok_or_else(|| {
        DataError::InvalidResponse(format!(
            "option metadata for {symbol} missing expire_datetime"
        ))
    })?;
    let option_class = metadata.option_class.as_deref().ok_or_else(|| {
        DataError::InvalidResponse(format!(
            "option metadata for {symbol} has unsupported option_class "
        ))
    })?;
    let strike_price = metadata.strike_price.ok_or_else(|| {
        DataError::InvalidResponse(format!(
            "option metadata for {symbol} has invalid strike_price"
        ))
    })?;
    let underlying_symbol = metadata.underlying_symbol.as_ref().ok_or_else(|| {
        DataError::InvalidResponse(format!(
            "option metadata for {symbol} missing underlying_symbol"
        ))
    })?;
    let quote_datetime = effective_quote_datetime(option_quote, underlying_quote);
    let option_last_price = effective_quote_price(option_quote);
    let underlying_last_price = effective_quote_price(underlying_quote);
    let time_to_expiry_years =
        time_to_expiry_years_from_quote_datetime(&quote_datetime, expire_datetime);
    let expire_rest_days = expire_rest_days_from_quote_datetime(&quote_datetime, expire_datetime)
        .or(metadata.expire_rest_days);

    let metrics = build_greeks_metrics(
        option_class,
        underlying_last_price,
        option_last_price,
        strike_price,
        risk_free_rate,
        time_to_expiry_years.unwrap_or(f64::NAN),
        explicit_volatility,
    );

    Ok(OptionGreeksRow {
        symbol: symbol.to_string(),
        instrument_id: metadata.instrument_id.as_str().to_string(),
        instrument_name: metadata.instrument_name.clone(),
        quote_datetime,
        option_class: option_class.to_string(),
        expire_rest_days,
        expire_datetime: metadata.expire_datetime_secs,
        underlying_symbol: underlying_symbol.as_str().to_string(),
        strike_price,
        option_last_price,
        underlying_last_price,
        volatility: metrics.volatility,
        delta: metrics.delta,
        gamma: metrics.gamma,
        theta: metrics.theta,
        vega: metrics.vega,
        rho: metrics.rho,
    })
}

fn effective_quote_datetime(quote: &Quote, fallback: &Quote) -> String {
    if !quote.datetime.is_empty() {
        return quote.datetime.clone();
    }
    if !fallback.datetime.is_empty() {
        return fallback.datetime.clone();
    }

    let Some(offset) = cst_offset() else {
        return String::new();
    };
    Utc::now()
        .with_timezone(&offset)
        .format("%Y-%m-%d %H:%M:%S%.6f")
        .to_string()
}

fn effective_quote_price(quote: &Quote) -> f64 {
    if quote.last_price.is_finite() && quote.last_price > 0.0 {
        return quote.last_price;
    }

    let bid = finite_non_negative_price(quote.bid_price1);
    let ask = finite_non_negative_price(quote.ask_price1);
    match (bid, ask) {
        (Some(bid), Some(ask)) => return (bid + ask) / 2.0,
        (Some(bid), None) => return bid,
        (None, Some(ask)) => return ask,
        (None, None) => {}
    }

    if quote.last_price.is_finite() && quote.last_price >= 0.0 {
        return quote.last_price;
    }

    if quote.pre_close.is_finite() && quote.pre_close > 0.0 {
        quote.pre_close
    } else {
        f64::NAN
    }
}

fn finite_non_negative_price(value: f64) -> Option<f64> {
    (value.is_finite() && value >= 0.0).then_some(value)
}

fn build_greeks_metrics(
    option_class: &str,
    underlying_last_price: f64,
    option_last_price: f64,
    strike_price: f64,
    risk_free_rate: f64,
    time_to_expiry_years: f64,
    explicit_volatility: Option<f64>,
) -> GreeksMetrics {
    let Some(sign) = option_class_sign(option_class) else {
        return GreeksMetrics::nan();
    };
    if !underlying_last_price.is_finite()
        || underlying_last_price <= 0.0
        || !option_last_price.is_finite()
        || option_last_price < 0.0
        || !strike_price.is_finite()
        || strike_price <= 0.0
        || !risk_free_rate.is_finite()
        || !time_to_expiry_years.is_finite()
        || time_to_expiry_years <= 0.0
    {
        return GreeksMetrics::nan();
    }

    let volatility = explicit_volatility.unwrap_or_else(|| {
        implied_volatility(
            underlying_last_price,
            option_last_price,
            strike_price,
            risk_free_rate,
            time_to_expiry_years,
            sign,
        )
    });
    if !volatility.is_finite() || volatility <= 0.0 {
        return GreeksMetrics {
            volatility,
            ..GreeksMetrics::nan()
        };
    }

    let d1 = bs_d1(
        underlying_last_price,
        strike_price,
        risk_free_rate,
        volatility,
        time_to_expiry_years,
    );
    if !d1.is_finite() {
        return GreeksMetrics {
            volatility,
            ..GreeksMetrics::nan()
        };
    }
    let d2 = d1 - volatility * time_to_expiry_years.sqrt();

    GreeksMetrics {
        volatility,
        delta: sign * normal_cdf(sign * d1),
        gamma: normal_pdf(d1) / (underlying_last_price * volatility * time_to_expiry_years.sqrt()),
        theta: -volatility * underlying_last_price * normal_pdf(d1)
            / (2.0 * time_to_expiry_years.sqrt())
            - sign
                * risk_free_rate
                * strike_price
                * (-risk_free_rate * time_to_expiry_years).exp()
                * normal_cdf(sign * d2),
        vega: underlying_last_price * time_to_expiry_years.sqrt() * normal_pdf(d1),
        rho: sign
            * strike_price
            * time_to_expiry_years
            * (-risk_free_rate * time_to_expiry_years).exp()
            * normal_cdf(sign * d2),
    }
}

fn implied_volatility(
    underlying_last_price: f64,
    option_last_price: f64,
    strike_price: f64,
    risk_free_rate: f64,
    time_to_expiry_years: f64,
    sign: f64,
) -> f64 {
    let lower_limit = sign
        * (underlying_last_price - strike_price * (-risk_free_rate * time_to_expiry_years).exp());
    if option_last_price < lower_limit || time_to_expiry_years <= 0.0 {
        return f64::NAN;
    }

    let mut volatility = DEFAULT_IMPLIED_VOLATILITY_SEED;
    let mut price = bs_price(
        underlying_last_price,
        strike_price,
        risk_free_rate,
        volatility,
        time_to_expiry_years,
        sign,
    );
    let mut vega_value = vega(
        underlying_last_price,
        strike_price,
        risk_free_rate,
        volatility,
        time_to_expiry_years,
    );
    let mut step = implied_volatility_step(option_last_price, price, vega_value);

    for _ in 0..IMPLIED_VOLATILITY_MAX_ITERATIONS {
        if (option_last_price - price).abs() < IMPLIED_VOLATILITY_EPSILON || step.is_nan() {
            break;
        }

        volatility = if volatility + step < 0.0 {
            volatility / 2.0
        } else if step > volatility / 2.0 {
            volatility * 1.5
        } else {
            volatility + step
        };

        price = bs_price(
            underlying_last_price,
            strike_price,
            risk_free_rate,
            volatility,
            time_to_expiry_years,
            sign,
        );
        vega_value = vega(
            underlying_last_price,
            strike_price,
            risk_free_rate,
            volatility,
            time_to_expiry_years,
        );
        step = implied_volatility_step(option_last_price, price, vega_value);
    }

    volatility
}

fn implied_volatility_step(option_last_price: f64, option_price: f64, vega: f64) -> f64 {
    if !vega.is_finite() || vega < IMPLIED_VOLATILITY_MIN_VEGA {
        f64::NAN
    } else {
        (option_last_price - option_price) / vega
    }
}

fn bs_price(
    underlying_last_price: f64,
    strike_price: f64,
    risk_free_rate: f64,
    volatility: f64,
    time_to_expiry_years: f64,
    sign: f64,
) -> f64 {
    let d1 = bs_d1(
        underlying_last_price,
        strike_price,
        risk_free_rate,
        volatility,
        time_to_expiry_years,
    );
    if !d1.is_finite() {
        return f64::NAN;
    }
    let d2 = d1 - volatility * time_to_expiry_years.sqrt();
    sign * (underlying_last_price * normal_cdf(sign * d1)
        - strike_price * (-risk_free_rate * time_to_expiry_years).exp() * normal_cdf(sign * d2))
}

fn vega(
    underlying_last_price: f64,
    strike_price: f64,
    risk_free_rate: f64,
    volatility: f64,
    time_to_expiry_years: f64,
) -> f64 {
    let d1 = bs_d1(
        underlying_last_price,
        strike_price,
        risk_free_rate,
        volatility,
        time_to_expiry_years,
    );
    if !d1.is_finite() {
        return f64::NAN;
    }
    underlying_last_price * time_to_expiry_years.sqrt() * normal_pdf(d1)
}

fn bs_d1(
    underlying_last_price: f64,
    strike_price: f64,
    risk_free_rate: f64,
    volatility: f64,
    time_to_expiry_years: f64,
) -> f64 {
    if !underlying_last_price.is_finite()
        || underlying_last_price <= 0.0
        || !strike_price.is_finite()
        || strike_price <= 0.0
        || !risk_free_rate.is_finite()
        || !volatility.is_finite()
        || volatility <= 0.0
        || !time_to_expiry_years.is_finite()
        || time_to_expiry_years <= 0.0
    {
        return f64::NAN;
    }

    ((underlying_last_price / strike_price).ln()
        + (risk_free_rate + 0.5 * volatility.powi(2)) * time_to_expiry_years)
        / (volatility * time_to_expiry_years.sqrt())
}

fn normal_cdf(value: f64) -> f64 {
    0.5 * (1.0 + erf_approx(value / std::f64::consts::SQRT_2))
}

fn normal_pdf(value: f64) -> f64 {
    (-0.5 * value * value).exp() / (2.0 * std::f64::consts::PI).sqrt()
}

fn erf_approx(value: f64) -> f64 {
    let sign = if value < 0.0 { -1.0 } else { 1.0 };
    let x = value.abs();
    let t = 1.0 / (1.0 + 0.327_591_1 * x);
    let y = 1.0
        - (((((1.061_405_429 * t - 1.453_152_027) * t) + 1.421_413_741) * t - 0.284_496_736) * t
            + 0.254_829_592)
            * t
            * (-(x * x)).exp();
    sign * y
}

fn option_class_sign(option_class: &str) -> Option<f64> {
    match option_class {
        "CALL" => Some(1.0),
        "PUT" => Some(-1.0),
        _ => None,
    }
}

fn time_to_expiry_years_from_quote_datetime(
    quote_datetime: &str,
    expire_datetime: i64,
) -> Option<f64> {
    let current_datetime = parse_quote_datetime(quote_datetime)?;
    let current_ts = current_datetime.timestamp() as f64
        + f64::from(current_datetime.timestamp_subsec_nanos()) / 1_000_000_000.0;
    Some((expire_datetime as f64 - current_ts) / (360.0 * 86_400.0))
}

fn expire_rest_days_from_quote_datetime(quote_datetime: &str, expire_datetime: i64) -> Option<i64> {
    let current_datetime = parse_quote_datetime(quote_datetime)?;
    let offset = cst_offset()?;
    let expire_datetime = offset.timestamp_opt(expire_datetime, 0).single()?;
    Some((expire_datetime.date_naive() - current_datetime.date_naive()).num_days())
}

fn parse_quote_datetime(quote_datetime: &str) -> Option<chrono::DateTime<FixedOffset>> {
    if quote_datetime.is_empty() {
        return None;
    }
    let naive = NaiveDateTime::parse_from_str(quote_datetime, QUOTE_DATETIME_FORMAT).ok()?;
    cst_offset()?.from_local_datetime(&naive).single()
}

fn cst_offset() -> Option<FixedOffset> {
    FixedOffset::east_opt(CST_OFFSET_SECONDS)
}

#[cfg(test)]
mod tests {
    use chrono::{Duration as ChronoDuration, TimeZone};

    use super::*;

    fn option_metadata(
        instrument_id: &str,
        option_class: &str,
        underlying_symbol: &str,
        expire_datetime_secs: i64,
    ) -> SymbolInfo {
        SymbolInfo {
            instrument_id: tqsdk_core::Symbol::new(instrument_id),
            instrument_name: "测试期权".to_string(),
            exchange_id: instrument_id
                .split_once('.')
                .map(|(exchange, _)| exchange.to_string())
                .unwrap_or_default(),
            product_id: String::new(),
            ins_class: "OPTION".to_string(),
            class: InstrumentClass::Option,
            price_tick: Some(0.01),
            volume_multiple: Some(1),
            open_limit: None,
            max_limit_order_volume: None,
            max_market_order_volume: None,
            min_limit_order_volume: None,
            min_market_order_volume: None,
            open_max_market_order_volume: None,
            open_max_limit_order_volume: None,
            open_min_market_order_volume: None,
            open_min_limit_order_volume: None,
            underlying_symbol: Some(tqsdk_core::Symbol::new(underlying_symbol)),
            strike_price: Some(100.0),
            expired: false,
            expire_datetime_secs: Some(expire_datetime_secs),
            expire_rest_days: None,
            delivery_year: None,
            delivery_month: None,
            last_exercise_datetime_secs: None,
            exercise_year: None,
            exercise_month: None,
            option_class: Some(option_class.to_string()),
            upper_limit: None,
            lower_limit: None,
            pre_settlement: None,
            pre_open_interest: None,
            pre_close: None,
            trading_time: tqsdk_core::TradingTime::default(),
        }
    }

    #[test]
    fn option_greeks_request_rejects_invalid_inputs() {
        let err = OptionGreeksRequest::new(Vec::<String>::new())
            .validate()
            .unwrap_err();
        assert!(matches!(
            err,
            DataError::Validation(message) if message == "symbols must not be empty"
        ));

        let err = OptionGreeksRequest::new(["SHFE.au2606C720"])
            .with_volatilities(vec![0.2, 0.3])
            .validate()
            .unwrap_err();
        assert!(matches!(
            err,
            DataError::Validation(message) if message == "volatilities length must match symbols length"
        ));

        let err = OptionGreeksRequest::new(["SHFE.au2606C720"])
            .with_volatilities(vec![-0.1])
            .validate()
            .unwrap_err();
        assert!(matches!(
            err,
            DataError::Validation(message)
                if message == "volatilities must be finite and greater than or equal to zero"
        ));
    }

    #[test]
    fn implied_volatility_recovers_black_scholes_seeded_price() {
        let option_price = bs_price(100.0, 100.0, 0.05, 0.2, 1.0, 1.0);
        let volatility = implied_volatility(100.0, option_price, 100.0, 0.05, 1.0, 1.0);
        assert!((volatility - 0.2).abs() < 1e-6, "{volatility}");
    }

    #[test]
    fn build_option_greeks_row_uses_explicit_volatility() {
        let mut metadata = option_metadata(
            "SHFE.au2606C720",
            "CALL",
            "SHFE.au2606",
            cst_offset()
                .unwrap()
                .with_ymd_and_hms(2026, 12, 27, 0, 0, 0)
                .single()
                .unwrap()
                .timestamp(),
        );
        metadata.instrument_name = "沪金看涨期权".to_string();
        metadata.expire_rest_days = Some(360);

        let option_quote = Quote {
            datetime: "2026-01-01 00:00:00.000000".to_string(),
            last_price: 10.450583572185565,
            ..Quote::default()
        };
        let underlying_quote = Quote {
            last_price: 100.0,
            ..Quote::default()
        };

        let row = build_option_greeks_row(
            "SHFE.au2606C720",
            &metadata,
            &option_quote,
            &underlying_quote,
            Some(0.2),
            0.05,
        )
        .unwrap();

        assert_eq!(row.symbol, "SHFE.au2606C720");
        assert_eq!(row.expire_rest_days, Some(360));
        assert!((row.volatility - 0.2).abs() < 1e-9);
        assert!((row.delta - 0.636_830_651).abs() < 1e-6, "{}", row.delta);
        assert!((row.gamma - 0.018_762_017).abs() < 1e-6, "{}", row.gamma);
        assert!((row.theta + 6.414_027_64).abs() < 1e-6, "{}", row.theta);
        assert!((row.vega - 37.524_034_69).abs() < 1e-6, "{}", row.vega);
        assert!((row.rho - 53.232_481_55).abs() < 3e-6, "{}", row.rho);
    }

    #[test]
    fn build_option_greeks_row_uses_implied_volatility_when_not_provided() {
        let start = cst_offset()
            .unwrap()
            .with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
            .single()
            .unwrap();
        let expire = start + ChronoDuration::days(360);
        let mut metadata = option_metadata(
            "SSE.510300C2601M000100",
            "CALL",
            "SSE.510300",
            expire.timestamp(),
        );
        metadata.ins_class = "ETF_OPTION".to_string();
        let option_quote = Quote {
            datetime: "2026-01-01 00:00:00.000000".to_string(),
            last_price: bs_price(100.0, 100.0, 0.05, 0.2, 1.0, 1.0),
            ..Quote::default()
        };
        let underlying_quote = Quote {
            last_price: 100.0,
            ..Quote::default()
        };

        let row = build_option_greeks_row(
            "SSE.510300C2601M000100",
            &metadata,
            &option_quote,
            &underlying_quote,
            None,
            0.05,
        )
        .unwrap();

        assert!((row.volatility - 0.2).abs() < 1e-6, "{}", row.volatility);
    }

    #[test]
    fn build_option_greeks_row_falls_back_to_mid_price_and_underlying_datetime() {
        let start = cst_offset()
            .unwrap()
            .with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
            .single()
            .unwrap();
        let expire = start + ChronoDuration::days(180);
        let metadata =
            option_metadata("SHFE.au2606C720", "CALL", "SHFE.au2606", expire.timestamp());
        let option_quote = Quote {
            ask_price1: 10.6,
            bid_price1: 10.4,
            ..Quote::default()
        };
        let underlying_quote = Quote {
            datetime: "2026-01-01 00:00:00.000000".to_string(),
            ask_price1: 100.2,
            bid_price1: 99.8,
            ..Quote::default()
        };

        let row = build_option_greeks_row(
            "SHFE.au2606C720",
            &metadata,
            &option_quote,
            &underlying_quote,
            Some(0.2),
            0.05,
        )
        .unwrap();

        assert_eq!(row.quote_datetime, "2026-01-01 00:00:00.000000");
        assert!((row.option_last_price - 10.5).abs() < 1e-9);
        assert!((row.underlying_last_price - 100.0).abs() < 1e-9);
    }
}
