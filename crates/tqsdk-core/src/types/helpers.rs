use serde::de::Error;
use serde::{Deserialize, Deserializer};

pub(super) fn default_nan() -> f64 {
    f64::NAN
}

pub(super) fn default_neg_one() -> i64 {
    -1
}

pub(super) fn default_true() -> bool {
    true
}

pub(super) fn default_currency() -> String {
    "CNY".to_string()
}

pub(super) fn deserialize_f64_or_nan<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::Error;

    let value = serde_json::Value::deserialize(deserializer)?;
    match value {
        serde_json::Value::Number(number) => number
            .as_f64()
            .ok_or_else(|| Error::custom("invalid number")),
        serde_json::Value::String(_) => Ok(f64::NAN),
        serde_json::Value::Null => Ok(f64::NAN),
        other => Err(Error::custom(format!(
            "expected number, string, or null, got {other}"
        ))),
    }
}

pub(super) fn deserialize_i64_or_zero<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(deserialize_option_i64_or_none(deserializer)?.unwrap_or_default())
}

pub(super) fn deserialize_option_i64_or_none<'de, D>(
    deserializer: D,
) -> Result<Option<i64>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    match value {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::Number(number) => {
            if let Some(value) = number.as_i64() {
                return Ok(Some(value));
            }
            if let Some(value) = number.as_u64() {
                return i64::try_from(value)
                    .map(Some)
                    .map_err(|_| Error::custom("u64 value does not fit in i64"));
            }
            let Some(value) = number.as_f64() else {
                return Err(Error::custom("invalid number"));
            };
            if !value.is_finite() || value.fract() != 0.0 {
                return Err(Error::custom(format!(
                    "expected integral number, got {value}"
                )));
            }
            if value < i64::MIN as f64 || value > i64::MAX as f64 {
                return Err(Error::custom(format!(
                    "integral number out of i64 range: {value}"
                )));
            }
            Ok(Some(value as i64))
        }
        serde_json::Value::String(text) if text.is_empty() || text == "-" => Ok(None),
        serde_json::Value::String(text) => text
            .parse::<i64>()
            .map(Some)
            .map_err(|err| Error::custom(format!("invalid integer string `{text}`: {err}"))),
        other => Err(Error::custom(format!(
            "expected integer-compatible number, string, or null, got {other}"
        ))),
    }
}

pub(super) fn deserialize_vec_or_default<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Ok(Option::<Vec<T>>::deserialize(deserializer)?.unwrap_or_default())
}
