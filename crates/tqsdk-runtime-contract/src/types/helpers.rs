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
        serde_json::Value::String(text) if text.is_empty() || text == "-" => Ok(f64::NAN),
        serde_json::Value::Null => Ok(f64::NAN),
        other => Err(Error::custom(format!(
            "expected number, empty string, \"-\", or null, got {other}"
        ))),
    }
}

pub(super) fn deserialize_i64_or_zero<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(Option::<i64>::deserialize(deserializer)?.unwrap_or_default())
}
