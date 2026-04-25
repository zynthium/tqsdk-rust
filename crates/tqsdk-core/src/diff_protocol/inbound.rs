use serde_json::Value;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DiffInboundAid {
    RtnData,
    QrySettlementInfo,
    Unknown,
}

impl DiffInboundAid {
    pub(crate) fn from_value(value: &Value) -> Self {
        match value.get("aid").and_then(Value::as_str) {
            Some("rtn_data") => Self::RtnData,
            Some("qry_settlement_info") => Self::QrySettlementInfo,
            _ => Self::Unknown,
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::DiffInboundAid;

    #[test]
    fn classifies_rtn_data() {
        let value = json!({ "aid": "rtn_data" });

        assert_eq!(DiffInboundAid::from_value(&value), DiffInboundAid::RtnData);
    }

    #[test]
    fn classifies_qry_settlement_info() {
        let value = json!({ "aid": "qry_settlement_info" });

        assert_eq!(
            DiffInboundAid::from_value(&value),
            DiffInboundAid::QrySettlementInfo
        );
    }

    #[test]
    fn missing_aid_is_unknown() {
        let value = json!({});

        assert_eq!(DiffInboundAid::from_value(&value), DiffInboundAid::Unknown);
    }
}
