#![cfg_attr(not(test), forbid(unsafe_code))]

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ContinuousContractParts<'a> {
    pub prefix: &'a str,
    pub exchange_id: &'a str,
    pub product_id: &'a str,
}

pub(crate) fn continuous_contract_parts(symbol: &str) -> Option<ContinuousContractParts<'_>> {
    let (prefix, underlying) = symbol.split_once('@')?;
    if !matches!(prefix, "KQ.m" | "KQ.i") {
        return None;
    }
    let (exchange_id, product_id) = underlying.split_once('.')?;
    if exchange_id.is_empty() || product_id.is_empty() {
        return None;
    }
    Some(ContinuousContractParts {
        prefix,
        exchange_id,
        product_id,
    })
}

pub(crate) fn exchange_id_for_symbol(symbol: &str) -> Option<&str> {
    continuous_contract_parts(symbol)
        .map(|parts| parts.exchange_id)
        .or_else(|| symbol.split_once('.').map(|(exchange_id, _)| exchange_id))
        .filter(|exchange_id| !exchange_id.is_empty())
}

pub(crate) fn supports_index_continuous_contract(exchange_id: &str) -> bool {
    !exchange_id.eq_ignore_ascii_case("KQD")
}

pub(crate) fn continuous_contract_display_name(symbol: &str) -> Option<String> {
    let parts = continuous_contract_parts(symbol)?;
    let product_name = futures_product_chinese_name(parts.exchange_id, parts.product_id)?;
    continuous_contract_display_name_from_product_name(parts.prefix, product_name)
}

pub(crate) fn continuous_contract_display_name_from_product_name(
    prefix: &str,
    product_name: &str,
) -> Option<String> {
    let suffix = continuous_contract_suffix(prefix)?;
    let product_name = product_name.trim();
    (!product_name.is_empty()).then(|| format!("{product_name}{suffix}"))
}

pub(crate) fn product_name_from_instrument_name(instrument_name: &str) -> Option<String> {
    let trimmed = instrument_name.trim();
    if trimmed.is_empty() {
        return None;
    }
    let product_name = trimmed.trim_end_matches(|ch: char| ch.is_ascii_digit());
    (!product_name.is_empty()).then(|| product_name.to_string())
}

pub(crate) fn futures_product_chinese_name(
    exchange_id: &str,
    product_id: &str,
) -> Option<&'static str> {
    match (
        exchange_id.to_ascii_uppercase().as_str(),
        product_id.to_ascii_lowercase().as_str(),
    ) {
        ("CFFEX", "if") => Some("沪深300"),
        ("CFFEX", "ih") => Some("上证50"),
        ("CFFEX", "ic") => Some("中证500"),
        ("CFFEX", "im") => Some("中证1000"),
        ("CFFEX", "t") => Some("10年国债"),
        ("CFFEX", "tf") => Some("5年国债"),
        ("CFFEX", "tl") => Some("30年国债"),
        ("CFFEX", "ts") => Some("2年国债"),

        ("SHFE", "ag") => Some("沪银"),
        ("SHFE", "al") => Some("沪铝"),
        ("SHFE", "ao") => Some("氧化铝"),
        ("SHFE", "au") => Some("沪金"),
        ("SHFE", "br") => Some("丁二烯橡胶"),
        ("SHFE", "bu") => Some("沥青"),
        ("SHFE", "cu") => Some("沪铜"),
        ("SHFE", "fu") => Some("燃油"),
        ("SHFE", "hc") => Some("热轧卷板"),
        ("SHFE", "ni") => Some("沪镍"),
        ("SHFE", "pb") => Some("沪铅"),
        ("SHFE", "rb") => Some("螺纹钢"),
        ("SHFE", "ru") => Some("橡胶"),
        ("SHFE", "sn") => Some("沪锡"),
        ("SHFE", "sp") => Some("纸浆"),
        ("SHFE", "ss") => Some("不锈钢"),
        ("SHFE", "wr") => Some("线材"),
        ("SHFE", "zn") => Some("沪锌"),

        ("INE", "bc") => Some("国际铜"),
        ("INE", "ec") => Some("集运欧线"),
        ("INE", "lu") => Some("低硫燃油"),
        ("INE", "nr") => Some("20号胶"),
        ("INE", "sc") => Some("原油"),

        ("DCE", "a") => Some("豆一"),
        ("DCE", "b") => Some("豆二"),
        ("DCE", "bb") => Some("胶合板"),
        ("DCE", "c") => Some("玉米"),
        ("DCE", "cs") => Some("玉米淀粉"),
        ("DCE", "eb") => Some("苯乙烯"),
        ("DCE", "eg") => Some("乙二醇"),
        ("DCE", "fb") => Some("纤维板"),
        ("DCE", "i") => Some("铁矿石"),
        ("DCE", "j") => Some("焦炭"),
        ("DCE", "jd") => Some("鸡蛋"),
        ("DCE", "jm") => Some("焦煤"),
        ("DCE", "l") => Some("聚乙烯"),
        ("DCE", "lh") => Some("生猪"),
        ("DCE", "m") => Some("豆粕"),
        ("DCE", "p") => Some("棕榈油"),
        ("DCE", "pg") => Some("液化石油气"),
        ("DCE", "pp") => Some("聚丙烯"),
        ("DCE", "rr") => Some("粳米"),
        ("DCE", "v") => Some("聚氯乙烯"),
        ("DCE", "y") => Some("豆油"),

        ("CZCE", "ap") => Some("苹果"),
        ("CZCE", "cf") => Some("棉花"),
        ("CZCE", "cj") => Some("红枣"),
        ("CZCE", "cy") => Some("棉纱"),
        ("CZCE", "fg") => Some("玻璃"),
        ("CZCE", "jr") => Some("粳稻"),
        ("CZCE", "lr") => Some("晚籼稻"),
        ("CZCE", "ma") => Some("甲醇"),
        ("CZCE", "oi") => Some("菜油"),
        ("CZCE", "pf") => Some("短纤"),
        ("CZCE", "pk") => Some("花生"),
        ("CZCE", "pm") => Some("普麦"),
        ("CZCE", "px") => Some("对二甲苯"),
        ("CZCE", "ri") => Some("早籼稻"),
        ("CZCE", "rm") => Some("菜粕"),
        ("CZCE", "sa") => Some("纯碱"),
        ("CZCE", "sf") => Some("硅铁"),
        ("CZCE", "sh") => Some("烧碱"),
        ("CZCE", "sm") => Some("锰硅"),
        ("CZCE", "sr") => Some("白糖"),
        ("CZCE", "ta") => Some("PTA"),
        ("CZCE", "ur") => Some("尿素"),
        ("CZCE", "wh") => Some("强麦"),
        ("CZCE", "zc") => Some("动力煤"),

        ("GFEX", "lc") => Some("碳酸锂"),
        ("GFEX", "ps") => Some("多晶硅"),
        ("GFEX", "si") => Some("工业硅"),
        _ => None,
    }
}

fn continuous_contract_suffix(prefix: &str) -> Option<&'static str> {
    match prefix {
        "KQ.m" => Some("主连"),
        "KQ.i" => Some("加权"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn continuous_contract_parts_extract_underlying_exchange() {
        let parts = continuous_contract_parts("KQ.i@DCE.m").unwrap();

        assert_eq!(parts.prefix, "KQ.i");
        assert_eq!(parts.exchange_id, "DCE");
        assert_eq!(parts.product_id, "m");
        assert_eq!(exchange_id_for_symbol("KQ.i@DCE.m"), Some("DCE"));
    }

    #[test]
    fn continuous_display_name_uses_futures_product_map() {
        assert_eq!(
            continuous_contract_display_name("KQ.i@DCE.m").as_deref(),
            Some("豆粕加权")
        );
        assert_eq!(
            continuous_contract_display_name("KQ.m@SHFE.au").as_deref(),
            Some("沪金主连")
        );
    }

    #[test]
    fn kqd_does_not_support_index_continuous_contracts() {
        assert!(!supports_index_continuous_contract("KQD"));
        assert!(supports_index_continuous_contract("DCE"));
    }
}
