use tqsdk::MarketCachePolicy;

#[test]
fn facade_string_universe_normalizes_compact_exclusions() {
    let legacy = MarketCachePolicy::new("unused-cache-dir")
        .record_universe("snapshot(contract:all;!contract:CFFEX.*,CZCE.ZC;!CFFEX.*)")
        .expect("legacy V2 expression parses through facade");
    let except = MarketCachePolicy::new("unused-cache-dir")
        .record_universe(
            "snapshot(contract:all;except(contract:CFFEX.*,CZCE.ZC);except(all:CFFEX.*))",
        )
        .expect("compact V2 expression parses through facade");

    assert_eq!(except.universe_spec(), legacy.universe_spec());
    assert_eq!(
        except
            .universe_spec()
            .expect("V2 expression stored by facade")
            .canonical_text(),
        "contract:all;!contract:CFFEX.*,CZCE.ZC;!CFFEX.*"
    );
}
