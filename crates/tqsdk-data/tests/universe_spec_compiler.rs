use std::cell::Cell;
use std::convert::Infallible;

use tqsdk_data::{
    CompiledUniverseInstrumentKind, SnapshotCapabilities, SnapshotContract, UniverseCompileError,
    UniverseProduct, UniverseSpec, UniverseSymbolClass, UniverseView, compile_snapshot_universe,
};

#[derive(Default)]
struct FakeSnapshotCapabilities {
    contract_queries: Cell<usize>,
}

impl SnapshotCapabilities for FakeSnapshotCapabilities {
    type Error = Infallible;

    fn current_contracts(&self) -> Result<Vec<SnapshotContract>, Self::Error> {
        self.contract_queries.set(self.contract_queries.get() + 1);
        Ok(vec![
            SnapshotContract::new("SHFE", "au2505", "au").expired(true),
            SnapshotContract::new("SHFE", "au2506", "au"),
            SnapshotContract::new("DCE", "m2509", "m"),
            SnapshotContract::new("DCE", "m2601", "m").eligible(false),
        ])
    }

    fn main_contract(&self, product: &UniverseProduct) -> Result<Option<String>, Self::Error> {
        Ok(match (product.exchange(), product.product()) {
            ("SHFE", "au") => Some("SHFE.au2506".to_string()),
            ("DCE", "m") => Some("DCE.m2509".to_string()),
            _ => None,
        })
    }

    fn top_contracts(
        &self,
        product: &UniverseProduct,
        _limit: u32,
    ) -> Result<Vec<String>, Self::Error> {
        Ok(self.main_contract(product)?.into_iter().collect())
    }

    fn continuous_symbol(&self, product: &UniverseProduct) -> Result<Option<String>, Self::Error> {
        Ok(Some(format!(
            "KQ.m@{}.{}",
            product.exchange(),
            product.product()
        )))
    }

    fn index_symbol(&self, product: &UniverseProduct) -> Result<Option<String>, Self::Error> {
        Ok(Some(format!(
            "KQ.i@{}.{}",
            product.exchange(),
            product.product()
        )))
    }

    fn classify_symbol(&self, symbol: &str) -> Result<Option<UniverseSymbolClass>, Self::Error> {
        Ok(match symbol {
            "SHFE.au2505" | "SHFE.au2506" => Some(UniverseSymbolClass::physical("SHFE", "au")),
            "DCE.m2509" | "DCE.m2601" => Some(UniverseSymbolClass::physical("DCE", "m")),
            "KQ.m@SHFE.au" => Some(UniverseSymbolClass::continuous("SHFE", "au")),
            "KQ.i@DCE.m" => Some(UniverseSymbolClass::index("DCE", "m")),
            _ => None,
        })
    }
}

#[test]
fn snapshot_compiler_distinguishes_broad_contracts_exact_expired_and_logical_views() {
    let spec = UniverseSpec::parse_v2(concat!(
        "contract:all;main:SHFE.au;continuous:SHFE.au;",
        "index:DCE.m;symbol:SHFE.au2506"
    ))
    .expect("valid V2 universe");
    let capabilities = FakeSnapshotCapabilities::default();

    let compiled = compile_snapshot_universe(&spec, &[], &capabilities).expect("compiled universe");

    assert_eq!(capabilities.contract_queries.get(), 1);
    assert_eq!(
        compiled
            .candidates()
            .iter()
            .map(|candidate| candidate.symbol())
            .collect::<Vec<_>>(),
        ["DCE.m2509", "KQ.i@DCE.m", "KQ.m@SHFE.au", "SHFE.au2506",]
    );
    let au = compiled
        .candidates()
        .iter()
        .find(|candidate| candidate.symbol() == "SHFE.au2506")
        .expect("shared physical candidate");
    assert_eq!(au.kind(), CompiledUniverseInstrumentKind::PhysicalContract);
    assert_eq!(
        au.provenance(),
        &[
            UniverseView::Contract,
            UniverseView::Main,
            UniverseView::Symbol
        ]
    );
    assert_eq!(
        compiled.physical_dependencies(),
        &["DCE.m2509", "SHFE.au2506"]
    );

    let exact_expired =
        UniverseSpec::parse_v2("contract:SHFE.au2505").expect("exact expired selector");
    let compiled = compile_snapshot_universe(&exact_expired, &[], &capabilities)
        .expect("known exact contract remains selectable");
    assert_eq!(compiled.candidates()[0].symbol(), "SHFE.au2505");
    assert_eq!(compiled.physical_dependencies(), &["SHFE.au2505"]);
}

#[test]
fn snapshot_compiler_preserves_other_provenance_when_one_view_is_excluded() {
    let capabilities = FakeSnapshotCapabilities::default();
    let spec = UniverseSpec::parse_v2("main:all;symbol:SHFE.au2506;index:DCE.m;!main:SHFE.au")
        .expect("valid V2 universe");
    let compiled = compile_snapshot_universe(&spec, &[], &capabilities).expect("compiled universe");

    let au = compiled
        .candidates()
        .iter()
        .find(|candidate| candidate.symbol() == "SHFE.au2506")
        .expect("symbol provenance survives");
    assert_eq!(au.provenance(), &[UniverseView::Symbol]);

    let remove_all = UniverseSpec::parse_v2("main:all;index:DCE.m;!symbol:SHFE.au2506")
        .expect("valid V2 universe");
    let compiled =
        compile_snapshot_universe(&remove_all, &["SHFE.au2506".to_string()], &capabilities)
            .expect("index survives");
    assert!(
        compiled
            .candidates()
            .iter()
            .all(|candidate| candidate.symbol() != "SHFE.au2506")
    );
}

#[test]
fn exact_global_contract_filter_does_not_remove_a_logical_instrument() {
    let capabilities = FakeSnapshotCapabilities::default();
    let spec = UniverseSpec::parse_v2("contract:SHFE.au2506;continuous:SHFE.au;!SHFE.au2506")
        .expect("valid V2 universe");

    let compiled = compile_snapshot_universe(&spec, &[], &capabilities).expect("logical survives");
    assert_eq!(compiled.candidates().len(), 1);
    assert_eq!(compiled.candidates()[0].symbol(), "KQ.m@SHFE.au");

    let remove_product = UniverseSpec::parse_v2("contract:SHFE.au2506;continuous:SHFE.au;!SHFE.au")
        .expect("valid V2 universe");
    assert!(matches!(
        compile_snapshot_universe(&remove_product, &[], &capabilities),
        Err(UniverseCompileError::NoCandidates)
    ));
}

#[test]
fn timeline_mode_fails_before_snapshot_capabilities_are_queried() {
    let capabilities = FakeSnapshotCapabilities::default();
    let spec = UniverseSpec::parse_v2("timeline(contract:all)").expect("valid V2 universe");

    assert!(matches!(
        compile_snapshot_universe(&spec, &[], &capabilities),
        Err(UniverseCompileError::WrongMode { .. })
    ));
    assert_eq!(capabilities.contract_queries.get(), 0);
}
