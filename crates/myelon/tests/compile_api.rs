#[test]
fn myelon_public_api_contract_is_enforced() {
    let t = trybuild::TestCases::new();
    t.pass("tests/ui/myelon_public_api.rs");
    t.compile_fail("tests/ui/myelon_disallow_full_crate_path.rs");
}
