#[test]
fn single_process_api_not_exposed() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/no_single_process_api.rs");
    t.compile_fail("tests/ui/no_internal_modules.rs");
    t.compile_fail("tests/ui/no_legacy_producer_barrier_exports.rs");
}
