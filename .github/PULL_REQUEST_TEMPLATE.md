## Summary

One or two sentences on what this PR changes and why.

## Crates affected

- [ ] disruptor-mp
- [ ] myelon
- [ ] myelon-dst
- [ ] perf-bench
- [ ] competitive-bench
- [ ] docs / CI / workspace tooling only

## Validation

- [ ] cargo build --workspace
- [ ] cargo test --workspace
- [ ] cargo clippy --workspace --all-targets -- -D warnings
- [ ] cargo fmt --all --check
- [ ] CHANGELOG.md updated (if user-visible change)
- [ ] Rustdoc updated (if public API changed)
- [ ] If DST code touched: RUSTFLAGS="--cfg dst" cargo test passes

## Breaking change?

Yes / No. If yes, describe the migration path.
