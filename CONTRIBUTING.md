# Contributing to `myelon`

The repository ships two publishable crates — `disruptor-mp` (Layer 0
multiprocess substrate) and `myelon` (Layers 1, 2, 3 + topology +
observability) — plus a set of internal crates (`perf-bench`,
`competitive-bench`, `myelon-dst`) used for benchmarking and
deterministic-simulation testing.

The repository is intentionally a single workspace: everything builds with
`cargo build`, everything tests with `cargo test`. Issues, RFCs, and PRs are
all welcome.

## TL;DR

```sh
git clone https://github.com/Venkat2811/myelon
cd myelon
cargo build
cargo test
cargo fmt --all -- --check
cargo clippy -p disruptor-mp -p myelon --lib --all-features -- -D warnings
cargo doc -p disruptor-mp -p myelon --no-deps
```

If those four checks pass on your branch, you're 90% of the way to a clean
PR.

## Workspace layout

```
crates/
├── disruptor-mp/      # Publishable. Layer 0: raw cross-process ring buffer.
├── myelon/            # Publishable. Layers 1, 2, 3 + topology + observability.
├── myelon-dst/        # Internal. Multiprocess DST harness (orchestrator only;
│                      #            DST primitives live in `disruptor-mp::dst`).
├── perf-bench/        # Internal. Performance benchmark consolidation.
└── competitive-bench/ # Internal. External transport comparison harness.

examples/              # Workspace-level runnable examples.
book/                  # mdBook source for the docs site (`mdbook build`).
```

Only `disruptor-mp` and `myelon` are published to crates.io.

## Filing issues

- **Bugs**: please include the workspace commit SHA, the platform
  (`uname -srv` on Unix, `winver` on Windows), the Rust version
  (`rustc --version`), and the reduction (smallest repro you can manage).
  Stack traces / `RUST_BACKTRACE=1` output very welcome.
- **Feature requests**: please open an issue first describing the use case
  and why an existing surface (Layer 0/1/2/3, observability, topology)
  doesn't solve it. We're generally cautious about adding new public API
  surface during the `0.1.0-alpha.x` window — pre-1.0 is the right time
  to subtract, not add.
- **Performance regressions**: please include `perf-bench-pingpong`
  numbers before/after (`cargo run --release -p perf-bench --bin
  perf-bench-pingpong -- --layer raw_ring --backend shm`).

## Sending a PR

Standard GitHub flow: fork → branch → PR.

### Before opening a PR

Run the four-gate sequence above (build / test / fmt / clippy + doc). The
two publishable crates carry `-D warnings` on `clippy::doc_markdown` and
`broken_intra_doc_links`; CI will reject otherwise-passing PRs that
introduce warnings on those surfaces.

If your change touches anything under `crates/disruptor-mp/src/dst/` or
`crates/myelon-dst/`, also run the DST test track:

```sh
RUSTFLAGS="--cfg dst" cargo test -p disruptor-mp -p myelon -p myelon-dst \
    --features myelon-dst/_runner_bin
```

### Commit message style

Look at recent history (`git log --oneline -20`) for the established voice:

- Short subject line (under 70 chars), area prefix where useful
  (`disruptor-mp:`, `myelon:`, `dst:`, `docs:`, `bench:`).
- Body that motivates the change — the **why**, not just the **what**.
- For non-trivial commits, include a "Verification" section at the end
  listing the gates you ran (e.g. `cargo test -p ... — 244 passed`).
- Co-authored-by trailers for paired or AI-assisted work are welcome
  and encouraged.

### Code conventions

- Workspace `[lints]` are the source of truth for what triggers warnings.
  Don't `#[allow(...)]` to dodge them — fix the root cause or open an
  issue if the lint is wrong for your case.
- Every `unsafe { ... }` block needs a `// SAFETY:` comment naming the
  invariant it relies on. `unsafe_op_in_unsafe_fn` is `warn` workspace-wide.
- Public items in `disruptor-mp` and `myelon` need rustdoc (the `missing_docs`
  lint is enabled on their `lib.rs` directly). Internal items don't.
- Format with `cargo fmt --all`. Configured via `rustfmt.toml`.
- We avoid `unwrap()` / `expect()` in non-test code unless the panic message
  documents an invariant a maintainer would verify before changing.

### What to expect from review

PRs typically get a first response within a few days. We aim to either
land or comment with concrete next steps on every PR, even if the answer
is "we don't want to take this — here's why." We're explicit about scope
trade-offs.

## RFCs

Substantive design changes (new public types, new wire-format envelopes,
new feature flags, semantic changes to existing public surfaces) should
go through an RFC. Historical RFC context has been folded into the workspace book and crate-local documentation. New ones follow the
same format:

1. Branch off `main`.
2. Draft `<NNNN>-<short-name>.md` in your branch and include it in the PR.
3. Open a PR with the RFC. Discussion happens in PR comments.
4. Land when there's reasonable consensus and a writer + reviewer agree on
   the direction. Implementation usually follows in a separate PR.

## Local development tips

- **Faster builds**: `cargo build --no-default-features` skips the
  `metrics` facade and shaves ~3-5 s off cold builds.
- **DST tests**: see the `RUSTFLAGS="--cfg dst"` invocation above. The
  rest of the workspace is unaffected.
- **mdBook docs**: `mdbook build book/` produces the site under
  `book/build/`. Source lives in `book/src/`.
- **Perf bench drills**: `crates/perf-bench/README.md` documents the
  layer × backend × mode matrix.
- **Examples**: `cargo run --release -p demos --example <name>` for the
  workspace-level runnable examples.

## License

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in the work by you, as defined in the
Apache-2.0 license, shall be dual licensed as MIT OR Apache-2.0, without
any additional terms or conditions.

## Code of conduct

Be kind, be specific, be substantive. We don't have a separate
`CODE_OF_CONDUCT.md` yet because the contributor count is small; we'll
add one if/when the project grows enough to need formal enforcement
mechanics. Until then: assume good faith, respond to substance not tone,
and use github issues for technical disputes.

## Where to ask if you're stuck

- GitHub Issues for anything code-shaped (bug, feature, perf, doc).
- GitHub Discussions for "how would you approach X?" questions.
