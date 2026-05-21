# Contributing to `myelon`

This repository publishes two crates:

- `disruptor-mp`: raw multiprocess SHM + mmap substrate
- `myelon`: layered transport crate on top of that substrate

Everything else in the workspace exists to test, benchmark, or demonstrate those two crates.

## TL;DR

```sh
git clone https://github.com/Venkat2811/myelon
cd myelon
cargo build
cargo test
cargo fmt --all --check
cargo clippy -p disruptor-mp -p myelon --lib --all-features -- -D warnings
cargo doc -p disruptor-mp -p myelon --no-deps
```

If those gates pass, the branch is usually in good shape for review.

## Workspace layout

```text
crates/
├── disruptor-mp/      # Publishable raw substrate.
├── myelon/            # Publishable layered transport crate.
├── myelon-env/        # Internal shared env-key and env-read helpers.
├── myelon-dst/        # Internal deterministic-simulation runner.
├── perf-bench/        # Internal transport sweep harness.
└── competitive-bench/ # Internal external-comparison harness.

examples/              # Runnable examples.
book/                  # mdBook source, maintained separately.
```

Only `disruptor-mp` and `myelon` are published to crates.io. `myelon-env`,
`myelon-dst`, `perf-bench`, and `competitive-bench` are internal workspace
support crates.

## Filing issues

- **Bugs**: include the workspace commit SHA, platform details, `rustc --version`, and the smallest repro you can manage.
- **Performance regressions**: include before/after numbers and the exact harness command that produced them.
- **Feature requests**: lead with the use case and why the current public surface is insufficient.

## Sending a PR

Standard GitHub flow: fork, branch, PR.

### Before opening a PR

Run the validation gates above.

If your change touches DST code under `crates/disruptor-mp/src/dst/` or `crates/myelon-dst/`, also run:

```sh
RUSTFLAGS="--cfg dst" cargo test -p disruptor-mp -p myelon -p myelon-dst \
    --features myelon-dst/_runner_bin
```

If your change touches benchmark code or benchmark-facing docs, also run the relevant smoke lane:

```sh
make -C crates/perf-bench super-tiny
make -C crates/competitive-bench super-tiny
```

### Commit message style

- Keep the subject short and concrete.
- Use an area prefix when it helps: `disruptor-mp:`, `myelon:`, `dst:`, `bench:`, `docs:`.
- For non-trivial commits, include a short verification block in the body.

### Code conventions

- Workspace lints are the source of truth. Do not add `#[allow(...)]` unless the lint itself is wrong for the case.
- Every `unsafe { ... }` block needs a `// SAFETY:` comment that states the invariant.
- Public items in `disruptor-mp` and `myelon` need rustdoc.
- Format with `cargo fmt --all`.
- Avoid `unwrap()` and `expect()` in non-test code unless the panic documents an invariant a maintainer would check before changing the code.

### What to expect from review

The review bar is straightforward:

- clear public API boundaries
- explicit invariants
- real verification, not assumed verification
- no hidden performance regressions

If a PR is out of scope, the preferred outcome is an explicit no with a concrete reason, not an ambiguous stall.

## RFCs

Use an RFC for substantive design changes:

- new public types
- new feature flags
- wire-format changes
- semantic changes to existing public behavior

The project already carries historical design context in the existing docs and book. New RFCs should stay lightweight and concrete.

## Local development tips

- `cargo build --no-default-features` is a good fast path when you do not need metrics/exporter integrations.
- `cargo run --release -p demos --example <name>` runs the first-party examples.
- `crates/perf-bench/README.md` documents the internal sweep surface.
- `crates/competitive-bench/README.md` documents the external-comparison surface.

## License

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as MIT OR Apache-2.0, without any additional terms or conditions.

## Code of conduct

Be specific, assume good faith, and argue from technical substance.

## Where to ask if you're stuck

- GitHub Issues for bugs, features, and performance work
- GitHub Discussions for design questions and usage questions
