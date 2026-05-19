CARGO ?= cargo
PERF_BENCH_MAKE := $(MAKE) -C crates/perf-bench
COMPETITIVE_BENCH_MAKE := $(MAKE) -C crates/competitive-bench
ROOT_TIMEOUT_BIN := $(shell if command -v timeout >/dev/null 2>&1; then printf '%s' timeout; elif command -v gtimeout >/dev/null 2>&1; then printf '%s' gtimeout; fi)
ROOT_EXAMPLE_TIMEOUT ?= 60

define ROOT_RUN_WITH_TIMEOUT
	@if [ -n "$(ROOT_TIMEOUT_BIN)" ]; then \
		$(ROOT_TIMEOUT_BIN) --preserve-status $(1) bash -lc 'set -euo pipefail; $(2)'; \
	else \
		bash -lc 'set -euo pipefail; $(2)'; \
	fi
endef

.PHONY: \
	help fmt check build test bench bench-mp bench-matrix bench-matrix-smoke \
	bench-matrix-raw bench-matrix-framed bench-matrix-codec bench-matrix-wait bench-matrix-competitive bench-matrix-layout \
	workspace-smoke competitive \
	test-rust-fast test-rust-extended test-rust-perf-gate \
	test-dst test-dst-fuzz test-dst-nightly run-rust-examples run-rust-benches \
	smoke orchestrate-rust validate-ci-workflows

help:
	@echo "myelon workspace commands"
	@echo "  make smoke               - perf-bench smoke lane (~60s)"
	@echo "  make workspace-smoke     - fast wiring check for rust tiers"
	@echo "  make orchestrate-rust    - rust-tier CI-style workflow"
	@echo "  make validate-ci-workflows - verify copied workflow path/platform expectations"
	@echo "  make fmt                 - cargo fmt for workspace"
	@echo "  make check               - cargo check --workspace --all-targets"
	@echo "  make build               - cargo build --workspace"
	@echo "  make test                - cargo test --workspace"
	@echo "  make bench-mp            - run raw multiprocess perf-bench subset"
	@echo "  make bench-matrix        - run perf-bench matrix targets"
	@echo "  make bench-matrix-smoke  - run perf-bench smoke subset"
	@echo "  make competitive         - run competitive-bench quick parity tier"
	@echo "  make test-rust-fast      - canonical disruptor-mp Linux Rust lane"
	@echo "  make test-rust-extended  - disruptor-mp tests + example/bench compile + perf smoke"
	@echo "  make test-rust-perf-gate - live workspace perf gate (perf-bench + competitive-bench super-tiny)"
	@echo "  make test-dst            - run deterministic DST lanes for disruptor-mp and myelon"
	@echo "  make test-dst-fuzz       - run 100-seed DST CI fuzz envelopes"
	@echo "  make test-dst-nightly    - run 1000-seed DST nightly fuzz envelopes"
	@echo "  make run-rust-examples   - run supported Rust examples with timeout protection"
	@echo "  make run-rust-benches    - run supported Rust benches with timeout protection"

fmt:
	@$(CARGO) fmt --all

check:
	@$(CARGO) check --workspace --all-targets

build:
	@$(CARGO) build --workspace

test:
	@$(CARGO) test --workspace

bench:
	@$(CARGO) bench --workspace

bench-mp:
	@$(PERF_BENCH_MAKE) raw

bench-matrix:
	@$(PERF_BENCH_MAKE) extensive

bench-matrix-raw:
	@$(PERF_BENCH_MAKE) raw

bench-matrix-framed:
	@$(PERF_BENCH_MAKE) framed

bench-matrix-codec:
	@$(PERF_BENCH_MAKE) codec

bench-matrix-wait:
	@$(PERF_BENCH_MAKE) wait

bench-matrix-competitive:
	@$(COMPETITIVE_BENCH_MAKE) quick

bench-matrix-layout:
	@$(PERF_BENCH_MAKE) layout

bench-matrix-smoke:
	@$(PERF_BENCH_MAKE) smoke

competitive:
	@$(COMPETITIVE_BENCH_MAKE) quick

test-rust-fast:
	@$(CARGO) test -p disruptor-mp --lib --tests

test-rust-extended:
	@$(CARGO) test -p disruptor-mp --lib --tests
	@$(CARGO) test -p demos --examples --no-run
	@$(CARGO) test -p disruptor-mp --benches --no-run
	@$(PERF_BENCH_MAKE) smoke

test-rust-perf-gate:
	@$(PERF_BENCH_MAKE) super-tiny
	@$(COMPETITIVE_BENCH_MAKE) super-tiny

test-dst:
	@RUSTFLAGS="--cfg dst" $(CARGO) test -p myelon-dst --features _runner_bin -- --test-threads=1
	@RUSTFLAGS="--cfg dst" $(CARGO) test -p myelon --test dst_framed -- --test-threads=1
	@RUSTFLAGS="--cfg dst" $(CARGO) test -p myelon --features 'rkyv flatbuffers' --test dst_codec -- --test-threads=1
	@RUSTFLAGS="--cfg dst" $(CARGO) test -p myelon --test dst_contract -- --test-threads=1
	@RUSTFLAGS="--cfg dst" $(CARGO) test -p myelon --test dst_profiles -- --test-threads=1
	@RUSTFLAGS="--cfg dst" $(CARGO) test -p myelon --test dst_runtime -- --test-threads=1

test-dst-fuzz:
	@RUSTFLAGS="--cfg dst" $(CARGO) test -p myelon-dst --features _runner_bin dst_fuzz_raw_ring_ci_seed_matrix -- --ignored --nocapture --test-threads=1
	@RUSTFLAGS="--cfg dst" $(CARGO) test -p myelon --test dst_framed dst_fuzz_framed_ci_seed_matrix -- --ignored --nocapture --test-threads=1
	@RUSTFLAGS="--cfg dst" $(CARGO) test -p myelon --features 'rkyv flatbuffers' --test dst_codec dst_fuzz_codec_ci_seed_matrix -- --ignored --nocapture --test-threads=1

test-dst-nightly:
	@RUSTFLAGS="--cfg dst" $(CARGO) test -p myelon-dst --features _runner_bin dst_fuzz_raw_ring_nightly_seed_matrix -- --ignored --nocapture --test-threads=1
	@RUSTFLAGS="--cfg dst" $(CARGO) test -p myelon --test dst_framed dst_fuzz_framed_nightly_seed_matrix -- --ignored --nocapture --test-threads=1
	@RUSTFLAGS="--cfg dst" $(CARGO) test -p myelon --features 'rkyv flatbuffers' --test dst_codec dst_fuzz_codec_nightly_seed_matrix -- --ignored --nocapture --test-threads=1

run-rust-examples:
	@$(CARGO) test -p demos --examples --no-run
	$(call ROOT_RUN_WITH_TIMEOUT,$(ROOT_EXAMPLE_TIMEOUT),$(CARGO) run -p demos --example fixed_inference_topology)

run-rust-benches:
	@$(CARGO) test -p disruptor-mp --benches --no-run
	@$(CARGO) bench -p perf-bench --no-run

validate-ci-workflows:
	@bash scripts/validate_ci_workflows.sh

smoke:
	@$(PERF_BENCH_MAKE) smoke

workspace-smoke:
	@$(CARGO) test -p disruptor-mp --lib --tests
	@$(CARGO) check -p myelon

orchestrate-rust:
	@$(CARGO) fmt --all
	@$(CARGO) clippy -p disruptor-mp -- -D warnings
	@$(CARGO) clippy -p myelon -- -D warnings
	@$(MAKE) test-rust-fast
	@$(MAKE) test-rust-perf-gate
	@$(CARGO) test -p myelon --tests
	@$(CARGO) test -p myelon --test compile_api
	@$(CARGO) test -p disruptor-mp --benches --no-run
	@$(CARGO) test -p demos --examples --no-run
