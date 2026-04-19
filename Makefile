CARGO ?= cargo
PERF_BENCH_MAKE := $(MAKE) -C crates/perf-bench
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
	workspace-smoke competitive myelon-sweep all-multi repeatability results \
	test-rust-fast test-rust-extended test-rust-perf-gate test-rust-manifest \
	test-py-fast test-py-extended test-py-manifest py-check py-test py-setup \
	check-layer-boundaries check-layout-refs check-hot-path-ffi \
	test-dst run-rust-examples run-rust-benches run-py-examples run-py-benches \
	smoke orchestrate-rust orchestrate-python orchestrate-all \
	validate-ci-workflows

help:
	@echo "myelon workspace commands"
	@echo "  make smoke               - perf-bench smoke lane (~60s)"
	@echo "  make workspace-smoke     - fast wiring check for rust+python tiers"
	@echo "  make orchestrate-rust    - rust-tier CI-style workflow"
	@echo "  make orchestrate-python  - python-tier workflow"
	@echo "  make orchestrate-all     - rust + python workflows"
	@echo "  make validate-ci-workflows - verify copied workflow path/platform expectations"
	@echo "  make fmt                 - cargo fmt for workspace"
	@echo "  make check               - cargo check --workspace --all-targets"
	@echo "  make build               - cargo build --workspace"
	@echo "  make test                - cargo test --workspace"
	@echo "  make bench-mp            - run multiprocess benchmark set"
	@echo "  make bench-matrix        - run perf-bench matrix targets"
	@echo "  make bench-matrix-smoke  - run perf-bench smoke subset"
	@echo "  make competitive         - run competitive ping-pong benches"
	@echo "  make myelon-sweep        - run framed + zero-copy myelon sweeps"
	@echo "  make all-multi           - run multi-consumer coverage benches"
	@echo "  make repeatability       - run repeated canonical perf-bench variance checks"
	@echo "  make results             - write benchmark JSON/CSV/MD artifacts"
	@echo "  make test-rust-fast      - canonical disruptor-mp Linux Rust lane"
	@echo "  make test-rust-extended  - disruptor-mp Linux lane + stress/perf smoke"
	@echo "  make test-rust-perf-gate - live disruptor-mp Linux perf regression gate"
	@echo "  make test-rust-manifest  - emit disruptor-mp machine-readable test manifest"
	@echo "  make test-py-fast        - canonical myelon-py Linux Python lane"
	@echo "  make test-py-extended    - myelon-py Linux lane + stress/perf/large-element lanes"
	@echo "  make test-py-manifest    - emit myelon-py machine-readable test manifest"
	@echo "  make py-check            - cargo check for Python extension crate"
	@echo "  make py-test             - alias for canonical myelon-py Linux Python lane"
	@echo "  make test-dst            - run deterministic DST lanes for disruptor-mp and myelon"
	@echo "  make run-rust-examples   - run supported Rust examples with timeout protection"
	@echo "  make run-rust-benches    - run supported Rust benches with timeout protection"
	@echo "  make run-py-examples     - run supported Python examples with timeout protection"
	@echo "  make run-py-benches      - run supported Python benchmark suite with timeout protection"
	@echo "  make check-layer-boundaries - fail cross-layer import violations"
	@echo "  make check-layout-refs   - fail stale pre-monorepo user-facing paths"
	@echo "  make check-hot-path-ffi  - fail unexpected Python hot-path per-event FFI spread"

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
	@$(CARGO) bench -p disruptor-mp --bench ipc_shm
	@$(CARGO) bench -p disruptor-mp --bench ipc_shm_high_load
	@$(CARGO) bench -p disruptor-mp --bench benchmark_all_wait_strategies_auto_rust
	@$(CARGO) bench -p disruptor-mp --bench competitive_pingpong

bench-matrix:
	@$(PERF_BENCH_MAKE) all

bench-matrix-raw:
	@$(PERF_BENCH_MAKE) raw

bench-matrix-framed:
	@$(PERF_BENCH_MAKE) framed

bench-matrix-codec:
	@$(PERF_BENCH_MAKE) codec

bench-matrix-wait:
	@$(PERF_BENCH_MAKE) wait

bench-matrix-competitive:
	@$(PERF_BENCH_MAKE) competitive

bench-matrix-layout:
	@$(PERF_BENCH_MAKE) layout

bench-matrix-smoke:
	@$(PERF_BENCH_MAKE) smoke

competitive:
	@$(PERF_BENCH_MAKE) competitive

myelon-sweep:
	@$(PERF_BENCH_MAKE) myelon-sweep

all-multi:
	@$(PERF_BENCH_MAKE) all-multi

repeatability:
	@$(PERF_BENCH_MAKE) repeatability

results:
	@$(PERF_BENCH_MAKE) results

test-rust-fast:
	@$(MAKE) test-linux

test-rust-extended:
	@$(MAKE) test-linux-extended

test-rust-perf-gate:
	@$(MAKE) test-perf-gate

test-rust-manifest:
	@$(MAKE) test-manifest-json

test-py-fast:
	@$(MAKE) -C python-surface-archive test-linux

test-py-extended:
	@$(MAKE) -C python-surface-archive test-linux-extended

test-py-manifest:
	@$(MAKE) -C python-surface-archive test-manifest-json

py-check:
	@$(CARGO) check -p python-surface-archive

py-test:
	@$(MAKE) test-py-fast

py-setup:
	@$(MAKE) -C python-surface-archive setup

test-dst:
	@$(MAKE) test-dst
	@$(CARGO) test -p myelon --test dst_contract -- --test-threads=1
	@$(CARGO) test -p myelon --test dst_profiles -- --test-threads=1
	@$(CARGO) test -p myelon --test dst_runtime -- --test-threads=1

run-rust-examples:
	@$(MAKE) example-all
	$(call ROOT_RUN_WITH_TIMEOUT,$(ROOT_EXAMPLE_TIMEOUT),$(CARGO) run -p myelon --example fixed_inference_topology)

run-rust-benches:
	@$(MAKE) bench-all

run-py-examples:
	@$(MAKE) -C python-surface-archive example-all

run-py-benches:
	@$(MAKE) -C python-surface-archive benchmark-all

check-layer-boundaries:
	@python3 scripts/check_layer_boundaries.py

check-layout-refs:
	@python3 scripts/check_monorepo_layout_refs.py

check-hot-path-ffi:
	@python3 scripts/check_python_hot_path_ffi.py

validate-ci-workflows:
	@bash scripts/validate_ci_workflows.sh

smoke:
	@$(PERF_BENCH_MAKE) smoke

workspace-smoke:
	@$(MAKE) check-layer-boundaries
	@$(MAKE) check-layout-refs
	@$(MAKE) check-hot-path-ffi
	@$(MAKE) drift-check
	@$(MAKE) drift-check-shell-matrix
	@$(MAKE) test-unit
	@$(CARGO) check -p myelon
	@$(CARGO) check -p python-surface-archive

orchestrate-rust:
	@$(CARGO) fmt --all
	@$(MAKE) check-layer-boundaries
	@$(MAKE) check-layout-refs
	@$(MAKE) check-hot-path-ffi
	@$(MAKE) drift-check
	@$(MAKE) drift-check-shell-matrix
	@$(CARGO) clippy -p disruptor-mp -- -D warnings
	@$(CARGO) clippy -p myelon -- -D warnings
	@$(MAKE) test-rust-fast
	@$(MAKE) test-rust-perf-gate
	@$(MAKE) test-rust-manifest
	@$(CARGO) test -p myelon --tests
	@$(CARGO) test -p myelon --test compile_api
	@$(CARGO) test -p disruptor-mp --benches --no-run
	@$(CARGO) test -p disruptor-mp --examples --no-run

orchestrate-python: validate-ci-workflows check-layer-boundaries check-layout-refs check-hot-path-ffi py-check test-py-fast test-py-manifest


orchestrate-all: orchestrate-rust orchestrate-python
