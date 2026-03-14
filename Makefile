CARGO ?= cargo

.PHONY: \
	help fmt check build test bench bench-mp \
	test-rust-fast test-rust-extended test-rust-manifest \
	test-py-fast test-py-extended test-py-manifest py-check py-test \
	check-layer-boundaries check-layout-refs check-hot-path-ffi \
	smoke orchestrate-rust orchestrate-python orchestrate-all \
	validate-ci-workflows

help:
	@echo "myelon workspace commands"
	@echo "  make smoke               - fast wiring check for rust+python tiers"
	@echo "  make orchestrate-rust    - rust-tier CI-style workflow"
	@echo "  make orchestrate-python  - python-tier workflow"
	@echo "  make orchestrate-all     - rust + python workflows"
	@echo "  make validate-ci-workflows - verify copied workflow path/platform expectations"
	@echo "  make fmt                 - cargo fmt for workspace"
	@echo "  make check               - cargo check --workspace --all-targets"
	@echo "  make build               - cargo build --workspace"
	@echo "  make test                - cargo test --workspace"
	@echo "  make bench-mp            - run multiprocess benchmark set"
	@echo "  make test-rust-fast      - canonical disruptor-mp Linux Rust lane"
	@echo "  make test-rust-extended  - disruptor-mp Linux lane + stress/perf smoke"
	@echo "  make test-rust-manifest  - emit disruptor-mp machine-readable test manifest"
	@echo "  make test-py-fast        - canonical myelon-py Linux Python lane"
	@echo "  make test-py-extended    - myelon-py Linux lane + stress/perf/large-element lanes"
	@echo "  make test-py-manifest    - emit myelon-py machine-readable test manifest"
	@echo "  make py-check            - cargo check for Python extension crate"
	@echo "  make py-test             - alias for canonical myelon-py Linux Python lane"
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

test-rust-fast:
	@$(MAKE) test-linux

test-rust-extended:
	@$(MAKE) test-linux-extended

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

check-layer-boundaries:
	@python3 scripts/check_layer_boundaries.py

check-layout-refs:
	@python3 scripts/check_monorepo_layout_refs.py

check-hot-path-ffi:
	@python3 scripts/check_python_hot_path_ffi.py

validate-ci-workflows:
	@bash scripts/validate_ci_workflows.sh

smoke:
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
	@$(MAKE) test-rust-manifest
	@$(CARGO) test -p myelon --tests
	@$(CARGO) test -p myelon --test compile_api
	@$(CARGO) test -p disruptor-mp --benches --no-run
	@$(CARGO) test -p disruptor-mp --examples --no-run

orchestrate-python: validate-ci-workflows check-layer-boundaries check-layout-refs check-hot-path-ffi py-check test-py-fast test-py-manifest


orchestrate-all: orchestrate-rust orchestrate-python
