CARGO ?= cargo

.PHONY: \
	help fmt check build test bench bench-mp \
	py-check py-test \
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
	@echo "  make py-check            - cargo check for Python extension crate"
	@echo "  make py-test             - run Python pytest suite"

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

py-check:
	@$(CARGO) check -p python-surface-archive

py-test:
	@cd python-surface-archive && python3 -m pytest

validate-ci-workflows:
	@bash scripts/validate_ci_workflows.sh

smoke:
	@$(MAKE) drift-check
	@$(MAKE) drift-check-shell-matrix
	@$(CARGO) check -p disruptor-mp --lib
	@$(CARGO) check -p myelon
	@$(CARGO) check -p python-surface-archive

orchestrate-rust:
	@$(CARGO) fmt --all
	@$(MAKE) drift-check
	@$(MAKE) drift-check-shell-matrix
	@$(CARGO) clippy -p disruptor-mp -- -D warnings
	@$(CARGO) clippy -p myelon -- -D warnings
	@$(CARGO) test -p disruptor-mp --lib
	@$(CARGO) test -p disruptor-mp --test multiprocess_cleanup
	@$(CARGO) test -p myelon --tests
	@$(CARGO) test -p disruptor-mp --test compile_api
	@$(CARGO) test -p myelon --test compile_api
	@$(CARGO) test -p disruptor-mp --benches --no-run
	@$(CARGO) test -p disruptor-mp --examples --no-run

orchestrate-python: validate-ci-workflows py-check py-test

orchestrate-all: orchestrate-rust orchestrate-python
