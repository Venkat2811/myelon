# competitive-bench Makefile — thin layer over competitive-bench-runner
#
# All orchestration (size tuning, adapter dispatch, server/client lifecycle,
# JSON collection) is in Rust: src/runner/ + src/bin/competitive_bench_runner.rs
#
# This Makefile handles: builds, third-party C/C++ builds, kill, clean.

SHELL := /bin/bash
WORKSPACE_MANIFEST := $(abspath ../..)/Cargo.toml
OUTDIR ?= $(CURDIR)/output/results
HEADON_DIR ?= $(CURDIR)/output/headon
BOOST_DIR := $(CURDIR)/third_party/boost_pingpong
OMPI_DIR := $(CURDIR)/third_party/ompi_pingpong
OMPI_MPICC ?= mpicc

RUNNER = cargo run -p competitive-bench --profile competitive \
	--manifest-path $(WORKSPACE_MANIFEST) --bin competitive_bench_runner --
SIGNAL = cargo run -p perf-bench --profile competitive \
	--manifest-path $(WORKSPACE_MANIFEST) --bin perf-bench-signal --
SUPER_TINY_OUTDIR := $(CURDIR)/output/super-tiny

.PHONY: build-all quick smoke extensive simple-smoke super-tiny \
	headon-smoke headon-full headon-extensive \
	kill-pingpong clean help

# === Build ===
build-all:
	@cargo build -p competitive-bench --profile competitive --manifest-path $(WORKSPACE_MANIFEST)
	@$(MAKE) -C $(BOOST_DIR)
	@$(MAKE) -C $(OMPI_DIR) MPICC="$(OMPI_MPICC)"

# === Runner tiers ===
quick: build-all
	$(RUNNER) --tier quick --adapters all --outdir $(OUTDIR) --throughput --fixed-rate --broadcast

smoke: build-all
	$(RUNNER) --tier smoke --adapters all --outdir $(OUTDIR) --throughput --fixed-rate --broadcast

extensive: build-all
	$(RUNNER) --tier extensive --adapters all --outdir $(OUTDIR) --throughput --fixed-rate --broadcast

simple-smoke: build-all
	@$(MAKE) kill-pingpong || true
	@rm -rf $(CURDIR)/output/simple-smoke
	$(RUNNER) --tier simple-smoke --adapters all --outdir $(CURDIR)/output/simple-smoke --throughput --fixed-rate --broadcast
	@echo "Results in: $(CURDIR)/output/simple-smoke"

super-tiny: build-all
	@$(MAKE) kill-pingpong || true
	@rm -rf $(SUPER_TINY_OUTDIR)
	@mkdir -p $(SUPER_TINY_OUTDIR)
	$(RUNNER) --tier super-tiny --adapters all --outdir $(SUPER_TINY_OUTDIR) --msgs 1000 --warmup 100 --throughput --fixed-rate --broadcast --timeout 60
	$(SIGNAL) --backend shm --consumers 1 --events 1000 --json-out $(SUPER_TINY_OUTDIR)/signal_shm.json
	$(SIGNAL) --backend mmap --consumers 1 --events 1000 --json-out $(SUPER_TINY_OUTDIR)/signal_mmap.json
	@echo "Results in: $(SUPER_TINY_OUTDIR)"

# === Headon ===
headon-smoke: build-all
	$(RUNNER) --tier headon-smoke --adapters disruptor-shm,rusteron --outdir $(HEADON_DIR) --throughput --fixed-rate

headon-full: build-all
	$(RUNNER) --tier headon-full --adapters disruptor-shm,rusteron --outdir $(HEADON_DIR) --throughput --fixed-rate

headon-extensive: build-all
	$(RUNNER) --tier headon-extensive --adapters disruptor-shm,rusteron --outdir $(HEADON_DIR) --throughput --fixed-rate

# === Individual adapter targets (convenience) ===
internal: build-all
	$(RUNNER) --adapters internal --outdir $(OUTDIR) --throughput --fixed-rate --broadcast

crossbar: build-all
	$(RUNNER) --adapters crossbar --outdir $(OUTDIR) --throughput --fixed-rate

boost: build-all
	$(RUNNER) --adapters boost --outdir $(OUTDIR) --throughput --fixed-rate

ompi: build-all
	$(RUNNER) --adapters ompi --outdir $(OUTDIR) --throughput --fixed-rate

zmq: build-all
	$(RUNNER) --adapters zeromq --outdir $(OUTDIR) --throughput --fixed-rate

iceoryx2: build-all
	$(RUNNER) --adapters iceoryx2 --outdir $(OUTDIR) --throughput --fixed-rate

rusteron: build-all
	$(RUNNER) --adapters rusteron --outdir $(OUTDIR) --throughput --fixed-rate

shmipc: build-all
	$(RUNNER) --adapters shmipc --outdir $(OUTDIR) --throughput --fixed-rate

# === Utilities ===
kill-pingpong:
	-@pkill -f 'perf-bench-pingpong' 2>/dev/null || true
	-@pkill -f 'perf-bench-broadcast' 2>/dev/null || true
	-@pkill -f 'crossbar_pingpong' 2>/dev/null || true
	-@pkill -f 'shmipc_pingpong' 2>/dev/null || true
	-@pkill -f 'boost_pingpong' 2>/dev/null || true
	-@pkill -f 'ompi_pingpong' 2>/dev/null || true
	-@pkill -f 'mpirun.*pingpong' 2>/dev/null || true
	-@pkill -f 'rusteron_pingpong' 2>/dev/null || true
	-@pkill -f 'iceoryx2_pingpong' 2>/dev/null || true
	-@pkill -f 'zmq_pingpong' 2>/dev/null || true
	-@pkill -f 'internal_broadcast' 2>/dev/null || true
	-@pkill -f 'crossbar_broadcast' 2>/dev/null || true

clean:
	rm -rf $(OUTDIR) $(HEADON_DIR) $(CURDIR)/output/simple-smoke $(SUPER_TINY_OUTDIR)

help:
	@echo "competitive-bench (Rust-first orchestration)"
	@echo "============================================="
	@echo ""
	@echo "Tiers (via runner binary):"
	@echo "  make quick          # core sizes (64B-2MB), all adapters, throughput + CO + broadcast"
	@echo "  make smoke          # smoke sizes, all adapters"
	@echo "  make extensive      # extensive sizes (16KB-64MB), all adapters"
	@echo "  make simple-smoke   # ultra-quick smoke"
	@echo "  make super-tiny     # broad fast gate: signal + ping-pong + broadcast at 100 warmup / 1k msgs"
	@echo ""
	@echo "Headon:"
	@echo "  make headon-smoke       # disruptor vs rusteron smoke"
	@echo "  make headon-full        # disruptor vs rusteron full"
	@echo "  make headon-extensive   # disruptor vs rusteron extensive"
	@echo ""
	@echo "Individual adapters:"
	@echo "  make internal   make crossbar   make boost   make ompi"
	@echo "  make zmq        make iceoryx2   make rusteron  make shmipc"
	@echo ""
	@echo "Utilities:"
	@echo "  make kill-pingpong  # kill leftover benchmark processes"
	@echo "  make clean          # remove all output"
