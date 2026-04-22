SHELL := /bin/bash

include config/parity.mk

WORKSPACE_MANIFEST := $(abspath ../..)/Cargo.toml
OUTDIR ?= $(CURDIR)/output/results
HEADON_DIR ?= $(CURDIR)/output/headon
BOOST_DIR := $(CURDIR)/third_party/boost_pingpong
OMPI_DIR := $(CURDIR)/third_party/ompi_pingpong
CROSSBAR_BIN := $(abspath ../..)/target/competitive/crossbar_pingpong
SHMIPC_BIN := $(abspath ../..)/target/competitive/shmipc_pingpong
RUSTERON_BIN := $(abspath ../..)/target/competitive/rusteron_pingpong
ZMQ_BIN := $(abspath ../..)/target/competitive/zmq_pingpong
BOOST_BIN := $(BOOST_DIR)/boost_pingpong
OMPI_BIN := $(OMPI_DIR)/pingpong
OMPI_MPICC ?= mpicc
OMPI_MPIRUN ?= mpirun

ifeq ($(strip $(CARGO_TIMEOUT_SEC)),)
  TIMEOUT_CMD :=
else
  TIMEOUT_CMD := timeout $(CARGO_TIMEOUT_SEC)s
endif

ifeq ($(strip $(OMPI_TIMEOUT_SEC)),)
  OMPI_TIMEOUT_CMD :=
else
  OMPI_TIMEOUT_CMD := timeout $(OMPI_TIMEOUT_SEC)s
endif

.PHONY: help clean build-all internal-build internal-run-quick internal-run-fixed-rate-quick \
	aggregate headon headon-smoke headon-full internal-headon verify-align verify-align-quick \
	kill-pingpong run-all-quick run-all-fixed-rate-quick ubermensh ubermensh-smoke \
	ubermensh-smoke-no-ompi ubermensh-smoke-no-timeout external-status \
	crossbar-build crossbar-run-quick crossbar-run-fixed-rate-quick \
	shmipc-build shmipc-run-quick shmipc-run-fixed-rate-quick \
	boost-build boost-run-quick boost-run-fixed-rate-quick \
	ompi-build ompi-run-quick ompi-run-fixed-rate-quick \
	rusteron-build rusteron-run-quick rusteron-run-fixed-rate-quick \
	zmq-build zmq-run-quick zmq-run-fixed-rate-quick \
	zmq-ipc-run-quick zmq-ipc-run-fixed-rate-quick \
	zmq-ipc-abs-run-quick zmq-ipc-abs-run-fixed-rate-quick \
	zmq-tcp-run-quick zmq-tcp-run-fixed-rate-quick

help:
	@echo "Competitive Bench (mp_ipc_world_domination parity scope)"
	@echo "========================================================="
	@echo "make internal-run-quick             # raw disruptor + raw myelon SHM+mmap throughput grids"
	@echo "make internal-run-fixed-rate-quick  # raw disruptor + raw myelon SHM+mmap fixed-rate CO grids"
	@echo "make aggregate                      # pretty aggregate tables from $(OUTDIR)"
	@echo "make internal-headon                # raw disruptor vs raw myelon SHM head-on summary"
	@echo "make headon-smoke                   # raw disruptor vs rusteron smoke head-on"
	@echo "make headon-full                    # raw disruptor vs rusteron full head-on"
	@echo "make run-all-quick                  # parity target surface (internal SHM+mmap + wired peers)"
	@echo "make run-all-fixed-rate-quick       # parity fixed-rate target surface (internal SHM+mmap + wired peers)"
	@echo "make ubermensh                      # aggregate whatever parity artifacts are present"
	@echo "make ubermensh-smoke                # smoke counts for current parity surface"
	@echo "make crossbar-run-quick             # crossbar throughput parity grid"
	@echo "make crossbar-run-fixed-rate-quick  # crossbar fixed-rate CO parity grid"
	@echo "make shmipc-run-quick               # shmipc-rs throughput parity grid"
	@echo "make shmipc-run-fixed-rate-quick    # shmipc-rs fixed-rate CO parity grid"
	@echo "make boost-run-quick                # boost::interprocess throughput parity grid"
	@echo "make boost-run-fixed-rate-quick     # boost::interprocess fixed-rate CO parity grid"
	@echo "make ompi-run-quick                 # Open MPI throughput parity grid"
	@echo "make ompi-run-fixed-rate-quick      # Open MPI fixed-rate CO parity grid"
	@echo "make rusteron-run-quick             # rusteron throughput parity grid"
	@echo "make rusteron-run-fixed-rate-quick  # rusteron fixed-rate CO parity grid"
	@echo "make zmq-run-quick                  # zeromq default(ipc) throughput parity grid"
	@echo "make zmq-run-fixed-rate-quick       # zeromq default(ipc) fixed-rate CO parity grid"
	@echo "make verify-align                   # raw disruptor + rusteron alignment verification"
	@echo "make kill-pingpong                  # clean leftover competitive peer processes"

clean:
	rm -rf $(OUTDIR) $(HEADON_DIR)

build-all: internal-build crossbar-build shmipc-build boost-build ompi-build rusteron-build zmq-build external-status

internal-build:
	@cargo test -p competitive-bench --manifest-path $(WORKSPACE_MANIFEST) --lib

internal-run-quick:
	@mkdir -p $(OUTDIR)
	@echo "[run] internal SHM+mmap parity throughput sizes: $(SIZES_SMALL)"
	@for sz in $(SIZES_SMALL); do \
		$(TIMEOUT_CMD) cargo bench -p perf-bench --profile competitive --manifest-path $(WORKSPACE_MANIFEST) --bench competitive_shm -- --json-canonical --no-compare -s $$sz -n $(NUM_MESSAGES) -w $(WARMUP) > $(OUTDIR)/disruptor_$${sz}.json && echo "  ✓ disruptor $$sz B" || exit 1; \
		$(TIMEOUT_CMD) cargo bench -p perf-bench --profile competitive --manifest-path $(WORKSPACE_MANIFEST) --bench competitive_mmap -- --json-canonical --no-compare -s $$sz -n $(NUM_MESSAGES) -w $(WARMUP) > $(OUTDIR)/disruptor_mmap_$${sz}.json && echo "  ✓ disruptor_mmap $$sz B" || exit 1; \
		$(TIMEOUT_CMD) cargo bench -p perf-bench --profile competitive --manifest-path $(WORKSPACE_MANIFEST) --bench competitive_raw_myelon_shm -- --json --no-compare -s $$sz -n $(NUM_MESSAGES) -w $(WARMUP) > $(OUTDIR)/myelon_raw_$${sz}.json && echo "  ✓ myelon_raw $$sz B" || exit 1; \
		$(TIMEOUT_CMD) cargo bench -p perf-bench --profile competitive --manifest-path $(WORKSPACE_MANIFEST) --bench competitive_raw_myelon_mmap -- --json --no-compare -s $$sz -n $(NUM_MESSAGES) -w $(WARMUP) > $(OUTDIR)/myelon_raw_mmap_$${sz}.json && echo "  ✓ myelon_raw_mmap $$sz B" || exit 1; \
	done

internal-run-fixed-rate-quick:
	@mkdir -p $(OUTDIR)
	@echo "[run] internal SHM+mmap parity fixed-rate sizes: $(SIZES_SMALL), rates: $(RATES)"
	@for sz in $(SIZES_SMALL); do \
		for rate in $(RATES); do \
			$(TIMEOUT_CMD) cargo bench -p perf-bench --profile competitive --manifest-path $(WORKSPACE_MANIFEST) --bench competitive_shm -- --json-canonical --no-compare -s $$sz -n $(NUM_MESSAGES) -w $(WARMUP) --target-rate $$rate > $(OUTDIR)/disruptor_$${sz}_$${rate}.json && echo "  ✓ disruptor $$sz B @ $$rate/s" || exit 1; \
			$(TIMEOUT_CMD) cargo bench -p perf-bench --profile competitive --manifest-path $(WORKSPACE_MANIFEST) --bench competitive_mmap -- --json-canonical --no-compare -s $$sz -n $(NUM_MESSAGES) -w $(WARMUP) --target-rate $$rate > $(OUTDIR)/disruptor_mmap_$${sz}_$${rate}.json && echo "  ✓ disruptor_mmap $$sz B @ $$rate/s" || exit 1; \
			$(TIMEOUT_CMD) cargo bench -p perf-bench --profile competitive --manifest-path $(WORKSPACE_MANIFEST) --bench competitive_raw_myelon_shm -- --json --no-compare -s $$sz -n $(NUM_MESSAGES) -w $(WARMUP) --target-rate $$rate > $(OUTDIR)/myelon_raw_$${sz}_$${rate}.json && echo "  ✓ myelon_raw $$sz B @ $$rate/s" || exit 1; \
			$(TIMEOUT_CMD) cargo bench -p perf-bench --profile competitive --manifest-path $(WORKSPACE_MANIFEST) --bench competitive_raw_myelon_mmap -- --json --no-compare -s $$sz -n $(NUM_MESSAGES) -w $(WARMUP) --target-rate $$rate > $(OUTDIR)/myelon_raw_mmap_$${sz}_$${rate}.json && echo "  ✓ myelon_raw_mmap $$sz B @ $$rate/s" || exit 1; \
		done; \
	done

aggregate:
	@python3 scripts/aggregate_results.py $(OUTDIR)

internal-headon:
	@mkdir -p $(HEADON_DIR)/disruptor $(HEADON_DIR)/myelon_raw
	@echo "[headon] internal raw disruptor vs raw myelon sizes: $(HEADON_SIZES)"
	@for sz in $(HEADON_SIZES); do \
		$(TIMEOUT_CMD) cargo bench -p perf-bench --profile competitive --manifest-path $(WORKSPACE_MANIFEST) --bench competitive_shm -- --json-canonical --no-compare -s $$sz -n $(NUM_MESSAGES) -w $(WARMUP) > $(HEADON_DIR)/disruptor/pingpong_$${sz}B.json && echo "  ✓ disruptor $$sz B" || exit 1; \
		$(TIMEOUT_CMD) cargo bench -p perf-bench --profile competitive --manifest-path $(WORKSPACE_MANIFEST) --bench competitive_raw_myelon_shm -- --json --no-compare -s $$sz -n $(NUM_MESSAGES) -w $(WARMUP) > $(HEADON_DIR)/myelon_raw/pingpong_$${sz}B.json && echo "  ✓ myelon_raw $$sz B" || exit 1; \
	done
	@python3 scripts/aggregate_headon.py $(HEADON_DIR)

run-all-quick: internal-run-quick shmipc-run-quick boost-run-quick ompi-run-quick rusteron-run-quick crossbar-run-quick zmq-run-quick
	@echo "Results in: $(OUTDIR)"

run-all-fixed-rate-quick: internal-run-fixed-rate-quick shmipc-run-fixed-rate-quick boost-run-fixed-rate-quick ompi-run-fixed-rate-quick rusteron-run-fixed-rate-quick crossbar-run-fixed-rate-quick zmq-run-fixed-rate-quick
	@echo "Results in: $(OUTDIR)"

ubermensh: build-all run-all-quick run-all-fixed-rate-quick
	@echo ""
	@echo "===== Aggregating results from $(OUTDIR) ====="
	@python3 scripts/aggregate_results.py $(OUTDIR) || true

ubermensh-smoke:
	@$(MAKE) kill-pingpong || true
	@rm -rf $(OUTDIR)
	@WARMUP=10 NUM_MESSAGES=100 RATES=400000 $(MAKE) ubermensh

ubermensh-smoke-no-timeout:
	@$(MAKE) kill-pingpong || true
	@rm -rf $(OUTDIR)
	@WARMUP=10 NUM_MESSAGES=100 RATES=400000 OMPI_TIMEOUT_SEC= $(MAKE) ubermensh

ubermensh-smoke-no-ompi:
	@$(MAKE) kill-pingpong || true
	@rm -rf $(OUTDIR)
	@WARMUP=10 NUM_MESSAGES=100 RATES=400000 $(MAKE) internal-run-quick internal-run-fixed-rate-quick shmipc-run-quick boost-run-quick rusteron-run-quick crossbar-run-quick zmq-run-quick shmipc-run-fixed-rate-quick boost-run-fixed-rate-quick rusteron-run-fixed-rate-quick crossbar-run-fixed-rate-quick zmq-run-fixed-rate-quick
	@echo ""
	@echo "===== Aggregating results from $(OUTDIR) ====="
	@python3 scripts/aggregate_results.py $(OUTDIR) || true

headon: headon-full
headon-smoke:
	@set -e; \
	 echo "[headon] raw disruptor vs rusteron (smoke) sizes: $(HEADON_SIZES)"; \
	 rm -rf $(HEADON_DIR) && mkdir -p $(HEADON_DIR)/disruptor $(HEADON_DIR)/rusteron; \
	 echo "[headon] rusteron throughput"; \
	 $(MAKE) WARMUP=10 NUM_MESSAGES=100 SIZES_SMALL="$(HEADON_SIZES)" rusteron-run-quick; \
	 for sz in $(HEADON_SIZES); do \
	   src="$(OUTDIR)/rusteron_$$sz.json"; \
	   [ -f "$$src" ] && cp -f "$$src" "$(HEADON_DIR)/rusteron/pingpong_$${sz}B.json" || true; \
	 done; \
	 echo "[headon] disruptor throughput"; \
	 for sz in $(HEADON_SIZES); do \
	   $(TIMEOUT_CMD) cargo bench -p perf-bench --profile competitive --manifest-path $(WORKSPACE_MANIFEST) --bench competitive_shm -- --json-canonical --no-compare -s $$sz -n 100 -w 10 > $(HEADON_DIR)/disruptor/pingpong_$${sz}B.json || exit 1; \
	 done; \
	 echo "[headon] rusteron fixed-rate (CO) @ $(HEADON_RATE_SMOKE)/s"; \
	 $(MAKE) WARMUP=$(CO_WARMUP) NUM_MESSAGES=$(CO_NUM_MESSAGES) SIZES_SMALL="$(HEADON_SIZES)" RATES=$(HEADON_RATE_SMOKE) rusteron-run-fixed-rate-quick; \
	 for f in $(OUTDIR)/rusteron_*_$(HEADON_RATE_SMOKE).json; do \
	   [ -f "$$f" ] || continue; \
	   b=$$(basename "$$f"); s=$${b#rusteron_}; size=$${s%_$(HEADON_RATE_SMOKE).json}; \
	   cp -f "$$f" "$(HEADON_DIR)/rusteron/co_$${size}B_$(HEADON_RATE_SMOKE).json"; \
	 done; \
	 echo "[headon] disruptor fixed-rate (CO) @ $(HEADON_RATE_SMOKE)/s"; \
	 for sz in $(HEADON_SIZES); do \
	   $(TIMEOUT_CMD) cargo bench -p perf-bench --profile competitive --manifest-path $(WORKSPACE_MANIFEST) --bench competitive_shm -- --json-canonical --no-compare -s $$sz -n $(CO_NUM_MESSAGES) -w $(CO_WARMUP) --target-rate $(HEADON_RATE_SMOKE) > $(HEADON_DIR)/disruptor/co_$${sz}B_$(HEADON_RATE_SMOKE).json || exit 1; \
	 done; \
	 echo ""; \
	 echo "===== raw disruptor vs rusteron (smoke) ====="; \
	 python3 scripts/aggregate_headon.py $(HEADON_DIR)
headon-full:
	@set -e; \
	 echo "[headon] raw disruptor vs rusteron (full) sizes: $(HEADON_SIZES)"; \
	 rm -rf $(HEADON_DIR) && mkdir -p $(HEADON_DIR)/disruptor $(HEADON_DIR)/rusteron; \
	 echo "[headon] rusteron throughput"; \
	 $(MAKE) WARMUP=$(WARMUP) NUM_MESSAGES=$(NUM_MESSAGES) SIZES_SMALL="$(HEADON_SIZES)" rusteron-run-quick; \
	 for sz in $(HEADON_SIZES); do \
	   src="$(OUTDIR)/rusteron_$$sz.json"; \
	   [ -f "$$src" ] && cp -f "$$src" "$(HEADON_DIR)/rusteron/pingpong_$${sz}B.json" || true; \
	 done; \
	 echo "[headon] disruptor throughput"; \
	 for sz in $(HEADON_SIZES); do \
	   $(TIMEOUT_CMD) cargo bench -p perf-bench --profile competitive --manifest-path $(WORKSPACE_MANIFEST) --bench competitive_shm -- --json-canonical --no-compare -s $$sz -n $(NUM_MESSAGES) -w $(WARMUP) > $(HEADON_DIR)/disruptor/pingpong_$${sz}B.json || exit 1; \
	 done; \
	 echo "[headon] rusteron fixed-rate (CO) full rates: $(RATES)"; \
	 $(MAKE) WARMUP=$(WARMUP) NUM_MESSAGES=$(NUM_MESSAGES) SIZES_SMALL="$(HEADON_SIZES)" rusteron-run-fixed-rate-quick; \
	 for f in $(OUTDIR)/rusteron_*_*.json; do \
	   [ -f "$$f" ] || continue; \
	   b=$$(basename "$$f"); rate=$${b##*_}; rate=$${rate%.json}; tmp=$${b%_$$rate.json}; size=$${tmp#rusteron_}; \
	   cp -f "$$f" "$(HEADON_DIR)/rusteron/co_$${size}B_$$rate.json"; \
	 done; \
	 echo "[headon] disruptor fixed-rate (CO) full rates: $(RATES)"; \
	 for sz in $(HEADON_SIZES); do \
	   for rate in $(RATES); do \
	     $(TIMEOUT_CMD) cargo bench -p perf-bench --profile competitive --manifest-path $(WORKSPACE_MANIFEST) --bench competitive_shm -- --json-canonical --no-compare -s $$sz -n $(NUM_MESSAGES) -w $(WARMUP) --target-rate $$rate > $(HEADON_DIR)/disruptor/co_$${sz}B_$$rate.json || exit 1; \
	   done; \
	 done; \
	 echo ""; \
	 echo "===== raw disruptor vs rusteron (full) ====="; \
	 python3 scripts/aggregate_headon.py $(HEADON_DIR)

verify-align:
	@$(MAKE) verify-align-disruptor verify-align-rusteron
verify-align-disruptor:
	@echo "[verify] raw disruptor alignment"
	@cargo run --quiet -p disruptor-mp --manifest-path $(WORKSPACE_MANIFEST) --example verify_alignment
verify-align-rusteron: rusteron-build
	@echo "[verify] rusteron (aeron ipc) alignment per size: $(SIZES_SMALL)"
	@for sz in $(SIZES_SMALL); do \
		aeron_libdirs=$$(find $(abspath ../..)/target/competitive/build -type f \( -name 'libaeron.so' -o -name 'libaeron_driver.so' \) -printf '%h:' | sed 's/:$$//'); \
		LD_LIBRARY_PATH="$$aeron_libdirs:$$LD_LIBRARY_PATH" $(RUSTERON_BIN) --verify-align --message-size $$sz || exit 1; \
	done
verify-align-quick: verify-align

kill-pingpong:
	-@pkill -f 'competitive_shm' 2>/dev/null || true
	-@pkill -f 'competitive_mmap' 2>/dev/null || true
	-@pkill -f 'competitive_raw_myelon_shm' 2>/dev/null || true
	-@pkill -f 'competitive_raw_myelon_mmap' 2>/dev/null || true
	-@pkill -f 'crossbar_pingpong' 2>/dev/null || true
	-@pkill -f 'shmipc_pingpong' 2>/dev/null || true
	-@pkill -f '/third_party/boost_pingpong/boost_pingpong' 2>/dev/null || true
	-@pkill -f '/third_party/ompi_pingpong/pingpong' 2>/dev/null || true
	-@pkill -f 'mpirun .*third_party/ompi_pingpong/pingpong' 2>/dev/null || true
	-@pkill -f 'rusteron_pingpong' 2>/dev/null || true
	-@pkill -f 'zmq_pingpong' 2>/dev/null || true
	-@pgrep -fa 'competitive_shm|competitive_mmap|competitive_raw_myelon_shm|competitive_raw_myelon_mmap|crossbar_pingpong|shmipc_pingpong|boost_pingpong|ompi_pingpong|rusteron_pingpong|zmq_pingpong|mpirun .*third_party/ompi_pingpong/pingpong' || true

external-status:
	@echo "[status] internal parity backends: raw disruptor/myelon over shm + mmap"
	@echo "[status] wired peers: shmipc-rs, boost, ompi, rusteron, crossbar, zeromq"
	@echo "         extra zeromq transports: ipc-abs, tcp"

crossbar-build:
	@cargo build -p competitive-bench --profile competitive --manifest-path $(WORKSPACE_MANIFEST) --bin crossbar_pingpong

crossbar-run-quick: crossbar-build
	@mkdir -p $(OUTDIR)
	@echo "[run] crossbar SHM parity throughput sizes: $(SIZES_SMALL)"
	@for sz in $(SIZES_SMALL); do \
		base="crossbar_$${sz}_$$RANDOM_$$PPID"; \
		$(TIMEOUT_CMD) $(CROSSBAR_BIN) --mode server --base $$base --message-size $$sz >/dev/null 2>&1 & \
		server_pid=$$!; \
		sleep 0.2; \
		$(TIMEOUT_CMD) $(CROSSBAR_BIN) --mode client --base $$base --message-size $$sz --num-messages $(NUM_MESSAGES) --warmup $(WARMUP) --json > $(OUTDIR)/crossbar_$${sz}.json; \
		status=$$?; \
		kill $$server_pid 2>/dev/null || true; \
		wait $$server_pid 2>/dev/null || true; \
		if [ $$status -ne 0 ]; then exit $$status; fi; \
		echo "  ✓ crossbar $$sz B"; \
	done

crossbar-run-fixed-rate-quick: crossbar-build
	@mkdir -p $(OUTDIR)
	@echo "[run] crossbar SHM parity fixed-rate sizes: $(SIZES_SMALL), rates: $(RATES)"
	@for sz in $(SIZES_SMALL); do \
		for rate in $(RATES); do \
			base="crossbar_$${sz}_$${rate}_$$RANDOM_$$PPID"; \
			$(TIMEOUT_CMD) $(CROSSBAR_BIN) --mode server --base $$base --message-size $$sz >/dev/null 2>&1 & \
			server_pid=$$!; \
			sleep 0.2; \
			$(TIMEOUT_CMD) $(CROSSBAR_BIN) --mode client --base $$base --message-size $$sz --num-messages $(NUM_MESSAGES) --warmup $(WARMUP) --target-rate $$rate --json > $(OUTDIR)/crossbar_$${sz}_$${rate}.json; \
			status=$$?; \
			kill $$server_pid 2>/dev/null || true; \
			wait $$server_pid 2>/dev/null || true; \
			if [ $$status -ne 0 ]; then exit $$status; fi; \
			echo "  ✓ crossbar $$sz B @ $$rate/s"; \
		done; \
	done

shmipc-build:
	@cargo build -p competitive-bench --profile competitive --manifest-path $(WORKSPACE_MANIFEST) --bin shmipc_pingpong
shmipc-run-quick: shmipc-build
	@mkdir -p $(OUTDIR)
	@echo "[run] shmipc-rs SHM parity throughput sizes: $(SIZES_SMALL)"
	@for sz in $(SIZES_SMALL); do \
		base="shmipc_$${sz}_$$RANDOM_$$PPID"; \
		$(TIMEOUT_CMD) $(SHMIPC_BIN) --mode server --base $$base --message-size $$sz >/dev/null 2>&1 & \
		server_pid=$$!; \
		sleep 0.2; \
		$(TIMEOUT_CMD) $(SHMIPC_BIN) --mode client --no-embedded-server --base $$base --message-size $$sz --num-messages $(NUM_MESSAGES) --warmup $(WARMUP) --json > $(OUTDIR)/shmipc_$${sz}.json; \
		status=$$?; \
		kill $$server_pid 2>/dev/null || true; \
		wait $$server_pid 2>/dev/null || true; \
		if [ $$status -ne 0 ]; then exit $$status; fi; \
		echo "  ✓ shmipc $$sz B"; \
	done
shmipc-run-fixed-rate-quick: shmipc-build
	@mkdir -p $(OUTDIR)
	@echo "[run] shmipc-rs SHM parity fixed-rate sizes: $(SIZES_SMALL), rates: $(RATES)"
	@for sz in $(SIZES_SMALL); do \
		for rate in $(RATES); do \
			base="shmipc_$${sz}_$${rate}_$$RANDOM_$$PPID"; \
			$(TIMEOUT_CMD) $(SHMIPC_BIN) --mode server --base $$base --message-size $$sz >/dev/null 2>&1 & \
			server_pid=$$!; \
			sleep 0.2; \
			$(TIMEOUT_CMD) $(SHMIPC_BIN) --mode client --no-embedded-server --base $$base --message-size $$sz --num-messages $(NUM_MESSAGES) --warmup $(WARMUP) --target-rate $$rate --json > $(OUTDIR)/shmipc_$${sz}_$${rate}.json; \
			status=$$?; \
			kill $$server_pid 2>/dev/null || true; \
			wait $$server_pid 2>/dev/null || true; \
			if [ $$status -ne 0 ]; then exit $$status; fi; \
			echo "  ✓ shmipc $$sz B @ $$rate/s"; \
		done; \
	done

boost-build:
	@$(MAKE) -C $(BOOST_DIR)
boost-run-quick: boost-build
	@mkdir -p $(OUTDIR)
	@echo "[run] boost::interprocess parity throughput sizes: $(SIZES_SMALL)"
	@for sz in $(SIZES_SMALL); do \
		base="boost_$${sz}_$$RANDOM_$$PPID"; \
		$(TIMEOUT_CMD) $(BOOST_BIN) --mode server --base $$base --message-size $$sz >/dev/null 2>&1 & \
		server_pid=$$!; \
		sleep 0.2; \
		$(TIMEOUT_CMD) $(BOOST_BIN) --mode client --base $$base --message-size $$sz --num-messages $(NUM_MESSAGES) --warmup $(WARMUP) --json > $(OUTDIR)/boost_$${sz}.json; \
		status=$$?; \
		kill $$server_pid 2>/dev/null || true; \
		wait $$server_pid 2>/dev/null || true; \
		if [ $$status -ne 0 ]; then exit $$status; fi; \
		echo "  ✓ boost $$sz B"; \
	done
boost-run-fixed-rate-quick: boost-build
	@mkdir -p $(OUTDIR)
	@echo "[run] boost::interprocess parity fixed-rate sizes: $(SIZES_SMALL), rates: $(RATES)"
	@for sz in $(SIZES_SMALL); do \
		for rate in $(RATES); do \
			base="boost_$${sz}_$${rate}_$$RANDOM_$$PPID"; \
			$(TIMEOUT_CMD) $(BOOST_BIN) --mode server --base $$base --message-size $$sz >/dev/null 2>&1 & \
			server_pid=$$!; \
			sleep 0.2; \
			$(TIMEOUT_CMD) $(BOOST_BIN) --mode client --base $$base --message-size $$sz --num-messages $(NUM_MESSAGES) --warmup $(WARMUP) --target-rate $$rate --json > $(OUTDIR)/boost_$${sz}_$${rate}.json; \
			status=$$?; \
			kill $$server_pid 2>/dev/null || true; \
			wait $$server_pid 2>/dev/null || true; \
			if [ $$status -ne 0 ]; then exit $$status; fi; \
			echo "  ✓ boost $$sz B @ $$rate/s"; \
		done; \
	done

ompi-build:
	@$(MAKE) -C $(OMPI_DIR) clean >/dev/null 2>&1 || true
	@$(MAKE) -C $(OMPI_DIR) MPICC="$(OMPI_MPICC)"
ompi-run-quick: ompi-build
	@mkdir -p $(OUTDIR)
	@echo "[run] Open MPI parity throughput sizes: $(SIZES_SMALL)"
	@for sz in $(SIZES_SMALL); do \
		( cd $(OMPI_DIR) && $(OMPI_TIMEOUT_CMD) $(OMPI_MPIRUN) --oversubscribe -n 2 --mca pml ob1 --mca btl vader,self ./pingpong --message-size $$sz --num-messages $(NUM_MESSAGES) --warmup $(WARMUP) --json ) > $(OUTDIR)/ompi_$${sz}.json && echo "  ✓ ompi $$sz B" || exit 1; \
	done
ompi-run-fixed-rate-quick: ompi-build
	@mkdir -p $(OUTDIR)
	@echo "[run] Open MPI parity fixed-rate sizes: $(SIZES_SMALL), rates: $(RATES)"
	@for sz in $(SIZES_SMALL); do \
		for rate in $(RATES); do \
			( cd $(OMPI_DIR) && $(OMPI_TIMEOUT_CMD) $(OMPI_MPIRUN) --oversubscribe -n 2 --mca pml ob1 --mca btl vader,self ./pingpong --message-size $$sz --num-messages $(NUM_MESSAGES) --warmup $(WARMUP) --target-rate $$rate --json ) > $(OUTDIR)/ompi_$${sz}_$${rate}.json && echo "  ✓ ompi $$sz B @ $$rate/s" || exit 1; \
		done; \
	done

rusteron-build:
	@cargo build -p competitive-bench --profile competitive --manifest-path $(WORKSPACE_MANIFEST) --bin rusteron_pingpong
rusteron-run-quick: rusteron-build
	@mkdir -p $(OUTDIR)
	@echo "[run] rusteron IPC parity throughput sizes: $(SIZES_SMALL)"
	@for sz in $(SIZES_SMALL); do \
		base="rusteron_$${sz}_$$RANDOM_$$PPID"; \
		aeron_libdirs=$$(find $(abspath ../..)/target/competitive/build -type f \( -name 'libaeron.so' -o -name 'libaeron_driver.so' \) -printf '%h:' | sed 's/:$$//'); \
		LD_LIBRARY_PATH="$$aeron_libdirs:$$LD_LIBRARY_PATH" $(TIMEOUT_CMD) $(RUSTERON_BIN) --mode server --base $$base --message-size $$sz >/dev/null 2>&1 & \
		server_pid=$$!; \
		sleep 0.5; \
		LD_LIBRARY_PATH="$$aeron_libdirs:$$LD_LIBRARY_PATH" $(TIMEOUT_CMD) $(RUSTERON_BIN) --mode client --base $$base --message-size $$sz --num-messages $(NUM_MESSAGES) --warmup $(WARMUP) --json > $(OUTDIR)/rusteron_$${sz}.json; \
		status=$$?; \
		kill $$server_pid 2>/dev/null || true; \
		wait $$server_pid 2>/dev/null || true; \
		rm -rf /dev/shm/$$base 2>/dev/null || true; \
		if [ $$status -ne 0 ]; then exit $$status; fi; \
		echo "  ✓ rusteron $$sz B"; \
	done
rusteron-run-fixed-rate-quick: rusteron-build
	@mkdir -p $(OUTDIR)
	@echo "[run] rusteron IPC parity fixed-rate sizes: $(SIZES_SMALL), rates: $(RATES)"
	@for sz in $(SIZES_SMALL); do \
		for rate in $(RATES); do \
			base="rusteron_$${sz}_$${rate}_$$RANDOM_$$PPID"; \
			aeron_libdirs=$$(find $(abspath ../..)/target/competitive/build -type f \( -name 'libaeron.so' -o -name 'libaeron_driver.so' \) -printf '%h:' | sed 's/:$$//'); \
			LD_LIBRARY_PATH="$$aeron_libdirs:$$LD_LIBRARY_PATH" $(TIMEOUT_CMD) $(RUSTERON_BIN) --mode server --base $$base --message-size $$sz >/dev/null 2>&1 & \
			server_pid=$$!; \
			sleep 0.5; \
			LD_LIBRARY_PATH="$$aeron_libdirs:$$LD_LIBRARY_PATH" $(TIMEOUT_CMD) $(RUSTERON_BIN) --mode client --base $$base --message-size $$sz --num-messages $(NUM_MESSAGES) --warmup $(WARMUP) --target-rate $$rate --json > $(OUTDIR)/rusteron_$${sz}_$${rate}.json; \
			status=$$?; \
			kill $$server_pid 2>/dev/null || true; \
			wait $$server_pid 2>/dev/null || true; \
			rm -rf /dev/shm/$$base 2>/dev/null || true; \
			if [ $$status -ne 0 ]; then exit $$status; fi; \
			echo "  ✓ rusteron $$sz B @ $$rate/s"; \
		done; \
	done

zmq-build:
	@cargo build -p competitive-bench --profile competitive --manifest-path $(WORKSPACE_MANIFEST) --bin zmq_pingpong
zmq-run-quick: zmq-ipc-run-quick
zmq-run-fixed-rate-quick: zmq-ipc-run-fixed-rate-quick
zmq-ipc-run-quick: zmq-build
	@mkdir -p $(OUTDIR)
	@echo "[run] ZeroMQ ipc parity throughput sizes: $(SIZES_SMALL)"
	@for sz in $(SIZES_SMALL); do \
		base="zmq_$${sz}_$$RANDOM_$$PPID"; \
		$(TIMEOUT_CMD) $(ZMQ_BIN) --mode server --base $$base --message-size $$sz --transport ipc >/dev/null 2>&1 & \
		server_pid=$$!; \
		sleep 0.2; \
		$(TIMEOUT_CMD) $(ZMQ_BIN) --mode client --base $$base --message-size $$sz --transport ipc --num-messages $(NUM_MESSAGES) --warmup $(WARMUP) --json > $(OUTDIR)/zmq_$${sz}.json; \
		status=$$?; \
		kill $$server_pid 2>/dev/null || true; \
		wait $$server_pid 2>/dev/null || true; \
		rm -f /dev/shm/$$base.zmq 2>/dev/null || true; \
		if [ $$status -ne 0 ]; then exit $$status; fi; \
		echo "  ✓ zmq(ipc) $$sz B"; \
	done
zmq-ipc-run-fixed-rate-quick: zmq-build
	@mkdir -p $(OUTDIR)
	@echo "[run] ZeroMQ ipc parity fixed-rate sizes: $(SIZES_SMALL), rates: $(RATES)"
	@for sz in $(SIZES_SMALL); do \
		for rate in $(RATES); do \
			base="zmq_$${sz}_$${rate}_$$RANDOM_$$PPID"; \
			$(TIMEOUT_CMD) $(ZMQ_BIN) --mode server --base $$base --message-size $$sz --transport ipc >/dev/null 2>&1 & \
			server_pid=$$!; \
			sleep 0.2; \
			$(TIMEOUT_CMD) $(ZMQ_BIN) --mode client --base $$base --message-size $$sz --transport ipc --num-messages $(NUM_MESSAGES) --warmup $(WARMUP) --target-rate $$rate --json > $(OUTDIR)/zmq_$${sz}_$${rate}.json; \
			status=$$?; \
			kill $$server_pid 2>/dev/null || true; \
			wait $$server_pid 2>/dev/null || true; \
			rm -f /dev/shm/$$base.zmq 2>/dev/null || true; \
			if [ $$status -ne 0 ]; then exit $$status; fi; \
			echo "  ✓ zmq(ipc) $$sz B @ $$rate/s"; \
		done; \
	done
zmq-ipc-abs-run-quick: zmq-build
	@mkdir -p $(OUTDIR)
	@echo "[run] ZeroMQ ipc-abs parity throughput sizes: $(SIZES_SMALL)"
	@for sz in $(SIZES_SMALL); do \
		base="zmqabs_$${sz}_$$RANDOM_$$PPID"; \
		$(TIMEOUT_CMD) $(ZMQ_BIN) --mode server --base $$base --message-size $$sz --transport ipc-abs >/dev/null 2>&1 & \
		server_pid=$$!; \
		sleep 0.2; \
		$(TIMEOUT_CMD) $(ZMQ_BIN) --mode client --base $$base --message-size $$sz --transport ipc-abs --num-messages $(NUM_MESSAGES) --warmup $(WARMUP) --json > $(OUTDIR)/zmqabs_$${sz}.json; \
		status=$$?; \
		kill $$server_pid 2>/dev/null || true; \
		wait $$server_pid 2>/dev/null || true; \
		if [ $$status -ne 0 ]; then exit $$status; fi; \
		echo "  ✓ zmq(ipc-abs) $$sz B"; \
	done
zmq-ipc-abs-run-fixed-rate-quick: zmq-build
	@mkdir -p $(OUTDIR)
	@echo "[run] ZeroMQ ipc-abs parity fixed-rate sizes: $(SIZES_SMALL), rates: $(RATES)"
	@for sz in $(SIZES_SMALL); do \
		for rate in $(RATES); do \
			base="zmqabs_$${sz}_$${rate}_$$RANDOM_$$PPID"; \
			$(TIMEOUT_CMD) $(ZMQ_BIN) --mode server --base $$base --message-size $$sz --transport ipc-abs >/dev/null 2>&1 & \
			server_pid=$$!; \
			sleep 0.2; \
			$(TIMEOUT_CMD) $(ZMQ_BIN) --mode client --base $$base --message-size $$sz --transport ipc-abs --num-messages $(NUM_MESSAGES) --warmup $(WARMUP) --target-rate $$rate --json > $(OUTDIR)/zmqabs_$${sz}_$${rate}.json; \
			status=$$?; \
			kill $$server_pid 2>/dev/null || true; \
			wait $$server_pid 2>/dev/null || true; \
			if [ $$status -ne 0 ]; then exit $$status; fi; \
			echo "  ✓ zmq(ipc-abs) $$sz B @ $$rate/s"; \
		done; \
	done
zmq-tcp-run-quick: zmq-build
	@mkdir -p $(OUTDIR)
	@echo "[run] ZeroMQ tcp parity throughput sizes: $(SIZES_SMALL)"
	@for sz in $(SIZES_SMALL); do \
		base="zmqtcp_$${sz}_$$RANDOM_$$PPID"; \
		$(TIMEOUT_CMD) $(ZMQ_BIN) --mode server --base $$base --message-size $$sz --transport tcp >/dev/null 2>&1 & \
		server_pid=$$!; \
		sleep 0.2; \
		$(TIMEOUT_CMD) $(ZMQ_BIN) --mode client --base $$base --message-size $$sz --transport tcp --num-messages $(NUM_MESSAGES) --warmup $(WARMUP) --json > $(OUTDIR)/zmqtcp_$${sz}.json; \
		status=$$?; \
		kill $$server_pid 2>/dev/null || true; \
		wait $$server_pid 2>/dev/null || true; \
		if [ $$status -ne 0 ]; then exit $$status; fi; \
		echo "  ✓ zmq(tcp) $$sz B"; \
	done
zmq-tcp-run-fixed-rate-quick: zmq-build
	@mkdir -p $(OUTDIR)
	@echo "[run] ZeroMQ tcp parity fixed-rate sizes: $(SIZES_SMALL), rates: $(RATES)"
	@for sz in $(SIZES_SMALL); do \
		for rate in $(RATES); do \
			base="zmqtcp_$${sz}_$${rate}_$$RANDOM_$$PPID"; \
			$(TIMEOUT_CMD) $(ZMQ_BIN) --mode server --base $$base --message-size $$sz --transport tcp >/dev/null 2>&1 & \
			server_pid=$$!; \
			sleep 0.2; \
			$(TIMEOUT_CMD) $(ZMQ_BIN) --mode client --base $$base --message-size $$sz --transport tcp --num-messages $(NUM_MESSAGES) --warmup $(WARMUP) --target-rate $$rate --json > $(OUTDIR)/zmqtcp_$${sz}_$${rate}.json; \
			status=$$?; \
			kill $$server_pid 2>/dev/null || true; \
			wait $$server_pid 2>/dev/null || true; \
			if [ $$status -ne 0 ]; then exit $$status; fi; \
			echo "  ✓ zmq(tcp) $$sz B @ $$rate/s"; \
		done; \
	done
