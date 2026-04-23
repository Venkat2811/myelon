SHELL := /bin/bash

include config/parity.mk

WORKSPACE_MANIFEST := $(abspath ../..)/Cargo.toml
OUTDIR ?= $(CURDIR)/output/results
HEADON_DIR ?= $(CURDIR)/output/headon
BOOST_DIR := $(CURDIR)/third_party/boost_pingpong
OMPI_DIR := $(CURDIR)/third_party/ompi_pingpong
DOCKER ?= docker
CROSSBAR_BIN := $(abspath ../..)/target/competitive/crossbar_pingpong
INTERNAL_BROADCAST_BIN := $(abspath ../..)/target/competitive/internal_broadcast
CROSSBAR_BROADCAST_BIN := $(abspath ../..)/target/competitive/crossbar_broadcast
SHMIPC_BIN := $(abspath ../..)/target/competitive/shmipc_pingpong
RUSTERON_BIN := $(abspath ../..)/target/competitive/rusteron_pingpong
ZMQ_BIN := $(abspath ../..)/target/competitive/zmq_pingpong
ICEORYX2_BIN := $(abspath ../..)/target/competitive/iceoryx2_pingpong
IGGY_BIN := $(abspath ../..)/target/competitive/iggy_pingpong
REDPANDA_BIN := $(abspath ../..)/target/competitive/redpanda_pingpong
BOOST_BIN := $(BOOST_DIR)/boost_pingpong
OMPI_BIN := $(OMPI_DIR)/pingpong
OMPI_MPICC ?= mpicc
OMPI_MPIRUN ?= mpirun
RUSTERON_MAX_MESSAGE_SIZE ?= 16777216
BROKER_RUN_SIZES ?= $(BROKER_SIZES)
IGGY_IMAGE ?= apache/iggy:latest
IGGY_CONTAINER ?= competitive-bench-iggy
IGGY_TCP_PORT ?= 8090
REDPANDA_IMAGE ?= docker.redpanda.com/redpandadata/redpanda:v26.1.5
REDPANDA_CONTAINER ?= competitive-bench-redpanda
REDPANDA_KAFKA_PORT ?= 19092

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

RUN_SIZES ?= $(SIZES_SMALL)
EXTENSIVE_RUN_SIZES ?= $(SIZES_EXTENSIVE)

SIZE_TUNING = if [ $$sz -ge $(GIANT_SIZE_THRESHOLD) ]; then msgs=$(GIANT_NUM_MESSAGES); warmup=$(GIANT_WARMUP); rates="$(GIANT_RATES)"; headon_smoke_rate=$(GIANT_HEADON_RATE_SMOKE); stack_kb=524288; \
elif [ $$sz -ge $(MASSIVE_SIZE_THRESHOLD) ]; then msgs=$(MASSIVE_NUM_MESSAGES); warmup=$(MASSIVE_WARMUP); rates="$(MASSIVE_RATES)"; headon_smoke_rate=$(MASSIVE_HEADON_RATE_SMOKE); stack_kb=262144; \
elif [ $$sz -ge $(HUGE_SIZE_THRESHOLD) ]; then msgs=$(HUGE_NUM_MESSAGES); warmup=$(HUGE_WARMUP); rates="$(HUGE_RATES)"; headon_smoke_rate=$(HUGE_HEADON_RATE_SMOKE); stack_kb=131072; \
elif [ $$sz -ge $(LARGE_SIZE_THRESHOLD) ]; then msgs=$(LARGE_NUM_MESSAGES); warmup=$(LARGE_WARMUP); rates="$(LARGE_RATES)"; headon_smoke_rate=$(LARGE_HEADON_RATE_SMOKE); stack_kb=65536; \
else msgs=$(NUM_MESSAGES); warmup=$(WARMUP); rates="$(RATES)"; headon_smoke_rate=$(HEADON_RATE_SMOKE); stack_kb=16384; fi

.PHONY: help clean build-all internal-build internal-run-quick internal-run-fixed-rate-quick \
	internal-run-extensive-quick internal-run-extensive-fixed-rate-quick \
	broadcast-build broadcast-run-quick broadcast-run-fixed-rate-quick \
	internal-broadcast-extensive-quick internal-broadcast-extensive-fixed-rate-quick \
	crossbar-broadcast-extensive-run-quick crossbar-broadcast-extensive-run-fixed-rate-quick \
	broadcast-run-extensive-quick broadcast-run-extensive-fixed-rate-quick \
	aggregate graphs cleanup-shm headon headon-smoke headon-full headon-extensive internal-headon verify-align verify-align-quick verify-align-extensive \
	simple-smoke simple-smoke-signal \
	kill-pingpong run-all-quick run-all-fixed-rate-quick ubermensh ubermensh-smoke \
	run-all-extensive-quick run-all-extensive-fixed-rate-quick ubermensh-extensive \
	ubermensh-smoke-no-ompi ubermensh-smoke-no-timeout external-status \
	broker-build iggy-up iggy-down iggy-run-quick iggy-run-fixed-rate-quick \
	redpanda-up redpanda-down redpanda-run-quick redpanda-run-fixed-rate-quick \
	iceoryx2-build iceoryx2-run-quick iceoryx2-run-fixed-rate-quick \
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
	@echo "make internal-run-extensive-quick   # extensive-only internal throughput ladder"
	@echo "make internal-run-extensive-fixed-rate-quick # extensive-only internal fixed-rate ladder"
	@echo "make broadcast-run-quick            # raw internal + crossbar broadcast throughput (4c/8c)"
	@echo "make broadcast-run-fixed-rate-quick # raw internal + crossbar broadcast fixed-rate CO (4c/8c)"
	@echo "make broadcast-run-extensive-quick  # broadcast throughput plus extensive sizes"
	@echo "make broadcast-run-extensive-fixed-rate-quick # broadcast fixed-rate plus extensive sizes"
	@echo "make aggregate                      # pretty aggregate tables + Pareto graphs from $(OUTDIR)"
	@echo "make cleanup-shm                    # remove competitive-bench-owned artifacts from /dev/shm"
	@echo "make simple-smoke                  # ultra-quick exact-size lane (32B..4KB) + signal + aggregate"
	@echo "make internal-headon                # raw disruptor vs raw myelon SHM head-on summary"
	@echo "make headon-smoke                   # raw disruptor vs rusteron smoke head-on"
	@echo "make headon-full                    # raw disruptor vs rusteron full head-on"
	@echo "make headon-extensive               # raw disruptor vs rusteron extensive head-on"
	@echo "make run-all-quick                  # parity target surface (internal SHM+mmap + wired peers + zmq tcp variants)"
	@echo "make run-all-fixed-rate-quick       # parity fixed-rate target surface (internal SHM+mmap + wired peers + zmq tcp variants)"
	@echo "make run-all-extensive-quick        # ping-pong + broadcast throughput plus extensive sizes"
	@echo "make run-all-extensive-fixed-rate-quick # ping-pong + broadcast fixed-rate plus extensive sizes"
	@echo "make ubermensh                      # aggregate whatever parity artifacts are present"
	@echo "make ubermensh-extensive            # full extensive parity run + graphs"
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
	@echo "make iceoryx2-run-quick             # iceoryx2 SHM throughput parity grid"
	@echo "make iceoryx2-run-fixed-rate-quick  # iceoryx2 SHM fixed-rate CO parity grid"
	@echo "make zmq-run-quick                  # zeromq default(ipc) throughput parity grid"
	@echo "make zmq-run-fixed-rate-quick       # zeromq default(ipc) fixed-rate CO parity grid"
	@echo "make iggy-run-quick                 # Apache Iggy TCP broker quick grid (64B, 1KB)"
	@echo "make iggy-run-fixed-rate-quick      # Apache Iggy TCP broker fixed-rate quick grid (@ $(BROKER_RATES)/s)"
	@echo "make redpanda-run-quick             # Redpanda Kafka/TCP broker quick grid (64B, 1KB)"
	@echo "make redpanda-run-fixed-rate-quick  # Redpanda Kafka/TCP broker fixed-rate quick grid (@ $(BROKER_RATES)/s)"
	@echo "make verify-align                   # raw disruptor + rusteron alignment verification"
	@echo "make kill-pingpong                  # clean leftover competitive peer processes"

clean:
	rm -rf $(OUTDIR) $(HEADON_DIR)

build-all: internal-build crossbar-build shmipc-build boost-build ompi-build rusteron-build iceoryx2-build zmq-build broker-build external-status

internal-build:
	@cargo test -p competitive-bench --manifest-path $(WORKSPACE_MANIFEST) --lib
	@cargo build -p competitive-bench --profile competitive --manifest-path $(WORKSPACE_MANIFEST) --bin internal_broadcast

internal-run-quick:
	@mkdir -p $(OUTDIR)
	@echo "[run] internal SHM+mmap parity throughput sizes: $(RUN_SIZES)"
	@for sz in $(RUN_SIZES); do \
		$(SIZE_TUNING); \
		ulimit -s $$stack_kb >/dev/null 2>&1 || true; $(TIMEOUT_CMD) cargo bench -p perf-bench --profile competitive --manifest-path $(WORKSPACE_MANIFEST) --bench competitive_shm -- --json-canonical --no-compare -s $$sz -n $$msgs -w $$warmup > $(OUTDIR)/disruptor_$${sz}.json && echo "  ✓ disruptor $$sz B" || exit 1; \
		ulimit -s $$stack_kb >/dev/null 2>&1 || true; $(TIMEOUT_CMD) cargo bench -p perf-bench --profile competitive --manifest-path $(WORKSPACE_MANIFEST) --bench competitive_mmap -- --json-canonical --no-compare -s $$sz -n $$msgs -w $$warmup > $(OUTDIR)/disruptor_mmap_$${sz}.json && echo "  ✓ disruptor_mmap $$sz B" || exit 1; \
		ulimit -s $$stack_kb >/dev/null 2>&1 || true; $(TIMEOUT_CMD) cargo bench -p perf-bench --profile competitive --manifest-path $(WORKSPACE_MANIFEST) --bench competitive_raw_myelon_shm -- --json --no-compare -s $$sz -n $$msgs -w $$warmup > $(OUTDIR)/myelon_raw_$${sz}.json && echo "  ✓ myelon_raw $$sz B" || exit 1; \
		ulimit -s $$stack_kb >/dev/null 2>&1 || true; $(TIMEOUT_CMD) cargo bench -p perf-bench --profile competitive --manifest-path $(WORKSPACE_MANIFEST) --bench competitive_raw_myelon_mmap -- --json --no-compare -s $$sz -n $$msgs -w $$warmup > $(OUTDIR)/myelon_raw_mmap_$${sz}.json && echo "  ✓ myelon_raw_mmap $$sz B" || exit 1; \
	done

internal-run-fixed-rate-quick:
	@mkdir -p $(OUTDIR)
	@echo "[run] internal SHM+mmap parity fixed-rate sizes: $(RUN_SIZES), core rates: $(RATES), large rates: $(LARGE_RATES)"
	@for sz in $(RUN_SIZES); do \
		$(SIZE_TUNING); \
		for rate in $$rates; do \
			ulimit -s $$stack_kb >/dev/null 2>&1 || true; $(TIMEOUT_CMD) cargo bench -p perf-bench --profile competitive --manifest-path $(WORKSPACE_MANIFEST) --bench competitive_shm -- --json-canonical --no-compare -s $$sz -n $$msgs -w $$warmup --target-rate $$rate > $(OUTDIR)/disruptor_$${sz}_$${rate}.json && echo "  ✓ disruptor $$sz B @ $$rate/s" || exit 1; \
			ulimit -s $$stack_kb >/dev/null 2>&1 || true; $(TIMEOUT_CMD) cargo bench -p perf-bench --profile competitive --manifest-path $(WORKSPACE_MANIFEST) --bench competitive_mmap -- --json-canonical --no-compare -s $$sz -n $$msgs -w $$warmup --target-rate $$rate > $(OUTDIR)/disruptor_mmap_$${sz}_$${rate}.json && echo "  ✓ disruptor_mmap $$sz B @ $$rate/s" || exit 1; \
			ulimit -s $$stack_kb >/dev/null 2>&1 || true; $(TIMEOUT_CMD) cargo bench -p perf-bench --profile competitive --manifest-path $(WORKSPACE_MANIFEST) --bench competitive_raw_myelon_shm -- --json --no-compare -s $$sz -n $$msgs -w $$warmup --target-rate $$rate > $(OUTDIR)/myelon_raw_$${sz}_$${rate}.json && echo "  ✓ myelon_raw $$sz B @ $$rate/s" || exit 1; \
			ulimit -s $$stack_kb >/dev/null 2>&1 || true; $(TIMEOUT_CMD) cargo bench -p perf-bench --profile competitive --manifest-path $(WORKSPACE_MANIFEST) --bench competitive_raw_myelon_mmap -- --json --no-compare -s $$sz -n $$msgs -w $$warmup --target-rate $$rate > $(OUTDIR)/myelon_raw_mmap_$${sz}_$${rate}.json && echo "  ✓ myelon_raw_mmap $$sz B @ $$rate/s" || exit 1; \
		done; \
	done

internal-run-extensive-quick:
	@$(MAKE) RUN_SIZES="$(EXTENSIVE_RUN_SIZES)" internal-run-quick

internal-run-extensive-fixed-rate-quick:
	@$(MAKE) RUN_SIZES="$(EXTENSIVE_RUN_SIZES)" internal-run-fixed-rate-quick

broadcast-build:
	@RUST_MIN_STACK=67108864 cargo build -p competitive-bench --profile competitive --manifest-path $(WORKSPACE_MANIFEST) --bin internal_broadcast --bin crossbar_broadcast

cleanup-shm:
	@python3 scripts/cleanup_shm.py

broadcast-run-quick: broadcast-build
	@mkdir -p $(OUTDIR)
	@echo "[run] broadcast throughput sizes: $(SIZES_SMALL), consumers: $(BROADCAST_CONSUMERS)"
	@for consumers in $(BROADCAST_CONSUMERS); do \
		for sz in $(SIZES_SMALL); do \
			$(SIZE_TUNING); \
			base="broadcast_$$consumers""c_$${sz}_$$RANDOM_$$PPID"; \
			ulimit -s $$stack_kb >/dev/null 2>&1 || true; $(TIMEOUT_CMD) $(INTERNAL_BROADCAST_BIN) --mode controller --adapter disruptor-shm --base $$base --message-size $$sz --consumers $$consumers --num-messages $$msgs --warmup $$warmup --json > $(OUTDIR)/broadcast_disruptor_$${sz}_$${consumers}c.json && echo "  ✓ disruptor-shm broadcast $$sz B $$consumers c" || exit 1; \
			base="broadcast_mmap_$$consumers""c_$${sz}_$$RANDOM_$$PPID"; \
			ulimit -s $$stack_kb >/dev/null 2>&1 || true; $(TIMEOUT_CMD) $(INTERNAL_BROADCAST_BIN) --mode controller --adapter disruptor-mmap --base $$base --message-size $$sz --consumers $$consumers --num-messages $$msgs --warmup $$warmup --json > $(OUTDIR)/broadcast_disruptor_mmap_$${sz}_$${consumers}c.json && echo "  ✓ disruptor-mmap broadcast $$sz B $$consumers c" || exit 1; \
			base="broadcast_myelon_$$consumers""c_$${sz}_$$RANDOM_$$PPID"; \
			ulimit -s $$stack_kb >/dev/null 2>&1 || true; $(TIMEOUT_CMD) $(INTERNAL_BROADCAST_BIN) --mode controller --adapter myelon-raw-shm --base $$base --message-size $$sz --consumers $$consumers --num-messages $$msgs --warmup $$warmup --json > $(OUTDIR)/broadcast_myelon_raw_$${sz}_$${consumers}c.json && echo "  ✓ myelon-raw-shm broadcast $$sz B $$consumers c" || exit 1; \
			base="broadcast_myelon_mmap_$$consumers""c_$${sz}_$$RANDOM_$$PPID"; \
			ulimit -s $$stack_kb >/dev/null 2>&1 || true; $(TIMEOUT_CMD) $(INTERNAL_BROADCAST_BIN) --mode controller --adapter myelon-raw-mmap --base $$base --message-size $$sz --consumers $$consumers --num-messages $$msgs --warmup $$warmup --json > $(OUTDIR)/broadcast_myelon_raw_mmap_$${sz}_$${consumers}c.json && echo "  ✓ myelon-raw-mmap broadcast $$sz B $$consumers c" || exit 1; \
			base="broadcast_crossbar_$$consumers""c_$${sz}_$$RANDOM_$$PPID"; \
			ulimit -s $$stack_kb >/dev/null 2>&1 || true; $(TIMEOUT_CMD) $(CROSSBAR_BROADCAST_BIN) --mode controller --base $$base --message-size $$sz --consumers $$consumers --num-messages $$msgs --warmup $$warmup --json > $(OUTDIR)/broadcast_crossbar_$${sz}_$${consumers}c.json && echo "  ✓ crossbar-pubsub broadcast $$sz B $$consumers c" || exit 1; \
		done; \
	done

broadcast-run-fixed-rate-quick: broadcast-build
	@mkdir -p $(OUTDIR)
	@echo "[run] broadcast fixed-rate sizes: $(SIZES_SMALL), consumers: $(BROADCAST_CONSUMERS)"
	@for consumers in $(BROADCAST_CONSUMERS); do \
		for sz in $(SIZES_SMALL); do \
			$(SIZE_TUNING); \
			for rate in $$rates; do \
				base="broadcast_$$consumers""c_$${sz}_$${rate}_$$RANDOM_$$PPID"; \
				ulimit -s $$stack_kb >/dev/null 2>&1 || true; $(TIMEOUT_CMD) $(INTERNAL_BROADCAST_BIN) --mode controller --adapter disruptor-shm --base $$base --message-size $$sz --consumers $$consumers --num-messages $$msgs --warmup $$warmup --target-rate $$rate --json > $(OUTDIR)/broadcast_disruptor_$${sz}_$${consumers}c_$${rate}.json && echo "  ✓ disruptor-shm broadcast $$sz B $$consumers c @ $$rate/s" || exit 1; \
				base="broadcast_mmap_$$consumers""c_$${sz}_$${rate}_$$RANDOM_$$PPID"; \
				ulimit -s $$stack_kb >/dev/null 2>&1 || true; $(TIMEOUT_CMD) $(INTERNAL_BROADCAST_BIN) --mode controller --adapter disruptor-mmap --base $$base --message-size $$sz --consumers $$consumers --num-messages $$msgs --warmup $$warmup --target-rate $$rate --json > $(OUTDIR)/broadcast_disruptor_mmap_$${sz}_$${consumers}c_$${rate}.json && echo "  ✓ disruptor-mmap broadcast $$sz B $$consumers c @ $$rate/s" || exit 1; \
				base="broadcast_myelon_$$consumers""c_$${sz}_$${rate}_$$RANDOM_$$PPID"; \
				ulimit -s $$stack_kb >/dev/null 2>&1 || true; $(TIMEOUT_CMD) $(INTERNAL_BROADCAST_BIN) --mode controller --adapter myelon-raw-shm --base $$base --message-size $$sz --consumers $$consumers --num-messages $$msgs --warmup $$warmup --target-rate $$rate --json > $(OUTDIR)/broadcast_myelon_raw_$${sz}_$${consumers}c_$${rate}.json && echo "  ✓ myelon-raw-shm broadcast $$sz B $$consumers c @ $$rate/s" || exit 1; \
				base="broadcast_myelon_mmap_$$consumers""c_$${sz}_$${rate}_$$RANDOM_$$PPID"; \
				ulimit -s $$stack_kb >/dev/null 2>&1 || true; $(TIMEOUT_CMD) $(INTERNAL_BROADCAST_BIN) --mode controller --adapter myelon-raw-mmap --base $$base --message-size $$sz --consumers $$consumers --num-messages $$msgs --warmup $$warmup --target-rate $$rate --json > $(OUTDIR)/broadcast_myelon_raw_mmap_$${sz}_$${consumers}c_$${rate}.json && echo "  ✓ myelon-raw-mmap broadcast $$sz B $$consumers c @ $$rate/s" || exit 1; \
				base="broadcast_crossbar_$$consumers""c_$${sz}_$${rate}_$$RANDOM_$$PPID"; \
				ulimit -s $$stack_kb >/dev/null 2>&1 || true; $(TIMEOUT_CMD) $(CROSSBAR_BROADCAST_BIN) --mode controller --base $$base --message-size $$sz --consumers $$consumers --num-messages $$msgs --warmup $$warmup --target-rate $$rate --json > $(OUTDIR)/broadcast_crossbar_$${sz}_$${consumers}c_$${rate}.json && echo "  ✓ crossbar-pubsub broadcast $$sz B $$consumers c @ $$rate/s" || exit 1; \
			done; \
		done; \
	done

internal-broadcast-extensive-quick: internal-build
	@mkdir -p $(OUTDIR)
	@echo "[run] internal broadcast throughput extensive sizes: $(EXTENSIVE_RUN_SIZES), consumers: $(BROADCAST_CONSUMERS)"
	@for consumers in $(BROADCAST_CONSUMERS); do \
		for sz in $(EXTENSIVE_RUN_SIZES); do \
			$(SIZE_TUNING); \
			base="broadcast_$$consumers""c_$${sz}_$$RANDOM_$$PPID"; \
			ulimit -s $$stack_kb >/dev/null 2>&1 || true; $(TIMEOUT_CMD) $(INTERNAL_BROADCAST_BIN) --mode controller --adapter disruptor-shm --base $$base --message-size $$sz --consumers $$consumers --num-messages $$msgs --warmup $$warmup --json > $(OUTDIR)/broadcast_disruptor_$${sz}_$${consumers}c.json && echo "  ✓ disruptor-shm broadcast $$sz B $$consumers c" || exit 1; \
			base="broadcast_mmap_$$consumers""c_$${sz}_$$RANDOM_$$PPID"; \
			ulimit -s $$stack_kb >/dev/null 2>&1 || true; $(TIMEOUT_CMD) $(INTERNAL_BROADCAST_BIN) --mode controller --adapter disruptor-mmap --base $$base --message-size $$sz --consumers $$consumers --num-messages $$msgs --warmup $$warmup --json > $(OUTDIR)/broadcast_disruptor_mmap_$${sz}_$${consumers}c.json && echo "  ✓ disruptor-mmap broadcast $$sz B $$consumers c" || exit 1; \
			base="broadcast_myelon_$$consumers""c_$${sz}_$$RANDOM_$$PPID"; \
			ulimit -s $$stack_kb >/dev/null 2>&1 || true; $(TIMEOUT_CMD) $(INTERNAL_BROADCAST_BIN) --mode controller --adapter myelon-raw-shm --base $$base --message-size $$sz --consumers $$consumers --num-messages $$msgs --warmup $$warmup --json > $(OUTDIR)/broadcast_myelon_raw_$${sz}_$${consumers}c.json && echo "  ✓ myelon-raw-shm broadcast $$sz B $$consumers c" || exit 1; \
			base="broadcast_myelon_mmap_$$consumers""c_$${sz}_$$RANDOM_$$PPID"; \
			ulimit -s $$stack_kb >/dev/null 2>&1 || true; $(TIMEOUT_CMD) $(INTERNAL_BROADCAST_BIN) --mode controller --adapter myelon-raw-mmap --base $$base --message-size $$sz --consumers $$consumers --num-messages $$msgs --warmup $$warmup --json > $(OUTDIR)/broadcast_myelon_raw_mmap_$${sz}_$${consumers}c.json && echo "  ✓ myelon-raw-mmap broadcast $$sz B $$consumers c" || exit 1; \
		done; \
	done

internal-broadcast-extensive-fixed-rate-quick: internal-build
	@mkdir -p $(OUTDIR)
	@echo "[run] internal broadcast fixed-rate extensive sizes: $(EXTENSIVE_RUN_SIZES), consumers: $(BROADCAST_CONSUMERS)"
	@for consumers in $(BROADCAST_CONSUMERS); do \
		for sz in $(EXTENSIVE_RUN_SIZES); do \
			$(SIZE_TUNING); \
			for rate in $$rates; do \
				base="broadcast_$$consumers""c_$${sz}_$${rate}_$$RANDOM_$$PPID"; \
				ulimit -s $$stack_kb >/dev/null 2>&1 || true; $(TIMEOUT_CMD) $(INTERNAL_BROADCAST_BIN) --mode controller --adapter disruptor-shm --base $$base --message-size $$sz --consumers $$consumers --num-messages $$msgs --warmup $$warmup --target-rate $$rate --json > $(OUTDIR)/broadcast_disruptor_$${sz}_$${consumers}c_$${rate}.json && echo "  ✓ disruptor-shm broadcast $$sz B $$consumers c @ $$rate/s" || exit 1; \
				base="broadcast_mmap_$$consumers""c_$${sz}_$${rate}_$$RANDOM_$$PPID"; \
				ulimit -s $$stack_kb >/dev/null 2>&1 || true; $(TIMEOUT_CMD) $(INTERNAL_BROADCAST_BIN) --mode controller --adapter disruptor-mmap --base $$base --message-size $$sz --consumers $$consumers --num-messages $$msgs --warmup $$warmup --target-rate $$rate --json > $(OUTDIR)/broadcast_disruptor_mmap_$${sz}_$${consumers}c_$${rate}.json && echo "  ✓ disruptor-mmap broadcast $$sz B $$consumers c @ $$rate/s" || exit 1; \
				base="broadcast_myelon_$$consumers""c_$${sz}_$${rate}_$$RANDOM_$$PPID"; \
				ulimit -s $$stack_kb >/dev/null 2>&1 || true; $(TIMEOUT_CMD) $(INTERNAL_BROADCAST_BIN) --mode controller --adapter myelon-raw-shm --base $$base --message-size $$sz --consumers $$consumers --num-messages $$msgs --warmup $$warmup --target-rate $$rate --json > $(OUTDIR)/broadcast_myelon_raw_$${sz}_$${consumers}c_$${rate}.json && echo "  ✓ myelon-raw-shm broadcast $$sz B $$consumers c @ $$rate/s" || exit 1; \
				base="broadcast_myelon_mmap_$$consumers""c_$${sz}_$${rate}_$$RANDOM_$$PPID"; \
				ulimit -s $$stack_kb >/dev/null 2>&1 || true; $(TIMEOUT_CMD) $(INTERNAL_BROADCAST_BIN) --mode controller --adapter myelon-raw-mmap --base $$base --message-size $$sz --consumers $$consumers --num-messages $$msgs --warmup $$warmup --target-rate $$rate --json > $(OUTDIR)/broadcast_myelon_raw_mmap_$${sz}_$${consumers}c_$${rate}.json && echo "  ✓ myelon-raw-mmap broadcast $$sz B $$consumers c @ $$rate/s" || exit 1; \
			done; \
		done; \
	done

crossbar-broadcast-extensive-run-quick: broadcast-build
	@mkdir -p $(OUTDIR)
	@echo "[run] crossbar-pubsub broadcast throughput extensive sizes: $(EXTENSIVE_RUN_SIZES), consumers: $(BROADCAST_CONSUMERS)"
	@for consumers in $(BROADCAST_CONSUMERS); do \
		for sz in $(EXTENSIVE_RUN_SIZES); do \
			$(SIZE_TUNING); \
			base="broadcast_crossbar_$$consumers""c_$${sz}_$$RANDOM_$$PPID"; \
			ulimit -s $$stack_kb >/dev/null 2>&1 || true; $(TIMEOUT_CMD) $(CROSSBAR_BROADCAST_BIN) --mode controller --base $$base --message-size $$sz --consumers $$consumers --num-messages $$msgs --warmup $$warmup --json > $(OUTDIR)/broadcast_crossbar_$${sz}_$${consumers}c.json && echo "  ✓ crossbar-pubsub broadcast $$sz B $$consumers c" || exit 1; \
		done; \
	done

crossbar-broadcast-extensive-run-fixed-rate-quick: broadcast-build
	@mkdir -p $(OUTDIR)
	@echo "[run] crossbar-pubsub broadcast fixed-rate extensive sizes: $(EXTENSIVE_RUN_SIZES), consumers: $(BROADCAST_CONSUMERS)"
	@for consumers in $(BROADCAST_CONSUMERS); do \
		for sz in $(EXTENSIVE_RUN_SIZES); do \
			$(SIZE_TUNING); \
			for rate in $$rates; do \
				base="broadcast_crossbar_$$consumers""c_$${sz}_$${rate}_$$RANDOM_$$PPID"; \
				ulimit -s $$stack_kb >/dev/null 2>&1 || true; $(TIMEOUT_CMD) $(CROSSBAR_BROADCAST_BIN) --mode controller --base $$base --message-size $$sz --consumers $$consumers --num-messages $$msgs --warmup $$warmup --target-rate $$rate --json > $(OUTDIR)/broadcast_crossbar_$${sz}_$${consumers}c_$${rate}.json && echo "  ✓ crossbar-pubsub broadcast $$sz B $$consumers c @ $$rate/s" || exit 1; \
			done; \
		done; \
	done

broadcast-run-extensive-quick:
	@$(MAKE) broadcast-run-quick
	@$(MAKE) internal-broadcast-extensive-quick
	@$(MAKE) crossbar-broadcast-extensive-run-quick

broadcast-run-extensive-fixed-rate-quick:
	@$(MAKE) broadcast-run-fixed-rate-quick
	@$(MAKE) internal-broadcast-extensive-fixed-rate-quick
	@$(MAKE) crossbar-broadcast-extensive-run-fixed-rate-quick

aggregate:
	@mkdir -p $(OUTDIR)
	@set -o pipefail; python3 scripts/aggregate_results.py $(OUTDIR) | tee $(OUTDIR)/aggregate.txt
	@python3 scripts/pareto_frontier.py $(OUTDIR)

graphs:
	@python3 scripts/pareto_frontier.py $(OUTDIR)

internal-headon:
	@mkdir -p $(HEADON_DIR)/disruptor $(HEADON_DIR)/myelon_raw
	@echo "[headon] internal raw disruptor vs raw myelon sizes: $(HEADON_SIZES)"
	@for sz in $(HEADON_SIZES); do \
		$(SIZE_TUNING); \
		$(TIMEOUT_CMD) cargo bench -p perf-bench --profile competitive --manifest-path $(WORKSPACE_MANIFEST) --bench competitive_shm -- --json-canonical --no-compare -s $$sz -n $$msgs -w $$warmup > $(HEADON_DIR)/disruptor/pingpong_$${sz}B.json && echo "  ✓ disruptor $$sz B" || exit 1; \
		$(TIMEOUT_CMD) cargo bench -p perf-bench --profile competitive --manifest-path $(WORKSPACE_MANIFEST) --bench competitive_raw_myelon_shm -- --json --no-compare -s $$sz -n $$msgs -w $$warmup > $(HEADON_DIR)/myelon_raw/pingpong_$${sz}B.json && echo "  ✓ myelon_raw $$sz B" || exit 1; \
	done
	@python3 scripts/aggregate_headon.py $(HEADON_DIR)

simple-smoke-signal:
	@mkdir -p $(OUTDIR)
	@echo "[run] simple-smoke signal lane"
	@MYELON_BENCH_JSON_OUT=$(OUTDIR)/signal_shm.json cargo bench -p perf-bench --profile competitive --manifest-path $(WORKSPACE_MANIFEST) --bench raw_ring_shm -- --timeout 30 --class signal --consumers 1 --events $(SIMPLE_SMOKE_SIGNAL_EVENTS) >/dev/null && echo "  ✓ signal-shm" || exit 1
	@MYELON_BENCH_JSON_OUT=$(OUTDIR)/signal_mmap.json cargo bench -p perf-bench --profile competitive --manifest-path $(WORKSPACE_MANIFEST) --bench raw_ring_mmap -- --timeout 30 --class signal --consumers 1 --events $(SIMPLE_SMOKE_SIGNAL_EVENTS) >/dev/null && echo "  ✓ signal-mmap" || exit 1

simple-smoke:
	@$(MAKE) kill-pingpong || true
	@$(MAKE) cleanup-shm || true
	@rm -rf $(SIMPLE_SMOKE_OUTDIR)
	@$(MAKE) OUTDIR=$(SIMPLE_SMOKE_OUTDIR) simple-smoke-signal
	@$(MAKE) OUTDIR=$(SIMPLE_SMOKE_OUTDIR) RUN_SIZES="$(SIMPLE_SMOKE_SIZES)" SIZES_SMALL="$(SIMPLE_SMOKE_SIZES)" BROKER_RUN_SIZES="$(SIMPLE_SMOKE_SIZES)" NUM_MESSAGES=$(SIMPLE_SMOKE_NUM_MESSAGES) WARMUP=$(SIMPLE_SMOKE_WARMUP) RATES="$(SIMPLE_SMOKE_RATES)" BROKER_RATES="$(SIMPLE_SMOKE_BROKER_RATES)" run-all-quick run-all-fixed-rate-quick
	@$(MAKE) OUTDIR=$(SIMPLE_SMOKE_OUTDIR) aggregate
	@echo "Results in: $(SIMPLE_SMOKE_OUTDIR)"

run-all-quick: internal-run-quick broadcast-run-quick shmipc-run-quick boost-run-quick ompi-run-quick rusteron-run-quick iceoryx2-run-quick crossbar-run-quick zmq-run-quick zmq-ipc-abs-run-quick zmq-tcp-run-quick iggy-run-quick redpanda-run-quick
	@echo "Results in: $(OUTDIR)"

run-all-fixed-rate-quick: internal-run-fixed-rate-quick broadcast-run-fixed-rate-quick shmipc-run-fixed-rate-quick boost-run-fixed-rate-quick ompi-run-fixed-rate-quick rusteron-run-fixed-rate-quick iceoryx2-run-fixed-rate-quick crossbar-run-fixed-rate-quick zmq-run-fixed-rate-quick zmq-ipc-abs-run-fixed-rate-quick zmq-tcp-run-fixed-rate-quick iggy-run-fixed-rate-quick redpanda-run-fixed-rate-quick
	@echo "Results in: $(OUTDIR)"

run-all-extensive-quick:
	@$(MAKE) run-all-quick
	@$(MAKE) RUN_SIZES="$(EXTENSIVE_RUN_SIZES)" internal-run-quick shmipc-run-quick boost-run-quick ompi-run-quick rusteron-run-quick crossbar-run-quick zmq-run-quick zmq-ipc-abs-run-quick zmq-tcp-run-quick
	@$(MAKE) internal-broadcast-extensive-quick
	@$(MAKE) crossbar-broadcast-extensive-run-quick

run-all-extensive-fixed-rate-quick:
	@$(MAKE) run-all-fixed-rate-quick
	@$(MAKE) RUN_SIZES="$(EXTENSIVE_RUN_SIZES)" internal-run-fixed-rate-quick shmipc-run-fixed-rate-quick boost-run-fixed-rate-quick ompi-run-fixed-rate-quick rusteron-run-fixed-rate-quick crossbar-run-fixed-rate-quick zmq-run-fixed-rate-quick zmq-ipc-abs-run-fixed-rate-quick zmq-tcp-run-fixed-rate-quick
	@$(MAKE) internal-broadcast-extensive-fixed-rate-quick
	@$(MAKE) crossbar-broadcast-extensive-run-fixed-rate-quick

ubermensh: build-all run-all-quick run-all-fixed-rate-quick
	@echo ""
	@echo "===== Aggregating results from $(OUTDIR) ====="
	@$(MAKE) aggregate || true

ubermensh-extensive: build-all run-all-extensive-quick run-all-extensive-fixed-rate-quick
	@echo ""
	@echo "===== Aggregating extensive results from $(OUTDIR) ====="
	@$(MAKE) aggregate || true

ubermensh-smoke:
	@$(MAKE) kill-pingpong || true
	@rm -rf $(OUTDIR)
	@WARMUP=10 NUM_MESSAGES=100 RATES=400000 LARGE_WARMUP=10 LARGE_NUM_MESSAGES=100 LARGE_RATES=200 $(MAKE) ubermensh

ubermensh-smoke-no-timeout:
	@$(MAKE) kill-pingpong || true
	@rm -rf $(OUTDIR)
	@WARMUP=10 NUM_MESSAGES=100 RATES=400000 LARGE_WARMUP=10 LARGE_NUM_MESSAGES=100 LARGE_RATES=200 OMPI_TIMEOUT_SEC= $(MAKE) ubermensh

ubermensh-smoke-no-ompi:
	@$(MAKE) kill-pingpong || true
	@rm -rf $(OUTDIR)
	@WARMUP=10 NUM_MESSAGES=100 RATES=400000 LARGE_WARMUP=10 LARGE_NUM_MESSAGES=100 LARGE_RATES=200 $(MAKE) internal-run-quick internal-run-fixed-rate-quick broadcast-run-quick broadcast-run-fixed-rate-quick shmipc-run-quick boost-run-quick rusteron-run-quick iceoryx2-run-quick crossbar-run-quick zmq-run-quick zmq-ipc-abs-run-quick zmq-tcp-run-quick iggy-run-quick redpanda-run-quick shmipc-run-fixed-rate-quick boost-run-fixed-rate-quick rusteron-run-fixed-rate-quick iceoryx2-run-fixed-rate-quick crossbar-run-fixed-rate-quick zmq-run-fixed-rate-quick zmq-ipc-abs-run-fixed-rate-quick zmq-tcp-run-fixed-rate-quick iggy-run-fixed-rate-quick redpanda-run-fixed-rate-quick
	@echo ""
	@echo "===== Aggregating results from $(OUTDIR) ====="
	@$(MAKE) aggregate || true

headon: headon-full
headon-smoke:
	@set -e; \
	 echo "[headon] raw disruptor vs rusteron (smoke) sizes: $(HEADON_SIZES)"; \
	 rm -rf $(HEADON_DIR) && mkdir -p $(HEADON_DIR)/disruptor $(HEADON_DIR)/rusteron; \
	 echo "[headon] rusteron throughput"; \
	 $(MAKE) WARMUP=10 NUM_MESSAGES=100 LARGE_WARMUP=10 LARGE_NUM_MESSAGES=100 SIZES_SMALL="$(HEADON_SIZES)" rusteron-run-quick; \
	 for sz in $(HEADON_SIZES); do \
	   src="$(OUTDIR)/rusteron_$$sz.json"; \
	   [ -f "$$src" ] && cp -f "$$src" "$(HEADON_DIR)/rusteron/pingpong_$${sz}B.json" || true; \
	 done; \
	 echo "[headon] disruptor throughput"; \
	 for sz in $(HEADON_SIZES); do \
	   $(TIMEOUT_CMD) cargo bench -p perf-bench --profile competitive --manifest-path $(WORKSPACE_MANIFEST) --bench competitive_shm -- --json-canonical --no-compare -s $$sz -n 100 -w 10 > $(HEADON_DIR)/disruptor/pingpong_$${sz}B.json || exit 1; \
	 done; \
	 echo "[headon] rusteron fixed-rate (CO) smoke"; \
	 $(MAKE) WARMUP=$(CO_WARMUP) NUM_MESSAGES=$(CO_NUM_MESSAGES) LARGE_WARMUP=10 LARGE_NUM_MESSAGES=100 SIZES_SMALL="$(HEADON_SIZES)" RATES=$(HEADON_RATE_SMOKE) LARGE_RATES=$(LARGE_HEADON_RATE_SMOKE) rusteron-run-fixed-rate-quick; \
	 for sz in $(HEADON_SIZES); do \
	   if [ $$sz -ge $(LARGE_SIZE_THRESHOLD) ]; then rate=$(LARGE_HEADON_RATE_SMOKE); else rate=$(HEADON_RATE_SMOKE); fi; \
	   src="$(OUTDIR)/rusteron_$${sz}_$${rate}.json"; \
	   [ -f "$$src" ] && cp -f "$$src" "$(HEADON_DIR)/rusteron/co_$${sz}B_$${rate}.json" || true; \
	 done; \
	 echo "[headon] disruptor fixed-rate (CO) smoke"; \
	 for sz in $(HEADON_SIZES); do \
	   if [ $$sz -ge $(LARGE_SIZE_THRESHOLD) ]; then msgs=100; warmup=10; rate=$(LARGE_HEADON_RATE_SMOKE); else msgs=$(CO_NUM_MESSAGES); warmup=$(CO_WARMUP); rate=$(HEADON_RATE_SMOKE); fi; \
	   $(TIMEOUT_CMD) cargo bench -p perf-bench --profile competitive --manifest-path $(WORKSPACE_MANIFEST) --bench competitive_shm -- --json-canonical --no-compare -s $$sz -n $$msgs -w $$warmup --target-rate $$rate > $(HEADON_DIR)/disruptor/co_$${sz}B_$${rate}.json || exit 1; \
	 done; \
	 echo ""; \
	 echo "===== raw disruptor vs rusteron (smoke) ====="; \
	 set -o pipefail; python3 scripts/aggregate_headon.py $(HEADON_DIR) | tee $(HEADON_DIR)/aggregate.txt
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
	   $(SIZE_TUNING); \
	   ulimit -s $$stack_kb >/dev/null 2>&1 || true; $(TIMEOUT_CMD) cargo bench -p perf-bench --profile competitive --manifest-path $(WORKSPACE_MANIFEST) --bench competitive_shm -- --json-canonical --no-compare -s $$sz -n $$msgs -w $$warmup > $(HEADON_DIR)/disruptor/pingpong_$${sz}B.json || exit 1; \
	 done; \
	 echo "[headon] rusteron fixed-rate (CO) full rates: core $(RATES), large $(LARGE_RATES)"; \
	 $(MAKE) WARMUP=$(WARMUP) NUM_MESSAGES=$(NUM_MESSAGES) SIZES_SMALL="$(HEADON_SIZES)" rusteron-run-fixed-rate-quick; \
	 for f in $(OUTDIR)/rusteron_*_*.json; do \
	   [ -f "$$f" ] || continue; \
	   b=$$(basename "$$f"); rate=$${b##*_}; rate=$${rate%.json}; tmp=$${b%_$$rate.json}; size=$${tmp#rusteron_}; \
	   cp -f "$$f" "$(HEADON_DIR)/rusteron/co_$${size}B_$$rate.json"; \
	 done; \
	 echo "[headon] disruptor fixed-rate (CO) full rates: core $(RATES), large $(LARGE_RATES)"; \
	 for sz in $(HEADON_SIZES); do \
	   $(SIZE_TUNING); \
	   for rate in $$rates; do \
	     ulimit -s $$stack_kb >/dev/null 2>&1 || true; $(TIMEOUT_CMD) cargo bench -p perf-bench --profile competitive --manifest-path $(WORKSPACE_MANIFEST) --bench competitive_shm -- --json-canonical --no-compare -s $$sz -n $$msgs -w $$warmup --target-rate $$rate > $(HEADON_DIR)/disruptor/co_$${sz}B_$$rate.json || exit 1; \
	   done; \
	 done; \
	 echo ""; \
	 echo "===== raw disruptor vs rusteron (full) ====="; \
	 set -o pipefail; python3 scripts/aggregate_headon.py $(HEADON_DIR) | tee $(HEADON_DIR)/aggregate.txt

headon-extensive:
	@$(MAKE) HEADON_SIZES="$(HEADON_SIZES_EXTENSIVE)" headon-full

verify-align:
	@$(MAKE) verify-align-disruptor verify-align-rusteron
verify-align-disruptor:
	@echo "[verify] raw disruptor alignment"
	@cargo run --quiet -p disruptor-mp --manifest-path $(WORKSPACE_MANIFEST) --example verify_alignment
verify-align-rusteron: rusteron-build
	@echo "[verify] rusteron (aeron ipc) alignment per size: $(RUN_SIZES)"
	@for sz in $(RUN_SIZES); do \
		aeron_libdirs=$$(find $(abspath ../..)/target/competitive/build -type f \( -name 'libaeron.so' -o -name 'libaeron_driver.so' \) -printf '%h:' | sed 's/:$$//'); \
		LD_LIBRARY_PATH="$$aeron_libdirs:$$LD_LIBRARY_PATH" $(RUSTERON_BIN) --verify-align --message-size $$sz || exit 1; \
	done
verify-align-quick: verify-align
verify-align-extensive:
	@$(MAKE) RUN_SIZES="$(EXTENSIVE_RUN_SIZES)" verify-align

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
	-@pkill -f 'iceoryx2_pingpong' 2>/dev/null || true
	-@pkill -f 'zmq_pingpong' 2>/dev/null || true
	-@pkill -f 'iggy_pingpong' 2>/dev/null || true
	-@pkill -f 'redpanda_pingpong' 2>/dev/null || true
	-@pkill -f 'internal_broadcast' 2>/dev/null || true
	-@pkill -f 'crossbar_broadcast' 2>/dev/null || true
	-@$(DOCKER) rm -f $(IGGY_CONTAINER) >/dev/null 2>&1 || true
	-@$(DOCKER) rm -f $(REDPANDA_CONTAINER) >/dev/null 2>&1 || true
	-@pgrep -fa 'competitive_shm|competitive_mmap|competitive_raw_myelon_shm|competitive_raw_myelon_mmap|internal_broadcast|crossbar_pingpong|crossbar_broadcast|shmipc_pingpong|boost_pingpong|ompi_pingpong|rusteron_pingpong|iceoryx2_pingpong|zmq_pingpong|iggy_pingpong|redpanda_pingpong|mpirun .*third_party/ompi_pingpong/pingpong' || true

external-status:
	@echo "[status] internal parity backends: raw disruptor/myelon ping-pong + broadcast over shm + mmap"
	@echo "[status] wired peers: shmipc-rs, boost, ompi, rusteron, iceoryx2, crossbar, zeromq, iggy, redpanda"
	@echo "         extra zeromq transports: ipc-abs, tcp"
	@echo "         local ipc peer: iceoryx2-shm"
	@echo "         brokered tcp peers: iggy-tcp, redpanda-kafka"
	@echo "         broadcast peer: crossbar-pubsub"

broker-build:
	@cargo build -p competitive-bench --profile competitive --manifest-path $(WORKSPACE_MANIFEST) --bin iggy_pingpong --bin redpanda_pingpong

iggy-up:
	@$(DOCKER) rm -f $(IGGY_CONTAINER) >/dev/null 2>&1 || true
	@$(DOCKER) run -d --rm \
		--name $(IGGY_CONTAINER) \
		-p $(IGGY_TCP_PORT):8090 \
		--tmpfs /data \
		--cap-add SYS_NICE \
		--security-opt seccomp=unconfined \
		--ulimit memlock=-1 \
		-e IGGY_ROOT_USERNAME=iggy \
		-e IGGY_ROOT_PASSWORD=iggy \
		-e IGGY_SYSTEM_PATH=/data \
		-e IGGY_TCP_ADDRESS=0.0.0.0:8090 \
		$(IGGY_IMAGE) \
		--with-default-root-credentials >/dev/null
	@for _ in $$(seq 1 60); do \
		if bash -lc 'exec 3<>/dev/tcp/127.0.0.1/$(IGGY_TCP_PORT)' >/dev/null 2>&1; then \
			exit 0; \
		fi; \
		sleep 1; \
	done; \
	echo "Iggy did not become ready on port $(IGGY_TCP_PORT)" >&2; \
	exit 1

iggy-down:
	@$(DOCKER) rm -f $(IGGY_CONTAINER) >/dev/null 2>&1 || true

iggy-run-quick: broker-build
	@mkdir -p $(OUTDIR)
	@set -euo pipefail; \
	trap '$(DOCKER) rm -f $(IGGY_CONTAINER) >/dev/null 2>&1 || true' EXIT; \
	$(MAKE) iggy-up; \
	echo "[run] iggy broker throughput sizes: $(BROKER_RUN_SIZES)"; \
	for sz in $(BROKER_RUN_SIZES); do \
		base="iggy_$${sz}_$${RANDOM}_$${PPID}"; \
		$(TIMEOUT_CMD) $(IGGY_BIN) --mode server --base $$base --message-size $$sz --address 127.0.0.1:$(IGGY_TCP_PORT) >/tmp/competitive_bench_iggy_server.log 2>&1 & \
		server_pid=$$!; \
		sleep 2; \
		$(TIMEOUT_CMD) $(IGGY_BIN) --mode client --base $$base --message-size $$sz --warmup $(WARMUP) --num-messages $(NUM_MESSAGES) --address 127.0.0.1:$(IGGY_TCP_PORT) --json > $(OUTDIR)/iggy_$${sz}.json; \
		status=$$?; \
		kill $$server_pid 2>/dev/null || true; \
		wait $$server_pid 2>/dev/null || true; \
		if [ $$status -ne 0 ]; then exit $$status; fi; \
		echo "  ✓ iggy $$sz B"; \
	done

iggy-run-fixed-rate-quick: broker-build
	@mkdir -p $(OUTDIR)
	@set -euo pipefail; \
	trap '$(DOCKER) rm -f $(IGGY_CONTAINER) >/dev/null 2>&1 || true' EXIT; \
	$(MAKE) iggy-up; \
	echo "[run] iggy broker fixed-rate sizes: $(BROKER_RUN_SIZES), rates: $(BROKER_RATES)"; \
	for sz in $(BROKER_RUN_SIZES); do \
		for rate in $(BROKER_RATES); do \
			base="iggy_$${sz}_$${rate}_$${RANDOM}_$${PPID}"; \
			$(TIMEOUT_CMD) $(IGGY_BIN) --mode server --base $$base --message-size $$sz --address 127.0.0.1:$(IGGY_TCP_PORT) >/tmp/competitive_bench_iggy_server.log 2>&1 & \
			server_pid=$$!; \
			sleep 2; \
			$(TIMEOUT_CMD) $(IGGY_BIN) --mode client --base $$base --message-size $$sz --warmup $(WARMUP) --num-messages $(NUM_MESSAGES) --target-rate $$rate --address 127.0.0.1:$(IGGY_TCP_PORT) --json > $(OUTDIR)/iggy_$${sz}_$${rate}.json; \
			status=$$?; \
			kill $$server_pid 2>/dev/null || true; \
			wait $$server_pid 2>/dev/null || true; \
			if [ $$status -ne 0 ]; then exit $$status; fi; \
			echo "  ✓ iggy $$sz B @ $$rate/s"; \
		done; \
	done

redpanda-up:
	@$(DOCKER) rm -f $(REDPANDA_CONTAINER) >/dev/null 2>&1 || true
	@$(DOCKER) run -d --rm \
		--name $(REDPANDA_CONTAINER) \
		--user 0:0 \
		-p $(REDPANDA_KAFKA_PORT):19092 \
		--tmpfs /var/lib/redpanda/data \
		$(REDPANDA_IMAGE) \
		redpanda start \
		--overprovisioned \
		--smp 1 \
		--memory 1G \
		--reserve-memory 0M \
		--check=false \
		--node-id 0 \
		--kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:19092 \
		--advertise-kafka-addr internal://127.0.0.1:9092,external://127.0.0.1:19092 \
		--rpc-addr 0.0.0.0:33145 \
		--advertise-rpc-addr 127.0.0.1:33145 \
		--mode dev-container >/dev/null
	@for _ in $$(seq 1 60); do \
		if bash -lc 'exec 3<>/dev/tcp/127.0.0.1/$(REDPANDA_KAFKA_PORT)' >/dev/null 2>&1; then \
			exit 0; \
		fi; \
		sleep 1; \
	done; \
	echo "Redpanda did not become ready on port $(REDPANDA_KAFKA_PORT)" >&2; \
	exit 1

redpanda-down:
	@$(DOCKER) rm -f $(REDPANDA_CONTAINER) >/dev/null 2>&1 || true

redpanda-run-quick: broker-build
	@mkdir -p $(OUTDIR)
	@set -euo pipefail; \
	trap '$(DOCKER) rm -f $(REDPANDA_CONTAINER) >/dev/null 2>&1 || true' EXIT; \
	$(MAKE) redpanda-up; \
	echo "[run] redpanda broker throughput sizes: $(BROKER_RUN_SIZES)"; \
	for sz in $(BROKER_RUN_SIZES); do \
		base="redpanda_$${sz}_$${RANDOM}_$${PPID}"; \
		$(TIMEOUT_CMD) $(REDPANDA_BIN) --mode server --base $$base --message-size $$sz --bootstrap 127.0.0.1:$(REDPANDA_KAFKA_PORT) >/tmp/competitive_bench_redpanda_server.log 2>&1 & \
		server_pid=$$!; \
		sleep 2; \
		$(TIMEOUT_CMD) $(REDPANDA_BIN) --mode client --base $$base --message-size $$sz --warmup $(WARMUP) --num-messages $(NUM_MESSAGES) --bootstrap 127.0.0.1:$(REDPANDA_KAFKA_PORT) --json > $(OUTDIR)/redpanda_$${sz}.json; \
		status=$$?; \
		kill $$server_pid 2>/dev/null || true; \
		wait $$server_pid 2>/dev/null || true; \
		if [ $$status -ne 0 ]; then exit $$status; fi; \
		echo "  ✓ redpanda $$sz B"; \
	done

redpanda-run-fixed-rate-quick: broker-build
	@mkdir -p $(OUTDIR)
	@set -euo pipefail; \
	trap '$(DOCKER) rm -f $(REDPANDA_CONTAINER) >/dev/null 2>&1 || true' EXIT; \
	$(MAKE) redpanda-up; \
	echo "[run] redpanda broker fixed-rate sizes: $(BROKER_RUN_SIZES), rates: $(BROKER_RATES)"; \
	for sz in $(BROKER_RUN_SIZES); do \
		for rate in $(BROKER_RATES); do \
			base="redpanda_$${sz}_$${rate}_$${RANDOM}_$${PPID}"; \
			$(TIMEOUT_CMD) $(REDPANDA_BIN) --mode server --base $$base --message-size $$sz --bootstrap 127.0.0.1:$(REDPANDA_KAFKA_PORT) >/tmp/competitive_bench_redpanda_server.log 2>&1 & \
			server_pid=$$!; \
			sleep 2; \
			$(TIMEOUT_CMD) $(REDPANDA_BIN) --mode client --base $$base --message-size $$sz --warmup $(WARMUP) --num-messages $(NUM_MESSAGES) --target-rate $$rate --bootstrap 127.0.0.1:$(REDPANDA_KAFKA_PORT) --json > $(OUTDIR)/redpanda_$${sz}_$${rate}.json; \
			status=$$?; \
			kill $$server_pid 2>/dev/null || true; \
			wait $$server_pid 2>/dev/null || true; \
			if [ $$status -ne 0 ]; then exit $$status; fi; \
			echo "  ✓ redpanda $$sz B @ $$rate/s"; \
		done; \
	done

crossbar-build:
	@RUST_MIN_STACK=67108864 cargo build -p competitive-bench --profile competitive --manifest-path $(WORKSPACE_MANIFEST) --bin crossbar_pingpong --bin crossbar_broadcast

crossbar-run-quick: crossbar-build
	@mkdir -p $(OUTDIR)
	@echo "[run] crossbar SHM parity throughput sizes: $(RUN_SIZES)"
	@for sz in $(RUN_SIZES); do \
		$(SIZE_TUNING); \
		base="crossbar_$${sz}_$$RANDOM_$$PPID"; \
		$(TIMEOUT_CMD) $(CROSSBAR_BIN) --mode server --base $$base --message-size $$sz >/dev/null 2>&1 & \
		server_pid=$$!; \
		sleep 0.2; \
		$(TIMEOUT_CMD) $(CROSSBAR_BIN) --mode client --base $$base --message-size $$sz --num-messages $$msgs --warmup $$warmup --json > $(OUTDIR)/crossbar_$${sz}.json; \
		status=$$?; \
		kill $$server_pid 2>/dev/null || true; \
		wait $$server_pid 2>/dev/null || true; \
		if [ $$status -ne 0 ]; then exit $$status; fi; \
		echo "  ✓ crossbar $$sz B"; \
	done

crossbar-run-fixed-rate-quick: crossbar-build
	@mkdir -p $(OUTDIR)
	@echo "[run] crossbar SHM parity fixed-rate sizes: $(RUN_SIZES), core rates: $(RATES), large rates: $(LARGE_RATES)"
	@for sz in $(RUN_SIZES); do \
		$(SIZE_TUNING); \
		for rate in $$rates; do \
			base="crossbar_$${sz}_$${rate}_$$RANDOM_$$PPID"; \
			$(TIMEOUT_CMD) $(CROSSBAR_BIN) --mode server --base $$base --message-size $$sz >/dev/null 2>&1 & \
			server_pid=$$!; \
			sleep 0.2; \
			$(TIMEOUT_CMD) $(CROSSBAR_BIN) --mode client --base $$base --message-size $$sz --num-messages $$msgs --warmup $$warmup --target-rate $$rate --json > $(OUTDIR)/crossbar_$${sz}_$${rate}.json; \
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
	@echo "[run] shmipc-rs SHM parity throughput sizes: $(RUN_SIZES)"
	@for sz in $(RUN_SIZES); do \
		$(SIZE_TUNING); \
		base="shmipc_$${sz}_$$RANDOM_$$PPID"; \
		$(TIMEOUT_CMD) $(SHMIPC_BIN) --mode server --base $$base --message-size $$sz >/dev/null 2>&1 & \
		server_pid=$$!; \
		sleep 0.2; \
		$(TIMEOUT_CMD) $(SHMIPC_BIN) --mode client --no-embedded-server --base $$base --message-size $$sz --num-messages $$msgs --warmup $$warmup --json > $(OUTDIR)/shmipc_$${sz}.json; \
		status=$$?; \
		kill $$server_pid 2>/dev/null || true; \
		wait $$server_pid 2>/dev/null || true; \
		if [ $$status -ne 0 ]; then exit $$status; fi; \
		echo "  ✓ shmipc $$sz B"; \
	done
shmipc-run-fixed-rate-quick: shmipc-build
	@mkdir -p $(OUTDIR)
	@echo "[run] shmipc-rs SHM parity fixed-rate sizes: $(RUN_SIZES), core rates: $(RATES), large rates: $(LARGE_RATES)"
	@for sz in $(RUN_SIZES); do \
		$(SIZE_TUNING); \
		for rate in $$rates; do \
			base="shmipc_$${sz}_$${rate}_$$RANDOM_$$PPID"; \
			$(TIMEOUT_CMD) $(SHMIPC_BIN) --mode server --base $$base --message-size $$sz >/dev/null 2>&1 & \
			server_pid=$$!; \
			sleep 0.2; \
			$(TIMEOUT_CMD) $(SHMIPC_BIN) --mode client --no-embedded-server --base $$base --message-size $$sz --num-messages $$msgs --warmup $$warmup --target-rate $$rate --json > $(OUTDIR)/shmipc_$${sz}_$${rate}.json; \
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
	@echo "[run] boost::interprocess parity throughput sizes: $(RUN_SIZES)"
	@for sz in $(RUN_SIZES); do \
		$(SIZE_TUNING); \
		base="boost_$${sz}_$$RANDOM_$$PPID"; \
		$(TIMEOUT_CMD) $(BOOST_BIN) --mode server --base $$base --message-size $$sz >/dev/null 2>&1 & \
		server_pid=$$!; \
		sleep 0.2; \
		$(TIMEOUT_CMD) $(BOOST_BIN) --mode client --base $$base --message-size $$sz --num-messages $$msgs --warmup $$warmup --json > $(OUTDIR)/boost_$${sz}.json; \
		status=$$?; \
		kill $$server_pid 2>/dev/null || true; \
		wait $$server_pid 2>/dev/null || true; \
		if [ $$status -ne 0 ]; then exit $$status; fi; \
		echo "  ✓ boost $$sz B"; \
	done
boost-run-fixed-rate-quick: boost-build
	@mkdir -p $(OUTDIR)
	@echo "[run] boost::interprocess parity fixed-rate sizes: $(RUN_SIZES), core rates: $(RATES), large rates: $(LARGE_RATES)"
	@for sz in $(RUN_SIZES); do \
		$(SIZE_TUNING); \
		for rate in $$rates; do \
			base="boost_$${sz}_$${rate}_$$RANDOM_$$PPID"; \
			$(TIMEOUT_CMD) $(BOOST_BIN) --mode server --base $$base --message-size $$sz >/dev/null 2>&1 & \
			server_pid=$$!; \
			sleep 0.2; \
			$(TIMEOUT_CMD) $(BOOST_BIN) --mode client --base $$base --message-size $$sz --num-messages $$msgs --warmup $$warmup --target-rate $$rate --json > $(OUTDIR)/boost_$${sz}_$${rate}.json; \
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
	@echo "[run] Open MPI parity throughput sizes: $(RUN_SIZES)"
	@for sz in $(RUN_SIZES); do \
		$(SIZE_TUNING); \
		( cd $(OMPI_DIR) && $(OMPI_TIMEOUT_CMD) $(OMPI_MPIRUN) --oversubscribe -n 2 --mca pml ob1 --mca btl vader,self ./pingpong --message-size $$sz --num-messages $$msgs --warmup $$warmup --json ) > $(OUTDIR)/ompi_$${sz}.json && echo "  ✓ ompi $$sz B" || exit 1; \
	done
ompi-run-fixed-rate-quick: ompi-build
	@mkdir -p $(OUTDIR)
	@echo "[run] Open MPI parity fixed-rate sizes: $(RUN_SIZES), core rates: $(RATES), large rates: $(LARGE_RATES)"
	@for sz in $(RUN_SIZES); do \
		$(SIZE_TUNING); \
		for rate in $$rates; do \
			( cd $(OMPI_DIR) && $(OMPI_TIMEOUT_CMD) $(OMPI_MPIRUN) --oversubscribe -n 2 --mca pml ob1 --mca btl vader,self ./pingpong --message-size $$sz --num-messages $$msgs --warmup $$warmup --target-rate $$rate --json ) > $(OUTDIR)/ompi_$${sz}_$${rate}.json && echo "  ✓ ompi $$sz B @ $$rate/s" || exit 1; \
		done; \
	done

rusteron-build:
	@cargo build -p competitive-bench --profile competitive --manifest-path $(WORKSPACE_MANIFEST) --bin rusteron_pingpong
rusteron-run-quick: rusteron-build
	@mkdir -p $(OUTDIR)
	@echo "[run] rusteron IPC parity throughput sizes: $(RUN_SIZES)"
	@for sz in $(RUN_SIZES); do \
		if [ $$sz -gt $(RUSTERON_MAX_MESSAGE_SIZE) ]; then \
			echo "  - skip rusteron $$sz B (Aeron max message length is $(RUSTERON_MAX_MESSAGE_SIZE) bytes)"; \
			continue; \
		fi; \
		$(SIZE_TUNING); \
		base="rusteron_$${sz}_$$RANDOM_$$PPID"; \
		aeron_libdirs=$$(find $(abspath ../..)/target/competitive/build -type f \( -name 'libaeron.so' -o -name 'libaeron_driver.so' \) -printf '%h:' | sed 's/:$$//'); \
		LD_LIBRARY_PATH="$$aeron_libdirs:$$LD_LIBRARY_PATH" $(TIMEOUT_CMD) $(RUSTERON_BIN) --mode server --base $$base --message-size $$sz >/dev/null 2>&1 & \
		server_pid=$$!; \
		sleep 0.5; \
		LD_LIBRARY_PATH="$$aeron_libdirs:$$LD_LIBRARY_PATH" $(TIMEOUT_CMD) $(RUSTERON_BIN) --mode client --base $$base --message-size $$sz --num-messages $$msgs --warmup $$warmup --json > $(OUTDIR)/rusteron_$${sz}.json; \
		status=$$?; \
		kill $$server_pid 2>/dev/null || true; \
		wait $$server_pid 2>/dev/null || true; \
		rm -rf /dev/shm/$$base 2>/dev/null || true; \
		if [ $$status -ne 0 ]; then exit $$status; fi; \
		echo "  ✓ rusteron $$sz B"; \
	done
rusteron-run-fixed-rate-quick: rusteron-build
	@mkdir -p $(OUTDIR)
	@echo "[run] rusteron IPC parity fixed-rate sizes: $(RUN_SIZES), core rates: $(RATES), large rates: $(LARGE_RATES)"
	@for sz in $(RUN_SIZES); do \
		if [ $$sz -gt $(RUSTERON_MAX_MESSAGE_SIZE) ]; then \
			echo "  - skip rusteron $$sz B fixed-rate (Aeron max message length is $(RUSTERON_MAX_MESSAGE_SIZE) bytes)"; \
			continue; \
		fi; \
		$(SIZE_TUNING); \
		for rate in $$rates; do \
			base="rusteron_$${sz}_$${rate}_$$RANDOM_$$PPID"; \
			aeron_libdirs=$$(find $(abspath ../..)/target/competitive/build -type f \( -name 'libaeron.so' -o -name 'libaeron_driver.so' \) -printf '%h:' | sed 's/:$$//'); \
			LD_LIBRARY_PATH="$$aeron_libdirs:$$LD_LIBRARY_PATH" $(TIMEOUT_CMD) $(RUSTERON_BIN) --mode server --base $$base --message-size $$sz >/dev/null 2>&1 & \
			server_pid=$$!; \
			sleep 0.5; \
			LD_LIBRARY_PATH="$$aeron_libdirs:$$LD_LIBRARY_PATH" $(TIMEOUT_CMD) $(RUSTERON_BIN) --mode client --base $$base --message-size $$sz --num-messages $$msgs --warmup $$warmup --target-rate $$rate --json > $(OUTDIR)/rusteron_$${sz}_$${rate}.json; \
			status=$$?; \
			kill $$server_pid 2>/dev/null || true; \
			wait $$server_pid 2>/dev/null || true; \
			rm -rf /dev/shm/$$base 2>/dev/null || true; \
			if [ $$status -ne 0 ]; then exit $$status; fi; \
			echo "  ✓ rusteron $$sz B @ $$rate/s"; \
		done; \
	done

iceoryx2-build:
	@cargo build -p competitive-bench --profile competitive --manifest-path $(WORKSPACE_MANIFEST) --bin iceoryx2_pingpong

iceoryx2-run-quick: iceoryx2-build
	@mkdir -p $(OUTDIR)
	@echo "[run] iceoryx2 SHM parity throughput sizes: $(RUN_SIZES)"
	@for sz in $(RUN_SIZES); do \
		$(SIZE_TUNING); \
		base="iceoryx2_$${sz}_$$RANDOM_$$PPID"; \
		$(TIMEOUT_CMD) $(ICEORYX2_BIN) --mode server --base $$base --message-size $$sz >/dev/null 2>&1 & \
		server_pid=$$!; \
		sleep 0.5; \
		$(TIMEOUT_CMD) $(ICEORYX2_BIN) --mode client --base $$base --message-size $$sz --num-messages $$msgs --warmup $$warmup --json > $(OUTDIR)/iceoryx2_$${sz}.json; \
		status=$$?; \
		kill $$server_pid 2>/dev/null || true; \
		wait $$server_pid 2>/dev/null || true; \
		if [ $$status -ne 0 ]; then exit $$status; fi; \
		echo "  ✓ iceoryx2 $$sz B"; \
	done

iceoryx2-run-fixed-rate-quick: iceoryx2-build
	@mkdir -p $(OUTDIR)
	@echo "[run] iceoryx2 SHM parity fixed-rate sizes: $(RUN_SIZES), core rates: $(RATES), large rates: $(LARGE_RATES)"
	@for sz in $(RUN_SIZES); do \
		$(SIZE_TUNING); \
		for rate in $$rates; do \
			base="iceoryx2_$${sz}_$${rate}_$$RANDOM_$$PPID"; \
			$(TIMEOUT_CMD) $(ICEORYX2_BIN) --mode server --base $$base --message-size $$sz >/dev/null 2>&1 & \
			server_pid=$$!; \
			sleep 0.5; \
			$(TIMEOUT_CMD) $(ICEORYX2_BIN) --mode client --base $$base --message-size $$sz --num-messages $$msgs --warmup $$warmup --target-rate $$rate --json > $(OUTDIR)/iceoryx2_$${sz}_$${rate}.json; \
			status=$$?; \
			kill $$server_pid 2>/dev/null || true; \
			wait $$server_pid 2>/dev/null || true; \
			if [ $$status -ne 0 ]; then exit $$status; fi; \
			echo "  ✓ iceoryx2 $$sz B @ $$rate/s"; \
		done; \
	done

zmq-build:
	@cargo build -p competitive-bench --profile competitive --manifest-path $(WORKSPACE_MANIFEST) --bin zmq_pingpong
zmq-run-quick: zmq-ipc-run-quick
zmq-run-fixed-rate-quick: zmq-ipc-run-fixed-rate-quick
zmq-ipc-run-quick: zmq-build
	@mkdir -p $(OUTDIR)
	@echo "[run] ZeroMQ ipc parity throughput sizes: $(RUN_SIZES)"
	@for sz in $(RUN_SIZES); do \
		$(SIZE_TUNING); \
		base="zmq_$${sz}_$$RANDOM_$$PPID"; \
		$(TIMEOUT_CMD) $(ZMQ_BIN) --mode server --base $$base --message-size $$sz --transport ipc >/dev/null 2>&1 & \
		server_pid=$$!; \
		sleep 0.2; \
		$(TIMEOUT_CMD) $(ZMQ_BIN) --mode client --base $$base --message-size $$sz --transport ipc --num-messages $$msgs --warmup $$warmup --json > $(OUTDIR)/zmq_$${sz}.json; \
		status=$$?; \
		kill $$server_pid 2>/dev/null || true; \
		wait $$server_pid 2>/dev/null || true; \
		rm -f /dev/shm/$$base.zmq 2>/dev/null || true; \
		if [ $$status -ne 0 ]; then exit $$status; fi; \
		echo "  ✓ zmq(ipc) $$sz B"; \
	done
zmq-ipc-run-fixed-rate-quick: zmq-build
	@mkdir -p $(OUTDIR)
	@echo "[run] ZeroMQ ipc parity fixed-rate sizes: $(RUN_SIZES), core rates: $(RATES), large rates: $(LARGE_RATES)"
	@for sz in $(RUN_SIZES); do \
		$(SIZE_TUNING); \
		for rate in $$rates; do \
			base="zmq_$${sz}_$${rate}_$$RANDOM_$$PPID"; \
			$(TIMEOUT_CMD) $(ZMQ_BIN) --mode server --base $$base --message-size $$sz --transport ipc >/dev/null 2>&1 & \
			server_pid=$$!; \
			sleep 0.2; \
			$(TIMEOUT_CMD) $(ZMQ_BIN) --mode client --base $$base --message-size $$sz --transport ipc --num-messages $$msgs --warmup $$warmup --target-rate $$rate --json > $(OUTDIR)/zmq_$${sz}_$${rate}.json; \
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
	@echo "[run] ZeroMQ ipc-abs parity throughput sizes: $(RUN_SIZES)"
	@for sz in $(RUN_SIZES); do \
		$(SIZE_TUNING); \
		base="zmqabs_$${sz}_$$RANDOM_$$PPID"; \
		$(TIMEOUT_CMD) $(ZMQ_BIN) --mode server --base $$base --message-size $$sz --transport ipc-abs >/dev/null 2>&1 & \
		server_pid=$$!; \
		sleep 0.2; \
		$(TIMEOUT_CMD) $(ZMQ_BIN) --mode client --base $$base --message-size $$sz --transport ipc-abs --num-messages $$msgs --warmup $$warmup --json > $(OUTDIR)/zmqabs_$${sz}.json; \
		status=$$?; \
		kill $$server_pid 2>/dev/null || true; \
		wait $$server_pid 2>/dev/null || true; \
		if [ $$status -ne 0 ]; then exit $$status; fi; \
		echo "  ✓ zmq(ipc-abs) $$sz B"; \
	done
zmq-ipc-abs-run-fixed-rate-quick: zmq-build
	@mkdir -p $(OUTDIR)
	@echo "[run] ZeroMQ ipc-abs parity fixed-rate sizes: $(RUN_SIZES), core rates: $(RATES), large rates: $(LARGE_RATES)"
	@for sz in $(RUN_SIZES); do \
		$(SIZE_TUNING); \
		for rate in $$rates; do \
			base="zmqabs_$${sz}_$${rate}_$$RANDOM_$$PPID"; \
			$(TIMEOUT_CMD) $(ZMQ_BIN) --mode server --base $$base --message-size $$sz --transport ipc-abs >/dev/null 2>&1 & \
			server_pid=$$!; \
			sleep 0.2; \
			$(TIMEOUT_CMD) $(ZMQ_BIN) --mode client --base $$base --message-size $$sz --transport ipc-abs --num-messages $$msgs --warmup $$warmup --target-rate $$rate --json > $(OUTDIR)/zmqabs_$${sz}_$${rate}.json; \
			status=$$?; \
			kill $$server_pid 2>/dev/null || true; \
			wait $$server_pid 2>/dev/null || true; \
			if [ $$status -ne 0 ]; then exit $$status; fi; \
			echo "  ✓ zmq(ipc-abs) $$sz B @ $$rate/s"; \
		done; \
	done
zmq-tcp-run-quick: zmq-build
	@mkdir -p $(OUTDIR)
	@echo "[run] ZeroMQ tcp parity throughput sizes: $(RUN_SIZES)"
	@for sz in $(RUN_SIZES); do \
		$(SIZE_TUNING); \
		base="zmqtcp_$${sz}_$$RANDOM_$$PPID"; \
		$(TIMEOUT_CMD) $(ZMQ_BIN) --mode server --base $$base --message-size $$sz --transport tcp >/dev/null 2>&1 & \
		server_pid=$$!; \
		sleep 0.2; \
		$(TIMEOUT_CMD) $(ZMQ_BIN) --mode client --base $$base --message-size $$sz --transport tcp --num-messages $$msgs --warmup $$warmup --json > $(OUTDIR)/zmqtcp_$${sz}.json; \
		status=$$?; \
		kill $$server_pid 2>/dev/null || true; \
		wait $$server_pid 2>/dev/null || true; \
		if [ $$status -ne 0 ]; then exit $$status; fi; \
		echo "  ✓ zmq(tcp) $$sz B"; \
	done
zmq-tcp-run-fixed-rate-quick: zmq-build
	@mkdir -p $(OUTDIR)
	@echo "[run] ZeroMQ tcp parity fixed-rate sizes: $(RUN_SIZES), core rates: $(RATES), large rates: $(LARGE_RATES)"
	@for sz in $(RUN_SIZES); do \
		$(SIZE_TUNING); \
		for rate in $$rates; do \
			base="zmqtcp_$${sz}_$${rate}_$$RANDOM_$$PPID"; \
			$(TIMEOUT_CMD) $(ZMQ_BIN) --mode server --base $$base --message-size $$sz --transport tcp >/dev/null 2>&1 & \
			server_pid=$$!; \
			sleep 0.2; \
			$(TIMEOUT_CMD) $(ZMQ_BIN) --mode client --base $$base --message-size $$sz --transport tcp --num-messages $$msgs --warmup $$warmup --target-rate $$rate --json > $(OUTDIR)/zmqtcp_$${sz}_$${rate}.json; \
			status=$$?; \
			kill $$server_pid 2>/dev/null || true; \
			wait $$server_pid 2>/dev/null || true; \
			if [ $$status -ne 0 ]; then exit $$status; fi; \
			echo "  ✓ zmq(tcp) $$sz B @ $$rate/s"; \
		done; \
	done
