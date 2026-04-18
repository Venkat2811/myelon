# perf-bench Makefile — benchmark matrix runner
#
# Usage:
#   make smoke          # Quick smoke test (~60s)
#   make competitive    # Competitive ping-pong benches
#   make myelon-sweep   # Myelon layer + nofrag sweeps
#   make all-multi      # Multi-consumer coverage matrix
#   make sweep          # Monster sweep SHM + mmap (full)
#   make sweep-quick    # Monster sweep quick mode
#   make layers         # Myelon layer comparison (raw vs framed vs rkyv_nofrag)
#   make nofrag         # No-frag zero-copy matrix (3 layers x 2 backends x 4 sizes)
#   make wait           # Wait strategy SHM + mmap (quick)
#   make wait-full      # Wait strategy full matrix (28 combos each)
#   make all            # Everything
#   make results        # Full matrix with JSON/CSV/MD output

BENCH = cargo bench -p perf-bench
OUT_DIR ?= /tmp/perf_bench_results
PROFILE ?= bench

.PHONY: smoke competitive myelon-sweep all-multi sweep sweep-quick layers nofrag wait wait-full codec codec-phase framed raw layout all results clean

# --- Smoke test: ~60 seconds, validates all binaries produce output ---
smoke:
	@echo "=== Smoke Test ==="
	$(BENCH) --bench raw_ring_shm -- --timeout 30 --class signal --consumers 1
	$(BENCH) --bench raw_ring_mmap -- --timeout 30 --class signal --consumers 1
	$(BENCH) --bench framed_shm -- --timeout 30 --payload 1K
	$(BENCH) --bench framed_mmap -- --timeout 30 --payload 1K
	$(BENCH) --bench codec_e2e_shm -- --timeout 30 --codec bincode --batch 8 --consumers 1
	$(BENCH) --bench codec_e2e_mmap -- --timeout 30 --codec bincode --batch 8 --consumers 1
	$(BENCH) --bench codec_nofrag_shm -- --timeout 30 --codec rkyv --batch 8 --consumers 1
	$(BENCH) --bench wait_strategy_shm -- --timeout 30 quick
	$(BENCH) --bench wait_strategy_mmap -- --timeout 30 quick
	$(BENCH) --bench myelon_layers -- --timeout 30 --size 1KB --consumers 2 --layer raw_ring
	$(BENCH) --bench nofrag_all -- --timeout 30 --size 1KB --consumers 2 --layer raw_ring --backend shm
	$(BENCH) --bench monster_sweep_shm -- --timeout 30 --quick --size 64B --mode throughput
	$(BENCH) --bench monster_sweep_mmap -- --timeout 30 --quick --size 64B --mode throughput
	$(BENCH) --bench competitive_shm -- --timeout 30 -s 64 -n 1000 -w 100 --json --no-compare
	$(BENCH) --bench competitive_mmap -- --timeout 30
	$(BENCH) --bench layout_validation --no-run
	@echo "=== Smoke Test PASSED ==="

# --- Competitive ping-pong ---
competitive:
	$(BENCH) --bench competitive_shm -- --timeout 60 -s 64 -n 100000 -w 10000 --no-compare
	$(BENCH) --bench competitive_shm -- --timeout 60 -s 64 -n 100000 -w 10000 --target-rate 20000 --no-compare
	$(BENCH) --bench competitive_shm -- --timeout 60 -s 64 -n 100000 -w 10000 --batch-timing --no-compare
	$(BENCH) --bench competitive_mmap -- --timeout 60 -s 64 -n 100000 -w 10000 --no-compare
	$(BENCH) --bench competitive_mmap -- --timeout 60 -s 64 -n 100000 -w 10000 --target-rate 20000 --no-compare
	$(BENCH) --bench competitive_mmap -- --timeout 60 -s 64 -n 100000 -w 10000 --batch-timing --no-compare

# --- Myelon layer sweeps ---
myelon-sweep:
	$(BENCH) --bench myelon_layers
	$(BENCH) --bench nofrag_all

# --- Full multi-consumer coverage set ---
all-multi:
	$(BENCH) --bench raw_ring_shm
	$(BENCH) --bench raw_ring_mmap
	$(BENCH) --bench framed_shm
	$(BENCH) --bench framed_mmap
	$(BENCH) --bench codec_e2e_shm
	$(BENCH) --bench codec_e2e_mmap
	$(BENCH) --bench codec_nofrag_shm
	$(BENCH) --bench myelon_layers
	$(BENCH) --bench nofrag_all

# --- Monster sweep ---
sweep:
	@mkdir -p $(OUT_DIR)
	$(BENCH) --bench monster_sweep_shm -- \
		--md-out $(OUT_DIR)/sweep_shm.md --json-out $(OUT_DIR)/sweep_shm.json --csv-out $(OUT_DIR)/sweep_shm.csv
	$(BENCH) --bench monster_sweep_mmap -- \
		--md-out $(OUT_DIR)/sweep_mmap.md --json-out $(OUT_DIR)/sweep_mmap.json --csv-out $(OUT_DIR)/sweep_mmap.csv

sweep-quick:
	@mkdir -p $(OUT_DIR)
	$(BENCH) --bench monster_sweep_shm -- --quick \
		--md-out $(OUT_DIR)/sweep_shm_quick.md --json-out $(OUT_DIR)/sweep_shm_quick.json
	$(BENCH) --bench monster_sweep_mmap -- --quick \
		--md-out $(OUT_DIR)/sweep_mmap_quick.md --json-out $(OUT_DIR)/sweep_mmap_quick.json

# --- Myelon layer comparison ---
layers:
	$(BENCH) --bench myelon_layers

# --- No-frag zero-copy matrix ---
nofrag:
	$(BENCH) --bench nofrag_all

# --- Wait strategy ---
wait:
	$(BENCH) --bench wait_strategy_shm
	$(BENCH) --bench wait_strategy_mmap

wait-full:
	@mkdir -p $(OUT_DIR)
	BENCH_MODE=full MYELON_BENCH_JSON_OUT=$(OUT_DIR)/wait_shm.json $(BENCH) --bench wait_strategy_shm
	BENCH_MODE=full MYELON_BENCH_JSON_OUT=$(OUT_DIR)/wait_mmap.json $(BENCH) --bench wait_strategy_mmap

# --- Codec ---
codec:
	$(BENCH) --bench codec_e2e_shm
	$(BENCH) --bench codec_e2e_mmap
	$(BENCH) --bench codec_nofrag_shm

codec-phase:
	$(BENCH) --bench codec_e2e_shm -- --mode phase_timing
	$(BENCH) --bench codec_e2e_mmap -- --mode phase_timing

# --- Framed ---
framed:
	$(BENCH) --bench framed_shm
	$(BENCH) --bench framed_mmap

# --- Raw ring ---
raw:
	$(BENCH) --bench raw_ring_shm
	$(BENCH) --bench raw_ring_mmap

# --- Layout ---
layout:
	$(BENCH) --bench layout_validation

# --- All benchmarks (sequential) ---
all: raw framed codec wait myelon-sweep competitive sweep layout

# --- Full matrix with all output formats ---
results:
	@mkdir -p $(OUT_DIR)
	@echo "=== Full Benchmark Matrix ==="
	@echo "Output: $(OUT_DIR)/"
	MYELON_BENCH_JSON_OUT=$(OUT_DIR)/raw_ring_shm.json $(BENCH) --bench raw_ring_shm
	MYELON_BENCH_JSON_OUT=$(OUT_DIR)/raw_ring_mmap.json $(BENCH) --bench raw_ring_mmap
	MYELON_BENCH_JSON_OUT=$(OUT_DIR)/framed_shm.json $(BENCH) --bench framed_shm
	MYELON_BENCH_JSON_OUT=$(OUT_DIR)/framed_mmap.json $(BENCH) --bench framed_mmap
	MYELON_BENCH_JSON_OUT=$(OUT_DIR)/codec_e2e_shm.json $(BENCH) --bench codec_e2e_shm
	MYELON_BENCH_JSON_OUT=$(OUT_DIR)/codec_e2e_mmap.json $(BENCH) --bench codec_e2e_mmap
	MYELON_BENCH_JSON_OUT=$(OUT_DIR)/codec_nofrag_shm.json $(BENCH) --bench codec_nofrag_shm
	DISRUPTOR_MP_BENCHMARK_JSON_OUT=$(OUT_DIR)/competitive_shm.json $(BENCH) --bench competitive_shm -- --json-canonical --no-compare --csv-out $(OUT_DIR)/competitive_shm.csv --md-out $(OUT_DIR)/competitive_shm.md
	DISRUPTOR_MP_BENCHMARK_JSON_OUT=$(OUT_DIR)/competitive_mmap.json $(BENCH) --bench competitive_mmap -- --json-canonical --no-compare --csv-out $(OUT_DIR)/competitive_mmap.csv --md-out $(OUT_DIR)/competitive_mmap.md
	$(BENCH) --bench layout_validation -- --json-out $(OUT_DIR)/layout_validation.json
	BENCH_MODE=full MYELON_BENCH_JSON_OUT=$(OUT_DIR)/wait_shm.json $(BENCH) --bench wait_strategy_shm
	BENCH_MODE=full MYELON_BENCH_JSON_OUT=$(OUT_DIR)/wait_mmap.json $(BENCH) --bench wait_strategy_mmap
	$(BENCH) --bench myelon_layers -- --json-out $(OUT_DIR)/myelon_layers.json --csv-out $(OUT_DIR)/myelon_layers.csv --md-out $(OUT_DIR)/myelon_layers.md
	$(BENCH) --bench nofrag_all -- --json-out $(OUT_DIR)/nofrag_all.json --csv-out $(OUT_DIR)/nofrag_all.csv --md-out $(OUT_DIR)/nofrag_all.md
	$(BENCH) --bench monster_sweep_shm -- \
		--md-out $(OUT_DIR)/sweep_shm.md --json-out $(OUT_DIR)/sweep_shm.json --csv-out $(OUT_DIR)/sweep_shm.csv
	$(BENCH) --bench monster_sweep_mmap -- \
		--md-out $(OUT_DIR)/sweep_mmap.md --json-out $(OUT_DIR)/sweep_mmap.json --csv-out $(OUT_DIR)/sweep_mmap.csv
	@echo "=== Full Matrix Complete ==="
	@echo "Results in $(OUT_DIR)/"

# --- Clean ---
clean:
	@rm -rf $(OUT_DIR)
	@echo "Cleaned $(OUT_DIR)"
