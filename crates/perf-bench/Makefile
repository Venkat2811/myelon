# perf-bench Makefile — benchmark matrix runner
#
# Usage:
#   make smoke          # Quick smoke test (~60s)
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

.PHONY: smoke sweep sweep-quick layers nofrag wait wait-full codec framed raw layout all results clean

# --- Smoke test: ~60 seconds, validates all binaries produce output ---
smoke:
	@echo "=== Smoke Test ==="
	$(BENCH) --bench raw_ring_shm -- --class signal --consumers 1
	$(BENCH) --bench raw_ring_mmap -- --class signal --consumers 1
	$(BENCH) --bench framed_shm -- --payload 1K
	$(BENCH) --bench framed_mmap -- --payload 1K
	$(BENCH) --bench codec_e2e_shm -- --mode throughput
	$(BENCH) --bench codec_nofrag_shm -- --batch 8
	$(BENCH) --bench layout_validation
	$(BENCH) --bench wait_strategy_shm
	$(BENCH) --bench wait_strategy_mmap
	$(BENCH) --bench myelon_layers
	$(BENCH) --bench nofrag_all
	$(BENCH) --bench monster_sweep_shm -- --quick
	$(BENCH) --bench monster_sweep_mmap -- --quick
	@echo "=== Smoke Test PASSED ==="

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
all: raw framed codec wait layers nofrag sweep layout

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
	$(BENCH) --bench layout_validation
	BENCH_MODE=full MYELON_BENCH_JSON_OUT=$(OUT_DIR)/wait_shm.json $(BENCH) --bench wait_strategy_shm
	BENCH_MODE=full MYELON_BENCH_JSON_OUT=$(OUT_DIR)/wait_mmap.json $(BENCH) --bench wait_strategy_mmap
	$(BENCH) --bench myelon_layers
	$(BENCH) --bench nofrag_all
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
