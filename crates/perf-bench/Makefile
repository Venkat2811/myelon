# perf-bench Makefile — tiered benchmark runner
#
# Tiers (RFC 0026):
#   make simple-smoke # ~30-60s: exact-size sanity lane (signal + raw pingpong + raw broadcast)
#   make super-tiny   # ~5-10 min: broad CI lane across all implemented scenario families
#   make quick       # ~1-2 min: endpoints only (signal + raw_ring + raw_myelon + typed_zc)
#   make smoke       # ~5-10 min: endpoints + 4 sizes
#   make large-only  # ~15-30 min: large payloads (64KB-32MB) + all layers
#   make extensive   # ~1-2 hrs: all layers, 8 sizes, multi-consumer
#   make full-blown  # ~6-12 hrs: everything + repeatability
#
# Individual layers:
#   make raw          make framed     make codec      make wait
#   make layers       make nofrag     make sweep      make layout

PP = cargo run -p perf-bench --profile competitive --bin perf-bench-pingpong --
BC = cargo run -p perf-bench --profile competitive --bin perf-bench-broadcast --
SG = cargo run -p perf-bench --profile competitive --bin perf-bench-signal --

# Release profile variants (for dual-profile comparison in quick/smoke)
PP_REL = cargo run -p perf-bench --release --bin perf-bench-pingpong --
SG_REL = cargo run -p perf-bench --release --bin perf-bench-signal --

# Message counts per RFC 0026
CORE_MSGS = 1000000
CORE_WARMUP = 50000
LARGE_MSGS = 500000
LARGE_WARMUP = 25000
HUGE_MSGS = 100000
HUGE_WARMUP = 5000
MASSIVE_MSGS = 50000
MASSIVE_WARMUP = 2500
GIANT_MSGS = 10000
GIANT_WARMUP = 500

CO_RATE = 400000
SIMPLE_MSGS = 1000
SIMPLE_WARMUP = 100
SIMPLE_SIGNAL_EVENTS = 1000
SIMPLE_CO_RATE = 50000
SIMPLE_BC_CONSUMERS = 4

.PHONY: simple-smoke super-tiny quick smoke large-only extensive full-blown \
	raw framed codec wait layers nofrag sweep layout \
	clean

# ============================================================
# Tier -1: simple-smoke (~30-60s)
# Exact-size sanity lane for code paths that honor the explicit size ladder.
# Sizes: 32B, 64B, 128B, 1KB, 2KB, 4KB | Consumers: 1 (pp), 4 (bc)
# ============================================================
simple-smoke:
	@echo "=== SIMPLE-SMOKE (signal + exact-size raw lanes, ~30-60s) ==="
	@# Signal
	$(SG) --backend shm --consumers 1 --events $(SIMPLE_SIGNAL_EVENTS) --tree
	$(SG) --backend mmap --consumers 1 --events $(SIMPLE_SIGNAL_EVENTS) --tree
	@# Pingpong: raw_ring + raw_myelon × {shm,mmap} × exact sizes × {throughput,CO}
	@for sz in 32 64 128 1024 2048 4096; do \
		for layer in raw_ring raw_myelon; do \
			$(PP) --layer $$layer --backend shm  --size $$sz -n $(SIMPLE_MSGS) -w $(SIMPLE_WARMUP) --tree; \
			$(PP) --layer $$layer --backend mmap --size $$sz -n $(SIMPLE_MSGS) -w $(SIMPLE_WARMUP) --tree; \
			$(PP) --layer $$layer --backend shm  --size $$sz -n $(SIMPLE_MSGS) -w $(SIMPLE_WARMUP) --mode co --target-rate $(SIMPLE_CO_RATE) --tree; \
			$(PP) --layer $$layer --backend mmap --size $$sz -n $(SIMPLE_MSGS) -w $(SIMPLE_WARMUP) --mode co --target-rate $(SIMPLE_CO_RATE) --tree; \
		done; \
	done
	@# Broadcast: raw_ring + raw_myelon × {shm,mmap} × 4c × exact sizes
	@for sz in 32 64 128 1024 2048 4096; do \
		for layer in raw_ring raw_myelon; do \
			$(BC) --layer $$layer --backend shm  --class message --size $$sz --consumers $(SIMPLE_BC_CONSUMERS) -n $(SIMPLE_MSGS) -w $(SIMPLE_WARMUP) --tree --timeout 120; \
			$(BC) --layer $$layer --backend mmap --class message --size $$sz --consumers $(SIMPLE_BC_CONSUMERS) -n $(SIMPLE_MSGS) -w $(SIMPLE_WARMUP) --tree --timeout 120; \
		done; \
	done
	@echo "=== SIMPLE-SMOKE PASSED ==="

# ============================================================
# Tier 0: super-tiny (~5-10 min)
# Broad CI lane across all implemented scenario families.
# Counts: 100 warmup, 1000 measured messages/events.
# ============================================================
super-tiny:
	@echo "=== SUPER-TINY (broad bench gate, ~5-10 min) ==="
	@# Signal
	$(SG) --backend shm --consumers 1 --events $(SIMPLE_MSGS)
	$(SG) --backend mmap --consumers 1 --events $(SIMPLE_MSGS)
	@# Exact-size raw ping-pong
	@for sz in 32 64 128 1024 2048 4096; do \
		for layer in raw_ring raw_myelon; do \
			$(PP) --layer $$layer --backend shm  --size $$sz -n $(SIMPLE_MSGS) -w $(SIMPLE_WARMUP); \
			$(PP) --layer $$layer --backend mmap --size $$sz -n $(SIMPLE_MSGS) -w $(SIMPLE_WARMUP); \
			$(PP) --layer $$layer --backend shm  --size $$sz -n $(SIMPLE_MSGS) -w $(SIMPLE_WARMUP) --mode co --target-rate $(SIMPLE_CO_RATE); \
			$(PP) --layer $$layer --backend mmap --size $$sz -n $(SIMPLE_MSGS) -w $(SIMPLE_WARMUP) --mode co --target-rate $(SIMPLE_CO_RATE); \
		done; \
	done
	@# Representative higher-layer ping-pong
	@for backend in shm mmap; do \
		$(PP) --layer framed --backend $$backend -n $(SIMPLE_MSGS) -w $(SIMPLE_WARMUP); \
		$(PP) --layer framed --backend $$backend -n $(SIMPLE_MSGS) -w $(SIMPLE_WARMUP) --mode co --target-rate $(SIMPLE_CO_RATE); \
		$(PP) --layer codec --backend $$backend --codec bincode -n $(SIMPLE_MSGS) -w $(SIMPLE_WARMUP); \
		$(PP) --layer codec --backend $$backend --codec rkyv -n $(SIMPLE_MSGS) -w $(SIMPLE_WARMUP); \
		$(PP) --layer codec --backend $$backend --codec rkyv -n $(SIMPLE_MSGS) -w $(SIMPLE_WARMUP) --mode co --target-rate $(SIMPLE_CO_RATE); \
		$(PP) --layer typed_zc --backend $$backend --codec rkyv -n $(SIMPLE_MSGS) -w $(SIMPLE_WARMUP); \
		$(PP) --layer typed_zc --backend $$backend --codec rkyv -n $(SIMPLE_MSGS) -w $(SIMPLE_WARMUP) --mode co --target-rate $(SIMPLE_CO_RATE); \
	done
	@# Exact-size raw broadcast
	@for sz in 32 64 128 1024 2048 4096; do \
		for layer in raw_ring raw_myelon; do \
			$(BC) --layer $$layer --backend shm  --class message --size $$sz --consumers $(SIMPLE_BC_CONSUMERS) -n $(SIMPLE_MSGS) -w $(SIMPLE_WARMUP) --timeout 180; \
			$(BC) --layer $$layer --backend mmap --class message --size $$sz --consumers $(SIMPLE_BC_CONSUMERS) -n $(SIMPLE_MSGS) -w $(SIMPLE_WARMUP) --timeout 180; \
		done; \
	done
	@# Representative fixed-rate raw broadcast
	@for layer in raw_ring raw_myelon; do \
		$(BC) --layer $$layer --backend shm  --class message --size 1024 --consumers $(SIMPLE_BC_CONSUMERS) -n $(SIMPLE_MSGS) -w $(SIMPLE_WARMUP) --mode co --target-rate $(SIMPLE_CO_RATE) --timeout 180; \
		$(BC) --layer $$layer --backend mmap --class message --size 1024 --consumers $(SIMPLE_BC_CONSUMERS) -n $(SIMPLE_MSGS) -w $(SIMPLE_WARMUP) --mode co --target-rate $(SIMPLE_CO_RATE) --timeout 180; \
	done
	@# Representative higher-family broadcast
	@for backend in shm mmap; do \
		$(BC) --layer framed --backend $$backend --size 1024 --consumers $(SIMPLE_BC_CONSUMERS) -n $(SIMPLE_MSGS) -w $(SIMPLE_WARMUP) --timeout 180; \
		$(BC) --layer codec --backend $$backend --codec rkyv --consumers $(SIMPLE_BC_CONSUMERS) --batch-size 8 -n $(SIMPLE_MSGS) -w $(SIMPLE_WARMUP) --timeout 180; \
		$(BC) --layer codec --backend $$backend --codec rkyv --consumers $(SIMPLE_BC_CONSUMERS) --batch-size 8 -n $(SIMPLE_MSGS) -w $(SIMPLE_WARMUP) --mode co --target-rate $(SIMPLE_CO_RATE) --timeout 180; \
		PERF_BENCH_WAIT_NUM_EVENTS=$(SIMPLE_MSGS) $(BC) --layer wait_strategy --backend $$backend --timeout 180; \
	done
	@# Sweep and layout families (single representative point each)
	$(BC) --layer myelon_layers --backend mmap --size 1024 --consumers $(SIMPLE_BC_CONSUMERS) --num-messages $(SIMPLE_MSGS) --quick --timeout 180
	$(BC) --layer monster_sweep --backend shm --size 1024 --consumers $(SIMPLE_BC_CONSUMERS) --num-messages $(SIMPLE_MSGS) --quick --timeout 180
	$(BC) --layer monster_sweep --backend mmap --size 1024 --consumers $(SIMPLE_BC_CONSUMERS) --num-messages $(SIMPLE_MSGS) --quick --timeout 180
	$(BC) --layer framed_sweep --backend mmap --size 1024 --consumers $(SIMPLE_BC_CONSUMERS) --num-messages $(SIMPLE_MSGS) --quick --timeout 180
	$(BC) --layer typed_zc_sweep --backend mmap --size 1024 --consumers $(SIMPLE_BC_CONSUMERS) --num-messages $(SIMPLE_MSGS) --quick --timeout 180
	$(BC) --layer nofrag --backend mmap --size 1024 --consumers $(SIMPLE_BC_CONSUMERS) --num-messages $(SIMPLE_MSGS) --quick --timeout 180
	PERF_BENCH_LAYOUT_ITERATIONS=10 $(BC) --layer layout --timeout 180
	@echo "=== SUPER-TINY PASSED ==="

# ============================================================
# Tier 0: quick (~1-2 min)
# Endpoints only: signal + raw_ring + raw_myelon + typed_zc
# Sizes: 64B, 1KB | Consumers: 1 (pp), 4 (bc) | Wait: busyspin
# ============================================================
quick:
	@echo "=== QUICK (endpoints, 64B+1KB, ~1-2 min) ==="
	@# Signal
	$(SG) --backend shm --consumers 1 --events 1000000 --tree
	$(SG) --backend mmap --consumers 1 --events 1000000 --tree
	@# Pingpong: raw_ring × {shm,mmap} × {64B,1KB} × {throughput,CO}
	@for sz in 64 1024; do \
		$(PP) --layer raw_ring --backend shm  --size $$sz -n $(CORE_MSGS) -w $(CORE_WARMUP) --tree; \
		$(PP) --layer raw_ring --backend mmap --size $$sz -n $(CORE_MSGS) -w $(CORE_WARMUP) --tree; \
		$(PP) --layer raw_ring --backend shm  --size $$sz -n $(CORE_MSGS) -w $(CORE_WARMUP) --mode co --target-rate $(CO_RATE) --tree; \
		$(PP) --layer raw_ring --backend mmap --size $$sz -n $(CORE_MSGS) -w $(CORE_WARMUP) --mode co --target-rate $(CO_RATE) --tree; \
	done
	@# Pingpong: raw_myelon × {shm,mmap} × {64B,1KB} × {throughput,CO}
	@for sz in 64 1024; do \
		$(PP) --layer raw_myelon --backend shm  --size $$sz -n $(CORE_MSGS) -w $(CORE_WARMUP) --tree; \
		$(PP) --layer raw_myelon --backend mmap --size $$sz -n $(CORE_MSGS) -w $(CORE_WARMUP) --tree; \
		$(PP) --layer raw_myelon --backend shm  --size $$sz -n $(CORE_MSGS) -w $(CORE_WARMUP) --mode co --target-rate $(CO_RATE) --tree; \
		$(PP) --layer raw_myelon --backend mmap --size $$sz -n $(CORE_MSGS) -w $(CORE_WARMUP) --mode co --target-rate $(CO_RATE) --tree; \
	done
	@# Pingpong: typed_zc × {shm,mmap} × {rkyv,flatbuf} × {throughput,CO}
	@# NOTE: typed_zc uses preset codec batch specs (b1/b8/b64/b256), --size is ignored
	@for codec in rkyv flatbuf; do \
		$(PP) --layer typed_zc --backend shm  --codec $$codec -n $(CORE_MSGS) -w $(CORE_WARMUP) --tree; \
		$(PP) --layer typed_zc --backend mmap --codec $$codec -n $(CORE_MSGS) -w $(CORE_WARMUP) --tree; \
		$(PP) --layer typed_zc --backend shm  --codec $$codec -n $(CORE_MSGS) -w $(CORE_WARMUP) --mode co --target-rate $(CO_RATE) --tree; \
		$(PP) --layer typed_zc --backend mmap --codec $$codec -n $(CORE_MSGS) -w $(CORE_WARMUP) --mode co --target-rate $(CO_RATE) --tree; \
	done
	@# Broadcast: raw_ring × {shm,mmap} × 4c × {64B,1KB}
	$(BC) --layer raw_ring --backend shm  --class message --consumers 4 --tree --timeout 300
	$(BC) --layer raw_ring --backend mmap --class message --consumers 4 --tree --timeout 300
	@echo ""
	@echo "=== QUICK: release profile (baseline comparison) ==="
	$(SG_REL) --backend shm --consumers 1 --events 1000000 --tree
	@for sz in 64 1024; do \
		$(PP_REL) --layer raw_ring --backend shm --size $$sz -n $(CORE_MSGS) -w $(CORE_WARMUP) --tree; \
	done
	$(PP_REL) --layer typed_zc --backend shm --codec rkyv -n $(CORE_MSGS) -w $(CORE_WARMUP) --tree
	@echo "=== QUICK PASSED ==="

# ============================================================
# Tier 1: smoke (~5-10 min)
# Same layers as quick but 4 sizes
# Sizes: 32B, 64B, 1KB, 4KB | Wait: busyspin
# ============================================================
smoke:
	@echo "=== SMOKE (endpoints, 32B-4KB, ~5-10 min) ==="
	@# Signal
	$(SG) --backend shm --consumers 1 --events 1000000 --tree
	$(SG) --backend mmap --consumers 1 --events 1000000 --tree
	@# Pingpong: raw_ring + raw_myelon × {shm,mmap} × 4 sizes × {throughput,CO}
	@for sz in 32 64 1024 4096; do \
		for layer in raw_ring raw_myelon; do \
			$(PP) --layer $$layer --backend shm  --size $$sz -n $(CORE_MSGS) -w $(CORE_WARMUP) --tree; \
			$(PP) --layer $$layer --backend mmap --size $$sz -n $(CORE_MSGS) -w $(CORE_WARMUP) --tree; \
			$(PP) --layer $$layer --backend shm  --size $$sz -n $(CORE_MSGS) -w $(CORE_WARMUP) --mode co --target-rate $(CO_RATE) --tree; \
			$(PP) --layer $$layer --backend mmap --size $$sz -n $(CORE_MSGS) -w $(CORE_WARMUP) --mode co --target-rate $(CO_RATE) --tree; \
		done; \
	done
	@# Pingpong: typed_zc × {shm,mmap} × {rkyv,flatbuf} (preset codec batch specs, --size ignored)
	@for codec in rkyv flatbuf; do \
		$(PP) --layer typed_zc --backend shm  --codec $$codec -n $(CORE_MSGS) -w $(CORE_WARMUP) --tree; \
		$(PP) --layer typed_zc --backend mmap --codec $$codec -n $(CORE_MSGS) -w $(CORE_WARMUP) --tree; \
	done
	@# Broadcast
	$(BC) --layer raw_ring --backend shm  --class message --consumers 4 --tree --timeout 300
	$(BC) --layer raw_ring --backend mmap --class message --consumers 4 --tree --timeout 300
	@echo ""
	@echo "=== SMOKE: release profile (baseline comparison) ==="
	$(SG_REL) --backend shm --consumers 1 --events 1000000 --tree
	@for sz in 32 64 1024 4096; do \
		$(PP_REL) --layer raw_ring --backend shm --size $$sz -n $(CORE_MSGS) -w $(CORE_WARMUP) --tree; \
	done
	$(PP_REL) --layer typed_zc --backend shm --codec rkyv -n $(CORE_MSGS) -w $(CORE_WARMUP) --tree
	@echo "=== SMOKE PASSED ==="

# ============================================================
# Tier 2: large-only (~15-30 min)
# All layers, large payloads only
# Sizes: 64KB, 1MB, 8MB, 32MB | Wait: all 4
# ============================================================
large-only:
	@echo "=== LARGE-ONLY (64KB-32MB, all layers, ~15-30 min) ==="
	@# Pingpong: all layers × {shm,mmap} × large sizes
	@for sz in 65536 1048576 8388608 33554432; do \
		msgs=$(LARGE_MSGS); warmup=$(LARGE_WARMUP); \
		if [ $$sz -ge 8388608 ]; then msgs=$(HUGE_MSGS); warmup=$(HUGE_WARMUP); fi; \
		if [ $$sz -ge 33554432 ]; then msgs=$(GIANT_MSGS); warmup=$(GIANT_WARMUP); fi; \
		for layer in raw_ring raw_myelon framed; do \
			$(PP) --layer $$layer --backend shm  --size $$sz -n $$msgs -w $$warmup --tree --timeout 120; \
			$(PP) --layer $$layer --backend mmap --size $$sz -n $$msgs -w $$warmup --tree --timeout 120; \
		done; \
		for codec in bincode rkyv flatbuf; do \
			$(PP) --layer codec --backend shm  --codec $$codec --size $$sz -n $$msgs -w $$warmup --tree --timeout 120; \
			$(PP) --layer codec --backend mmap --codec $$codec --size $$sz -n $$msgs -w $$warmup --tree --timeout 120; \
		done; \
		for codec in rkyv flatbuf; do \
			$(PP) --layer typed_zc --backend shm  --codec $$codec --size $$sz -n $$msgs -w $$warmup --tree --timeout 120; \
			$(PP) --layer typed_zc --backend mmap --codec $$codec --size $$sz -n $$msgs -w $$warmup --tree --timeout 120; \
		done; \
	done
	@# Wait strategy
	$(BC) --layer wait_strategy --backend shm  --tree
	$(BC) --layer wait_strategy --backend mmap --tree
	@# Broadcast
	$(BC) --layer raw_ring --backend shm  --class message --consumers 4 --tree
	$(BC) --layer raw_ring --backend mmap --class message --consumers 4 --tree
	@echo "=== LARGE-ONLY PASSED ==="

# ============================================================
# Tier 3: extensive (~1-2 hrs)
# All layers, 8 sizes, multi-consumer, 3 CO rates
# ============================================================
extensive:
	@echo "=== EXTENSIVE (all layers, 8 sizes, multi-consumer, ~1-2 hrs) ==="
	@# Full pingpong matrix
	@for sz in 64 512 1024 16384 65536 524288 2097152 8388608; do \
		msgs=$(CORE_MSGS); warmup=$(CORE_WARMUP); \
		if [ $$sz -ge 524288 ]; then msgs=$(LARGE_MSGS); warmup=$(LARGE_WARMUP); fi; \
		if [ $$sz -ge 8388608 ]; then msgs=$(HUGE_MSGS); warmup=$(HUGE_WARMUP); fi; \
		for layer in raw_ring raw_myelon framed; do \
			$(PP) --layer $$layer --backend shm  --size $$sz -n $$msgs -w $$warmup --tree --timeout 120; \
			$(PP) --layer $$layer --backend mmap --size $$sz -n $$msgs -w $$warmup --tree --timeout 120; \
		done; \
		for codec in bincode rkyv flatbuf; do \
			$(PP) --layer codec --backend shm  --codec $$codec --size $$sz -n $$msgs -w $$warmup --tree --timeout 120; \
			$(PP) --layer codec --backend mmap --codec $$codec --size $$sz -n $$msgs -w $$warmup --tree --timeout 120; \
		done; \
		for codec in rkyv flatbuf; do \
			$(PP) --layer typed_zc --backend shm  --codec $$codec --size $$sz -n $$msgs -w $$warmup --tree --timeout 120; \
			$(PP) --layer typed_zc --backend mmap --codec $$codec --size $$sz -n $$msgs -w $$warmup --tree --timeout 120; \
		done; \
	done
	@# CO-aware with 3 rates
	@for rate in 200000 600000 1000000; do \
		$(PP) --layer raw_ring --backend shm --size 64 -n $(CORE_MSGS) -w $(CORE_WARMUP) --mode co --target-rate $$rate --tree; \
		$(PP) --layer typed_zc --backend shm --codec rkyv --size 64 -n $(CORE_MSGS) -w $(CORE_WARMUP) --mode co --target-rate $$rate --tree; \
	done
	@# Broadcast with multi-consumer
	@for consumers in 4 8; do \
		$(BC) --layer raw_ring --backend shm  --class message --consumers $$consumers --tree; \
		$(BC) --layer raw_ring --backend mmap --class message --consumers $$consumers --tree; \
	done
	@# Signal multi-consumer
	@for consumers in 1 4 8; do \
		$(SG) --backend shm --consumers $$consumers --events 1000000 --tree; \
	done
	@# Wait strategy all 4
	$(BC) --layer wait_strategy --backend shm  --tree
	$(BC) --layer wait_strategy --backend mmap --tree
	@# Sweeps
	$(BC) --layer myelon_layers --tree
	$(BC) --layer nofrag --tree
	@# Layout
	$(BC) --layer layout --tree
	@echo "=== EXTENSIVE PASSED ==="

# ============================================================
# Tier 4: full-blown (~6-12 hrs)
# Everything + repeatability
# ============================================================
full-blown:
	@echo "=== FULL-BLOWN (16 sizes, full consumer range, repeatability, ~6-12 hrs) ==="
	@echo "Run extensive first, then add 16-size ladder and repeatability."
	$(MAKE) extensive
	@# Additional sizes not in extensive
	@for sz in 128 2048 32768 131072 1048576 16777216 33554432 67108864; do \
		msgs=$(CORE_MSGS); warmup=$(CORE_WARMUP); \
		if [ $$sz -ge 524288 ]; then msgs=$(LARGE_MSGS); warmup=$(LARGE_WARMUP); fi; \
		if [ $$sz -ge 8388608 ]; then msgs=$(HUGE_MSGS); warmup=$(HUGE_WARMUP); fi; \
		if [ $$sz -ge 16777216 ]; then msgs=$(MASSIVE_MSGS); warmup=$(MASSIVE_WARMUP); fi; \
		if [ $$sz -ge 33554432 ]; then msgs=$(GIANT_MSGS); warmup=$(GIANT_WARMUP); fi; \
		for layer in raw_ring raw_myelon framed; do \
			$(PP) --layer $$layer --backend shm  --size $$sz -n $$msgs -w $$warmup --tree --timeout 300; \
			$(PP) --layer $$layer --backend mmap --size $$sz -n $$msgs -w $$warmup --tree --timeout 300; \
		done; \
		for codec in bincode rkyv flatbuf; do \
			$(PP) --layer codec --backend shm  --codec $$codec --size $$sz -n $$msgs -w $$warmup --tree --timeout 300; \
			$(PP) --layer codec --backend mmap --codec $$codec --size $$sz -n $$msgs -w $$warmup --tree --timeout 300; \
		done; \
		for codec in rkyv flatbuf; do \
			$(PP) --layer typed_zc --backend shm  --codec $$codec --size $$sz -n $$msgs -w $$warmup --tree --timeout 300; \
			$(PP) --layer typed_zc --backend mmap --codec $$codec --size $$sz -n $$msgs -w $$warmup --tree --timeout 300; \
		done; \
	done
	@# CO 5-rate sweep
	@for rate in 200000 400000 600000 800000 1000000; do \
		$(PP) --layer raw_ring --backend shm --size 64 -n $(CORE_MSGS) -w $(CORE_WARMUP) --mode co --target-rate $$rate --tree; \
		$(PP) --layer typed_zc --backend shm --codec rkyv --size 64 -n $(CORE_MSGS) -w $(CORE_WARMUP) --mode co --target-rate $$rate --tree; \
	done
	@# Full signal consumer range
	@for consumers in 1 2 4 6 8 12; do \
		$(SG) --backend shm --consumers $$consumers --events 1000000 --tree; \
	done
	@echo "=== FULL-BLOWN PASSED ==="

# ============================================================
# Individual layer targets (convenience)
# ============================================================
raw:
	$(BC) --layer raw_ring --backend shm  --tree
	$(BC) --layer raw_ring --backend mmap --tree

framed:
	$(BC) --layer framed --backend shm  --tree
	$(BC) --layer framed --backend mmap --tree

codec:
	$(BC) --layer codec --backend shm  --codec rkyv --tree
	$(BC) --layer codec --backend mmap --codec rkyv --tree

wait:
	$(BC) --layer wait_strategy --backend shm  --tree
	$(BC) --layer wait_strategy --backend mmap --tree

layers:
	$(BC) --layer myelon_layers --tree

nofrag:
	$(BC) --layer nofrag --tree

sweep:
	$(BC) --layer monster_sweep --backend shm  --tree
	$(BC) --layer monster_sweep --backend mmap --tree

layout:
	$(BC) --layer layout --tree

clean:
	rm -rf output/
