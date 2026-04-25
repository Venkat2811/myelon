# perf-bench Makefile — 5-tier benchmark runner
#
# Tiers (RFC 0026):
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

.PHONY: quick smoke large-only extensive full-blown \
	raw framed codec wait layers nofrag sweep layout \
	clean

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
	@# Pingpong: typed_zc × {shm,mmap} × {rkyv,flatbuf} × {64B,1KB} × {throughput,CO}
	@for sz in 64 1024; do for codec in rkyv flatbuf; do \
		$(PP) --layer typed_zc --backend shm  --codec $$codec --size $$sz -n $(CORE_MSGS) -w $(CORE_WARMUP) --tree; \
		$(PP) --layer typed_zc --backend mmap --codec $$codec --size $$sz -n $(CORE_MSGS) -w $(CORE_WARMUP) --tree; \
		$(PP) --layer typed_zc --backend shm  --codec $$codec --size $$sz -n $(CORE_MSGS) -w $(CORE_WARMUP) --mode co --target-rate $(CO_RATE) --tree; \
		$(PP) --layer typed_zc --backend mmap --codec $$codec --size $$sz -n $(CORE_MSGS) -w $(CORE_WARMUP) --mode co --target-rate $(CO_RATE) --tree; \
	done; done
	@# Broadcast: raw_ring × {shm,mmap} × 4c × {64B,1KB}
	$(BC) --layer raw_ring --backend shm  --class message --consumers 4 --tree --timeout 300
	$(BC) --layer raw_ring --backend mmap --class message --consumers 4 --tree --timeout 300
	@echo ""
	@echo "=== QUICK: release profile (baseline comparison) ==="
	$(SG_REL) --backend shm --consumers 1 --events 1000000 --tree
	@for sz in 64 1024; do 		$(PP_REL) --layer raw_ring --backend shm --size $$sz -n $(CORE_MSGS) -w $(CORE_WARMUP) --tree; 		$(PP_REL) --layer typed_zc --backend shm --codec rkyv --size $$sz -n $(CORE_MSGS) -w $(CORE_WARMUP) --tree; 	done
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
	@# Pingpong: raw_ring + raw_myelon + typed_zc × {shm,mmap} × 4 sizes × {throughput,CO}
	@for sz in 32 64 1024 4096; do \
		for layer in raw_ring raw_myelon; do \
			$(PP) --layer $$layer --backend shm  --size $$sz -n $(CORE_MSGS) -w $(CORE_WARMUP) --tree; \
			$(PP) --layer $$layer --backend mmap --size $$sz -n $(CORE_MSGS) -w $(CORE_WARMUP) --tree; \
			$(PP) --layer $$layer --backend shm  --size $$sz -n $(CORE_MSGS) -w $(CORE_WARMUP) --mode co --target-rate $(CO_RATE) --tree; \
			$(PP) --layer $$layer --backend mmap --size $$sz -n $(CORE_MSGS) -w $(CORE_WARMUP) --mode co --target-rate $(CO_RATE) --tree; \
		done; \
		for codec in rkyv flatbuf; do \
			$(PP) --layer typed_zc --backend shm  --codec $$codec --size $$sz -n $(CORE_MSGS) -w $(CORE_WARMUP) --tree; \
			$(PP) --layer typed_zc --backend mmap --codec $$codec --size $$sz -n $(CORE_MSGS) -w $(CORE_WARMUP) --tree; \
		done; \
	done
	@# Broadcast
	$(BC) --layer raw_ring --backend shm  --class message --consumers 4 --tree --timeout 300
	$(BC) --layer raw_ring --backend mmap --class message --consumers 4 --tree --timeout 300
	@echo ""
	@echo "=== SMOKE: release profile (baseline comparison) ==="
	$(SG_REL) --backend shm --consumers 1 --events 1000000 --tree
	@for sz in 32 64 1024 4096; do 		$(PP_REL) --layer raw_ring --backend shm --size $$sz -n $(CORE_MSGS) -w $(CORE_WARMUP) --tree; 		$(PP_REL) --layer typed_zc --backend shm --codec rkyv --size $$sz -n $(CORE_MSGS) -w $(CORE_WARMUP) --tree; 	done
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
