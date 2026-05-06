# examples

Workspace-level runnable examples for [`myelon`](../crates/myelon/) — the simplified façade for [`disruptor-mp`](../crates/disruptor-mp/)'s core capabilities (Layer 0) plus framing, codecs, typed zero-copy, and topology on top.

Two dependency profiles are demonstrated side-by-side:

- **Via `myelon` (default for most users)** — single dep, full Layer 0 + Layers 1–3 + topology + observability surface reachable through `myelon::*`. `shm_disruptor`, `mmap_disruptor`, `pingpong`, `counters`, and `fixed_inference_topology` use this profile.
- **Direct on `disruptor-mp` (substrate-only)** — depend on `disruptor-mp` alone when you don't want the framing / codec / typed-zero-copy / topology surface compiled into your binary. `disruptor_mp_shm` and `disruptor_mp_mmap` are templates for that profile.

Same multiprocess pattern, same correctness primitives, same runtime behaviour — only the import paths and the `Cargo.toml` dependency choice differ.

> **Everything here is multiprocess.** The crate name is `disruptor-mp` and the **mp** is not a suggestion. Every example spawns its peer(s) as real OS child processes via `current_exe()` plus an env-var role dispatch (see `src/lib.rs`); none of them simulate multiprocess behavior with threads.

## What's here

| Example | What it shows | Imports through |
|---|---|---|
| [`shm_disruptor.rs`](shm_disruptor.rs) | Layer 0 quick start over a POSIX shared-memory segment. 1 producer + 1 consumer in two real OS processes. | `myelon::*` |
| [`mmap_disruptor.rs`](mmap_disruptor.rs) | Same shape as `shm_disruptor`, backed by a memory-mapped file. Region survives reboots, no macOS `PSHMNAMLEN` (31-byte) ceiling. | `myelon::*` |
| [`disruptor_mp_shm.rs`](disruptor_mp_shm.rs) | Same shape as `shm_disruptor`, but with `disruptor-mp` as a direct dependency (substrate-only profile). | `disruptor_mp::*` |
| [`disruptor_mp_mmap.rs`](disruptor_mp_mmap.rs) | Same shape as `mmap_disruptor`, but with `disruptor-mp` as a direct dependency. | `disruptor_mp::*` |
| [`pingpong.rs`](pingpong.rs) | Multiprocess request/response RTT. Two SHM rings, parent measures end-to-end round-trip latency. | `myelon::*` |
| [`counters.rs`](counters.rs) | RFC-0040 hot-path observability end-to-end through the `myelon` re-export of `disruptor_mp::observability`. | `myelon::*` |
| [`fixed_inference_topology.rs`](fixed_inference_topology.rs) | One scheduler / N workers (2..=8) topology with discovery and rendezvous baked in via `myelon::FixedTopology`. | `myelon::*` |

## Run them

```bash
cargo run --release -p examples --example shm_disruptor
cargo run --release -p examples --example mmap_disruptor
cargo run --release -p examples --example disruptor_mp_shm
cargo run --release -p examples --example disruptor_mp_mmap
cargo run --release -p examples --example pingpong
cargo run --release -p examples --example counters
cargo run --release -p examples --example fixed_inference_topology
```

Always use `--release` for representative numbers.

## When you outgrow the examples

Examples deliberately stay small — single happy-path, single-producer, single-consumer (or the topology shape the example is named after). When you need to exercise the full surface, drop into the bench harnesses:

### `crates/perf-bench/` — broad internal sweep

Covers what the examples don't:

- **All four layers** — raw ring, framed, codec (bincode / rkyv / flatbuffers), typed zero-copy.
- **Both backends** — `--backend shm` and `--backend mmap`.
- **All wait strategies** — `--wait-strategy busyspin | spinloop | sleep | block` (RFC 0017.5 §8 covers the cost model).
- **Coordination modes** — implicit through the per-scenario CLI; internal benchmarks use `UnifiedCoordination` (single cache-line-padded SHM segment), external benches use `BenchmarkCoordination` (multi-cursor pattern). Both modes are exercised side-by-side.
- **Three measurement modes** — max-throughput, fixed-rate coordinated-omission-aware, low-overhead batch-timing.
- **Required-consumer liveness** — pass `--enable-counters` for the RFC-0040 observability path; `--liveness on|off` is wired for parity benchmarks.
- **Fragmentation** — `--frag` / `--nofrag` to compare 64KB-slot multi-frame messages vs right-sized single-frame messages.
- **Per-process latency histograms + counters** through the `metrics-rs` facade.

```bash
# Layer 0 raw ring, SHM, max throughput
cargo run --release -p perf-bench --bin perf-bench-pingpong -- \
    --layer raw_ring --backend shm

# Codec layer with rkyv + counters on
cargo run --release -p perf-bench --bin perf-bench-pingpong -- \
    --layer codec --backend shm --codec rkyv --enable-counters
```

See [`crates/perf-bench/README.md`](../crates/perf-bench/README.md) for the binary inventory, layer / backend / mode matrix, and file/directory structure.

### `crates/competitive-bench/` — apples-to-apples external comparison

Strict 1p1c ping-pong and 1p4c / 1p8c broadcast against external transports: `crossbar`, `shmipc`, `rusteron` (Aeron client), `iceoryx2`, `zmq`, `iggy`, `redpanda`. Internal `disruptor-mp` and `myelon` raw-ring lanes serve as the baseline.

See [`crates/competitive-bench/README.md`](../crates/competitive-bench/README.md).

## How the multiprocess wiring works

Each example follows the same shape via the tiny helper in [`src/lib.rs`](src/lib.rs):

```rust
fn main() -> Result<(), Box<dyn std::error::Error>> {
    if let Some(role) = examples::child_role() {
        // Re-entered as a child process; dispatch on the role.
        return run_child(&role);
    }
    // Original parent process: spawn child(ren) with
    // `examples::spawn_self("role", segment)` and run the parent path.
    run_parent()
}
```

Children inherit stdout/stderr so you see one unified log. The parent renders all SHM segment names *once* (via `portable_shm_segment_name`, which adds a per-call salt) and passes them to children verbatim through the env so both sides agree on the exact strings.
