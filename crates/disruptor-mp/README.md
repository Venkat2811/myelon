# disruptor-mp

Multiprocess shared-memory ring buffer support for Disruptor-style publication.

This crate is intentionally **multiprocess-only**.
It no longer vendors/copypastes single-process internals from `disruptor-rs`.

## Boundary

- `disruptor-mp` owns:
  - shared-memory lifecycle, producer/consumer coordination, and multiprocess benchmarks/tests via stable namespaces:
    - `disruptor_mp::shared_memory`
    - `disruptor_mp::lock_free`
    - `disruptor_mp::backend`
  - high-level constructors and types:
    - `attach_shared_consumer`
    - `build_shared_single_producer`
    - `SharedProducer`, `SharedConsumer`, `SharedCursor`, `SharedRingBuffer`, `ShmRingBuffer`
    - `CoordinationMode`, `DiscoveryMode`, `ConsumerBarrier`, `ProducerBarrier`
- crates.io `disruptor` owns:
  - single-process/threaded disruptor APIs (`build_single_producer`, `build_multi_producer`, wait strategies, pollers).

See `the workspace book` for migration details.
See `the workspace book` for shared-memory layout versioning rules.
See `the workspace book` for Linux CPU affinity controls and benchmark usage.
See `docs/MAKE_TARGETS.md` for the included Makefile fragment layout and runtime tiers.
See `the workspace book` for current Linux core-to-core optimization measurements.

## Dependency Model

Internally this crate depends on crates.io `disruptor` as:

```toml
[dependencies]
disruptor_core = { package = "disruptor", version = "3.7.1" }
```

## Quick Start (Multiprocess)

```toml
[dependencies]
disruptor = { package = "disruptor-mp", version = "3.7.1" }
```

```rust,no_run
use disruptor_mp::build_shared_single_producer;
use disruptor_mp::shared_memory::ShmRingBuffer;

#[derive(Copy, Clone, Default)]
struct Event {
    value: i64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _type_hint: Option<ShmRingBuffer<Event>> = None;

    let mut producer = build_shared_single_producer::<Event>("mp_ring", 1024)
        .build_producer(Event::default)?;

    producer.publish(|e| {
        e.value = 42;
    });

    Ok(())
}
```

## Public Namespaces

- `disruptor_mp::shared_memory::{SharedRingBuffer, ShmRingBuffer, SharedMemoryConfig}`
- `disruptor_mp::lock_free::{SharedCursor, ConsumerBarrier, ProducerBarrier, DiscoveryMode}`
- `disruptor_mp::{CoordinationMode, DiscoveryMode, SharedProducer, SharedConsumer}`
- `disruptor_mp::backend::shared_memory::*` (same API, backend-oriented path)

## API Stability

- Stable API is `disruptor_mp::{...}` namespaces and explicit re-exports documented above.
- Internal modules (`builder`, `producer`, `consumer`, `ringbuffer`, `cursor`, `wait`) are intentionally private.
- Breaking changes to public surface are tracked by kanban card and include migration notes.
- Internal refactors that do not change the public surface are not part of this compatibility policy.

## Platform Support

- Linux: officially supported.
- macOS: known to work for core multiprocess flows, but currently unsupported for official guarantees.
- Windows: explicitly unsupported.

## Discovery Contract

- Prefer `discover_consumer_with_prefix(...)` for fixed topologies that need deterministic naming.
- `enable_discovery(n)` now uses coordination-backed slot IDs (`ad_0`, `ad_1`, ...) when startup coordination is active.
- Legacy PID-based scanning remains available only as a best-effort fallback for non-coordinated or older flows and should not be treated as deterministic under child-process churn.

## Development

```bash
make drift-check
make test-linux
make test-linux-extended ITERATIONS=10
make test-manifest-json \
  TEST_MANIFEST_OUT=/tmp/disruptor_mp_test_manifest.json
make test-stress-report-json \
  ITERATIONS=50 \
  STRESS_REPORT_OUT=/tmp/disruptor_mp_test_stress.json \
  STRESS_LOG_DIR=/tmp/disruptor_mp_test_stress_logs
make test-mmap-stress-report-json \
  ITERATIONS=5 \
  STRESS_REPORT_OUT=/tmp/disruptor_mp_mmap_stress.json \
  STRESS_LOG_DIR=/tmp/disruptor_mp_mmap_stress_logs
make bench-multiprocess
make bench-affinity-matrix
make bench-r10-ab
```

## Canonical Test Lanes

- `test-unit`: crate library tests plus doctests
- `test-integration`: API namespace, DST contract/runtime/profile, layout, and shared-memory lifecycle tests
- `test-multiprocess`: true child-process producer/consumer and deadlock regression tests
- `test-stress`: repeated cleanup/lifecycle lane (`ITERATIONS` controls depth)
- `test-stress-report-json`: same lane, with explicit JSON artifact path via `STRESS_REPORT_OUT`
- `test-mmap-stress-report-json`: repeated true-multiprocess mmap stress matrix with archived flake logs
- `test-perf-smoke`: compile benchmark/example surfaces and run alignment validation helper
- `test-linux`: default Linux Rust validation gate
- `test-linux-extended`: Linux gate plus `test-stress` and `test-perf-smoke`
- `test-macos-smoke`: cross-platform-safe subset while macOS remains unsupported for guarantees
- `test-manifest-json`: emit the machine-readable lane manifest used by workspace orchestration/CI tooling

`test-manifest-json` writes JSON to `TEST_MANIFEST_OUT` and includes lane budgets, platform scope,
expected exclusions, and the exact commands rendered from the current `Makefile`.
`test-stress-report-json` writes per-iteration pass/fail data, failure fingerprints, and retained
log paths to `STRESS_REPORT_OUT`; failing iterations always keep a log under `STRESS_LOG_DIR`.

## Machine-Readable Benchmark Artifacts

`ipc_shm` supports JSON-first output for reproducible perf artifacts.

```bash
COMPETITOR=disruptor \
make bench-ipc-json \
  BENCHMARK_JSON_OUT=/tmp/disruptor_mp_ipc_shm.json

COMPETITOR=disruptor \
make bench-ipc-json-validate \
  BENCHMARK_JSON_OUT=/tmp/disruptor_mp_ipc_shm.json

COMPETITOR=disruptor-broadcast-12 \
make bench-high-load-json-validate \
  BENCHMARK_JSON_OUT=/tmp/disruptor_mp_ipc_shm_high_load.json

make bench-wait-strategies-json-validate \
  WAIT_STRATEGIES_MODE=limited \
  BENCHMARK_JSON_OUT=/tmp/disruptor_mp_wait_strategies.json

make bench-competitive-json-validate \
  COMPETITIVE_MESSAGE_SIZE=64 \
  COMPETITIVE_NUM_MESSAGES=20000 \
  COMPETITIVE_WARMUP_MESSAGES=2000 \
  BENCHMARK_JSON_OUT=/tmp/disruptor_mp_competitive_pingpong.json

make bench-competitor-json-validate \
  COMPETITOR_COMPETITOR=rust-1p1c-test \
  BENCHMARK_JSON_OUT=/tmp/disruptor_mp_competitor.json

python3 -m pip install -r crates/disruptor-mp/benches/competitor/requirements.txt
make bench-competitor-json-validate \
  COMPETITOR_COMPETITOR=compare-1p1c-test \
  BENCHMARK_JSON_OUT=/tmp/disruptor_mp_competitor_compare.json

make benchmark-summary-json \
  BENCHMARK_SUMMARY_OUT=/tmp/disruptor_mp_summary.json \
  BENCHMARK_SUMMARY_INPUTS="/tmp/disruptor_mp_ipc_shm.json /tmp/disruptor_mp_competitor_compare.json"
```

The JSON report includes:

- normalized `benchmark_id`, emitted `benchmark_name`, and competitor selector
- OS, architecture, kernel release, CPU count, CPU model, package version, and git commit
- relevant affinity/output and benchmark-shaping environment overrides
- result-level `library_id`, `scenario_id`, event count, payload size, and buffer size
- per-library producer/consumer throughput, data rate, total time, deterministic `buffer_memory_bytes`, and latency `p50`/`p95`/`p99` when the benchmark collects latency

`bench-ipc-json-validate` runs the benchmark and fails if the JSON artifact is missing
required metadata, has non-positive metrics, or reports invalid latency quantiles.
`competitive_pingpong` uses `--json-canonical` for this artifact path so the older
competitive `--json` output remains available for legacy helper scripts until those
consumers are migrated.
`competitor_vs_rust_benchmark` now emits the same canonical artifact format for the Rust
orchestrator lane; the fast validation target defaults to `COMPETITOR_COMPETITOR=rust-1p1c-test`
so the JSON contract stays cheap to exercise on Linux before the broader mixed Rust/Python
comparison lanes are hardened.
Mixed Rust/Python Competitor comparison lanes require `numpy` in the system interpreter; use
`make check-competitor-python-deps` or install from
`crates/disruptor-mp/benches/competitor/requirements.txt` before running `compare-*` or
`shorter-run` JSON targets.
`benchmark-summary-json` merges one or more canonical benchmark artifacts into a single
summary report so a benchmark run can be archived as one machine-readable bundle instead of
loosely related per-benchmark files.
