# myelon

`myelon` is an extreme low-latency, high-throughput, high-performance inference fabric.

This monorepo is the Linux-first workspace for the `myelon` Rust façade, Python bindings,
and the lower-level multiprocess transport it builds on.

## Layout

- `crates/disruptor-mp`: low-level multiprocess shared-memory disruptor core.
- `crates/legacy-wip`: `myelon` Rust inference-fabric façade.
- `python-surface-archive`: Python bindings and integrations for `myelon`.

## Architecture Boundaries

| Layer | Owner | Purpose |
|:------|:------|:--------|
| `crates/disruptor-mp` | low-level data plane | shared-memory layout, lock-free coordination, producer/consumer primitives |
| `crates/legacy-wip` | `myelon` Rust layer | inference-fabric API, stable Rust-facing surface, and future topology/domain policy |
| `python-surface-archive/src` + `python/disruptor_rs/multiprocess.py` | Python data plane | PyO3 bridge and raw producer/consumer operations |
| `python-surface-archive/python/disruptor_rs/__init__.py`, `compat.py`, `legacy.py`, `external_integrations/` | Python control plane | stable imports, compatibility shims, and framework adapters |

Boundary rules and enforced checks live in the workspace book.

## Status

- Linux: priority target
- macOS: works for core paths, currently unsupported for official release guarantees
- Windows: unsupported

## Competitive Bench

`crates/competitive-bench` is the narrow apples-to-apples transport comparison harness.
It owns the competitive ping-pong surface and durable result bundles for:

- raw internal baselines over `shm` and `mmap`
- external peers such as `crossbar`, `shmipc-rs`, `iceoryx2`, `boost`, `ompi`, `rusteron`, and `zeromq`
- Docker-managed broker peers for quick TCP comparison:
  - `iggy-tcp`
  - `redpanda-kafka`
- protocol-separated aggregate tables and SVG frontier plots

Start there when you want local IPC or transport-comparison numbers without triggering the full
internal `perf-bench` matrix. See:

- `crates/competitive-bench/README.md`
- `crates/competitive-bench/SETUP.md`

## One-Command Workflows

- Competitive exact-size smoke:
  - `make -C crates/competitive-bench simple-smoke`
- Internal exact-size smoke:
  - `make -C crates/perf-bench simple-smoke`
- Fast benchmark smoke (~60s):
  - `make smoke`
- Workspace wiring + crate boundary checks:
  - `make workspace-smoke`
- Rust-tier orchestration (format/lint/tests/bench+example compile checks):
  - `make orchestrate-rust`
- Python-tier orchestration:
  - `make orchestrate-python`
- Full monorepo orchestration:
  - `make orchestrate-all`

## Rust Fixed Topology

`crates/legacy-wip` now exposes a first domain-layer topology API for
fixed scheduler-to-worker pools:

```rust
use myelon::inference::{FixedTopology, WorkerCount};
use std::time::Duration;

#[derive(Copy, Clone, Default)]
struct InferenceEvent {
    token_id: u32,
    worker_id: u16,
    end_of_batch: bool,
}

let topology = FixedTopology::new("infer_demo", 1024, WorkerCount::Three)
    .with_coordination_timeout(Duration::from_secs(5));

let _scheduler_builder = topology.scheduler_builder::<InferenceEvent>();
for worker_index in topology.worker_indices() {
    let _worker_builder = topology.worker_builder::<InferenceEvent>(worker_index)?;
}
```

This keeps the domain crate thin:

- worker counts are typed (`WorkerCount::Two` through `WorkerCount::Eight`)
- coordination timeout stays explicit
- worker ids are generated and validated by the topology wrapper
- underlying producer/consumer handles still come from the canonical `disruptor-mp` API

## Platform Policy

- Linux is the only officially supported platform for this monorepo.
- macOS is exercised and expected to work for primary multiprocess paths, but is not an officially supported target.
- Windows is explicitly not supported.
