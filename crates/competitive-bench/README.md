# competitive-bench

> **Internal.** Not published to crates.io. Owns the narrow apples-to-apples external transport comparison surface for [`disruptor-mp`](../disruptor-mp/) and [`myelon`](../myelon/). The broad internal sweep universe lives in [`crates/perf-bench`](../perf-bench/).

The contract: every adapter runs the same `1p1c` ping-pong (and `1p4c` / `1p8c` broadcast) on the same payload ladder, in the same modes, emits the same JSON schema, so the resulting numbers are comparable across transports without methodology footnotes.

## Scope

Current competitive families:

- signal
  - internal raw-ring only
  - SHM + mmap
  - exposed through `super-tiny`
- `1p1c` ping-pong
  - max-throughput mode
  - fixed-rate coordinated-omission-aware mode
- broadcast
  - `1p4c`
  - `1p8c`
  - max-throughput mode
  - fixed-rate coordinated-omission-aware mode
- raw internal baselines
  - `disruptor-shm`
  - `disruptor-mmap`
  - `myelon-raw-shm`
  - `myelon-raw-mmap`
- external peers
  - `crossbar-channel`
  - `crossbar-pubsub`
  - `shmipc-rs`
  - `iceoryx2-shm`
  - `boost-message-queue`
  - `ompi-vader-self`
  - `rusteron-aeron-ipc`
  - `zeromq-ipc`
  - `zeromq-ipc-abs`
  - `zeromq-tcp`

Size ladders:

- simple-smoke ladder:
  - `32`, `64`, `128`, `1KB`, `2KB`, `4KB`
- super-tiny fixed-rate lane:
  - one representative CO-aware rate per scenario: `50K/s`
- default parity ladder:
  - `64`, `512`, `1024`, `2048`, `2MB`
- extensive ladder:
  - `16KB`, `32KB`, `64KB`, `128KB`, `512KB`, `1MB`, `8MB`, `16MB`, `32MB`, `64MB`

The default run stays narrow and fast enough for regular parity work. The extensive ladder is opt-in and targets large-object analysis.

## What This Crate Does Not Do

- It does not run `perf-bench results`.
- It does not run framed / typed / zero-copy internal matrices.
- It does not mix competitive ping-pong with broader internal sweep reporting.

Use `crates/perf-bench` when you want the full internal benchmark platform.

## Layout

```
crates/competitive-bench/
├── Cargo.toml
├── Makefile                 # thin layer over competitive-bench-runner
├── README.md, SETUP.md
├── config/
│   └── parity.mk            # default + extensive size/rate/count policy
│                            # (loaded by infra::parity at compile time)
├── output/                  # local-only result bundles (gitignored)
├── patches/                 # workspace cargo patches
├── src/
│   ├── lib.rs               # pub mod adapters, infra, runner
│   ├── bin/                 # entry points -- thin 3-line wrappers
│   │   ├── competitive_bench_runner.rs   (the orchestrator)
│   │   ├── crossbar_pingpong.rs          → adapters::crossbar::pingpong
│   │   ├── crossbar_broadcast.rs         → adapters::crossbar::broadcast
│   │   ├── iceoryx2_pingpong.rs          → adapters::iceoryx2::pingpong
│   │   ├── internal_broadcast.rs         → adapters::internal::broadcast
│   │   ├── rusteron_pingpong.rs          → adapters::rusteron::pingpong
│   │   ├── shmipc_pingpong.rs            → adapters::shmipc::pingpong
│   │   └── zmq_pingpong.rs               → adapters::zmq::pingpong
│   ├── runner/              # cross-adapter orchestration
│   │   ├── cli.rs           CLI surface
│   │   ├── dispatch.rs      adapter -> ExecutionStrategy
│   │   ├── executor.rs      spawn/collect/cleanup
│   │   └── platform.rs      OS-specific paths (shm, aeron env)
│   ├── infra/               # cross-adapter library code
│   │   ├── adapter.rs       AdapterId / Origin / parity registry
│   │   ├── parity.rs        ParityConfig + per-size message-count tuning
│   │   ├── pingpong.rs      shared protocol helpers (control, pacing)
│   │   └── result_json.rs   JSON output schema
│   └── adapters/            # per-IPC-library implementations
│       ├── crossbar/{pingpong,broadcast}.rs
│       ├── iceoryx2/pingpong.rs
│       ├── internal/broadcast.rs        # disruptor + myelon raw broadcast
│       ├── rusteron/pingpong.rs
│       ├── shmipc/pingpong.rs
│       └── zmq/pingpong.rs
└── third_party/             # pinned crate-local peer source trees
    ├── boost_pingpong/      # C++ (Boost.Interprocess message_queue)
    ├── crossbar/            # vendored Rust crate
    └── ompi_pingpong/       # C with OpenMPI
```

### Why this layout

- **`adapters/<peer>/`** is the boundary. Every external transport has its own subdir under `adapters/` and exposes the same pingpong / broadcast contract. A new transport is one new directory plus a `[[bin]]` entry — nothing else changes.
- **`bin/` files are 3-line wrappers** that hand off to the matching `adapters::*` module. Keeping the dispatch in `lib.rs` means the adapters are testable without going through `cargo run`.
- **`infra/` holds cross-adapter library code** so the JSON schema, size ladder, pacing helpers, and the `AdapterId` registry have exactly one source of truth. Adapters call into `infra`, never the other way around.
- **`runner/` is the single dispatch point.** `competitive-bench-runner` reads tier configs from `config/parity.mk`, resolves adapter→strategy via `dispatch.rs`, and uses `executor.rs` to spawn / monitor / collect / cleanup. Adapters don't know about tiers — only about their own scenario.
- **`third_party/` is for source-pinned peers only**, not for Cargo-managed peers. See [Why `third_party/` Only Has Three Peers](#why-third_party-only-has-three-peers) below.

Shared event types and competitor reference data come from [`perf_bench::bench_support`](../perf-bench/src/bench_support/) so internal `disruptor-mp` / `myelon` baselines and external adapters all see the same `BenchmarkEvent<SIZE>` on the wire.

Result aggregation / pareto plotting is not in-tree -- the runner emits JSON per run and the JSON is the source of truth.

## Why `third_party/` Only Has Three Peers

Current crate-local source trees:

- `third_party/crossbar`
- `third_party/boost_pingpong`
- `third_party/ompi_pingpong`

The rest are intentionally not vendored here:

- `shmipc-rs`
  - built from Cargo crates
- `iceoryx2`
  - built from Cargo crates
- `rusteron`
  - built from Cargo crates
- `zeromq`
  - adapter is our Rust code, transport library comes from system `libzmq`

Rule:

- vendor or pin source only when exact local source materially affects reproducibility
- keep Cargo-managed peers Cargo-managed unless source pinning becomes necessary
- keep system-managed dependencies system-managed when that is the simpler and more honest setup

## Build and Run

Typical flow (from `crates/competitive-bench`):

```bash
make help            # show all available targets
make build-all       # cargo + boost C++ + ompi C
make simple-smoke    # ultra-quick sanity sweep
make super-tiny      # broad fast CI lane: signal + pingpong + broadcast
make quick           # core sizes (64B-2MB), all 14 adapters,
                     # throughput + fixed-rate (CO) + broadcast
make headon-smoke    # disruptor-shm vs rusteron-aeron-ipc
```

Larger sweeps:

```bash
make extensive       # 16KB-64MB sizes, all 14 adapters
make headon-extensive
```

Direct runner invocation (skip the Makefile when iterating):

```bash
cargo run -p competitive-bench --profile competitive \
    --bin competitive_bench_runner -- --help
```

The runner is the single source of dispatch logic; the Makefile is a thin convenience layer that knows about build prerequisites and tier shorthands.

## Result Layout

Per-run JSON files are written under `--outdir` (default `output/results/`, or `output/headon/` for headon tiers). One JSON file per (adapter, size, mode) tuple. Each file carries:

- `adapter`, `family` (`pingpong` / `broadcast`)
- `config` (size, message count, warmup, wait strategy, consumers)
- `throughput`, `messages_processed`, `duration_secs`
- `latency_stats` -- 12 percentiles (P1, P10, P25, P50, P75, P90, P95, P99, P99.9, P99.99, P99.999, P99.9999)
- `measurement_mode` (`max_throughput` | `fixed_rate`)
- `target_rate` and `coordinated_omission_stats` when the run used fixed-rate CO mode
- `consumer_count` for broadcast runs

Outputs are local-only -- the `output/` directory is gitignored.

## Setup

See `SETUP.md` for:

- submodule initialization
- system package prerequisites
- how each peer is built
- how each peer is executed
- output, cleanup, and large-object run behavior
