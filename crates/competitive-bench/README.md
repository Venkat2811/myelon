# competitive-bench

`competitive-bench` is the narrow external-comparison harness for `myelon`.
It owns the apples-to-apples transport comparison surface. It does not own the broad
internal sweep universe in `crates/perf-bench`.

## Scope

Current competitive families:

- signal
  - internal raw-ring only
  - SHM + mmap
  - exposed through `simple-smoke`
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
  - `iggy-tcp`
  - `redpanda-kafka`

Size ladders:

- simple-smoke ladder:
  - `32`, `64`, `128`, `1KB`, `2KB`, `4KB`
- default parity ladder:
  - `64`, `512`, `1024`, `2048`, `2MB`
- extensive ladder:
  - `16KB`, `32KB`, `64KB`, `128KB`, `512KB`, `1MB`, `8MB`, `16MB`, `32MB`, `64MB`

The default run stays narrow and fast enough for regular parity work. The extensive ladder is
opt-in and targets large-object analysis.

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

Result aggregation / pareto plotting is not in-tree -- the runner emits
JSON per run and the JSON is the source of truth.

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
- `iggy`
  - Docker-managed broker peer
- `redpanda`
  - Docker-managed broker peer

Broker peers are intentionally minimal:

- sizes:
  - `64B`
  - `1KB`
- modes:
  - max throughput
  - fixed-rate CO at `1K/s`, `3K/s`, and `5K/s`

Override `BROKER_RATES` if you explicitly want an overload run instead of the default below-saturation ladder.

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

The runner is the single source of dispatch logic; the Makefile is a
thin convenience layer that knows about build prerequisites and tier
shorthands.

## Result Layout

Per-run JSON files are written under `--outdir` (default `output/results/`,
or `output/headon/` for headon tiers). One JSON file per (adapter, size,
mode) tuple. Each file carries:

- `adapter`, `family` (`pingpong` / `broadcast`)
- `config` (size, message count, warmup, wait strategy, consumers)
- `throughput`, `messages_processed`, `duration_secs`
- `latency_stats` -- 12 percentiles (P1, P10, P25, P50, P75, P90, P95,
  P99, P99.9, P99.99, P99.999, P99.9999)
- `measurement_mode` (`max_throughput` | `fixed_rate`)
- `target_rate` and `coordinated_omission_stats` when the run used
  fixed-rate CO mode
- `consumer_count` for broadcast runs

Outputs are local-only -- the `output/` directory is gitignored.

## Setup

See `SETUP.md` for:

- submodule initialization
- system package prerequisites
- how each peer is built
- how each peer is executed
- output, cleanup, and large-object run behavior
