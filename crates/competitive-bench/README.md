# competitive-bench

Internal external-comparison harness for `disruptor-mp` and `myelon`.

`competitive-bench` answers a narrower question than `perf-bench`: under the same scenario contract, how do the internal raw baselines compare against a short list of serious external peers?

It does not try to be the full internal sweep system. That is [`perf-bench`](../perf-bench/).

## Scope

The contract is:

- same scenario shape across adapters
- same payload ladder per tier
- same output schema
- internal raw baselines included beside external peers

Current families:

| Family | Shape |
|---|---|
| signal | internal raw-ring only, exposed through small smoke lanes |
| ping-pong | `1p1c`, throughput and fixed-rate CO-aware |
| broadcast | `1p4c` and `1p8c`, throughput and fixed-rate CO-aware where wired |

## Coverage matrix

| Surface | Internal raw baselines | crossbar | rusteron | iceoryx2 | shmipc-rs | boost | ompi | zeromq-* |
|---|---|---|---|---|---|---|---|---|
| signal | yes | no | no | no | no | no | no | no |
| ping-pong throughput | yes | yes | yes | yes | yes | yes | yes | yes |
| ping-pong fixed-rate | yes | yes | yes | yes | yes | yes | yes | yes |
| broadcast throughput | yes | yes | no | no | no | no | no | no |
| broadcast fixed-rate | yes | yes | no | no | no | no | no | no |

Broadcast is intentionally narrower than ping-pong. Signal headline numbers should come from `perf-bench`, not from this crate.

## Adapters

Internal baselines:

- `disruptor-shm`
- `disruptor-mmap`
- `myelon-raw-shm`
- `myelon-raw-mmap`

External peers currently wired:

- `crossbar-channel`
- `crossbar-pubsub`
- `shmipc-rs`
- `iceoryx2-shm`
- `boost-message-queue`
- `ompi-vader-self`
- `rusteron-aeron-ipc`
- `zeromq-default`
- `zeromq-ipc`
- `zeromq-ipc-abs`
- `zeromq-tcp`

## Crate layout

| Path | Purpose |
|---|---|
| `src/adapters/` | per-transport adapter implementations |
| `src/runner/` | orchestration, dispatch, execution, cleanup |
| `src/infra/` | shared parity config, JSON schema, pacing helpers |
| `src/bin/` | thin wrappers over adapter and runner entry points |
| `config/` | pinned adapter/build configuration used by runner and scripts |
| `scripts/` | helper scripts for building or driving external peers |
| `output/` | local run output (gitignored) |
| `third_party/` | pinned source trees only where local source pinning materially helps reproducibility |

## Build and run

The bench commands below use `--profile competitive` (max-perf, `panic = "abort"`, stripped) — bench-fairness defaults, not production. For production builds use `release` or `prod-max`. See the workspace README's *Validation and benchmarks* section.

Typical flow from `crates/competitive-bench`:

```bash
make help
make build-all
make simple-smoke
make super-tiny
make quick
make headon-smoke
```

Larger sweeps:

```bash
make extensive
make headon-extensive
```

Direct runner invocation:

```bash
cargo run -p competitive-bench --profile competitive \
    --bin competitive_bench_runner -- --help
```

## Result layout

One JSON file is emitted per scenario tuple under `--outdir`. The JSON is the source of truth.

Each result carries the usual fields:

- adapter
- family
- size
- mode
- throughput
- duration
- latency percentiles
- target rate and CO metadata for fixed-rate runs
- consumer count for broadcast

## Sample output

Pingpong throughput at 1 KB payload across 12 in-machine IPC adapters:

<p align="center">
  <img src="../../assets/bench-pingpong-throughput-1kb.png" alt="Pingpong throughput at 1 KB across 12 IPC adapters" width="800">
</p>

Broadcast P99 latency at 1 KB · 4 consumers under sustained 400 K msgs/s (coordinated-omission-corrected):

<p align="center">
  <img src="../../assets/bench-broadcast-co-p99-1kb-4c.png" alt="Broadcast CO P99 latency at 1 KB 4c 400K/s sustained" width="800">
</p>

Pingpong throughput heatmap across the full adapter × payload matrix:

<p align="center">
  <img src="../../assets/bench-throughput-heatmap.png" alt="Pingpong throughput heatmap across adapters and payloads" width="800">
</p>

## Setup

Operator details live in [`SETUP.md`](SETUP.md):

- submodules
- system packages
- build prerequisites
- per-adapter build contract
- cleanup and output hygiene

## License

Licensed under either of [Apache License, Version 2.0](../../LICENSE-APACHE) or [MIT license](../../LICENSE-MIT) at your option.
