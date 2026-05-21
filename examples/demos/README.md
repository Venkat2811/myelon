# demos

Runnable first-party examples for `myelon` and `disruptor-mp`.

These examples are intentionally small. They show real multiprocess wiring, not thread-based simulation.

## Start here

If you only run one example, start with:

```bash
cargo run --release -p demos --example shm_disruptor
```

That gives you the simplest real SHM producer and consumer pair through `myelon`.

If you want to compare the one-dependency path against the raw direct-dependency path, run these back to back:

```bash
cargo run --release -p demos --example shm_disruptor
cargo run --release -p demos --example disruptor_mp_shm
```

They exercise the same runtime shape with different import surfaces.

## Example ladder

| Example | Run it when... | Import profile |
|---|---|---|
| `shm_disruptor.rs` | You want the simplest raw SHM quick start through `myelon` | `myelon` |
| `mmap_disruptor.rs` | You want the same shape backed by mmap instead of SHM | `myelon` |
| `disruptor_mp_shm.rs` | You want the same SHM shape with a direct `disruptor-mp` dependency | `disruptor-mp` |
| `disruptor_mp_mmap.rs` | You want the same mmap shape with a direct `disruptor-mp` dependency | `disruptor-mp` |
| `pingpong.rs` | You want a two-ring request/response round-trip | `myelon` |
| `counters.rs` | You want RFC-0040 observability counters end to end | `myelon` |
| `fixed_inference_topology.rs` | You want the fixed scheduler and N-worker topology helpers | `myelon` |
| `required_consumer_liveness.rs` | You want to see same-ID rejoin recovery for required-consumer liveness | `myelon` plus liveness config types |

## Run them

```bash
cargo run --release -p demos --example shm_disruptor
cargo run --release -p demos --example mmap_disruptor
cargo run --release -p demos --example disruptor_mp_shm
cargo run --release -p demos --example disruptor_mp_mmap
cargo run --release -p demos --example pingpong
cargo run --release -p demos --example counters
cargo run --release -p demos --example fixed_inference_topology
cargo run --release -p demos --example required_consumer_liveness
```

Use `--release` for any run where latency or throughput matters.

## Multiprocess wiring

Each example follows the same shape:

- parent process starts normally
- child roles are re-entered through `current_exe()` plus env-var role dispatch
- parent and children share the exact same SHM or mmap names through the environment

The helper code for that lives in `examples/demos/src/lib.rs`.

## What to do next

Use the examples to understand the API shape. Use the benchmark crates when you want measurement:

- [`crates/perf-bench`](../../crates/perf-bench/): broad internal sweep
- [`crates/competitive-bench`](../../crates/competitive-bench/): external transport comparison
