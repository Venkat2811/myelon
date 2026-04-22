# third_party

Pinned external benchmark dependencies live here for `competitive-bench`.

This directory is intentionally crate-local rather than repo-root so benchmark-only
source pinning stays scoped to `competitive-bench`.

Phase-one parity target set from `mp_ipc_world_domination`:

- `shmipc-rs`
- `boost::interprocess message_queue`
- `ompi`
- `rusteron` / Aeron IPC
- `zeromq`

Additional same-contract peer wired here:

- `crossbar`
  - pinned as a crate-local git submodule
  - benchmarked on the exact same `1p1c` throughput and fixed-rate CO grids
  - does not change the parity scenario contract

Currently wired without local source pinning:

- `shmipc-rs`
- `rusteron`
- `zeromq`

Currently wired with crate-local source:

- `crossbar`
- `boost_pingpong`
- `ompi_pingpong`

These peers are Cargo-backed today. If we later need exact upstream source pinning
for published parity tables, they can also move under `third_party/`.

Admission rule:

- add source trees here only when source pinning materially affects benchmark reproducibility
- pure system dependencies may stay system-managed with explicit version capture
- Bazel is allowed later if mixed-language dependency management becomes cleaner that way

Current output/reporting contract:

- internal raw baselines cover both `shm` and `mmap`
- SHM / IPC and `mmap` results render in separate aggregate tables
- latency exports include:
  - `P1`
  - `P10`
  - `P25`
  - `P50`
  - `P90`
  - `P95`
  - `P99`
  - `P99.9`
  - `P99.99`
  - `P99.999`
  - `P99.9999`
