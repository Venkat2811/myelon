# myelon

Monorepo playground for Linux-first multiprocess disruptor IPC and inference integrations.

## Layout

- `crates/disruptor-mp`: low-level multiprocess shared-memory disruptor core.
- `crates/legacy-wip`: high-level Rust API layer.
- `python-surface-archive`: Python bindings and integrations.

## Status

- Linux: priority target
- macOS: works for core paths, currently unsupported for official release guarantees
- Windows: unsupported

## One-Command Workflows

- Fast smoke (command wiring + crate boundary checks):
  - `make smoke`
- Rust-tier orchestration (format/lint/tests/bench+example compile checks):
  - `make orchestrate-rust`
- Python-tier orchestration:
  - `make orchestrate-python`
- Full monorepo orchestration:
  - `make orchestrate-all`

## Platform Policy

- Linux is the only officially supported platform for this monorepo.
- macOS is exercised and expected to work for primary multiprocess paths, but is not an officially supported target.
- Windows is explicitly not supported.
