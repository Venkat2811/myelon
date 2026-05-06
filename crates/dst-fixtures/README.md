# dst-fixtures

> **Internal**. Not published to crates.io. Shared across `disruptor-mp`
> and `myelon` integration tests so the two crates can validate identical deterministic-simulation contracts.

## Purpose

`dst-fixtures` carries the deterministic-simulation (DST) test support types that integration tests in both `disruptor-mp` and `myelon` rely on. Centralising them here means a single source-of-truth for:

- assertion kinds and assertion logs
- buggify-style fault injection profiles
- DST contract identifiers
- DST runtime mappings

## Modules

| Module | Purpose |
|---|---|
| `dst_assertions` | Assertion enum + log used by tests to record DST observations. |
| `dst_buggify` | Probabilistic fault injector. |
| `dst_contract` | Stable DST property contract (e.g. `OrderingPreserved`, `NoLoss`). |
| `dst_mapping` | Convert raw bench events into DST-checkable observations. |
| `dst_profiles` | Named scenarios that combine probabilities and seeds. |
| `dst_runtime` | Test-time runtime context. |

## Usage

`dst-fixtures` is consumed via `path = "../dst-fixtures"` from `disruptor-mp` and `myelon`'s `dev-dependencies`. It is gated behind those crates' `dst` feature.

## License

MIT.
