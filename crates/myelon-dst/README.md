# myelon-dst

> **Internal**. Not published to crates.io. Used by integration tests
> in `disruptor-mp` and `myelon`.

## Purpose

`myelon-dst` is the internal multiprocess deterministic-simulation harness. A test launches a parent process that spawns producer and consumer children with a controlled config, fault-injection profile, and oracle. The runner collects per-child reports, runs the assertion oracle, and verifies the DST contract.

## Modules

| Module | Purpose |
|---|---|
| `config` | `DstConfig`, `BackendKind`, `CodecKind`, `WaitStrategyKind`, `CoordinationKind`. |
| `fault` | `FaultInjector`, `FaultEvent`, `FaultKind`. |
| `oracle` | `MessageOracle`, `OracleMessage`, `OracleViolation`, `payload_bytes`, `stable_payload_hash`. |
| `report` | `ChildReport`, `DstRunReport`, `DstProperty`, `TransportKind`. |
| `runner` | `DstRunner`, `DstRunnerError`, `RawRingHarness`, `RequiredConsumerLivenessPolicy`. |
| `verify` | Cross-child verification helpers. |

## Cargo features

| Feature | What it enables |
|---|---|
| (default) | Library-only surface; the child runner binary stays disabled so normal workspace builds stay lean. |
| `_runner_bin` | Enables the internal `myelon-dst-runner-child` binary used by DST test lanes. |

## Usage

Workspace DST runs set `RUSTFLAGS="--cfg dst"` so `disruptor-mp` and `myelon` compile their DST hooks, then enable `_runner_bin` when the harness needs to spawn `myelon-dst-runner-child`. Tests construct a `DstRunner`, register harnesses, and assert on the resulting `DstRunReport`.

## License

Licensed under either of [Apache License, Version 2.0](../../LICENSE-APACHE) or [MIT license](../../LICENSE-MIT) at your option.
