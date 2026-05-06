# dst-runner

> **Internal**. Not published to crates.io. Used by integration tests
> in `disruptor-mp` and `myelon`.

## Purpose

`dst-runner` is a multiprocess deterministic-simulation harness. A
test launches a parent process that spawns producer and consumer
children with a controlled config, fault-injection profile, and
oracle. The runner collects per-child reports, runs the assertion
oracle, and verifies the DST contract.

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
| (default) | Real multiprocess runner against `disruptor-mp` without DST hooks. |
| `dst` | Forwards to `disruptor_mp/dst` so integration tests exercise the deterministic-simulation surface. |

## Usage

`dst-runner` is consumed via `path = "../dst-runner"` from
`myelon`'s `dev-dependencies`. Tests construct a
`DstRunner`, register harnesses, and assert on the resulting
`DstRunReport`.

## License

MIT.
