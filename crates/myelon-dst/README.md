# myelon-dst

Internal deterministic-simulation runner for `disruptor-mp` and `myelon`.

This crate is not published to crates.io. It exists to run the kind of multiprocess fault-injection and invariant checks that the publishable crates need, but should not carry as part of their public dependency surface.

## Purpose

`myelon-dst` provides the runner side of the DST story:

- spawn producer and consumer child processes
- apply controlled configs and fault profiles
- collect per-child reports
- verify cross-child invariants

The DST primitives themselves live in `disruptor_mp::dst::*` so production-path call sites resolve without depending on this crate.

## Modules

| Module | Purpose |
|---|---|
| `runner_config` | DST run configuration: backends, codecs, wait strategies, coordination modes |
| `runner_fault` | Fault injection |
| `runner_oracle` | Message-level invariant checking |
| `runner_report` | Per-child and aggregate reporting |
| `runner` | `DstRunner` and built-in harnesses |
| `runner_verify` | Cross-child verification |

## Cargo features

| Feature | What it enables |
|---|---|
| default | Library-only surface |
| `_runner_bin` | Internal child-runner binary used by DST test lanes |

## Usage

Workspace DST runs set:

```sh
RUSTFLAGS="--cfg dst"
```

Then tests enable `_runner_bin` when they need to spawn the child runner.

## License

Licensed under either of [Apache License, Version 2.0](../../LICENSE-APACHE) or [MIT license](../../LICENSE-MIT) at your option.
