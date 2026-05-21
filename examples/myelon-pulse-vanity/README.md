# myelon-pulse

Internal brand vanity demo for `myelon`.

It is also the source of the animated GIF used at the top of the workspace `README.md`.

This crate is not a transport benchmark and not a transport integration. It is a visual demo.

## What it is

The scene renders a cord-like branching structure on a dark canvas and drives three motion shapes that map onto real `disruptor-mp` / `myelon` behaviour:

- **Broadcast**: every leaf branch lights up on each pulse. Strict
  broadcast, multiprocess: one publish, every consumer sees it.
  Travel time visible: ~0.5 s seed → leaf.
- **Ping-pong**: every branch fires an outbound pulse simultaneously;
  each pulse hits its leaf tip and a return pulse fires back to the
  seed. All branches do the round-trip in parallel, with a brief
  pause between rounds. Rhythm: `out — back — pause — repeat`.
- **Signal**: the whole cord (seed + every leaf) lights up in one
  instant per beat. No propagation is drawn, because at ~275M
  signals/s the per-event travel time is invisible to the eye.
  Beats overlap (~11 Hz spawn, ~180 ms fade) so the cord reads as
  continuously buzzing: that's what "signal is so fast it has no
  visible motion" actually looks like.

By default the demo cycles through broadcast → ping-pong → signal,
~7 s each, so the contrast between visible travel (broadcast),
visible round-trip (ping-pong), and invisible propagation (signal)
is obvious. `--mode broadcast`, `--mode pingpong`, `--mode signal`,
or `--mode alternating` (default) to lock one in.

A `MEASURED VALUES` box under the active mode caption wraps a
two-row stat block (shm + mmap) with canonical throughput in
msgs/s plus P50 / P99 / P99.99 latency anchors. The box header
spells out the mode, topology (`1p1c`), and payload; a footer
notes that the wider 1p2c–1p12c ladder is also measured (without
claiming linear scaling, because signal throughput in particular
drops as consumers contend with the publisher). Numbers come from
the uncontended Pack A / Pack B perf-bench tables, not
promotional marketing. Example for SIGNAL:

```
┌──────────────────────────────────────────────────────────────────────┐
│           MEASURED VALUES  ·  signal  ·  1p1c  ·  event             │
│                                                                      │
│  shm   ·  332M msgs/s  ·  P50 188ns  ·  P99 282ns  ·  P99.99 13.3µs │
│  mmap  ·  238M msgs/s  ·  P50 186ns  ·  P99 453ns  ·  P99.99 15.2µs │
│                                                                      │
│               also measured  ·  1p2c through 1p12c                  │
└──────────────────────────────────────────────────────────────────────┘
```

Signal and pingpong show identical per-event latency on purpose:
they ride the same underlying ring, so per-event latency is
ring-bounded. The difference is throughput: signal pipelines
without waiting for ack, so it does ~60× the msgs/s. Broadcast
is quoted at 1KB payload using `framed_batch` numbers so
throughput stays in millions and P50 stays in nanoseconds; shm
wins throughput and the P99.99 tail, mmap edges shm at P99 by
~0.4µs (real minor noise, not anomaly).

## Fun fact

The visual direction comes straight from the House M.D. opening-sequence angiogram aesthetic, not from a generic sci-fi network diagram. Reference clip: <https://www.youtube.com/watch?v=x5i5ERDE_2E>. That is why the scene leans anatomical: branching trunk, ember glow, and pulses moving like a stylized diagnostic trace.

## What it is not

- not yet wired to live `disruptor-mp` or `myelon` processes
- not a benchmark harness
- not yet built for WASM in this pass

## What you see

Click `START` and the cord starts to grow:

1. The seed brightens. A trunk extends rightward.
2. Primary branches sprout, then secondary, then fine periphery:
   recursive fractal growth over ~5 seconds. The MYELON wordmark
   begins crackling alive in the lower right.
3. A small subtitle walks the lifecycle in plain words:
   `INITIALIZE TRANSPORT` → `DISCOVER CONSUMERS` → `ATTACH RING` →
   `READY` (crossfading between each).
4. At `READY`, the seed glows brighter for a moment ("system online")
   and pulses begin travelling along the established branches.
5. The subtitle settles to the current run mode: `BROADCAST`,
   `PING-PONG`, or `SIGNAL`. A small line under it names the
   headline throughput / latency for that mode.

`STOP` clears it back to empty.

## Controls

| Action | How |
|---|---|
| Start a run | `START` or `space` |
| Stop and clear | `STOP` or `space` |
| Quit | `Esc` or `Q` |

With `--record`, a `REC` button appears:

- first click starts GIF capture
- second click finalizes the GIF
- if `ffmpeg` is on `PATH`, an H.264 MP4 is emitted alongside it

Outputs land in `./myelon-pulse-captures/`.

## Run

```bash
cargo run --release -p myelon-pulse-vanity                       # default (alternating)
cargo run --release -p myelon-pulse-vanity -- --mode broadcast
cargo run --release -p myelon-pulse-vanity -- --mode pingpong
cargo run --release -p myelon-pulse-vanity -- --mode signal
cargo run --release -p myelon-pulse-vanity -- --branches 9
cargo run --release -p myelon-pulse-vanity -- --autostart
cargo run --release -p myelon-pulse-vanity -- --record
cargo run --release -p myelon-pulse-vanity -- --rotate
```

## Topology mapping

The default leaf count is derived from the machine:

- `2/3` of physical cores
- rounded down to even
- clamped to `[2, 16]`

That rule is meant to stay aligned with the eventual real-process version of the demo.

## License

Licensed under either of [Apache License, Version 2.0](../../LICENSE-APACHE) or [MIT license](../../LICENSE-MIT) at your option.
