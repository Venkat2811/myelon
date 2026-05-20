# myelon-pulse

> **Internal · brand vanity demo.** Not published. The vibe `myelon`
> produces in your machine, rendered on a tiny desktop canvas.

## What it is, in plain words

`myelon` borrows its name from the Greek for *spinal cord* — the cord
that carries every signal between brain and body, wrapped in myelin
sheaths so the impulses travel fast and clean. The crate does the
same thing for processes: a low-latency fabric that lets independent
OS processes pass messages without copies or context switches.

This demo is the visual reading of that:

- A glowing seed at the left edge — call it the cord's origin.
- A trunk that extends rightward, then recursively fractal-branches
  into finer and finer tendrils — nerve roots fanning out into the
  peripheral fibres.
- Pulses that travel along those branches once everything's wired up
  — action potentials racing down the cord and back.

Three motion shapes that map onto real `disruptor-mp` / `myelon`
behaviour:

- **Broadcast** — every leaf branch lights up on each pulse. Strict
  broadcast, multiprocess: one publish, every consumer sees it.
  Travel time visible: ~0.5 s seed → leaf.
- **Ping-pong** — every branch fires an outbound pulse simultaneously;
  each pulse hits its leaf tip and a return pulse fires back to the
  seed. All branches do the round-trip in parallel, with a brief
  pause between rounds. Rhythm: `out — back — pause — repeat`.
- **Signal** — the whole cord (seed + every leaf) lights up in one
  instant per beat. No propagation is drawn, because at ~275M
  signals/s the per-event travel time is invisible to the eye.
  Beats overlap (~11 Hz spawn, ~180 ms fade) so the cord reads as
  continuously buzzing — that's what "signal is so fast it has no
  visible motion" actually looks like.

By default the demo cycles through broadcast → ping-pong → signal,
~7 s each, so the contrast between visible travel (broadcast),
visible round-trip (ping-pong), and invisible propagation (signal)
is obvious. `--mode broadcast`, `--mode pingpong`, `--mode signal`,
or `--mode alternating` (default) to lock one in.

A small dim line under the active mode caption names the canonical
throughput / latency anchor for that mode (e.g.
`275M signals/s · 21ns avg queue` for SIGNAL). Numbers come from
the perf-bench surface, not promotional marketing.

## Aesthetic

The deep-black canvas, ember-orange filaments, soft bloom, drifting
ash particles, and lower-right title block come from watching the
title sequence of *House M.D.* on loop and shamelessly stealing the
mood (yes, technically the show's title is an angiogram — vessels,
not nerves — but the silhouette is too good to pass up, and the
warm-on-black palette sells "low-latency electricity" better than
literal axon imagery would). The MYELON wordmark crackles per-letter
like a live nerve fibre — independent brightness flicker per glyph,
amber spark particles around it, and an extra ignition burst right
at the moment the system goes from initialising to live.

## What you see

Click `START` and the cord starts to grow:

1. The seed brightens. A trunk extends rightward.
2. Primary branches sprout, then secondary, then fine periphery —
   recursive fractal growth over ~5 seconds. The MYELON wordmark
   begins crackling alive in the lower right.
3. A small subtitle walks the lifecycle in plain words —
   `INITIALIZE TRANSPORT` → `DISCOVER CONSUMERS` → `ATTACH RING` →
   `READY` — crossfading between each.
4. At `READY`, the seed glows brighter for a moment ("system online")
   and pulses begin travelling along the established branches.
5. The subtitle settles to the current run mode: `BROADCAST`,
   `PING-PONG`, or `SIGNAL`. A small line under it names the
   headline throughput / latency for that mode.

`STOP` clears it back to empty.

## Controls

| Action | How |
|---|---|
| Start a fresh run | `START` button (only when Idle) — or `space` |
| Stop and clear | `STOP` button (only when running) — or `space` |
| Quit | `Esc` or `Q` |

When started with `--record`, an additional `REC` button appears:

- First click starts a GIF recording. The button turns red with a
  `●` indicator.
- Second click stops, finalizes the GIF, and — if `ffmpeg` is on
  `PATH` — converts the GIF to a Twitter-friendly H.264 MP4
  alongside (libx264, yuv420p, +faststart, scaled to ≤1280 px wide).
  For a typical 5–10 s recording the MP4 lands ~50–70× smaller than
  the GIF.
- If you quit (Esc / Q) while a recording is in flight, both files
  are finalized cleanly on exit.

If `ffmpeg` isn't installed, REC still saves the raw GIF and prints a
one-line note that MP4 conversion was skipped.

Recordings land in `./myelon-pulse-captures/` (relative to the
binary's cwd; under `cargo run` that's the workspace root). The
directory is gitignored. Filenames embed a `unix_secs-millis`
timestamp and the GIF and MP4 share a stem:

```
myelon-pulse-captures/recording-1714979147-103.gif
myelon-pulse-captures/recording-1714979147-103.mp4
```

Share the MP4 — Twitter caps GIFs at 15 MB on web (5 MB on mobile)
and 1280×1080 dimensions; retina-resolution recordings exceed both,
but the MP4 sails through.

A small toast in the bottom-left confirms each save with the
absolute path. Same line prints to the terminal.

## Run

```bash
cargo run --release -p myelon-pulse-vanity                       # default (alternating)
cargo run --release -p myelon-pulse-vanity -- --mode broadcast
cargo run --release -p myelon-pulse-vanity -- --mode pingpong
cargo run --release -p myelon-pulse-vanity -- --mode signal
cargo run --release -p myelon-pulse-vanity -- --branches 9
cargo run --release -p myelon-pulse-vanity -- --autostart        # skip START click
cargo run --release -p myelon-pulse-vanity -- --record           # capture mode
cargo run --release -p myelon-pulse-vanity -- --rotate           # X-pitch breathing
```

`--rotate` enables the 3D X-axis pitch oscillation (~±45° at ~16 s
period) that gives the spinal cord a volumetric "breathing" feel.
Off by default — without it the scene stays flat 2D.

## Topology mapping

By default the leaf-branch count is derived from your machine: `2/3`
of physical cores, rounded down to even, clamped to `[2, 16]`. On an
8-core machine that's 4 leaves; on a 16-core machine 10. The
rendered tree has more total segments because each leaf is reached
by recursive fractal branching from the trunk.

That same rule is what the multiprocess version of this demo will
use to pick how many real producer/consumer OS processes to spawn —
so when the synthetic timeline gets replaced by real `disruptor-mp`
events, the silhouette stays honest, it just becomes a real
instrument.

## What this is not (yet)

- **Not yet wired to real `disruptor-mp` / `myelon` processes.**
  Pulses run on a synthetic timeline. Cadences match realistic
  broadcast / RTT shapes, and the topology rule matches what the
  multiprocess version will use, so when the wiring lands the
  visual stays the same — it just starts being driven by real
  cross-process events.
- **Not yet built for WASM.** `macroquad` supports it; the demo
  doesn't cross-compile in this pass.

## License

Licensed under either of [Apache License, Version 2.0](../../LICENSE-APACHE) or [MIT license](../../LICENSE-MIT) at your option.
