# myelon-pulse

> **Internal.** Brand vanity demo. Not published. The motion that the
> `myelon` library produces in your machine, rendered as a House-MD-titles
> -style angiogram.

## What you see

A near-black canvas with a faint warm wash. A glowing seed at the
left edge. Click **START** and a trunk extends rightward, then
fractal sub-branches sprout from it, then sub-sub-branches sprout
from those — a recursive dendrite tree growing in over a few
seconds. The "MYELON" caption fades in under the spread. Drifting
embers in the background, soft amber glow on every branch.

Once the tree is fully grown, pulses begin travelling along the
branches in two demonstrable shapes:

- **Broadcast** — every leaf branch lights up in lockstep on each
  pulse. This is what `disruptor-mp`'s strict-broadcast multiprocess
  ring does on every publish.
- **Ping-pong** — every branch fires an outbound pulse
  simultaneously; each pulse hits its leaf tip and a return pulse
  fires back to the seed. All branches do the round-trip in parallel,
  with a brief pause between rounds. The rhythm is
  `out — back — pause — repeat`.

By default the demo alternates between the two modes every ~7
seconds so the contrast is obvious. Pass `--mode broadcast`,
`--mode pingpong`, or `--mode alternating` (default) to lock one in.

## Controls

- `START` button — generate a fresh tree and grow it in. Only
  enabled while the canvas is empty (Idle); to regenerate, hit
  `STOP` first.
- `STOP` button — clear the canvas back to empty. Only enabled
  while a tree is growing or live.
- `space` — keyboard toggle: starts a fresh run from Idle; stops
  any in-flight run otherwise.
- `Esc` / `Q` — quit.

When started with `--record`, an additional `REC` button appears:

- First click starts a GIF recording. The button turns red and
  shows a `●` indicator.
- Second click stops the recording, finalizes the GIF, and — if
  `ffmpeg` is on `PATH` — converts the GIF to a Twitter-friendly
  H.264 MP4 alongside it (libx264, yuv420p, +faststart, scaled to
  ≤1280 px wide). For a typical 5–10 s recording the MP4 lands
  about 50–70× smaller than the GIF.
- If you quit (Esc / Q) while a recording is in flight, the GIF
  and the MP4 (when ffmpeg is available) are finalized cleanly on
  exit.

If `ffmpeg` is not installed, REC still saves the raw GIF and
prints a one-line note that MP4 conversion was skipped.

Recordings land in `./myelon-pulse-captures/` (relative to the
binary's current working directory; with `cargo run` that's the
workspace root). The directory is gitignored. Filenames embed a
`unix_secs-millis` timestamp; the GIF and MP4 share a stem:

```
myelon-pulse-captures/recording-1714979147-103.gif
myelon-pulse-captures/recording-1714979147-103.mp4
```

Use the MP4 for sharing — Twitter caps GIFs at 15 MB on web
(5 MB mobile) and 1280×1080 dimensions; our retina-resolution
recordings exceed both, but the MP4 satisfies them.

A short toast in the bottom-left of the window confirms each
recording start and finalize, and shows the absolute path. The
same line is printed to the terminal.

## Run

```bash
cargo run --release -p myelon-pulse
cargo run --release -p myelon-pulse -- --mode broadcast
cargo run --release -p myelon-pulse -- --mode pingpong
cargo run --release -p myelon-pulse -- --branches 9
cargo run --release -p myelon-pulse -- --autostart   # skip START click
cargo run --release -p myelon-pulse -- --record      # capture buttons on
```

> **GIF file size note** — captures happen at the framebuffer's
> native resolution (2× logical on retina displays) at 15 fps, so a
> 10 s recording at the default window size easily lands in the
> 50–100 MB range. Resize the window down before recording, or
> downscale the GIF after the fact with `ffmpeg`/`gifsicle`. A
> proper video pipeline would need an `ffmpeg` subprocess; that's
> intentionally deferred for now.

## Topology mapping

By default the leaf-branch count is derived from your machine: it
picks `2/3` of the physical core count, rounds down to an even
number, clamps to `[2, 16]`. On an 8-core machine that's 4 leaves;
on a 16-core machine 10. The actual rendered tree has more total
segments because each leaf is reached by recursive fractal branching
from the trunk. Override the leaf target with `--branches N`.

That mapping is the same rule the planned multiprocess version of
this demo uses to decide how many real producer/consumer OS
processes to spawn — the visual stays honest about what's happening
even though this first-pass version drives the animation from a
synthetic clock rather than from real disruptor traffic.

## What this is not (yet)

- Not wired to real `disruptor-mp` / `myelon` processes yet. Pulses
  are driven by a synthetic timeline. The animation timing matches
  realistic broadcast / RTT cadences, and the topology rule matches
  what the multiprocess version will use, so when the wiring lands
  the visual doesn't change shape — it just becomes a real
  instrument.
- Not built for WASM yet. macroquad supports it; the demo doesn't
  pull the trigger on cross-compilation in this pass.

## Aesthetic

Inspired by the title sequence of *House M.D.* — branching
CT-angiogram-style nerve / vessel imagery, deep black background,
warm amber on active paths, ember dust in the field, soft bloom.
The library is named after the spinal cord and shows up on the wire
as branching electrical signal; the visual is the obvious one.

## License

MIT.
