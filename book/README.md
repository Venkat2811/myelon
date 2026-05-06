# book/

Source for the [myelon](https://github.com/Venkat2811/myelon) documentation site, built with [mdBook](https://rust-lang.github.io/mdBook/).

## Authoring

```bash
# One-time install
cargo install mdbook

# Live preview at http://localhost:3000
mdbook serve --open

# Build static HTML into ./build (gitignored)
mdbook build
```

The chapter list is `src/SUMMARY.md`. Add a new page by adding a line to that file and creating the matching `.md` under `src/`.

## Layout

```
book/
├── book.toml              # mdBook configuration
├── README.md              # this file
├── src/
│   ├── SUMMARY.md         # chapter list (the ToC)
│   ├── introduction.md
│   ├── onion/             # the layered architecture story
│   ├── layer0/            # disruptor-mp substrate chapters
│   ├── layer1/            # myelon framing chapters
│   ├── layer2/            # myelon codec chapters
│   ├── layer3/            # myelon typed-zero-copy chapters
│   ├── topology/          # FixedTopology
│   ├── observability/     # RFC-0040 counters + metrics-rs
│   ├── examples/          # walkthroughs of crates/examples/
│   ├── benches/           # pointers to perf-bench / competitive-bench
│   ├── dst/               # deterministic-simulation surface
│   └── project/           # platform / versioning / license
└── build/                 # mdBook output (gitignored)
```

## API reference

The book covers concepts. Per-type / per-method API reference is at:

- [docs.rs/disruptor-mp](https://docs.rs/disruptor-mp)
- [docs.rs/myelon](https://docs.rs/myelon)

Both are auto-published when the crates publish to crates.io.

## Hosting

Plan: Cloudflare Pages, custom domain. Build command `mdbook build`, output dir `book/build`.
