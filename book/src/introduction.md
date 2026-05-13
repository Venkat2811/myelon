# Introduction

`myelon` is multiprocess shared-memory transport for inference and other low-latency pipelines. It is the **single, simplified façade** for the [`disruptor-mp`](https://crates.io/crates/disruptor-mp) substrate: every relevant type from `disruptor-mp` (Layer 0 — the raw cross-process ring buffer plus its coordination, discovery, liveness, and observability primitives) is re-exported by `myelon`, and three more layers (framing, codec, typed zero-copy) plus topology and layout helpers sit on top. One dependency, one stable API, the whole stack.

> **Everything here is multiprocess.** `disruptor-mp`'s **mp** is not a suggestion. Producers and consumers live in different OS processes and coordinate through a shared-memory segment with cache-line-padded sequence cursors and a fixed-size ring buffer. Single-process / threaded Disruptor APIs come from the upstream [`disruptor`](https://crates.io/crates/disruptor) crate.

## Quick links

- API reference: [docs.rs/myelon](https://docs.rs/myelon) and [docs.rs/disruptor-mp](https://docs.rs/disruptor-mp)
- Source: [github.com/Venkat2811/myelon](https://github.com/Venkat2811/myelon)
- Issues / discussions: GitHub Issues on the repo

## What this book covers

This book covers the *concepts* — the layered model, when to use each layer, the orthogonal concerns (coordination, discovery, liveness, observability), worked examples, and the bench harnesses. The API reference (every type, every method) lives on docs.rs and is regenerated automatically when the crates publish.

If you want to skip ahead, the most useful starting points are:

- [Layered architecture](onion/index.md) — the central organising idea.
- [When to use which layer](onion/when-to-use-which.md) — decision matrix.
- [Layer 0 quick start](examples/shm.md) — the smallest end-to-end multiprocess example.

> **Status (`0.1.0-alpha.1`).** Three chapters are written: this introduction, [layered architecture](onion/index.md), and [when to use which layer](onion/when-to-use-which.md). The remaining ~28 chapters are scaffolds that redirect to the docs.rs API reference and the workspace README. Both are comprehensive — `disruptor-mp` and `myelon` ship at 0 missing-docs on their published surfaces. The book chapters will be filled in over the alpha-iteration window; the TOC structure exists today to make scope obvious. PRs welcome.

## Why this exists

Two real OS processes that need to exchange events at sub-microsecond latency without Linux-only assumptions, with strict broadcast semantics, with deterministic-simulation tests, and with optional hot-path counters that don't tax the steady-state path. That's the contract.

The published crates that deliver the contract:

| Crate | Role | When to depend on it | crates.io |
|---|---|---|---|
| [`myelon`](https://docs.rs/myelon) | Layers 1–3 (framing, codecs, typed zero-copy) plus FixedTopology, layout helpers, and observability re-exports. **Re-exports every relevant `disruptor-mp` type**, so depending on this single crate gives you the full stack. | **Default for almost all users.** | [crates.io/myelon](https://crates.io/crates/myelon) |
| [`disruptor-mp`](https://docs.rs/disruptor-mp) | Layer 0 alone: raw cross-process ring buffer + coordination + discovery + liveness + observability primitives. | When you want the substrate alone with no framing / codec / topology surface compiled in (e.g. you publish your own wire format). | [crates.io/disruptor-mp](https://crates.io/crates/disruptor-mp) |
