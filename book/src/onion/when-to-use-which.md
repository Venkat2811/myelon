# When to use which layer

Both `myelon` and `disruptor-mp` are first-class entry points. Every row in the table below is reachable from `myelon`; the right-most column flags rows where depending on `disruptor-mp` directly is also a clean choice — pick by what surface your code actually needs.

| Need | Layer | Type | Direct on `disruptor-mp`? |
|---|---|---|---|
| Cross-process publish/consume of a fixed-size `Copy` event over a ring buffer | 0 — raw | `SharedProducer<E>` / `SharedConsumer<E>` (SHM) or `MmapProducer<E>` / `MmapConsumer<E>` (mmap). All re-exported by `myelon`. | Yes — substrate alone. |
| Variable-length `&[u8]` payloads, possibly larger than one ring slot. Need start/end flags + a message id. | 1 — framed | `FramedTransportProducer<F>` / `FramedTransportConsumer<F>` (SHM) or `MmapFramedTransportProducer<F>` / `MmapFramedTransportConsumer<F>` (mmap) | No — only on `myelon`. |
| Typed message with serialisation (bincode / rkyv / flatbuffers). Owned decode on the consumer side. | 2 — codec | `TypedProducer<F>` / `TypedConsumer<F>` + a `Codec` impl | No. |
| Same as Layer 2 but consumer reads serialised data in-place — no `deserialize` allocation. | 3 — typed zero-copy | `TypedProducer<F>` / `TypedConsumer<F>` + a `ZeroCopyCodec` impl | No. |
| Fixed scheduler / N-worker topology with discovery + rendezvous | orthogonal | `FixedTopology`, `WorkerCount` (2..=8) | No. |
| Per-process hot-path counters (events_published, consumer_lag_max, …) | orthogonal | `observability::*` | Yes — same surface lives on both crates. |

Rule of thumb: start at the highest layer that matches your data, profile, and only step down if you see allocator or codec cost in the profile. Layer 0 is always available — through `myelon` for combined use, or through `disruptor-mp` directly for substrate-only consumers.
