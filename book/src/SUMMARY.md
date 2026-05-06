# Summary

[Introduction](introduction.md)

# The onion

- [Layered architecture](onion/index.md)
- [When to use which layer](onion/when-to-use-which.md)
- [Costs per layer](onion/costs.md)
- [Two senses of zero-copy](onion/zero-copy.md)

# Layer 0 — disruptor-mp

- [Substrate overview](layer0/index.md)
- [SHM vs mmap backends](layer0/shm-vs-mmap.md)
- [Coordination modes](layer0/coordination.md)
- [Discovery](layer0/discovery.md)
- [Required-consumer liveness (RFC 0017.5)](layer0/liveness.md)
- [Observability counters (RFC 0040)](layer0/observability.md)

# Layer 1 — myelon framing

- [Framed transport](layer1/index.md)
- [Choosing a frame size](layer1/frame-size.md)
- [Multi-frame fragmentation](layer1/fragmentation.md)

# Layer 2 — myelon codec

- [The Codec trait](layer2/index.md)
- [bincode / rkyv / flatbuffers](layer2/codecs.md)

# Layer 3 — typed zero-copy

- [The ZeroCopyCodec trait](layer3/index.md)
- [When to reach for it](layer3/when-to-use.md)

# Topology + observability

- [FixedTopology](topology/index.md)
- [Observability via the metrics-rs facade](observability/index.md)

# Worked examples

- [Layer 0 quick start (shm_disruptor)](examples/shm.md)
- [Layer 0 over mmap (mmap_disruptor)](examples/mmap.md)
- [Multiprocess RTT (pingpong)](examples/pingpong.md)
- [Counters end-to-end (counters)](examples/counters.md)
- [Fixed scheduler / N-worker (fixed_inference_topology)](examples/topology.md)

# Going further

- [Bench harness: perf-bench](benches/perf-bench.md)
- [External comparison: competitive-bench](benches/competitive-bench.md)
- [Deterministic simulation (DST)](dst/index.md)
- [API reference (docs.rs)](api-reference.md)

# Project

- [Platform support](project/platforms.md)
- [Versioning + MSRV](project/versioning.md)
- [License](project/license.md)
