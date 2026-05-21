# third_party

Pinned benchmark peer source trees for `competitive-bench`.

This directory is intentionally crate-local so source pinning stays scoped to the benchmark harness that needs it.

## What lives here

- `crossbar`
- `boost_pingpong`
- `ompi_pingpong`

## Policy

Keep a peer here only when local source pinning materially improves reproducibility or the adapter source itself is part of the benchmark contract.

Do not vendor peers here just because they exist.
