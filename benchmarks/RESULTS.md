# Benchmark results

Tier 1 — deterministic CPU micro-benchmarks (see
[`docs/benchmarking-proposal.md`](../docs/benchmarking-proposal.md)).

Infra: **[mitata](https://github.com/evanwashere/mitata)** — the JS analog of
BenchmarkDotNet (warmup, percentiles, allocation). We run it Node-native because
the client is a Fable binding to a JS SDK and cannot execute under .NET.

These measure only the CPU our wrapper owns: the patterns serializer and the
append/read input builders. No network, no token.

## Run it

```bash
npm run bench
```

## Snapshot

Environment: Apple M4 · Node 24.14.1 (arm64-darwin) · ~4.30 GHz. Numbers are
indicative and machine-dependent — re-run locally before trusting small deltas.

| Benchmark | avg / iter |
|-----------|-----------|
| **Serialization (patterns `Json`)** | |
| `serialize` small (~30 B) | 261 ns |
| `serialize` 64 KiB | 56.8 µs |
| `serialize` 1 MiB | 883 µs |
| `deserialize` small | 186 ns |
| round-trip small | 469 ns |
| **Wrapper mapping (build path, no network)** | |
| `Record.text` (DU ctor) | 25 ns |
| `mkInput` 1 text record | 324 ns |
| `mkInput` 100 text records | 6.04 µs |
| `mkInput` 1 bytes record | 321 ns |
| `readInput` (full options) | 1.75 µs |

## What it tells us

- **The wrapper adds no meaningful CPU.** Building an append input is ~320 ns for
  one record and ~6 µs for a 100-record batch; building a read query is ~1.75 µs;
  a `Record` DU ctor is ~25 ns. All are *orders of magnitude* below a single S2
  round-trip (≈ ms), so the F# mapping layer is free in practice. This is the
  honest "confirm it's negligible" result the proposal predicted.
- **Serialization is the cost that scales.** `Json.serialize` grows linearly with
  payload (~0.86 ns/byte; 261 ns small → 883 µs at 1 MiB). For large messages,
  serialization — not the wrapper or the SDK call shape — dominates client-side
  CPU. This is also exactly the path that crosses the >1 MiB chunking boundary,
  so it is the right thing to keep an eye on if a faster serializer is ever
  considered (the open question in the proposal).
- **Deserialize is cheaper than serialize** (186 ns vs 261 ns small) — `JSON.parse`
  + `TextDecoder` beats `JSON.stringify` + `TextEncoder` here.

# Tier 2 — live E2E throughput

Network-bound, on-demand (**not** CI). Records/sec for the append/read APIs
against the live AWS endpoint, on an ephemeral stream (`N = 100`). **Compare the
ratios, not the absolute numbers** — they are dominated by round-trip latency to
the region.

## Run it

```bash
npm run bench:e2e
```

## Snapshot

Environment: Apple M4 · Node 24.14.1 · AWS S2 (`aws.s2.dev`), ~100 ms RTT.

| API | rec/sec | vs unary |
|-----|--------:|---------:|
| unary `append` (1 rec / call) | 9.7 | 1× |
| session `submitAck` (1 rec) | 10.3 | ~1× |
| patterns `producer.submit` (typed) | 10.3 | ~1× |
| session **pipelined** (submit-all → ack-all) | 465 | **~48×** |
| **batched** `append` (100 recs / 1 call) | 1064 | **~110×** |
| `read` (bulk, 501 recs / 1 call) | 1392 | — |

## What it tells us

- **One-round-trip-per-record APIs are latency-bound.** `unary append`,
  `session.submitAck`, and `producer.submit` all land at ~10 rec/sec — each
  awaits durability of a single record, so throughput ≈ 1/RTT. Use them when
  volume is low or you need strict per-record acknowledgement.
- **Batch or pipeline for throughput.** A single batched `append` (many records
  in one call) is **~110×** faster; **pipelining** a session (submit many, then
  await the tickets) is **~48×**. Either converts a latency-bound workload into a
  bandwidth-bound one.
- **`producer.submit` is durable-per-message** (like `submitAck`), so it inherits
  the same latency bound. For high-throughput typed messaging you would pipeline
  it — the package's `WritableStream`/`pipeTo` path, which our wrapper does not
  yet expose. A clear next ergonomic addition if/when it is needed.
- **Reads are bulk by default** — one `read` returned 501 records at ~1400 rec/sec.

All numbers are single-machine, single-region, single-run and indicative. Re-run
`npm run bench` / `npm run bench:e2e` on the target hardware/region before
trusting small deltas.
