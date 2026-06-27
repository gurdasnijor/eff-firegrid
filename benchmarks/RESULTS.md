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

Numbers are single-machine, single-run indicative figures. Re-run with `npm run
bench` on the target hardware for decisions.
