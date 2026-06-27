# Eff Firegrid Benchmarking Proposal

## Status

Proposed.

## Context

`eff-firegrid` is a Fable/F# system that wraps and composes the S2 JavaScript
SDK (`@s2-dev/streamstore` + `@s2-dev/streamstore-patterns`) into F#-native
APIs. The surface is REPL/script-driven:

- `repl.fsx` is the live playground / full-surface tour.
- `tests/Suite.fsx` is the script-style integration suite (`npm test`).
- A static quality gate now exists (Fantomas + FSharpLint via `npm run check`).

The sibling repo `eff-sharp` benchmarks with **BenchmarkDotNet** — a dedicated
`benchmarks/Effect.Benchmarks` project, `[<MemoryDiagnoser; ShortRunJob>]`
types, `[<Benchmark(Baseline = true)>]` current-vs-native pairs, a
`BenchmarkSwitcher` entrypoint, and a committed `RESULTS.md`. That structure is
worth borrowing. The *tool* is not.

## Why not BenchmarkDotNet

BenchmarkDotNet measures **.NET execution**. eff-sharp can use it because its
code is pure F#/FSharp.Core data structures that run on both .NET and JS.

Our code cannot. The S2 client is a **Fable binding** — every operation bottoms
out in `import`/`[<Emit>]`/`jsNative` calls that only exist in JavaScript and
throw under the .NET runtime. There is nothing for BenchmarkDotNet to run.

So we keep eff-sharp's *shape* (dedicated dir, switcher, baseline-vs-alternative,
committed results) but run it as a **Node-native harness**: F# compiled by Fable
to JS, executed in Node — the same toolchain as `npm test` and `npm run play`.

## What is actually worth benchmarking

A binding has two very different cost profiles, and conflating them produces
misleading numbers.

1. **Wrapper overhead (negligible).** `toAck`, `toRecord`, option/config
   mapping — these are a handful of field reads per call and are dwarfed by the
   network round-trip. Microbenchmarking them in isolation measures noise.
2. **CPU work we own (real).** The patterns serialization path —
   `Json.serialize`/`deserialize`, and the producer's `serialize → chunk →
   frame` pipeline — is genuine, allocation-heavy CPU work whose cost scales
   with message size. This is deterministic and worth tracking.
3. **End-to-end throughput (real, but network-bound).** "How many records/sec
   can I push?" is the question users actually ask. It is dominated by S2 and
   the network, not our F#, but the *relative* comparison between API choices
   (unary vs session vs producer) is directly actionable.

This yields two tiers.

## Goals

1. Keep benchmarks script-first and consistent with the REPL workflow.
2. Author benchmarks in F#, compiled by Fable, run in Node — no second language.
3. Two tiers: deterministic CPU micro-benchmarks, and live E2E throughput.
4. Make the micro tier CI-safe (deterministic, no network, no token).
5. Frame results as baseline-vs-alternative where a decision is at stake
   (e.g. unary append vs session append vs producer).
6. Make adding a benchmark a one-line registration.
7. Commit a `RESULTS.md` snapshot, like eff-sharp.
8. Keep absolute numbers honest about machine/network dependence.

## Non-Goals

1. Do not use BenchmarkDotNet or any .NET-execution harness.
2. Do not microbenchmark wrapper field-mapping in isolation; it is network-noise.
3. Do not gate CI on live E2E throughput numbers (network variance is not a
   regression signal).
4. Do not build a benchmarking framework before two benchmarks need it.
5. Do not commit absolute E2E numbers as if they were portable; commit ratios
   and the environment, or keep them local.

## Core Idea

A `benchmarks/` directory with two Fable scripts and a committed results file:

```text
benchmarks/
  Micro.fsx        # Tier 1 — deterministic CPU, via mitata. CI-safe.
  Throughput.fsx   # Tier 2 — live S2 ops/sec. On-demand.
  RESULTS.md       # committed snapshot + how-to-run + environment
```

Each script `#load`s the production modules directly (same as `tests/Suite.fsx`)
and registers benchmarks. `npm run bench` compiles + runs in Node.

## Tier 1 — CPU micro-benchmarks (deterministic, CI-safe)

Use **[mitata](https://github.com/evanwashere/mitata)** — a tiny, accurate JS
microbenchmark library (ns/op, warmup, outlier handling; the JS analog of
BenchmarkDotNet). Bound via Fable interop:

```fsharp
[<Import("bench", "mitata")>]
let bench (name: string) (fn: unit -> unit) : unit = jsNative

[<Import("run", "mitata")>]
let run () : JS.Promise<obj> = jsNative
```

Targets — the CPU work we own, swept across message size to show the chunking
cliff (>1 MiB splits into multiple records + frame headers):

```fsharp
let small = { Id = 1; Text = "hello" }
let big = { Id = 2; Text = System.String('x', 2 * 1024 * 1024) }

bench "Json.serialize small" (fun () -> S2Patterns.Json.serialize small |> ignore)
bench "Json.serialize 2MiB" (fun () -> S2Patterns.Json.serialize big |> ignore)
bench "Json round-trip small" (fun () ->
    small |> S2Patterns.Json.serialize |> S2Patterns.Json.deserialize<Msg> |> ignore)

run () |> ignore
```

Where a real decision exists, pit current-vs-alternative as eff-sharp does
(e.g. `JSON.stringify`+`TextEncoder` vs a `Buffer`-based path, or a future
MessagePack serializer). The harness prints mitata's table; we paste the
relevant rows into `RESULTS.md`.

Adding a benchmark = one `bench "name" (fun () -> ...)` line. This tier is fast,
deterministic, needs no token, and can run in CI as a smoke (and later as a
regression guard).

## Tier 2 — E2E throughput (live, on-demand)

The actionable question is which append/read API to reach for. A tiny timing
helper over the real client answers it:

```fsharp
let timed (name: string) (n: int) (op: int -> Async<unit>) =
    async {
        let t0 = now ()
        for i in 1..n do
            do! op i
        let secs = (now () - t0) / 1000.0
        printfn "%-28s %8.0f ops/sec  (%d ops, %.2fs)" name (float n / secs) n secs
    }

// unary append vs pipelined session vs typed producer
do! timed "append (unary)" 200 (fun i -> stream |> S2.appendStrings [ sprintf "m%d" i ] |> Async.Ignore)
do! timed "appendSession.submitAck" 200 (fun i -> sess |> S2.submitAck [ S2.Record.text (sprintf "m%d" i) ] |> Async.Ignore)
do! timed "patterns.producer.submit" 200 (fun i -> p |> S2Patterns.submit { Id = i; Text = "m" } |> Async.Ignore)
```

This is the "current vs alternative" framing that helps a user choose — e.g.
"session append is N× unary throughput." It is network-bound and noisy, runs on
an ephemeral stream with cleanup (like the suite), is **not** part of CI, and
its absolute numbers are environment-specific. Report ratios, the region, and
the machine.

## Commands

```json
{
  "scripts": {
    "bench": "dotnet fable benchmarks/Micro.fsx --outDir build_bench && node build_bench/Micro.js",
    "bench:e2e": "dotnet fable benchmarks/Throughput.fsx --outDir build_bench && node build_bench/Throughput.js"
  }
}
```

Add `build_bench/` to `.gitignore`. mitata is a dev dependency
(`npm i -D mitata`).

## RESULTS.md

Mirror eff-sharp's `RESULTS.md`: a short intro, the run commands, an
environment line (OS, Node version, S2 region for E2E), and a results table.
Keep the micro table as committed truth (portable-ish, CPU-only) and mark the
E2E table as indicative.

## Milestones

### First milestone — micro harness

1. `benchmarks/Micro.fsx` with mitata bindings and the `Json` serialize /
   deserialize / round-trip benches across small + >1 MiB.
2. `npm run bench`.
3. `benchmarks/RESULTS.md` with the committed micro snapshot.

No E2E, no CI wiring, no regression thresholds yet.

### Second milestone — E2E throughput

1. `benchmarks/Throughput.fsx` comparing unary / session / producer append, and
   read throughput, on an ephemeral stream with cleanup.
2. `npm run bench:e2e`.
3. An E2E section in `RESULTS.md`, clearly marked indicative + environment-bound.

### Third milestone — CI smoke + regression guard

1. Run the micro tier in the CI quality job (deterministic, no token) purely to
   ensure it still compiles and runs.
2. Only after the micro numbers are stable, add an optional regression check
   (fail if a tracked bench regresses past a threshold vs a committed baseline).

## Design Principles

1. Benchmarks are Fable scripts that `#load` production modules directly.
2. Two tiers, never conflated: deterministic CPU vs network-bound E2E.
3. The micro tier is the CI-safe, regression-trackable one.
4. The E2E tier is a decision aid, not a gate; report ratios, not raw absolutes.
5. Baseline-vs-alternative wherever a real choice exists.
6. Adding a benchmark is one line; the harness handles warmup/stats.
7. `RESULTS.md` is committed and states its environment.

## Open Questions

1. mitata vs a tiny hand-rolled `performance.now()` timer for Tier 1 — mitata
   gives real stats for a small dep; is the dependency acceptable?
2. Should `RESULTS.md` commit absolute micro numbers, or only ratios + a
   "re-run locally" note, given machine variance?
3. For Tier 2, is `s2 lite` (local) a better, lower-noise target than the live
   AWS endpoint for relative comparisons?
4. What regression threshold (if any) is meaningful for the micro tier given
   Node JIT warm-up variance?
5. Should the producer micro-bench stub the network (measure only
   serialize→chunk→frame), and if so, how cleanly can we isolate that path from
   the JS package's session I/O?

## Recommendation

Start with **Tier 1 (mitata micro-benchmarks)** of the serialization path. It is
deterministic, CI-safe, needs no token, and is where the only CPU cost we own
actually lives. Keep it script-first and one-line-to-add.

Add **Tier 2 (E2E throughput)** next as a decision aid for the append/read API
choice, run on demand, reported as ratios.

Defer CI regression-gating until the micro numbers prove stable. Do not reach
for BenchmarkDotNet — it cannot run Fable/JS code.
