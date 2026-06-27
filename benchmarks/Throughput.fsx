// Tier 2 — live E2E throughput (see docs/benchmarking-proposal.md).
//
// Network-bound, on-demand, NOT part of CI. Measures records/sec for the
// different append/read APIs against the real S2 endpoint, on an ephemeral
// stream that is deleted afterward. Compare RATIOS between rows, not the
// absolute numbers (they are dominated by network latency to the region).
//
//   npm run bench:e2e

#r "nuget: Fable.Core, 5.1.0"
#load "../src/S2/Interop.fs"
#load "../src/S2/Errors.fs"
#load "../src/S2/Client.fs"
#load "../src/S2/Cli.fs"
#load "../src/S2/Patterns.fs"

open Fable.Core
open Eff

[<Emit("process.exit($0)")>]
let exit (code: int) : unit = jsNative

[<Emit("Date.now()")>]
let now () : float = jsNative

type Msg = { Id: int; Text: string }

/// Ops per per-call benchmark (each unary op is one network round-trip).
let N = 100

let body (i: int) = sprintf "msg-%06d" i

/// Time `work` (which performs `count` durable records) and print records/sec.
let measure (name: string) (count: int) (work: Async<unit>) =
    async {
        let t0 = now ()
        do! work
        let secs = max 1e-6 ((now () - t0) / 1000.0)
        printfn "  %-30s %9.1f rec/sec   (%d in %5.2fs)" name (float count / secs) count secs
    }

let run =
    async {
        let s2 = S2Cli.connect ()
        let basin = s2 |> S2.basin "test-basin-885234"
        let sname = sprintf "fsharp-bench-%d" (int64 (now ()))
        do! basin |> S2.ensureStream sname
        let stream = basin |> S2.stream sname
        printfn "stream: test-basin-885234/%s   (N = %d)\n" sname N

        // Warm the basin data-plane connection so the first bench isn't charged for TLS setup.
        do! stream |> S2.appendStrings [ "warmup" ] |> Async.Ignore

        printfn "append throughput:"

        do!
            measure
                "unary append (1 rec/call)"
                N
                (async {
                    for i in 1..N do
                        do! stream |> S2.appendStrings [ body i ] |> Async.Ignore
                })

        do!
            measure
                "batched append (N/1 call)"
                N
                (async {
                    let! _ = stream |> S2.appendStrings [ for i in 1..N -> body i ]
                    return ()
                })

        let! sess = stream |> S2.appendSession

        do!
            measure
                "session submitAck (1 rec)"
                N
                (async {
                    for i in 1..N do
                        do! sess |> S2.submitAck [ S2.Record.text (body i) ] |> Async.Ignore
                })

        do!
            measure
                "session pipelined"
                N
                (async {
                    let tickets = ResizeArray()

                    for i in 1..N do
                        let! t = sess |> S2.submit [ S2.Record.text (body i) ]
                        tickets.Add t

                    for t in tickets do
                        let! _ = S2.ack t
                        ()
                })

        do! sess |> S2.closeAppendSession

        let! p = stream |> S2Patterns.producer S2Patterns.Json.serialize

        do!
            measure
                "patterns producer.submit"
                N
                (async {
                    for i in 1..N do
                        do! p |> S2Patterns.submit { Id = i; Text = body i } |> Async.Ignore
                })

        do! p |> S2Patterns.closeProducer

        printfn "\nread throughput:"
        let! tail = stream |> S2.checkTail
        let toRead = min 1000 (int tail.SeqNum)

        do!
            measure
                "read (unary, bulk)"
                toRead
                (async {
                    let! _ = stream |> S2.read (S2.FromSeqNum 0L) toRead
                    return ()
                })

        do! basin |> S2.deleteStream sname
        printfn "\ncleaned up %s" sname
    }

async {
    printfn "S2 F# Tier 2 — live E2E throughput\n"

    try
        do! run
    with e ->
        printfn "✗ %s" e.Message

    exit 0
}
|> Async.StartImmediate
