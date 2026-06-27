// Tier 1 — deterministic CPU micro-benchmarks (see docs/benchmarking-proposal.md).
//
// Runs in Node via mitata (the JS analog of BenchmarkDotNet). No network, no
// token — measures only the CPU work our wrapper owns: the patterns serializer
// and the append/read input builders.
//
//   npm run bench

#r "nuget: Fable.Core, 5.1.0"
#load "../src/S2/Interop.fs"
#load "../src/S2/Errors.fs"
#load "../src/S2/Client.fs"
#load "../src/S2/Cli.fs"
#load "../src/S2/Patterns.fs"

open System
open Fable.Core
open Eff

// ---- mitata bindings ----
[<Import("bench", "mitata")>]
let bench (name: string) (fn: unit -> 'a) : unit = jsNative

[<Import("run", "mitata")>]
let run () : JS.Promise<obj> = jsNative

// ---- fixtures ----
type Msg = { Id: int; Text: string }

let small = { Id = 1; Text = "hello world" }

let mid =
    { Id = 2
      Text = String('x', 64 * 1024) } // 64 KiB

let big =
    { Id = 3
      Text = String('x', 1024 * 1024) } // 1 MiB

let smallBytes = S2Patterns.Json.serialize small

let textRecords1 = [ S2.Record.text "hello" ]
let textRecords100 = [ for i in 1..100 -> S2.Record.text (sprintf "msg-%d" i) ]
let bytesRecords1 = [ S2.Record.bytes (Text.Encoding.UTF8.GetBytes "payload") ]

let readOpts =
    { S2.ReadOptions.empty with
        Start = Some(S2.FromSeqNum 0L)
        Count = Some 100
        IgnoreCommandRecords = true }

// ---- serialization: the real CPU we own (patterns Json) ----
bench "Json.serialize small (~30 B)" (fun () -> S2Patterns.Json.serialize small)
bench "Json.serialize 64 KiB" (fun () -> S2Patterns.Json.serialize mid)
bench "Json.serialize 1 MiB" (fun () -> S2Patterns.Json.serialize big)
bench "Json.deserialize small" (fun () -> S2Patterns.Json.deserialize<Msg> smallBytes)
bench "Json round-trip small" (fun () -> small |> S2Patterns.Json.serialize |> S2Patterns.Json.deserialize<Msg>)

// ---- wrapper mapping: the append/read build path (no network) ----
bench "Record.text (DU ctor)" (fun () -> S2.Record.text "hello")
bench "mkInput 1 text record" (fun () -> S2.mkInput textRecords1 S2.AppendOptions.none)
bench "mkInput 100 text records" (fun () -> S2.mkInput textRecords100 S2.AppendOptions.none)
bench "mkInput 1 bytes record" (fun () -> S2.mkInput bytesRecords1 S2.AppendOptions.none)
bench "readInput (full options)" (fun () -> S2.readInput readOpts)

async {
    let! _ = Async.AwaitPromise(run ())
    return ()
}
|> Async.StartImmediate
