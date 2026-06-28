// S2 F# integration suite.
//
// Adapts the highest-signal scenarios from the TS SDK's *.e2e.test.ts and runs
// them THROUGH our F# client against the real S2 API. Each run uses an ephemeral
// stream (unique name) in test-basin-885234 and deletes it afterward.
//
//   Run:  npm test
//
// Exits non-zero if any check fails.

#r "nuget: Fable.Core, 5.1.0"
#load "../src/S2/Interop.fs"
#load "../src/S2/Errors.fs"
#load "../src/S2/Client.fs"
#load "../src/S2/Cli.fs"
#load "../src/S2/Patterns.fs"
#load "../src/Proofs/TraceSql.fs"

open Fable.Core
open Fable.Core.JsInterop
open Eff
open Eff.Proofs

[<Emit("process.exit($0)")>]
let exit (code: int) : unit = jsNative

[<Emit("Date.now()")>]
let now () : float = jsNative

let fs: obj = importAll "node:fs"
let os: obj = importAll "node:os"
let path: obj = importAll "node:path"

[<Emit("$0.mkdtempSync($1)")>]
let mkdtempSync (_fs: obj) (_prefix: string) : string = jsNative

[<Emit("$0.tmpdir()")>]
let tmpdir (_os: obj) : string = jsNative

[<Emit("$0.join($1, $2)")>]
let pathJoin (_path: obj) (_left: string) (_right: string) : string = jsNative

[<Emit("$0.writeFileSync($1, $2, 'utf8')")>]
let writeFileSync (_fs: obj) (_path: string) (_content: string) : unit = jsNative

[<Emit("$0.rmSync($1, { recursive: true, force: true })")>]
let rmSync (_fs: obj) (_path: string) : unit = jsNative

// ---- tiny assertion harness ----
let mutable passed = 0
let mutable failed = 0
let mutable failures: string list = []
let mutable counter = 0

let uniq (prefix: string) : string =
    counter <- counter + 1
    sprintf "%s-%d-%d" prefix (int64 (now ())) counter

let check (name: string) (cond: bool) =
    if cond then
        passed <- passed + 1
        printfn "  ✓ %s" name
    else
        failed <- failed + 1
        failures <- name :: failures
        printfn "  ✗ %s" name

let test (name: string) (body: Async<unit>) =
    async {
        printfn "• %s" name

        try
            do! body
        with e ->
            failed <- failed + 1
            failures <- (name + " — threw: " + e.Message) :: failures
            printfn "  ✗ threw: %s" e.Message
    }

// ---- message type for the patterns tests ----
type Msg = { Id: int; Text: string }

// ---- scenarios ----
let basinName = "test-basin-885234"

let suite =
    async {
        do!
            test
                "proof traces: chdb spanExists"
                (async {
                    let root = mkdtempSync fs (pathJoin path (tmpdir os) "eff-firegrid-proof-")
                    let spansJsonl = pathJoin path root "spans.jsonl"

                    try
                        writeFileSync
                            fs
                            spansJsonl
                            """{"trial_id":"trial-1","name":"proof.subject_history.append_expected","attributes":{"proof.subject":"subject-1","proof.expected.version":"0"}}
"""

                        let store: TraceSql.TraceStore =
                            { TrialId = "trial-1"
                              SpansJsonl = spansJsonl }

                        let! found =
                            TraceSql.spanExists "proof.subject_history.append_expected" [ "proof.subject", "subject-1" ]
                            |> TraceSql.exists store

                        check "spanExists finds proof-owned span" found

                        let! missing =
                            TraceSql.spanExists "proof.subject_history.append_expected" [ "proof.subject", "missing" ]
                            |> TraceSql.exists store

                        check "spanExists rejects missing span" (not missing)
                    finally
                        rmSync fs root
                })

        let s2 = S2Cli.connect ()
        let basin = s2 |> S2.basin basinName
        let sname = uniq "fsharp-test"
        printfn "test stream: %s/%s\n" basinName sname

        do!
            test
                "lifecycle: create + list"
                (async {
                    do! basin |> S2.createStream sname
                    let! streams = basin |> S2.listStreamsWith sname
                    check "listStreamsWith finds the stream" (streams |> List.exists (fun s -> s.Name = sname))
                })

        let stream = basin |> S2.stream sname

        do!
            test
                "append + read round-trip"
                (async {
                    let! ack = stream |> S2.appendStrings [ "a"; "b"; "c" ]
                    check "ack range is 0..3" (ack.Start.SeqNum = 0L && ack.End.SeqNum = 3L)
                    let! recs = stream |> S2.read (S2.FromSeqNum 0L) 100
                    check "read back 3 records" (List.length recs = 3)
                    check "bodies in order" ((recs |> List.map (fun r -> r.Body)) = [ "a"; "b"; "c" ])
                    check "seq nums 0,1,2" ((recs |> List.map (fun r -> r.SeqNum)) = [ 0L; 1L; 2L ])
                })

        do!
            test
                "conditional append (matchSeqNum)"
                (async {
                    let! tail = stream |> S2.checkTail
                    let! _ = stream |> S2.appendIfSeqNum tail.SeqNum [ S2.Record.text "cond-ok" ]
                    check "append at correct tail succeeds" true

                    let! r =
                        stream
                        |> S2.tryAppendWith
                            (S2.AppendOptions.none |> S2.AppendOptions.matchSeqNum 99999L)
                            [ S2.Record.text "no" ]

                    match r with
                    | Error(S2Errors.SeqNumMismatch _) -> check "wrong matchSeqNum -> SeqNumMismatch" true
                    | _ -> check "wrong matchSeqNum -> SeqNumMismatch" false
                })

        do!
            test
                "fencing token"
                (async {
                    let! _ = stream |> S2.append [ S2.Record.fence "tok-1" ]
                    let! _ = stream |> S2.appendIfFenced "tok-1" [ S2.Record.text "fenced-ok" ]
                    check "append with correct fencing token succeeds" true

                    let! r =
                        stream
                        |> S2.tryAppendWith
                            (S2.AppendOptions.none |> S2.AppendOptions.fencingToken "wrong")
                            [ S2.Record.text "no" ]

                    match r with
                    | Error(S2Errors.FencingTokenMismatch _) -> check "wrong fencing token -> FencingTokenMismatch" true
                    | _ -> check "wrong fencing token -> FencingTokenMismatch" false
                })

        do!
            test
                "bytes record"
                (async {
                    let! ack =
                        stream
                        |> S2.append [ S2.Record.bytes (System.Text.Encoding.UTF8.GetBytes "binpayload") ]

                    check "bytes append acknowledged" (ack.End.SeqNum > ack.Start.SeqNum)
                })

        do!
            test
                "range not satisfiable -> typed error"
                (async {
                    let! tail = stream |> S2.checkTail

                    try
                        // Reading from a seq num beyond the tail (no clamp) -> 416.
                        let! _ = stream |> S2.read (S2.FromSeqNum(tail.SeqNum + 1000L)) 10
                        check "expected a range error" false
                    with e ->
                        match S2Errors.classify e with
                        | S2Errors.RangeNotSatisfiable _ -> check "read beyond tail -> RangeNotSatisfiable" true
                        | other -> check (sprintf "expected RangeNotSatisfiable, got %A" other) false
                })

        do!
            test
                "sessions: append + bounded read"
                (async {
                    let! tail = stream |> S2.checkTail
                    let! sess = stream |> S2.appendSession
                    let! _ = sess |> S2.submitAck [ S2.Record.text "sess-1" ]
                    let! _ = sess |> S2.submitAck [ S2.Record.text "sess-2" ]
                    do! sess |> S2.closeAppendSession

                    let! rsess =
                        stream
                        |> S2.readSession
                            { S2.ReadOptions.empty with
                                Start = Some(S2.FromSeqNum tail.SeqNum) }

                    let! got = rsess |> S2.take 2
                    check "session read 2 records" (List.length got = 2)
                    check "session bodies" ((got |> List.map (fun r -> r.Body)) = [ "sess-1"; "sess-2" ])
                })

        do!
            test
                "stream config: create-with + get round-trip"
                (async {
                    let cname = uniq "fsharp-cfg"

                    let cfg =
                        { S2.StreamConfig.empty with
                            RetentionPolicy = Some(S2.RetainForSecs 86400) }

                    do! basin |> S2.createStreamWith cfg cname
                    let! got = basin |> S2.getStreamConfig cname
                    check "retention round-trips (86400s)" (got.RetentionPolicy = Some(S2.RetainForSecs 86400))
                    do! basin |> S2.deleteStream cname
                })

        do!
            test
                "ensure is idempotent"
                (async {
                    let! r = basin |> S2.ensureStreamWith S2.StreamConfig.empty sname
                    check "ensure on existing stream -> Noop/Updated" (r = S2.Noop || r = S2.Updated)
                })

        do!
            test
                "managers: locations"
                (async {
                    let! locs = s2 |> S2.listLocations
                    check "list locations is non-empty" (not (List.isEmpty locs))
                    let! def = s2 |> S2.getDefaultLocation
                    check "default location has a name" (def.Name <> "")
                })

        do!
            test
                "managers: account metrics"
                (async {
                    let endT = System.DateTime.UtcNow
                    let startT = endT.AddDays(-1.0)
                    let! m = s2 |> S2.accountMetrics S2.ActiveBasins startT endT
                    check "account metrics returns values" (not (List.isEmpty m))
                })

        do!
            test
                "managers: list tokens"
                (async {
                    let! _ = s2 |> S2.listTokens
                    check "list tokens succeeds" true
                })

        do!
            test
                "connectWith builds a working client"
                (async {
                    let s2b = S2.connectWith (S2.ConnectOptions.create (S2Cli.accessToken ()))
                    let! basins = s2b |> S2.listBasins
                    check "connectWith client lists basins" (not (List.isEmpty basins))
                })

        do!
            test
                "patterns: typed round-trip"
                (async {
                    let! tail0 = stream |> S2.checkTail
                    let! p = stream |> S2Patterns.producer S2Patterns.Json.serialize
                    let! _ = p |> S2Patterns.submit { Id = 1; Text = "hello" }
                    let! _ = p |> S2Patterns.submit { Id = 2; Text = "world" }
                    do! p |> S2Patterns.closeProducer

                    let! c =
                        stream
                        |> S2Patterns.consumer S2Patterns.Json.deserialize (S2.FromSeqNum tail0.SeqNum)

                    let! msgs = c |> S2Patterns.take 2
                    check "got 2 typed messages" (List.length msgs = 2)

                    check
                        "fields round-trip"
                        (match msgs with
                         | [ a; b ] -> a.Id = 1 && a.Text = "hello" && b.Id = 2 && b.Text = "world"
                         | _ -> false)
                })

        do!
            test
                "patterns: large message chunking (>1 MiB)"
                (async {
                    let! tail0 = stream |> S2.checkTail
                    let big = System.String('x', 1258291) // ~1.2 MiB -> must span >1 record
                    let! p = stream |> S2Patterns.producer S2Patterns.Json.serialize
                    let! _ = p |> S2Patterns.submit { Id = 99; Text = big }
                    do! p |> S2Patterns.closeProducer

                    let! c =
                        stream
                        |> S2Patterns.consumer S2Patterns.Json.deserialize (S2.FromSeqNum tail0.SeqNum)

                    let! msgs = c |> S2Patterns.take 1

                    check
                        "reassembled into 1 message"
                        (match msgs with
                         | [ m ] -> m.Id = 99 && m.Text.Length = big.Length
                         | _ -> false)
                })

        // cleanup
        do! basin |> S2.deleteStream sname
        printfn "\ncleaned up %s" sname
    }

async {
    printfn "S2 F# integration suite\n"

    try
        do! suite
    with e ->
        printfn "FATAL: %s" e.Message
        failed <- failed + 1

    printfn "\n%d passed, %d failed" passed failed

    for f in List.rev failures do
        printfn "  FAILED: %s" f

    exit (if failed > 0 then 1 else 0)
}
|> Async.StartImmediate
