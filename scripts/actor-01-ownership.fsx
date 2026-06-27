// Capability C4: Ownership (lease / fence)  —  Rung 1 of the durable-actor tower
//
// Companion SDD: docs/durable-actors-sdd.md
//
// The bug this kills: with only L1 (SubjectHistory), two hosts can both fold the
// log into state and both append. CAS-on-tail makes them race, but BOTH stay
// alive and interleave writes — that is two hosts fighting over one log, not one
// actor. Single-writership must be enforced by the STORE, not an in-memory lock.
//
// Proves:
//   P1 admission is mutually exclusive          (pure L1: appendExpected / CAS)
//   P2 fencing durably demotes a stale owner     (raw L0: fence token)
//   P3 the current owner IS the tail             (owner-local read needs no checkTail)
//
// Run:
//   dotnet fable scripts/actor-01-ownership.fsx --outDir build_script --runScript

#r "nuget: Fable.Core, 5.1.0"
#load "../src/S2/Interop.fs"
#load "../src/S2/Errors.fs"
#load "../src/S2/Client.fs"
#load "../src/S2/Cli.fs"
#load "../src/Foundation/SubjectHistory.fs"

open Fable.Core
open Eff
open Eff.Foundation

[<Emit("process.exit($0)")>]
let exit (code: int) : unit = jsNative

[<Emit("Date.now()")>]
let now () : float = jsNative

module Proof =
    let mutable passed = 0
    let mutable failed = 0
    let mutable failures: string list = []
    let mutable counter = 0

    let uniq (prefix: string) : string =
        counter <- counter + 1
        sprintf "%s-%d-%d" prefix (int64 (now ())) counter

    let check name condition =
        if condition then
            passed <- passed + 1
            printfn "  ok  %s" name
        else
            failed <- failed + 1
            failures <- name :: failures
            printfn "  bad %s" name

    let section name body =
        async {
            printfn "\n== %s ==" name

            try
                do! body
            with e ->
                failed <- failed + 1
                failures <- (name + " threw: " + e.Message) :: failures
                printfn "  bad threw: %s" e.Message
        }

    let finish () =
        printfn "\n%d passed, %d failed" passed failed

        if failed > 0 then
            failures |> List.rev |> List.iter (printfn "  - %s")
            exit 1
        else
            exit 0

// --- Admission vocabulary: a single typed "Claimed" record (pure L1, folds clean) ---
type Gov = Claimed of owner: string

module Gov =
    let encode (Claimed owner) = "claimed|" + owner

    let decode (body: string) =
        match body.Split('|') |> Array.toList with
        | [ "claimed"; owner ] -> Ok(Claimed owner)
        | _ -> Error(sprintf "unknown gov record: %s" body)

    let codec: SubjectHistory.Codec<Gov> = { Encode = encode; Decode = decode }

let basinName = "test-basin-885234"

let main =
    async {
        let s2 = S2Cli.connect ()
        let basin = s2 |> S2.basin basinName
        let created = System.Collections.Generic.List<string>()

        let createStream name =
            async {
                do! basin |> S2.createStream name
                created.Add(name)
            }

        try
            // ---- P1: admission is mutually exclusive (pure L1 CAS) ----
            let mxName = Proof.uniq "actor-ownership-mx"
            let mxSubject = SubjectHistory.SubjectId mxName
            do! createStream mxName

            do!
                Proof.section
                    "admission is mutually exclusive"
                    (async {
                        let! t0 = SubjectHistory.tail basin mxSubject
                        Proof.check "fresh subject starts at v0" (t0 = SubjectHistory.Version 0L)

                        let! a = SubjectHistory.appendExpected basin Gov.codec mxSubject t0 [ Claimed "host-A" ]
                        Proof.check "host A wins admission at v0" (a = Ok(SubjectHistory.Version 1L))

                        // host B races at the SAME (now stale) tail — exactly one winner
                        let! b = SubjectHistory.appendExpected basin Gov.codec mxSubject t0 [ Claimed "host-B" ]

                        match b with
                        | Error(SubjectHistory.AppendFailure.Conflict details) ->
                            Proof.check "host B admission conflicts" (details.Actual = SubjectHistory.Version 1L)

                            Proof.check
                                "conflict reveals host A as the winner"
                                (match details.Conflicting with
                                 | SubjectHistory.ConflictRecord.Found record -> record.Body = Claimed "host-A"
                                 | _ -> false)
                        | other -> Proof.check (sprintf "host B should conflict, got %A" other) false
                    })

            // ---- P2 + P3: fencing + owner-local read (raw L0 token; no typed fold here) ----
            let fenceName = Proof.uniq "actor-ownership-fence"
            do! createStream fenceName
            let raw = basin |> S2.stream fenceName

            do!
                Proof.section
                    "fencing durably demotes a stale owner"
                    (async {
                        let tokenA = Proof.uniq "token-A"

                        // owner A installs its fence token (no prior token to satisfy)
                        let! _ = raw |> S2.append [ S2.Record.fence tokenA; S2.Record.text "owner|host-A" ]

                        let! writeA =
                            raw
                            |> S2.tryAppendWith
                                (S2.AppendOptions.none |> S2.AppendOptions.fencingToken tokenA)
                                [ S2.Record.text "A-write-1" ]

                        Proof.check
                            "current owner A writes under its token"
                            (match writeA with
                             | Ok _ -> true
                             | _ -> false)

                        // takeover: owner B installs a new token, demoting A
                        let tokenB = Proof.uniq "token-B"
                        let! _ = raw |> S2.append [ S2.Record.fence tokenB; S2.Record.text "owner|host-B" ]

                        // A has NOT observed the takeover and retries under its old token
                        let! staleA =
                            raw
                            |> S2.tryAppendWith
                                (S2.AppendOptions.none |> S2.AppendOptions.fencingToken tokenA)
                                [ S2.Record.text "A-write-2" ]

                        Proof.check
                            "demoted owner A is fenced out by the store"
                            (match staleA with
                             | Error(S2Errors.FencingTokenMismatch _) -> true
                             | _ -> false)

                        // P3: the current owner's own ack tells it the tail — no checkTail needed
                        let! writeB =
                            raw
                            |> S2.tryAppendWith
                                (S2.AppendOptions.none |> S2.AppendOptions.fencingToken tokenB)
                                [ S2.Record.text "B-write-1" ]

                        match writeB with
                        | Ok ack ->
                            let! pos = raw |> S2.checkTail

                            Proof.check
                                "owner-local end equals checkTail (owner IS the tail)"
                                (ack.End.SeqNum = pos.SeqNum)
                        | other -> Proof.check (sprintf "owner B write should succeed, got %A" other) false
                    })

            for name in created do
                do! basin |> S2.deleteStream name

            Proof.finish ()
        with e ->
            printfn "\nfatal: %s" e.Message

            for name in created do
                try
                    do! basin |> S2.deleteStream name
                with cleanup ->
                    printfn "cleanup failed for %s: %s" name cleanup.Message

            Proof.failed <- Proof.failed + 1
            Proof.failures <- ("fatal: " + e.Message) :: Proof.failures
            Proof.finish ()
    }

main |> Async.StartImmediate
