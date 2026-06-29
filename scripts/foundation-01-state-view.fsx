// Capability C2: StateView
//
// Script-first proof for a host-local state view over SubjectHistory.
//
// Run:
//   dotnet fable scripts/foundation-01-state-view.fsx --outDir build_state_view --runScript

#r "nuget: Fable.Core, 5.1.0"
#load "../src/S2/Interop.fs"
#load "../src/S2/Errors.fs"
#load "../src/S2/Client.fs"
#load "../src/S2/Cli.fs"
#load "../src/Foundation/SubjectHistory.fs"
#load "../src/Foundation/StateView.fs"

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

    let checkFails name (expected: string) (body: Async<'a>) =
        async {
            try
                let! _ = body
                check name false
            with e ->
                check name (e.Message.Contains(expected))
        }

    let finish () =
        printfn "\n%d passed, %d failed" passed failed

        if failed > 0 then
            failures |> List.rev |> List.iter (printfn "  - %s")
            exit 1
        else
            exit 0

type CounterRecord =
    | Add of amount: int
    | Mark of label: string

module CounterRecord =
    let encode record =
        match record with
        | Add amount -> "add|" + string amount
        | Mark label -> "mark|" + label

    let decode (body: string) =
        let parts = body.Split('|') |> Array.toList

        match parts with
        | [ "add"; amount ] ->
            match System.Int32.TryParse amount with
            | true, value -> Ok(Add value)
            | false, _ -> Error("bad add amount: " + amount)
        | [ "mark"; label ] -> Ok(Mark label)
        | _ -> Error("unknown record body: " + body)

    let codec: SubjectHistory.Codec<CounterRecord> =
        { Encode = encode; Decode = decode }

module RawRecord =
    let codec: SubjectHistory.Codec<string> = { Encode = id; Decode = Ok }

type CounterState = { Total: int; Labels: string list }

module CounterState =
    let empty = { Total = 0; Labels = [] }

    let apply state (record: SubjectHistory.StoredRecord<CounterRecord>) =
        match record.Body with
        | Add amount ->
            { state with
                Total = state.Total + amount }
        | Mark label ->
            { state with
                Labels = state.Labels @ [ label ] }

let basinName = "test-basin-885234"

let main =
    async {
        let s2 = S2Cli.connect ()
        let basin = s2 |> S2.basin basinName
        let subjectName = Proof.uniq "foundation-state-view"
        let subject = SubjectHistory.SubjectId subjectName
        let mutable created = false
        let mutable view: StateView<CounterRecord, CounterState> option = None

        printfn "state-view subject stream: %s/%s" basinName subjectName

        try
            do! basin |> S2.createStream subjectName
            created <- true

            do!
                Proof.section
                    "start and fold existing history"
                    (async {
                        let! seeded = SubjectHistory.append basin CounterRecord.codec subject [ Add 3; Mark "seeded" ]

                        let! started =
                            StateView.start
                                basin
                                CounterRecord.codec
                                subject
                                (SubjectHistory.Seq 0L)
                                CounterState.empty
                                CounterState.apply

                        view <- Some started

                        let! strong = StateView.read Strong started

                        Proof.check "strong read catches up to seeded tail" (strong.AppliedTail = seeded)

                        Proof.check
                            "local fold state updates from tail records"
                            (strong.State.Total = 3 && strong.State.Labels = [ "seeded" ])
                    })

            do!
                Proof.section
                    "eventual read"
                    (async {
                        match view with
                        | None -> Proof.check "view started" false
                        | Some started ->
                            let! eventual = StateView.read Eventual started

                            Proof.check
                                "eventual read observes current local state immediately"
                                (eventual.State.Total = 3
                                 && eventual.State.Labels = [ "seeded" ]
                                 && eventual.AppliedTail = SubjectHistory.Version 2L)
                    })

            do!
                Proof.section
                    "strong read after follower append"
                    (async {
                        match view with
                        | None -> Proof.check "view started" false
                        | Some started ->
                            let! appended =
                                SubjectHistory.append basin CounterRecord.codec subject [ Add 4; Mark "after-start" ]

                            let! strong = StateView.read Strong started

                            Proof.check
                                "strong read catches up to append made after start"
                                (strong.AppliedTail = appended)

                            Proof.check
                                "strong read returns state through checked tail"
                                (strong.State.Total = 7 && strong.State.Labels = [ "seeded"; "after-start" ])
                    })

            match view with
            | Some started ->
                do! StateView.stop started
                view <- None
                Proof.check "stop closes the StateView cursor" true
            | None -> ()

            do!
                Proof.section
                    "terminal pump failure"
                    (async {
                        let failedSubjectName = Proof.uniq "foundation-state-view-failure"
                        let failedSubject = SubjectHistory.SubjectId failedSubjectName
                        let mutable failedCreated = false
                        let mutable failedView: StateView<CounterRecord, CounterState> option = None

                        let cleanup () =
                            async {
                                match failedView with
                                | Some started ->
                                    try
                                        do! StateView.stop started
                                    with cleanup ->
                                        printfn "failed-view cleanup failed: %s" cleanup.Message

                                    failedView <- None
                                | None -> ()

                                if failedCreated then
                                    do! basin |> S2.deleteStream failedSubjectName
                                    failedCreated <- false
                            }

                        try
                            do! basin |> S2.createStream failedSubjectName
                            failedCreated <- true
                            let! _ = SubjectHistory.append basin RawRecord.codec failedSubject [ "corrupt" ]

                            let! started =
                                StateView.start
                                    basin
                                    CounterRecord.codec
                                    failedSubject
                                    (SubjectHistory.Seq 0L)
                                    CounterState.empty
                                    CounterState.apply

                            failedView <- Some started

                            do!
                                Proof.checkFails
                                    "strong reads fail on pump decode error"
                                    "decode failed"
                                    (StateView.read Strong started)

                            do!
                                Proof.checkFails
                                    "eventual reads do not mask terminal pump failure"
                                    "decode failed"
                                    (StateView.read Eventual started)

                            do! StateView.stop started
                            failedView <- None
                            Proof.check "stop completes after terminal pump failure" true
                            do! cleanup ()
                        with e ->
                            do! cleanup ()
                            return raise e
                    })

            do! basin |> S2.deleteStream subjectName
            created <- false
            Proof.finish ()
        with e ->
            printfn "\nfatal: %s" e.Message

            match view with
            | Some started ->
                try
                    do! StateView.stop started
                with cleanup ->
                    printfn "view cleanup failed: %s" cleanup.Message
            | None -> ()

            if created then
                try
                    do! basin |> S2.deleteStream subjectName
                    printfn "deleted %s after failure" subjectName
                with cleanup ->
                    printfn "stream cleanup failed: %s" cleanup.Message

            Proof.failed <- Proof.failed + 1
            Proof.failures <- ("fatal: " + e.Message) :: Proof.failures
            Proof.finish ()
    }

main |> Async.StartImmediate
