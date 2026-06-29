// Capability C1: SubjectHistory
//
// Script-first proof for the lowest durable execution kernel boundary:
// one S2 stream as one authoritative subject history.
//
// Run:
//   dotnet fable scripts/foundation-00-subject-history.fsx --outDir build_script --runScript

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

type WorkRecord =
    | Started of input: string
    | StepRequested of opId: string
    | StepCompleted of opId: string * value: string
    | TimerRequested of opId: string * wakeAt: string

module WorkRecord =
    let encode record =
        match record with
        | Started input -> "started|" + input
        | StepRequested opId -> "step-requested|" + opId
        | StepCompleted(opId, value) -> sprintf "step-completed|%s|%s" opId value
        | TimerRequested(opId, wakeAt) -> sprintf "timer-requested|%s|%s" opId wakeAt

    let decode (body: string) =
        let parts = body.Split('|') |> Array.toList

        match parts with
        | [ "started"; input ] -> Ok(Started input)
        | [ "step-requested"; opId ] -> Ok(StepRequested opId)
        | [ "step-completed"; opId; value ] -> Ok(StepCompleted(opId, value))
        | [ "timer-requested"; opId; wakeAt ] -> Ok(TimerRequested(opId, wakeAt))
        | _ -> Error(sprintf "unknown record body: %s" body)

    let codec: SubjectHistory.Codec<WorkRecord> = { Encode = encode; Decode = decode }

type WorkStatus =
    | Empty
    | Running
    | Waiting

type WorkSnapshot =
    { Status: WorkStatus
      Completed: Map<string, string>
      PendingTimers: Map<string, string> }

module WorkSnapshot =
    let empty =
        { Status = Empty
          Completed = Map.empty
          PendingTimers = Map.empty }

    let apply snapshot (record: SubjectHistory.StoredRecord<WorkRecord>) =
        match record.Body with
        | Started _ -> { snapshot with Status = Running }
        | StepRequested _ -> snapshot
        | StepCompleted(opId, value) ->
            { snapshot with
                Completed = snapshot.Completed |> Map.add opId value }
        | TimerRequested(opId, wakeAt) ->
            { snapshot with
                Status = Waiting
                PendingTimers = snapshot.PendingTimers |> Map.add opId wakeAt }

let basinName = "test-basin-885234"

let main =
    async {
        let s2 = S2Cli.connect ()
        let basin = s2 |> S2.basin basinName
        let subjectName = Proof.uniq "foundation-subject-history"
        let subject = SubjectHistory.SubjectId subjectName
        let mutable created = false

        printfn "subject stream: %s/%s" basinName subjectName

        try
            do! basin |> S2.createStream subjectName
            created <- true

            do!
                Proof.section
                    "expected-sequence append"
                    (async {
                        let! tail0 = SubjectHistory.tail basin subject
                        Proof.check "new subject starts at v0" (tail0 = SubjectHistory.Version 0L)

                        let! appended =
                            SubjectHistory.appendExpected basin WorkRecord.codec subject tail0 [ Started "invoice-123" ]

                        Proof.check "append at expected v0 succeeds" (appended = Ok(SubjectHistory.Version 1L))

                        let! tail1 = SubjectHistory.tail basin subject
                        Proof.check "tail advances to v1" (tail1 = SubjectHistory.Version 1L)
                    })

            do!
                Proof.section
                    "conflict semantics"
                    (async {
                        let stale = SubjectHistory.Version 0L

                        let! sameBody =
                            SubjectHistory.appendExpected basin WorkRecord.codec subject stale [ Started "invoice-123" ]

                        match sameBody with
                        | Error(SubjectHistory.AppendFailure.Conflict details) ->
                            Proof.check
                                "same-body stale append still conflicts with expected/actual versions"
                                (details.Expected = stale && details.Actual = SubjectHistory.Version 1L)

                            Proof.check
                                "same-body conflict exposes winning record"
                                (match details.Conflicting with
                                 | SubjectHistory.ConflictRecord.Found record ->
                                     record.Seq = SubjectHistory.Seq 0L && record.Body = Started "invoice-123"
                                 | _ -> false)
                        | other -> Proof.check (sprintf "unexpected same-body outcome: %A" other) false

                        let! conflict =
                            SubjectHistory.appendExpected
                                basin
                                WorkRecord.codec
                                subject
                                stale
                                [ Started "different-input" ]

                        match conflict with
                        | Error(SubjectHistory.AppendFailure.Conflict details) ->
                            Proof.check
                                "different stale append conflicts with expected/actual versions"
                                (details.Expected = stale && details.Actual = SubjectHistory.Version 1L)

                            Proof.check
                                "conflict exposes winning record"
                                (match details.Conflicting with
                                 | SubjectHistory.ConflictRecord.Found record ->
                                     record.Seq = SubjectHistory.Seq 0L && record.Body = Started "invoice-123"
                                 | _ -> false)
                        | other -> Proof.check (sprintf "unexpected conflict outcome: %A" other) false
                    })

            do!
                Proof.section
                    "typed cursor"
                    (async {
                        let! cursor = SubjectHistory.openCursor basin WorkRecord.codec subject (SubjectHistory.Seq 0L)

                        let! first = SubjectHistory.tryNext cursor
                        do! SubjectHistory.closeCursor cursor

                        Proof.check
                            "openCursor and tryNext expose first typed record"
                            (match first with
                             | Ok(Some record) ->
                                 record.Seq = SubjectHistory.Seq 0L && record.Body = Started "invoice-123"
                             | _ -> false)
                    })

            do!
                Proof.section
                    "deterministic fold"
                    (async {
                        let! before = SubjectHistory.tail basin subject

                        let! after =
                            SubjectHistory.append
                                basin
                                WorkRecord.codec
                                subject
                                [ StepRequested "reserve"
                                  StepCompleted("reserve", "reservation-777")
                                  TimerRequested("timeout", "2026-06-28T00:00:00Z") ]

                        Proof.check "owner-style append advanced by 3" (after = SubjectHistory.Version 4L)

                        let! snapshot, applied =
                            SubjectHistory.foldTo
                                basin
                                WorkRecord.codec
                                subject
                                (SubjectHistory.Seq(SubjectHistory.versionNumber before))
                                after
                                WorkSnapshot.empty
                                WorkSnapshot.apply

                        Proof.check "fold applies through append tail" (applied = after)

                        Proof.check
                            "fold applies records in order through target version"
                            (snapshot.Status = Waiting
                             && snapshot.Completed.TryFind "reserve" = Some "reservation-777"
                             && snapshot.PendingTimers.TryFind "timeout" = Some "2026-06-28T00:00:00Z")

                        Proof.check
                            "fold records completed operation"
                            (snapshot.Completed.TryFind "reserve" = Some "reservation-777")

                        Proof.check
                            "fold records pending timer"
                            (snapshot.PendingTimers.TryFind "timeout" = Some "2026-06-28T00:00:00Z")
                    })

            do!
                Proof.section
                    "follower read barrier"
                    (async {
                        let! checkedTail = SubjectHistory.tail basin subject

                        let! snapshot, applied =
                            SubjectHistory.foldTo
                                basin
                                WorkRecord.codec
                                subject
                                (SubjectHistory.Seq 0L)
                                checkedTail
                                WorkSnapshot.empty
                                WorkSnapshot.apply

                        Proof.check "follower catches up to checked tail" (applied = checkedTail)
                        Proof.check "follower observes folded state" (snapshot.Status = Waiting)
                    })

            do! basin |> S2.deleteStream subjectName
            created <- false
            Proof.finish ()
        with e ->
            printfn "\nfatal: %s" e.Message

            if created then
                try
                    do! basin |> S2.deleteStream subjectName
                    printfn "deleted %s after failure" subjectName
                with cleanup ->
                    printfn "cleanup failed: %s" cleanup.Message

            Proof.failed <- Proof.failed + 1
            Proof.failures <- ("fatal: " + e.Message) :: Proof.failures
            Proof.finish ()
    }

main |> Async.StartImmediate
