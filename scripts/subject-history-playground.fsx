// SubjectHistory playground
//
// Interactive sketch for the layer below StateView/KvStore.
//
// Run:
//   dotnet fable scripts/subject-history-playground.fsx --outDir build_subject_history --runScript
//
// Edit `scenario` below to focus the output.

#r "nuget: Fable.Core, 5.1.0"
#load "../src/S2/Interop.fs"
#load "../src/S2/Errors.fs"
#load "../src/S2/Client.fs"
#load "../src/S2/Cli.fs"
#load "../src/Foundation/SubjectHistory.fs"

open Fable.Core
open Eff
open Eff.Foundation

[<Emit("Date.now()")>]
let now () : float = jsNative

[<Emit("process.exit($0)")>]
let exit (code: int) : unit = jsNative

type Scenario =
    | CasOnly
    | FoldOnly
    | All

let scenario = All
let cleanupStream = true

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

module Print =
    let line () =
        printfn "----------------------------------------------------------------"

    let version label version =
        printfn "%s v%d" label (SubjectHistory.versionNumber version)

    let outcome label outcome =
        match outcome with
        | Ok next -> printfn "%s appended -> v%d" label (SubjectHistory.versionNumber next)
        | Error(SubjectHistory.AppendFailure.Conflict conflict) ->
            printfn
                "%s conflict -> expected v%d, actual v%d"
                label
                (SubjectHistory.versionNumber conflict.Expected)
                (SubjectHistory.versionNumber conflict.Actual)

            match conflict.Conflicting with
            | SubjectHistory.ConflictRecord.Found record ->
                printfn "  conflicting record at seq %d: %A" (SubjectHistory.seqNumber record.Seq) record.Body
            | SubjectHistory.ConflictRecord.Unavailable -> printfn "  conflicting record unavailable"
            | SubjectHistory.ConflictRecord.LookupFailed message ->
                printfn "  conflicting record lookup failed: %s" message
        | Error(SubjectHistory.AppendFailure.Failed failure) -> printfn "%s failed -> %A" label failure

    let snapshot label snapshot version =
        printfn "%s at v%d:" label (SubjectHistory.versionNumber version)
        printfn "  status        = %A" snapshot.Status
        printfn "  completed     = %A" snapshot.Completed
        printfn "  pendingTimers = %A" snapshot.PendingTimers

module Lab =
    let casRace basin subject =
        async {
            Print.line ()
            printfn "CAS / expected-sequence append"
            printfn ""
            printfn "Use case: unowned admission records, create-run, external delivery dedup."
            printfn "Tradeoff: every concurrent writer races on the current tail."
            printfn "Note: same-body stale appends are still conflicts; caller-owned ids prove retries."
            printfn ""

            let! tail0 = SubjectHistory.tail basin subject
            Print.version "initial tail" tail0

            let start = Started "invoice-123"
            let! first = SubjectHistory.appendExpected basin WorkRecord.codec subject tail0 [ start ]
            Print.outcome "host A append Started" first

            let! sameBody = SubjectHistory.appendExpected basin WorkRecord.codec subject tail0 [ start ]
            Print.outcome "host A stale same-body append" sameBody

            let! different =
                SubjectHistory.appendExpected basin WorkRecord.codec subject tail0 [ Started "different-input" ]

            Print.outcome "host B stale different append" different
        }

    let foldHistory basin subject =
        async {
            Print.line ()
            printfn "Deterministic fold / materialization"
            printfn ""
            printfn "Use case: rebuild current state from authoritative subject history."
            printfn "Tradeoff: snapshots can accelerate this later, but the log remains authority."
            printfn ""

            let! before = SubjectHistory.tail basin subject

            let records =
                [ StepRequested "reserve"
                  StepCompleted("reserve", "reservation-777")
                  TimerRequested("timeout", "2026-06-28T00:00:00Z") ]

            let! after = SubjectHistory.append basin WorkRecord.codec subject records
            Print.version "unconditional owner-style append advanced to" after

            let! snapshot, applied =
                SubjectHistory.foldTo
                    basin
                    WorkRecord.codec
                    subject
                    (SubjectHistory.Seq(SubjectHistory.versionNumber before))
                    after
                    WorkSnapshot.empty
                    WorkSnapshot.apply

            Print.snapshot "folded suffix" snapshot applied
        }

    let followerCatchUp basin subject =
        async {
            Print.line ()
            printfn "Follower read barrier"
            printfn ""
            printfn "Use case: a non-owner wants a strong read."
            printfn "Mechanic: checkTail, then fold until local AppliedTail reaches that tail."
            printfn ""

            let! checkedTail = SubjectHistory.tail basin subject
            Print.version "checked tail" checkedTail

            let! snapshot, applied =
                SubjectHistory.foldTo
                    basin
                    WorkRecord.codec
                    subject
                    (SubjectHistory.Seq 0L)
                    checkedTail
                    WorkSnapshot.empty
                    WorkSnapshot.apply

            Print.snapshot "follower snapshot after catch-up" snapshot applied
        }

let main =
    async {
        let s2 = S2Cli.connect ()
        let basinName = "test-basin-885234"
        let basin = s2 |> S2.basin basinName
        let subjectName = sprintf "subject-history-playground-%d" (int64 (now ()))
        let subject = SubjectHistory.SubjectId subjectName

        do! basin |> S2.createStream subjectName

        printfn "subject stream: %s/%s" basinName subjectName

        match scenario with
        | CasOnly -> do! Lab.casRace basin subject
        | FoldOnly -> do! Lab.foldHistory basin subject
        | All ->
            do! Lab.casRace basin subject
            do! Lab.foldHistory basin subject
            do! Lab.followerCatchUp basin subject

        Print.line ()
        printfn ""
        printfn "What this does not prove yet:"
        printfn "- fenced owner-local reads"
        printfn "- lease expiry and self-demotion"
        printfn "- snapshot equivalence checks"
        printfn "- cross-subject command persistence barriers"

        if cleanupStream then
            do! basin |> S2.deleteStream subjectName
            printfn ""
            printfn "deleted ephemeral subject stream"
        else
            printfn ""
            printfn "left stream for inspection: %s/%s" basinName subjectName

        exit 0
    }

main |> Async.StartImmediate
