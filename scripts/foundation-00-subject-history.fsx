// Capability C1: SubjectHistory
//
// Script-first proof for the lowest durable execution kernel boundary:
// one S2 stream as one authoritative subject history.
//
// This file is a *definition module*: it exposes `proof` and is executed by
// scripts/all.fsx through the shared harness in scripts/_prelude.fsx. Run it:
//
//   npm run script:subject-history          # this proof only
//   npm run scripts                         # this proof + every other one
//   PRESERVE=1 npm run script:subject-history   # keep the stream on failure

#r "nuget: Fable.Core, 5.1.0"
#load "../src/S2/Interop.fs"
#load "../src/S2/Errors.fs"
#load "../src/S2/Client.fs"
#load "../src/S2/Cli.fs"
#load "../src/Foundation/SubjectHistory.fs"
#load "_prelude.fsx"

module FoundationSubjectHistory

open Eff
open Eff.Foundation

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

let proof =
    Prelude.proof "foundation-00-subject-history" (fun ctx ->
        async {
            let s2 = S2Cli.connect ()
            let basin = s2 |> S2.basin Prelude.config.Basin
            let subjectName = Prelude.uniq ctx "foundation-subject-history"
            let subject = SubjectHistory.SubjectId subjectName

            printfn "subject stream: %s/%s" Prelude.config.Basin subjectName

            do! basin |> S2.createStream subjectName

            let deleteSubject () = basin |> S2.deleteStream subjectName
            Prelude.onCleanup ctx (sprintf "delete stream %s" subjectName) deleteSubject

            do!
                Prelude.section
                    ctx
                    "expected-sequence append"
                    (async {
                        let! tail0 = SubjectHistory.tail basin subject
                        Prelude.check ctx "new subject starts at v0" (tail0 = SubjectHistory.Version 0L)

                        let! appended =
                            SubjectHistory.appendExpected basin WorkRecord.codec subject tail0 [ Started "invoice-123" ]

                        Prelude.check ctx "append at expected v0 succeeds" (appended = Ok(SubjectHistory.Version 1L))

                        let! tail1 = SubjectHistory.tail basin subject
                        Prelude.check ctx "tail advances to v1" (tail1 = SubjectHistory.Version 1L)
                    })

            do!
                Prelude.section
                    ctx
                    "conflict semantics"
                    (async {
                        let stale = SubjectHistory.Version 0L

                        let! sameBody =
                            SubjectHistory.appendExpected basin WorkRecord.codec subject stale [ Started "invoice-123" ]

                        match sameBody with
                        | Error(SubjectHistory.AppendFailure.Conflict details) ->
                            Prelude.check
                                ctx
                                "same-body stale append still conflicts"
                                (details.Actual = SubjectHistory.Version 1L)

                            Prelude.check
                                ctx
                                "same-body conflict exposes winning record"
                                (match details.Conflicting with
                                 | SubjectHistory.ConflictRecord.Found record ->
                                     record.Seq = SubjectHistory.Seq 0L && record.Body = Started "invoice-123"
                                 | _ -> false)
                        | other -> Prelude.check ctx (sprintf "unexpected same-body outcome: %A" other) false

                        let! conflict =
                            SubjectHistory.appendExpected
                                basin
                                WorkRecord.codec
                                subject
                                stale
                                [ Started "different-input" ]

                        match conflict with
                        | Error(SubjectHistory.AppendFailure.Conflict details) ->
                            Prelude.check
                                ctx
                                "different stale append conflicts"
                                (details.Actual = SubjectHistory.Version 1L)

                            Prelude.check
                                ctx
                                "conflict exposes winning record"
                                (match details.Conflicting with
                                 | SubjectHistory.ConflictRecord.Found record -> record.Body = Started "invoice-123"
                                 | _ -> false)
                        | other -> Prelude.check ctx (sprintf "unexpected conflict outcome: %A" other) false
                    })

            do!
                Prelude.section
                    ctx
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

                        Prelude.check ctx "owner-style append advanced by 3" (after = SubjectHistory.Version 4L)

                        let! snapshot, applied =
                            SubjectHistory.foldTo
                                basin
                                WorkRecord.codec
                                subject
                                (SubjectHistory.Seq(SubjectHistory.versionNumber before))
                                after
                                WorkSnapshot.empty
                                WorkSnapshot.apply

                        Prelude.check ctx "fold applies through append tail" (applied = after)

                        Prelude.check
                            ctx
                            "fold records completed operation"
                            (snapshot.Completed.TryFind "reserve" = Some "reservation-777")

                        Prelude.check
                            ctx
                            "fold records pending timer"
                            (snapshot.PendingTimers.TryFind "timeout" = Some "2026-06-28T00:00:00Z")
                    })

            do!
                Prelude.section
                    ctx
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

                        Prelude.check ctx "follower catches up to checked tail" (applied = checkedTail)
                        Prelude.check ctx "follower observes folded state" (snapshot.Status = Waiting)
                    })
        })
