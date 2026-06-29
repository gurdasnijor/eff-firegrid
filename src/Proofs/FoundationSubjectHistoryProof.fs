namespace Eff.Proofs

open Eff
open Eff.Foundation

module FoundationSubjectHistoryProof =
    type WorkRecord =
        | Started of input: string
        | StepRequested of opId: string
        | StepCompleted of opId: string * value: string
        | TimerRequested of opId: string * wakeAt: string

    type WorkStatus =
        | Empty
        | Running
        | Waiting

    type WorkSnapshot =
        { Status: WorkStatus
          Completed: Map<string, string>
          PendingTimers: Map<string, string> }

    type SubjectHistoryProofResult =
        { EmptyTail: bool
          AppendAtExpectedVersion: bool
          TailAdvancesAfterAppend: bool
          StaleSameBodyConflicts: bool
          SameBodyConflictReportsWinningRecord: bool
          StaleDifferentBodyConflicts: bool
          DifferentConflictReportsWinningRecord: bool
          CursorReadsFirstTypedRecord: bool
          OwnerAppendAdvancedByCount: bool
          FoldAppliesThroughAppendTail: bool
          FoldAppliesRecordsInOrderThroughTarget: bool
          FoldRecordsCompletedOperation: bool
          FoldRecordsPendingTimer: bool
          FollowerCatchesUpToCheckedTail: bool
          FollowerObservesFoldedState: bool }

    module private WorkRecord =
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

    module private WorkSnapshot =
        let empty: WorkSnapshot =
            { Status = Empty
              Completed = Map.empty
              PendingTimers = Map.empty }

        let apply (snapshot: WorkSnapshot) (record: SubjectHistory.StoredRecord<WorkRecord>) =
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

    let private conflictVersions expected actual =
        function
        | Error(SubjectHistory.AppendFailure.Conflict conflict) ->
            conflict.Expected = expected && conflict.Actual = actual
        | _ -> false

    let private conflictRecord expectedSeq expectedBody =
        function
        | Error(SubjectHistory.AppendFailure.Conflict conflict) ->
            match conflict.Conflicting with
            | SubjectHistory.ConflictRecord.Found record -> record.Seq = expectedSeq && record.Body = expectedBody
            | _ -> false
        | _ -> false

    let private runWorkload ctx =
        ProofOperation.run
            ctx
            "foundation.subject_history"
            "foundation-subject-history"
            { ProofOperationOptions.empty with
                Key = Some "foundation-subject-history" }
            (async {
                let s2 = WorkloadContext.requireS2 ctx

                let suffix = string (int64 (Reports.nowMillis ()))
                let basinName = "fnd-" + suffix
                let subjectName = "subject-" + suffix
                let subject = SubjectHistory.SubjectId subjectName

                let! _ = s2.Client |> S2.createBasin basinName
                let basin = s2.Client |> S2.basin basinName
                do! basin |> S2.createStream subjectName

                let! emptyTail = SubjectHistory.tail basin subject

                let! appended =
                    SubjectHistory.appendExpected basin WorkRecord.codec subject emptyTail [ Started "invoice-123" ]

                let appendAtExpectedVersion = appended = Ok(SubjectHistory.Version 1L)

                let! tailAfterAppend = SubjectHistory.tail basin subject
                let tailAdvancesAfterAppend = tailAfterAppend = SubjectHistory.Version 1L

                let! staleSame =
                    SubjectHistory.appendExpected
                        basin
                        WorkRecord.codec
                        subject
                        (SubjectHistory.Version 0L)
                        [ Started "invoice-123" ]

                let! staleDifferent =
                    SubjectHistory.appendExpected
                        basin
                        WorkRecord.codec
                        subject
                        (SubjectHistory.Version 0L)
                        [ Started "different-input" ]

                let staleSameBodyConflicts =
                    staleSame
                    |> conflictVersions (SubjectHistory.Version 0L) (SubjectHistory.Version 1L)

                let sameBodyConflictReportsWinningRecord =
                    staleSame |> conflictRecord (SubjectHistory.Seq 0L) (Started "invoice-123")

                let staleDifferentBodyConflicts =
                    staleDifferent
                    |> conflictVersions (SubjectHistory.Version 0L) (SubjectHistory.Version 1L)

                let differentConflictReportsWinningRecord =
                    staleDifferent |> conflictRecord (SubjectHistory.Seq 0L) (Started "invoice-123")

                let! cursor = SubjectHistory.openCursor basin WorkRecord.codec subject (SubjectHistory.Seq 0L)

                let! first = SubjectHistory.tryNext cursor
                do! SubjectHistory.closeCursor cursor

                let cursorReadsFirstTypedRecord =
                    first = Ok(
                        Some
                            { Seq = SubjectHistory.Seq 0L
                              Body = Started "invoice-123" }
                    )

                let! beforeOwnerAppend = SubjectHistory.tail basin subject

                let! afterOwnerAppend =
                    SubjectHistory.append
                        basin
                        WorkRecord.codec
                        subject
                        [ StepRequested "reserve"
                          StepCompleted("reserve", "reservation-777")
                          TimerRequested("timeout", "2026-06-28T00:00:00Z") ]

                let ownerAppendAdvancedByCount = afterOwnerAppend = SubjectHistory.Version 4L

                let! folded, foldedVersion =
                    SubjectHistory.foldTo
                        basin
                        WorkRecord.codec
                        subject
                        (SubjectHistory.Seq(SubjectHistory.versionNumber beforeOwnerAppend))
                        afterOwnerAppend
                        WorkSnapshot.empty
                        WorkSnapshot.apply

                let foldAppliesThroughAppendTail = foldedVersion = afterOwnerAppend

                let foldAppliesRecordsInOrderThroughTarget =
                    folded.Status = Waiting
                    && folded.Completed.TryFind "reserve" = Some "reservation-777"
                    && folded.PendingTimers.TryFind "timeout" = Some "2026-06-28T00:00:00Z"

                let foldRecordsCompletedOperation =
                    folded.Completed.TryFind "reserve" = Some "reservation-777"

                let foldRecordsPendingTimer =
                    folded.PendingTimers.TryFind "timeout" = Some "2026-06-28T00:00:00Z"

                let! checkedTail = SubjectHistory.tail basin subject

                let! caughtUp, caughtUpVersion =
                    SubjectHistory.foldTo
                        basin
                        WorkRecord.codec
                        subject
                        (SubjectHistory.Seq 0L)
                        checkedTail
                        WorkSnapshot.empty
                        WorkSnapshot.apply

                let followerCatchesUpToCheckedTail = caughtUpVersion = checkedTail
                let followerObservesFoldedState = caughtUp.Status = Waiting

                do! basin |> S2.deleteStream subjectName

                let result =
                    { EmptyTail = emptyTail = SubjectHistory.Version 0L
                      AppendAtExpectedVersion = appendAtExpectedVersion
                      TailAdvancesAfterAppend = tailAdvancesAfterAppend
                      StaleSameBodyConflicts = staleSameBodyConflicts
                      SameBodyConflictReportsWinningRecord = sameBodyConflictReportsWinningRecord
                      StaleDifferentBodyConflicts = staleDifferentBodyConflicts
                      DifferentConflictReportsWinningRecord = differentConflictReportsWinningRecord
                      CursorReadsFirstTypedRecord = cursorReadsFirstTypedRecord
                      OwnerAppendAdvancedByCount = ownerAppendAdvancedByCount
                      FoldAppliesThroughAppendTail = foldAppliesThroughAppendTail
                      FoldAppliesRecordsInOrderThroughTarget = foldAppliesRecordsInOrderThroughTarget
                      FoldRecordsCompletedOperation = foldRecordsCompletedOperation
                      FoldRecordsPendingTimer = foldRecordsPendingTimer
                      FollowerCatchesUpToCheckedTail = followerCatchesUpToCheckedTail
                      FollowerObservesFoldedState = followerObservesFoldedState }

                do!
                    ctx.EmitSpan
                        "proof.foundation.subject_history.completed"
                        [ "proof.property", "foundation.subject-history"
                          "foundation.empty_tail", string result.EmptyTail
                          "foundation.conflict", string result.StaleDifferentBodyConflicts
                          "foundation.fold", string result.FoldAppliesThroughAppendTail
                          "foundation.catch_up", string result.FollowerCatchesUpToCheckedTail ]

                return result
            })

    let subjectHistoryProperty =
        property "foundation.subject-history" {
            s2Lite ""
            workload runWorkload

            verify (fun v ->
                [ v.Expect.Workload "new subject starts at Version 0" (fun result -> result.EmptyTail)
                  v.Expect.Workload "append at expected Version 0 succeeds" (fun result ->
                      result.AppendAtExpectedVersion)
                  v.Expect.Workload "tail advances after append" (fun result -> result.TailAdvancesAfterAppend)
                  v.Expect.Workload "same-body stale append is conflict" (fun result -> result.StaleSameBodyConflicts)
                  v.Expect.Workload "same-body conflict exposes winning record" (fun result ->
                      result.SameBodyConflictReportsWinningRecord)
                  v.Expect.Workload "different stale append is conflict" (fun result ->
                      result.StaleDifferentBodyConflicts)
                  v.Expect.Workload "different conflict exposes winning record" (fun result ->
                      result.DifferentConflictReportsWinningRecord)
                  v.Expect.Workload "typed cursor exposes first typed record" (fun result ->
                      result.CursorReadsFirstTypedRecord)
                  v.Expect.Workload "owner-style append advances by record count" (fun result ->
                      result.OwnerAppendAdvancedByCount)
                  v.Expect.Workload "fold applies through append tail" (fun result ->
                      result.FoldAppliesThroughAppendTail)
                  v.Expect.Workload "fold applies records in order through target" (fun result ->
                      result.FoldAppliesRecordsInOrderThroughTarget)
                  v.Expect.Workload "fold records completed operation" (fun result ->
                      result.FoldRecordsCompletedOperation)
                  v.Expect.Workload "fold records pending timer" (fun result -> result.FoldRecordsPendingTimer)
                  v.Expect.Workload "follower catches up to checked tail" (fun result ->
                      result.FollowerCatchesUpToCheckedTail)
                  v.Expect.Workload "follower observes folded state" (fun result -> result.FollowerObservesFoldedState)
                  v.Trace.SpanExists
                      "foundation proof span emitted"
                      "proof.foundation.subject_history.completed"
                      [ "proof.property", "foundation.subject-history" ]
                  v.Trace.Operation
                      "foundation operation was recorded"
                      ({ TraceOperationMatch.named "foundation.subject_history" with
                          Status = Some "ok"
                          OutputContains =
                              [ "FollowerCatchesUpToCheckedTail"
                                "StaleSameBodyConflicts"
                                "FoldRecordsPendingTimer" ]
                          Count = Some 1 }) ])
        }

    let proof =
        proof "foundation.subject-history" {
            describedAs "SubjectHistory append, conflict, cursor, fold, and follower catch-up invariants."
            property subjectHistoryProperty
        }
