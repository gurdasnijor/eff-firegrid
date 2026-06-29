namespace Eff.Proofs

open Eff
open Eff.Foundation

module FoundationSubjectHistoryProof =
    type SubjectHistoryProofResult =
        { EmptyTail: bool
          AppendExpectedAdvancesTail: bool
          StaleSameBodyConflicts: bool
          StaleDifferentBodyConflicts: bool
          ConflictReportsCommittedRecord: bool
          CursorReadsInOrder: bool
          FoldToVersion: bool
          FollowerCatchUpBarrier: bool }

    let private codec: SubjectHistory.Codec<string> = { Encode = id; Decode = Ok }

    let private foundBody expected =
        function
        | SubjectHistory.ConflictRecord.Found record -> record.Body = expected
        | _ -> false

    let private conflictAt expected actual expectedBody =
        function
        | Error(SubjectHistory.AppendFailure.Conflict conflict) ->
            conflict.Expected = expected
            && conflict.Actual = actual
            && foundBody expectedBody conflict.Conflicting
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
                    SubjectHistory.appendExpected basin codec subject (SubjectHistory.Version 0L) [ "alpha"; "beta" ]

                let appendExpectedAdvancesTail = appended = Ok(SubjectHistory.Version 2L)

                let! staleSame =
                    SubjectHistory.appendExpected basin codec subject (SubjectHistory.Version 0L) [ "alpha" ]

                let! staleDifferent =
                    SubjectHistory.appendExpected basin codec subject (SubjectHistory.Version 0L) [ "gamma" ]

                let staleSameBodyConflicts =
                    staleSame
                    |> conflictAt (SubjectHistory.Version 0L) (SubjectHistory.Version 2L) "alpha"

                let staleDifferentBodyConflicts =
                    staleDifferent
                    |> conflictAt (SubjectHistory.Version 0L) (SubjectHistory.Version 2L) "alpha"

                let conflictReportsCommittedRecord = staleDifferentBodyConflicts

                let! cursor = SubjectHistory.openCursor basin codec subject (SubjectHistory.Seq 0L)

                let! first = SubjectHistory.tryNext cursor
                let! second = SubjectHistory.tryNext cursor
                do! SubjectHistory.closeCursor cursor

                let cursorReadsInOrder =
                    first = Ok(
                        Some
                            { Seq = SubjectHistory.Seq 0L
                              Body = "alpha" }
                    )
                    && second = Ok(
                        Some
                            { Seq = SubjectHistory.Seq 1L
                              Body = "beta" }
                    )

                let! folded, foldedVersion =
                    SubjectHistory.foldTo
                        basin
                        codec
                        subject
                        (SubjectHistory.Seq 0L)
                        (SubjectHistory.Version 2L)
                        []
                        (fun state record -> state @ [ record.Body ])

                let foldToVersion =
                    folded = [ "alpha"; "beta" ] && foldedVersion = SubjectHistory.Version 2L

                let! durableWriteVersion = SubjectHistory.append basin codec subject [ "delta" ]
                let! checkedTail = SubjectHistory.tail basin subject

                let! caughtUp, caughtUpVersion =
                    SubjectHistory.foldTo
                        basin
                        codec
                        subject
                        (SubjectHistory.Seq 2L)
                        checkedTail
                        folded
                        (fun state record -> state @ [ record.Body ])

                let followerCatchUpBarrier =
                    durableWriteVersion = SubjectHistory.Version 3L
                    && checkedTail = SubjectHistory.Version 3L
                    && caughtUpVersion = checkedTail
                    && caughtUp = [ "alpha"; "beta"; "delta" ]

                let result =
                    { EmptyTail = emptyTail = SubjectHistory.Version 0L
                      AppendExpectedAdvancesTail = appendExpectedAdvancesTail
                      StaleSameBodyConflicts = staleSameBodyConflicts
                      StaleDifferentBodyConflicts = staleDifferentBodyConflicts
                      ConflictReportsCommittedRecord = conflictReportsCommittedRecord
                      CursorReadsInOrder = cursorReadsInOrder
                      FoldToVersion = foldToVersion
                      FollowerCatchUpBarrier = followerCatchUpBarrier }

                do!
                    ctx.EmitSpan
                        "proof.foundation.subject_history.completed"
                        [ "proof.property", "foundation.subject-history"
                          "foundation.empty_tail", string result.EmptyTail
                          "foundation.conflict", string result.StaleDifferentBodyConflicts
                          "foundation.fold", string result.FoldToVersion
                          "foundation.catch_up", string result.FollowerCatchUpBarrier ]

                return result
            })

    let subjectHistoryProperty =
        property "foundation.subject-history" {
            s2Lite ""
            workload runWorkload

            verify (fun v ->
                [ v.Expect.Workload "new subject starts at Version 0" (fun result -> result.EmptyTail)
                  v.Expect.Workload "appendExpected advances tail" (fun result -> result.AppendExpectedAdvancesTail)
                  v.Expect.Workload "same-body stale append is conflict" (fun result -> result.StaleSameBodyConflicts)
                  v.Expect.Workload "different stale append is conflict" (fun result ->
                      result.StaleDifferentBodyConflicts)
                  v.Expect.Workload "conflict reports committed record" (fun result ->
                      result.ConflictReportsCommittedRecord)
                  v.Expect.Workload "cursor reads records in order" (fun result -> result.CursorReadsInOrder)
                  v.Expect.Workload "foldTo applies through target version" (fun result -> result.FoldToVersion)
                  v.Expect.Workload "tail plus foldTo is follower catch-up barrier" (fun result ->
                      result.FollowerCatchUpBarrier)
                  v.Trace.SpanExists
                      "foundation proof span emitted"
                      "proof.foundation.subject_history.completed"
                      [ "proof.property", "foundation.subject-history" ]
                  v.Trace.Operation
                      "foundation operation was recorded"
                      ({ TraceOperationMatch.named "foundation.subject_history" with
                          Status = Some "ok"
                          OutputContains = [ "FollowerCatchUpBarrier"; "StaleSameBodyConflicts" ]
                          Count = Some 1 }) ])
        }

    let proof =
        proof "foundation.subject-history" {
            describedAs "SubjectHistory append, conflict, cursor, fold, and follower catch-up invariants."
            property subjectHistoryProperty
        }
