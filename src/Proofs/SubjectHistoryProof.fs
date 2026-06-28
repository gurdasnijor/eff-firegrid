namespace Eff.Proofs

open Eff
open Eff.Foundation
open Eff.Foundation.WorkHistory

/// Capability C1: SubjectHistory — one S2 stream as one authoritative subject
/// history. This proof drives the foundation WorkHistory surface, which factors
/// the concrete event codec and fold out of the proof module.
module SubjectHistoryProof =

    let proof =
        Harness.proof "foundation-00-subject-history" (fun ctx ->
            async {
                let s2 = S2Cli.connect ()
                let basin = s2 |> S2.basin Harness.config.Basin
                let subjectName = Harness.uniq ctx "foundation-subject-history"
                let subject = SubjectHistory.SubjectId subjectName

                printfn "subject stream: %s/%s" Harness.config.Basin subjectName

                do! basin |> S2.createStream subjectName

                let deleteSubject () = basin |> S2.deleteStream subjectName
                Harness.onCleanup ctx (sprintf "delete stream %s" subjectName) deleteSubject

                do!
                    Harness.section
                        ctx
                        "expected-sequence append"
                        (async {
                            let! tail0 = SubjectHistory.tail basin subject
                            Harness.expectEqual ctx "new subject starts at v0" (SubjectHistory.Version 0L) tail0

                            let! appended = WorkHistory.appendExpected basin subject tail0 [ Started "invoice-123" ]

                            Harness.expectEqual
                                ctx
                                "append at expected v0 succeeds"
                                (Ok(SubjectHistory.Version 1L))
                                appended

                            let! tail1 = SubjectHistory.tail basin subject
                            Harness.expectEqual ctx "tail advances to v1" (SubjectHistory.Version 1L) tail1
                        })

                do!
                    Harness.section
                        ctx
                        "conflict semantics"
                        (async {
                            let stale = SubjectHistory.Version 0L

                            let! sameBody = WorkHistory.appendExpected basin subject stale [ Started "invoice-123" ]

                            match sameBody with
                            | Error(SubjectHistory.AppendFailure.Conflict details) ->
                                Harness.check
                                    ctx
                                    "same-body stale append still conflicts"
                                    (details.Actual = SubjectHistory.Version 1L)

                                Harness.check
                                    ctx
                                    "same-body conflict exposes winning record"
                                    (match details.Conflicting with
                                     | SubjectHistory.ConflictRecord.Found record ->
                                         record.Seq = SubjectHistory.Seq 0L && record.Body = Started "invoice-123"
                                     | _ -> false)
                            | other -> Harness.check ctx (sprintf "unexpected same-body outcome: %A" other) false

                            let! conflict =
                                WorkHistory.appendExpected basin subject stale [ Started "different-input" ]

                            match conflict with
                            | Error(SubjectHistory.AppendFailure.Conflict details) ->
                                Harness.check
                                    ctx
                                    "different stale append conflicts"
                                    (details.Actual = SubjectHistory.Version 1L)

                                Harness.check
                                    ctx
                                    "conflict exposes winning record"
                                    (match details.Conflicting with
                                     | SubjectHistory.ConflictRecord.Found record ->
                                         record.Body = Started "invoice-123"
                                     | _ -> false)
                            | other -> Harness.check ctx (sprintf "unexpected conflict outcome: %A" other) false
                        })

                do!
                    Harness.section
                        ctx
                        "deterministic fold"
                        (async {
                            let! before = SubjectHistory.tail basin subject

                            let! after =
                                WorkHistory.append
                                    basin
                                    subject
                                    [ StepRequested "reserve"
                                      StepCompleted("reserve", "reservation-777")
                                      TimerRequested("timeout", "2026-06-28T00:00:00Z") ]

                            Harness.expectEqual
                                ctx
                                "owner-style append advanced by 3"
                                (SubjectHistory.Version 4L)
                                after

                            let! snapshot, applied =
                                WorkHistory.foldTo
                                    basin
                                    subject
                                    (SubjectHistory.Seq(SubjectHistory.versionNumber before))
                                    after

                            Harness.note (sprintf "fold snapshot: %A" snapshot)
                            Harness.expectEqual ctx "fold applies through append tail" after applied

                            Harness.check
                                ctx
                                "fold records completed operation"
                                (snapshot.Completed.TryFind "reserve" = Some "reservation-777")

                            Harness.check
                                ctx
                                "fold records pending timer"
                                (snapshot.PendingTimers.TryFind "timeout" = Some "2026-06-28T00:00:00Z")
                        })

                do!
                    Harness.section
                        ctx
                        "follower read barrier"
                        (async {
                            let! checkedTail = SubjectHistory.tail basin subject

                            let! snapshot, applied =
                                WorkHistory.foldTo basin subject (SubjectHistory.Seq 0L) checkedTail

                            Harness.expectEqual ctx "follower catches up to checked tail" checkedTail applied
                            Harness.check ctx "follower observes folded state" (snapshot.Status = Waiting)
                        })
            })
