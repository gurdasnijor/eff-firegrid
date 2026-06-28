namespace Eff.Proofs

open Eff
open Eff.Foundation

/// The minimal durable-execution property over the log: journaled steps replay
/// effectively-once. A compiled proof — type-checked against the production
/// `EffectLedger` surface by `dotnet build` — that drives it against real S2 and
/// simulates a crash the only faithful way: discard in-memory state and re-derive
/// the ledger by re-folding the log (`EffectLedger.load`).
module EffectLedgerProof =

    let proof =
        Harness.proof "effect-ledger" (fun ctx ->
            async {
                let s2 = S2Cli.connect ()
                let basin = s2 |> S2.basin Harness.config.Basin
                let subjectName = Harness.uniq ctx "effect-ledger"
                let subject = SubjectHistory.SubjectId subjectName

                printfn "subject stream: %s/%s" Harness.config.Basin subjectName

                do! basin |> S2.createStream subjectName

                Harness.onCleanup ctx (sprintf "delete stream %s" subjectName) (fun () ->
                    basin |> S2.deleteStream subjectName)

                // an observable side effect: counts how many times it REALLY ran, per id
                let runs = ref Map.empty

                let bump id =
                    runs.Value <- Map.add id ((Map.tryFind id runs.Value |> Option.defaultValue 0) + 1) runs.Value

                let count id =
                    Map.tryFind id runs.Value |> Option.defaultValue 0

                let effect id : unit -> Async<string> =
                    fun () ->
                        async {
                            bump id
                            return sprintf "result-of-%d" id
                        }

                do!
                    Harness.section
                        ctx
                        "effectively-once: a completed step is not re-run on replay"
                        (async {
                            let! l0 = EffectLedger.load basin subject
                            Harness.expectEqual ctx "fresh ledger's next id is 0" 0 (EffectLedger.Ledger.nextId l0)

                            let! r0, _ = EffectLedger.step basin subject l0 0 "reserve" (effect 0)
                            Harness.expectEqual ctx "step 0 produced its result" "result-of-0" r0
                            Harness.check ctx "step 0 executed exactly once" (count 0 = 1)

                            // CRASH: drop in-memory state; recover by re-folding the log
                            let! recovered = EffectLedger.load basin subject

                            Harness.check
                                ctx
                                "recovered ledger shows step 0 completed"
                                (Map.containsKey 0 recovered.Completed)

                            // replaying the same step must skip and return the recorded result
                            let! r0replay, _ = EffectLedger.step basin subject recovered 0 "reserve" (effect 0)

                            Harness.expectEqual
                                ctx
                                "replayed step 0 returns the recorded result"
                                "result-of-0"
                                r0replay

                            Harness.check ctx "replayed step 0 did NOT re-execute" (count 0 = 1)
                        })

                do!
                    Harness.section
                        ctx
                        "at-least-once: a step interrupted before its result re-runs on recovery"
                        (async {
                            // simulate a crash AFTER the intent, BEFORE the result was journaled
                            let! _ =
                                SubjectHistory.append
                                    basin
                                    EffectLedger.Entry.codec
                                    subject
                                    [ EffectLedger.EffectRequested(1, "charge") ]

                            let! pending = EffectLedger.load basin subject

                            Harness.check
                                ctx
                                "step 1 is pending (requested, not completed)"
                                (Set.contains 1 pending.Requested && not (Map.containsKey 1 pending.Completed))

                            // recovery resumes the pending step — it MUST re-run (effects are idempotent)
                            let! r1, after = EffectLedger.step basin subject pending 1 "charge" (effect 1)
                            Harness.expectEqual ctx "recovered step 1 produced its result" "result-of-1" r1
                            Harness.check ctx "recovered step 1 executed once on resume" (count 1 = 1)
                            Harness.check ctx "recovered step 1 is now completed" (Map.containsKey 1 after.Completed)

                            // a further replay now skips it — convergence to effectively-once
                            let! settled = EffectLedger.load basin subject
                            let! _ = EffectLedger.step basin subject settled 1 "charge" (effect 1)
                            Harness.check ctx "step 1 not re-run after completion" (count 1 = 1)
                        })

                do!
                    Harness.section
                        ctx
                        "the ledger and step ids are a deterministic function of the log"
                        (async {
                            let! a = EffectLedger.load basin subject
                            let! b = EffectLedger.load basin subject
                            Harness.check ctx "two independent folds agree on Completed" (a.Completed = b.Completed)
                            Harness.check ctx "two independent folds agree on Requested" (a.Requested = b.Requested)

                            Harness.expectEqual
                                ctx
                                "two independent folds agree on the next id"
                                (EffectLedger.Ledger.nextId a)
                                (EffectLedger.Ledger.nextId b)
                        })
            })
