namespace Eff.Proofs

open Eff.Foundation.Durable

module DurableSemanticsProof =
    let proof =
        Harness.proof "durable-semantics-tier1" (fun ctx ->
            async {
                do!
                    Harness.section
                        ctx
                        "activity replay"
                        (async {
                            let activity = { Name = "send-email"; Input = "42" }
                            let program = Durable.perform activity

                            match Durable.replay History.empty program with
                            | Blocked(OpId 0, NeedsActivity blocked) ->
                                Harness.expectEqual ctx "blocked activity matches" activity blocked
                            | other -> Harness.check ctx (sprintf "expected blocked activity, got %A" other) false

                            let completed =
                                History.empty
                                |> History.append (ActivityCalled(OpId 0, activity))
                                |> History.append (ActivityCompleted(OpId 0, "ok"))

                            let continued =
                                Durable.perform activity
                                |> Durable.bind (fun value -> Durable.result ("result:" + value))

                            match Durable.replay completed continued with
                            | Done value -> Harness.expectEqual ctx "completion is replayed once" "result:ok" value
                            | other -> Harness.check ctx (sprintf "expected completed replay, got %A" other) false
                        })

                do!
                    Harness.section
                        ctx
                        "event replay"
                        (async {
                            let timer = Durable.await (Timer 123L)

                            match Durable.replay (History.empty |> History.append (TimerFired(OpId 0))) timer with
                            | Done value -> Harness.expectEqual ctx "timer returns unit payload" "" value
                            | other -> Harness.check ctx (sprintf "expected fired timer, got %A" other) false

                            let signal = Durable.await (Signal "approved")

                            let signaled =
                                History.empty |> History.append (SignalReceived(OpId 0, "approved", "yes"))

                            match Durable.replay signaled signal with
                            | Done value -> Harness.expectEqual ctx "signal payload is replayed" "yes" value
                            | other -> Harness.check ctx (sprintf "expected signal replay, got %A" other) false
                        })

                do!
                    Harness.section
                        ctx
                        "substrate naming"
                        (async {
                            let key = StorageKey "orders/123"
                            Harness.expectEqual ctx "log stream" "orders/123/log" (StorageKey.logStreamName key)
                            Harness.expectEqual ctx "inbox stream" "orders/123/in" (StorageKey.inboxStreamName key)
                        })
            })
