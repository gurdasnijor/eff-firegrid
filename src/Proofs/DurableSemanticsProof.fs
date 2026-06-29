namespace Eff.Proofs

open Eff.Foundation.Durable

module DurableSemanticsProof =

    // --- A deterministic environment + driver, used to state the replay laws. ---
    // The driver resolves a blocked op to a fixed value and appends only the
    // completion the replayer reads (ActivityCompleted / TimerFired /
    // SignalReceived). A "crash" is just re-driving from a prefix of the history.

    let private resolveEvents (opId: OpId) (need: Need) : Event list =
        match need with
        | NeedsActivity activity -> [ ActivityCompleted(opId, "done:" + activity.Name) ]
        | NeedsActivities pending ->
            pending
            |> List.map (fun (id, activity) -> ActivityCompleted(id, "done:" + activity.Name))
        | NeedsEvent(Timer _) -> [ TimerFired opId ]
        | NeedsEvent(Signal name) -> [ SignalReceived(opId, name, "sig:" + name) ]

    /// Drive a program to completion from a starting history. Deterministic, so
    /// re-driving from any prefix must reproduce the same result and history.
    let private drive (start: History) (program: Durable<string>) : string * History =
        let rec loop history =
            match Durable.replay history program with
            | Done value -> value, history
            | Blocked(opId, need) -> loop (resolveEvents opId need |> List.fold (fun h e -> History.append e h) history)

        loop start

    // Representative program family the laws are checked against (enumerated, not
    // random — Fable has no FsCheck; this is the in-Node law suite).
    let private programs: (string * Durable<string>) list =
        [ "single activity", Durable.perform { Name = "a"; Input = "1" }

          "two activities chained",
          Durable.perform { Name = "reserve"; Input = "x" }
          |> Durable.bind (fun r -> Durable.perform { Name = "charge"; Input = r })
          |> Durable.bind (fun c -> Durable.result ("done:" + c))

          "activity, timer, activity",
          Durable.perform { Name = "reserve"; Input = "x" }
          |> Durable.bind (fun _ -> Durable.await (Timer 1000L))
          |> Durable.bind (fun _ -> Durable.perform { Name = "ship"; Input = "y" })

          "fan out fan in activities",
          Durable.performAll
              [ { Name = "backup"; Input = "a.txt" }
                { Name = "backup"; Input = "b.txt" }
                { Name = "backup"; Input = "c.txt" } ]
          |> Durable.bind (fun values -> Durable.result (String.concat "," values))

          "signal then activity",
          Durable.await (Signal "approved")
          |> Durable.bind (fun s -> Durable.perform { Name = "act"; Input = s })
          |> Durable.bind (fun r -> Durable.result ("final:" + r)) ]

    /// The replay-correctness theorem, decomposed into checks over one program.
    let private checkLaws ctx (label: string) (program: Durable<string>) =
        let result, full = drive History.empty program
        let events = History.toList full

        // L1 determinism — two clean drives agree on result and history.
        let result2, full2 = drive History.empty program

        Harness.check
            ctx
            (label + ": deterministic (two clean drives agree)")
            (result2 = result && History.toList full2 = events)

        // L2 effectively-once — replaying the full committed history finishes and
        // never re-dispatches a completed op.
        match Durable.replay full program with
        | Done value -> Harness.expectEqual ctx (label + ": effectively-once on replay") result value
        | other -> Harness.check ctx (sprintf "%s: expected Done on full replay, got %A" label other) false

        // L3 at-least-once — dropping the last completion re-blocks at that op
        // (a crash before the result was committed re-dispatches it).
        match List.rev events with
        | _ :: restRev ->
            match Durable.replay (History.ofList (List.rev restRev)) program with
            | Blocked _ -> Harness.check ctx (label + ": missing last completion re-blocks (at-least-once)") true
            | Done _ -> Harness.check ctx (label + ": expected re-dispatch when last completion dropped") false
        | [] -> ()

        // L4 crash/replay equivalence — re-driving from EVERY prefix of the
        // committed history reproduces the same result and the same final history.
        let prefixes = [ for i in 0 .. List.length events -> events |> List.truncate i ]

        let equivalent =
            prefixes
            |> List.forall (fun prefix ->
                let r, h = drive (History.ofList prefix) program
                r = result && History.toList h = events)

        Harness.check ctx (label + ": crash/replay equivalence over all prefixes") equivalent

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
                        "fan-out/fan-in replay"
                        (async {
                            let activities =
                                [ { Name = "copy"; Input = "a" }
                                  { Name = "copy"; Input = "b" }
                                  { Name = "copy"; Input = "c" } ]

                            let program =
                                Durable.performAll activities
                                |> Durable.bind (fun values -> Durable.result (String.concat "|" values))

                            match Durable.replay History.empty program with
                            | Blocked(OpId 0, NeedsActivities pending) ->
                                Harness.expectEqual
                                    ctx
                                    "batch dispatches positional op ids"
                                    [ OpId 0; OpId 1; OpId 2 ]
                                    (pending |> List.map fst)

                                Harness.expectEqual
                                    ctx
                                    "batch carries activities in source order"
                                    activities
                                    (pending |> List.map snd)
                            | other -> Harness.check ctx (sprintf "expected activity batch, got %A" other) false

                            let partial = History.empty |> History.append (ActivityCompleted(OpId 0, "a-done"))

                            match Durable.replay partial program with
                            | Blocked(OpId 0, NeedsActivities missing) ->
                                Harness.expectEqual
                                    ctx
                                    "partial batch re-dispatches only missing activities"
                                    [ OpId 1; OpId 2 ]
                                    (missing |> List.map fst)
                            | other -> Harness.check ctx (sprintf "expected partial batch block, got %A" other) false

                            let completedOutOfOrder =
                                History.empty
                                |> History.append (ActivityCompleted(OpId 2, "c-done"))
                                |> History.append (ActivityCompleted(OpId 0, "a-done"))
                                |> History.append (ActivityCompleted(OpId 1, "b-done"))

                            match Durable.replay completedOutOfOrder program with
                            | Done value ->
                                Harness.expectEqual
                                    ctx
                                    "batch result follows source order, not completion order"
                                    "a-done|b-done|c-done"
                                    value
                            | other -> Harness.check ctx (sprintf "expected completed batch, got %A" other) false
                        })

                do!
                    Harness.section
                        ctx
                        "replay laws (determinism, effectively-once, crash/resume)"
                        (async { programs |> List.iter (fun (label, program) -> checkLaws ctx label program) })

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
