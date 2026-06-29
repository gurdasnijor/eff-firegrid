namespace Eff.Proofs

open Eff.Foundation.Durable

module DurableSemanticsProof =
    type DurableSemanticsResult =
        { ActivityReplay: bool
          EventReplay: bool
          FanOutReplay: bool
          WhenAnyReplay: bool
          TimeAndLoggingReplay: bool
          ReplayLaws: bool
          SubstrateNaming: bool }

    let private resolveEvents (opId: OpId) (need: Need) : Event list =
        match need with
        | NeedsActivity activity -> [ ActivityCompleted(opId, "done:" + activity.Name) ]
        | NeedsActivities pending ->
            pending
            |> List.map (fun (id, activity) -> ActivityCompleted(id, "done:" + activity.Name))
        | NeedsEvent(Timer _) -> [ TimerFired opId ]
        | NeedsEvent(Signal name) -> [ SignalReceived(opId, name, "sig:" + name) ]
        | NeedsRace pending ->
            match pending with
            | (id, RaceActivity activity) :: _ -> [ ActivityCompleted(id, "done:" + activity.Name) ]
            | (id, RaceEvent(Timer _)) :: _ -> [ TimerFired id ]
            | (id, RaceEvent(Signal name)) :: _ -> [ SignalReceived(id, name, "sig:" + name) ]
            | [] -> []
        | NeedsTimerCancellation timers -> timers |> List.map TimerCanceled
        | NeedsCurrentTime ->
            let (OpId value) = opId
            [ CurrentTimeRecorded(opId, 1000L + int64 value) ]
        | NeedsLog message -> [ LogEmitted(opId, message) ]

    let private drive (start: History) (program: Durable<string>) : string * History =
        let rec loop history =
            match Durable.replay history program with
            | Done value -> value, history
            | Blocked(opId, need) -> loop (resolveEvents opId need |> List.fold (fun h e -> History.append e h) history)

        loop start

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
          |> Durable.bind (fun r -> Durable.result ("final:" + r))

          "when-any signal or timeout",
          Durable.whenAny [ RaceEvent(Signal "approved"); RaceEvent(Timer 1000L) ]
          |> Durable.bind (fun winner ->
              match winner with
              | EventWon(0, Signal "approved", value) -> Durable.result ("approved:" + value)
              | EventWon(1, Timer _, _) -> Durable.result "timeout"
              | _ -> Durable.result "unexpected")

          "monitor time and log",
          Durable.currentTime
          |> Durable.bind (fun now ->
              Durable.log ("monitor:" + string now)
              |> Durable.bind (fun () ->
                  Durable.await (Timer(now + 10L))
                  |> Durable.bind (fun _ -> Durable.result ("done:" + string now)))) ]

    let private replayLawsHold (program: Durable<string>) =
        let result, full = drive History.empty program
        let events = History.toList full
        let result2, full2 = drive History.empty program

        let deterministic = result2 = result && History.toList full2 = events

        let effectivelyOnce =
            match Durable.replay full program with
            | Done value -> value = result
            | _ -> false

        let atLeastOnce =
            match List.rev events with
            | _ :: restRev ->
                match Durable.replay (History.ofList (List.rev restRev)) program with
                | Blocked _ -> true
                | Done _ -> false
            | [] -> true

        let crashEquivalent =
            [ for i in 0 .. List.length events -> events |> List.truncate i ]
            |> List.forall (fun prefix ->
                let r, h = drive (History.ofList prefix) program
                r = result && History.toList h = events)

        deterministic && effectivelyOnce && atLeastOnce && crashEquivalent

    let private checkActivityReplay () =
        let activity = { Name = "send-email"; Input = "42" }
        let program = Durable.perform activity

        let blocks =
            match Durable.replay History.empty program with
            | Blocked(OpId 0, NeedsActivity blocked) -> blocked = activity
            | _ -> false

        let completed =
            History.empty
            |> History.append (ActivityCalled(OpId 0, activity))
            |> History.append (ActivityCompleted(OpId 0, "ok"))

        let continued =
            Durable.perform activity
            |> Durable.bind (fun value -> Durable.result ("result:" + value))

        let replays =
            match Durable.replay completed continued with
            | Done value -> value = "result:ok"
            | _ -> false

        blocks && replays

    let private checkEventReplay () =
        let timer =
            match
                Durable.replay (History.empty |> History.append (TimerFired(OpId 0))) (Durable.await (Timer 123L))
            with
            | Done value -> value = ""
            | _ -> false

        let signal =
            let history =
                History.empty |> History.append (SignalReceived(OpId 0, "approved", "yes"))

            match Durable.replay history (Durable.await (Signal "approved")) with
            | Done value -> value = "yes"
            | _ -> false

        timer && signal

    let private checkFanOutReplay () =
        let activities =
            [ { Name = "copy"; Input = "a" }
              { Name = "copy"; Input = "b" }
              { Name = "copy"; Input = "c" } ]

        let program =
            Durable.performAll activities
            |> Durable.bind (fun values -> Durable.result (String.concat "|" values))

        let dispatches =
            match Durable.replay History.empty program with
            | Blocked(OpId 0, NeedsActivities pending) ->
                (pending |> List.map fst) = [ OpId 0; OpId 1; OpId 2 ]
                && (pending |> List.map snd) = activities
            | _ -> false

        let partialReplaysMissing =
            let partial = History.empty |> History.append (ActivityCompleted(OpId 0, "a-done"))

            match Durable.replay partial program with
            | Blocked(OpId 0, NeedsActivities missing) -> (missing |> List.map fst) = [ OpId 1; OpId 2 ]
            | _ -> false

        let preservesSourceOrder =
            let completedOutOfOrder =
                History.empty
                |> History.append (ActivityCompleted(OpId 2, "c-done"))
                |> History.append (ActivityCompleted(OpId 0, "a-done"))
                |> History.append (ActivityCompleted(OpId 1, "b-done"))

            match Durable.replay completedOutOfOrder program with
            | Done value -> value = "a-done|b-done|c-done"
            | _ -> false

        dispatches && partialReplaysMissing && preservesSourceOrder

    let private checkWhenAnyReplay () =
        let phoneVerification =
            Durable.whenAny [ RaceEvent(Signal "approved"); RaceEvent(Timer 5000L) ]
            |> Durable.bind (fun winner ->
                match winner with
                | EventWon(0, Signal "approved", payload) -> Durable.result ("approved:" + payload)
                | EventWon(1, Timer _, _) -> Durable.result "timeout"
                | _ -> Durable.result "unexpected")

        let dispatchesRace =
            match Durable.replay History.empty phoneVerification with
            | Blocked(OpId 0, NeedsRace pending) ->
                pending = [ OpId 0, RaceEvent(Signal "approved"); OpId 1, RaceEvent(Timer 5000L) ]
            | _ -> false

        let signalWinRequiresTimerCancel =
            let signaled =
                History.empty |> History.append (SignalReceived(OpId 0, "approved", "yes"))

            match Durable.replay signaled phoneVerification with
            | Blocked(OpId 0, NeedsTimerCancellation [ OpId 1 ]) -> true
            | _ -> false

        let signalWinAfterTimerCancel =
            let history =
                History.empty
                |> History.append (SignalReceived(OpId 0, "approved", "yes"))
                |> History.append (TimerCanceled(OpId 1))

            match Durable.replay history phoneVerification with
            | Done value -> value = "approved:yes"
            | _ -> false

        let timerWin =
            let history = History.empty |> History.append (TimerFired(OpId 1))

            match Durable.replay history phoneVerification with
            | Done value -> value = "timeout"
            | _ -> false

        let earliestHistoryEventWins =
            let history =
                History.empty
                |> History.append (TimerFired(OpId 1))
                |> History.append (SignalReceived(OpId 0, "approved", "late"))

            match Durable.replay history phoneVerification with
            | Done value -> value = "timeout"
            | _ -> false

        let activityWinCancelsTimer =
            let program =
                Durable.whenAny [ RaceActivity { Name = "call-phone"; Input = "555" }; RaceEvent(Timer 30L) ]
                |> Durable.bind (fun winner ->
                    match winner with
                    | ActivityWon(0, value) -> Durable.result ("activity:" + value)
                    | EventWon(1, Timer _, _) -> Durable.result "timeout"
                    | _ -> Durable.result "unexpected")

            let activityCompleted =
                History.empty |> History.append (ActivityCompleted(OpId 0, "ok"))

            let cancellationRequested =
                match Durable.replay activityCompleted program with
                | Blocked(OpId 0, NeedsTimerCancellation [ OpId 1 ]) -> true
                | _ -> false

            let completed =
                activityCompleted
                |> History.append (TimerCanceled(OpId 1))
                |> fun history ->
                    match Durable.replay history program with
                    | Done value -> value = "activity:ok"
                    | _ -> false

            cancellationRequested && completed

        dispatchesRace
        && signalWinRequiresTimerCancel
        && signalWinAfterTimerCancel
        && timerWin
        && earliestHistoryEventWins
        && activityWinCancelsTimer

    let private checkTimeAndLoggingReplay () =
        let timeThenLog =
            Durable.currentTime
            |> Durable.bind (fun now ->
                Durable.log ("observed:" + string now)
                |> Durable.bind (fun () -> Durable.result ("done:" + string now)))

        let currentTimeBlocks =
            match Durable.replay History.empty timeThenLog with
            | Blocked(OpId 0, NeedsCurrentTime) -> true
            | _ -> false

        let recordedTimeDrivesLogNeed =
            let history = History.empty |> History.append (CurrentTimeRecorded(OpId 0, 42L))

            match Durable.replay history timeThenLog with
            | Blocked(OpId 1, NeedsLog "observed:42") -> true
            | _ -> false

        let recordedLogReplaysWithoutDuplicate =
            let history =
                History.empty
                |> History.append (CurrentTimeRecorded(OpId 0, 42L))
                |> History.append (LogEmitted(OpId 1, "observed:42"))

            match Durable.replay history timeThenLog with
            | Done value -> value = "done:42"
            | _ -> false

        let rec monitor remaining =
            Durable.currentTime
            |> Durable.bind (fun now ->
                Durable.log (sprintf "monitor:%d:%d" remaining now)
                |> Durable.bind (fun () ->
                    if remaining = 0 then
                        Durable.result ("complete:" + string now)
                    else
                        Durable.await (Timer(now + 10L))
                        |> Durable.bind (fun _ -> monitor (remaining - 1))))

        let result, full = drive History.empty (monitor 2)
        let events = History.toList full

        let monitorCompletes = result.StartsWith("complete:")

        let logEvents =
            events
            |> List.choose (function
                | LogEmitted(_, message) -> Some message
                | _ -> None)

        let logsOncePerIteration =
            logEvents.Length = 3 && (logEvents |> Set.ofList |> Set.count) = 3

        let replayDoesNotAddLogNeeds =
            match Durable.replay full (monitor 2) with
            | Done replayed -> replayed = result
            | Blocked(_, NeedsLog _) -> false
            | _ -> false

        let crashEquivalent =
            [ for i in 0 .. events.Length -> events |> List.truncate i ]
            |> List.forall (fun prefix ->
                let replayed, completed = drive (History.ofList prefix) (monitor 2)
                replayed = result && History.toList completed = events)

        currentTimeBlocks
        && recordedTimeDrivesLogNeed
        && recordedLogReplaysWithoutDuplicate
        && monitorCompletes
        && logsOncePerIteration
        && replayDoesNotAddLogNeeds
        && crashEquivalent

    let private runWorkload ctx =
        ProofOperation.run
            ctx
            "durable.replay.laws"
            "durable-semantics-tier1"
            { ProofOperationOptions.empty with
                Key = Some "durable-semantics-tier1" }
            (async {
                let result =
                    { ActivityReplay = checkActivityReplay ()
                      EventReplay = checkEventReplay ()
                      FanOutReplay = checkFanOutReplay ()
                      WhenAnyReplay = checkWhenAnyReplay ()
                      TimeAndLoggingReplay = checkTimeAndLoggingReplay ()
                      ReplayLaws = programs |> List.forall (snd >> replayLawsHold)
                      SubstrateNaming =
                        let key = StorageKey "orders/123"

                        StorageKey.logStreamName key = "orders/123/log"
                        && StorageKey.inboxStreamName key = "orders/123/in" }

                do!
                    ctx.EmitSpan
                        "proof.durable_semantics.completed"
                        [ "proof.property", "durable-semantics-tier1"
                          "proof.activity_replay", string result.ActivityReplay
                          "proof.fan_out", string result.FanOutReplay
                          "proof.when_any", string result.WhenAnyReplay
                          "proof.time_logging", string result.TimeAndLoggingReplay ]

                return result
            })

    let private runBrokenReplayLaw ctx =
        ProofOperation.run
            ctx
            "durable.replay.laws.negative"
            "durable-semantics-tier1"
            { ProofOperationOptions.empty with
                Key = Some "durable-semantics-tier1" }
            (async {
                return
                    { ActivityReplay = true
                      EventReplay = true
                      FanOutReplay = true
                      WhenAnyReplay = true
                      TimeAndLoggingReplay = true
                      ReplayLaws = false
                      SubstrateNaming = true }
            })

    let private brokenReplayLawControl =
        negativeControl<DurableSemanticsResult> "broken replay law is caught" {
            workload runBrokenReplayLaw

            verify (fun v -> [ v.Expect.Workload "replay laws" (fun result -> result.ReplayLaws) ])

            expectFailure "replay laws"
        }

    let tier1 =
        property "durable-semantics-tier1" {
            workload runWorkload
            requiresNegativeControl

            verify (fun v ->
                [ v.Expect.Workload "activity replay" (fun result -> result.ActivityReplay)
                  v.Expect.Workload "event replay" (fun result -> result.EventReplay)
                  v.Expect.Workload "fan-out/fan-in replay" (fun result -> result.FanOutReplay)
                  v.Expect.Workload "when-any replay" (fun result -> result.WhenAnyReplay)
                  v.Expect.Workload "time/logging replay" (fun result -> result.TimeAndLoggingReplay)
                  v.Expect.Workload "replay laws" (fun result -> result.ReplayLaws)
                  v.Expect.Workload "substrate naming" (fun result -> result.SubstrateNaming)
                  v.Trace.SpanExists
                      "runner trace evidence is queryable"
                      "proof.durable_semantics.completed"
                      [ "proof.property", "durable-semantics-tier1" ]
                  v.Trace.Operation
                      "durable operation was recorded"
                      ({ TraceOperationMatch.named "durable.replay.laws" with
                          Status = Some "ok"
                          OutputContains = [ "ActivityReplay"; "TimeAndLoggingReplay"; "ReplayLaws" ]
                          Count = Some 1 }) ])

            negativeControl brokenReplayLawControl
        }

    let proof =
        proof "durable-semantics" {
            describedAs "Executable replay semantics and first substrate naming invariants."
            property tier1
        }
