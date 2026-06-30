namespace Eff.Proofs

open Eff
open Eff.Foundation.Durable

module DurableStepperProof =
    type DurableStepperResult =
        { ActivityDispatchOnce: bool
          FanOutDispatchesMissingOnly: bool
          TimerDispatchOnce: bool
          WhenAnyCancelsLosingTimer: bool
          TimeLogTimerSequence: bool
          StepRecordCodecRoundTrips: bool
          StepRecordCodecRejectsMalformed: bool
          S2CommitPersistsPlan: bool
          S2WaitingPlanDoesNotCommit: bool }

    let private commitRecords =
        function
        | Commit records -> records
        | Complete _
        | Waiting _ -> []

    let private appendEvents history records =
        let appended =
            records
            |> List.choose (function
                | Incoming(HistoryEvent event) -> Some event
                | _ -> None)

        History.ofList (History.toList history @ appended)

    let private forceDecoded entries =
        entries
        |> List.map (fun (_, decoded) ->
            match decoded with
            | Ok entry -> entry
            | Error error -> failwith error)

    let private checkActivityDispatchOnce () =
        let activity = Activities.create "send-email" "42"
        let program = Workflow.call activity.Name activity.Input

        let first = DurableStepper.plan 100L History.empty program
        let firstRecords = commitRecords first
        let history = appendEvents History.empty firstRecords
        let second = DurableStepper.plan 101L history program
        let completed = history |> History.append (ActivityCompleted(OpId 0, "ok"))

        let completes =
            match DurableStepper.plan 102L completed program with
            | Complete value -> value = "ok"
            | _ -> false

        firstRecords = [ Incoming(HistoryEvent(ActivityCalled(OpId 0, activity)))
                         Outgoing(Command(CallActivity(OpId 0, activity))) ]
        && (match second with
            | Waiting(OpId 0, NeedsActivity blocked) -> blocked = activity
            | _ -> false)
        && completes

    let private checkFanOutMissingOnly () =
        let activities =
            [ Activities.create "copy" "a"
              Activities.create "copy" "b"
              Activities.create "copy" "c" ]

        let program = Workflow.all activities

        let partial =
            History.empty |> History.append (ActivityCalled(OpId 0, activities.[0]))

        let plan = DurableStepper.plan 100L partial program

        let records = commitRecords plan

        records = [ Incoming(HistoryEvent(ActivityCalled(OpId 1, activities.[1])))
                    Outgoing(Command(CallActivity(OpId 1, activities.[1])))
                    Incoming(HistoryEvent(ActivityCalled(OpId 2, activities.[2])))
                    Outgoing(Command(CallActivity(OpId 2, activities.[2]))) ]

    let private checkTimerDispatchOnce () =
        let program = Workflow.sleepUntil 5000L
        let first = DurableStepper.plan 100L History.empty program
        let records = commitRecords first
        let history = appendEvents History.empty records

        records = [ Incoming(HistoryEvent(TimerCreated(OpId 0, 5000L)))
                    Outgoing(Command(ScheduleTimer(OpId 0, 5000L))) ]
        && (match DurableStepper.plan 101L history program with
            | Waiting(OpId 0, NeedsEvent(Timer 5000L)) -> true
            | _ -> false)

    let private checkWhenAnyCancelsLosingTimer () =
        let program =
            durable {
                let! winner = Workflow.any [ DurableTask.signal "approved"; DurableTask.timer 30L ]

                match winner with
                | EventWon(0, Signal "approved", payload) -> return "approved:" + payload
                | EventWon(1, Timer _, _) -> return "timeout"
                | _ -> return "unexpected"
            }

        let racePlan = DurableStepper.plan 100L History.empty program
        let raceRecords = commitRecords racePlan

        let signaled =
            appendEvents History.empty raceRecords
            |> History.append (SignalReceived(OpId 0, "approved", "yes"))

        let cancelPlan = DurableStepper.plan 101L signaled program

        raceRecords = [ Incoming(HistoryEvent(TimerCreated(OpId 1, 30L)))
                        Outgoing(Command(ScheduleTimer(OpId 1, 30L))) ]
        && commitRecords cancelPlan = [ Incoming(HistoryEvent(TimerCanceled(OpId 1)))
                                        Outgoing(Command(CancelTimer(OpId 1))) ]

    let private checkTimeLogTimerSequence () =
        let program =
            durable {
                let! now = Workflow.currentTime
                do! Workflow.log ("observed:" + string now)
                do! Workflow.sleepUntil (now + 10L)
                return "done"
            }

        let timePlan = DurableStepper.plan 1000L History.empty program
        let timeHistory = appendEvents History.empty (commitRecords timePlan)

        let logPlan = DurableStepper.plan 1001L timeHistory program
        let logHistory = appendEvents timeHistory (commitRecords logPlan)

        let timerPlan = DurableStepper.plan 1002L logHistory program

        commitRecords timePlan = [ Incoming(HistoryEvent(CurrentTimeRecorded(OpId 0, 1000L))) ]
        && commitRecords logPlan = [ Incoming(HistoryEvent(LogEmitted(OpId 1, "observed:1000")))
                                     Outgoing(Command(WriteLog(OpId 1, "observed:1000"))) ]
        && commitRecords timerPlan = [ Incoming(HistoryEvent(TimerCreated(OpId 2, 1010L)))
                                       Outgoing(Command(ScheduleTimer(OpId 2, 1010L))) ]

    let private checkStepRecordCodecRoundTrips () =
        let records =
            [ Incoming(WorkflowStarted(WorkflowName "checkout|flow", "input:body"))
              Incoming(
                  HistoryEvent(
                      ActivityCalled(
                          OpId 7,
                          { Name = "act|name"
                            Input = "input:with|pipes" }
                      )
                  )
              )
              Incoming(HistoryEvent(ActivityCompleted(OpId 7, "done|value")))
              Incoming(HistoryEvent(CurrentTimeRecorded(OpId 8, 123456789L)))
              Incoming(HistoryEvent(LogEmitted(OpId 9, "log:message|with separators")))
              Incoming(HistoryEvent(TimerCreated(OpId 10, 444L)))
              Incoming(HistoryEvent(TimerFired(OpId 10)))
              Incoming(HistoryEvent(TimerCanceled(OpId 11)))
              Incoming(HistoryEvent(SignalReceived(OpId 12, "sig|nal", "pay:load|x")))
              Outgoing(Command(CallActivity(OpId 13, { Name = "call|x"; Input = "body:1" })))
              Outgoing(Command(ScheduleTimer(OpId 14, 999L)))
              Outgoing(Command(CancelTimer(OpId 15)))
              Outgoing(Command(WriteLog(OpId 16, "write|log:message"))) ]

        records
        |> List.forall (fun record -> StepRecordCodec.decode (StepRecordCodec.encode record) = Ok record)

    let private checkStepRecordCodecRejectsMalformed () =
        [ ""
          "event.timer-created|1:0"
          "in|event.timer-created|1:0"
          "out|command.timer|1:x"
          "in|event.unknown|1:0"
          "in|event.log|1:03:abc1:z" ]
        |> List.forall (StepRecordCodec.decode >> Result.isError)

    let private runWorkload ctx =
        ProofOperation.run
            ctx
            "durable.stepper"
            "durable-stepper"
            { ProofOperationOptions.empty with
                Key = Some "durable-stepper" }
            (async {
                let s2 = WorkloadContext.requireS2 ctx

                let suffix = string (int64 (Reports.nowMillis ()))
                let basinName = "durable-stepper-" + suffix
                let key = StorageKey("workflow-" + suffix)

                let! _ = s2.Client |> S2.createBasin basinName
                let basin = s2.Client |> S2.basin basinName
                do! S2Substrate.ensureStreams basin key

                let pair = S2Substrate.streams basin key
                let! owner = S2Substrate.claimWith (FenceToken "stepper/fence") pair

                let activity = Activities.create "reserve" "order-1"
                let program = Workflow.call activity.Name activity.Input
                let plan = DurableStepper.plan 100L History.empty program

                let! commit = DurableStepper.commit StepRecordCodec.encode owner plan

                let! rawReplay = S2Substrate.readLogText StepRecordCodec.decode owner
                let replayed = forceDecoded rawReplay
                let replayHistory = DurableStepper.historyFromRecords replayed

                let s2CommitPersistsPlan =
                    match commit with
                    | StepCommitted ack ->
                        ack.Start.SeqNum = 1L
                        && replayed = commitRecords plan
                        && History.toList replayHistory = [ ActivityCalled(OpId 0, activity) ]
                    | _ -> false

                let waitingPlan = DurableStepper.plan 101L replayHistory program
                let! waitingCommit = DurableStepper.commit StepRecordCodec.encode owner waitingPlan

                let s2WaitingPlanDoesNotCommit =
                    match waitingCommit, waitingPlan with
                    | StepNotRequired, Waiting(OpId 0, NeedsActivity blocked) -> blocked = activity
                    | _ -> false

                do! basin |> S2.deleteStream (StorageKey.inboxStreamName key)
                do! basin |> S2.deleteStream (StorageKey.logStreamName key)

                let result =
                    { ActivityDispatchOnce = checkActivityDispatchOnce ()
                      FanOutDispatchesMissingOnly = checkFanOutMissingOnly ()
                      TimerDispatchOnce = checkTimerDispatchOnce ()
                      WhenAnyCancelsLosingTimer = checkWhenAnyCancelsLosingTimer ()
                      TimeLogTimerSequence = checkTimeLogTimerSequence ()
                      StepRecordCodecRoundTrips = checkStepRecordCodecRoundTrips ()
                      StepRecordCodecRejectsMalformed = checkStepRecordCodecRejectsMalformed ()
                      S2CommitPersistsPlan = s2CommitPersistsPlan
                      S2WaitingPlanDoesNotCommit = s2WaitingPlanDoesNotCommit }

                do!
                    ctx.EmitSpan
                        "proof.durable_stepper.completed"
                        [ "proof.property", "durable-stepper"
                          "stepper.activity", string result.ActivityDispatchOnce
                          "stepper.timer", string result.TimerDispatchOnce
                          "stepper.codec", string result.StepRecordCodecRoundTrips
                          "stepper.s2_commit", string result.S2CommitPersistsPlan ]

                return result
            })

    let stepperProperty =
        property "durable-stepper" {
            s2Lite ""
            workload runWorkload

            verify (fun v ->
                [ v.Expect.Workload "activity dispatch is committed once" (fun result -> result.ActivityDispatchOnce)
                  v.Expect.Workload "fan-out dispatches missing activities only" (fun result ->
                      result.FanOutDispatchesMissingOnly)
                  v.Expect.Workload "timer dispatch is committed once" (fun result -> result.TimerDispatchOnce)
                  v.Expect.Workload "when-any cancels losing timer" (fun result -> result.WhenAnyCancelsLosingTimer)
                  v.Expect.Workload "time log timer sequence is planned" (fun result -> result.TimeLogTimerSequence)
                  v.Expect.Workload "step record codec round-trips all cases" (fun result ->
                      result.StepRecordCodecRoundTrips)
                  v.Expect.Workload "step record codec rejects malformed input" (fun result ->
                      result.StepRecordCodecRejectsMalformed)
                  v.Expect.Workload "S2 commit persists planned records" (fun result -> result.S2CommitPersistsPlan)
                  v.Expect.Workload "waiting plan does not commit" (fun result -> result.S2WaitingPlanDoesNotCommit)
                  v.Trace.SpanExists
                      "durable stepper proof span emitted"
                      "proof.durable_stepper.completed"
                      [ "proof.property", "durable-stepper" ]
                  v.Trace.Operation
                      "durable stepper operation was recorded"
                      ({ TraceOperationMatch.named "durable.stepper" with
                          Status = Some "ok"
                          OutputContains =
                              [ "ActivityDispatchOnce"
                                "FanOutDispatchesMissingOnly"
                                "StepRecordCodecRoundTrips"
                                "S2CommitPersistsPlan" ]
                          Count = Some 1 }) ])
        }

    let proof =
        proof "durable-stepper" {
            describedAs "Durable replay needs are translated into idempotent fenced step commits."
            property stepperProperty
        }
