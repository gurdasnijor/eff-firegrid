namespace Eff.Proofs

open Eff
open Eff.Foundation.Durable

module DurableTimerAdapterProof =
    type DurableTimerAdapterResult =
        { TimerDoesNotFireBeforeDeadline: bool
          TimerFiresAtDeadlineThroughInbox: bool
          CanceledTimerDoesNotFire: bool
          PublishedBeforeCheckpointRetryDoesNotDuplicate: bool
          HostTickAdvancesSleepUntil: bool }

    let private sleepProgram deadline =
        durable {
            do! Workflow.sleepUntil deadline
            return "awake"
        }

    let private options hostId timestamp =
        { DurableHostTickOptions.create hostId timestamp with
            MaxInboxRecords = 10
            MaxActivityCommands = 10
            MaxTimerCommands = 10 }

    let private forceDecoded entries =
        entries
        |> List.map (fun (seqNum, decoded) ->
            match decoded with
            | Ok entry -> seqNum, entry
            | Error error -> failwith error)

    let private readLog owner =
        async {
            let! raw = S2Substrate.readLogText StepRecordCodec.decode owner
            return forceDecoded raw
        }

    let private activityRegistry = ActivityRegistry.empty

    let private timerFiredCount entries =
        entries
        |> List.filter (function
            | _, Incoming(HistoryEvent(TimerFired(OpId 0))) -> true
            | _ -> false)
        |> List.length

    let private inboxTimerCount owner =
        async {
            try
                let! records = S2Substrate.readInbox 0L 100 owner

                return
                    records
                    |> List.filter (fun record ->
                        match InboxEnvelopeCodec.decode record.Body with
                        | Ok { Source = source
                               Message = FireTimer(OpId 0, _) } -> source = TimerCommandAdapter.timerSource
                        | Ok _ -> false
                        | Error error -> failwith error)
                    |> List.length
            with error ->
                match S2Errors.classify error with
                | S2Errors.RangeNotSatisfiable _ -> return 0
                | _ -> return raise error
        }

    let private appendPublishedTimer pair sourceSeqNum deadline =
        let envelope =
            { Source = TimerCommandAdapter.timerSource
              SourceSeqNum = sourceSeqNum
              Message = FireTimer(OpId 0, deadline) }

        S2Substrate.appendInboxText [] (InboxEnvelopeCodec.encode envelope) pair

    let private seedTimer owner deadline =
        S2Substrate.commitText
            StepRecordCodec.encode
            [ Incoming(HistoryEvent(TimerCreated(OpId 0, deadline)))
              Outgoing(Command(ScheduleTimer(OpId 0, deadline))) ]
            owner

    let private seedCanceledTimer owner deadline =
        S2Substrate.commitText
            StepRecordCodec.encode
            [ Incoming(HistoryEvent(TimerCreated(OpId 0, deadline)))
              Outgoing(Command(ScheduleTimer(OpId 0, deadline)))
              Incoming(HistoryEvent(TimerCanceled(OpId 0)))
              Outgoing(Command(CancelTimer(OpId 0))) ]
            owner

    let private timerSourceSeq entries =
        entries
        |> List.pick (function
            | seqNum, Outgoing(Command(ScheduleTimer(OpId 0, _))) -> Some seqNum
            | _ -> None)

    let private runWorkload ctx =
        ProofOperation.run
            ctx
            "durable.timer_adapter"
            "durable-timer-adapter"
            { ProofOperationOptions.empty with
                Key = Some "durable-timer-adapter" }
            (async {
                let s2 = WorkloadContext.requireS2 ctx

                let suffix = string (int64 (Reports.nowMillis ()))
                let basinName = "durable-timer-adapter-" + suffix
                let key = StorageKey("workflow-" + suffix)
                let canceledKey = StorageKey("canceled-" + suffix)
                let retryKey = StorageKey("retry-" + suffix)

                let! _ = s2.Client |> S2.createBasin basinName
                let basin = s2.Client |> S2.basin basinName

                do! S2Substrate.ensureStreams basin key
                do! S2Substrate.ensureStreams basin canceledKey
                do! S2Substrate.ensureStreams basin retryKey

                let pair = S2Substrate.streams basin key
                let canceledPair = S2Substrate.streams basin canceledKey
                let retryPair = S2Substrate.streams basin retryKey

                let! owner = S2Substrate.claimWith (FenceToken "timer-adapter/owner") pair

                let! first =
                    DurableHost.runOwnedTick (options "timer-adapter" 100L) activityRegistry owner (sleepProgram 500L)

                let! firstLog = readLog owner
                let! firstInboxCount = inboxTimerCount owner

                let timerDoesNotFireBeforeDeadline =
                    match first with
                    | DurableHostTickStatus.Advanced report ->
                        match report.Timers with
                        | Some timerReport ->
                            timerReport.Published = []
                            && timerReport.NotDue.IsSome
                            && firstInboxCount = 0
                            && timerFiredCount firstLog = 0
                        | None -> false
                    | _ -> false

                let! _beforeDeadline =
                    DurableHost.runOwnedTick (options "timer-adapter" 499L) activityRegistry owner (sleepProgram 500L)

                let! atDeadline =
                    DurableHost.runOwnedTick (options "timer-adapter" 500L) activityRegistry owner (sleepProgram 500L)

                let! afterAtDeadlineInboxCount = inboxTimerCount owner
                let! afterAtDeadlineLog = readLog owner

                let timerPublishedAtDeadline =
                    match atDeadline with
                    | DurableHostTickStatus.Advanced report ->
                        match report.Timers with
                        | Some timerReport ->
                            timerReport.Published.Length = 1
                            && afterAtDeadlineInboxCount = 1
                            && timerFiredCount afterAtDeadlineLog = 0
                        | None -> false
                    | _ -> false

                let! folded =
                    DurableHost.runOwnedTick (options "timer-adapter" 501L) activityRegistry owner (sleepProgram 500L)

                let! foldedLog = readLog owner

                let timerFiresAtDeadlineThroughInbox =
                    timerPublishedAtDeadline
                    && match folded with
                       | DurableHostTickStatus.Completed("awake", _) -> timerFiredCount foldedLog = 1
                       | _ -> false

                let! canceledOwner = S2Substrate.claimWith (FenceToken "timer-adapter/canceled") canceledPair
                let! _canceledSeed = seedCanceledTimer canceledOwner 300L

                let! canceled = TimerCommandAdapter.runOnce StepRecordCodec.decode 1000L 10 canceledOwner

                let! canceledInboxCount = inboxTimerCount canceledOwner

                let canceledTimerDoesNotFire =
                    match canceled with
                    | TimerCommandAdapterStatus.Processed report ->
                        report.Canceled = 1 && List.isEmpty report.Published && canceledInboxCount = 0
                    | _ -> false

                let! retryOwner = S2Substrate.claimWith (FenceToken "timer-adapter/retry") retryPair
                let! _retrySeed = seedTimer retryOwner 300L
                let! retryLog = readLog retryOwner
                let retrySourceSeq = timerSourceSeq retryLog
                let! _publishedButNotCheckpointed = appendPublishedTimer retryPair retrySourceSeq 300L

                let! retry = TimerCommandAdapter.runOnce StepRecordCodec.decode 1000L 10 retryOwner

                let! retryInboxCount = inboxTimerCount retryOwner

                let publishedBeforeCheckpointRetryDoesNotDuplicate =
                    match retry with
                    | TimerCommandAdapterStatus.Processed report ->
                        report.AlreadyPublished = 1
                        && List.isEmpty report.Published
                        && retryInboxCount = 1
                    | _ -> false

                let hostTickAdvancesSleepUntil =
                    timerDoesNotFireBeforeDeadline && timerFiresAtDeadlineThroughInbox

                do! basin |> S2.deleteStream (StorageKey.inboxStreamName retryKey)
                do! basin |> S2.deleteStream (StorageKey.logStreamName retryKey)
                do! basin |> S2.deleteStream (StorageKey.inboxStreamName canceledKey)
                do! basin |> S2.deleteStream (StorageKey.logStreamName canceledKey)
                do! basin |> S2.deleteStream (StorageKey.inboxStreamName key)
                do! basin |> S2.deleteStream (StorageKey.logStreamName key)

                let result =
                    { TimerDoesNotFireBeforeDeadline = timerDoesNotFireBeforeDeadline
                      TimerFiresAtDeadlineThroughInbox = timerFiresAtDeadlineThroughInbox
                      CanceledTimerDoesNotFire = canceledTimerDoesNotFire
                      PublishedBeforeCheckpointRetryDoesNotDuplicate = publishedBeforeCheckpointRetryDoesNotDuplicate
                      HostTickAdvancesSleepUntil = hostTickAdvancesSleepUntil }

                do!
                    ctx.EmitSpan
                        "proof.durable_timer_adapter.completed"
                        [ "proof.property", "durable-timer-adapter"
                          "timer.before_deadline", string result.TimerDoesNotFireBeforeDeadline
                          "timer.fires", string result.TimerFiresAtDeadlineThroughInbox
                          "timer.canceled", string result.CanceledTimerDoesNotFire
                          "timer.retry", string result.PublishedBeforeCheckpointRetryDoesNotDuplicate ]

                return result
            })

    let timerAdapterProperty =
        property "durable-timer-adapter" {
            s2Lite ""
            workload runWorkload

            verify (fun v ->
                [ v.Expect.Workload "timer does not fire before its deadline" (fun result ->
                      result.TimerDoesNotFireBeforeDeadline)
                  v.Expect.Workload "timer fires through inbox at its deadline" (fun result ->
                      result.TimerFiresAtDeadlineThroughInbox)
                  v.Expect.Workload "canceled timer does not fire" (fun result -> result.CanceledTimerDoesNotFire)
                  v.Expect.Workload "publish-before-checkpoint retry does not duplicate timer firing" (fun result ->
                      result.PublishedBeforeCheckpointRetryDoesNotDuplicate)
                  v.Expect.Workload "host tick advances sleepUntil through timer adapter and inbox fold" (fun result ->
                      result.HostTickAdvancesSleepUntil)
                  v.Trace.SpanExists
                      "durable timer adapter proof span emitted"
                      "proof.durable_timer_adapter.completed"
                      [ "proof.property", "durable-timer-adapter" ]
                  v.Trace.Operation
                      "durable timer adapter operation was recorded"
                      ({ TraceOperationMatch.named "durable.timer_adapter" with
                          Status = Some "ok"
                          OutputContains =
                              [ "TimerDoesNotFireBeforeDeadline"
                                "TimerFiresAtDeadlineThroughInbox"
                                "PublishedBeforeCheckpointRetryDoesNotDuplicate" ]
                          Count = Some 1 }) ])
        }

    let proof =
        proof "durable-timer-adapter" {
            describedAs
                "Committed timer commands publish deadline-respecting timer firings through the inbox and host tick."

            property timerAdapterProperty
        }
