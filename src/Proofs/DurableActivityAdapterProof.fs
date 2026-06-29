namespace Eff.Proofs

open Eff
open Eff.Foundation.Durable

module DurableActivityAdapterProof =
    type DurableActivityAdapterResult =
        { CommittedCallInvokesHandler: bool
          CompletionPublishedToInbox: bool
          RetryDoesNotInvokeCheckpointedActivity: bool
          CrashAfterPublishBeforeCheckpointSkipsHandler: bool
          MissingHandlerFailsWithoutCheckpoint: bool
          NonActivityCommandsAreIgnored: bool
          StaleOwnerCannotCheckpointProgress: bool }

    let private forceDecoded entries =
        entries
        |> List.map (fun (seqNum, decoded) ->
            match decoded with
            | Ok entry -> seqNum, entry
            | Error error -> failwith error)

    let private activity = Activities.create "reserve" "order-1"

    let private callRecords =
        [ Incoming(HistoryEvent(ActivityCalled(OpId 0, activity)))
          Outgoing(Command(CallActivity(OpId 0, activity))) ]

    let private register name handler =
        match ActivityRegistry.empty |> ActivityRegistry.register name handler with
        | Ok registry -> registry
        | Error error -> failwith (string error)

    let private appendPublishedCompletion pair sourceSeqNum value =
        let envelope =
            { Source = ActivityCommandAdapter.completionSource
              SourceSeqNum = sourceSeqNum
              Message = CompleteActivity(OpId 0, value) }

        S2Substrate.appendInboxText [] (InboxEnvelopeCodec.encode envelope) pair

    let private readInbox owner =
        async {
            try
                let! records = S2Substrate.readInbox 0L 100 owner

                return
                    records
                    |> List.map (fun record ->
                        match InboxEnvelopeCodec.decode record.Body with
                        | Ok envelope -> record.SeqNum, envelope
                        | Error error -> failwith error)
            with error ->
                match S2Errors.classify error with
                | S2Errors.RangeNotSatisfiable _ -> return []
                | _ -> return raise error
        }

    let private hasActivityCheckpoint entries =
        entries
        |> List.exists (function
            | _, Incoming(CommandDispatchCheckpoint("activity", _)) -> true
            | _ -> false)

    let private hasDirectCompletion entries =
        entries
        |> List.exists (function
            | _, Incoming(HistoryEvent(ActivityCompleted(OpId 0, _))) -> true
            | _ -> false)

    let private completionEnvelope sourceSeqNum value =
        function
        | _,
          { Source = source
            SourceSeqNum = seqNum
            Message = CompleteActivity(OpId 0, completed) } ->
            source = ActivityCommandAdapter.completionSource
            && seqNum = sourceSeqNum
            && completed = value
        | _ -> false

    let private runWorkload ctx =
        ProofOperation.run
            ctx
            "durable.activity_adapter"
            "durable-activity-adapter"
            { ProofOperationOptions.empty with
                Key = Some "durable-activity-adapter" }
            (async {
                let s2 = WorkloadContext.requireS2 ctx

                let suffix = string (int64 (Reports.nowMillis ()))
                let basinName = "durable-activity-adapter-" + suffix
                let key = StorageKey("workflow-" + suffix)
                let missingKey = StorageKey("missing-" + suffix)
                let ignoredKey = StorageKey("ignored-" + suffix)
                let crashKey = StorageKey("crash-" + suffix)
                let staleKey = StorageKey("stale-" + suffix)

                let! _ = s2.Client |> S2.createBasin basinName
                let basin = s2.Client |> S2.basin basinName

                do! S2Substrate.ensureStreams basin key
                do! S2Substrate.ensureStreams basin missingKey
                do! S2Substrate.ensureStreams basin ignoredKey
                do! S2Substrate.ensureStreams basin crashKey
                do! S2Substrate.ensureStreams basin staleKey

                let pair = S2Substrate.streams basin key
                let missingPair = S2Substrate.streams basin missingKey
                let ignoredPair = S2Substrate.streams basin ignoredKey
                let crashPair = S2Substrate.streams basin crashKey
                let stalePair = S2Substrate.streams basin staleKey

                let mutable reserveCalls = 0

                let activities =
                    register "reserve" (fun input ->
                        async {
                            reserveCalls <- reserveCalls + 1
                            return "reserved:" + input
                        })

                let! owner = S2Substrate.claimWith (FenceToken "activity-adapter/fence-1") pair
                let! _seed = S2Substrate.commitText StepRecordCodec.encode callRecords owner

                let! first =
                    ActivityCommandAdapter.runOnce StepRecordCodec.encode StepRecordCodec.decode 10 activities owner

                let! afterFirstRaw = S2Substrate.readLogText StepRecordCodec.decode owner
                let afterFirst = forceDecoded afterFirstRaw
                let! firstInbox = readInbox owner

                let committedCallInvokesHandler =
                    match first with
                    | ActivityCommandAdapterStatus.Processed report ->
                        reserveCalls = 1
                        && (report.Completed |> List.map (fun completion -> completion.Value)) = [ "reserved:order-1" ]
                        && report.AlreadyCompleted = 0
                        && report.AlreadyPublished = 0
                        && report.Ignored = 0
                    | _ -> false

                let sourceSeqNum =
                    afterFirst
                    |> List.pick (function
                        | seqNum, Outgoing(Command(CallActivity(OpId 0, called))) when called = activity -> Some seqNum
                        | _ -> None)

                let completionPublishedToInbox =
                    (firstInbox |> List.exists (completionEnvelope sourceSeqNum "reserved:order-1"))
                    && hasActivityCheckpoint afterFirst
                    && not (hasDirectCompletion afterFirst)

                let! retry =
                    ActivityCommandAdapter.runOnce StepRecordCodec.encode StepRecordCodec.decode 10 activities owner

                let retryDoesNotInvokeCheckpointedActivity =
                    match retry with
                    | ActivityCommandAdapterStatus.Processed report ->
                        reserveCalls = 1
                        && List.isEmpty report.Completed
                        && report.AlreadyCompleted = 0
                        && report.AlreadyPublished = 0
                    | _ -> false

                let! missingOwner = S2Substrate.claimWith (FenceToken "activity-adapter/missing") missingPair
                let! _missingSeed = S2Substrate.commitText StepRecordCodec.encode callRecords missingOwner

                let! missing =
                    ActivityCommandAdapter.runOnce
                        StepRecordCodec.encode
                        StepRecordCodec.decode
                        10
                        ActivityRegistry.empty
                        missingOwner

                let! afterMissingRaw = S2Substrate.readLogText StepRecordCodec.decode missingOwner
                let afterMissing = forceDecoded afterMissingRaw

                let missingHandlerFailsWithoutCheckpoint =
                    match missing with
                    | ActivityCommandAdapterStatus.Failed(ActivityCommandAdapterFailure.MissingHandler(ActivityName "reserve")) ->
                        not (hasActivityCheckpoint afterMissing)
                    | _ -> false

                let! ignoredOwner = S2Substrate.claimWith (FenceToken "activity-adapter/ignored") ignoredPair

                let! _ignoredSeed =
                    S2Substrate.commitText
                        StepRecordCodec.encode
                        [ Outgoing(Command(ScheduleTimer(OpId 1, 500L))) ]
                        ignoredOwner

                let! ignored =
                    ActivityCommandAdapter.runOnce
                        StepRecordCodec.encode
                        StepRecordCodec.decode
                        10
                        activities
                        ignoredOwner

                let nonActivityCommandsAreIgnored =
                    match ignored with
                    | ActivityCommandAdapterStatus.Processed report ->
                        reserveCalls = 1 && List.isEmpty report.Completed && report.Ignored = 1
                    | _ -> false

                let mutable crashCalls = 0

                let crashActivities =
                    register "reserve" (fun input ->
                        async {
                            crashCalls <- crashCalls + 1
                            return "reserved-again:" + input
                        })

                let! crashOwner = S2Substrate.claimWith (FenceToken "activity-adapter/crash") crashPair
                let! _crashSeed = S2Substrate.commitText StepRecordCodec.encode callRecords crashOwner
                let! _publishedButNotCheckpointed = appendPublishedCompletion crashPair 2L "reserved:order-1"

                let! crashRetry =
                    ActivityCommandAdapter.runOnce
                        StepRecordCodec.encode
                        StepRecordCodec.decode
                        10
                        crashActivities
                        crashOwner

                let! afterCrashRaw = S2Substrate.readLogText StepRecordCodec.decode crashOwner
                let afterCrash = forceDecoded afterCrashRaw

                let crashAfterPublishBeforeCheckpointSkipsHandler =
                    match crashRetry with
                    | ActivityCommandAdapterStatus.Processed report ->
                        crashCalls = 0
                        && List.isEmpty report.Completed
                        && report.AlreadyCompleted = 0
                        && report.AlreadyPublished = 1
                        && hasActivityCheckpoint afterCrash
                    | _ -> false

                let! staleOwner = S2Substrate.claimWith (FenceToken "activity-adapter/stale") stalePair
                let! _staleSeed = S2Substrate.commitText StepRecordCodec.encode callRecords staleOwner
                let! _stalePublished = appendPublishedCompletion stalePair 2L "reserved:order-1"
                let! _freshOwner = S2Substrate.claimWith (FenceToken "activity-adapter/fresh") stalePair

                let! stale =
                    ActivityCommandAdapter.runOnce
                        StepRecordCodec.encode
                        StepRecordCodec.decode
                        10
                        activities
                        staleOwner

                let staleOwnerCannotCheckpointProgress =
                    match stale with
                    | ActivityCommandAdapterStatus.Deposed expected -> expected = "activity-adapter/fresh"
                    | _ -> false

                do! basin |> S2.deleteStream (StorageKey.inboxStreamName staleKey)
                do! basin |> S2.deleteStream (StorageKey.logStreamName staleKey)
                do! basin |> S2.deleteStream (StorageKey.inboxStreamName crashKey)
                do! basin |> S2.deleteStream (StorageKey.logStreamName crashKey)
                do! basin |> S2.deleteStream (StorageKey.inboxStreamName ignoredKey)
                do! basin |> S2.deleteStream (StorageKey.logStreamName ignoredKey)
                do! basin |> S2.deleteStream (StorageKey.inboxStreamName missingKey)
                do! basin |> S2.deleteStream (StorageKey.logStreamName missingKey)
                do! basin |> S2.deleteStream (StorageKey.inboxStreamName key)
                do! basin |> S2.deleteStream (StorageKey.logStreamName key)

                let result =
                    { CommittedCallInvokesHandler = committedCallInvokesHandler
                      CompletionPublishedToInbox = completionPublishedToInbox
                      RetryDoesNotInvokeCheckpointedActivity = retryDoesNotInvokeCheckpointedActivity
                      CrashAfterPublishBeforeCheckpointSkipsHandler = crashAfterPublishBeforeCheckpointSkipsHandler
                      MissingHandlerFailsWithoutCheckpoint = missingHandlerFailsWithoutCheckpoint
                      NonActivityCommandsAreIgnored = nonActivityCommandsAreIgnored
                      StaleOwnerCannotCheckpointProgress = staleOwnerCannotCheckpointProgress }

                do!
                    ctx.EmitSpan
                        "proof.durable_activity_adapter.completed"
                        [ "proof.property", "durable-activity-adapter"
                          "activity.invoked", string result.CommittedCallInvokesHandler
                          "activity.inbox", string result.CompletionPublishedToInbox
                          "activity.retry", string result.RetryDoesNotInvokeCheckpointedActivity
                          "activity.crash_window", string result.CrashAfterPublishBeforeCheckpointSkipsHandler
                          "activity.stale", string result.StaleOwnerCannotCheckpointProgress ]

                return result
            })

    let activityAdapterProperty =
        property "durable-activity-adapter" {
            s2Lite ""
            workload runWorkload

            verify (fun v ->
                [ v.Expect.Workload "committed CallActivity invokes the registered handler" (fun result ->
                      result.CommittedCallInvokesHandler)
                  v.Expect.Workload "activity completion is published to the inbox before checkpoint" (fun result ->
                      result.CompletionPublishedToInbox)
                  v.Expect.Workload "retry does not invoke a checkpointed activity" (fun result ->
                      result.RetryDoesNotInvokeCheckpointedActivity)
                  v.Expect.Workload "publish-before-checkpoint crash retry skips handler" (fun result ->
                      result.CrashAfterPublishBeforeCheckpointSkipsHandler)
                  v.Expect.Workload "missing handler fails without checkpointing" (fun result ->
                      result.MissingHandlerFailsWithoutCheckpoint)
                  v.Expect.Workload "non-activity commands are ignored by the activity adapter" (fun result ->
                      result.NonActivityCommandsAreIgnored)
                  v.Expect.Workload "stale owner cannot checkpoint activity adapter progress" (fun result ->
                      result.StaleOwnerCannotCheckpointProgress)
                  v.Trace.SpanExists
                      "durable activity adapter proof span emitted"
                      "proof.durable_activity_adapter.completed"
                      [ "proof.property", "durable-activity-adapter" ]
                  v.Trace.Operation
                      "durable activity adapter operation was recorded"
                      ({ TraceOperationMatch.named "durable.activity_adapter" with
                          Status = Some "ok"
                          OutputContains =
                              [ "CommittedCallInvokesHandler"
                                "CompletionPublishedToInbox"
                                "CrashAfterPublishBeforeCheckpointSkipsHandler" ]
                          Count = Some 1 }) ])
        }

    let proof =
        proof "durable-activity-adapter" {
            describedAs
                "Committed activity commands invoke registered handlers, publish inbox completions, then checkpoint."

            property activityAdapterProperty
        }
