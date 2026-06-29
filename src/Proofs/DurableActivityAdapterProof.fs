namespace Eff.Proofs

open Eff
open Eff.Foundation.Durable

module DurableActivityAdapterProof =
    type DurableActivityAdapterResult =
        { CommittedCallInvokesHandler: bool
          CompletionBeforeCheckpoint: bool
          RetryDoesNotInvokeCompletedActivity: bool
          CrashAfterCompletionBeforeCheckpointSkipsHandler: bool
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

                let committedCallInvokesHandler =
                    match first with
                    | ActivityCommandAdapterStatus.Processed report ->
                        reserveCalls = 1
                        && (report.Completed |> List.map (fun completion -> completion.Value)) = [ "reserved:order-1" ]
                        && report.AlreadyCompleted = 0
                        && report.Ignored = 0
                    | _ -> false

                let completionSeq =
                    afterFirst
                    |> List.tryPick (function
                        | seqNum, Incoming(HistoryEvent(ActivityCompleted(OpId 0, "reserved:order-1"))) -> Some seqNum
                        | _ -> None)

                let checkpointSeq =
                    afterFirst
                    |> List.tryPick (function
                        | seqNum, Incoming(CommandDispatchCheckpoint("activity", _)) -> Some seqNum
                        | _ -> None)

                let completionBeforeCheckpoint =
                    match completionSeq, checkpointSeq with
                    | Some completedAt, Some checkpointAt -> completedAt < checkpointAt
                    | _ -> false

                let! retry =
                    ActivityCommandAdapter.runOnce StepRecordCodec.encode StepRecordCodec.decode 10 activities owner

                let retryDoesNotInvokeCompletedActivity =
                    match retry with
                    | ActivityCommandAdapterStatus.Processed report ->
                        reserveCalls = 1 && List.isEmpty report.Completed && report.AlreadyCompleted = 0
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
                        afterMissing
                        |> List.exists (function
                            | _, Incoming(CommandDispatchCheckpoint("activity", _)) -> true
                            | _ -> false)
                        |> not
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

                let! _crashSeed =
                    S2Substrate.commitText
                        StepRecordCodec.encode
                        [ Incoming(HistoryEvent(ActivityCalled(OpId 0, activity)))
                          Outgoing(Command(CallActivity(OpId 0, activity)))
                          Incoming(HistoryEvent(ActivityCompleted(OpId 0, "reserved:order-1"))) ]
                        crashOwner

                let! crashRetry =
                    ActivityCommandAdapter.runOnce
                        StepRecordCodec.encode
                        StepRecordCodec.decode
                        10
                        crashActivities
                        crashOwner

                let! afterCrashRaw = S2Substrate.readLogText StepRecordCodec.decode crashOwner
                let afterCrash = forceDecoded afterCrashRaw

                let crashAfterCompletionBeforeCheckpointSkipsHandler =
                    match crashRetry with
                    | ActivityCommandAdapterStatus.Processed report ->
                        crashCalls = 0
                        && List.isEmpty report.Completed
                        && report.AlreadyCompleted = 1
                        && (afterCrash
                            |> List.exists (function
                                | _, Incoming(CommandDispatchCheckpoint("activity", _)) -> true
                                | _ -> false))
                    | _ -> false

                let! staleOwner = S2Substrate.claimWith (FenceToken "activity-adapter/stale") stalePair

                let! _staleSeed =
                    S2Substrate.commitText
                        StepRecordCodec.encode
                        [ Incoming(HistoryEvent(ActivityCalled(OpId 0, activity)))
                          Outgoing(Command(CallActivity(OpId 0, activity)))
                          Incoming(HistoryEvent(ActivityCompleted(OpId 0, "reserved:order-1"))) ]
                        staleOwner

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
                      CompletionBeforeCheckpoint = completionBeforeCheckpoint
                      RetryDoesNotInvokeCompletedActivity = retryDoesNotInvokeCompletedActivity
                      CrashAfterCompletionBeforeCheckpointSkipsHandler =
                        crashAfterCompletionBeforeCheckpointSkipsHandler
                      MissingHandlerFailsWithoutCheckpoint = missingHandlerFailsWithoutCheckpoint
                      NonActivityCommandsAreIgnored = nonActivityCommandsAreIgnored
                      StaleOwnerCannotCheckpointProgress = staleOwnerCannotCheckpointProgress }

                do!
                    ctx.EmitSpan
                        "proof.durable_activity_adapter.completed"
                        [ "proof.property", "durable-activity-adapter"
                          "activity.invoked", string result.CommittedCallInvokesHandler
                          "activity.ordering", string result.CompletionBeforeCheckpoint
                          "activity.retry", string result.RetryDoesNotInvokeCompletedActivity
                          "activity.crash_window", string result.CrashAfterCompletionBeforeCheckpointSkipsHandler
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
                  v.Expect.Workload "activity completion is committed before dispatch checkpoint" (fun result ->
                      result.CompletionBeforeCheckpoint)
                  v.Expect.Workload "retry does not invoke an already completed activity" (fun result ->
                      result.RetryDoesNotInvokeCompletedActivity)
                  v.Expect.Workload "completion-before-checkpoint crash retry skips handler" (fun result ->
                      result.CrashAfterCompletionBeforeCheckpointSkipsHandler)
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
                                "CompletionBeforeCheckpoint"
                                "CrashAfterCompletionBeforeCheckpointSkipsHandler" ]
                          Count = Some 1 }) ])
        }

    let proof =
        proof "durable-activity-adapter" {
            describedAs
                "Committed activity commands invoke registered handlers, admit durable completions, then checkpoint."

            property activityAdapterProperty
        }
