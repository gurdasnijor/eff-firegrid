namespace Eff.Proofs

open Eff
open Eff.Foundation.Durable

module DurableHostTickProof =
    type DurableHostTickResult =
        { ClaimAndTickReportsOwner: bool
          RepeatedTicksCompleteTwoActivityWorkflow: bool
          WaitingRetryDispatchesPendingCommand: bool
          MissingHandlerIsTypedFailure: bool
          StaleOwnerCannotAdvanceTick: bool }

    let private reserve = Activities.create "reserve" "order-1"

    let private charge input = Activities.create "charge" input

    let private program =
        durable {
            let! reserved = Workflow.call reserve.Name reserve.Input
            let! charged = Workflow.call "charge" reserved
            return charged
        }

    let private options hostId timestamp =
        { DurableHostTickOptions.create hostId timestamp with
            MaxInboxRecords = 10
            MaxActivityCommands = 10 }

    let private registerActivities () =
        let registered =
            ActivityRegistry.empty
            |> ActivityRegistry.register "reserve" (fun input -> async { return "reserved:" + input })
            |> Result.bind (ActivityRegistry.register "charge" (fun input -> async { return "charged:" + input }))

        match registered with
        | Ok registry -> registry
        | Error error -> failwith (string error)

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

    let private activityCommandCount activity entries =
        entries
        |> List.filter (function
            | _, Outgoing(Command(CallActivity(_, called))) when called = activity -> true
            | _ -> false)
        |> List.length

    let private activityCompletionCount opId value entries =
        entries
        |> List.filter (function
            | _, Incoming(HistoryEvent(ActivityCompleted(id, completed))) when id = opId && completed = value -> true
            | _ -> false)
        |> List.length

    let private inboxCompletionCount owner =
        async {
            try
                let! records = S2Substrate.readInbox 0L 100 owner

                return
                    records
                    |> List.filter (fun record ->
                        match InboxEnvelopeCodec.decode record.Body with
                        | Ok { Message = CompleteActivity _ } -> true
                        | Ok _ -> false
                        | Error error -> failwith error)
                    |> List.length
            with error ->
                match S2Errors.classify error with
                | S2Errors.RangeNotSatisfiable _ -> return 0
                | _ -> return raise error
        }

    let private completedActivities =
        function
        | DurableHostTickStatus.Advanced report ->
            report.Activities
            |> Option.map (fun activityReport ->
                activityReport.Completed |> List.map (fun completion -> completion.Value))
            |> Option.defaultValue []
        | _ -> []

    let private runWorkload ctx =
        ProofOperation.run
            ctx
            "durable.host_tick"
            "durable-host-tick"
            { ProofOperationOptions.empty with
                Key = Some "durable-host-tick" }
            (async {
                let s2 = WorkloadContext.requireS2 ctx

                let suffix = string (int64 (Reports.nowMillis ()))
                let basinName = "durable-host-tick-" + suffix
                let key = StorageKey("workflow-" + suffix)
                let retryKey = StorageKey("retry-" + suffix)
                let missingKey = StorageKey("missing-" + suffix)
                let staleKey = StorageKey("stale-" + suffix)

                let! _ = s2.Client |> S2.createBasin basinName
                let basin = s2.Client |> S2.basin basinName

                do! S2Substrate.ensureStreams basin key
                do! S2Substrate.ensureStreams basin retryKey
                do! S2Substrate.ensureStreams basin missingKey
                do! S2Substrate.ensureStreams basin staleKey

                let pair = S2Substrate.streams basin key
                let retryPair = S2Substrate.streams basin retryKey
                let missingPair = S2Substrate.streams basin missingKey
                let stalePair = S2Substrate.streams basin staleKey
                let activities = registerActivities ()

                let! first = DurableHost.claimAndRunTick (options "host-tick" 100L) activities pair program

                let claimAndTickReportsOwner =
                    match first with
                    | DurableHostTickStatus.Advanced report ->
                        report.Key = key
                        && (FenceToken.value report.Fence).StartsWith("host-tick/")
                        && completedActivities first = [ "reserved:order-1" ]
                    | _ -> false

                let! owner = S2Substrate.claimWith (FenceToken "host-tick/owner") pair

                let! second = DurableHost.runOwnedTick (options "host-tick" 101L) activities owner program

                let! third = DurableHost.runOwnedTick (options "host-tick" 102L) activities owner program

                let! afterThird = readLog owner

                let repeatedTicksCompleteTwoActivityWorkflow =
                    match first, second, third with
                    | DurableHostTickStatus.Advanced _,
                      DurableHostTickStatus.Advanced _,
                      DurableHostTickStatus.Completed("charged:reserved:order-1", _) ->
                        activityCommandCount reserve afterThird = 1
                        && activityCommandCount (charge "reserved:order-1") afterThird = 1
                        && activityCompletionCount (OpId 0) "reserved:order-1" afterThird = 1
                        && activityCompletionCount (OpId 1) "charged:reserved:order-1" afterThird = 1
                    | _ -> false

                let! retryOwner = S2Substrate.claimWith (FenceToken "host-tick/retry") retryPair

                let! _stepOnly =
                    DurableHost.runOnce StepRecordCodec.encode StepRecordCodec.decode 200L retryOwner program

                let! retryTick = DurableHost.runOwnedTick (options "host-tick" 201L) activities retryOwner program

                let! retryInboxCount = inboxCompletionCount retryOwner

                let waitingRetryDispatchesPendingCommand =
                    match retryTick with
                    | DurableHostTickStatus.Advanced report ->
                        match report.Step, report.Activities with
                        | Some(DurableHostStatus.Waiting(OpId 0, NeedsActivity blocked)), Some activityReport ->
                            blocked = reserve
                            && (activityReport.Completed |> List.map (fun completion -> completion.Value)) = [ "reserved:order-1" ]
                            && retryInboxCount = 1
                        | _ -> false
                    | _ -> false

                let! missingOwner = S2Substrate.claimWith (FenceToken "host-tick/missing") missingPair

                let! missing =
                    DurableHost.runOwnedTick
                        (options "host-tick" 300L)
                        ActivityRegistry.empty
                        missingOwner
                        (Workflow.call reserve.Name reserve.Input)

                let missingHandlerIsTypedFailure =
                    match missing with
                    | DurableHostTickStatus.Failed(DurableHostTickFailure.ActivityFailed(ActivityCommandAdapterFailure.MissingHandler(ActivityName "reserve")),
                                                   report) ->
                        match report.Step with
                        | Some(DurableHostStatus.Committed _) -> true
                        | _ -> false
                    | _ -> false

                let! staleOwner = S2Substrate.claimWith (FenceToken "host-tick/stale") stalePair
                let! _freshOwner = S2Substrate.claimWith (FenceToken "host-tick/fresh") stalePair

                let! stale =
                    DurableHost.runOwnedTick
                        (options "host-tick" 400L)
                        activities
                        staleOwner
                        (Workflow.call reserve.Name reserve.Input)

                let! staleInboxCount = inboxCompletionCount staleOwner

                let staleOwnerCannotAdvanceTick =
                    match stale with
                    | DurableHostTickStatus.Deposed("host-tick/fresh", report) ->
                        report.Activities.IsNone && staleInboxCount = 0
                    | _ -> false

                do! basin |> S2.deleteStream (StorageKey.inboxStreamName staleKey)
                do! basin |> S2.deleteStream (StorageKey.logStreamName staleKey)
                do! basin |> S2.deleteStream (StorageKey.inboxStreamName missingKey)
                do! basin |> S2.deleteStream (StorageKey.logStreamName missingKey)
                do! basin |> S2.deleteStream (StorageKey.inboxStreamName retryKey)
                do! basin |> S2.deleteStream (StorageKey.logStreamName retryKey)
                do! basin |> S2.deleteStream (StorageKey.inboxStreamName key)
                do! basin |> S2.deleteStream (StorageKey.logStreamName key)

                let result =
                    { ClaimAndTickReportsOwner = claimAndTickReportsOwner
                      RepeatedTicksCompleteTwoActivityWorkflow = repeatedTicksCompleteTwoActivityWorkflow
                      WaitingRetryDispatchesPendingCommand = waitingRetryDispatchesPendingCommand
                      MissingHandlerIsTypedFailure = missingHandlerIsTypedFailure
                      StaleOwnerCannotAdvanceTick = staleOwnerCannotAdvanceTick }

                do!
                    ctx.EmitSpan
                        "proof.durable_host_tick.completed"
                        [ "proof.property", "durable-host-tick"
                          "host_tick.owner", string result.ClaimAndTickReportsOwner
                          "host_tick.completed", string result.RepeatedTicksCompleteTwoActivityWorkflow
                          "host_tick.retry", string result.WaitingRetryDispatchesPendingCommand
                          "host_tick.stale", string result.StaleOwnerCannotAdvanceTick ]

                return result
            })

    let hostTickProperty =
        property "durable-host-tick" {
            s2Lite ""
            workload runWorkload

            verify (fun v ->
                [ v.Expect.Workload "claim-and-run tick reports claimed owner" (fun result ->
                      result.ClaimAndTickReportsOwner)
                  v.Expect.Workload "repeated ticks complete a two-activity workflow" (fun result ->
                      result.RepeatedTicksCompleteTwoActivityWorkflow)
                  v.Expect.Workload "waiting retry dispatches an uncheckpointed pending command" (fun result ->
                      result.WaitingRetryDispatchesPendingCommand)
                  v.Expect.Workload "missing handler is surfaced as a typed tick failure" (fun result ->
                      result.MissingHandlerIsTypedFailure)
                  v.Expect.Workload "stale owner cannot advance a composed tick" (fun result ->
                      result.StaleOwnerCannotAdvanceTick)
                  v.Trace.SpanExists
                      "durable host tick proof span emitted"
                      "proof.durable_host_tick.completed"
                      [ "proof.property", "durable-host-tick" ]
                  v.Trace.Operation
                      "durable host tick operation was recorded"
                      ({ TraceOperationMatch.named "durable.host_tick" with
                          Status = Some "ok"
                          OutputContains =
                              [ "RepeatedTicksCompleteTwoActivityWorkflow"
                                "WaitingRetryDispatchesPendingCommand"
                                "StaleOwnerCannotAdvanceTick" ]
                          Count = Some 1 }) ])
        }

    let proof =
        proof "durable-host-tick" {
            describedAs
                "A composed host tick claims an instance, folds inbox, steps workflow replay, and dispatches activity commands."

            property hostTickProperty
        }
