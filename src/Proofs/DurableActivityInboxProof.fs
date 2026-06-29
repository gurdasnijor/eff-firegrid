namespace Eff.Proofs

open Eff
open Eff.Foundation.Durable

module DurableActivityInboxProof =
    type DurableActivityInboxResult =
        { TwoActivityWorkflowCompletes: bool
          HostWaitsUntilInboxFold: bool
          AdapterDoesNotWriteDirectCompletion: bool
          RestartBeforeFoldStillAdvances: bool
          DuplicateCompletionEnvelopeIgnored: bool }

    let private reserve = Activities.create "reserve" "order-1"

    let private charge input = Activities.create "charge" input

    let private program =
        durable {
            let! reserved = Workflow.call reserve.Name reserve.Input
            let! charged = Workflow.call "charge" reserved
            return charged
        }

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

    let private activityCommandSource activity entries =
        entries
        |> List.pick (function
            | seqNum, Outgoing(Command(CallActivity(_, called))) when called = activity -> Some seqNum
            | _ -> None)

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

    let private hasDirectActivityCompletion entries =
        entries
        |> List.exists (function
            | _, Incoming(HistoryEvent(ActivityCompleted _)) -> true
            | _ -> false)

    let private appendDuplicateCompletion pair sourceSeqNum value =
        let envelope =
            { Source = ActivityCommandAdapter.completionSource
              SourceSeqNum = sourceSeqNum
              Message = CompleteActivity(OpId 0, value) }

        S2Substrate.appendInboxText [] (InboxEnvelopeCodec.encode envelope) pair

    let private fold owner =
        InboxFold.runOnce StepRecordCodec.encode StepRecordCodec.decode InboxEnvelopeCodec.decode 10 owner

    let private runWorkload ctx =
        ProofOperation.run
            ctx
            "durable.activity_inbox"
            "durable-activity-inbox"
            { ProofOperationOptions.empty with
                Key = Some "durable-activity-inbox" }
            (async {
                let s2 = WorkloadContext.requireS2 ctx

                let suffix = string (int64 (Reports.nowMillis ()))
                let basinName = "durable-activity-inbox-" + suffix
                let key = StorageKey("workflow-" + suffix)

                let! _ = s2.Client |> S2.createBasin basinName
                let basin = s2.Client |> S2.basin basinName

                do! S2Substrate.ensureStreams basin key

                let pair = S2Substrate.streams basin key
                let activities = registerActivities ()

                let! owner1 = S2Substrate.claimWith (FenceToken "activity-inbox/owner-1") pair

                let! firstHost = DurableHost.runOnce StepRecordCodec.encode StepRecordCodec.decode 100L owner1 program

                let! afterFirstHost = readLog owner1
                let reserveSourceSeqNum = activityCommandSource reserve afterFirstHost

                let! firstAdapter =
                    ActivityCommandAdapter.runOnce StepRecordCodec.encode StepRecordCodec.decode 10 activities owner1

                let! afterFirstAdapter = readLog owner1

                let adapterDoesNotWriteDirectCompletion =
                    match firstAdapter with
                    | ActivityCommandAdapterStatus.Processed report ->
                        (report.Completed |> List.map (fun completion -> completion.Value)) = [ "reserved:order-1" ]
                        && not (hasDirectActivityCompletion afterFirstAdapter)
                    | _ -> false

                let! hostBeforeFold =
                    DurableHost.runOnce StepRecordCodec.encode StepRecordCodec.decode 101L owner1 program

                let! afterHostBeforeFold = readLog owner1

                let hostWaitsUntilInboxFold =
                    match hostBeforeFold with
                    | DurableHostStatus.Waiting(OpId 0, NeedsActivity blocked) ->
                        blocked = reserve
                        && activityCommandCount (charge "reserved:order-1") afterHostBeforeFold = 0
                    | _ -> false

                let! owner2 = S2Substrate.claimWith (FenceToken "activity-inbox/owner-2") pair

                let! firstFold = fold owner2
                let! afterFirstFold = readLog owner2

                let restartBeforeFoldStillAdvances =
                    match firstFold with
                    | InboxFoldStatus.Folded report ->
                        report.Accepted.Length = 1
                        && activityCompletionCount (OpId 0) "reserved:order-1" afterFirstFold = 1
                    | _ -> false

                let! _duplicate = appendDuplicateCompletion pair reserveSourceSeqNum "reserved:order-1"
                let! duplicateFold = fold owner2
                let! afterDuplicateFold = readLog owner2

                let duplicateCompletionEnvelopeIgnored =
                    match duplicateFold with
                    | InboxFoldStatus.Folded report ->
                        report.Scanned = 1
                        && report.Duplicates = 1
                        && List.isEmpty report.Accepted
                        && activityCompletionCount (OpId 0) "reserved:order-1" afterDuplicateFold = 1
                    | _ -> false

                let! secondHost = DurableHost.runOnce StepRecordCodec.encode StepRecordCodec.decode 102L owner2 program

                let! afterSecondHost = readLog owner2
                let chargeActivity = charge "reserved:order-1"

                let secondActivityScheduled =
                    match secondHost with
                    | DurableHostStatus.Committed _ -> activityCommandCount chargeActivity afterSecondHost = 1
                    | _ -> false

                let! secondAdapter =
                    ActivityCommandAdapter.runOnce StepRecordCodec.encode StepRecordCodec.decode 10 activities owner2

                let secondActivityPublished =
                    match secondAdapter with
                    | ActivityCommandAdapterStatus.Processed report ->
                        (report.Completed |> List.map (fun completion -> completion.Value)) = [ "charged:reserved:order-1" ]
                    | _ -> false

                let! secondFold = fold owner2
                let! afterSecondFold = readLog owner2

                let secondCompletionFolded =
                    match secondFold with
                    | InboxFoldStatus.Folded report ->
                        report.Accepted.Length = 1
                        && activityCompletionCount (OpId 1) "charged:reserved:order-1" afterSecondFold = 1
                    | _ -> false

                let! finalHost = DurableHost.runOnce StepRecordCodec.encode StepRecordCodec.decode 103L owner2 program

                let twoActivityWorkflowCompletes =
                    match firstHost, finalHost with
                    | DurableHostStatus.Committed _, DurableHostStatus.Completed "charged:reserved:order-1" ->
                        secondActivityScheduled && secondActivityPublished && secondCompletionFolded
                    | _ -> false

                do! basin |> S2.deleteStream (StorageKey.inboxStreamName key)
                do! basin |> S2.deleteStream (StorageKey.logStreamName key)

                let result =
                    { TwoActivityWorkflowCompletes = twoActivityWorkflowCompletes
                      HostWaitsUntilInboxFold = hostWaitsUntilInboxFold
                      AdapterDoesNotWriteDirectCompletion = adapterDoesNotWriteDirectCompletion
                      RestartBeforeFoldStillAdvances = restartBeforeFoldStillAdvances
                      DuplicateCompletionEnvelopeIgnored = duplicateCompletionEnvelopeIgnored }

                do!
                    ctx.EmitSpan
                        "proof.durable_activity_inbox.completed"
                        [ "proof.property", "durable-activity-inbox"
                          "activity_inbox.completed", string result.TwoActivityWorkflowCompletes
                          "activity_inbox.waits_for_fold", string result.HostWaitsUntilInboxFold
                          "activity_inbox.restart", string result.RestartBeforeFoldStillAdvances
                          "activity_inbox.duplicate", string result.DuplicateCompletionEnvelopeIgnored ]

                return result
            })

    let activityInboxProperty =
        property "durable-activity-inbox" {
            s2Lite ""
            workload runWorkload

            verify (fun v ->
                [ v.Expect.Workload "two-activity workflow completes through inbox fold" (fun result ->
                      result.TwoActivityWorkflowCompletes)
                  v.Expect.Workload "host waits until inbox fold admits activity completion" (fun result ->
                      result.HostWaitsUntilInboxFold)
                  v.Expect.Workload "activity adapter does not write direct completion history" (fun result ->
                      result.AdapterDoesNotWriteDirectCompletion)
                  v.Expect.Workload "restart after inbox publish before fold still advances" (fun result ->
                      result.RestartBeforeFoldStillAdvances)
                  v.Expect.Workload "duplicate activity completion envelope is ignored" (fun result ->
                      result.DuplicateCompletionEnvelopeIgnored)
                  v.Trace.SpanExists
                      "durable activity inbox proof span emitted"
                      "proof.durable_activity_inbox.completed"
                      [ "proof.property", "durable-activity-inbox" ]
                  v.Trace.Operation
                      "durable activity inbox operation was recorded"
                      ({ TraceOperationMatch.named "durable.activity_inbox" with
                          Status = Some "ok"
                          OutputContains =
                              [ "TwoActivityWorkflowCompletes"
                                "HostWaitsUntilInboxFold"
                                "DuplicateCompletionEnvelopeIgnored" ]
                          Count = Some 1 }) ])
        }

    let proof =
        proof "durable-activity-inbox" {
            describedAs
                "Activity command completions are published to inbox, folded into history, and consumed by the host."

            property activityInboxProperty
        }
