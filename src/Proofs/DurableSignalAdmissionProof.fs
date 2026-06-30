namespace Eff.Proofs

open Eff
open Eff.Foundation.Durable

module DurableSignalAdmissionProof =
    type DurableSignalAdmissionResult =
        { StartReachesWaitingSignal: bool
          SignalAcceptedBeforeDelivery: bool
          DuplicateSignalFoldsOnce: bool
          SignalDeliveryAdvancesHost: bool
          WorkflowCompletesFromSignalPayload: bool
          SignalIsConsumedOnce: bool }

    let private workflowName = WorkflowName.create "await-approval"

    let private instance suffix = InstanceId.create ("signal-" + suffix)

    let private options hostId timestamp =
        { DurableHostTickOptions.create hostId timestamp with
            MaxInboxRecords = 10
            MaxActivityCommands = 10
            MaxTimerCommands = 10 }

    let private registerWorkflows () =
        match
            WorkflowRegistry.empty
            |> WorkflowRegistry.register "await-approval" (fun _ ->
                durable {
                    let! approved = Workflow.waitForSignal "approved"
                    return "approved:" + approved
                })
        with
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

    let private signalAcceptedCount entries =
        entries
        |> List.filter (function
            | _,
              Incoming(InboxMessageAccepted { Source = "client:signal"
                                              SourceSeqNum = 0L
                                              Message = RaiseSignal("approved", "yes") }) -> true
            | _ -> false)
        |> List.length

    let private signalReceivedCount entries =
        entries
        |> List.filter (function
            | _, Incoming(HistoryEvent(SignalReceived(OpId 0, "approved", "yes"))) -> true
            | _ -> false)
        |> List.length

    let private signalDeliveredCount entries =
        entries
        |> List.filter (function
            | _, Incoming(SignalDelivered("client:signal", 0L, OpId 0)) -> true
            | _ -> false)
        |> List.length

    let private runWorkload ctx =
        ProofOperation.run
            ctx
            "durable.signal_admission"
            "durable-signal-admission"
            { ProofOperationOptions.empty with
                Key = Some "durable-signal-admission" }
            (async {
                let s2 = WorkloadContext.requireS2 ctx

                let suffix = string (int64 (Reports.nowMillis ()))
                let basinName = "durable-signal-admission-" + suffix
                let instanceId = instance suffix

                let! _ = s2.Client |> S2.createBasin basinName
                let basin = s2.Client |> S2.basin basinName

                let key = DurableClient.instanceKey instanceId
                let workflows = registerWorkflows ()

                let! start = DurableClient.startWith basin instanceId workflowName "order-1"
                let pair = S2Substrate.streams basin key
                let! owner = S2Substrate.claimWith (FenceToken "signal-admission/owner") pair

                let! firstTick =
                    DurableHost.runWorkflowTick (options "signal-admission" 100L) workflows ActivityRegistry.empty owner

                let startReachesWaitingSignal =
                    match start, firstTick with
                    | DurableClientStartStatus.Accepted _,
                      DurableWorkflowHostStatus.Ticked(DurableHostTickStatus.Waiting(OpId 0,
                                                                                     NeedsEvent(Signal "approved"),
                                                                                     report)) ->
                        match report.Signals with
                        | Some signalReport -> signalReport.PendingSignals = 0 && signalReport.Delivered.IsNone
                        | None -> false
                    | DurableClientStartStatus.Accepted _,
                      DurableWorkflowHostStatus.Ticked(DurableHostTickStatus.Advanced report) ->
                        match report.Step, report.Signals with
                        | Some(DurableHostStatus.Waiting(OpId 0, NeedsEvent(Signal "approved"))), Some signalReport ->
                            signalReport.PendingSignals = 0 && signalReport.Delivered.IsNone
                        | _ -> false
                    | _ -> false

                let! firstSignal = DurableClient.raiseSignalWith basin instanceId 0L "approved" "yes"
                let! retrySignal = DurableClient.raiseSignalWith basin instanceId 0L "approved" "yes"

                let signalAcceptedBeforeDelivery =
                    match firstSignal, retrySignal with
                    | DurableClientSignalStatus.Accepted first, DurableClientSignalStatus.Accepted retry ->
                        first.InstanceId = instanceId
                        && first.SignalName = "approved"
                        && first.SourceSeqNum = 0L
                        && retry.SourceSeqNum = 0L
                        && retry.InboxSeqNum = first.InboxSeqNum + 1L
                    | _ -> false

                let! secondTick =
                    DurableHost.runWorkflowTick (options "signal-admission" 101L) workflows ActivityRegistry.empty owner

                let! afterSecondTick = readLog owner

                let duplicateSignalFoldsOnce = signalAcceptedCount afterSecondTick = 1

                let signalDeliveryAdvancesHost =
                    match secondTick with
                    | DurableWorkflowHostStatus.Ticked(DurableHostTickStatus.Advanced report) ->
                        match report.Signals with
                        | Some { Delivered = Some delivered } ->
                            delivered.Source = "client:signal"
                            && delivered.SourceSeqNum = 0L
                            && delivered.OpId = OpId 0
                            && delivered.Name = "approved"
                            && delivered.Payload = "yes"
                        | _ -> false
                    | _ -> false

                let! thirdTick =
                    DurableHost.runWorkflowTick (options "signal-admission" 102L) workflows ActivityRegistry.empty owner

                let! fourthTick =
                    DurableHost.runWorkflowTick (options "signal-admission" 103L) workflows ActivityRegistry.empty owner

                let! afterFourthTick = readLog owner

                let workflowCompletesFromSignalPayload =
                    match thirdTick, fourthTick with
                    | DurableWorkflowHostStatus.Ticked(DurableHostTickStatus.Completed("approved:yes", _)),
                      DurableWorkflowHostStatus.Ticked(DurableHostTickStatus.Completed("approved:yes", _)) -> true
                    | _ -> false

                let signalIsConsumedOnce =
                    signalReceivedCount afterFourthTick = 1
                    && signalDeliveredCount afterFourthTick = 1

                do! basin |> S2.deleteStream (StorageKey.inboxStreamName key)
                do! basin |> S2.deleteStream (StorageKey.logStreamName key)

                let result =
                    { StartReachesWaitingSignal = startReachesWaitingSignal
                      SignalAcceptedBeforeDelivery = signalAcceptedBeforeDelivery
                      DuplicateSignalFoldsOnce = duplicateSignalFoldsOnce
                      SignalDeliveryAdvancesHost = signalDeliveryAdvancesHost
                      WorkflowCompletesFromSignalPayload = workflowCompletesFromSignalPayload
                      SignalIsConsumedOnce = signalIsConsumedOnce }

                do!
                    ctx.EmitSpan
                        "proof.durable_signal_admission.completed"
                        [ "proof.property", "durable-signal-admission"
                          "signal.waiting", string result.StartReachesWaitingSignal
                          "signal.accepted", string result.SignalAcceptedBeforeDelivery
                          "signal.duplicate", string result.DuplicateSignalFoldsOnce
                          "signal.completed", string result.WorkflowCompletesFromSignalPayload ]

                return result
            })

    let signalAdmissionProperty =
        property "durable-signal-admission" {
            s2Lite ""
            workload runWorkload

            verify (fun v ->
                [ v.Expect.Workload "start reaches a durable signal wait" (fun result ->
                      result.StartReachesWaitingSignal)
                  v.Expect.Workload "signal is durably accepted before delivery" (fun result ->
                      result.SignalAcceptedBeforeDelivery)
                  v.Expect.Workload "duplicate signal admission folds once" (fun result ->
                      result.DuplicateSignalFoldsOnce)
                  v.Expect.Workload "signal delivery advances the host" (fun result ->
                      result.SignalDeliveryAdvancesHost)
                  v.Expect.Workload "workflow completes from delivered signal payload" (fun result ->
                      result.WorkflowCompletesFromSignalPayload)
                  v.Expect.Workload "signal source sequence is consumed once" (fun result ->
                      result.SignalIsConsumedOnce)
                  v.Trace.SpanExists
                      "durable signal admission proof span emitted"
                      "proof.durable_signal_admission.completed"
                      [ "proof.property", "durable-signal-admission" ]
                  v.Trace.Operation
                      "durable signal admission operation was recorded"
                      ({ TraceOperationMatch.named "durable.signal_admission" with
                          Status = Some "ok"
                          OutputContains =
                              [ "StartReachesWaitingSignal"
                                "DuplicateSignalFoldsOnce"
                                "SignalIsConsumedOnce" ]
                          Count = Some 1 }) ])
        }

    let proof =
        proof "durable-signal-admission" {
            describedAs
                "Durable client RaiseSignalWith admits retriable signals and host delivery consumes each source sequence once."

            property signalAdmissionProperty
        }
