namespace Eff.Proofs

open Eff
open Eff.Foundation.Durable

module DurableStatusProof =
    type DurableStatusResult =
        { EmptyInstanceIsNotFound: bool
          FoldedStartNeedingStepIsRunning: bool
          WaitingStatusComesFromReplay: bool
          CompletedStatusComesFromHistory: bool
          MissingWorkflowIsTypedFailure: bool }

    let private signalWorkflow = WorkflowName.create "status-signal"

    let private activityWorkflow = WorkflowName.create "status-activity"

    let private missingWorkflow = WorkflowName.create "status-missing"

    let private instance suffix name = InstanceId.create (name + "-" + suffix)

    let private options hostId timestamp =
        { DurableHostTickOptions.create hostId timestamp with
            MaxInboxRecords = 10
            MaxActivityCommands = 10
            MaxTimerCommands = 10 }

    let private registerWorkflows () =
        let registered =
            WorkflowRegistry.empty
            |> WorkflowRegistry.register "status-signal" (fun _ ->
                durable {
                    let! approved = Workflow.waitForSignal "approved"
                    return "approved:" + approved
                })
            |> Result.bind (
                WorkflowRegistry.register "status-activity" (fun orderId ->
                    durable {
                        let! reserved = Workflow.call "reserve" orderId
                        return reserved
                    })
            )

        match registered with
        | Ok registry -> registry
        | Error error -> failwith (string error)

    let private statusOf basin workflows instanceId =
        DurableClient.getStatusWith basin workflows instanceId

    let private commitFoldedStart basin instanceId workflowName input =
        async {
            let key = DurableClient.instanceKey instanceId
            do! S2Substrate.ensureStreams basin key
            let pair = S2Substrate.streams basin key
            let! owner = S2Substrate.claimWith (FenceToken("status/" + InstanceId.value instanceId)) pair

            let! _ =
                S2Substrate.commitText StepRecordCodec.encode [ Incoming(WorkflowStarted(workflowName, input)) ] owner

            return owner
        }

    let private deleteInstance basin instanceId =
        async {
            let key = DurableClient.instanceKey instanceId
            do! basin |> S2.deleteStream (StorageKey.inboxStreamName key)
            do! basin |> S2.deleteStream (StorageKey.logStreamName key)
        }

    let private runWorkload ctx =
        ProofOperation.run
            ctx
            "durable.status"
            "durable-status"
            { ProofOperationOptions.empty with
                Key = Some "durable-status" }
            (async {
                let s2 = WorkloadContext.requireS2 ctx

                let suffix = string (int64 (Reports.nowMillis ()))
                let basinName = "durable-status-" + suffix
                let emptyInstance = instance suffix "empty"
                let runningInstance = instance suffix "running"
                let signalInstance = instance suffix "signal"
                let missingInstance = instance suffix "missing"

                let! _ = s2.Client |> S2.createBasin basinName
                let basin = s2.Client |> S2.basin basinName

                let workflows = registerWorkflows ()

                let! empty = statusOf basin workflows emptyInstance

                let emptyInstanceIsNotFound =
                    match empty with
                    | DurableClientStatusRead.Succeeded InstanceNotFound -> true
                    | _ -> false

                let! _runningOwner = commitFoldedStart basin runningInstance activityWorkflow "order-1"
                let! running = statusOf basin workflows runningInstance

                let foldedStartNeedingStepIsRunning =
                    match running with
                    | DurableClientStatusRead.Succeeded(InstanceRunning name) -> name = activityWorkflow
                    | _ -> false

                let! _start = DurableClient.startWith basin signalInstance signalWorkflow "order-2"
                let signalKey = DurableClient.instanceKey signalInstance
                let signalPair = S2Substrate.streams basin signalKey
                let! signalOwner = S2Substrate.claimWith (FenceToken "status/signal-owner") signalPair

                let! _firstTick =
                    DurableHost.runWorkflowTick (options "status" 100L) workflows ActivityRegistry.empty signalOwner

                let! waiting = statusOf basin workflows signalInstance

                let waitingStatusComesFromReplay =
                    match waiting with
                    | DurableClientStatusRead.Succeeded(InstanceWaiting(name, OpId 0, NeedsEvent(Signal "approved"))) ->
                        name = signalWorkflow
                    | _ -> false

                let! _signal = DurableClient.raiseSignalWith basin signalInstance 0L "approved" "yes"

                let! _secondTick =
                    DurableHost.runWorkflowTick (options "status" 101L) workflows ActivityRegistry.empty signalOwner

                let! completed = statusOf basin workflows signalInstance

                let completedStatusComesFromHistory =
                    match completed with
                    | DurableClientStatusRead.Succeeded(InstanceCompleted(name, "approved:yes")) ->
                        name = signalWorkflow
                    | _ -> false

                let! _missingStart = DurableClient.startWith basin missingInstance missingWorkflow "order-3"
                let missingKey = DurableClient.instanceKey missingInstance
                let missingPair = S2Substrate.streams basin missingKey
                let! missingOwner = S2Substrate.claimWith (FenceToken "status/missing-owner") missingPair

                let! _missingTick =
                    DurableHost.runWorkflowTick (options "status" 200L) workflows ActivityRegistry.empty missingOwner

                let! missing = statusOf basin workflows missingInstance

                let missingWorkflowIsTypedFailure =
                    match missing with
                    | DurableClientStatusRead.Failed(DurableClientStatusFailure.WorkflowNotFound name) ->
                        name = missingWorkflow
                    | _ -> false

                do! deleteInstance basin missingInstance
                do! deleteInstance basin signalInstance
                do! deleteInstance basin runningInstance
                do! deleteInstance basin emptyInstance

                let result =
                    { EmptyInstanceIsNotFound = emptyInstanceIsNotFound
                      FoldedStartNeedingStepIsRunning = foldedStartNeedingStepIsRunning
                      WaitingStatusComesFromReplay = waitingStatusComesFromReplay
                      CompletedStatusComesFromHistory = completedStatusComesFromHistory
                      MissingWorkflowIsTypedFailure = missingWorkflowIsTypedFailure }

                do!
                    ctx.EmitSpan
                        "proof.durable_status.completed"
                        [ "proof.property", "durable-status"
                          "status.not_found", string result.EmptyInstanceIsNotFound
                          "status.running", string result.FoldedStartNeedingStepIsRunning
                          "status.waiting", string result.WaitingStatusComesFromReplay
                          "status.completed", string result.CompletedStatusComesFromHistory ]

                return result
            })

    let statusProperty =
        property "durable-status" {
            s2Lite ""
            workload runWorkload

            verify (fun v ->
                [ v.Expect.Workload "empty instance is not found" (fun result -> result.EmptyInstanceIsNotFound)
                  v.Expect.Workload "folded start needing a step is running" (fun result ->
                      result.FoldedStartNeedingStepIsRunning)
                  v.Expect.Workload "waiting status is derived from replay" (fun result ->
                      result.WaitingStatusComesFromReplay)
                  v.Expect.Workload "completed status is derived from history" (fun result ->
                      result.CompletedStatusComesFromHistory)
                  v.Expect.Workload "missing workflow is a typed status failure" (fun result ->
                      result.MissingWorkflowIsTypedFailure)
                  v.Trace.SpanExists
                      "durable status proof span emitted"
                      "proof.durable_status.completed"
                      [ "proof.property", "durable-status" ]
                  v.Trace.Operation
                      "durable status operation was recorded"
                      ({ TraceOperationMatch.named "durable.status" with
                          Status = Some "ok"
                          OutputContains =
                              [ "EmptyInstanceIsNotFound"
                                "WaitingStatusComesFromReplay"
                                "CompletedStatusComesFromHistory" ]
                          Count = Some 1 }) ])
        }

    let proof =
        proof "durable-status" {
            describedAs "DurableClient.getStatusWith derives instance status from folded durable history and replay."

            property statusProperty
        }
