namespace Eff.Proofs

open Eff
open Eff.Foundation.Durable

module DurableRuntimeProof =
    type DurableRuntimeResult =
        { GeneratedStartReturnsInstance: bool
          ActivityWorkflowCompletesThroughRuntime: bool
          RuntimeStatusReportsCompletion: bool
          RuntimeSignalUsesIsolatedSource: bool
          SignalWorkflowCompletesThroughRuntime: bool }

    let private checkout = WorkflowName.create "runtime-checkout"

    let private approval = WorkflowName.create "runtime-approval"

    let private registerWorkflows () =
        let registered =
            WorkflowRegistry.empty
            |> WorkflowRegistry.register "runtime-checkout" (fun orderId ->
                durable {
                    let! reserved = Workflow.call "reserve" orderId
                    return reserved
                })
            |> Result.bind (
                WorkflowRegistry.register "runtime-approval" (fun _ ->
                    durable {
                        let! approved = Workflow.waitForSignal "approved"
                        return "approved:" + approved
                    })
            )

        match registered with
        | Ok registry -> registry
        | Error error -> failwith (string error)

    let private registerActivities () =
        match
            ActivityRegistry.empty
            |> ActivityRegistry.register "reserve" (fun orderId -> async { return "reserved:" + orderId })
        with
        | Ok registry -> registry
        | Error error -> failwith (string error)

    let private deleteInstance basin instanceId =
        async {
            let key = DurableClient.instanceKey instanceId
            do! basin |> S2.deleteStream (StorageKey.inboxStreamName key)
            do! basin |> S2.deleteStream (StorageKey.logStreamName key)
        }

    let private lastStatus =
        function
        | [] -> None
        | statuses -> statuses |> List.rev |> List.tryHead

    let private runWorkload ctx =
        ProofOperation.run
            ctx
            "durable.runtime"
            "durable-runtime"
            { ProofOperationOptions.empty with
                Key = Some "durable-runtime" }
            (async {
                let s2 = WorkloadContext.requireS2 ctx

                let suffix = string (int64 (Reports.nowMillis ()))
                let basinName = "durable-runtime-" + suffix

                let! _ = s2.Client |> S2.createBasin basinName
                let basin = s2.Client |> S2.basin basinName

                let options =
                    { DurableRuntimeOptions.create "runtime-proof" with
                        MaxRunUntilIdleTicks = 10 }

                let runtime =
                    DurableRuntime.create options basin (registerWorkflows ()) (registerActivities ())

                let! checkoutStart = runtime.Client.Start checkout "order-1"

                let checkoutInstance =
                    match checkoutStart with
                    | DurableClientStartStatus.Accepted ack -> Some ack.InstanceId
                    | _ -> None

                let generatedStartReturnsInstance =
                    match checkoutInstance with
                    | Some instanceId ->
                        InstanceId.value instanceId <> ""
                        && (InstanceId.value instanceId).StartsWith(WorkflowName.value checkout)
                    | None -> false

                let! checkoutTicks =
                    match checkoutInstance with
                    | Some instanceId -> runtime.Host.RunUntilIdle instanceId
                    | None -> async { return [] }

                let activityWorkflowCompletesThroughRuntime =
                    match lastStatus checkoutTicks with
                    | Some(DurableWorkflowHostStatus.Ticked(DurableHostTickStatus.Completed("reserved:order-1", _))) ->
                        true
                    | _ -> false

                let! checkoutStatus =
                    match checkoutInstance with
                    | Some instanceId -> runtime.Client.GetStatus instanceId
                    | None -> async { return DurableClientStatusRead.Succeeded InstanceNotFound }

                let runtimeStatusReportsCompletion =
                    match checkoutStatus with
                    | DurableClientStatusRead.Succeeded(InstanceCompleted(name, "reserved:order-1")) -> name = checkout
                    | _ -> false

                let approvalInstance = InstanceId.create ("approval-" + suffix)
                let! _approvalStart = runtime.Client.StartWith approvalInstance approval "order-2"
                let! _waitingTick = runtime.Host.RunOnce approvalInstance
                let! firstSignal = runtime.Client.RaiseSignal approvalInstance "approved" "yes"
                let! secondSignal = runtime.Client.RaiseSignal approvalInstance "ignored" "no"

                let runtimeSignalUsesIsolatedSource =
                    match firstSignal, secondSignal with
                    | DurableClientSignalStatus.Accepted first, DurableClientSignalStatus.Accepted second ->
                        first.Source <> "client:signal"
                        && first.Source = second.Source
                        && first.SourceSeqNum = 0L
                        && second.SourceSeqNum = 1L
                    | _ -> false

                let! approvalTicks = runtime.Host.RunUntilIdle approvalInstance

                let signalWorkflowCompletesThroughRuntime =
                    match lastStatus approvalTicks with
                    | Some(DurableWorkflowHostStatus.Ticked(DurableHostTickStatus.Completed("approved:yes", _))) -> true
                    | _ -> false

                match checkoutInstance with
                | Some instanceId -> do! deleteInstance basin instanceId
                | None -> ()

                do! deleteInstance basin approvalInstance

                let result =
                    { GeneratedStartReturnsInstance = generatedStartReturnsInstance
                      ActivityWorkflowCompletesThroughRuntime = activityWorkflowCompletesThroughRuntime
                      RuntimeStatusReportsCompletion = runtimeStatusReportsCompletion
                      RuntimeSignalUsesIsolatedSource = runtimeSignalUsesIsolatedSource
                      SignalWorkflowCompletesThroughRuntime = signalWorkflowCompletesThroughRuntime }

                do!
                    ctx.EmitSpan
                        "proof.durable_runtime.completed"
                        [ "proof.property", "durable-runtime"
                          "runtime.start", string result.GeneratedStartReturnsInstance
                          "runtime.activity", string result.ActivityWorkflowCompletesThroughRuntime
                          "runtime.status", string result.RuntimeStatusReportsCompletion
                          "runtime.signal", string result.SignalWorkflowCompletesThroughRuntime ]

                return result
            })

    let runtimeProperty =
        property "durable-runtime" {
            s2Lite ""
            workload runWorkload

            verify (fun v ->
                [ v.Expect.Workload "generated start returns an instance id" (fun result ->
                      result.GeneratedStartReturnsInstance)
                  v.Expect.Workload "activity workflow completes through runtime host" (fun result ->
                      result.ActivityWorkflowCompletesThroughRuntime)
                  v.Expect.Workload "runtime status reports completion" (fun result ->
                      result.RuntimeStatusReportsCompletion)
                  v.Expect.Workload "runtime signal uses an isolated monotonic source" (fun result ->
                      result.RuntimeSignalUsesIsolatedSource)
                  v.Expect.Workload "signal workflow completes through runtime host" (fun result ->
                      result.SignalWorkflowCompletesThroughRuntime)
                  v.Trace.SpanExists
                      "durable runtime proof span emitted"
                      "proof.durable_runtime.completed"
                      [ "proof.property", "durable-runtime" ]
                  v.Trace.Operation
                      "durable runtime operation was recorded"
                      ({ TraceOperationMatch.named "durable.runtime" with
                          Status = Some "ok"
                          OutputContains =
                              [ "GeneratedStartReturnsInstance"
                                "RuntimeStatusReportsCompletion"
                                "SignalWorkflowCompletesThroughRuntime" ]
                          Count = Some 1 }) ])
        }

    let proof =
        proof "durable-runtime" {
            describedAs
                "DurableRuntime.create exposes the first ergonomic client and host facade over the proven durable core."

            property runtimeProperty
        }
