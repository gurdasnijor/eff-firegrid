namespace Eff.Proofs

open Eff
open Eff.Foundation.Durable
open Eff.Foundation.Durable.App

module DurableTypedStatusProof =
    module D = Eff.Foundation.Durable.App.Durable

    type DurableTypedStatusResult =
        { TypedCompletionDecodes: bool
          RawStatusRemainsAvailable: bool
          WaitingStatusUsesWorkflowHandle: bool
          WorkflowMismatchFailsClosed: bool
          OutputDecodeFailureIsTyped: bool }

    let private addOne =
        Activity.defineWith "typed-status-add-one" string int string int (fun value -> async { return value + 1 })

    let private typedMath =
        Workflow.defineWith "typed-status-math" string int string int (fun value ->
            durable {
                let! incremented = D.call addOne value
                return incremented + 10
            })

    let private approved = Signal.define "typed-status-approved"

    let private approval =
        Workflow.define "typed-status-approval" (fun orderId ->
            durable {
                let! approver = D.waitForSignal approved
                return orderId + ":" + approver
            })

    let private decodeBroken =
        Workflow.defineWith
            "typed-status-decode-broken"
            id
            id
            id
            (fun _ -> failwith "status decode failed")
            (fun value -> durable { return value })

    let private app =
        durableApp {
            activity addOne
            workflow typedMath
            workflow approval
            workflow decodeBroken
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
            "durable.typed_status"
            "durable-typed-status"
            { ProofOperationOptions.empty with
                Key = Some "durable-typed-status" }
            (async {
                let s2 = WorkloadContext.requireS2 ctx
                let suffix = string (int64 (Reports.nowMillis ()))
                let basinName = "durable-typed-status-" + suffix

                let! _ = s2.Client |> S2.createBasin basinName
                let basin = s2.Client |> S2.basin basinName
                let storage = DurableStorage.s2 basin

                let client =
                    app |> DurableApp.clientWith ({ Storage = storage }: DurableAppClientConfig)

                let worker =
                    app
                    |> DurableApp.workerWith (
                        { Storage = storage
                          HostId = "typed-status"
                          MaxRunUntilIdleTicks = Some 10 }
                        : DurableAppWorkerConfig
                    )

                let typedMathInstance = InstanceId.create ("typed-status-math-" + suffix)
                let! _ = client.startWith typedMathInstance typedMath 31
                let! _ = worker.runUntilIdle typedMathInstance

                let! typedMathStatus = client.statusOf typedMath typedMathInstance
                let! rawMathStatus = client.status typedMathInstance
                let! mismatchStatus = client.statusOf approval typedMathInstance

                let typedCompletionDecodes =
                    typedMathStatus = DurableAppTypedWorkflowStatus.Completed 42

                let rawStatusRemainsAvailable =
                    rawMathStatus = DurableAppWorkflowStatus.Completed("typed-status-math", "42")

                let expectedMismatch =
                    DurableAppTypedWorkflowStatus.Failed(
                        DurableAppStatusFailure.WorkflowMismatch("typed-status-approval", "typed-status-math")
                    )

                let workflowMismatchFailsClosed = mismatchStatus = expectedMismatch

                let approvalInstance = InstanceId.create ("typed-status-approval-" + suffix)
                let! _ = client.startWith approvalInstance approval "order-1"
                let! _ = worker.runUntilIdle approvalInstance
                let! waitingStatus = client.statusOf approval approvalInstance

                let waitingStatusUsesWorkflowHandle =
                    waitingStatus = DurableAppTypedWorkflowStatus.Waiting(DurableAppNeed.Signal "typed-status-approved")

                let brokenInstance = InstanceId.create ("typed-status-broken-" + suffix)
                let! _ = client.startWith brokenInstance decodeBroken "ok"
                let! _ = worker.runUntilIdle brokenInstance
                let! brokenStatus = client.statusOf decodeBroken brokenInstance

                let outputDecodeFailureIsTyped =
                    match brokenStatus with
                    | DurableAppTypedWorkflowStatus.Failed failure ->
                        match failure with
                        | DurableAppStatusFailure.OutputDecodeFailed("typed-status-decode-broken", error) ->
                            error.Contains("status decode failed")
                        | _ -> false
                    | _ -> false

                do! deleteInstance basin typedMathInstance
                do! deleteInstance basin approvalInstance
                do! deleteInstance basin brokenInstance

                let result =
                    { TypedCompletionDecodes = typedCompletionDecodes
                      RawStatusRemainsAvailable = rawStatusRemainsAvailable
                      WaitingStatusUsesWorkflowHandle = waitingStatusUsesWorkflowHandle
                      WorkflowMismatchFailsClosed = workflowMismatchFailsClosed
                      OutputDecodeFailureIsTyped = outputDecodeFailureIsTyped }

                do!
                    ctx.EmitSpan
                        "proof.durable_typed_status.completed"
                        [ "proof.property", "durable-typed-status"
                          "typed_status.completed", string result.TypedCompletionDecodes
                          "typed_status.raw", string result.RawStatusRemainsAvailable
                          "typed_status.waiting", string result.WaitingStatusUsesWorkflowHandle
                          "typed_status.mismatch", string result.WorkflowMismatchFailsClosed
                          "typed_status.decode_failure", string result.OutputDecodeFailureIsTyped ]

                return result
            })

    let typedStatusProperty =
        property "durable-typed-status" {
            s2Lite ""
            workload runWorkload

            verify (fun v ->
                [ v.Expect.Workload "typed completion status decodes through workflow handle" (fun result ->
                      result.TypedCompletionDecodes)
                  v.Expect.Workload "raw string status remains available" (fun result ->
                      result.RawStatusRemainsAvailable)
                  v.Expect.Workload "waiting typed status uses the workflow handle" (fun result ->
                      result.WaitingStatusUsesWorkflowHandle)
                  v.Expect.Workload "wrong workflow handle fails closed" (fun result ->
                      result.WorkflowMismatchFailsClosed)
                  v.Expect.Workload "output decode failure is typed" (fun result -> result.OutputDecodeFailureIsTyped)
                  v.Trace.SpanExists
                      "durable typed status proof span emitted"
                      "proof.durable_typed_status.completed"
                      [ "proof.property", "durable-typed-status" ]
                  v.Trace.Operation
                      "durable typed status operation was recorded"
                      ({ TraceOperationMatch.named "durable.typed_status" with
                          Status = Some "ok"
                          OutputContains =
                              [ "TypedCompletionDecodes"
                                "WorkflowMismatchFailsClosed"
                                "OutputDecodeFailureIsTyped" ]
                          Count = Some 1 }) ])
        }

    let proof =
        proof "durable-typed-status" {
            describedAs
                "Durable app clients can read workflow-specific status that decodes completion payloads through the workflow handle and fails closed on handle mismatch."

            property typedStatusProperty
        }
