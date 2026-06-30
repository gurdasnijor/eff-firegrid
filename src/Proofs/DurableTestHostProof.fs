namespace Eff.Proofs

open Eff
open Eff.Foundation.Durable
open Eff.Foundation.Durable.App

module DurableTestHostProof =
    module D = Eff.Foundation.Durable.App.Durable

    type DurableTestHostResult =
        { HostProvisioned: bool
          TypedWorkflowCompleted: bool
          CompletionAssertionUsedDurableStatus: bool
          CleanupRemovedTrackedInstance: bool }

    let private addOne =
        Activity.defineWith "test-add-one" string int string int (fun value -> async { return value + 1 })

    let private typedMath =
        Workflow.defineWith "test-typed-math" string int string int (fun value ->
            durable {
                let! incremented = D.call addOne value
                return incremented + 10
            })

    let private app =
        durableApp {
            activity addOne
            workflow typedMath
        }

    let private startedInstance =
        function
        | DurableAppStartResult.Started instanceId -> Some instanceId
        | DurableAppStartResult.Rejected _ -> None

    let private lastStatus =
        function
        | [] -> None
        | statuses -> statuses |> List.rev |> List.tryHead

    let private runWorkload ctx =
        ProofOperation.run
            ctx
            "durable.test_host"
            "durable-test-host"
            { ProofOperationOptions.empty with
                Key = Some "durable-test-host" }
            (async {
                let! host = DurableTestHost.start ctx app

                let! start = host.client.start typedMath 31
                let instance = startedInstance start

                let! ticks =
                    match instance with
                    | Some instanceId -> host.worker.runUntilIdle instanceId
                    | None -> async { return [] }

                let typedWorkflowCompleted =
                    match lastStatus ticks with
                    | Some(DurableWorkflowHostStatus.Ticked(DurableHostTickStatus.Completed("42", _))) -> true
                    | _ -> false

                let! completionAssertionUsedDurableStatus =
                    match instance with
                    | Some instanceId -> host.expect.completed instanceId "42"
                    | None -> async { return false }

                do! host.cleanup ()

                let! cleanupRemovedTrackedInstance =
                    match instance with
                    | Some instanceId -> host.expect.notFound instanceId
                    | None -> async { return false }

                let result =
                    { HostProvisioned = host.basinName.StartsWith("durable-test-host-")
                      TypedWorkflowCompleted = typedWorkflowCompleted
                      CompletionAssertionUsedDurableStatus = completionAssertionUsedDurableStatus
                      CleanupRemovedTrackedInstance = cleanupRemovedTrackedInstance }

                do!
                    ctx.EmitSpan
                        "proof.durable_test_host.completed"
                        [ "proof.property", "durable-test-host"
                          "test_host.provisioned", string result.HostProvisioned
                          "test_host.completed", string result.TypedWorkflowCompleted
                          "test_host.assertion", string result.CompletionAssertionUsedDurableStatus
                          "test_host.cleanup", string result.CleanupRemovedTrackedInstance ]

                return result
            })

    let testHostProperty =
        property "durable-test-host" {
            s2Lite ""
            workload runWorkload

            verify (fun v ->
                [ v.Expect.Workload "test host provisions isolated basin" (fun result -> result.HostProvisioned)
                  v.Expect.Workload "test host completes typed workflow" (fun result -> result.TypedWorkflowCompleted)
                  v.Expect.Workload "test host assertion uses durable status" (fun result ->
                      result.CompletionAssertionUsedDurableStatus)
                  v.Expect.Workload "test host cleanup removes tracked instance" (fun result ->
                      result.CleanupRemovedTrackedInstance)
                  v.Trace.SpanExists
                      "durable test host proof span emitted"
                      "proof.durable_test_host.completed"
                      [ "proof.property", "durable-test-host" ]
                  v.Trace.Operation
                      "durable test host operation was recorded"
                      ({ TraceOperationMatch.named "durable.test_host" with
                          Status = Some "ok"
                          OutputContains =
                              [ "HostProvisioned"
                                "CompletionAssertionUsedDurableStatus"
                                "CleanupRemovedTrackedInstance" ]
                          Count = Some 1 }) ])
        }

    let proof =
        proof "durable-test-host" {
            describedAs
                "DurableTestHost provisions isolated proof storage, exposes tracked app client and worker handles, and verifies assertions by reading durable status."

            property testHostProperty
        }
