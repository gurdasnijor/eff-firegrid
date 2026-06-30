namespace Eff.Proofs

open Eff
open Eff.Foundation.Durable
open Eff.Foundation.Durable.App

module DurableWorkerLoopProof =
    module D = Eff.Foundation.Durable.App.Durable

    type DurableWorkerLoopResult =
        { DiscoversStartedInstances: bool
          RunReadyCompletesDiscoveredInstances: bool
          RunReadyStatusReadsCompletion: bool
          RunReadyHonorsBound: bool
          CompletedInstancesBecomeInactive: bool }

    let private addOne =
        Activity.defineWith "worker-loop-add-one" string int string int (fun value -> async { return value + 1 })

    let private typedMath =
        Workflow.defineWith "worker-loop-typed-math" string int string int (fun value ->
            durable {
                let! incremented = D.call addOne value
                return incremented + 10
            })

    let private app =
        durableApp {
            activity addOne
            workflow typedMath
        }

    let private deleteInstance basin instanceId =
        async {
            let key = DurableClient.instanceKey instanceId
            do! basin |> S2.deleteStream (StorageKey.inboxStreamName key)
            do! basin |> S2.deleteStream (StorageKey.logStreamName key)
        }

    let private completedOutput =
        function
        | DurableAppWorkflowStatus.Completed(_, output) -> Some output
        | _ -> None

    let private runWorkload ctx =
        ProofOperation.run
            ctx
            "durable.worker_loop"
            "durable-worker-loop"
            { ProofOperationOptions.empty with
                Key = Some "durable-worker-loop" }
            (async {
                let s2 = WorkloadContext.requireS2 ctx
                let suffix = string (int64 (Reports.nowMillis ()))
                let basinName = "durable-worker-loop-" + suffix

                let! _ = s2.Client |> S2.createBasin basinName
                let basin = s2.Client |> S2.basin basinName
                let storage = DurableStorage.s2 basin

                let client =
                    app |> DurableApp.clientWith ({ Storage = storage }: DurableAppClientConfig)

                let worker =
                    app
                    |> DurableApp.workerWith (
                        { Storage = storage
                          HostId = "loop-proof"
                          MaxRunUntilIdleTicks = Some 10 }
                        : DurableAppWorkerConfig
                    )

                let first = InstanceId.create ("worker-loop-first-" + suffix)
                let second = InstanceId.create ("worker-loop-second-" + suffix)

                let! _ = client.startWith first typedMath 31
                let! _ = client.startWith second typedMath 32

                let! discovered = worker.discover ()
                let discoveredSet = discovered |> Set.ofList

                let discoversStartedInstances =
                    discoveredSet.Contains first && discoveredSet.Contains second

                let! boundedPass = worker.runReadyWith 1

                let runReadyHonorsBound = boundedPass.Instances.Length = 1

                let! readyPass = worker.runReady ()

                let completedInstances =
                    readyPass.Instances |> List.map (fun result -> result.InstanceId) |> Set.ofList

                let runReadyCompletesDiscoveredInstances =
                    completedInstances.Contains first && completedInstances.Contains second

                let! firstStatus = client.status first
                let! secondStatus = client.status second

                let runReadyStatusReadsCompletion =
                    completedOutput firstStatus = Some "42"
                    && completedOutput secondStatus = Some "43"

                let! idlePass = worker.runReady ()

                let completedInstancesBecomeInactive =
                    idlePass.ActiveInstances = 0
                    && idlePass.Instances
                       |> List.filter (fun result -> completedInstances.Contains result.InstanceId)
                       |> List.forall (fun result -> not result.Active)

                do! deleteInstance basin first
                do! deleteInstance basin second

                let result =
                    { DiscoversStartedInstances = discoversStartedInstances
                      RunReadyCompletesDiscoveredInstances = runReadyCompletesDiscoveredInstances
                      RunReadyStatusReadsCompletion = runReadyStatusReadsCompletion
                      RunReadyHonorsBound = runReadyHonorsBound
                      CompletedInstancesBecomeInactive = completedInstancesBecomeInactive }

                do!
                    ctx.EmitSpan
                        "proof.durable_worker_loop.completed"
                        [ "proof.property", "durable-worker-loop"
                          "worker_loop.discovered", string result.DiscoversStartedInstances
                          "worker_loop.completed", string result.RunReadyCompletesDiscoveredInstances
                          "worker_loop.status", string result.RunReadyStatusReadsCompletion
                          "worker_loop.bound", string result.RunReadyHonorsBound
                          "worker_loop.inactive", string result.CompletedInstancesBecomeInactive ]

                return result
            })

    let workerLoopProperty =
        property "durable-worker-loop" {
            s2Lite ""
            workload runWorkload

            verify (fun v ->
                [ v.Expect.Workload "worker discovers started instances" (fun result ->
                      result.DiscoversStartedInstances)
                  v.Expect.Workload "runReady completes discovered instances" (fun result ->
                      result.RunReadyCompletesDiscoveredInstances)
                  v.Expect.Workload "runReady status reads completion" (fun result ->
                      result.RunReadyStatusReadsCompletion)
                  v.Expect.Workload "runReadyWith honors max instance bound" (fun result -> result.RunReadyHonorsBound)
                  v.Expect.Workload "completed instances become inactive for service polling" (fun result ->
                      result.CompletedInstancesBecomeInactive)
                  v.Trace.SpanExists
                      "durable worker loop proof span emitted"
                      "proof.durable_worker_loop.completed"
                      [ "proof.property", "durable-worker-loop" ]
                  v.Trace.Operation
                      "durable worker loop operation was recorded"
                      ({ TraceOperationMatch.named "durable.worker_loop" with
                          Status = Some "ok"
                          OutputContains =
                              [ "DiscoversStartedInstances"
                                "RunReadyCompletesDiscoveredInstances"
                                "RunReadyHonorsBound" ]
                          Count = Some 1 }) ])
        }

    let proof =
        proof "durable-worker-loop" {
            describedAs
                "Durable app workers can discover instance inbox streams and run bounded ready-instance passes over the existing per-instance durable worker."

            property workerLoopProperty
        }
