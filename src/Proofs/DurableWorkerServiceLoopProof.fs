namespace Eff.Proofs

open Eff
open Eff.Foundation.Durable
open Eff.Foundation.Durable.App

module DurableWorkerServiceLoopProof =
    module D = Eff.Foundation.Durable.App.Durable

    type DurableWorkerServiceLoopResult =
        { StartsManyInstances: bool
          RunReadyWithHonorsBound: bool
          RepeatedRunReadyCompletesAllRunnable: bool
          CompletedInstancesBecomeInactive: bool
          WaitingInstanceDoesNotStarveRunnable: bool
          RunForeverCancellationExitsWithoutWrites: bool }

    let private addOne =
        Activity.defineWith "service-loop-add-one" string int string int (fun value -> async { return value + 1 })

    let private approved = Signal.define "service-loop-approved"

    let private typedMath =
        Workflow.defineWith "service-loop-typed-math" string int string int (fun value ->
            durable {
                let! incremented = D.call addOne value
                return incremented + 10
            })

    let private waitingApproval =
        Workflow.define "service-loop-waiting-approval" (fun requestId ->
            durable {
                let! approver = D.waitForSignal approved
                return requestId + ":" + approver
            })

    let private app =
        durableApp {
            activity addOne
            workflow typedMath
            workflow waitingApproval
        }

    let private deleteInstance basin instanceId =
        async {
            let key = DurableClient.instanceKey instanceId
            do! basin |> S2.deleteStream (StorageKey.inboxStreamName key)
            do! basin |> S2.deleteStream (StorageKey.logStreamName key)
        }

    let private completedInt =
        function
        | DurableAppTypedWorkflowStatus.Completed value -> Some value
        | _ -> None

    let private isWaiting =
        function
        | DurableAppTypedWorkflowStatus.Waiting _ -> true
        | _ -> false

    let private retryRangeRead operation =
        let rec loop remaining =
            async {
                let! result = Async.Catch operation

                match result with
                | Choice1Of2 value -> return value
                | Choice2Of2 error when remaining > 0 && error.Message.Contains("Range not satisfiable") ->
                    do! Async.Sleep 25
                    return! loop (remaining - 1)
                | Choice2Of2 error -> return raise error
            }

        loop 3

    let private runReadyUntilAllComplete
        (worker: DurableAppWorker)
        (client: DurableAppClient)
        (workflow: Workflow<int, int>)
        (instances: (InstanceId * int) list)
        maxAttempts
        =
        let rec loop attempt =
            async {
                if attempt > maxAttempts then
                    return false
                else
                    let! _ = retryRangeRead (worker.runReady ())

                    let mutable allCompleted = true

                    for instanceId, expected in instances do
                        let! status = client.statusOf workflow instanceId
                        allCompleted <- allCompleted && completedInt status = Some expected

                    if allCompleted then
                        return true
                    else
                        return! loop (attempt + 1)
            }

        loop 1

    let private runWorkload ctx =
        ProofOperation.run
            ctx
            "durable.worker_service_loop"
            "durable-worker-service-loop"
            { ProofOperationOptions.empty with
                Key = Some "durable-worker-service-loop" }
            (async {
                let s2 = WorkloadContext.requireS2 ctx
                let suffix = string (int64 (Reports.nowMillis ()))
                let basinName = "durable-worker-service-loop-" + suffix

                let! _ = s2.Client |> S2.createBasin basinName
                let basin = s2.Client |> S2.basin basinName
                let storage = DurableStorage.s2 basin

                let client =
                    app |> DurableApp.clientWith ({ Storage = storage }: DurableAppClientConfig)

                let worker =
                    app
                    |> DurableApp.workerWith (
                        { Storage = storage
                          HostId = "svc-loop"
                          MaxRunUntilIdleTicks = Some 20 }
                        : DurableAppWorkerConfig
                    )

                let mathInstances =
                    [ for value in 1..5 ->
                          InstanceId.create ("service-loop-math-" + string value + "-" + suffix), value, value + 11 ]

                for instanceId, input, _ in mathInstances do
                    let! _ = client.startWith instanceId typedMath input
                    ()

                let! discovered = worker.discover ()
                let discoveredSet = discovered |> Set.ofList

                let startsManyInstances =
                    mathInstances
                    |> List.forall (fun (instanceId, _, _) -> discoveredSet.Contains instanceId)

                let! boundedPass = retryRangeRead (worker.runReadyWith 2)
                let runReadyWithHonorsBound = boundedPass.Instances.Length <= 2

                let expectedMath =
                    mathInstances
                    |> List.map (fun (instanceId, _, expected) -> instanceId, expected)

                let! repeatedRunReadyCompletesAllRunnable =
                    runReadyUntilAllComplete worker client typedMath expectedMath 10

                let! idlePass = retryRangeRead (worker.runReady ())

                let completedInstancesBecomeInactive =
                    idlePass.Instances
                    |> List.filter (fun result ->
                        mathInstances
                        |> List.exists (fun (instanceId, _, _) -> instanceId = result.InstanceId))
                    |> List.forall (fun result -> not result.Active)

                let waitingId = InstanceId.create ("service-loop-waiting-" + suffix)
                let! _ = client.startWith waitingId waitingApproval "request-1"
                let! _ = retryRangeRead (worker.runUntilIdle waitingId)

                let lateRunnableId = InstanceId.create ("service-loop-late-" + suffix)
                let! _ = client.startWith lateRunnableId typedMath 99
                let! _ = retryRangeRead (worker.runUntilIdle lateRunnableId)
                let! lateRunnableStatus = client.statusOf typedMath lateRunnableId
                let! waitingStatus = client.statusOf waitingApproval waitingId

                let waitingInstanceDoesNotStarveRunnable =
                    completedInt lateRunnableStatus = Some 110 && isWaiting waitingStatus

                let allInstances =
                    waitingId
                    :: lateRunnableId
                    :: (mathInstances |> List.map (fun (instanceId, _, _) -> instanceId))

                let idleBasinName = basinName + "-idle"
                let! _ = s2.Client |> S2.createBasin idleBasinName
                let idleBasin = s2.Client |> S2.basin idleBasinName
                let idleStorage = DurableStorage.s2 idleBasin

                let idleWorker =
                    app
                    |> DurableApp.workerWith (
                        { Storage = idleStorage
                          HostId = "svc-idle"
                          MaxRunUntilIdleTicks = Some 20 }
                        : DurableAppWorkerConfig
                    )

                let! beforeIdleStreams = idleBasin |> S2.listStreamsWith ""

                use cts = new System.Threading.CancellationTokenSource()
                cts.CancelAfter(50)
                do! idleWorker.runForever cts.Token

                let! afterIdleStreams = idleBasin |> S2.listStreamsWith ""

                let beforeIdleStreamNames =
                    beforeIdleStreams |> List.map (fun stream -> stream.Name)

                let afterIdleStreamNames = afterIdleStreams |> List.map (fun stream -> stream.Name)

                let runForeverCancellationExitsWithoutWrites =
                    beforeIdleStreamNames = afterIdleStreamNames

                for instanceId in allInstances do
                    do! deleteInstance basin instanceId

                let result =
                    { StartsManyInstances = startsManyInstances
                      RunReadyWithHonorsBound = runReadyWithHonorsBound
                      RepeatedRunReadyCompletesAllRunnable = repeatedRunReadyCompletesAllRunnable
                      CompletedInstancesBecomeInactive = completedInstancesBecomeInactive
                      WaitingInstanceDoesNotStarveRunnable = waitingInstanceDoesNotStarveRunnable
                      RunForeverCancellationExitsWithoutWrites = runForeverCancellationExitsWithoutWrites }

                do!
                    ctx.EmitSpan
                        "proof.durable_worker_service_loop.completed"
                        [ "proof.property", "durable-worker-service-loop"
                          "service_loop.starts_many", string result.StartsManyInstances
                          "service_loop.bound", string result.RunReadyWithHonorsBound
                          "service_loop.eventual_completion", string result.RepeatedRunReadyCompletesAllRunnable
                          "service_loop.inactive", string result.CompletedInstancesBecomeInactive
                          "service_loop.waiting_no_starve", string result.WaitingInstanceDoesNotStarveRunnable
                          "service_loop.cancel_no_writes", string result.RunForeverCancellationExitsWithoutWrites ]

                return result
            })

    let workerServiceLoopProperty =
        property "durable-worker-service-loop" {
            s2Lite ""
            workload runWorkload

            verify (fun v ->
                [ v.Expect.Workload "worker starts many service-loop instances" (fun result ->
                      result.StartsManyInstances)
                  v.Expect.Workload "runReadyWith honors the instance bound" (fun result ->
                      result.RunReadyWithHonorsBound)
                  v.Expect.Workload "repeated runReady eventually completes all runnable instances" (fun result ->
                      result.RepeatedRunReadyCompletesAllRunnable)
                  v.Expect.Workload "completed instances become inactive" (fun result ->
                      result.CompletedInstancesBecomeInactive)
                  v.Expect.Workload "waiting signal instance does not starve runnable instances" (fun result ->
                      result.WaitingInstanceDoesNotStarveRunnable)
                  v.Expect.Workload "runForever cancellation exits without extra durable writes" (fun result ->
                      result.RunForeverCancellationExitsWithoutWrites)
                  v.Trace.SpanExists
                      "durable worker service loop proof span emitted"
                      "proof.durable_worker_service_loop.completed"
                      [ "proof.property", "durable-worker-service-loop" ]
                  v.Trace.Operation
                      "durable worker service loop operation was recorded"
                      ({ TraceOperationMatch.named "durable.worker_service_loop" with
                          Status = Some "ok"
                          OutputContains =
                              [ "StartsManyInstances"
                                "RunReadyWithHonorsBound"
                                "RepeatedRunReadyCompletesAllRunnable"
                                "WaitingInstanceDoesNotStarveRunnable"
                                "RunForeverCancellationExitsWithoutWrites" ]
                          Count = Some 1 }) ])
        }

    let proof =
        proof "durable-worker-service-loop" {
            describedAs
                "Durable app worker service loops process many instances across bounded passes, leave waiting instances idle, and exit cancellation without extra writes."

            property workerServiceLoopProperty
        }
