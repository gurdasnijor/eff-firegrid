namespace Eff.Proofs

open Eff
open Eff.Foundation.Durable
open Eff.Foundation.Durable.App

type DurableTestClient internal (inner: DurableAppClient, track: InstanceId -> unit) =
    member _.start (workflow: Workflow<'input, 'output>) (input: 'input) =
        async {
            let! result = inner.start workflow input

            match result with
            | DurableAppStartResult.Started instanceId -> track instanceId
            | DurableAppStartResult.Rejected _ -> ()

            return result
        }

    member _.startWith (instanceId: InstanceId) (workflow: Workflow<'input, 'output>) (input: 'input) =
        async {
            let! result = inner.startWith instanceId workflow input

            match result with
            | DurableAppStartResult.Started acceptedId -> track acceptedId
            | DurableAppStartResult.Rejected _ -> ()

            return result
        }

    member _.signal (instanceId: InstanceId) (signal: Signal<'payload>) (payload: 'payload) =
        inner.signal instanceId signal payload

    member _.status instanceId = inner.status instanceId

    member _.statusOf (workflow: Workflow<'input, 'output>) instanceId = inner.statusOf workflow instanceId

type DurableTestExpect internal (client: DurableTestClient) =
    member _.completed instanceId expectedOutput =
        async {
            let! status = client.status instanceId

            return
                match status with
                | DurableAppWorkflowStatus.Completed(_, output) -> output = expectedOutput
                | _ -> false
        }

    member _.notFound instanceId =
        async {
            let! status = client.status instanceId
            return status = DurableAppWorkflowStatus.NotFound
        }

    member _.completedOf (workflow: Workflow<'input, 'output>) instanceId expectedOutput =
        async {
            let! status = client.statusOf workflow instanceId

            return
                match status with
                | DurableAppTypedWorkflowStatus.Completed output -> output = expectedOutput
                | _ -> false
        }

type DurableTestHost =
    { client: DurableTestClient
      worker: DurableAppWorker
      expect: DurableTestExpect
      basinName: string
      cleanup: unit -> Async<unit> }

[<RequireQualifiedAccess>]
module DurableTestHost =
    let private deleteInstance basin instanceId =
        async {
            let key = DurableClient.instanceKey instanceId
            do! basin |> S2.deleteStream (StorageKey.inboxStreamName key)
            do! basin |> S2.deleteStream (StorageKey.logStreamName key)
        }

    let start (ctx: WorkloadContext) app =
        async {
            let s2 = WorkloadContext.requireS2 ctx
            let suffix = string (int64 (Reports.nowMillis ()))
            let basinName = "durable-test-host-" + suffix

            let! _ = s2.Client |> S2.createBasin basinName

            let basin = s2.Client |> S2.basin basinName
            let storage = DurableStorage.s2 basin
            let tracked = ResizeArray<InstanceId>()

            let track instanceId =
                if tracked |> Seq.exists ((=) instanceId) |> not then
                    tracked.Add instanceId

            let innerClient =
                app |> DurableApp.clientWith ({ Storage = storage }: DurableAppClientConfig)

            let client = DurableTestClient(innerClient, track)

            let worker =
                app
                |> DurableApp.workerWith (
                    { Storage = storage
                      HostId = "test-host"
                      MaxRunUntilIdleTicks = Some 20 }
                    : DurableAppWorkerConfig
                )

            let cleanup () =
                async {
                    for instanceId in tracked do
                        do! deleteInstance basin instanceId

                    do!
                        ctx.EmitSpan
                            "durable.test_host.cleaned"
                            [ "basin.name", basinName; "instance.count", string tracked.Count ]
                }

            do! ctx.EmitSpan "durable.test_host.started" [ "basin.name", basinName; "resource.kind", "durableTestHost" ]

            return
                { client = client
                  worker = worker
                  expect = DurableTestExpect client
                  basinName = basinName
                  cleanup = cleanup }
        }

    let runUntilCompleted
        (host: DurableTestHost)
        (workflow: Workflow<'input, 'output>)
        (input: 'input)
        (expectedOutput: 'output)
        =
        async {
            let! start = host.client.start workflow input

            match start with
            | DurableAppStartResult.Rejected _ -> return false
            | DurableAppStartResult.Started instanceId ->
                let! _ = host.worker.runUntilIdle instanceId
                return! host.expect.completedOf workflow instanceId expectedOutput
        }
