namespace Eff.Foundation.Durable.App

open Eff.Foundation.Durable

type DurableApp =
    private
        { Activities: ActivityRegistration list
          Workflows: WorkflowRegistration list }

[<RequireQualifiedAccess>]
module DurableApp =
    let empty = { Activities = []; Workflows = [] }

    let private failRegistry error =
        failwith ("durable app registry assembly failed: " + string error)

    let private activityRegistration (activity: Activity<'input, 'output>) : ActivityRegistration =
        { Name = ActivityName.value activity.Name
          Register =
            fun registry ->
                ActivityRegistry.register
                    (ActivityName.value activity.Name)
                    (fun input ->
                        async {
                            let! output = activity.Handler(activity.DecodeInput input)
                            return activity.EncodeOutput output
                        })
                    registry }

    let private workflowRegistration (workflow: Workflow<'input, 'output>) : WorkflowRegistration =
        { Name = WorkflowName.value workflow.Name
          Register =
            fun registry ->
                WorkflowRegistry.register
                    (WorkflowName.value workflow.Name)
                    (fun input ->
                        workflow.Factory(workflow.DecodeInput input)
                        |> Eff.Foundation.Durable.Durable.map workflow.EncodeOutput)
                    registry }

    let addActivity (activity: Activity<'input, 'output>) (app: DurableApp) =
        { app with
            Activities = app.Activities @ [ activityRegistration activity ] }

    let addWorkflow (workflow: Workflow<'input, 'output>) (app: DurableApp) =
        { app with
            Workflows = app.Workflows @ [ workflowRegistration workflow ] }

    let private activities (app: DurableApp) =
        ((Ok ActivityRegistry.empty), app.Activities)
        ||> List.fold (fun state registration -> state |> Result.bind registration.Register)
        |> function
            | Ok registry -> registry
            | Error error -> failRegistry error

    let private workflows (app: DurableApp) =
        ((Ok WorkflowRegistry.empty), app.Workflows)
        ||> List.fold (fun state registration -> state |> Result.bind registration.Register)
        |> function
            | Ok registry -> registry
            | Error error -> failRegistry error

    let private runtimeFrom options storage (app: DurableApp) =
        let (DurableStorage basin) = storage
        DurableRuntime.create options basin (workflows app) (activities app)

    let clientWith (config: DurableAppClientConfig) app =
        let runtime =
            runtimeFrom (DurableRuntimeOptions.create "durable-app-client") config.Storage app

        DurableAppClient runtime

    let client (config: DurableAppEnvironmentClientConfig) app =
        let storage = DurableAppEnvironment.storage config.Environment config.BasinName
        clientWith { Storage = storage } app

    let workerWith (config: DurableAppWorkerConfig) app =
        if config.HostId.Length > 15 then
            invalidArg (nameof config.HostId) "HostId must be 15 characters or fewer."

        let options =
            { DurableRuntimeOptions.create config.HostId with
                MaxRunUntilIdleTicks = config.MaxRunUntilIdleTicks |> Option.defaultValue 100 }

        let runtime = runtimeFrom options config.Storage app
        let (DurableStorage basin) = config.Storage

        let discover () =
            DurableAppWorkerSupport.discoverInstances basin

        let runReadyWith maxInstances =
            async {
                if maxInstances < 1 then
                    invalidArg (nameof maxInstances) "maxInstances must be positive"

                let! instances = discover ()
                let results = ResizeArray<DurableAppWorkerInstanceResult>()

                for instanceId in instances |> List.truncate maxInstances do
                    let! ticks = runtime.Host.RunUntilIdle instanceId

                    results.Add(
                        { InstanceId = instanceId
                          Ticks = ticks
                          Active = DurableAppWorkerSupport.ticksActive ticks }
                    )

                let instances = List.ofSeq results

                return
                    { Instances = instances
                      ActiveInstances = instances |> List.filter (fun result -> result.Active) |> List.length }
            }

        let runReady () = runReadyWith 100

        let runForever (cancellationToken: System.Threading.CancellationToken) =
            async {
                while not cancellationToken.IsCancellationRequested do
                    let! pass = runReady ()

                    if pass.ActiveInstances = 0 then
                        do! Async.Sleep 250
            }

        { runOnce = runtime.Host.RunOnce
          runUntilIdle = runtime.Host.RunUntilIdle
          runUntilIdleWith = runtime.Host.RunUntilIdleWith
          discover = discover
          runReady = runReady
          runReadyWith = runReadyWith
          runForever = runForever }

    let worker (config: DurableAppEnvironmentWorkerConfig) app =
        let storage = DurableAppEnvironment.storage config.Environment config.BasinName

        workerWith
            { Storage = storage
              HostId = config.HostId
              MaxRunUntilIdleTicks = config.MaxRunUntilIdleTicks }
            app

type DurableAppBuilder() =
    member _.Yield(()) = DurableApp.empty

    member _.Zero() = DurableApp.empty

    member _.Delay(generator: unit -> DurableApp) = generator ()

    member _.Run(app) = app

    [<CustomOperation("activity")>]
    member _.Activity(app, activity) = DurableApp.addActivity activity app

    [<CustomOperation("workflow")>]
    member _.Workflow(app, workflow) = DurableApp.addWorkflow workflow app

[<AutoOpen>]
module DurableAppSyntax =
    let durableApp = DurableAppBuilder()
