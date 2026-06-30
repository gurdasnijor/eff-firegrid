namespace Eff.Foundation.Durable.App

open Eff
open Eff.Foundation.Durable

type Activity<'input, 'output> =
    internal
        { Name: ActivityName
          EncodeInput: 'input -> Payload
          DecodeInput: Payload -> 'input
          EncodeOutput: 'output -> Payload
          DecodeOutput: Payload -> 'output
          Handler: 'input -> Async<'output> }

type Workflow<'input, 'output> =
    internal
        { Name: WorkflowName
          EncodeInput: 'input -> Payload
          DecodeInput: Payload -> 'input
          EncodeOutput: 'output -> Payload
          DecodeOutput: Payload -> 'output
          Factory: 'input -> Durable<'output> }

type Signal<'payload> =
    internal
        { Name: string
          Encode: 'payload -> Payload
          Decode: Payload -> 'payload }

type DurableStorage = private DurableStorage of S2.Basin

type DurableAppClientConfig = { Storage: DurableStorage }

type DurableAppWorkerConfig =
    { Storage: DurableStorage
      HostId: string
      MaxRunUntilIdleTicks: int option }

type DurableAppClient =
    { start: Workflow<string, string> -> string -> Async<DurableClientStartStatus>
      startWith: InstanceId -> Workflow<string, string> -> string -> Async<DurableClientStartStatus>
      signal: InstanceId -> Signal<string> -> string -> Async<DurableClientSignalStatus>
      status: InstanceId -> Async<DurableClientStatusRead> }

type DurableAppWorker =
    { runOnce: InstanceId -> Async<DurableWorkflowHostStatus>
      runUntilIdle: InstanceId -> Async<DurableWorkflowHostStatus list>
      runUntilIdleWith: int -> InstanceId -> Async<DurableWorkflowHostStatus list> }

type private ActivityRegistration =
    { Name: string
      Register: ActivityRegistry -> Result<ActivityRegistry, DurableRegistryError> }

type private WorkflowRegistration =
    { Name: string
      Register: WorkflowRegistry -> Result<WorkflowRegistry, DurableRegistryError> }

type DurableApp =
    private
        { Activities: ActivityRegistration list
          Workflows: WorkflowRegistration list }

[<RequireQualifiedAccess>]
module Activity =
    let define name handler : Activity<string, string> =
        { Name = ActivityName.create name
          EncodeInput = id
          DecodeInput = id
          EncodeOutput = id
          DecodeOutput = id
          Handler = handler }

[<RequireQualifiedAccess>]
module Workflow =
    let define name factory : Workflow<string, string> =
        { Name = WorkflowName.create name
          EncodeInput = id
          DecodeInput = id
          EncodeOutput = id
          DecodeOutput = id
          Factory = factory }

[<RequireQualifiedAccess>]
module Signal =
    let define name : Signal<string> =
        { Name = name
          Encode = id
          Decode = id }

[<RequireQualifiedAccess>]
module DurableTask =
    let signal (signal: Signal<string>) =
        Eff.Foundation.Durable.DurableTask.signal signal.Name

    let timer = Eff.Foundation.Durable.DurableTask.timer

[<RequireQualifiedAccess>]
module Durable =
    let call (activity: Activity<string, string>) input =
        Eff.Foundation.Durable.Workflow.call (ActivityName.value activity.Name) (activity.EncodeInput input)
        |> Eff.Foundation.Durable.Durable.map activity.DecodeOutput

    let waitForSignal (signal: Signal<string>) =
        Eff.Foundation.Durable.Workflow.waitForSignal signal.Name
        |> Eff.Foundation.Durable.Durable.map signal.Decode

    let sleepUntil = Eff.Foundation.Durable.Workflow.sleepUntil

    let any tasks =
        Eff.Foundation.Durable.Workflow.any tasks

    let currentTime = Eff.Foundation.Durable.Workflow.currentTime

    let log = Eff.Foundation.Durable.Workflow.log

[<RequireQualifiedAccess>]
module DurableStorage =
    let s2 basin = DurableStorage basin

[<RequireQualifiedAccess>]
module DurableApp =
    let empty = { Activities = []; Workflows = [] }

    let private failRegistry error =
        failwith ("durable app registry assembly failed: " + string error)

    let private activityRegistration (activity: Activity<string, string>) : ActivityRegistration =
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

    let private workflowRegistration (workflow: Workflow<string, string>) : WorkflowRegistration =
        { Name = WorkflowName.value workflow.Name
          Register =
            fun registry ->
                WorkflowRegistry.register
                    (WorkflowName.value workflow.Name)
                    (fun input ->
                        workflow.Factory(workflow.DecodeInput input)
                        |> Eff.Foundation.Durable.Durable.map workflow.EncodeOutput)
                    registry }

    let addActivity (activity: Activity<string, string>) (app: DurableApp) =
        { app with
            Activities = app.Activities @ [ activityRegistration activity ] }

    let addWorkflow (workflow: Workflow<string, string>) (app: DurableApp) =
        { app with
            Workflows = app.Workflows @ [ workflowRegistration workflow ] }

    let private activities app =
        ((Ok ActivityRegistry.empty), app.Activities)
        ||> List.fold (fun state registration -> state |> Result.bind registration.Register)
        |> function
            | Ok registry -> registry
            | Error error -> failRegistry error

    let private workflows app =
        ((Ok WorkflowRegistry.empty), app.Workflows)
        ||> List.fold (fun state registration -> state |> Result.bind registration.Register)
        |> function
            | Ok registry -> registry
            | Error error -> failRegistry error

    let private runtimeFrom options storage app =
        let (DurableStorage basin) = storage
        DurableRuntime.create options basin (workflows app) (activities app)

    let clientWith (config: DurableAppClientConfig) app =
        let runtime =
            runtimeFrom (DurableRuntimeOptions.create "durable-app-client") config.Storage app

        { start = fun workflow input -> runtime.Client.Start workflow.Name (workflow.EncodeInput input)
          startWith =
            fun instanceId workflow input ->
                runtime.Client.StartWith instanceId workflow.Name (workflow.EncodeInput input)
          signal =
            fun instanceId signal payload -> runtime.Client.RaiseSignal instanceId signal.Name (signal.Encode payload)
          status = runtime.Client.GetStatus }

    let workerWith (config: DurableAppWorkerConfig) app =
        if config.HostId.Length > 15 then
            invalidArg (nameof config.HostId) "HostId must be 15 characters or fewer."

        let options =
            { DurableRuntimeOptions.create config.HostId with
                MaxRunUntilIdleTicks = config.MaxRunUntilIdleTicks |> Option.defaultValue 100 }

        let runtime = runtimeFrom options config.Storage app

        { runOnce = runtime.Host.RunOnce
          runUntilIdle = runtime.Host.RunUntilIdle
          runUntilIdleWith = runtime.Host.RunUntilIdleWith }

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
