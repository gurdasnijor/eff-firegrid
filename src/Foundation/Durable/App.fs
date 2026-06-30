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

[<RequireQualifiedAccess>]
type DurableAppStartFailure = AppendFailed of string

[<RequireQualifiedAccess>]
type DurableAppSignalFailure = AppendFailed of string

[<RequireQualifiedAccess>]
type DurableAppStatusFailure =
    | ReadFailed of string
    | DecodeFailed of seqNum: int64 * error: string
    | WorkflowNotFound of workflow: string

[<RequireQualifiedAccess>]
type DurableAppStartResult =
    | Started of InstanceId
    | Rejected of DurableAppStartFailure

[<RequireQualifiedAccess>]
type DurableAppSignalResult =
    | Accepted
    | Rejected of DurableAppSignalFailure

[<RequireQualifiedAccess>]
type DurableAppNeed =
    | Activity of name: string
    | Activities of names: string list
    | Timer of deadline: int64
    | Signal of name: string
    | Race of contenders: string list
    | TimerCancellation of count: int
    | CurrentTime
    | Log of message: string

[<RequireQualifiedAccess>]
type DurableAppWorkflowStatus =
    | NotFound
    | Running of workflow: string
    | Waiting of workflow: string * need: DurableAppNeed
    | Completed of workflow: string * output: string
    | Failed of DurableAppStatusFailure

type DurableAppClient internal (runtime: DurableRuntime) =
    static member private StartResult =
        function
        | DurableClientStartStatus.Accepted ack -> DurableAppStartResult.Started ack.InstanceId
        | DurableClientStartStatus.Failed failure ->
            match failure with
            | DurableClientFailure.StartAppendFailed error
            | DurableClientFailure.SignalAppendFailed error ->
                DurableAppStartResult.Rejected(DurableAppStartFailure.AppendFailed error)

    static member private SignalResult =
        function
        | DurableClientSignalStatus.Accepted _ -> DurableAppSignalResult.Accepted
        | DurableClientSignalStatus.Failed failure ->
            match failure with
            | DurableClientFailure.SignalAppendFailed error
            | DurableClientFailure.StartAppendFailed error ->
                DurableAppSignalResult.Rejected(DurableAppSignalFailure.AppendFailed error)

    static member private TaskText =
        function
        | RaceActivity activity -> "activity:" + activity.Name
        | RaceEvent(Timer deadline) -> "timer:" + string deadline
        | RaceEvent(Signal name) -> "signal:" + name

    static member private NeedSummary =
        function
        | NeedsActivity activity -> DurableAppNeed.Activity activity.Name
        | NeedsActivities activities ->
            activities
            |> List.map (fun (_, activity) -> activity.Name)
            |> DurableAppNeed.Activities
        | NeedsEvent(Timer deadline) -> DurableAppNeed.Timer deadline
        | NeedsEvent(Signal name) -> DurableAppNeed.Signal name
        | NeedsRace tasks ->
            tasks
            |> List.map (fun (_, task) -> DurableAppClient.TaskText task)
            |> DurableAppNeed.Race
        | NeedsTimerCancellation timers -> DurableAppNeed.TimerCancellation timers.Length
        | NeedsCurrentTime -> DurableAppNeed.CurrentTime
        | NeedsLog message -> DurableAppNeed.Log message

    static member private StatusFailure =
        function
        | DurableClientStatusFailure.StatusReadFailed error -> DurableAppStatusFailure.ReadFailed error
        | DurableClientStatusFailure.StatusDecodeFailed(seqNum, error) ->
            DurableAppStatusFailure.DecodeFailed(seqNum, error)
        | DurableClientStatusFailure.WorkflowNotFound workflow ->
            DurableAppStatusFailure.WorkflowNotFound(WorkflowName.value workflow)

    static member private WorkflowStatus =
        function
        | DurableClientStatusRead.Succeeded InstanceNotFound -> DurableAppWorkflowStatus.NotFound
        | DurableClientStatusRead.Succeeded(InstanceRunning workflow) ->
            DurableAppWorkflowStatus.Running(WorkflowName.value workflow)
        | DurableClientStatusRead.Succeeded(InstanceWaiting(workflow, _, need)) ->
            DurableAppWorkflowStatus.Waiting(WorkflowName.value workflow, DurableAppClient.NeedSummary need)
        | DurableClientStatusRead.Succeeded(InstanceCompleted(workflow, payload)) ->
            DurableAppWorkflowStatus.Completed(WorkflowName.value workflow, payload)
        | DurableClientStatusRead.Failed failure ->
            DurableAppWorkflowStatus.Failed(DurableAppClient.StatusFailure failure)

    member _.start (workflow: Workflow<'input, 'output>) (input: 'input) =
        async {
            let! result = runtime.Client.Start workflow.Name (workflow.EncodeInput input)
            return DurableAppClient.StartResult result
        }

    member _.startWith (instanceId: InstanceId) (workflow: Workflow<'input, 'output>) (input: 'input) =
        async {
            let! result = runtime.Client.StartWith instanceId workflow.Name (workflow.EncodeInput input)
            return DurableAppClient.StartResult result
        }

    member _.signal (instanceId: InstanceId) (signal: Signal<'payload>) (payload: 'payload) =
        async {
            let! result = runtime.Client.RaiseSignal instanceId signal.Name (signal.Encode payload)
            return DurableAppClient.SignalResult result
        }

    member _.status instanceId =
        async {
            let! result = runtime.Client.GetStatus instanceId
            return DurableAppClient.WorkflowStatus result
        }

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
    let defineWith name encodeInput decodeInput encodeOutput decodeOutput handler : Activity<'input, 'output> =
        { Name = ActivityName.create name
          EncodeInput = encodeInput
          DecodeInput = decodeInput
          EncodeOutput = encodeOutput
          DecodeOutput = decodeOutput
          Handler = handler }

    let define name handler : Activity<string, string> = defineWith name id id id id handler

[<RequireQualifiedAccess>]
module Workflow =
    let defineWith name encodeInput decodeInput encodeOutput decodeOutput factory : Workflow<'input, 'output> =
        { Name = WorkflowName.create name
          EncodeInput = encodeInput
          DecodeInput = decodeInput
          EncodeOutput = encodeOutput
          DecodeOutput = decodeOutput
          Factory = factory }

    let define name factory : Workflow<string, string> = defineWith name id id id id factory

[<RequireQualifiedAccess>]
module Signal =
    let defineWith name encode decode : Signal<'payload> =
        { Name = name
          Encode = encode
          Decode = decode }

    let define name : Signal<string> = defineWith name id id

[<RequireQualifiedAccess>]
module DurableTask =
    let signal (signal: Signal<'payload>) =
        Eff.Foundation.Durable.DurableTask.signal signal.Name

    let timer = Eff.Foundation.Durable.DurableTask.timer

[<RequireQualifiedAccess>]
module Durable =
    let call (activity: Activity<'input, 'output>) (input: 'input) =
        Eff.Foundation.Durable.Workflow.call (ActivityName.value activity.Name) (activity.EncodeInput input)
        |> Eff.Foundation.Durable.Durable.map activity.DecodeOutput

    let waitForSignal (signal: Signal<'payload>) =
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

        DurableAppClient runtime

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
