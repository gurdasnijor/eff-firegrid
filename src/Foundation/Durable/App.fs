namespace Eff.Foundation.Durable.App

open Eff
open Eff.Foundation.Durable
open Fable.Core

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

type DurableRace<'result> =
    internal
        { Task: RaceTask
          Project: RaceResult -> 'result option }

type DurableStorage = private DurableStorage of S2.Basin

type DurableAppClientConfig = { Storage: DurableStorage }

type DurableAppWorkerConfig =
    { Storage: DurableStorage
      HostId: string
      MaxRunUntilIdleTicks: int option }

type DurableAppEnvironmentClientConfig =
    { Environment: string
      BasinName: string option }

type DurableAppEnvironmentWorkerConfig =
    { Environment: string
      BasinName: string option
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
    | WorkflowMismatch of expected: string * actual: string
    | OutputDecodeFailed of workflow: string * error: string

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

[<RequireQualifiedAccess>]
type DurableAppTypedWorkflowStatus<'output> =
    | NotFound
    | Running
    | Waiting of DurableAppNeed
    | Completed of 'output
    | Failed of DurableAppStatusFailure

type DurableAppWorkerInstanceResult =
    { InstanceId: InstanceId
      Ticks: DurableWorkflowHostStatus list
      Active: bool }

type DurableAppWorkerPass =
    { Instances: DurableAppWorkerInstanceResult list
      ActiveInstances: int }

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

    static member private DecodeWorkflowOutput (workflow: Workflow<'input, 'output>) payload =
        try
            Ok(workflow.DecodeOutput payload)
        with error ->
            Error(DurableAppStatusFailure.OutputDecodeFailed(WorkflowName.value workflow.Name, error.Message))

    static member private TypedWorkflowStatus (workflow: Workflow<'input, 'output>) status =
        let expected = WorkflowName.value workflow.Name

        let mismatch actual =
            DurableAppStatusFailure.WorkflowMismatch(expected, actual)

        match status with
        | DurableAppWorkflowStatus.NotFound -> DurableAppTypedWorkflowStatus.NotFound
        | DurableAppWorkflowStatus.Running actual ->
            if actual = expected then
                DurableAppTypedWorkflowStatus.Running
            else
                DurableAppTypedWorkflowStatus.Failed(mismatch actual)
        | DurableAppWorkflowStatus.Waiting(actual, need) ->
            if actual = expected then
                DurableAppTypedWorkflowStatus.Waiting need
            else
                DurableAppTypedWorkflowStatus.Failed(mismatch actual)
        | DurableAppWorkflowStatus.Completed(actual, payload) ->
            if actual = expected then
                match DurableAppClient.DecodeWorkflowOutput workflow payload with
                | Ok output -> DurableAppTypedWorkflowStatus.Completed output
                | Error failure -> DurableAppTypedWorkflowStatus.Failed failure
            else
                DurableAppTypedWorkflowStatus.Failed(mismatch actual)
        | DurableAppWorkflowStatus.Failed failure -> DurableAppTypedWorkflowStatus.Failed failure

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

    member this.statusOf (workflow: Workflow<'input, 'output>) instanceId =
        async {
            let! status = this.status instanceId
            return DurableAppClient.TypedWorkflowStatus workflow status
        }

type DurableAppWorker =
    { runOnce: InstanceId -> Async<DurableWorkflowHostStatus>
      runUntilIdle: InstanceId -> Async<DurableWorkflowHostStatus list>
      runUntilIdleWith: int -> InstanceId -> Async<DurableWorkflowHostStatus list>
      discover: unit -> Async<InstanceId list>
      runReady: unit -> Async<DurableAppWorkerPass>
      runReadyWith: int -> Async<DurableAppWorkerPass>
      runForever: System.Threading.CancellationToken -> Async<unit> }

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

    let raceSignal (signal: Signal<'payload>) project : DurableRace<'result> =
        { Task = DurableTask.signal signal
          Project =
            function
            | EventWon(_, Signal name, payload) when name = signal.Name -> Some(project (signal.Decode payload))
            | _ -> None }

    let raceTimer deadline result : DurableRace<'result> =
        { Task = DurableTask.timer deadline
          Project =
            function
            | EventWon(_, Timer actual, _) when actual = deadline -> Some result
            | _ -> None }

    let anyOf races =
        let races = races |> List.ofSeq

        races
        |> List.map (fun race -> race.Task)
        |> Eff.Foundation.Durable.Workflow.any
        |> Eff.Foundation.Durable.Durable.map (fun winner ->
            races
            |> List.tryPick (fun race -> race.Project winner)
            |> Option.defaultWith (fun () ->
                failwith ("durable race winner did not match a facade race task: " + string winner)))

    let currentTime = Eff.Foundation.Durable.Workflow.currentTime

    let log = Eff.Foundation.Durable.Workflow.log

[<RequireQualifiedAccess>]
module DurableStorage =
    let s2 basin = DurableStorage basin

[<RequireQualifiedAccess>]
module DurableAppEnvironment =
    [<Emit("process.env[$0] || ''")>]
    let private env (_name: string) : string = jsNative

    let private isBlank value = System.String.IsNullOrWhiteSpace value

    let private normalize (environment: string) =
        environment.Trim().ToUpperInvariant().Replace("-", "_")

    let private envOption name =
        let value = env name

        if isBlank value then None else Some value

    let private configured environment suffix =
        let environmentKey = "EFF_FIREGRID_" + normalize environment + "_" + suffix
        let fallbackKey = "EFF_FIREGRID_" + suffix

        match envOption environmentKey with
        | Some value -> Some value
        | None -> envOption fallbackKey

    let private configuredBasin environment basinName =
        match basinName with
        | Some name when not (isBlank name) -> name
        | _ ->
            match configured environment "BASIN" with
            | Some basin -> basin
            | None ->
                invalidArg
                    (nameof basinName)
                    ("BasinName is required, or set EFF_FIREGRID_"
                     + normalize environment
                     + "_BASIN / EFF_FIREGRID_BASIN.")

    let private connectClient environment =
        let accessToken = configured environment "ACCESS_TOKEN"
        let accountEndpoint = configured environment "S2_ACCOUNT_ENDPOINT"
        let basinEndpoint = configured environment "S2_BASIN_ENDPOINT"

        match accessToken, accountEndpoint, basinEndpoint with
        | None, None, None -> S2Cli.connect ()
        | _ ->
            S2.connectWith
                { S2.ConnectOptions.create (accessToken |> Option.defaultValue "durable-app-environment") with
                    AccountEndpoint = accountEndpoint
                    BasinEndpoint = basinEndpoint }

    let storage environment basinName =
        if isBlank environment then
            invalidArg (nameof environment) "Environment must be non-empty."

        let basinName = configuredBasin environment basinName
        let s2 = connectClient environment
        s2 |> S2.basin basinName |> DurableStorage.s2

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

    let private discoverInstances basin =
        async {
            let! streams = basin |> S2.listStreamsWith ""

            return
                streams
                |> List.choose (fun stream ->
                    if stream.DeletedAt.IsNone && stream.Name.EndsWith("/in") then
                        stream.Name.Substring(0, stream.Name.Length - 3) |> InstanceId.create |> Some
                    else
                        None)
                |> List.distinct
        }

    let private checkpointed =
        function
        | CommandDispatchCheckpointResult.Checkpointed _ -> true
        | CommandDispatchCheckpointResult.NotRequired
        | CommandDispatchCheckpointResult.Deposed _
        | CommandDispatchCheckpointResult.Failed _ -> false

    let private activityReportActive (report: ActivityCommandAdapterReport) =
        not (List.isEmpty report.Completed)
        || report.AlreadyPublished > 0
        || report.Ignored > 0
        || checkpointed report.Checkpoint

    let private timerReportActive (report: TimerCommandAdapterReport) =
        not (List.isEmpty report.Published)
        || report.AlreadyPublished > 0
        || report.Canceled > 0
        || report.Ignored > 0
        || checkpointed report.Checkpoint

    let private tickReportActive (report: DurableHostTickReport<Payload>) =
        match report.Inbox with
        | Some inbox when inbox.Commit.IsSome -> true
        | _ ->
            match report.Step with
            | Some(DurableHostStatus.Committed _) -> true
            | _ ->
                match report.Signals with
                | Some signal when signal.Delivered.IsSome || signal.AlreadyDelivered > 0 -> true
                | _ ->
                    match report.Activities with
                    | Some activity when activityReportActive activity -> true
                    | _ ->
                        match report.Timers with
                        | Some timer when timerReportActive timer -> true
                        | _ -> false

    let private tickActive =
        function
        | DurableWorkflowHostStatus.Ticked(DurableHostTickStatus.Advanced report) -> tickReportActive report
        | DurableWorkflowHostStatus.Ticked(DurableHostTickStatus.Completed(_, report))
        | DurableWorkflowHostStatus.Ticked(DurableHostTickStatus.Waiting(_, _, report))
        | DurableWorkflowHostStatus.Ticked(DurableHostTickStatus.Deposed(_, report))
        | DurableWorkflowHostStatus.Ticked(DurableHostTickStatus.Failed(_, report)) -> tickReportActive report
        | DurableWorkflowHostStatus.Deposed _
        | DurableWorkflowHostStatus.Failed _ -> false

    let private ticksActive ticks = ticks |> List.exists tickActive

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

        let discover () = discoverInstances basin

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
                          Active = ticksActive ticks }
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
