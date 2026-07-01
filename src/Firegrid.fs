namespace Eff.Firegrid

open Eff
open Eff.Foundation.Durable
open Eff.Foundation.Durable.App

type Step<'input, 'output> =
    internal
        { Activity: Activity<'input, 'output> }

type Workflow<'input, 'output> =
    internal
        { Workflow: Eff.Foundation.Durable.App.Workflow<'input, 'output> }

type Signal<'payload> =
    internal
        { Signal: Eff.Foundation.Durable.App.Signal<'payload> }

type Durable<'output> =
    internal
    | Program of Eff.Foundation.Durable.Durable<'output>
    | StepCall of Activity: Eff.Foundation.Durable.Activity * Decode: (Payload -> 'output)

type internal StepRegistration =
    { Name: string
      Run: Payload -> Async<Payload> }

type FiregridApp =
    private
        { App: DurableApp
          Steps: string list
          Workflows: string list
          StepRegistrations: StepRegistration list }

type Storage = private { Inner: DurableStorage }

type LocalStepExecution =
    { Name: string
      Input: string
      Output: string }

[<RequireQualifiedAccess>]
type StartResult =
    | Started of InstanceId
    | Rejected of string

[<RequireQualifiedAccess>]
type WorkflowStatus<'output> =
    | NotFound
    | Running
    | Waiting of string
    | Completed of 'output
    | Failed of string

type LocalWorkflowRun<'output> =
    { Status: WorkflowStatus<'output>
      Steps: LocalStepExecution list }

module private DurableProgram =
    let rec toProgram =
        function
        | Program program -> program
        | StepCall(activity, decode) ->
            Eff.Foundation.Durable.Workflow.call activity.Name activity.Input
            |> Eff.Foundation.Durable.Durable.map decode

    let result value =
        Eff.Foundation.Durable.Durable.result value |> Program

    let bind binder operation =
        operation
        |> toProgram
        |> Eff.Foundation.Durable.Durable.bind (binder >> toProgram)
        |> Program

    let map mapper operation = bind (mapper >> result) operation

module private Status =
    let startResult =
        function
        | DurableAppStartResult.Started instanceId -> StartResult.Started instanceId
        | DurableAppStartResult.Rejected failure -> StartResult.Rejected(string failure)

    let workflowStatus =
        function
        | DurableAppTypedWorkflowStatus.NotFound -> WorkflowStatus.NotFound
        | DurableAppTypedWorkflowStatus.Running -> WorkflowStatus.Running
        | DurableAppTypedWorkflowStatus.Waiting need -> WorkflowStatus.Waiting(string need)
        | DurableAppTypedWorkflowStatus.Completed output -> WorkflowStatus.Completed output
        | DurableAppTypedWorkflowStatus.Failed failure -> WorkflowStatus.Failed(string failure)

module private Local =
    let currentTimeMillis () =
        System.DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()

    let private activityDescription (activity: Eff.Foundation.Durable.Activity) = "activity:" + activity.Name

    let private eventDescription =
        function
        | EventKey.Timer deadline -> "timer:" + string deadline
        | EventKey.Signal name -> "signal:" + name

    let private raceTaskDescription (task: RaceTask) =
        match task with
        | RaceActivity activity -> activityDescription activity
        | RaceEvent key -> eventDescription key

    let waitingOn (need: Need) =
        match need with
        | NeedsActivity activity -> activityDescription activity
        | NeedsActivities activities ->
            activities
            |> List.map (fun (_, activity: Eff.Foundation.Durable.Activity) -> activityDescription activity)
            |> String.concat ","
        | NeedsEvent key -> eventDescription key
        | NeedsRace tasks ->
            tasks
            |> List.map (fun (_, task) -> raceTaskDescription task)
            |> String.concat ","
        | NeedsTimerCancellation timers -> "timer-cancellation:" + string timers.Length
        | NeedsCurrentTime -> "current-time"
        | NeedsLog message -> "log:" + message

type DurableBuilder() =
    member _.Return value = DurableProgram.result value

    member _.ReturnFrom operation = operation

    member _.Bind(operation, binder) = DurableProgram.bind binder operation

    member _.Delay(generator: unit -> Durable<'output>) = generator ()

    member _.Zero() = DurableProgram.result ()

    member _.Combine(first: Durable<unit>, second: Durable<'output>) =
        DurableProgram.bind (fun () -> second) first

    member _.MergeSources(left: Durable<'left>, right: Durable<'right>) =
        match left, right with
        | StepCall(leftActivity, leftDecode), StepCall(rightActivity, rightDecode) ->
            Eff.Foundation.Durable.Workflow.all [ leftActivity; rightActivity ]
            |> Eff.Foundation.Durable.Durable.map (function
                | [ leftPayload; rightPayload ] -> leftDecode leftPayload, rightDecode rightPayload
                | _ -> failwith "durable parallel bind expected exactly two results")
            |> Program
        | _ ->
            DurableProgram.bind
                (fun leftValue -> DurableProgram.map (fun rightValue -> leftValue, rightValue) right)
                left

[<RequireQualifiedAccess>]
module Step =
    let defineWith name encodeInput decodeInput encodeOutput decodeOutput handler : Step<'input, 'output> =
        { Activity = Activity.defineWith name encodeInput decodeInput encodeOutput decodeOutput handler }

    let define name handler : Step<string, string> = defineWith name id id id id handler

    let toActivity step = step.Activity

[<RequireQualifiedAccess>]
module Workflow =
    let defineWith name encodeInput decodeInput encodeOutput decodeOutput factory : Workflow<'input, 'output> =
        { Workflow =
            Eff.Foundation.Durable.App.Workflow.defineWith
                name
                encodeInput
                decodeInput
                encodeOutput
                decodeOutput
                (fun input -> factory input |> DurableProgram.toProgram) }

    let define name factory : Workflow<string, string> = defineWith name id id id id factory

    let toDurableWorkflow workflow = workflow.Workflow

[<RequireQualifiedAccess>]
module Signal =
    let defineWith name encode decode : Signal<'payload> =
        { Signal = Eff.Foundation.Durable.App.Signal.defineWith name encode decode }

    let define name : Signal<string> = defineWith name id id

    let toDurableSignal signal = signal.Signal

[<RequireQualifiedAccess>]
module FiregridApp =
    let empty =
        { App = DurableApp.empty
          Steps = []
          Workflows = []
          StepRegistrations = [] }

    let addStep (step: Step<'input, 'output>) app =
        let registration =
            { Name = ActivityName.value step.Activity.Name
              Run =
                fun payload ->
                    async {
                        let input = step.Activity.DecodeInput payload
                        let! output = step.Activity.Handler input
                        return step.Activity.EncodeOutput output
                    } }

        { app with
            App = DurableApp.addActivity step.Activity app.App
            Steps = app.Steps @ [ registration.Name ]
            StepRegistrations = app.StepRegistrations @ [ registration ] }

    let addWorkflow (workflow: Workflow<'input, 'output>) app =
        { app with
            App = DurableApp.addWorkflow workflow.Workflow app.App
            Workflows = app.Workflows @ [ WorkflowName.value workflow.Workflow.Name ] }

    let toDurableApp app = app.App

    let stepNames (app: FiregridApp) = app.Steps

    let workflowNames (app: FiregridApp) = app.Workflows

[<RequireQualifiedAccess>]
module Storage =
    let s2 basin = { Inner = DurableStorage.s2 basin }

    let environment environment basinName =
        { Inner = DurableAppEnvironment.storage environment basinName }

type Client internal (inner: DurableAppClient) =
    member _.start (workflow: Workflow<'input, 'output>) (input: 'input) =
        async {
            let! result = inner.start (Workflow.toDurableWorkflow workflow) input
            return Status.startResult result
        }

    member _.startWith instanceId (workflow: Workflow<'input, 'output>) (input: 'input) =
        async {
            let! result = inner.startWith instanceId (Workflow.toDurableWorkflow workflow) input
            return Status.startResult result
        }

    member _.status (workflow: Workflow<'input, 'output>) instanceId =
        async {
            let! status = inner.statusOf (Workflow.toDurableWorkflow workflow) instanceId
            return Status.workflowStatus status
        }

    member this.result workflow instanceId =
        async {
            let! status = this.status workflow instanceId

            return
                match status with
                | WorkflowStatus.Completed output -> Some output
                | _ -> None
        }

    member _.signal instanceId (signal: Signal<'payload>) (payload: 'payload) =
        async {
            let! result = inner.signal instanceId (Signal.toDurableSignal signal) payload

            return
                match result with
                | DurableAppSignalResult.Accepted -> Ok()
                | DurableAppSignalResult.Rejected failure -> Error(string failure)
        }

type Worker internal (inner: DurableAppWorker) =
    member _.runUntilIdle instanceId =
        async {
            let! _ = inner.runUntilIdle instanceId
            return ()
        }

    member _.runReady() =
        async {
            let! pass = inner.runReady ()
            return pass.ActiveInstances
        }

type TestHost internal (client: Client, worker: Worker) =
    member _.client = client

    member _.worker = worker

    member _.startAndRunWith instanceId (workflow: Workflow<'input, 'output>) (input: 'input) =
        async {
            let! start = client.startWith instanceId workflow input

            match start with
            | StartResult.Rejected error -> return failwith ("workflow start rejected: " + error)
            | StartResult.Started instanceId ->
                do! worker.runUntilIdle instanceId
                let! status = client.status workflow instanceId
                return instanceId, status
        }

    member this.runWith instanceId (workflow: Workflow<'input, 'output>) (input: 'input) =
        async {
            let! _, status = this.startAndRunWith instanceId workflow input

            match status with
            | WorkflowStatus.Completed output -> return output
            | WorkflowStatus.NotFound -> return failwith "workflow did not complete: not found"
            | WorkflowStatus.Running -> return failwith "workflow did not complete: still running"
            | WorkflowStatus.Waiting need -> return failwith ("workflow did not complete: waiting for " + need)
            | WorkflowStatus.Failed error -> return failwith ("workflow failed: " + error)
        }

    member _.startAndRun (workflow: Workflow<'input, 'output>) (input: 'input) =
        async {
            let! start = client.start workflow input

            match start with
            | StartResult.Rejected error -> return failwith ("workflow start rejected: " + error)
            | StartResult.Started instanceId ->
                do! worker.runUntilIdle instanceId
                let! status = client.status workflow instanceId
                return instanceId, status
        }

    member this.run (workflow: Workflow<'input, 'output>) (input: 'input) =
        async {
            let! _, status = this.startAndRun workflow input

            match status with
            | WorkflowStatus.Completed output -> return output
            | WorkflowStatus.NotFound -> return failwith "workflow did not complete: not found"
            | WorkflowStatus.Running -> return failwith "workflow did not complete: still running"
            | WorkflowStatus.Waiting need -> return failwith ("workflow did not complete: waiting for " + need)
            | WorkflowStatus.Failed error -> return failwith ("workflow failed: " + error)
        }

type LocalTestHost internal (app: FiregridApp) =
    let steps =
        app.StepRegistrations
        |> List.map (fun registration -> registration.Name, registration.Run)
        |> Map.ofList

    let workflowIsRegistered (workflow: Workflow<'input, 'output>) =
        app.Workflows |> List.contains (WorkflowName.value workflow.Workflow.Name)

    let runActivity (activity: Eff.Foundation.Durable.Activity) =
        async {
            match Map.tryFind activity.Name steps with
            | None -> return Error("missing local step: " + activity.Name)
            | Some run ->
                try
                    let! output = run activity.Input
                    return Ok output
                with error ->
                    return Error("local step failed: " + activity.Name + ": " + error.Message)
        }

    let rec advance history trace program =
        async {
            match Eff.Foundation.Durable.Durable.replay history program with
            | Done output ->
                return
                    { Status = WorkflowStatus.Completed output
                      Steps = List.rev trace }
            | Blocked(opId, NeedsActivity activity) ->
                let! result = runActivity activity

                match result with
                | Ok output ->
                    let history = History.append (ActivityCompleted(opId, output)) history

                    let execution =
                        { Name = activity.Name
                          Input = activity.Input
                          Output = output }

                    return! advance history (execution :: trace) program
                | Error error ->
                    return
                        { Status = WorkflowStatus.Failed error
                          Steps = List.rev trace }
            | Blocked(_, NeedsActivities activities) ->
                let! results =
                    activities
                    |> List.map (fun (opId, activity) ->
                        async {
                            let! result = runActivity activity
                            return opId, result
                        })
                    |> Async.Parallel

                match
                    results
                    |> Array.tryPick (fun (_, result) ->
                        match result with
                        | Ok _ -> None
                        | Error error -> Some error)
                with
                | Some error ->
                    return
                        { Status = WorkflowStatus.Failed error
                          Steps = List.rev trace }
                | None ->
                    let history =
                        (history, results)
                        ||> Array.fold (fun history (opId, result) ->
                            match result with
                            | Ok output -> History.append (ActivityCompleted(opId, output)) history
                            | Error _ -> history)

                    let executions =
                        (activities, results |> Array.toList)
                        ||> List.zip
                        |> List.map (fun ((_, activity), (_, result)) ->
                            match result with
                            | Ok output ->
                                { Name = activity.Name
                                  Input = activity.Input
                                  Output = output }
                            | Error error ->
                                { Name = activity.Name
                                  Input = activity.Input
                                  Output = error })

                    return! advance history (List.rev executions @ trace) program
            | Blocked(opId, NeedsCurrentTime) ->
                let history =
                    History.append (CurrentTimeRecorded(opId, Local.currentTimeMillis ())) history

                return! advance history trace program
            | Blocked(opId, NeedsLog message) ->
                let history = History.append (LogEmitted(opId, message)) history
                return! advance history trace program
            | Blocked(_, NeedsTimerCancellation timers) ->
                let history =
                    (history, timers)
                    ||> List.fold (fun history opId -> History.append (TimerCanceled opId) history)

                return! advance history trace program
            | Blocked(_, need) ->
                return
                    { Status = WorkflowStatus.Waiting(Local.waitingOn need)
                      Steps = List.rev trace }
        }

    member _.inspect (workflow: Workflow<'input, 'output>) (input: 'input) =
        async {
            if not (workflowIsRegistered workflow) then
                return
                    { Status =
                        WorkflowStatus.Failed(
                            "workflow is not registered in this app: "
                            + WorkflowName.value workflow.Workflow.Name
                        )
                      Steps = [] }
            else
                try
                    let program =
                        workflow.Workflow.Factory input
                        |> Eff.Foundation.Durable.Durable.map workflow.Workflow.EncodeOutput

                    let! run = advance History.empty [] program

                    return
                        { Status =
                            match run.Status with
                            | WorkflowStatus.Completed payload ->
                                WorkflowStatus.Completed(workflow.Workflow.DecodeOutput payload)
                            | WorkflowStatus.NotFound -> WorkflowStatus.NotFound
                            | WorkflowStatus.Running -> WorkflowStatus.Running
                            | WorkflowStatus.Waiting need -> WorkflowStatus.Waiting need
                            | WorkflowStatus.Failed error -> WorkflowStatus.Failed error
                          Steps = run.Steps }
                with error ->
                    return
                        { Status = WorkflowStatus.Failed error.Message
                          Steps = [] }
        }

    member this.tryRun workflow input =
        async {
            let! run = this.inspect workflow input
            return run.Status
        }

    member this.run workflow input =
        async {
            let! status = this.tryRun workflow input

            match status with
            | WorkflowStatus.Completed output -> return output
            | WorkflowStatus.NotFound -> return failwith "local workflow did not complete: not found"
            | WorkflowStatus.Running -> return failwith "local workflow did not complete: still running"
            | WorkflowStatus.Waiting need -> return failwith ("local workflow did not complete: waiting for " + need)
            | WorkflowStatus.Failed error -> return failwith ("local workflow failed: " + error)
        }

[<RequireQualifiedAccess>]
module Firegrid =
    let clientWith storage app =
        app
        |> FiregridApp.toDurableApp
        |> DurableApp.clientWith ({ Storage = storage.Inner }: DurableAppClientConfig)
        |> Client

    let workerWith storage hostId app =
        app
        |> FiregridApp.toDurableApp
        |> DurableApp.workerWith (
            { Storage = storage.Inner
              HostId = hostId
              MaxRunUntilIdleTicks = Some 100 }
            : DurableAppWorkerConfig
        )
        |> Worker

    let testHostWith storage hostId app =
        TestHost(clientWith storage app, workerWith storage hostId app)

    let localTestHost app = LocalTestHost app

[<RequireQualifiedAccess>]
module Durable =
    let Parallel operations =
        let operations = operations |> List.ofSeq

        match operations with
        | [] -> DurableProgram.result []
        | _ ->
            let stepCalls =
                operations
                |> List.map (function
                    | StepCall(activity, decode) -> Some(activity, decode)
                    | Program _ -> None)

            if stepCalls |> List.forall Option.isSome then
                let calls = stepCalls |> List.choose id
                let activities = calls |> List.map fst
                let decoders = calls |> List.map snd

                Eff.Foundation.Durable.Workflow.all activities
                |> Eff.Foundation.Durable.Durable.map (fun payloads ->
                    List.map2 (fun decode payload -> decode payload) decoders payloads)
                |> Program
            else
                let cons head tail =
                    DurableProgram.map (fun values -> head :: values) tail

                (DurableProgram.result [], operations |> List.rev)
                ||> List.fold (fun tail operation -> DurableProgram.bind (fun value -> cons value tail) operation)

type FiregridBuilder() =
    member _.Yield(()) = FiregridApp.empty

    member _.Zero() = FiregridApp.empty

    member _.Delay(generator: unit -> FiregridApp) = generator ()

    member _.Run(app) = app

    [<CustomOperation("step")>]
    member _.Step(app, step) = FiregridApp.addStep step app

    [<CustomOperation("workflow")>]
    member _.Workflow(app, workflow) = FiregridApp.addWorkflow workflow app

[<AutoOpen>]
module Syntax =
    let durable = DurableBuilder()

    let firegrid = FiregridBuilder()

    let step name handler = Step.define name handler

    let stepWith name encodeInput decodeInput encodeOutput decodeOutput handler =
        Step.defineWith name encodeInput decodeInput encodeOutput decodeOutput handler

    let workflow name factory = Workflow.define name factory

    let workflowWith name encodeInput decodeInput encodeOutput decodeOutput factory =
        Workflow.defineWith name encodeInput decodeInput encodeOutput decodeOutput factory

    let signal name = Signal.define name

    let signalWith name encode decode = Signal.defineWith name encode decode

    let call (step: Step<'input, 'output>) input =
        let activity =
            Eff.Foundation.Durable.Activities.create
                (ActivityName.value step.Activity.Name)
                (step.Activity.EncodeInput input)

        StepCall(activity, step.Activity.DecodeOutput)

    let waitFor name =
        Eff.Foundation.Durable.Workflow.waitForSignal name |> Program

    let waitForWith name decode =
        waitFor name |> DurableProgram.map decode

    let waitForSignal signal =
        Eff.Foundation.Durable.App.Durable.waitForSignal (Signal.toDurableSignal signal)
        |> Program
