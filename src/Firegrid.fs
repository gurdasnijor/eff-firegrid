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

type FiregridApp =
    private
        { App: DurableApp
          Steps: string list
          Workflows: string list }

type Storage = private { Inner: DurableStorage }

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

type DurableBuilder() =
    member _.Return value = DurableProgram.result value

    member _.ReturnFrom operation = operation

    member _.Bind(operation, binder) = DurableProgram.bind binder operation

    member _.Delay(generator: unit -> Durable<'output>) = generator ()

    member _.Zero() = DurableProgram.result ()

    member _.Combine(first: Durable<unit>, second: Durable<'output>) =
        DurableProgram.bind (fun () -> second) first

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
          Workflows = [] }

    let addStep (step: Step<'input, 'output>) app =
        { app with
            App = DurableApp.addActivity step.Activity app.App
            Steps = app.Steps @ [ ActivityName.value step.Activity.Name ] }

    let addWorkflow (workflow: Workflow<'input, 'output>) app =
        { app with
            App = DurableApp.addWorkflow workflow.Workflow app.App
            Workflows = app.Workflows @ [ WorkflowName.value workflow.Workflow.Name ] }

    let toDurableApp app = app.App

    let stepNames app = app.Steps

    let workflowNames app = app.Workflows

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

    let all operations =
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

    let both left right =
        match left, right with
        | StepCall(leftActivity, leftDecode), StepCall(rightActivity, rightDecode) ->
            Eff.Foundation.Durable.Workflow.all [ leftActivity; rightActivity ]
            |> Eff.Foundation.Durable.Durable.map (function
                | [ leftPayload; rightPayload ] -> leftDecode leftPayload, rightDecode rightPayload
                | _ -> failwith "both expected exactly two results")
            |> Program
        | _ ->
            durable {
                let! leftValue = left
                let! rightValue = right
                return leftValue, rightValue
            }

    let waitFor name =
        Eff.Foundation.Durable.Workflow.waitForSignal name |> Program

    let waitForWith name decode =
        waitFor name |> DurableProgram.map decode

    let waitForSignal signal =
        Eff.Foundation.Durable.App.Durable.waitForSignal (Signal.toDurableSignal signal)
        |> Program
