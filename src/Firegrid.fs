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

type Durable<'output> =
    internal
    | Program of Eff.Foundation.Durable.Durable<'output>
    | StepCall of Activity: Eff.Foundation.Durable.Activity * Decode: (Payload -> 'output)

type FiregridApp =
    private
        { App: DurableApp
          Steps: string list
          Workflows: string list }

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

    let all2 left right =
        durable {
            let! leftValue = left
            let! rightValue = right
            return leftValue, rightValue
        }

    let waitFor name =
        Eff.Foundation.Durable.Workflow.waitForSignal name |> Program

    let waitForWith name decode =
        waitFor name |> DurableProgram.map decode
