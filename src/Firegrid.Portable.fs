namespace Eff.Firegrid

open Eff.Foundation.Durable

type WorkflowFlow<'value> =
    private
        { Build: DurableIrDraft -> DurableIrDraft * 'value }

[<RequireQualifiedAccess>]
module WorkflowFlow =
    let run flow draft = flow.Build draft

    let value text = ValueExpr.literal text

    let activity name input =
        { Build = fun draft -> DurableIr.appendCallActivity name input draft }

    let activities calls =
        { Build = fun draft -> DurableIr.appendCallActivities calls draft }

    let waitForSignal name =
        { Build = fun draft -> DurableIr.appendAwaitEvent (Signal name) draft }

    let currentTime = { Build = fun draft -> DurableIr.appendCurrentTime draft }

    let log message =
        { Build =
            fun draft ->
                let draft = DurableIr.appendLog message draft
                draft, () }

    let workflow name flow =
        let draft, result = run flow DurableIr.empty

        draft |> DurableIr.finish result |> DurableWorkflow.create name

type WorkflowFlowBuilder() =
    member _.Return value = { Build = fun draft -> draft, value }

    member _.ReturnFrom flow = flow

    member _.Bind(flow, binder) =
        { Build =
            fun draft ->
                let draft, value = WorkflowFlow.run flow draft
                WorkflowFlow.run (binder value) draft }

    member this.Zero() = this.Return()

    member _.Combine(first: WorkflowFlow<unit>, second) =
        { Build =
            fun draft ->
                let draft, _ = WorkflowFlow.run first draft
                WorkflowFlow.run second draft }

    member _.Delay(generator: unit -> WorkflowFlow<'value>) =
        { Build = fun draft -> WorkflowFlow.run (generator ()) draft }

    member _.Run(flow) = flow

type NamedWorkflowBuilder(name: string) =
    member _.Yield(()) =
        { Build = fun draft -> draft, ValueExpr.literal "" }

    member _.Run(flow) = WorkflowFlow.workflow name flow

    member _.Delay(generator: unit -> WorkflowFlow<'value>) = generator ()

    member _.Return value = { Build = fun draft -> draft, value }

    member _.ReturnFrom flow = flow

    member _.Bind(flow, binder) =
        { Build =
            fun draft ->
                let draft, value = WorkflowFlow.run flow draft
                WorkflowFlow.run (binder value) draft }

    member this.Zero() = this.Return(ValueExpr.literal "")

    member _.Combine(first: WorkflowFlow<unit>, second) =
        { Build =
            fun draft ->
                let draft, _ = WorkflowFlow.run first draft
                WorkflowFlow.run second draft }

[<AutoOpen>]
module PortableFiregridSyntax =
    let workflow name = NamedWorkflowBuilder name

    let flow = WorkflowFlowBuilder()

    let firegrid workflows = DurableIrApp.create workflows

    let value text = WorkflowFlow.value text

    let activity name input = WorkflowFlow.activity name input

    let activities calls = WorkflowFlow.activities calls

    let waitForSignal name = WorkflowFlow.waitForSignal name

    let currentTime = WorkflowFlow.currentTime

    let log message = WorkflowFlow.log message
