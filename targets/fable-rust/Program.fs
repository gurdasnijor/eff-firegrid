module FiregridRust.Program

open Eff.Foundation.Durable

let summarizeNeed need =
    match need with
    | NeedsActivities pending ->
        let hasCharge =
            pending
            |> List.exists (fun (_, activity) -> activity.Name = "charge" && activity.Input = "reserved:order-1")

        let hasNotify =
            pending
            |> List.exists (fun (_, activity) -> activity.Name = "notify" && activity.Input = "order-1")

        if pending.Length = 2 && hasCharge && hasNotify then
            "blocked:grouped-activities"
        else
            "blocked:other"
    | NeedsActivity activity when activity.Name = "charge" && activity.Input = "reserved:order-1" ->
        "blocked:dependent-activity"
    | NeedsActivity activity when activity.Name = "reserve" && activity.Input = "order-1" -> "blocked:activity"
    | _ -> "blocked:other"

let summarizeOutcome (outcome: Outcome<Value>) =
    match outcome with
    | Done _ -> "done"
    | Blocked(_, need) -> summarizeNeed need

let summarizeWorkflowReplay replay =
    match replay with
    | InvalidWorkflow _ -> "invalid-workflow"
    | ValidWorkflow outcome -> summarizeOutcome outcome

let summarizeAppReplay replay =
    match replay with
    | InvalidDurableIrApp _ -> "invalid-app"
    | DurableIrWorkflowNotFound _ -> "missing-workflow"
    | DurableIrWorkflowReplay workflowReplay -> summarizeWorkflowReplay workflowReplay

let replaySummary () =
    let draft, reservation =
        DurableIr.empty
        |> DurableIr.appendCallActivity "reserve" (ValueExpr.literal "order-1")

    let draft, _ =
        draft
        |> DurableIr.appendCallActivities [ "charge", reservation; "notify", ValueExpr.literal "order-1" ]

    let workflow =
        draft |> DurableIr.finish reservation |> DurableWorkflow.create "checkout"

    let app = DurableIrApp.empty |> DurableIrApp.bindWorkflow workflow

    let history =
        History.empty |> History.append (ActivityCompleted(OpId 0, "reserved:order-1"))

    DurableIrApp.replayWorkflow "checkout" history app |> summarizeAppReplay

[<EntryPoint>]
let main _ =
    printfn "%s" (replaySummary ())
    0
