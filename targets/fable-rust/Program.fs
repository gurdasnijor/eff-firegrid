module FiregridRust.Program

open Eff.Foundation.Durable

let isActivityCommand name input record =
    match record with
    | DurableIrCommitCommand(DurableIrCommandCallActivity(_, activity)) ->
        activity.Name = name && activity.Input = input
    | _ -> false

let summarizePlan plan =
    match plan with
    | InvalidDurableIrAppPlan _ -> "invalid-app"
    | DurableIrPlanWorkflowNotFound _ -> "missing-workflow"
    | DurableIrPlanReady ready ->
        match ready with
        | DurableIrPlanComplete _ -> "complete"
        | DurableIrPlanWaiting _ -> "waiting"
        | DurableIrPlanCommit records ->
            let hasCharge =
                records |> List.exists (isActivityCommand "charge" "reserved:order-1")

            let hasNotify = records |> List.exists (isActivityCommand "notify" "order-1")

            if records.Length = 4 && hasCharge && hasNotify then
                "commit:grouped-activities"
            else
                "commit:other"

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

    DurableIrApp.planWorkflow 123L "checkout" history app |> summarizePlan

[<EntryPoint>]
let main _ =
    printfn "%s" (replaySummary ())
    0
