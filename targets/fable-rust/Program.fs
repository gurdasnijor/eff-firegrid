module FiregridRust.Program

open Eff.Foundation.Durable
open Eff.Firegrid.Portable

let activityCommand opId name input =
    DurableIrCommitCommand(DurableIrCommandCallActivity(opId, { Name = name; Input = input }))
    |> DurableIrCommitCodec.encode

let summarizePlan plan =
    match plan with
    | InvalidDurableIrAppPlan _ -> "invalid-app"
    | DurableIrPlanWorkflowNotFound _ -> "missing-workflow"
    | DurableIrPlanReady ready ->
        match ready with
        | DurableIrPlanComplete _ -> "complete"
        | DurableIrPlanWaiting _ -> "waiting"
        | DurableIrPlanCommit records ->
            let encoded = records |> List.map DurableIrCommitCodec.encode

            let chargeCommand = activityCommand (OpId 1) "charge" "reserved:order-1"

            let notifyCommand = activityCommand (OpId 2) "notify" "order-1"

            let hasCharge = encoded |> List.contains chargeCommand

            let hasNotify = encoded |> List.contains notifyCommand

            if records.Length = 4 && hasCharge && hasNotify then
                "encoded:grouped-activities"
            else
                "commit:other"

let replaySummary () =
    let reserve = step "reserve"
    let charge = step "charge"
    let notify = step "notify"

    let checkout =
        workflow "checkout" {
            let! reservation = call reserve input

            let! _ = calls [ charge, reservation; notify, value "order-1" ]

            return reservation
        }

    let app = firegrid [ reserve; charge; notify ] [ checkout ]

    let history =
        History.empty |> History.append (ActivityCompleted(OpId 0, "reserved:order-1"))

    DurableIrApp.planWorkflow 123L "checkout" "order-1" history app |> summarizePlan

[<EntryPoint>]
let main _ =
    printfn "%s" (replaySummary ())
    0
