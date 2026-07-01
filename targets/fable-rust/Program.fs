module FiregridRust.Program

open Eff.Foundation.Durable

let replaySummary () =
    let draft, reservation =
        DurableIr.empty
        |> DurableIr.appendCallActivity "reserve" (ValueExpr.literal "order-1")

    let draft, receipt = draft |> DurableIr.appendCallActivity "charge" reservation

    let workflow =
        draft |> DurableIr.finish receipt |> DurableWorkflow.create "checkout"

    let history =
        History.empty |> History.append (ActivityCompleted(OpId 0, "reserved:order-1"))

    let replay = DurableWorkflow.replay history workflow

    match replay with
    | InvalidWorkflow _ -> "invalid"
    | ValidWorkflow outcome ->
        match outcome with
        | Done _ -> "done"
        | Blocked(_, need) ->
            match need with
            | NeedsActivity activity when activity.Name = "charge" && activity.Input = "reserved:order-1" ->
                "blocked:dependent-activity"
            | NeedsActivity activity when activity.Name = "reserve" && activity.Input = "order-1" -> "blocked:activity"
            | _ -> "blocked:other"

[<EntryPoint>]
let main _ =
    printfn "%s" (replaySummary ())
    0
