module FiregridRust.Program

open Eff.Foundation.Durable

let replaySummary () =
    let draft, reservation =
        DurableIr.empty
        |> DurableIr.appendCallActivity "reserve" (ValueExpr.literal "order-1")

    let draft, receipt = draft |> DurableIr.appendCallActivity "charge" reservation

    let program = draft |> DurableIr.finish receipt

    let history =
        History.empty |> History.append (ActivityCompleted(OpId 0, "reserved:order-1"))

    match DurableIr.replay history program with
    | Done _ -> "done"
    | Blocked(_,
              NeedsActivity { Name = "charge"
                              Input = "reserved:order-1" }) -> "blocked:dependent-activity"
    | Blocked(_, NeedsActivity { Name = "reserve"; Input = "order-1" }) -> "blocked:activity"
    | Blocked(_, _) -> "blocked:other"

[<EntryPoint>]
let main _ =
    printfn "%s" (replaySummary ())
    0
