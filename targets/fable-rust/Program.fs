module FiregridRust.Program

open Eff.Foundation.Durable

let replaySummary () =
    let program =
        DurableIr.create
            [ DurableIr.callActivity OpId.zero "reserve" (ValueExpr.literal "order-1") ]
            (ValueExpr.activityResult OpId.zero)

    match DurableIr.replay History.empty program with
    | Done _ -> "done"
    | Blocked(_, NeedsActivity { Name = "reserve"; Input = "order-1" }) -> "blocked:activity"
    | Blocked(_, _) -> "blocked:other"

[<EntryPoint>]
let main _ =
    printfn "%s" (replaySummary ())
    0
