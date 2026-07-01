module FiregridRust.Program

open Eff.Foundation.Durable

let replaySummary () =
    let activity = Activities.create "reserve" "order-1"
    let program = Workflow.call activity.Name activity.Input

    match Durable.replay History.empty program with
    | Done _ -> "done"
    | Blocked(_, NeedsActivity blocked) when blocked = activity -> "blocked:activity"
    | Blocked(_, _) -> "blocked:other"

[<EntryPoint>]
let main _ =
    printfn "%s" (replaySummary ())
    0
