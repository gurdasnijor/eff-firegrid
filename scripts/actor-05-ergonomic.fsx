// Capability C8: Ergonomic authoring  —  Rung 5 (the summit) of the durable-actor tower
//
// Companion SDD: docs/durable-actors-sdd.md
//
// The C7 decider-as-data is faithful but verbose. The ergonomic surface is an
// imperative handler with a durable context — the Temporal/DBOS shape:
//
//   let workflow (ctx: Ctx) = async {
//       let! r = ctx.Step "reserve" (fun () -> async { return "reserved" })
//       let! c = ctx.Step "charge"  (fun () -> async { return "charged" })
//       do!     ctx.Sleep "cooldown"
//       let! s = ctx.Step "ship"    (fun () -> async { return "shipped" })
//       return s }
//
// It is implemented by REPLAYING the handler top-to-bottom on every turn:
//   - Ctx.Step memoizes through the C5 ledger (a completed step returns its
//     logged result WITHOUT re-running its thunk)
//   - Ctx.Sleep observes a fired timer and proceeds, or journals TimerSet and
//     raises Suspend to end the turn
// This composes C4–C7 without exposing them.
//
// Proves:
//   P1 ergonomic completion   — the authored workflow runs to completion across a real suspend/resume
//   P2 invariants preserved   — each Step's side effect runs exactly once DESPITE the handler being
//                               replayed top-to-bottom on every turn (memoization holds)
//   P3 canonical journal      — the journal equals the deterministic sequence the C7 decider produces
//
// Run:
//   dotnet fable scripts/actor-05-ergonomic.fsx --outDir build_script --runScript

#r "nuget: Fable.Core, 5.1.0"
#load "../src/S2/Interop.fs"
#load "../src/S2/Errors.fs"
#load "../src/S2/Client.fs"
#load "../src/S2/Cli.fs"
#load "../src/Foundation/SubjectHistory.fs"

open Fable.Core
open Eff
open Eff.Foundation

[<Emit("process.exit($0)")>]
let exit (code: int) : unit = jsNative

[<Emit("Date.now()")>]
let now () : float = jsNative

module Proof =
    let mutable passed = 0
    let mutable failed = 0
    let mutable failures: string list = []
    let mutable counter = 0

    let uniq (prefix: string) : string =
        counter <- counter + 1
        sprintf "%s-%d-%d" prefix (int64 (now ())) counter

    let check name condition =
        if condition then
            passed <- passed + 1
            printfn "  ok  %s" name
        else
            failed <- failed + 1
            failures <- name :: failures
            printfn "  bad %s" name

    let section name body =
        async {
            printfn "\n== %s ==" name

            try
                do! body
            with e ->
                failed <- failed + 1
                failures <- (name + " threw: " + e.Message) :: failures
                printfn "  bad threw: %s" e.Message
        }

    let finish () =
        printfn "\n%d passed, %d failed" passed failed

        if failed > 0 then
            failures |> List.rev |> List.iter (printfn "  - %s")
            exit 1
        else
            exit 0

exception SuspendSignal

// --- Journal vocabulary (mirrors the C7 decider's canonical shape) ---
type J =
    | Started of order: string
    | StepReq of ord: int * label: string
    | StepDone of ord: int * result: string
    | TimerSet of timer: string
    | TimerFired of timer: string
    | Finished of result: string

module J =
    let encode j =
        match j with
        | Started order -> "started|" + order
        | StepReq(ord, label) -> sprintf "step-req|%d|%s" ord label
        | StepDone(ord, result) -> sprintf "step-done|%d|%s" ord result
        | TimerSet timer -> "timer-set|" + timer
        | TimerFired timer -> "timer-fired|" + timer
        | Finished result -> "finished|" + result

    let decode (body: string) =
        match body.Split('|') |> Array.toList with
        | [ "started"; order ] -> Ok(Started order)
        | [ "step-req"; ord; label ] -> Ok(StepReq(int ord, label))
        | [ "step-done"; ord; result ] -> Ok(StepDone(int ord, result))
        | [ "timer-set"; timer ] -> Ok(TimerSet timer)
        | [ "timer-fired"; timer ] -> Ok(TimerFired timer)
        | [ "finished"; result ] -> Ok(Finished result)
        | _ -> Error(sprintf "unknown journal record: %s" body)

    let codec: SubjectHistory.Codec<J> = { Encode = encode; Decode = decode }

// --- Folded view used for memoization on each replay ---
type View =
    { Started: bool
      Requested: Set<int>
      Completed: Map<int, string>
      SetTimers: Set<string>
      FiredTimers: Set<string>
      Done: string option }

module View =
    let empty =
        { Started = false
          Requested = Set.empty
          Completed = Map.empty
          SetTimers = Set.empty
          FiredTimers = Set.empty
          Done = None }

    let apply v (record: SubjectHistory.StoredRecord<J>) =
        match record.Body with
        | Started _ -> { v with Started = true }
        | StepReq(ord, _) ->
            { v with
                Requested = Set.add ord v.Requested }
        | StepDone(ord, result) ->
            { v with
                Completed = Map.add ord result v.Completed }
        | TimerSet timer ->
            { v with
                SetTimers = Set.add timer v.SetTimers }
        | TimerFired timer ->
            { v with
                FiredTimers = Set.add timer v.FiredTimers }
        | Finished result -> { v with Done = Some result }

// --- The ergonomic context handed to the author ---
type Ctx =
    { Step: string -> (unit -> Async<string>) -> Async<string>
      Sleep: string -> Async<unit> }

// the canonical journal the C7 decider produces for this same logic
let expectedJournal =
    [ Started "order-1"
      StepReq(0, "reserve")
      StepDone(0, "reserved")
      StepReq(1, "charge")
      StepDone(1, "charged")
      TimerSet "cooldown"
      TimerFired "cooldown"
      StepReq(2, "ship")
      StepDone(2, "shipped")
      Finished "shipped" ]

// --- Observable side effects, to prove a thunk runs at most once ---
let mutable runCounts: Map<int, int> = Map.empty

let private bump ord =
    runCounts <- Map.add ord ((Map.tryFind ord runCounts |> Option.defaultValue 0) + 1) runCounts

let count ord =
    Map.tryFind ord runCounts |> Option.defaultValue 0

// the workflow, authored against the ergonomic surface (knows nothing of the journal)
let workflow (ctx: Ctx) =
    async {
        let! _reserved = ctx.Step "reserve" (fun () -> async { return "reserved" })
        let! _charged = ctx.Step "charge" (fun () -> async { return "charged" })
        do! ctx.Sleep "cooldown"
        let! shipped = ctx.Step "ship" (fun () -> async { return "shipped" })
        return shipped
    }

type Outcome =
    | OComplete of string
    | OSuspend

let basinName = "test-basin-885234"

let main =
    async {
        let s2 = S2Cli.connect ()
        let basin = s2 |> S2.basin basinName
        let subjectName = Proof.uniq "actor-ergonomic"
        let subject = SubjectHistory.SubjectId subjectName
        let mutable created = false
        let turns = ref 0

        let load () =
            async {
                let! t = SubjectHistory.tail basin subject
                let! v, _ =
                    SubjectHistory.foldTo basin J.codec subject (SubjectHistory.Seq 0L) t View.empty View.apply

                return v
            }

        let journal () =
            async {
                let! t = SubjectHistory.tail basin subject
                let! js, _ =
                    SubjectHistory.foldTo basin J.codec subject (SubjectHistory.Seq 0L) t [] (fun acc r -> r.Body :: acc)

                return List.rev js
            }

        let append records =
            async {
                let! _ = SubjectHistory.append basin J.codec subject records
                return ()
            }

        let onlyIf cond records =
            if cond then append records else async { return () }

        // run the handler once, top-to-bottom, against a Ctx bound to this turn's folded view
        let runTurn (handler: Ctx -> Async<string>) =
            async {
                turns.Value <- turns.Value + 1
                let! view = load ()

                if view.Done.IsSome then
                    // already finished: a spurious re-activation is a clean no-op
                    return OComplete view.Done.Value
                else
                    do! onlyIf (not view.Started) [ Started "order-1" ]

                    let stepOrd = ref 0

                    let ctx =
                        { Step =
                            fun label thunk ->
                                async {
                                    let ord = stepOrd.Value
                                    stepOrd.Value <- ord + 1

                                    match Map.tryFind ord view.Completed with
                                    | Some recorded -> return recorded // memoized: thunk is NOT run
                                    | None ->
                                        do! onlyIf (not (Set.contains ord view.Requested)) [ StepReq(ord, label) ]
                                        bump ord // the real side effect
                                        let! result = thunk ()
                                        do! append [ StepDone(ord, result) ]
                                        return result
                                }
                          Sleep =
                            fun timer ->
                                async {
                                    if Set.contains timer view.FiredTimers then
                                        return ()
                                    else
                                        do! onlyIf (not (Set.contains timer view.SetTimers)) [ TimerSet timer ]
                                        return raise SuspendSignal
                                } }

                    try
                        let! result = handler ctx
                        do! append [ Finished result ]
                        return OComplete result
                    with
                    | SuspendSignal -> return OSuspend
            }

        // drive turns to completion; on suspend the scheduler delivers the pending wake
        let activate (handler: Ctx -> Async<string>) =
            async {
                let mutable result = None
                let mutable go = true

                while go do
                    let! outcome = runTurn handler

                    match outcome with
                    | OComplete r ->
                        result <- Some r
                        go <- false
                    | OSuspend ->
                        let! v = load ()

                        for timer in v.SetTimers do
                            do! onlyIf (not (Set.contains timer v.FiredTimers)) [ TimerFired timer ]

                return result
            }

        printfn "subject stream: %s/%s" basinName subjectName

        try
            do! basin |> S2.createStream subjectName
            created <- true

            do!
                Proof.section
                    "ergonomic workflow runs to completion across suspend/resume"
                    (async {
                        let! result = activate workflow
                        Proof.check "authored workflow completes" (result = Some "shipped")
                        Proof.check "the handler was replayed (a real suspend occurred)" (turns.Value >= 2)
                    })

            do!
                Proof.section
                    "invariants preserved: each step runs exactly once despite replay"
                    (async {
                        Proof.check "reserve thunk ran exactly once" (count 0 = 1)
                        Proof.check "charge thunk ran exactly once" (count 1 = 1)
                        Proof.check "ship thunk ran exactly once" (count 2 = 1)
                    })

            do!
                Proof.section
                    "ergonomic actor produces the canonical decider journal"
                    (async {
                        let! j = journal ()
                        Proof.check "journal equals the C7 canonical sequence" (j = expectedJournal)

                        // re-activating a finished actor is a no-op (no replay side effects, no new records)
                        let! again = activate workflow
                        Proof.check "re-activation returns the same result" (again = Some "shipped")
                        let! j2 = journal ()
                        Proof.check "no new records on re-activation" (j2 = expectedJournal)
                        Proof.check "no step re-ran on re-activation" (count 0 = 1 && count 1 = 1 && count 2 = 1)
                    })

            do! basin |> S2.deleteStream subjectName
            created <- false
            Proof.finish ()
        with e ->
            printfn "\nfatal: %s" e.Message

            if created then
                try
                    do! basin |> S2.deleteStream subjectName
                with cleanup ->
                    printfn "cleanup failed: %s" cleanup.Message

            Proof.failed <- Proof.failed + 1
            Proof.failures <- ("fatal: " + e.Message) :: Proof.failures
            Proof.finish ()
    }

main |> Async.StartImmediate
