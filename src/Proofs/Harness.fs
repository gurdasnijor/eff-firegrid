namespace Eff.Proofs

open Fable.Core

module Harness =
    [<Emit("Date.now()")>]
    let private nowMillis () : float = jsNative

    [<Emit("process.exit($0)")>]
    let private processExit (_code: int) : unit = jsNative

    [<Emit("(process.env[$0] != null ? process.env[$0] : '')")>]
    let private envOr (_name: string) : string = jsNative

    type Config = { Basin: string }

    let config =
        { Basin =
            match envOr "S2_BASIN" with
            | "" -> "test-basin-885234"
            | name -> name }

    type Context =
        { mutable Passed: int
          mutable Failed: int
          mutable Failures: string list
          mutable Counter: int
          mutable Cleanups: (string * (unit -> Async<unit>)) list }

    type Proof =
        { Name: string
          Run: Context -> Async<unit> }

    let proof name run = { Name = name; Run = run }

    let private newContext () =
        { Passed = 0
          Failed = 0
          Failures = []
          Counter = 0
          Cleanups = [] }

    let uniq ctx prefix =
        ctx.Counter <- ctx.Counter + 1
        sprintf "%s-%d-%d" prefix (int64 (nowMillis ())) ctx.Counter

    let check ctx name condition =
        if condition then
            ctx.Passed <- ctx.Passed + 1
            printfn "  ✓ %s" name
        else
            ctx.Failed <- ctx.Failed + 1
            ctx.Failures <- name :: ctx.Failures
            printfn "  ✗ %s" name

    let expectEqual ctx name expected actual =
        if expected = actual then
            check ctx name true
        else
            ctx.Failed <- ctx.Failed + 1
            ctx.Failures <- name :: ctx.Failures
            printfn "  ✗ %s" name
            printfn "      expected: %A" expected
            printfn "      actual:   %A" actual

    let note message = printfn "  · %s" message

    let section ctx name body =
        async {
            printfn "\n== %s ==" name

            try
                do! body
            with e ->
                ctx.Failed <- ctx.Failed + 1
                ctx.Failures <- (name + " threw: " + e.Message) :: ctx.Failures
                printfn "  ✗ threw: %s" e.Message
        }

    let onCleanup ctx label thunk =
        ctx.Cleanups <- (label, thunk) :: ctx.Cleanups

    let private envFlag name =
        match envOr name with
        | ""
        | "0"
        | "false" -> false
        | _ -> true

    let private selectedNames () =
        (envOr "PROOF").Split(',')
        |> Array.map (fun value -> value.Trim())
        |> Array.filter ((<>) "")
        |> Array.toList

    let private matchesFilter (filter: string list) (name: string) =
        match filter with
        | [] -> true
        | names -> names |> List.exists (fun candidate -> name.Contains candidate)

    let private runCleanups ctx =
        async {
            if envFlag "PRESERVE" && ctx.Failed > 0 then
                printfn "\n[PRESERVE=1 with failures - skipping cleanup so resources can be inspected]"

                for (label, _) in List.rev ctx.Cleanups do
                    printfn "  preserved: %s" label
            else
                for (label, thunk) in ctx.Cleanups do
                    try
                        do! thunk ()
                    with e ->
                        printfn "  cleanup failed (%s): %s" label e.Message
        }

    let runProofs proofs =
        let ctx = newContext ()
        let filter = selectedNames ()
        let selected = proofs |> List.filter (fun proof -> matchesFilter filter proof.Name)

        async {
            if List.isEmpty selected then
                let label =
                    match filter with
                    | [] -> "no registered proofs"
                    | names -> sprintf "no proofs matched filter: %s" (String.concat ", " names)

                ctx.Failed <- ctx.Failed + 1
                ctx.Failures <- label :: ctx.Failures
                printfn "%s" label

            for proof in selected do
                printfn "\n### %s" proof.Name

                try
                    do! proof.Run ctx
                with e ->
                    ctx.Failed <- ctx.Failed + 1
                    ctx.Failures <- (proof.Name + " fatal: " + e.Message) :: ctx.Failures
                    printfn "  ✗ fatal: %s" e.Message

            do! runCleanups ctx

            printfn "\n%d passed, %d failed" ctx.Passed ctx.Failed

            for failure in List.rev ctx.Failures do
                printfn "  FAILED: %s" failure

            processExit (if ctx.Failed > 0 then 1 else 0)
        }
        |> Async.StartImmediate
