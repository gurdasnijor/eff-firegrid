// Durable actors — the ergonomic API, end to end.
//
// This is what an APPLICATION AUTHOR writes. Nothing here touches journals,
// folds, replay, ownership, or codecs-for-state — that is all hidden inside
// src/Foundation/Actor.fs. Two surfaces:
//
//   1. a STATEFUL ACTOR   — a bank account: persisted state via a reducer
//   2. a DURABLE WORKFLOW  — an order: multi-step, survives crashes and sleeps
//
// (The actor-0N proof spikes validate the underlying properties; you do not read
//  those to USE the library — you read this.)
//
// Run:
//   dotnet fable scripts/durable-demo.fsx --outDir build_script --runScript

#r "nuget: Fable.Core, 5.1.0"
#load "../src/S2/Interop.fs"
#load "../src/S2/Errors.fs"
#load "../src/S2/Client.fs"
#load "../src/S2/Cli.fs"
#load "../src/Foundation/SubjectHistory.fs"
#load "../src/Foundation/Actor.fs"

open Fable.Core
open Eff
open Eff.Foundation

[<Emit("process.exit($0)")>]
let exit (code: int) : unit = jsNative

[<Emit("Date.now()")>]
let now () : float = jsNative

// ─────────────────────────────────────────────────────────────────────────
// 1. A stateful actor: a bank account.
//    Define a reducer over commands; state is rebuilt from the log on demand.
// ─────────────────────────────────────────────────────────────────────────
type Cmd =
    | Deposit of int
    | Withdraw of int

let account =
    Durable.actor
        "account"
        0 // initial balance
        (fun balance cmd ->
            match cmd with
            | Deposit n -> balance + n
            | Withdraw n -> max 0 (balance - n))
        (Durable.codec
            (function
            | Deposit n -> "deposit|" + string n
            | Withdraw n -> "withdraw|" + string n)
            (fun s ->
                match s.Split('|') |> Array.toList with
                | [ "deposit"; n ] -> Deposit(int n)
                | [ "withdraw"; n ] -> Withdraw(int n)
                | other -> failwith ("bad command: " + String.concat "|" other)))

// ─────────────────────────────────────────────────────────────────────────
// 2. A durable workflow: fulfill an order.
//    Imperative body; each Step runs once even across crashes; Sleep is durable.
// ─────────────────────────────────────────────────────────────────────────
let order =
    Durable.workflow "order" (fun (sku: string) ctx ->
        async {
            let! reservation = ctx.Step("reserve", (fun () -> async { return "res-" + sku }))
            let! _payment = ctx.Step("charge", (fun () -> async { return "paid-" + sku }))
            do! ctx.Sleep 400.0 // durable wait: survives a crash, resumes on the deadline
            let! tracking = ctx.Step("ship", (fun () -> async { return "trk-" + reservation }))
            return tracking
        })

let main =
    async {
        let s2 = S2Cli.connect ()
        let basin = s2 |> S2.basin "test-basin-885234"
        let stamp = int64 (now ())
        let acct = Durable.Id(sprintf "acct-%d" stamp)
        let ord = Durable.Id(sprintf "order-%d" stamp)

        printfn "── stateful actor: account %A ──" acct
        do! Durable.send basin account acct (Deposit 100)
        do! Durable.send basin account acct (Deposit 50)
        do! Durable.send basin account acct (Withdraw 30)

        let! balance = Durable.state basin account acct
        printfn "  balance after +100 +50 -30 = %d" balance

        // there is no in-memory state: a fresh read just folds the durable log again
        let! recovered = Durable.state basin account acct
        printfn "  balance recovered from the log = %d   (crash-survival is free)" recovered

        printfn "\n── durable workflow: order %A ──" ord
        let! tracking = Durable.run basin order ord "widget-42"
        printfn "  order fulfilled, tracking = %s" tracking

        // re-running a completed workflow is a no-op and returns the same result
        let! again = Durable.run basin order ord "widget-42"
        printfn "  re-run of the finished workflow = %s   (idempotent)" again

        do! Durable.remove basin account acct
        do! Durable.remove basin order ord
        printfn "\ndone."
        exit 0
    }

main |> Async.StartImmediate
