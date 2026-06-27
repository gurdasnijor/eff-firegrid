namespace Eff.Proofs

open Fable.Core

/// The generic, behavior-free proof harness. It is compiled into
/// eff-firegrid.fsproj so `dotnet build`, FS1182, and fsharplint enforce that
/// proofs stay in sync with the production APIs they exercise — drift is a
/// build error, not a runtime surprise in a loose script.
///
/// It contains NO workflow / object / S2 behavior — only:
///   - section / check / expectEqual / note printing
///   - unique, readable, sortable resource-name generation
///   - a cleanup registry (skipped on failure when PRESERVE=1)
///   - script config (the S2 basin name, overridable with S2_BASIN)
///   - a runner that executes a registry of proofs, prints one combined
///     summary, and sets the process exit code
///
/// Proofs run under Fable/Node (the S2 client is JS interop), so the
/// JS-specific emits below are unused on the .NET build path and live only to
/// drive the Node run.
module Harness =

    [<Emit("Date.now()")>]
    let private nowMillis () : float = jsNative

    [<Emit("process.exit($0)")>]
    let private processExit (code: int) : unit = jsNative

    [<Emit("(process.env[$0] != null ? process.env[$0] : '')")>]
    let private envOr (name: string) : string = jsNative

    /// Script configuration sourced from the environment, with the same default
    /// the scratchpads hard-coded. Override the basin with S2_BASIN=...
    type Config = { Basin: string }

    let config: Config =
        { Basin =
            match envOr "S2_BASIN" with
            | "" -> "test-basin-885234"
            | name -> name }

    /// Mutable run state. One Context is shared across every proof in a run so
    /// the final summary aggregates all sections and checks.
    type Context =
        { mutable Passed: int
          mutable Failed: int
          mutable Failures: string list
          mutable Counter: int
          mutable Cleanups: (string * (unit -> Async<unit>)) list }

    /// A named proof body. The body receives the shared Context so its checks
    /// and cleanups land in the same summary as every other proof.
    type Proof =
        { Name: string
          Run: Context -> Async<unit> }

    let proof (name: string) (run: Context -> Async<unit>) : Proof = { Name = name; Run = run }

    let private newContext () =
        { Passed = 0
          Failed = 0
          Failures = []
          Counter = 0
          Cleanups = [] }

    /// A unique, readable, sortable resource name: "<prefix>-<epochMillis>-<n>".
    let uniq (ctx: Context) (prefix: string) : string =
        ctx.Counter <- ctx.Counter + 1
        sprintf "%s-%d-%d" prefix (int64 (nowMillis ())) ctx.Counter

    let check (ctx: Context) (name: string) (condition: bool) =
        if condition then
            ctx.Passed <- ctx.Passed + 1
            printfn "  ✓ %s" name
        else
            ctx.Failed <- ctx.Failed + 1
            ctx.Failures <- name :: ctx.Failures
            printfn "  ✗ %s" name

    /// Equality check that prints both sides on mismatch.
    let expectEqual (ctx: Context) (name: string) (expected: 'a) (actual: 'a) =
        if expected = actual then
            check ctx name true
        else
            ctx.Failed <- ctx.Failed + 1
            ctx.Failures <- name :: ctx.Failures
            printfn "  ✗ %s" name
            printfn "      expected: %A" expected
            printfn "      actual:   %A" actual

    /// Print an informational line (an authoritative record, a fold result...).
    let note (message: string) = printfn "  · %s" message

    /// Run one named section. A thrown exception is recorded as one failure and
    /// does not abort the rest of the proof.
    let section (ctx: Context) (name: string) (body: Async<unit>) =
        async {
            printfn "\n== %s ==" name

            try
                do! body
            with e ->
                ctx.Failed <- ctx.Failed + 1
                ctx.Failures <- (name + " threw: " + e.Message) :: ctx.Failures
                printfn "  ✗ threw: %s" e.Message
        }

    /// Register cleanup to run after the proof. Cleanups run last-in-first-out,
    /// so register them in creation order.
    let onCleanup (ctx: Context) (label: string) (thunk: unit -> Async<unit>) =
        ctx.Cleanups <- (label, thunk) :: ctx.Cleanups

    let private envFlag (name: string) =
        match envOr name with
        | ""
        | "0"
        | "false" -> false
        | _ -> true

    /// Proof-name filter from the PROOF env var (comma-separated substrings).
    let private selectedNames () =
        let raw = envOr "PROOF"

        raw.Split(',')
        |> Array.map (fun s -> s.Trim())
        |> Array.filter (fun s -> s <> "")
        |> Array.toList

    let private matchesFilter (filter: string list) (name: string) =
        match filter with
        | [] -> true
        | names -> names |> List.exists (fun n -> name.Contains n)

    let private runCleanups (ctx: Context) =
        async {
            if envFlag "PRESERVE" && ctx.Failed > 0 then
                printfn "\n[PRESERVE=1 with failures — skipping cleanup so resources can be inspected]"

                for (label, _) in List.rev ctx.Cleanups do
                    printfn "  preserved: %s" label
            else
                // Registered LIFO; the list is already newest-first.
                for (label, thunk) in ctx.Cleanups do
                    try
                        do! thunk ()
                    with e ->
                        printfn "  cleanup failed (%s): %s" label e.Message
        }

    /// Run a registry of proofs (optionally filtered by the PROOF env var),
    /// print one combined summary, run cleanups, and set the process exit code.
    let runProofs (proofs: Proof list) =
        let ctx = newContext ()
        let filter = selectedNames ()
        let selected = proofs |> List.filter (fun p -> matchesFilter filter p.Name)

        async {
            if List.isEmpty selected then
                printfn "no proofs matched filter: %s" (String.concat ", " filter)

            for p in selected do
                printfn "\n### %s" p.Name

                try
                    do! p.Run ctx
                with e ->
                    ctx.Failed <- ctx.Failed + 1
                    ctx.Failures <- (p.Name + " fatal: " + e.Message) :: ctx.Failures
                    printfn "  ✗ fatal: %s" e.Message

            do! runCleanups ctx

            printfn "\n%d passed, %d failed" ctx.Passed ctx.Failed

            for f in List.rev ctx.Failures do
                printfn "  FAILED: %s" f

            processExit (if ctx.Failed > 0 then 1 else 0)
        }
        |> Async.StartImmediate
