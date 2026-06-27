// Capability C5: EffectLedger (durable steps)  —  Rung 2 of the durable-actor tower
//
// Companion SDD: docs/durable-actors-sdd.md
//
// The bug this kills: the owner runs side effects (charge a card, call an API)
// and crashes land mid-handler. "Charge, then append OrderPlaced" — crash in
// between → recovery re-folds, sees no OrderPlaced, and charges AGAIN.
//
// Fix: journal the INTENT before the effect and the RESULT after; skip on replay.
// The effect id is the deterministic call-site ordinal, so a recovering owner
// asks the same "have I already done effect 7?" question.
//
// "Crash/resume" is simulated faithfully by DISCARDING in-memory state and
// re-deriving the ledger from the log (foldTo) — recovery is just a re-fold.
//
// Proves:
//   P1 effectively-once     — a completed effect is not re-run on replay
//   P2 at-least-once        — an effect crashed-before-result is re-run on recovery
//   P3 deterministic ledger — re-folding the log yields the identical ledger / ids
//
// Run:
//   dotnet fable scripts/actor-02-durable-steps.fsx --outDir build_script --runScript

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

// --- Journal vocabulary ---
type Step =
    | RunStarted of input: string
    | EffectRequested of id: int * label: string
    | EffectCompleted of id: int * result: string
    | RunCompleted of result: string

module Step =
    let encode step =
        match step with
        | RunStarted input -> "run-started|" + input
        | EffectRequested(id, label) -> sprintf "eff-req|%d|%s" id label
        | EffectCompleted(id, result) -> sprintf "eff-done|%d|%s" id result
        | RunCompleted result -> "run-done|" + result

    let decode (body: string) =
        match body.Split('|') |> Array.toList with
        | [ "run-started"; input ] -> Ok(RunStarted input)
        | [ "eff-req"; id; label ] -> Ok(EffectRequested(int id, label))
        | [ "eff-done"; id; result ] -> Ok(EffectCompleted(int id, result))
        | [ "run-done"; result ] -> Ok(RunCompleted result)
        | _ -> Error(sprintf "unknown step record: %s" body)

    let codec: SubjectHistory.Codec<Step> = { Encode = encode; Decode = decode }

// --- Ledger: the deterministic fold of the journal ---
type Ledger =
    { Started: bool
      Requested: Set<int>
      Completed: Map<int, string>
      Done: string option }

module Ledger =
    let empty =
        { Started = false
          Requested = Set.empty
          Completed = Map.empty
          Done = None }

    let apply ledger (record: SubjectHistory.StoredRecord<Step>) =
        match record.Body with
        | RunStarted _ -> { ledger with Started = true }
        | EffectRequested(id, _) ->
            { ledger with
                Requested = Set.add id ledger.Requested }
        | EffectCompleted(id, result) ->
            { ledger with
                Completed = Map.add id result ledger.Completed }
        | RunCompleted result -> { ledger with Done = Some result }

    // next NEW effect id = how many effects this run has already requested (positional, deterministic)
    let nextId ledger = Set.count ledger.Requested

// --- An observable side effect: counts how many times it REALLY executed ---
let mutable runCounts: Map<int, int> = Map.empty

let private bump id =
    runCounts <- Map.add id ((Map.tryFind id runCounts |> Option.defaultValue 0) + 1) runCounts

let count id =
    Map.tryFind id runCounts |> Option.defaultValue 0

let mkEffect id : unit -> Async<string> =
    fun () ->
        async {
            bump id
            return sprintf "result-of-%d" id
        }

let basinName = "test-basin-885234"

let main =
    async {
        let s2 = S2Cli.connect ()
        let basin = s2 |> S2.basin basinName
        let subjectName = Proof.uniq "actor-durable-steps"
        let subject = SubjectHistory.SubjectId subjectName
        let mutable created = false

        // recovery == re-fold the whole log from 0 to the current tail
        let loadLedger () =
            async {
                let! t = SubjectHistory.tail basin subject
                let! ledger, _ =
                    SubjectHistory.foldTo basin Step.codec subject (SubjectHistory.Seq 0L) t Ledger.empty Ledger.apply

                return ledger
            }

        // the durable step: intent-before, result-after, skip-on-replay
        let step (ledger: Ledger) (id: int) (label: string) (thunk: unit -> Async<string>) =
            async {
                match Map.tryFind id ledger.Completed with
                | Some recorded ->
                    // already durably completed → return the logged result, never re-run
                    return recorded, ledger
                | None ->
                    // journal the intent once (idempotent if recovering a pending request)
                    let! ledger1 =
                        if Set.contains id ledger.Requested then
                            async { return ledger }
                        else
                            async {
                                let! _ = SubjectHistory.append basin Step.codec subject [ EffectRequested(id, label) ]

                                return
                                    { ledger with
                                        Requested = Set.add id ledger.Requested }
                            }

                    let! result = thunk ()
                    let! _ = SubjectHistory.append basin Step.codec subject [ EffectCompleted(id, result) ]

                    return
                        result,
                        { ledger1 with
                            Completed = Map.add id result ledger1.Completed }
            }

        printfn "subject stream: %s/%s" basinName subjectName

        try
            do! basin |> S2.createStream subjectName
            created <- true
            let! _ = SubjectHistory.append basin Step.codec subject [ RunStarted "invoice-123" ]

            do!
                Proof.section
                    "effectively-once across a crash"
                    (async {
                        let! l0 = loadLedger ()
                        Proof.check "next deterministic id starts at 0" (Ledger.nextId l0 = 0)

                        let! r0, _ = step l0 0 "reserve" (mkEffect 0)
                        Proof.check "effect 0 produced its result" (r0 = "result-of-0")
                        Proof.check "effect 0 executed exactly once" (count 0 = 1)

                        // CRASH: drop in-memory state, recover by re-folding the log
                        let! recovered = loadLedger ()

                        Proof.check
                            "recovered ledger shows effect 0 completed"
                            (Map.containsKey 0 recovered.Completed)

                        // replay the same step — must be skipped, recorded result returned
                        let! r0replay, _ = step recovered 0 "reserve" (mkEffect 0)
                        Proof.check "replayed effect 0 returns the recorded result" (r0replay = "result-of-0")
                        Proof.check "replayed effect 0 did NOT re-execute" (count 0 = 1)
                    })

            do!
                Proof.section
                    "at-least-once on crash-before-result"
                    (async {
                        // simulate a crash AFTER intent, BEFORE the result was journaled
                        let! _ = SubjectHistory.append basin Step.codec subject [ EffectRequested(1, "charge") ]

                        let! pending = loadLedger ()

                        Proof.check
                            "effect 1 is pending (requested, not completed)"
                            (Set.contains 1 pending.Requested && not (Map.containsKey 1 pending.Completed))

                        // recovery resumes the pending effect — it MUST re-run (hence: effects are idempotent)
                        let! r1, after = step pending 1 "charge" (mkEffect 1)
                        Proof.check "recovered effect 1 produced its result" (r1 = "result-of-1")
                        Proof.check "recovered effect 1 executed once on resume" (count 1 = 1)
                        Proof.check "recovered effect 1 is now completed" (Map.containsKey 1 after.Completed)

                        // a further replay now skips it — convergence to effectively-once
                        let! settled = loadLedger ()
                        let! _, _ = step settled 1 "charge" (mkEffect 1)
                        Proof.check "effect 1 not re-run after completion" (count 1 = 1)
                    })

            do!
                Proof.section
                    "ledger is a deterministic function of the log"
                    (async {
                        let! a = loadLedger ()
                        let! b = loadLedger ()
                        Proof.check "two independent folds agree on Completed" (a.Completed = b.Completed)
                        Proof.check "two independent folds agree on Requested" (a.Requested = b.Requested)
                        Proof.check "two independent folds agree on next id" (Ledger.nextId a = Ledger.nextId b)
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
