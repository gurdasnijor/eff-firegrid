// Capability C7: DurableActor runtime (the decider loop)  —  Rung 4 of the tower
//
// Companion SDD: docs/durable-actors-sdd.md
//
// This ties C5 (effect ledger) and C6 (suspension) together under a single loop:
// fold the journal -> run a PURE decider (state -> next action) -> journal the
// effect/timer -> execute pending effects exactly once -> repeat. It is StateView
// that writes back.
//
// The determinism contract (as load-bearing as the Version Invariant): `decide`
// is a pure function of (folded state, next event). ALL nondeterminism — clock,
// randomness, IO — flows through journaled effects/timers, never read inside
// `decide`. Each `turn` reloads from the log and holds ZERO state between calls,
// so every turn is a cold crash-recovery.
//
// Workflow under test:  reserve -> charge -> sleep(cooldown) -> ship -> done
//
// Proves:
//   P1 crash/resume equivalence — a hard restart mid-flight resumes to the SAME
//      terminal journal as a clean run
//   P2 no duplicated effects    — each effect's side effect runs exactly once,
//      even across the restart
//   P3 journal determinism      — the committed record sequence equals an
//      enumerated expected sequence (the log is a function of logic, not timing)
//   P4 spurious re-activation    — re-activating a completed actor is a no-op
//
// Run:
//   dotnet fable scripts/actor-04-runtime.fsx --outDir build_script --runScript

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

// --- Unified journal vocabulary (effects from C5 + timers from C6) ---
type Ev =
    | Started of order: string
    | EffReq of id: int * label: string
    | EffDone of id: int * result: string
    | TimerSet of timerId: string
    | TimerFired of timerId: string
    | Finished of result: string

module Ev =
    let encode ev =
        match ev with
        | Started order -> "started|" + order
        | EffReq(id, label) -> sprintf "eff-req|%d|%s" id label
        | EffDone(id, result) -> sprintf "eff-done|%d|%s" id result
        | TimerSet id -> "timer-set|" + id
        | TimerFired id -> "timer-fired|" + id
        | Finished result -> "finished|" + result

    let decode (body: string) =
        match body.Split('|') |> Array.toList with
        | [ "started"; order ] -> Ok(Started order)
        | [ "eff-req"; id; label ] -> Ok(EffReq(int id, label))
        | [ "eff-done"; id; result ] -> Ok(EffDone(int id, result))
        | [ "timer-set"; id ] -> Ok(TimerSet id)
        | [ "timer-fired"; id ] -> Ok(TimerFired id)
        | [ "finished"; result ] -> Ok(Finished result)
        | _ -> Error(sprintf "unknown ev record: %s" body)

    let codec: SubjectHistory.Codec<Ev> = { Encode = encode; Decode = decode }

// --- Folded workflow state ---
type WfState =
    { Started: bool
      Requested: Set<int>
      Completed: Map<int, string>
      Timers: Set<string>
      FiredTimers: Set<string>
      Done: string option }

module WfState =
    let empty =
        { Started = false
          Requested = Set.empty
          Completed = Map.empty
          Timers = Set.empty
          FiredTimers = Set.empty
          Done = None }

    let apply s (record: SubjectHistory.StoredRecord<Ev>) =
        match record.Body with
        | Started _ -> { s with Started = true }
        | EffReq(id, _) ->
            { s with
                Requested = Set.add id s.Requested }
        | EffDone(id, result) ->
            { s with
                Completed = Map.add id result s.Completed }
        | TimerSet id -> { s with Timers = Set.add id s.Timers }
        | TimerFired id ->
            { s with
                Timers = Set.remove id s.Timers
                FiredTimers = Set.add id s.FiredTimers }
        | Finished result -> { s with Done = Some result }

// --- The PURE decider: workflow logic as a function of folded state ---
type Decision =
    | DoStart
    | DoEffect of int * string
    | DoSetTimer of string
    | DoWait of string
    | DoFinish of string
    | DoNoop of string

let decide (s: WfState) =
    if not s.Started then
        DoStart
    elif not (Map.containsKey 0 s.Completed) then
        DoEffect(0, "reserve")
    elif not (Map.containsKey 1 s.Completed) then
        DoEffect(1, "charge")
    elif not (Set.contains "cooldown" s.FiredTimers) then
        if Set.contains "cooldown" s.Timers then
            DoWait "cooldown"
        else
            DoSetTimer "cooldown"
    elif not (Map.containsKey 2 s.Completed) then
        DoEffect(2, "ship")
    elif s.Done.IsNone then
        DoFinish(sprintf "shipped:%s" (Map.find 2 s.Completed))
    else
        DoNoop(s.Done |> Option.defaultValue "")

// the deterministic journal this workflow MUST produce, regardless of crash timing
let expectedJournal =
    [ Started "order-1"
      EffReq(0, "reserve")
      EffDone(0, "result-of-0")
      EffReq(1, "charge")
      EffDone(1, "result-of-1")
      TimerSet "cooldown"
      TimerFired "cooldown"
      EffReq(2, "ship")
      EffDone(2, "result-of-2")
      Finished "shipped:result-of-2" ]

// --- Observable side effects, keyed per (stream, id) so each run is independent ---
let mutable runCounts: Map<string, int> = Map.empty

let private bump k =
    runCounts <- Map.add k ((Map.tryFind k runCounts |> Option.defaultValue 0) + 1) runCounts

let count k =
    Map.tryFind k runCounts |> Option.defaultValue 0

let key name id = sprintf "%s#%d" name id

type Progress =
    | Continued
    | Suspended of string
    | Completed of string

let basinName = "test-basin-885234"

let main =
    async {
        let s2 = S2Cli.connect ()
        let basin = s2 |> S2.basin basinName
        let created = System.Collections.Generic.List<string>()

        let createStream name =
            async {
                do! basin |> S2.createStream name
                created.Add(name)
            }

        let load subject =
            async {
                let! t = SubjectHistory.tail basin subject
                let! s, _ =
                    SubjectHistory.foldTo basin Ev.codec subject (SubjectHistory.Seq 0L) t WfState.empty WfState.apply

                return s
            }

        let journal subject =
            async {
                let! t = SubjectHistory.tail basin subject
                let! evs, _ =
                    SubjectHistory.foldTo basin Ev.codec subject (SubjectHistory.Seq 0L) t [] (fun acc r -> r.Body :: acc)

                return List.rev evs
            }

        let append subject records =
            async {
                let! _ = SubjectHistory.append basin Ev.codec subject records
                return ()
            }

        // one atomic turn: cold-recover from the log, decide, perform ONE journaled action
        let turn (name: string) subject =
            async {
                let! s = load subject

                match decide s with
                | DoStart ->
                    do! append subject [ Started "order-1" ]
                    return Continued
                | DoEffect(id, label) ->
                    // journal intent only if this is a fresh request (idempotent on recovery)
                    do!
                        if not (Set.contains id s.Requested) then
                            append subject [ EffReq(id, label) ]
                        else
                            async { return () }

                    bump (key name id) // the real side effect
                    let result = sprintf "result-of-%d" id
                    do! append subject [ EffDone(id, result) ]
                    return Continued
                | DoSetTimer id ->
                    do! append subject [ TimerSet id ]
                    return Continued
                | DoWait id -> return Suspended id
                | DoFinish result ->
                    do! append subject [ Finished result ]
                    return Completed result
                | DoNoop result -> return Completed result
            }

        // drive turns to completion; on suspension the scheduler delivers the wake
        let drive name subject =
            async {
                let mutable result = None
                let mutable go = true

                while go do
                    let! p = turn name subject

                    match p with
                    | Continued -> ()
                    | Suspended timerId -> do! append subject [ TimerFired timerId ]
                    | Completed r ->
                        result <- Some r
                        go <- false

                return result
            }

        try
            let nameMain = Proof.uniq "actor-runtime-main"
            let subjectMain = SubjectHistory.SubjectId nameMain
            do! createStream nameMain

            do!
                Proof.section
                    "deterministic run to completion"
                    (async {
                        let! result = drive nameMain subjectMain
                        Proof.check "workflow reaches Done" (result = Some "shipped:result-of-2")

                        let! j = journal subjectMain
                        Proof.check "journal equals the enumerated deterministic sequence" (j = expectedJournal)
                        Proof.check "reserve ran exactly once" (count (key nameMain 0) = 1)
                        Proof.check "charge ran exactly once" (count (key nameMain 1) = 1)
                        Proof.check "ship ran exactly once" (count (key nameMain 2) = 1)
                    })

            do!
                Proof.section
                    "hard restart mid-flight resumes identically"
                    (async {
                        let nameR = Proof.uniq "actor-runtime-restart"
                        let subjectR = SubjectHistory.SubjectId nameR
                        do! createStream nameR

                        // run only a few turns, then ABANDON all in-memory state
                        let! _ = turn nameR subjectR // DoStart
                        let! _ = turn nameR subjectR // reserve
                        let! _ = turn nameR subjectR // charge

                        // cold resume from the log (turn holds no memory across calls)
                        let! result = drive nameR subjectR
                        Proof.check "cold-restart workflow completes with same result" (result = Some "shipped:result-of-2")

                        let! j = journal subjectR
                        Proof.check "restarted journal equals the deterministic sequence" (j = expectedJournal)
                        Proof.check "reserve still ran exactly once across the restart" (count (key nameR 0) = 1)
                        Proof.check "charge still ran exactly once across the restart" (count (key nameR 1) = 1)
                        Proof.check "ship ran exactly once" (count (key nameR 2) = 1)
                    })

            do!
                Proof.section
                    "spurious re-activation is a no-op"
                    (async {
                        let! before = journal subjectMain
                        let! p = turn nameMain subjectMain

                        Proof.check
                            "re-activating a completed actor returns Completed"
                            (match p with
                             | Completed _ -> true
                             | _ -> false)

                        let! after = journal subjectMain
                        Proof.check "no new records appended on re-activation" (after = before)

                        Proof.check
                            "no effect re-ran on re-activation"
                            (count (key nameMain 0) = 1
                             && count (key nameMain 1) = 1
                             && count (key nameMain 2) = 1)
                    })

            for name in created do
                do! basin |> S2.deleteStream name

            Proof.finish ()
        with e ->
            printfn "\nfatal: %s" e.Message

            for name in created do
                try
                    do! basin |> S2.deleteStream name
                with cleanup ->
                    printfn "cleanup failed for %s: %s" name cleanup.Message

            Proof.failed <- Proof.failed + 1
            Proof.failures <- ("fatal: " + e.Message) :: Proof.failures
            Proof.finish ()
    }

main |> Async.StartImmediate
