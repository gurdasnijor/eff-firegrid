// Capability C6: Suspension — Timers + Inbox  —  Rung 3 of the durable-actor tower
//
// Companion SDD: docs/durable-actors-sdd.md
//
// The bug this kills: the handler must WAIT — "sleep 24h then remind" or "await
// approval." You cannot block a host for 24h; it will redeploy. So waiting itself
// must be durable and must RELEASE the host. A wait is a record plus returning
// from the handler; the actor re-enters only when a new record resolves the wait.
//
// The scheduler that emits TimerFired is itself a C4–C5 actor (state = pending
// timers); it adds no new machinery, it dog-foods the tower.
//
// Proves:
//   P1 durable wait     — TimerSet survives a re-fold; actor is Waiting from the log alone
//   P2 re-entrancy      — TimerFired clears the pending timer and resumes
//   P3 idempotent wake  — a duplicated TimerFired folds to the same state (at-least-once OK)
//   P4 inbox dedupe     — the same msgId delivered twice is admitted exactly once
//
// Run:
//   dotnet fable scripts/actor-03-suspension.fsx --outDir build_script --runScript

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

// --- Suspension vocabulary ---
type Wait =
    | TimerSet of timerId: string * wakeAt: string
    | TimerFired of timerId: string
    | MsgAccepted of msgId: string * payload: string

module Wait =
    let encode w =
        match w with
        | TimerSet(id, wakeAt) -> sprintf "timer-set|%s|%s" id wakeAt
        | TimerFired id -> "timer-fired|" + id
        | MsgAccepted(msgId, payload) -> sprintf "msg|%s|%s" msgId payload

    let decode (body: string) =
        match body.Split('|') |> Array.toList with
        | [ "timer-set"; id; wakeAt ] -> Ok(TimerSet(id, wakeAt))
        | [ "timer-fired"; id ] -> Ok(TimerFired id)
        | [ "msg"; msgId; payload ] -> Ok(MsgAccepted(msgId, payload))
        | _ -> Error(sprintf "unknown wait record: %s" body)

    let codec: SubjectHistory.Codec<Wait> = { Encode = encode; Decode = decode }

// --- Mailbox: the deterministic fold of suspension state ---
type Mailbox =
    { PendingTimers: Set<string>
      Seen: Set<string>
      Inbox: string list }

module Mailbox =
    let empty =
        { PendingTimers = Set.empty
          Seen = Set.empty
          Inbox = [] }

    let apply mb (record: SubjectHistory.StoredRecord<Wait>) =
        match record.Body with
        | TimerSet(id, _) ->
            { mb with
                PendingTimers = Set.add id mb.PendingTimers }
        | TimerFired id ->
            { mb with
                PendingTimers = Set.remove id mb.PendingTimers }
        | MsgAccepted(msgId, payload) ->
            // dedupe in the fold: a duplicated admission record collapses to one entry
            if Set.contains msgId mb.Seen then
                mb
            else
                { mb with
                    Seen = Set.add msgId mb.Seen
                    Inbox = mb.Inbox @ [ payload ] }

    let isWaiting mb = not (Set.isEmpty mb.PendingTimers)

let basinName = "test-basin-885234"

let main =
    async {
        let s2 = S2Cli.connect ()
        let basin = s2 |> S2.basin basinName
        let subjectName = Proof.uniq "actor-suspension"
        let subject = SubjectHistory.SubjectId subjectName
        let mutable created = false

        let load () =
            async {
                let! t = SubjectHistory.tail basin subject
                let! mb, _ =
                    SubjectHistory.foldTo basin Wait.codec subject (SubjectHistory.Seq 0L) t Mailbox.empty Mailbox.apply

                return mb
            }

        // admit a message exactly once: skip if already seen, else CAS it in at the tail
        let admit (mb: Mailbox) (msgId: string) (payload: string) =
            async {
                if Set.contains msgId mb.Seen then
                    return false, mb
                else
                    let! t = SubjectHistory.tail basin subject
                    let! r = SubjectHistory.appendExpected basin Wait.codec subject t [ MsgAccepted(msgId, payload) ]

                    match r with
                    | Ok _ ->
                        return
                            true,
                            { mb with
                                Seen = Set.add msgId mb.Seen
                                Inbox = mb.Inbox @ [ payload ] }
                    | Error _ -> return false, mb // lost the admission race; caller should reload
            }

        printfn "subject stream: %s/%s" basinName subjectName

        try
            do! basin |> S2.createStream subjectName
            created <- true

            do!
                Proof.section
                    "durable wait survives restart"
                    (async {
                        let! _ = SubjectHistory.append basin Wait.codec subject [ TimerSet("t-1", "2026-06-28T00:00:00Z") ]

                        let! mb = load ()
                        Proof.check "pending timer is derived from the log" (Set.contains "t-1" mb.PendingTimers)
                        Proof.check "exactly one pending timer" (Set.count mb.PendingTimers = 1)
                        Proof.check "actor is Waiting (no in-memory timer needed)" (Mailbox.isWaiting mb)
                    })

            do!
                Proof.section
                    "wake re-enters and is idempotent"
                    (async {
                        let! _ = SubjectHistory.append basin Wait.codec subject [ TimerFired "t-1" ]

                        let! fired = load ()
                        Proof.check "fired timer clears pending" (not (Set.contains "t-1" fired.PendingTimers))
                        Proof.check "actor is no longer Waiting" (not (Mailbox.isWaiting fired))

                        // at-least-once scheduler delivery: the SAME timer fires twice
                        let! _ = SubjectHistory.append basin Wait.codec subject [ TimerFired "t-1" ]
                        let! again = load ()

                        Proof.check
                            "duplicate TimerFired folds to identical state"
                            (again.PendingTimers = fired.PendingTimers)
                    })

            do!
                Proof.section
                    "inbox admits each message exactly once"
                    (async {
                        let! mb0 = load ()

                        let! admitted1, mb1 = admit mb0 "msg-1" "payload-A"
                        Proof.check "first delivery of msg-1 is admitted" admitted1
                        Proof.check "inbox holds the payload" (List.contains "payload-A" mb1.Inbox)

                        // duplicate delivery of the same caller-owned id — rejected as a retry
                        let! admitted2, mb2 = admit mb1 "msg-1" "payload-A"
                        Proof.check "second delivery of msg-1 is a duplicate (not admitted)" (not admitted2)

                        // and even a raw duplicate record (bypassing admit) folds to one entry
                        let! _ = SubjectHistory.append basin Wait.codec subject [ MsgAccepted("msg-1", "payload-A") ]
                        let! mb3 = load ()

                        Proof.check
                            "inbox holds payload-A exactly once after a duplicate record"
                            (List.length (mb3.Inbox |> List.filter ((=) "payload-A")) = 1)

                        ignore mb2
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
