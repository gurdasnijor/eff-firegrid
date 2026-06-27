# Eff Firegrid — Durable Actors SDD (companion to `foundational-sdd.md`)

## Status

Design + script-first spikes. The base SDD (`foundational-sdd.md`) builds
`L0 S2 client → L1 SubjectHistory → L2 StateView → L3 KvStore`. That track
validates the *fold-a-log-into-state* mechanism with a KV demo. This companion
continues **up** from L1 to the actual summit named in the base SDD's *Deferred*
list: **discrete durable execution with stateful actors** — an Orleans/Temporal-
style runtime where addressable actors run multi-step, crash-resumable handlers
with exactly-once effects and durable waits.

Everything here is *derived* from L1, not bolted on. The discipline of this
document: **each layer is forced into existence by a concrete bug you hit using
only the layer below, and each new operation is implementable as a short program
over the layer below plus one or two new record types in the same single
stream.** If a layer cannot be written in terms of the layer beneath it, it does
not belong in the tower. The only storage that ever exists is one S2 subject
stream of typed records; every "distributed-systems feature" (leases, exactly-
once, timers, virtual actors) is an *emergent reading* of that log.

## Start here — the API you actually write

The whole tower exists to make these two snippets work. This is the forest;
everything after it is the trees. The ergonomic surface lives in
`src/Foundation/Actor.fs` (module `Durable`); a runnable demo is
`scripts/durable-demo.fsx` (`npm run demo`).

**A stateful actor** — persisted state via a reducer over a command log. You
serialize commands, never state; crash-survival is free because state is just a
fold of the log:

```fsharp
type Cmd = Deposit of int | Withdraw of int

let account =
    Durable.actor "account" 0
        (fun balance cmd ->
            match cmd with
            | Deposit n  -> balance + n
            | Withdraw n -> max 0 (balance - n))
        accountCmdCodec

do!    Durable.send  basin account (Durable.Id "acct-1") (Deposit 100)
let! b = Durable.state basin account (Durable.Id "acct-1")   // 100, rebuilt from the log
```

**A durable workflow** — an imperative body that survives crashes and durable
sleeps. Each `Step` runs exactly once across replays; `Sleep` suspends and
resumes on its deadline:

```fsharp
let order =
    Durable.workflow "order" (fun sku ctx -> async {
        let! reservation = ctx.Step("reserve", fun () -> async { return reserve sku })
        let! _payment    = ctx.Step("charge",  fun () -> async { return charge sku })
        do!                ctx.Sleep 86_400_000.0          // 24h, durable
        let! tracking    = ctx.Step("ship",    fun () -> async { return ship reservation })
        return tracking
    })

let! tracking = Durable.run basin order (Durable.Id "order-123") "widget-42"
```

That is the entire author-facing surface: `actor` / `send` / `state` / `remove`
and `workflow` / `run`. No journals, folds, replay, ownership, or codecs-for-
state appear in application code. The rest of this document explains, layer by
layer, how that surface is built from one S2 stream — and the `actor-0N` spikes
prove each underlying property. **You read the demo to *use* the library; you
read the spikes to *trust* it.**

## The one substrate

All layers are built from exactly these L1 operations (`SubjectHistory`):

```fsharp
appendExpected : Version -> 'r list -> Async<Result<Version, AppendFailure<'r>>>  // CAS on tail
append         : 'r list -> Async<Version>                                        // unconditional
foldTo         : Seq -> Version -> 'state -> ('state -> StoredRecord<'r> -> 'state) -> Async<'state * Version>
tail           : Async<Version>
openCursor / tryNext / closeCursor                                                // streaming fold
```

plus, for L4 only, two **raw L0 primitives** that L1 deliberately does not yet
expose (the base SDD's C1 note: "Fenced owner writes are deferred"):

```fsharp
S2.Record.fence    : string -> Record                 // command record: sets the stream fencing token
S2.appendIfFenced  : string -> Record list -> Stream -> Async<AppendAck>   // reject if token != current
S2Errors.FencingTokenMismatch                          // the rejection, classified
```

## Dependency map (continuing the base SDD)

```
L8  DurableActor authoring + Directory   (ergonomic)   depends on: L7
L7  DurableActor runtime (decider loop)                depends on: L4, L5, L6
L6  Suspension: Timers + Inbox                          depends on: L5
L5  EffectLedger (durable steps)                        depends on: L4, L1
L4  Ownership (lease / fence)                           depends on: L1, raw L0 fencing
---------------------------------------------------------------- base SDD ----
L3  KvStore        L2  StateView        L1  SubjectHistory        L0  S2 client
```

`StateView` (L2) and the actor runtime (L7) are siblings, not parent/child: both
fold a log into state. StateView's `apply` is a *passive* fold; the actor
runtime's handler is an *active decider* that also emits the next records. The
runtime is "StateView that writes back."

## Version invariant (inherited, load-bearing)

`Version n` = the seq num of the next unwritten record = the count of committed
records. `Seq` is 0-based; record `k` lives at `Seq k`; folding `[from, until)`
applies records `from .. until-1`. Every layer here preserves this.

---

## Capability C4: Ownership (lease / fence)

Layer: L4. Spike: `scripts/actor-01-ownership.fsx`.

### The bug it kills

With only L1, two hosts can both `foldTo` the log to state and both `append`.
`appendExpected` (CAS-on-tail) makes them *race* — one append wins each round —
but **both stay alive and interleave writes.** That is not one actor; it is two
hosts fighting over one log, and the loser's in-memory fold is silently stale.
A "stateful actor" requires *single-writership*, and it must be enforced by the
**store**, not by an in-memory lock (which a GC pause or partition defeats).

### Construction

Two independent mechanisms compose into ownership:

1. **Admission (who becomes owner) — pure L1.** A `Claimed (owner, epoch)`
   record admitted with `appendExpected expectedTail`. At a given tail exactly
   one claimant wins the CAS; the loser gets `AppendFailure.Conflict` whose
   `Conflicting` is the winner's record. Mutual exclusion of *admission* needs
   nothing but L1.

2. **Enforcement (the loser cannot write even if it never learned it lost) —
   raw L0 fencing.** The winner installs a fresh fencing **token** via
   `S2.Record.fence token`; every owner write is `S2.appendIfFenced token`. When
   a later owner installs a new token, the previous owner's fenced writes are
   rejected by the store with `FencingTokenMismatch`, *durably and without the
   demoted owner observing anything.* This is the property that makes
   **owner-local reads linearizable without `checkTail`**: if your fenced write
   succeeded, you are the tail.

### Proposed surface (promotion target)

```fsharp
type Epoch = Epoch of int64
type Lease = { Subject: SubjectId; Owner: string; Token: string; ClaimedAt: Version }

module Ownership =
    val claim       : S2.Basin -> SubjectId -> owner: string -> Async<Result<Lease, AppendFailure<_>>>
    val appendOwned : S2.Basin -> Lease -> Codec<'r> -> 'r list -> Async<Result<Version, S2Errors.S2Failure>>
    val ownerTail   : Lease -> Version          // owner-local; no round-trip
```

Promotion note: native fence records are *command records*. `SubjectHistory`
folds with `ReadOptions.IgnoreCommandRecords = false`, so a fenced stream cannot
be folded by the current typed codec. Promotion of L4 into `src/Foundation/`
must (a) add a fenced `appendOwned` to `SubjectHistory`, and (b) flip its read
path to `IgnoreCommandRecords = true` so folds skip fence/trim markers. Until
then, **L5–L8 carry a logical `epoch` in a normal typed record** (portable, folds
cleanly) and rely on L1 CAS for admission; `actor-01` proves the native-fence
hardening separately so the promotion is de-risked.

### Properties proven by the spike

- **P1 admission is mutually exclusive** — two claims at the same expected tail:
  one `Ok`, one `Conflict` exposing the winner.
- **P2 fencing durably demotes** — a previously valid owner's `appendIfFenced`
  fails with `FencingTokenMismatch` after a takeover, with no notification.
- **P3 owner-local read is the tail** — the current owner's last `End.SeqNum`
  equals `checkTail`; a follower needs `checkTail` + fold, the owner does not.

---

## Capability C5: EffectLedger (durable steps)

Layer: L5. Spike: `scripts/actor-02-durable-steps.fsx`.

### The bug it kills

The owner runs **side effects** (charge a card, call an API) and crashes land
*mid-handler*. If the handler "charges, then appends `OrderPlaced`," a crash
between the two makes the recovering owner re-fold, see no `OrderPlaced`, and
charge **again**. The log records *decisions* but not *effects in flight*.

### Construction

Append the **intent** before the effect and the **result** after, and skip on
replay:

```fsharp
type Step<'eff,'res> =
    | EffectRequested of id: int64 * eff: 'eff
    | EffectCompleted of id: int64 * res: 'res

// id = number of EffectRequested folded so far  → deterministic, regenerated identically on replay
let step ledger (id: int64) (eff: unit -> Async<'res>) = async {
    match Map.tryFind id ledger.Completed with
    | Some r -> return r                                 // replay: never re-run; return logged result
    | None ->
        do! appendOwned [ EffectRequested(id, descr) ]   // intent
        let! r = eff ()                                  // THE single non-deterministic point
        do! appendOwned [ EffectCompleted(id, r) ]       // result
        return r }
```

`id` is **derived from the fold** (count of requests), so a recovering owner
regenerates the same id sequence and asks the same "have I done effect 7?"
question. The fold reconstructs `Completed : Map<id,res>` and
`Pending = requested − completed`.

### Properties proven by the spike

- **P1 effectively-once** — a completed effect is *not* re-run after a re-fold;
  the recorded result is returned (observable via a side-effect counter that
  stays at 1).
- **P2 at-least-once on crash-before-result** — an `EffectRequested` with no
  `EffectCompleted` (crash after intent) is re-executed on recovery and then
  completed. (This is *why* effects must be idempotent — surfaced explicitly.)
- **P3 deterministic ids** — re-folding the same log regenerates the identical
  next-id; no effect is skipped or duplicated by id drift.
- **P4 result fidelity** — the value handed to the handler on replay equals the
  value originally computed and journaled.

---

## Capability C6: Suspension — Timers + Inbox

Layer: L6. Spike: `scripts/actor-03-suspension.fsx`.

### The bug it kills

The handler must **wait** — "sleep 24h, then remind" or "await approval." You
cannot block a host for 24h; it will redeploy. So *waiting itself* must be
durable and must **release the host**.

### Construction

A wait is a record plus *returning from the handler*. The actor re-enters only
when a new record resolves the wait:

```fsharp
type Wait<'msg> =
    | TimerSet   of timerId: string * wakeAtMs: float
    | TimerFired of timerId: string
    | MsgAccepted of msgId: string * 'msg          // inbound signal/call, admitted once

// fold reconstructs: PendingTimers : Set<timerId>, Inbox : 'msg list, Seen : Set<msgId>
```

- **Durable sleep** = `TimerSet`; the fold reports the actor `Waiting`; a
  `TimerFired` clears the pending timer and re-enters. The scheduler that emits
  `TimerFired` is *itself a C4–C5 actor* whose state is "pending timers" — it
  introduces no new machinery, it dog-foods the tower.
- **Inbox dedupe** = admit a `MsgAccepted msgId` only if `msgId ∉ Seen`, using
  `appendExpected` so two hosts delivering the same external message converge to
  one admission (the caller owns `msgId`; this is the retry-identity the base
  SDD's C1 deliberately pushed up here).

### Properties proven by the spike

- **P1 durable wait** — `TimerSet` survives a re-fold; the actor is `Waiting`
  purely from the log, with no in-memory timer.
- **P2 re-entrancy** — `TimerFired` clears the pending timer and resumes; status
  derived solely from the fold.
- **P3 idempotent wake** — a duplicated `TimerFired` (at-least-once scheduler
  delivery) folds to the same state as a single one; no double-resume.
- **P4 inbox dedupe** — the same `msgId` delivered twice is admitted once; the
  folded inbox holds the payload exactly once.

---

## Capability C7: DurableActor runtime (the decider loop)

Layer: L7. Spike: `scripts/actor-04-runtime.fsx`.

### What it is

The generalization of StateView: a loop that folds the journal to rebuild
`(state, ledger, waits)`, runs a **pure decider** `decide : state -> Event ->
Decision`, journals the emitted effects/timers via L5/L6, executes pending
effects exactly once, and acks. It owns the subject via L4. On crash, *any* host
runs the same loop, folds the same log, reaches the same decisions, and skips
completed effects.

```fsharp
type Decision<'eff> =
    | Continue of emit: 'eff list
    | Sleep    of ms: float
    | Done     of result: string

val activate : S2.Basin -> SubjectId -> decide -> Async<Outcome>
```

### The determinism contract (state this as loudly as the Version Invariant)

`decide` must be a pure function of `(folded state, next event)`. **All**
nondeterminism — wall clock, randomness, IO — flows through journaled
effects/timers, never read directly inside `decide`. This is the single rule
that makes crash/resume produce identical histories.

### Properties proven by the spike

A concrete workflow actor — `reserve → charge → sleep → ship → done` — is driven
by a runtime that **crashes (discards memory and re-folds) after every journal
append**:

- **P1 crash/resume equivalence** — despite crash injection at every boundary,
  the workflow reaches `Done` exactly once and the final folded state equals a
  straight-through run on a second stream.
- **P2 no duplicated effects** — each effect's side-effect counter is exactly 1;
  `reserve`/`charge`/`ship` each run once across all the crash/resume cycles.
- **P3 journal determinism** — the sequence of committed record bodies is
  identical between the chaotic run and the clean run (the log is a deterministic
  function of inputs, not of crash timing).

---

## Capability C8: Ergonomic authoring + Directory (the summit)

Layer: L8. Spike: `scripts/actor-05-ergonomic.fsx`.

### What it is — IMPLEMENTED in `src/Foundation/Actor.fs` (module `Durable`)

This layer is no longer a sketch: it is the real module behind the *Start here*
snippets. Two surfaces over one substrate:

```fsharp
// stateful actor — state is a fold of a command log (you serialize commands, not state)
val actor    : kind:string -> initial:'state -> ('state -> 'cmd -> 'state) -> Codec<'cmd> -> Actor<'cmd,'state>
val send     : S2.Basin -> Actor<'cmd,'state> -> Id -> 'cmd -> Async<unit>
val state    : S2.Basin -> Actor<'cmd,'state> -> Id -> Async<'state>
val remove   : S2.Basin -> Actor<'cmd,'state> -> Id -> Async<unit>

// durable workflow — an imperative body driven by deterministic replay
type Ctx = { Step: string * (unit -> Async<string>) -> Async<string>; Sleep: float -> Async<unit> }
val workflow : kind:string -> (input:string -> Ctx -> Async<string>) -> Workflow
val run      : S2.Basin -> Workflow -> Id -> input:string -> Async<string>
```

The `actor` surface is C2 StateView generalized to an arbitrary reducer (the KV
demo is just the `Put/Delete` special case). The `workflow` surface is C7's
decider wrapped in an imperative `Ctx`. It is implemented by **replaying the
handler from the top on every turn**:
`Ctx.Step` memoizes through the C5 ledger (a completed step returns its logged
result without re-running its thunk); `Ctx.Sleep` either observes a fired timer
and proceeds, or journals `TimerSet` and raises a `Suspend` the runtime catches
to end the turn. This *is* how durable-functions engines work, and it composes
C4–C7 without exposing them.

### Directory (addressing → virtual actors)

`actorId → subjectId` is a convention (`subject = hash actorId`) or a row in a
directory actor (itself C4–C8). "Send to X" = admit a `MsgAccepted` to X's
subject (C6). "Activate X" = if no live claim, `Ownership.claim` and run the
loop (C4 + C7). Virtual/Orleans-style actors *fall out of* "a message is a
record + claim-on-demand"; there is no new primitive.

### Properties proven by the spike

- **P1 ergonomic completion** — a workflow authored with imperative `Ctx.Step` /
  `Ctx.Sleep` runs to completion across a real suspend/resume.
- **P2 invariants preserved** — each `Ctx.Step` side effect runs exactly once
  despite the handler being replayed top-to-bottom on every turn (memoization via
  the C5 ledger).
- **P3 equivalence with the hand-rolled decider** — the journal produced by the
  ergonomic actor is identical to the C7 decider's journal for the same logic:
  the nice API is a faithful refinement, not a new semantics.

The shipped `Durable` module folds this into one library; `npm run demo` runs
`scripts/durable-demo.fsx`, the ~90-line application-author view of all of it.

---

## Build order (script-first, then promote)

Each capability is proven in a Fable spike against real ephemeral S2 streams
*before* any typed `src/Foundation/*.fs` module is written — matching the base
SDD's methodology.

| # | Spike | Capability | Promotes to |
|---|-------|-----------|-------------|
| 1 | `scripts/actor-01-ownership.fsx`     | C4 Ownership / fence | `src/Foundation/Ownership.fs` |
| 2 | `scripts/actor-02-durable-steps.fsx` | C5 EffectLedger      | `src/Foundation/EffectLedger.fs` |
| 3 | `scripts/actor-03-suspension.fsx`    | C6 Timers + Inbox    | `src/Foundation/Suspension.fs` |
| 4 | `scripts/actor-04-runtime.fsx`       | C7 DurableActor loop | `src/Foundation/DurableActor.fs` |
| 5 | `scripts/actor-05-ergonomic.fsx`     | C8 Authoring + Directory | `src/Foundation/Actor.fs` ✅ shipped |

The C8 surface is already promoted: `src/Foundation/Actor.fs` (module `Durable`)
is the ergonomic library, and `scripts/durable-demo.fsx` (`npm run demo`) is the
application-author view. C4–C7 remain script-proven and are promoted to typed
`src/Foundation/*.fs` modules next (the `Durable` workflow path currently inlines
a simplified C5/C6 replay engine; ownership/fencing from C4 hardens `send`/`run`
once promoted).

Run any spike with its `npm run` target (added to `package.json`), e.g.
`npm run actor:ownership`, or directly:
`dotnet fable scripts/actor-01-ownership.fsx --outDir build_script --runScript`.
Each prints `N passed, M failed` and exits non-zero on any failure, so they slot
into the proof runner alongside `foundation-00`.

## Acceptance criteria

1. C4 proves single-writer admission (L1 CAS) **and** durable fencing (raw L0).
2. C5 proves effectively-once on replay and at-least-once on crash-before-result.
3. C6 proves durable waits, idempotent wakes, and inbox dedupe.
4. C7 proves crash/resume journal equivalence for a multi-step workflow.
5. C8 proves the imperative authoring surface preserves every lower invariant and
   is journal-equivalent to the C7 decider.

## Deferred (beyond this companion)

- Snapshot/checkpoint records + race-safe trim to bound fold cost on long-lived
  actors (base SDD's *checkpoint trimming*, *Yjs-style checkpoint races*).
- Large effect payloads stored by reference (keep the journal lean).
- Cross-actor sagas / multi-subject command barriers (no multi-stream txn in S2).
- Request/response correlation across actors over reply subjects.
- Lease renewal / TTL self-demotion timing under real clock skew.
