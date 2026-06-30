# SDD R2 — Substrate on S2 (`Eff.S2`), multi-host, stock Fable

**Supersedes:** §7 and §11 of `durable-fsharp-sdd.md`. **Leaves intact:** the programming-model layer (§3–§6, §9 of R1) — three function kinds, `Durable<'a>`, entities, critical sections, correctness obligations O1/O2. Those were always stock-F#-expressible; the corrections don't touch them.

## Verdict

Two corrections — *(a)* stock F#/Fable only, no Effect/Schema/Thoth; *(b)* S2 is the durable backbone and this is a multi-host distributed system — localize entirely to the substrate. They also **resolve** the one dependency R1 left open:

- **R1 (was "open"): does S2 expose a linearizable append-if-tail or writer-fence?** Resolved. Your client exposes *both*: `S2.appendIfSeqNum` (optimistic CAS) and `S2.appendIfFenced` + `Record.fence` (single-writer), surfaced through `tryAppendWith → Result<AppendAck, S2Failure>` with `SeqNumMismatch`/`FencingTokenMismatch` carrying recovery data. The paper's `commit` *is* a fenced append.

Three load-bearing resolutions, now concrete instead of hand-waved:

1. **Commit = fenced append.** `appendIfFenced token` is S-Commit; `FencingTokenMismatch` is "our claim is stale" (§5.3); `Record.fence` claims ownership.
2. **Two streams per key.** `{key}/log` (single-writer, fenced) + `{key}/in` (multi-writer, unconditional). This dissolves the conditional-append-vs-arrivals tension that one-stream-per-key creates.
3. **Placement is racy; fencing is the safety net.** Multi-host correctness never depends on getting ownership assignment right — only on at-most-one fence holder committing. This is §5.3 realized on `appendIfFenced`.

Implementation status:

- Start with this document as the architecture of record.
- Use `durable-execution-library-sdd.md` as the public API and library packaging
  contract above this substrate.
- Reuse the pure replay semantics as the executable contract, but do not let it defer the S2 binding. The implementation path is: pure replay core → S2 two-stream substrate → relay/timer dispatch.
- Treat `FencingTokenMismatch` precisely: it means the stream's current fencing token is different from ours. That usually means another host has claimed ownership; it does **not** by itself prove the other host has committed user work.
- Pick one log record format per path. Base S2 text records are fine for the first implementation; use bytes + `S2Patterns.producer/consumer` only when chunking/dedup framing is required. Do not write text records and then read them through the bytes-pattern consumer.
- Build upward using the Azure Durable Functions F# samples as a conformance ladder, because they are concrete programs over the paper's implementation model: `HelloSequence` (sequential activity replay), `BackupSiteContent` (`WhenAll` fan-out/fan-in), `Monitor` (deterministic time + timer loop + replay-safe logging), and `PhoneVerification` (`WhenAny` race between external event and timer, with cancellation).

Implemented slices:

- **Tier 1 replay core.** `Durable<'a>` terms, positional `OpId`s, `History`, and deterministic `replay`, with compiled replay-law validation.
- **Tier 1.1 fan-out/fan-in.** `PerformAll` / `Durable.performAll`, modelling the `Task.WhenAll` activity batch shape from `BackupSiteContent`; proof checks positional dispatch, partial replay of only missing completions, and source-order results independent of completion order.
- **Substrate skeleton.** S2 storage keys, two-stream naming, fenced claim/commit, inbox append, and text relay batch helpers.
- **Tier 1.2 `WhenAny`.** `Durable.whenAny` models races over activities,
  timers, and signals. Replay picks the first completed raced operation in
  history order, and losing timers block for cancellation before the workflow
  continues. The compiled durable semantics proof covers signal-vs-timeout,
  timer victory, activity-vs-timer, history-order winner selection, and
  replay-law compatibility.
- **Tier 1.3 deterministic time and replay-safe logging.**
  `Durable.currentTime` records logical time in history before use, and
  `Durable.log` records log emission in history so replay can skip duplicate
  emissions. The compiled proof covers direct time/log replay and a
  Monitor-style timer loop.
- **Tier 1.4 ergonomic API lowering.**
  `durable { ... }`, `Workflow.*`, `Activities.*`, and `DurableTask.*` provide
  the first author-facing F# surface while lowering directly into the proven
  replay terms. The compiled proof checks sequential calls, fan-out,
  `WhenAny`, deterministic time, timers, and replay-safe logging through that
  API.
- **Tier 2.1 S2 two-stream substrate binding.**
  `S2Substrate` now has compiled proof coverage against real ephemeral S2
  streams for `{key}/log` + `{key}/in` provisioning, fence claims, fenced
  commits, replay that ignores fence command records, stale-owner rejection,
  inbox append/read, and relay of `Outgoing` log entries.
- **Tier 2.2 durable stepper.**
  `DurableStepper.plan` translates blocked replay `Need`s into idempotent
  `HistoryEntry<StepRecord>` commits: activity calls, timer creation,
  cancellation, deterministic time, replay-safe logging, and associated
  commands. The compiled proof covers duplicate suppression and a real fenced
  S2 commit/readback path. `StepRecordCodec` is the shared length-prefixed text
  codec for those records, with proof coverage for all cases, separator-bearing
  payloads, and malformed input rejection.
- **Tier 2.3 DurableHost.runOnce.**
  `DurableHost.stepOnce/runOnce` is the first production host work-item
  boundary, still deliberately not a daemon. It reads `{key}/log` through
  `S2Substrate.readLogText`, decodes with `StepRecordCodec`, derives history
  with `DurableStepper.historyFromRecords`, runs `DurableStepper.plan`, commits
  only `Commit` plans through `DurableStepper.commit`, and returns explicit
  `Completed`, `Committed`, `Waiting`, `Deposed`, or `Failed` status. The
  compiled proof covers first activity commit, duplicate suppression on a
  second run, completed-activity advancement to the next need, and stale-fence
  deposition.
- **Tier 2.4 durable command dispatch boundary.**
  `DurableCommandDispatch` scans committed log records, selects only
  `Outgoing(Command _)` records with their source sequence numbers, and writes
  a fenced dispatcher-scoped `CommandDispatchCheckpoint` record after a caller
  has processed the scanned batch. The checkpoint cursor is reconstructed from
  the durable log per dispatcher, so a restarted activity adapter does not
  reselect already-checkpointed activity work and cannot advance a future timer
  adapter's cursor.
  This is still not an activity worker, timer service, or daemon scheduler; it
  is the typed outbox boundary those adapters will sit on. The compiled proof
  covers command-only selection, batch-limit cursor advancement, fail-closed
  decode behavior, checkpoint codec round-trip, dispatcher-scoped cursors,
  duplicate suppression after an S2 checkpoint, and stale-owner checkpoint
  rejection.
- **Tier 2.5 activity command adapter.**
  `ActivityCommandAdapter.runOnce` processes the `"activity"` dispatch cursor:
  it invokes registered handlers only for committed `CallActivity` commands,
  publishes `CompleteActivity` inbox envelopes with source-sequence provenance,
  and checkpoints the activity dispatcher only after durable inbox publication.
  The compiled proof covers committed-command invocation, inbox publication,
  retry suppression after checkpoint, publish-before-checkpoint crash retry
  suppression, missing-handler failure without checkpointing, non-activity
  command skipping, and stale-owner checkpoint rejection.
- **Tier 2.6 inbox fold.**
  `InboxFold.runOnce` admits fresh `{key}/in` arrivals into the fenced log. The
  inbox body shape is `InboxEnvelope`, carrying source id, source sequence
  number, and a `StartWorkflow`, `RaiseSignal`, or `CompleteActivity` message.
  The fold reconstructs its inbox cursor and per-source highwater from log
  records, commits accepted arrivals plus `InboxCheckpoint` in one fenced
  append, and converts `CompleteActivity` arrivals into
  `ActivityCompleted` history events. The compiled proof covers activity
  completion admission, cursor reconstruction after restart, source-message
  dedup, fail-closed malformed inbox decoding, stale-owner rejection, and inbox
  envelope codec round-trip.
- **Tier 2.7 activity completion through inbox composition.**
  `DurableHost.runOnce`, `ActivityCommandAdapter.runOnce`, and
  `InboxFold.runOnce` now compose into the first end-to-end activity completion
  path: host emits a committed `CallActivity`, the adapter publishes a
  `CompleteActivity` inbox envelope, the fold admits it into history, and the
  next host tick advances replay. The compiled proof covers a two-activity
  workflow one narrow tick at a time, host waiting before fold admission,
  absence of direct adapter completion writes, restart after inbox publish
  before fold, and duplicate completion-envelope suppression.
- **Tier 2.8 composed host tick.**
  `DurableHost.runOwnedTick` / `claimAndRunTick` are now the first ergonomic
  host operation over the proven lower layers. A tick claims or uses an owned
  instance, folds inbox arrivals, steps workflow replay, and dispatches
  activity commands, returning a typed `Completed`, `Waiting`, `Advanced`,
  `Deposed`, or `Failed` result with a compact report. The compiled proof
  covers claimed-owner reporting, repeated ticks completing a two-activity
  workflow, retry after step-commit-before-dispatch, typed missing-handler
  failure, and stale-owner rejection.
- **Tier 2.9 timer command adapter.**
  `TimerCommandAdapter.runOnce` consumes committed `ScheduleTimer` /
  `CancelTimer` commands on a dispatcher-scoped timer cursor, publishes due
  `FireTimer` inbox envelopes, and intentionally does not checkpoint past
  future timers. `InboxFold.runOnce` converts accepted `FireTimer` arrivals into
  `TimerFired` history, so `Workflow.sleepUntil` progresses through the same
  inbox/fold path as activity completions. The compiled proof covers no fire
  before deadline, fire at deadline through inbox, cancellation suppression,
  publish-before-checkpoint retry deduplication, and host-tick advancement of
  `sleepUntil`.
- **Tier 2.10 client start admission.**
  `DurableClient.startWith` is now the first client-facing admission path. It
  ensures the instance streams, appends a `StartWorkflow` envelope to
  `{instance}/in`, and returns after durable inbox acknowledgement.
  `InboxFold.runOnce` records accepted starts as `WorkflowStarted`, and
  `DurableHost.runWorkflowTick` uses the folded start record to select the
  registered workflow factory before running the composed host tick. The
  compiled proof covers durable start acknowledgement before host execution,
  duplicate `StartWith` folding to one effective start, registered-workflow
  execution, activity completion after start admission, typed missing-workflow
  failure, and no-start detection.
- **Tier 2.11 client signal admission and delivery.**
  `DurableClient.raiseSignalWith` admits external events through the same
  durable inbox path as starts, activity completions, and timers. The caller
  supplies a source sequence number, making retries explicit and fold-level
  dedup deterministic. `InboxFold.runOnce` records accepted signal envelopes
  but does not assign workflow operation ids. `DurableHost.runOwnedTick`
  delivers an accepted signal only after replay exposes a matching
  `NeedsEvent(Signal _)` or `NeedsRace` signal branch, then commits both
  `SignalReceived` and `SignalDelivered(source, sourceSeqNum, opId)` in one
  fenced append. The delivery marker prevents the same admitted signal from
  satisfying a later wait. The compiled proof covers waiting before signal
  admission, durable signal acknowledgement before delivery, duplicate
  `RaiseSignalWith` folding to one accepted signal, host advancement on
  delivery, completion from the delivered payload, and exactly-once signal
  consumption by source sequence.
- **Tier 2.12 durable status query.**
  `DurableClient.getStatusWith` is the first client observation path. It reads
  the folded durable log for an instance, finds `WorkflowStarted`, resolves the
  registered workflow factory, rebuilds history, and replays without committing
  new records. The result is `InstanceNotFound`, `InstanceRunning`,
  `InstanceWaiting`, or `InstanceCompleted`, with typed failure when the folded
  workflow name is not registered. The status is therefore derived from durable
  history, not a host process cache. The compiled proof covers empty instances,
  folded starts that still need a host step, waiting status from replay,
  completed status from signal-delivered history, and missing workflow failure.
- **Tier 2.13 ergonomic runtime facade.**
  `DurableRuntime.create` closes over an S2 basin, workflow registry, activity
  registry, and host options to expose the first consumable developer surface:
  generated-instance `Client.Start`, deterministic `Client.StartWith`,
  runtime-isolated monotonic `Client.RaiseSignal`, deterministic
  `Client.RaiseSignalWith`, runtime-bound `Client.GetStatus`, `Host.RunOnce`,
  and bounded `Host.RunUntilIdle`. The wrapper still delegates to the proven
  substrate/client/host layers; it does not introduce a daemon scheduler or a
  new commit path. The compiled proof covers generated starts, completion of an
  activity workflow through `RunUntilIdle`, runtime status observation,
  isolated monotonic signal sources, and signal workflow completion through the
  runtime host.

Next slices, still one layer + proof at a time:

- add API polish around registration/configuration, public examples, and
  packaging boundaries so application code can consume the runtime without
  reaching into the lower substrate modules.

One surprising constraint surfaced from your own code — see §1.

---

## 1. Stack: stock F# `Async` + Fable interop, and the DU-codec constraint

Everything above S2 is the same idiom as `Eff.S2` itself: `Async<_>` + `Fable.Core.JsInterop`. No Effect, no Schema, no Thoth. That answers "how far with stock Fable" — **all the way**, with exactly one sharp edge, which your `S2Patterns.Json` already documents:

> `JSON.parse` + `unbox` yields a structurally-correct object with working field access, **but not a real F# record/union instance** — no structural equality, no pattern matching.

This matters because the runtime's wire types — `HistoryEntry` (`Incoming`/`Outgoing`) and `TaskMessage` (5 cases) — are **DUs you `match` on during replay**. Fable compiles a DU case to a tagged object; `JSON.parse` won't reconstruct the tag, so `match` silently falls through. Therefore:

**Hand-roll a tag-dispatched codec for every DU you pattern-match on.** ~30 lines, zero deps. `S2Patterns.Json` is safe *only* for records you field-access and never `match`.

```fsharp
module Codec =
    open Fable.Core            // JS.JSON

    // TaskMessage <-> plain JS object, tag-dispatched. (representative slice; rest mechanical)
    let private msgToObj (g: TaskMessage) : obj =
        match g with
        | DeliverRequest  (k, r)        -> box {| t="req";   key = keyStr k;  req = reqToObj r |}
        | DeliverResponse (d, res, i)   -> box {| t="resp";  d = instStr d;   res = resToObj res; i = reqIdStr i |}
        | StartOrchestration (d, n, c)  -> box {| t="start"; d = instStr d;   n = orchStr n;  c = payStr c |}
        | StartActivity   (i, n, c, d)  -> box {| t="act";   i = reqIdStr i;  n = actStr n;   c = payStr c; d = instStr d |}
        | InitEntityState (k, s)        -> box {| t="init";  key = keyStr k;  s = payStr s |}

    let private msgOfObj (o: obj) : TaskMessage =
        match unbox<string> o?t with
        | "req"   -> DeliverRequest  (keyOf o?key, reqOf o?req)
        | "resp"  -> DeliverResponse (instOf o?d, resOf o?res, reqIdOf o?i)
        | "start" -> StartOrchestration (instOf o?d, orchOf o?n, payOf o?c)
        | "act"   -> StartActivity   (reqIdOf o?i, actOf o?n, payOf o?c, instOf o?d)
        | "init"  -> InitEntityState (keyOf o?key, payOf o?s)
        | t       -> failwithf "Codec: bad msg tag %s" t

    let private entryToObj = function
        | Incoming g -> box {| w="in";  g = msgToObj g |}
        | Outgoing g -> box {| w="out"; g = msgToObj g |}
    let private entryOfObj (o: obj) =
        match unbox<string> o?w with
        | "in"  -> Incoming (msgOfObj o?g)
        | "out" -> Outgoing (msgOfObj o?g)
        | w     -> failwithf "Codec: bad entry tag %s" w

    // For base S2 (text records):
    let encodeEntry (e: HistoryEntry) : string = JS.JSON.stringify (entryToObj e)
    let decodeEntry (s: string)       : HistoryEntry = entryOfObj (JS.JSON.parse s)

    // For the patterns layer (bytes; gives chunking + single-writer dedup framing):
    let encodeEntryBytes : HistoryEntry -> byte[] = encodeEntry >> toUtf8
    let decodeEntryBytes : byte[] -> HistoryEntry = ofUtf8 >> decodeEntry
```

**Key consequence:** `S2Patterns.producer`/`consumer` take a *custom* `serialize`/`deserialize`, so feed them `Codec.encodeEntryBytes`/`decodeEntryBytes` and you get chunking (>1 MiB) + dedup framing **with** correct DU reconstruction. Use the base `S2.append`/`read` + `Codec` for the simple path; reach for `S2Patterns` when payloads are large or you want its retry-dedup. Never use `S2Patterns.Json` for a type you `match`.

---

## 2. Commit, concretely — discharges O1

| Paper (§5/§6) | `Eff.S2` primitive |
|---|---|
| `commit(...)` accepted iff `(g_in, x_pre)` match (§5.1.3) | `appendIfFenced token` (single-writer) — or `appendIfSeqNum tail` (optimistic) |
| "only one worker can commit" under faults (§5.3) | append rejected with `FencingTokenMismatch` / `SeqNumMismatch` |
| claim / rotate the right to commit | `S2.append [ Record.fence token ]` (sets the stream fencing token) |
| typed accept/reject | `tryAppendWith opts recs → Async<Result<AppendAck, S2Failure>>` |
| history `h` records | `Record.text (Codec.encodeEntry e)` |
| `g_out` produced messages | `Outgoing` entries in the same fenced append; delivered by the relay (§6) |
| trim history on continue-as-new (§3.2.3) | `Record.trim seqNum` + `RetentionPolicy.RetainForSecs` |
| read queue `g` / replay `h` | `S2.read (FromSeqNum cursor) n`, `readSession` + `iter`, `WaitSecs` tail, `IgnoreCommandRecords = true` |
| tail / recovery | `S2.checkTail`, `S2.readLast` |

Recovery data is the nice part. `SeqNumMismatch expected` hands back the *new* tail seq num, so an optimistic loser can read `[oldTail, expected)`, fold the gap, and retry without a full reload. `FencingTokenMismatch expected` tells a deposed owner the fencing token S2 expected, so it can stand down cleanly.

---

## 3. Two streams per key — resolves the arrivals/CAS tension

One stream per key (the literal `κ⟨g,x⟩`) breaks on S2: producers (other orchestrations, clients, the relay) are **not** the fenced owner, so they cannot append to the fenced log. And if the log were unfenced, a CAS on its tail would spuriously fail every time an unrelated inbound message arrived during processing — the opposite of the paper's "new messages `g_new` survive the commit" (§5.1.3).

Split it:

```
{key}/log   single-writer, FENCED      = history h  (= x; §6 collapse). Only the owner appends.
{key}/in    multi-writer, UNCONDITIONAL = queue  g.  Anyone may deliver; S2 linearizes arrivals.
```

- "`g_new` survives commit" becomes trivial: the fenced log append never touches `/in`, so concurrent arrivals can't fail it. The owner just advances a **cursor** over `/in`.
- The cursor and the per-source dedup highwater are written *into* `/log` (as part of each committed delta), so replay reconstructs them — O2 holds for the consumption position, not just execution state.
- Per-origin FIFO (§3.3.1) holds because each origin is a single writer appending to `/in` in program order; S2 preserves append order.

---

## 4. Work-item loop (stock F#, on `Eff.S2`)

```fsharp
open Eff      // S2, S2Errors, S2Patterns

type OwnedKey =
    { Key: StorageKey
      Fence: string         // fencing token we currently hold on {key}/log
      Log: S2.Stream        // {key}/log  — single-writer, fenced
      Inbox: S2.Stream }    // {key}/in   — multi-writer, unconditional

/// Claim ownership by rotating the fence. Latest fence wins ⇒ deposes any prior owner.
let claim (host: string) (log: S2.Stream) (inbox: S2.Stream) (key: StorageKey) : Async<OwnedKey> =
    async {
        let token = sprintf "%s/%d" host (nowMillis ())      // unique per claim
        let! _ = log |> S2.append [ S2.Record.fence token ]   // sets the stream fencing token
        return { Key = key; Fence = token; Log = log; Inbox = inbox }
    }

/// One work-item: rehydrate from log, drain new inbound, step forward, commit (fenced).
let runWorkItem (app: App) (o: OwnedKey) : Async<WorkOutcome> =
    async {
        // 1. Replay log ⇒ x_pre + inbox cursor + per-source dedup highwater. (O2: deterministic, Lemma 6.5)
        let! logRecs = o.Log |> S2.readWith { S2.ReadOptions.empty with IgnoreCommandRecords = true }
        let st = replay app o.Key (logRecs |> List.map (fun r -> Codec.decodeEntry r.Body))

        // 2. Inbound past the recorded cursor; drop already-seen (src,seq). (exactly-once-internal)
        let! arrivals = o.Inbox |> S2.read (S2.FromSeqNum st.Cursor) 256
        let fresh = arrivals |> List.filter (fun r -> not (alreadySeen st r))

        // 3. Record-execute until block or complete (the §6.2 record pass; effects suppressed during replay above).
        let r = step app st fresh    // -> { Delta: HistoryEntry list; Status: Blocked | Completed of Result }

        // 4. Commit: ONE fenced append of the delta (Outgoing intents ride along).
        let recs = r.Delta |> List.map (fun e -> S2.Record.text (Codec.encodeEntry e))
        let opts = S2.AppendOptions.none |> S2.AppendOptions.fencingToken o.Fence
        match! o.Log |> S2.tryAppendWith opts recs with
        | Ok _                                          -> return Committed r.Status
        | Error (S2Errors.FencingTokenMismatch winner)  -> return Deposed winner   // lost ownership; discard. Thm 5.3 safe.
        | Error f                                       -> return Failed f
    }
```

The `Deposed` branch is the safety story: a stale or paused owner that resumes and tries to commit is rejected, its work discarded, and a fresh owner re-derives identical state from `/log`. The mismatch only proves our token is stale; it does not prove the new owner has already committed. Observably exactly-once (Thm 5.3) comes from the fact that only the current fence holder can append user-visible log deltas.

---

## 5. Multi-host ownership — the distributed dimension

**Ownership of key K = holding the current fencing token on `{K}/log`.** Nothing else. Claiming rotates the token (§4); the deposed owner discovers it at its next `appendIfFenced`.

**Placement is a policy; fencing is the safety net.** Decide *which host should own K* however you like — correctness does not depend on the answer being unique or current, only on at-most-one fence holder committing (§5.3). Two viable policies:

- **Partitioned assignment (recommended — this is Netherite's shape, which the paper cites in §7).** Hash keys → P partitions; a membership/assignment service maps partitions → hosts. Record assignments in their own S2 stream so the map is itself durable and tailable. A host owns all keys in its partitions and tails their `/in` streams. **Rebalancing** = reassign a partition; the new host writes a new fence on each affected `/log`; the old host is deposed on its next commit; any in-flight work it did is simply discarded and replayed. Split-brain during rebalance is *safe*, just briefly wasteful — exactly the §5.3 guarantee.
- **Dispatch + work-stealing.** A "ready" stream records "K has pending work"; idle hosts read it, `claim` K (fence its `/log`), process. Claim races resolve by fence: the loser's fenced append fails and it moves on. Simpler ownership, more contention.

Start with partitioned assignment; the fence makes either correct. Do **not** build a lease/heartbeat liveness protocol for correctness — the fence already provides at-most-one-writer without timing assumptions (this is precisely the property the paper contrasts favorably against storage leases in C4, §1).

---

## 6. Outbox relay + exactly-once-internal (no multi-stream transaction)

S2 has no cross-stream atomic commit, so `enq(g_out, …)` distributing to many destination queues (§5.1.1) is reconstructed:

1. **Intents are durable at commit.** Produced messages are `Outgoing` entries inside the fenced `/log` append (§4 step 4). The instant the owner commits, the intents are persisted.
2. **Relay delivers.** Per `/log`, a relay reads committed `Outgoing` entries and appends each to its destination `{dest}/in` with provenance headers. If the log is written as base text records, use base S2 reads + `Codec.decodeEntry`. If it is written through `S2Patterns.producer Codec.encodeEntryBytes`, use `S2Patterns.consumer Codec.decodeEntryBytes`. Do not mix the two formats.
3. **Destination dedups.** Inbox is *multi-writer*, so the `S2Patterns` single-writer dedup does **not** cover it; dedup is app-level on `(src, seq)` carried in headers. The owner records a per-source highwater in `/log`, so replay rebuilds it and skips re-delivered intents.

```fsharp
/// Deliver committed Outgoing intents to destination inboxes, idempotently.
let relay (o: OwnedKey) (inboxOf: StorageKey -> S2.Stream) : Async<unit> =
    async {
        // Text-record path. For the patterns path, write bytes with S2Patterns.producer
        // and read bytes with S2Patterns.consumer instead.
        let! sess = o.Log |> S2.readSession { S2.ReadOptions.empty with WaitSecs = Some 30 }
        return!
            sess |> S2.iter (fun rec ->
                match Codec.decodeEntry rec.Body with
                | Outgoing g ->
                    let dest = inboxOf (destinationKey g)
                    let hdrs = [ "src", S2.streamName o.Log; "seq", string rec.SeqNum ]
                    dest |> S2.append [ S2.Record.textWith hdrs (Codec.encodeMsg g) ] |> Async.Ignore
                | Incoming _ -> async.Return ())
    }
```

Re-running the relay after a crash re-delivers the *same* intent with the *same* `(src, seq)` → destination dedups → at-most-once effective delivery. Internal messages are exactly-once (Thm 5.3) without any distributed transaction.

---

## 7. Lifecycle on S2 — all native to your client

| DF semantics | `Eff.S2` mechanism |
|---|---|
| Entity auto-start on first delivery (§4.4.1, `AutoStart`) | `BasinConfig.CreateStreamOnAppend = true` — producer's append to `{k}/in` creates it |
| Collect uninteresting entity (§4.4.1) | `StreamConfig.DeleteOnEmptyMinAgeSecs` — empty, idle stream auto-deleted |
| `continue_as_new` truncates history (§3.2.3, `RStep-Cont`) | append restart marker, then `Record.trim restartSeq` on `/log` |
| Bound replay cost / eternal orchestration | `RetentionPolicy.RetainForSecs` (time) or trim-after-checkpoint (size) |
| Sub-orchestration keeps parent history small (§3.5) | child gets its own `{sub}/log`; parent `/log` records only the call + result |

---

## 8. Timers and scheduled signals across hosts

A durable timer (`create_timer`, §3.2.3) and a scheduled signal (§3.3.4) both fire at wall-clock time on *some* host. Realize as: a timer is a record carrying its fire-time; a per-partition **timer dispatcher** tails a pending-timers stream and, at fire-time, delivers the message to `{target}/in`. S2's timestamping plus `ReadOptions.UntilTimestamp` and `WaitSecs` support the "wake near time T" read. Scheduled signals bypass entity FIFO (§3.3.4), so they land via a separate lane, consistent with R1 §6.2's note that a single ordered `/in` is insufficient for priority delivery. Dispatcher placement follows the same fencing discipline (own the dispatcher per partition; fence its progress stream).

---

## 9. Edits to fold into `durable-fsharp-sdd.md`

- **§3.1** — drop the eff-sharp/Schema framing; "stock F# `Async` + Fable interop." Keep the no-HKT point (still true and now the whole stack).
- **§5** — `Durable<'a>` encoding stands verbatim; remove the "eff-sharp already does this" aside. The defunctionalized free encoding is stock F#.
- **§7** — replace wholesale with this document.
- **§11** — codecs are **hand-rolled tag-dispatched DU codecs** (this doc §1), not Schema. `S2Patterns.Json` is field-access-only; never for matched types.
- **§13** — remove R1 from open questions (resolved, this doc §2). New open items, all distribution-policy rather than correctness: *(i)* partition-assignment service (durable assignment stream + rebalance trigger), *(ii)* timer-dispatcher placement, *(iii)* multi-writer-inbox dedup-highwater compaction (when does a source's highwater get trimmed from `/log`), *(iv)* `/in` retention vs. slow consumers (backpressure / trim policy).

---

*Grounded against `eff-firegrid/src/S2/{Client,Patterns,Errors}.fs` @ `0631541`. Commit/fencing semantics: `S2.appendIfFenced`, `S2.appendIfSeqNum`, `Record.fence`, `tryAppendWith`, `S2Errors.S2Failure`. Messaging: `S2Patterns.{producer,consumer}` (custom serializer ⇒ DU-safe), `S2Patterns.Json` (field-access-only). Paper refs: Burckhardt et al., OOPSLA 2021, §5.1/§5.3/§6; Netherite partition/commit-log shape per §7 + [Burckhardt et al. 2021].*
