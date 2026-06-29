# Eff Firegrid Foundational SDD

## Status

Happy-path foundation design, aligned to the S2 KV demo.

This document intentionally excludes output-producing invocation guarantees,
operation ledgers, coordination claims, leases, fencing, checkpoint trimming,
timers, schedules, workflow APIs, and full proof-runner work. The goal is to
build the smallest real S2-backed state-view path we can validate with F#/Fable
compiled proof-runner properties.

## Objective

Build the happy-path substrate for stream-derived state on S2:

```text
S2 client
  -> SubjectHistory
    -> Deterministic fold
      -> StateView
        -> KvStore
```

`StateView` is the S2 KV-demo-style orchestrator: one host-local loop owns local
state, tails an S2 stream, applies records into that state, serves eventual
reads immediately, and serves strong reads by waiting until local state has
applied the current S2 tail.

`InvocationRuntime` is deferred. Pulsar-style at-least-once/effectively-once
processing belongs to output-producing consumers, not to the in-memory state
view path.

Sources:

- <https://s2.dev/blog/kv-store#part-2-designing-a-replicated-kv-store>
- <https://github.com/s2-streamstore/s2-kv-demo/blob/main/src/main.rs>
- <https://pulsar.apache.org/docs/4.2.x/functions-concepts/#processing-guarantees-and-subscription-types>
- <https://pulsar.apache.org/docs/4.2.x/functions-concepts/>
- <https://s2.dev/blog/durable-yjs-rooms>

## Dependency Map

```text
L3  KvStore
    depends on: L2 StateView
    purpose:    first concrete proof of the S2 KV pattern

L2  StateView
    depends on: L1 SubjectHistory and L0 pull-based read sessions
    purpose:    fold one typed stream into local state and serve reads

L1  SubjectHistory
    depends on: L0 S2 client
    purpose:    one authoritative subject history over one S2 stream

L0  S2 client
    exists:     src/S2/Client.fs and src/S2/Patterns.fs
```

Deferred layers:

- output-producing `InvocationRuntime`
- `EffectivelyOnce`
- operation ledger
- coordination claims
- fencing
- checkpoint and trim
- Yjs-style checkpoint races
- timers and wake indexes
- workflow/object/service authoring APIs

## Reference Model

The S2 KV demo is the reference for `StateView` and `KvStore`.

What it establishes:

- No progress stream exists for a view.
- State is an in-memory map plus `applied_state`, an exclusive upper bound.
- Startup begins from `recover_from` and replays the stream.
- Writes append mutation records to S2.
- Writes acknowledge when S2 acknowledges the append, before local apply.
- That fast write ack is valid because `put` and `delete` do not return prior
  values.
- Eventual reads return current local state.
- Strong reads call `checkTail`, then wait until local `AppliedTail >= tail`.
- Pending strong reads are keyed by required tail and drained as records apply.
- A bus-stand optimization can batch concurrent `checkTail` calls later.

Yjs rooms add the later checkpoint/fencing story:

- load checkpoint plus sequence metadata
- replay records from checkpoint sequence to tail
- checkpoint/trim races require fencing
- trim and fence reset should be committed consistently

Those are deferred from the happy path.

## Version Invariant

Use one convention everywhere:

```text
Version = exclusive upper bound = next sequence number
```

Examples:

- empty subject tail is `Version 0`
- record `Seq 0` is applied when `AppliedTail >= Version 1`
- S2 append ack `End.SeqNum` is the append batch end-exclusive version
- S2 `checkTail` returns the current end-exclusive version
- strong read waits for `AppliedTail >= checkedTail`

Do not use `Version` to mean "last applied record sequence."

## Capability C0: S2 Client Pull Cursor

Layer: L0.

The KV-demo orchestrator races command handling against the next tail record.
That requires a pull-one read cursor. The existing `S2.iter` is useful for
simple consumption, but it runs until completion, and a tailing read session may
not complete. The existing `S2.take` cancels the session after a bounded read.

Required production surface:

```fsharp
type ReadCursor

module S2 =
    val readCursor : ReadOptions -> Stream -> Async<ReadCursor>
    val tryNext : ReadCursor -> Async<ReadRecord option>
    val closeReadCursor : ReadCursor -> Async<unit>
```

This is load-bearing for `StateView`.

## Capability C1: SubjectHistory

Layer: L1.

`SubjectHistory` is the typed durable-kernel boundary over S2. It models one S2
stream as one authoritative durable subject history. `StateView`, `KvStore`, and
later durable-work kernels consume typed `SubjectHistory` records, not raw SDK
records.

### Existing Lower Surfaces

`src/S2/Client.fs` provides:

- `S2.append`
- `S2.appendIfSeqNum`
- `S2.tryAppendWith`
- `S2.read`
- `S2.checkTail`
- `S2.appendSession`
- `S2.readSession`
- `S2.readCursor`
- `S2.tryNext`

`src/S2/Patterns.fs` provides typed producer/consumer wrappers with chunking,
framing, reassembly, and dedupe headers. It is useful for large messages or
message-level retry semantics, but it is not the foundation substrate.

Happy-path decision:

- foundation `SubjectHistory` uses one durable record per raw S2 record with an
  explicit codec
- `S2Patterns` is opt-in later for large event payloads
- `StateView` should call `SubjectHistory`, not `S2Patterns` directly
- list-returning "read all from seq" helpers are not part of the kernel surface;
  replay/materialization is cursor/fold based

### Proposed Surface

```fsharp
type SubjectId = SubjectId of string
type Seq = Seq of int64
type Version = Version of int64

type StoredRecord<'record> =
    { Seq: Seq
      Body: 'record }

type ConflictRecord<'record> =
    | Found of StoredRecord<'record>
    | Unavailable
    | LookupFailed of message: string

type AppendConflict<'record> =
    { Expected: Version
      Actual: Version
      Conflicting: ConflictRecord<'record> }

type AppendFailure<'record> =
    | Conflict of AppendConflict<'record>
    | Failed of S2Errors.S2Failure

type Codec<'record> =
    { Encode: 'record -> string
      Decode: string -> Result<'record, string> }

type Cursor<'record>

module SubjectHistory =
    val tail :
        S2.Basin -> SubjectId -> Async<Version>

    val append :
        S2.Basin -> Codec<'record> -> SubjectId -> 'record list ->
            Async<Version>

    val appendExpected :
        S2.Basin -> Codec<'record> -> SubjectId -> Version -> 'record list ->
            Async<Result<Version, AppendFailure<'record>>>

    val openCursor :
        S2.Basin -> Codec<'record> -> SubjectId -> Seq ->
            Async<Cursor<'record>>

    val tryNext :
        Cursor<'record> -> Async<Result<StoredRecord<'record> option, string>>

    val foldTo :
        S2.Basin -> Codec<'record> -> SubjectId -> Seq -> Version ->
        'state -> ('state -> StoredRecord<'record> -> 'state) ->
            Async<'state * Version>
```

`appendExpected` is for unowned or contended admission records: create-run,
external delivery dedupe, and other single-index claims. It reports the winning
record at the expected sequence when that exact record is still readable, but it
does not infer idempotency from body equality. A caller that wants retry
classification must put a unique attempt/command id in the record body and
interpret conflicts at its own semantic layer.

Non-conflict S2 failures stay typed as `S2Errors.S2Failure`; the foundation layer
must not turn fencing-token loss or transient S2 errors into generic exceptions.

`append` is for the owner-style path after a lane owner/fence has already been
established. Fencing and owner-local reads are a later capability; this C1 only
keeps the physical append/fold pieces honest.

### Compiled Proof

Proof:

```text
src/Proofs/FoundationSubjectHistoryProof.fs
```

Prove against real ephemeral S2 streams:

- new subject starts at `Version 0`
- append at expected version succeeds and advances the subject tail
- stale append conflict returns expected version, actual version, and the
  exact conflicting committed record when available
- same-body stale append is still classified as conflict, not idempotent retry
- different stale append is classified as conflict
- deterministic fold applies committed records in order through a target version
- follower read barrier is `tail` plus `foldTo` to that version

Notes:

- This proof deliberately does not cover fenced owner-local reads, lease expiry,
  snapshot equivalence, or cross-subject command barriers.
- S2 `matchSeqNum` and S2 fencing tokens are different primitives. The first
  proof covers expected-sequence admission. Fenced owner writes are deferred.

## Capability C2: StateView

Layer: L2.

`StateView` is the KV-demo orchestrator generalized over event and state types.
It is not an `InvocationRuntime` specialization and it has no progress stream.

### Proposed Surface

```fsharp
type ReadConsistency =
    | Eventual
    | Strong

type ViewState<'state> =
    { State: 'state
      AppliedTail: Version }

type StateView<'record, 'state>

module StateView =
    val start :
        S2.Basin ->
        Codec<'record> ->
        SubjectId ->
        recoverFrom: Seq ->
        initial: 'state ->
        apply: ('state -> StoredRecord<'record> -> 'state) ->
            Async<StateView<'record, 'state>>

    val read :
        ReadConsistency -> StateView<'record, 'state> ->
            Async<ViewState<'state>>

    val stop : StateView<'record, 'state> -> Async<unit>
```

Internal loop:

- open typed subject-history cursor from `recoverFrom`
- fold every pulled record into local state
- set `AppliedTail = record.Seq + 1`
- serve eventual reads from current local state
- for follower strong reads, call `SubjectHistory.tail`, then wait until
  `AppliedTail >= checkedTail`
- keep pending strong reads in required-tail order
- drain pending reads as `AppliedTail` advances

Write ack invariant:

- writes acknowledge on durable S2 append, not on local apply
- this is valid only for commands like `put` and `delete` that do not return
  prior state
- operations that must return prior state need a different path
- owner-local linearizable reads are deferred until keyed ownership/fencing is
  introduced; they require self-demotion when the lease expires

### Compiled Proof

Next proof:

```text
src/Proofs/FoundationStateViewProof.fs
```

Prove:

- local fold state updates from tail records
- eventual read can observe current local state
- strong read calls `checkTail` and waits for `AppliedTail`
- second host can append; first host strong-read catches up to that append
- terminal cursor/decode/apply failures fail reads and stop cleanly

The write-ack-before-local-apply window belongs to the first API that owns
writes. `StateView` has no write method; C2 writes in scripts are direct
`SubjectHistory.append` calls used to drive the follower. Prove that window in
C3 `KvStore.put/delete`, where the store can acknowledge the append before its
local `StateView` has applied it.

## Capability C3: KvStore

Layer: L3.

`KvStore` is the first concrete `StateView`, aligned with the S2 KV demo.

### Proposed Surface

```fsharp
type KvEvent<'key, 'value> =
    | Put of key: 'key * value: 'value
    | Delete of key: 'key

type KvStore<'key, 'value>

module KvStore =
    val start :
        S2.Basin ->
        Codec<KvEvent<'key, 'value>> ->
        SubjectId ->
        recoverFrom: Seq ->
            Async<KvStore<'key, 'value>>

    val put :
        'key -> 'value -> KvStore<'key, 'value> ->
            Async<Version>

    val delete :
        'key -> KvStore<'key, 'value> ->
            Async<Version>

    val get :
        ReadConsistency -> 'key -> KvStore<'key, 'value> ->
            Async<Version * 'value option>
```

Write path:

```text
put/delete append KvEvent to the authoritative KV stream
S2 append ack is the write ack
StateView later applies the record through its read cursor
```

Read path:

```text
eventual get = local map now
strong get   = checkTail + wait for StateView catch-up
```

## Deferred

- output-producing `InvocationRuntime`
- Pulsar-style at-least-once/effectively-once processing
- deterministic output ids
- operation ledger
- coordination claims
- fencing
- checkpoint and trim
- Yjs-style checkpoint races
- timers and wake indexes
- workflow/object/service authoring APIs
- richer proof-runner authoring APIs

## Initial Package Shape

```text
src/
  Foundation/
    SubjectHistory.fs
    StateView.fs
    KvStore.fs
  Proofs/
    FoundationSubjectHistoryProof.fs
    FoundationStateViewProof.fs
    FoundationKvStoreProof.fs
```

## Build Order

Each layer lands with a compiled proof-runner property that uses production
modules and real ephemeral S2 streams.

1. Implement `src/Foundation/SubjectHistory.fs`; prove it in
   `src/Proofs/FoundationSubjectHistoryProof.fs`.
2. Implement `src/Foundation/StateView.fs`; prove it in
   `src/Proofs/FoundationStateViewProof.fs`.
3. Implement `src/Foundation/KvStore.fs`; prove it in
   `src/Proofs/FoundationKvStoreProof.fs`.

## Acceptance Criteria

The happy-path foundation is ready for the next design pass when:

1. L0 proves pull-one read cursor behavior against real S2.
2. `SubjectHistory` proves expected-sequence append, conflict classification,
   cursor-based fold, and follower catch-up against real S2.
3. `StateView` proves eventual and strong reads over a KV-demo-style
   orchestrator loop.
4. `KvStore` proves the S2 KV pattern end to end.
5. Every proof is a compiled proof-runner property using production modules,
   trace-backed assertions, negative controls where appropriate, and real S2
   streams.
