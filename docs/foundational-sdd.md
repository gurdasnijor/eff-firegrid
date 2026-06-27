# Eff Firegrid Foundational SDD

## Status

Foundational infrastructure design, aligned to the S2 KV-store and durable Yjs
room patterns.

The earlier in-memory REPL proof ladder has been removed. The foundation should
be proven against real S2 streams through scripts that load production modules.

## Objective

Build the smallest F#/Fable substrate for durable coordination and replicated
state on S2.

The first product is not a workflow engine. The first product is the F#/Fable
analog of the S2 KV demo's core runtime pattern:

- S2 stream as the authoritative shared log
- append session for writes
- tailing read session for local state catch-up
- local state owned by one invocation-local event loop
- eventual reads from current local state
- strong reads through `checkTail` plus waiting for local catch-up
- checkpoint/trim coordination with S2 fencing when replay cost matters

Sources:

- <https://s2.dev/blog/kv-store#part-2-designing-a-replicated-kv-store>
- <https://github.com/s2-streamstore/s2-kv-demo/blob/main/src/main.rs>
- <https://s2.dev/blog/durable-yjs-rooms>
- <https://s2.dev/blog/distributed-ai-agents>
- <https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html>

## Dependency Map

Use this as the mental model. Higher rows depend on lower rows.

```text
L6  Workflow / objects / approvals / schedules
    depends on: L5 + selected capabilities below

L5  Operation ledger
    depends on: L1 StreamLog
    may use:    L4 Coordination, L4 Fencing for external effects

L4  Coordination and fencing
    depends on: L1 StreamLog and native S2 append preconditions / fencing
    independent of: L2 StreamStateReplica and L3 KV

L3  KV store
    depends on: L2 StreamStateReplica
    purpose:    first concrete proof of replicated state

L2  StreamStateReplica
    depends on: L1 StreamLog, S2 append session, S2 read session, checkTail
    purpose:    invocation-local replica of one stream-derived state value

L1  Typed StreamLog
    depends on: existing src/S2/Client.fs
    purpose:    typed append/read/checkTail/conditional append over S2

L0  S2 client
    exists:     src/S2/Client.fs
    purpose:    direct F#/Fable bindings over S2 primitives
```

Important consequences:

- KV does not back coordination. KV is the first concrete state replica.
- Coordination does not require KV. It can be a claim stream with conditional
  append.
- Fencing does not require KV. It requires a protected resource to enforce a
  monotonic token.
- Workflow concepts depend on these lower capabilities; they should not shape
  the foundation.

## Source Patterns

### S2 KV Store

The S2 KV post designs a multi-primary replicated store on three stream
operations: append, read, and `checkTail`.

Required pattern:

1. Encode each mutation as a log entry.
2. Append the entry to S2 and wait for the S2 append acknowledgement before
   acknowledging the write.
3. Keep a tailing read session open.
4. Apply every sequenced record to local materialized state.
5. For eventual reads, return the current local state.
6. For strong reads, call `checkTail`, then wait until local state has applied
   that tail.

The implementation uses an event loop because local state, pending append acks,
pending strong reads, and the tailing reader must be coordinated by one owner.
This SDD calls that event-loop component `StreamStateReplica`.

### Durable Yjs Rooms

The durable Yjs post adds three foundation lessons:

1. A stream can be both storage and live transport: invocations append client
   updates and tail the same stream for live updates.
2. Recovery is checkpoint plus replay: load the latest checkpoint, then read
   records from checkpoint sequence to current tail.
3. Checkpoint and trim need coordination: multiple invocations may race to
   checkpoint, so the checkpoint writer needs S2 fencing, and trim should be
   committed consistently with the checkpoint decision.

This does not mean checkpointing is foundational layer 2. It means the state
replica must expose enough cursor information for a later checkpoint/trim
component.

## Required Capabilities

### C1. Typed StreamLog

Layer: L1.

Depends on:

- L0 `src/S2/Client.fs`

Provides:

- typed event encoding over S2 text records
- append
- append with expected stream version
- read from sequence
- check tail
- typed conflict mapping

Does not provide:

- local state
- strong reads
- coordination policy
- workflow concepts

Proposed module:

```text
src/Foundation/StreamLog.fs
```

Proposed surface:

```fsharp
type StreamId = StreamId of string
type SeqNum = SeqNum of int64
type StreamVersion = StreamVersion of int64

type LogRecord<'event> =
    { SeqNum: SeqNum
      Event: 'event }

type AppendConflict =
    { Expected: StreamVersion
      Actual: StreamVersion }

type EventCodec<'event> =
    { Encode: 'event -> string
      Decode: string -> Result<'event, string> }

module StreamLog =
    val append :
        S2.Basin -> EventCodec<'event> -> StreamId -> 'event list ->
            Async<StreamVersion>

    val appendExpected :
        S2.Basin -> EventCodec<'event> -> StreamId -> StreamVersion -> 'event list ->
            Async<Result<StreamVersion, AppendConflict>>

    val readFrom :
        S2.Basin -> EventCodec<'event> -> StreamId -> SeqNum ->
            Async<Result<LogRecord<'event> list, string>>

    val checkTail :
        S2.Basin -> StreamId -> Async<StreamVersion>
```

S2 lowering:

- `append` uses `S2.append`
- `appendExpected` uses `S2.tryAppendWith` with `MatchSeqNum`
- `readFrom` uses `S2.read` / later `readSession`
- `checkTail` uses `S2.checkTail`
- `SeqNumMismatch` maps to `AppendConflict`

### C2. StreamStateReplica

Layer: L2.

This is the F#/Fable analog of the S2 KV demo's orchestrator task.

Depends on:

- L1 `StreamLog`
- S2 append sessions
- S2 read sessions
- S2 `checkTail`

Provides:

- one invocation-local replica of state derived from one stream
- write submission through S2 append session
- local state catch-up through S2 read session
- eventual reads
- strong reads
- applied-version cursor for checkpointing and observability

Does not provide:

- KV semantics by itself
- cross-stream transactions
- durable workflow execution
- external side-effect exactly-once guarantees

Proposed module:

```text
src/Foundation/StreamStateReplica.fs
```

Proposed concepts:

```fsharp
type ReadConsistency =
    | Eventual
    | Strong

type ReplicaState<'state> =
    { State: 'state
      Applied: StreamVersion }

type StreamStateReplica<'event, 'state>
```

Proposed surface:

```fsharp
module StreamStateReplica =
    val start :
        S2.Basin ->
        EventCodec<'event> ->
        StreamId ->
        initial: 'state ->
        apply: ('state -> 'event -> 'state) ->
        Async<StreamStateReplica<'event, 'state>>

    val append :
        'event list -> StreamStateReplica<'event, 'state> ->
            Async<Result<StreamVersion, string>>

    val read :
        ReadConsistency -> StreamStateReplica<'event, 'state> ->
            Async<ReplicaState<'state>>

    val stop :
        StreamStateReplica<'event, 'state> -> Async<unit>
```

Internal event loop responsibilities:

- receive caller commands
- submit write records through append session
- pair pending write responses with append acknowledgements
- tail the read session from the current applied sequence
- decode and apply records to local state
- track `Applied`
- hold strong-read waiters until `Applied >= requiredTail`
- optionally coalesce strong-read `checkTail` calls with the bus-stand pattern

Strong read sequence:

```text
caller -> checkTail(stream)
caller -> replica.ReadStrong(requiredTail)
replica waits until Applied >= requiredTail
replica replies with state and Applied
```

This is the layer represented by the "Invocation 1 / Invocation 2 /
Invocation 3" picture: each invocation may read and write the shared log, and
each invocation's local state is only a replica of the log prefix it has
applied.

### C3. KV Store

Layer: L3.

Depends on:

- L2 `StreamStateReplica`

Provides:

- the first concrete replicated state component
- `put`, `delete`, and `get`
- a proof that strong reads behave like the S2 KV post

Does not provide:

- a general database abstraction
- coordination locks
- workflow state model

Proposed module:

```text
src/Foundation/KvStore.fs
```

Proposed surface:

```fsharp
type KvEvent<'key, 'value> =
    | Put of key: 'key * value: 'value
    | Delete of key: 'key

type KvStore<'key, 'value> =
    StreamStateReplica<KvEvent<'key, 'value>, Map<'key, 'value>>

module KvStore =
    val start :
        S2.Basin ->
        EventCodec<KvEvent<'key, 'value>> ->
        StreamId ->
        Async<KvStore<'key, 'value>>

    val put :
        'key -> 'value -> KvStore<'key, 'value> ->
            Async<Result<StreamVersion, string>>

    val delete :
        'key -> KvStore<'key, 'value> ->
            Async<Result<StreamVersion, string>>

    val get :
        ReadConsistency -> 'key -> KvStore<'key, 'value> ->
            Async<StreamVersion * 'value option>
```

Why build this:

- it is the direct analog of the S2 KV demo
- it proves `StreamStateReplica` with a simple state model
- later indexes are map-shaped: run metadata, claim views, wake candidates,
  schedule buckets, host membership, and query views

### C4. Coordination

Layer: L4.

Depends on:

- L1 `StreamLog`
- S2 conditional append

Provides:

- claim/admission decisions over a coordination stream
- deterministic race resolution through stream ordering

Does not depend on:

- L2 `StreamStateReplica`
- L3 `KvStore`

Proposed module:

```text
src/Foundation/Coordination.fs
```

Proposed surface:

```fsharp
type ClaimId = ClaimId of string
type ActorId = ActorId of string

type ClaimEvent =
    | ClaimRecorded of claimId: ClaimId * actorId: ActorId

type ClaimResult =
    | Claimed of version: StreamVersion
    | AlreadyClaimed
    | LostRace of AppendConflict

module Coordination =
    val tryClaim :
        S2.Basin -> EventCodec<ClaimEvent> -> StreamId -> ClaimId -> ActorId ->
            Async<ClaimResult>
```

Semantics:

1. Read coordination stream to determine whether the claim is already decided.
2. Read the stream tail.
3. Append `ClaimRecorded` with `MatchSeqNum = tail`.
4. If conditional append loses, re-read before deciding what happened.

### C5. Fencing And Checkpoint Lease

Layer: L4.

Depends on:

- L1 `StreamLog`
- native S2 fencing-token command records
- native S2 trim command records

May use:

- L2 `StreamStateReplica` cursor information
- C7 checkpoint storage

Provides:

- stale-writer protection for checkpoint/trim or other protected writes
- the durable Yjs pattern for racing checkpoint writers

Does not provide:

- general mutual exclusion for arbitrary resources unless those resources
  enforce the token

Proposed module:

```text
src/Foundation/Fencing.fs
```

Proposed surface:

```fsharp
type FenceToken = FenceToken of string

type FenceLease =
    { Token: FenceToken
      ExpiresAtMillis: int64 }

module Fencing =
    val tryAcquire :
        S2.Stream -> current: FenceToken option -> requested: FenceLease ->
            Async<Result<FenceLease, string>>

    val appendProtected :
        S2.Stream -> FenceToken -> S2.Record list ->
            Async<Result<S2.AppendAck, S2Errors.S2Failure>>

    val releaseAndTrim :
        S2.Stream -> FenceToken -> trimBefore: int64 ->
            Async<Result<S2.AppendAck, S2Errors.S2Failure>>
```

Durable Yjs lesson:

- checkpoint state is derived from a stream prefix
- checkpoint writes can race
- only the holder of the current fencing token should commit checkpoint/trim
- trim and fence reset should be committed consistently, ideally in one S2
  append batch of command records where S2 allows it

### C6. Checkpoint

Layer: L4.

Depends on:

- L2 `StreamStateReplica` cursor
- L1 `StreamLog` for replay
- C5 `Fencing` for checkpoint/trim coordination when multiple invocations race

Provides:

- faster recovery by storing a state snapshot plus applied stream version
- replay-from-checkpoint to current tail

Does not provide:

- authority independent of the stream
- correctness if replay from checkpoint to tail is skipped

Proposed module:

```text
src/Foundation/Checkpoint.fs
```

Proposed surface:

```fsharp
type Checkpoint<'state> =
    { State: 'state
      Applied: StreamVersion }

module Checkpoint =
    val save :
        StreamId -> Checkpoint<'state> -> Async<unit>

    val loadLatest :
        StreamId -> Async<Checkpoint<'state> option>

    val recover :
        S2.Basin ->
        EventCodec<'event> ->
        StreamId ->
        apply: ('state -> 'event -> 'state) ->
        Checkpoint<'state> option ->
            Async<Result<ReplicaState<'state>, string>>
```

Storage decision is intentionally open:

- first implementation can use a checkpoint stream if that keeps everything in
  S2
- object storage can be evaluated later if large snapshots need it

### C7. Operation Ledger

Layer: L5.

Depends on:

- L1 `StreamLog`

May use:

- C4 `Coordination` for admission
- C5 `Fencing` or external idempotency keys for protected side effects

Provides:

- replay of completed operation results
- a durable record that user code has already produced a result

Does not provide:

- exactly-once external side effects by itself

Proposed module:

```text
src/Foundation/OperationLedger.fs
```

Proposed surface:

```fsharp
type OperationId = OperationId of string

type OperationEvent<'result> =
    | OperationStarted of OperationId
    | OperationCompleted of OperationId * 'result
    | OperationFailed of OperationId * string

type OperationOutcome<'result> =
    | Executed of 'result
    | Replayed of 'result
    | FailedPreviously of string
    | InFlight

module OperationLedger =
    val runOnce :
        S2.Basin ->
        EventCodec<OperationEvent<'result>> ->
        StreamId ->
        OperationId ->
        action: (unit -> Async<'result>) ->
            Async<OperationOutcome<'result>>
```

## Initial Package Shape

```text
src/
  Foundation/
    StreamLog.fs
    StreamStateReplica.fs
    KvStore.fs
    Coordination.fs
    Fencing.fs
    Checkpoint.fs
    OperationLedger.fs

scripts/
  foundation-00-stream-log.fsx
  foundation-01-stream-state-replica.fsx
  foundation-02-kv-store.fsx
  foundation-03-coordination.fsx
  foundation-04-fencing-checkpoint.fsx
  foundation-05-operation-ledger.fsx
```

Scripts must use real S2 streams. They may create ephemeral streams and delete
them after the run. They should not introduce an in-memory fake backend.

## Build Order

1. Harden `src/S2/Client.fs` only where foundation modules need missing S2
   operations. Do not replace it.
2. Implement `Foundation.StreamLog`.
3. Implement `Foundation.StreamStateReplica` around append session, read
   session, local state, pending append acks, and pending strong reads.
4. Implement `Foundation.KvStore` as the first concrete replica.
5. Prove two hosts: host A writes, host B strong-reads after `checkTail` and
   catch-up.
6. Implement `Foundation.Coordination` for claim streams.
7. Implement `Foundation.Fencing` for checkpoint/trim protection.
8. Implement `Foundation.Checkpoint` recovery: checkpoint plus replay to tail.
9. Implement `Foundation.OperationLedger`.

## What Higher-Level Workflow Gets Later

After the foundation is proven:

- run state is a stream-derived state replica or KV view
- timers are records plus a derived due-time replica/index
- schedules are records plus a derived due-bucket replica/index
- approvals are external-event records plus idempotent delivery
- wake indexes are derived state, not authority
- services, virtual objects, and workflows are capability bundles over the same
  lower layers

## Acceptance Criteria

The foundation is ready for workflow ergonomics when:

1. `StreamLog` proves append, read, `checkTail`, and conditional append against
   real S2.
2. `StreamStateReplica` proves eventual read versus strong read across two host
   instances.
3. `KvStore` proves the exact S2 KV pattern: write ack after durable append,
   eventual read may be stale, strong read catches up to checked tail.
4. `Coordination` proves two hosts race for the same claim and only one wins.
5. `Fencing` proves a stale checkpoint writer cannot commit with an old token.
6. `Checkpoint` proves load checkpoint, replay to tail, and resume live reads.
7. `OperationLedger` proves replay does not re-enter user code after recorded
   completion.
8. Every proof is an F#/Fable script using production modules and real S2
   streams.
