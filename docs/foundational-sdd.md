# Eff Firegrid Foundational SDD

## Status

Foundational infrastructure design.

This SDD replaces the earlier in-memory REPL proof ladder. That code has been
removed because it risked becoming a fake parallel implementation. The
foundation must be designed around real S2 primitives from the start.

## Design Center

`eff-firegrid` should be a small F#/Fable substrate for durable coordination and
materialized state on top of S2 streams.

The first milestone is not a workflow engine. The first milestone is an
S2-backed materialization and coordination runtime that proves these properties:

- append records durably and observe their assigned order
- maintain local materialized state from a stream prefix
- provide strong reads through `checkTail` plus catch-up
- coordinate multiple hosts through conditional append
- fence stale writers when correctness depends on exclusive access
- replay completed operation results without re-entering user code

The S2 KV demo is the closest reference shape. Its core is an orchestrator task
that owns local state, receives commands over a channel, submits writes through
an append session, applies a tailing read session into local state, queues
strong-read responses until the required log prefix has been applied, and
batches `checkTail` calls with a "bus stand" optimization. We should build the
F#/Fable analog of that runtime before building workflow concepts.

Sources:

- <https://s2.dev/blog/kv-store#checktail-operations-for-strong-reads>
- <https://github.com/s2-streamstore/s2-kv-demo/blob/main/src/main.rs>
- <https://s2.dev/blog/distributed-ai-agents>
- <https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html>

## Core Distinctions

### State Consensus

State consensus means multiple hosts agree on application state by applying the
same ordered log prefix.

Required primitive:

> Append events to an S2 stream, tail the stream, fold events into state, and
> use `checkTail` as the barrier for linearizable reads.

This is the KV-store lane.

### Coordination

Coordination means hosts decide who should attempt work.

Required primitive:

> Append claims or decisions to a coordination stream using conditional append.

This is the distributed-agents lane. Coordination streams are not automatically
the same streams as application-state logs.

### Fencing

Fencing means a stale worker cannot mutate a protected resource after losing
authority.

Required primitive:

> Every protected write carries a monotonic token, and the protected resource
> rejects tokens older than the highest accepted token.

Leases and timeouts help liveness. They are not the correctness boundary.

### Idempotent Execution

Idempotent execution means replay sees a durable result before running user code
again.

Required primitive:

> Record operation attempts and completions in a durable ledger, keyed by stable
> operation id.

This does not make arbitrary external side effects exactly-once. External
effects need idempotency keys, fencing-token enforcement, or an outbox pattern.

## Required Capabilities

### C1. Typed Stream Log

Purpose:

- provide the thin F# abstraction over S2 streams
- preserve S2 semantics instead of hiding them behind workflow vocabulary

Required operations:

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
        S2.Basin -> EventCodec<'event> -> StreamId -> 'event list -> Async<StreamVersion>

    val appendExpected :
        S2.Basin -> EventCodec<'event> -> StreamId -> StreamVersion -> 'event list ->
            Async<Result<StreamVersion, AppendConflict>>

    val readFrom :
        S2.Basin -> EventCodec<'event> -> StreamId -> SeqNum ->
            Async<Result<LogRecord<'event> list, string>>

    val checkTail :
        S2.Basin -> StreamId -> Async<StreamVersion>
```

Actual infrastructure:

- `src/Foundation/StreamLog.fs`
- uses existing `src/S2/Client.fs`
- maps `StreamId` to `S2.stream`
- encodes events as text records first
- lowers conditional append to `S2.tryAppendWith` plus `MatchSeqNum`
- maps `S2Errors.SeqNumMismatch` to `AppendConflict`

Do not add a memory backend in the first pass. Deterministic tests can use real
ephemeral S2 streams until we have a reason to abstract the backend.

### C2. S2 KV-Style Materialization Runtime

Purpose:

- provide the F#/Fable analog of the S2 KV demo's orchestrator task
- own local materialized state in one event loop instead of direct mutation
- append writes through S2 and acknowledge them only after S2 append ack
- apply a tailing read session into local state
- expose eventual reads and strong reads
- provide the primitive that higher-level indexes and state views reuse

Required operations:

```fsharp
type Materialized<'state> =
    { State: 'state
      Applied: StreamVersion }

type ReadConsistency =
    | Eventual
    | Strong

type MaterializerCommand<'event, 'state> =
    | Append of events: 'event list * reply: AsyncReplyChannel<Result<StreamVersion, string>>
    | ReadEventual of reply: AsyncReplyChannel<Materialized<'state>>
    | ReadStrong of reply: AsyncReplyChannel<Materialized<'state>>
    | Stop of reply: AsyncReplyChannel<unit>

module MaterializationRuntime =
    val start :
        S2.Basin ->
        EventCodec<'event> ->
        StreamId ->
        initial: 'state ->
        apply: ('state -> 'event -> 'state) ->
        Async<MaterializationRuntime<'state, 'event>>

    val append :
        'event list -> MaterializationRuntime<'state, 'event> -> Async<Result<StreamVersion, string>>

    val read :
        ReadConsistency -> MaterializationRuntime<'state, 'event> -> Async<Materialized<'state>>

    val stop : MaterializationRuntime<'state, 'event> -> Async<unit>
```

Actual infrastructure:

- `src/Foundation/MaterializationRuntime.fs`
- one long-lived event loop per materialized stream
- one append session for writes, with pending append acknowledgements paired to
  callers
- one tailing read session starting from the current applied version
- local state owned by the event loop/mailbox
- pending strong-read responses keyed by the required tail version
- optional bus-stand component that coalesces strong-read `checkTail` calls
- `Eventual` read sends a read command and returns current local state
- `Strong` read calls the bus-stand `checkTail`, sends a strong-read command
  with the required tail, and waits until `Applied >= requiredTail`
- write acknowledgement follows the S2 KV demo: reply after the append is
  durably acknowledged by S2, not necessarily after every local runtime has
  applied it

This is the component that should be directly aligned with the S2 KV demo. It
should be generic in event/state types, but the implementation shape should
remain recognizable: command inbox, append session, pending append acks, tailing
reader, local state, pending strong reads, and batched `checkTail`.

### C3. KV Store Built On The Materialization Runtime

Purpose:

- prove the materialization runtime against the concrete S2 KV-store model
- provide a reusable map-like view for indexes and metadata
- clarify which state is authoritative and which state is derived

Required operations:

```fsharp
type KvEvent<'key, 'value> =
    | Put of key: 'key * value: 'value
    | Delete of key: 'key

module KvStore =
    val start :
        S2.Basin ->
        EventCodec<KvEvent<'key, 'value>> ->
        StreamId ->
        Async<MaterializationRuntime<Map<'key, 'value>, KvEvent<'key, 'value>>>

    val put :
        'key -> 'value -> MaterializationRuntime<Map<'key, 'value>, KvEvent<'key, 'value>> ->
            Async<Result<StreamVersion, string>>

    val delete :
        'key -> MaterializationRuntime<Map<'key, 'value>, KvEvent<'key, 'value>> ->
            Async<Result<StreamVersion, string>>

    val get :
        ReadConsistency -> 'key -> MaterializationRuntime<Map<'key, 'value>, KvEvent<'key, 'value>> ->
            Async<StreamVersion * 'value option>
```

Actual infrastructure:

- `src/Foundation/KvStore.fs`
- implemented as the first concrete consumer of `MaterializationRuntime`
- one stream is the authoritative mutation log
- the local map is a materialized view only
- `put` and `delete` append `KvEvent` through the runtime append path
- strong `get` requires the runtime's applied version to catch up to
  `checkTail`

Decision:

> Yes, build this as the F#/Fable analog of the S2 KV demo. It should be the
> first real proof that the materialization runtime works against S2. Many later
> capabilities need strongly readable materialized maps: run metadata, indexes,
> task lists, claim views, schedules, wake candidates, and host membership.

### C4. Coordination Log

Purpose:

- represent admission decisions separately from application state
- let multiple hosts race safely through conditional append

Required operations:

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

Actual infrastructure:

- `src/Foundation/Coordination.fs`
- reads claim stream to see whether the claim id is already decided
- obtains claim-stream tail
- appends `ClaimRecorded` with `MatchSeqNum = tail`
- loser re-reads instead of assuming failure semantics

This supports agent task claims and work admission without introducing workflow
runs.

### C5. Fencing

Purpose:

- prevent stale actors from mutating protected resources
- make lock/lease limitations explicit

Required operations:

```fsharp
type FenceToken = FenceToken of int64

module Fencing =
    val fromStreamVersion : StreamVersion -> FenceToken
    val requireToken : FenceToken -> S2.AppendOptions -> S2.AppendOptions
```

Actual infrastructure:

- `src/Foundation/Fencing.fs`
- derive monotonic tokens from claim-stream versions or S2 fencing tokens
- provide helpers for S2 protected writes
- document that external resources must enforce tokens themselves

This follows Kleppmann's warning: a lease without fencing is only an efficiency
optimization.

### C6. Operation Ledger

Purpose:

- provide idempotent result replay for durable work units
- separate operation result durability from work scheduling

Required operations:

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

Actual infrastructure:

- `src/Foundation/OperationLedger.fs`
- reads operation ledger by stable operation id
- returns completed/failed result before invoking `action`
- records start with conditional append
- records completion after action succeeds
- does not claim exactly-once external effects unless paired with C5 or an
  external idempotency contract

### C7. Snapshot / Checkpoint

Purpose:

- bound materialization-runtime startup cost
- keep snapshots subordinate to the stream

Required operations:

```fsharp
type Snapshot<'state> =
    { State: 'state
      Applied: StreamVersion }

module Checkpoint =
    val save : StreamId -> Snapshot<'state> -> Async<unit>
    val loadLatest : StreamId -> Async<Snapshot<'state> option>
```

Actual infrastructure:

- initially ordinary S2 snapshot/checkpoint stream records
- later evaluate native S2 snapshot support where appropriate
- every loaded snapshot must be reconciled by reading from `Applied` to current
  tail

Snapshots are performance state, not authority.

### C8. Observability / Replay

Purpose:

- make distributed coordination inspectable
- support audit and debugging without bespoke debug state

Required operations:

- inspect stream tail
- read a stream range
- decode records with versioned codecs
- show materialization-runtime applied version versus stream tail
- show pending strong-read waiters
- show claim losers and winners

Actual infrastructure:

- CLI/script helpers over `StreamLog`
- no separate observability database in the foundation

## Initial Package Shape

Keep the first production package small:

```text
src/
  Foundation/
    StreamLog.fs
    MaterializationRuntime.fs
    KvStore.fs
    Coordination.fs
    Fencing.fs
    OperationLedger.fs
    Checkpoint.fs

scripts/
  foundation-00-stream-log.fsx
  foundation-01-kv-runtime.fsx
  foundation-02-coordination.fsx
  foundation-03-fencing.fsx
  foundation-04-operation-ledger.fsx
```

The scripts must use real S2 streams. They may create ephemeral streams and
delete them after the run. They should not use an in-memory fake backend.

## Build Order

1. Harden `src/S2/Client.fs` only where the foundation needs missing S2
   operations. Do not replace it.
2. Implement `Foundation.StreamLog` on top of the existing S2 client.
3. Build `Foundation.MaterializationRuntime` as the S2 KV-demo-style event-loop
   component.
4. Build `Foundation.KvStore` as the first consumer and proof of strong reads.
5. Add `Foundation.Coordination` for claim streams.
6. Add `Foundation.Fencing` helpers and one S2-backed fenced-write proof.
7. Add `Foundation.OperationLedger` and explicitly document side-effect limits.
8. Add checkpointing only after materialization-runtime replay cost becomes
   visible.

## What Higher-Level Workflow Gets Later

After the foundation is proven:

- run state is a KV/materialized view over workflow events
- timers are records plus a due-time materialized index
- schedules are records plus a due-bucket materialized index
- approvals are external-event records plus idempotent delivery
- wake indexes are derived materialized views
- services, virtual objects, and workflows are capability bundles over the same
  foundation

Do not introduce these before the materialization runtime and coordination
components are working against real S2.

## Acceptance Criteria

The foundation is ready for workflow ergonomics when:

1. `StreamLog` proves append, read, check-tail, and conditional append against
   real S2.
2. `MaterializationRuntime` proves eventual read versus strong read behavior
   across two host instances.
3. `KvStore` proves a strongly consistent `get` after a write from another
   host.
4. `Coordination` proves two hosts race for the same claim and only one wins.
5. `Fencing` proves a stale actor cannot write to an S2-protected stream with an
   old token.
6. `OperationLedger` proves replay does not re-enter user code after completion.
7. Each proof is expressed as an F#/Fable script using production modules, not
   fake code.
