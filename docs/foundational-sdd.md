# Eff Firegrid Foundational SDD

## Status

Happy-path foundation design, aligned to the S2 KV demo.

This document intentionally excludes output-producing invocation guarantees,
operation ledgers, coordination claims, leases, fencing, checkpoint trimming,
timers, schedules, workflow APIs, and full proof-runner work. The goal is to
build the smallest real S2-backed state-view path we can validate with F#/Fable
scripts.

## Objective

Build the happy-path substrate for stream-derived state on S2:

```text
S2 client
  -> StreamLog
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
    depends on: L1 StreamLog and L0 pull-based read sessions
    purpose:    fold one typed stream into local state and serve reads

L1  StreamLog
    depends on: L0 S2 client
    purpose:    typed append/read/session/checkTail over S2

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
StreamVersion = exclusive upper bound = next sequence number
```

Examples:

- empty stream tail is `StreamVersion 0`
- record `SeqNum 0` is applied when `AppliedTail >= StreamVersion 1`
- S2 append ack `End.SeqNum` is the append batch end-exclusive version
- S2 `checkTail` returns the current end-exclusive version
- strong read waits for `AppliedTail >= checkedTail`

Do not use `StreamVersion` to mean "last applied record sequence."

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

## Capability C1: StreamLog

Layer: L1.

`StreamLog` is the typed boundary over S2. `StateView` and `KvStore` consume
typed `StreamLog` records, not raw SDK records.

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

- foundation `StreamLog` uses one event per raw S2 record with an explicit codec
- `S2Patterns` is opt-in later for large event payloads
- `StateView` should call `StreamLog`, not `S2Patterns` directly

### Proposed Surface

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

type AppendRange =
    { Start: SeqNum
      EndExclusive: StreamVersion
      Tail: StreamVersion }

type AppendTicket =
    { Ack: Async<AppendRange> }

type TypedAppendSession<'event>
type TypedReadSession<'event>
type TypedReadCursor<'event>

module StreamLog =
    val append :
        S2.Basin -> EventCodec<'event> -> StreamId -> 'event list ->
            Async<AppendRange>

    val appendExpected :
        S2.Basin -> EventCodec<'event> -> StreamId -> StreamVersion -> 'event list ->
            Async<Result<AppendRange, AppendConflict>>

    val readFrom :
        S2.Basin -> EventCodec<'event> -> StreamId -> SeqNum ->
            Async<Result<LogRecord<'event> list, string>>

    val checkTail :
        S2.Basin -> StreamId -> Async<StreamVersion>

    val openAppendSession :
        S2.Basin -> EventCodec<'event> -> StreamId ->
            Async<TypedAppendSession<'event>>

    val submit :
        'event list -> TypedAppendSession<'event> -> Async<AppendTicket>

    val openReadSession :
        S2.Basin -> EventCodec<'event> -> StreamId -> SeqNum ->
            Async<TypedReadSession<'event>>

    val openReadCursor :
        S2.Basin -> EventCodec<'event> -> StreamId -> SeqNum ->
            Async<TypedReadCursor<'event>>

    val tryNext :
        TypedReadCursor<'event> -> Async<Result<LogRecord<'event> option, string>>
```

### Script First

First proof:

```text
scripts/foundation-00-stream-log.fsx
```

Prove against real ephemeral S2 streams:

- typed append/read round trip
- `checkTail` returns end-exclusive version
- append ack/session ack returns `AppendRange`
- conditional append conflict maps to `AppendConflict`
- typed read session tails records
- typed read cursor pulls one record at a time without closing the session

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
      AppliedTail: StreamVersion }

type StateView<'event, 'state>

module StateView =
    val start :
        S2.Basin ->
        EventCodec<'event> ->
        StreamId ->
        recoverFrom: SeqNum ->
        initial: 'state ->
        apply: ('state -> 'event -> 'state) ->
            Async<StateView<'event, 'state>>

    val read :
        ReadConsistency -> StateView<'event, 'state> ->
            Async<ViewState<'state>>

    val stop : StateView<'event, 'state> -> Async<unit>
```

Internal loop:

- open typed append session for writes that originate through the view
- open typed read cursor from `recoverFrom`
- fold every pulled record into local state
- set `AppliedTail = record.SeqNum + 1`
- serve eventual reads from current local state
- for strong reads, call `StreamLog.checkTail`, then wait until
  `AppliedTail >= checkedTail`
- keep pending strong reads in required-tail order
- drain pending reads as `AppliedTail` advances

Write ack invariant:

- writes acknowledge on durable S2 append, not on local apply
- this is valid only for commands like `put` and `delete` that do not return
  prior state
- operations that must return prior state need a different path

### Script First

Next proof:

```text
scripts/foundation-01-state-view.fsx
```

Prove:

- local fold state updates from tail records
- eventual read can observe current local state
- strong read calls `checkTail` and waits for `AppliedTail`
- write ack returns before local apply where the script can observe that window
- second host can append; first host strong-read catches up to that append

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
        EventCodec<KvEvent<'key, 'value>> ->
        StreamId ->
        recoverFrom: SeqNum ->
            Async<KvStore<'key, 'value>>

    val put :
        'key -> 'value -> KvStore<'key, 'value> ->
            Async<AppendRange>

    val delete :
        'key -> KvStore<'key, 'value> ->
            Async<AppendRange>

    val get :
        ReadConsistency -> 'key -> KvStore<'key, 'value> ->
            Async<StreamVersion * 'value option>
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
- full proof runner

## Initial Package Shape

```text
src/
  Foundation/
    StreamLog.fs
    StateView.fs
    KvStore.fs

scripts/
  foundation-00-stream-log.fsx
  foundation-01-state-view.fsx
  foundation-02-kv-store.fsx
```

## Build Order

Each layer starts as a script proof, then promotes the stable code into `src/`.

1. Write `scripts/foundation-00-stream-log.fsx`; promote
   `src/Foundation/StreamLog.fs`.
2. Write `scripts/foundation-01-state-view.fsx`; promote
   `src/Foundation/StateView.fs`.
3. Write `scripts/foundation-02-kv-store.fsx`; promote
   `src/Foundation/KvStore.fs`.

## Acceptance Criteria

The happy-path foundation is ready for the next design pass when:

1. L0 proves pull-one read cursor behavior against real S2.
2. `StreamLog` proves typed append/read/checkTail/session/cursor behavior
   against real S2.
3. `StateView` proves eventual and strong reads over a KV-demo-style
   orchestrator loop.
4. `KvStore` proves the S2 KV pattern end to end.
5. Every proof is an F#/Fable script using production modules and real S2
   streams.
