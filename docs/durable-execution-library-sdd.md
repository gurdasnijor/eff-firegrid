# Durable Execution Library SDD

## Status

Draft target contract for the consumable infrastructure library. This document
does not replace `durable-fsharp-sdd-r2-substrate.md`; it sits above it and
defines the public library shape we are building toward.

The substrate document answers "how can this be correct over S2?" This document
answers "what should users be able to install and use?"

## Objective

Build `Eff.Foundation.Durable` into a stock F#/Fable durable execution library
over S2.

The target user experience:

```fsharp
let reserve input = async { return "reserved:" + input }
let charge input = async { return "charged:" + input }

let workflow orderId =
    durable {
        let! reserved = Workflow.call "reserve" orderId
        let! charged = Workflow.call "charge" reserved
        do! Workflow.log ("charged " + orderId)
        return charged
    }

let activities =
    ActivityRegistry.empty
    |> ActivityRegistry.register "reserve" reserve
    |> ActivityRegistry.register "charge" charge

let! runtime =
    DurableRuntime.create
        { Basin = basin
          HostId = "host-a"
          Activities = activities
          Workflows =
              WorkflowRegistry.empty
              |> WorkflowRegistry.register "checkout" workflow }

let! instance = runtime.Client.Start (WorkflowName "checkout") "order-123"
do! runtime.Host.RunUntilIdle instance
let! status = runtime.Client.GetStatus instance
```

That example is intentionally aspirational. The lower layers already exist, but
the public runtime/client/host API does not.

## Current Runtime Assets

Implemented and proof-backed today:

- `durable { ... }`, `Workflow.*`, and `DurableTask.*` lower author code into
  the pure replay model.
- `Durable.replay` reconstructs deterministic progress from history.
- `S2Substrate` owns the `{key}/log` fenced stream and `{key}/in` inbox stream
  model.
- `DurableStepper.plan` turns replay needs into durable `StepRecord` commits.
- `StepRecordCodec` is the shared DU-safe log codec for durable step records.
- `DurableHost.runOnce` reads a log, plans one step, and commits under a fence.
- `DurableCommandDispatch` selects committed `Outgoing(Command _)` records and
  advances a durable dispatch checkpoint.
- `ActivityRegistry` and `WorkflowRegistry` provide immutable, explicit
  registration maps with duplicate rejection and typed missing-handler errors.
- The compiled proof runner validates the above against pure laws and ephemeral
  S2 streams.

These are infrastructure internals. Users should not have to directly compose
`OwnedKey`, `FenceToken`, `StepRecordCodec`, `DurableStepper`, or
`DurableCommandDispatch` for normal application code.

## Public Surface

### Workflow Authoring

Keep the authoring API small and F#-native:

```fsharp
type WorkflowName = WorkflowName of string
type ActivityName = ActivityName of string
type InstanceId = InstanceId of string
type Payload = string

module Workflow =
    val call : name: string -> input: Payload -> Durable<Payload>
    val all : Activity list -> Durable<Payload list>
    val waitForSignal : name: string -> Durable<Payload>
    val sleepUntil : deadline: int64 -> Durable<unit>
    val any : RaceTask seq -> Durable<RaceResult>
    val currentTime : Durable<int64>
    val log : message: string -> Durable<unit>
```

The first payload type is `string`, matching the current implementation. Typed
payload codecs can be layered later without changing the runtime invariants.

### Registries

Registries are plain immutable values. Registration must be explicit, local, and
easy to test.

```fsharp
type ActivityHandler = Payload -> Async<Payload>
type WorkflowFactory = Payload -> Durable<Payload>

type ActivityRegistry
type WorkflowRegistry

module ActivityRegistry =
    val empty : ActivityRegistry
    val register : name: string -> ActivityHandler -> ActivityRegistry -> ActivityRegistry
    val tryFind : name: string -> ActivityRegistry -> ActivityHandler option

module WorkflowRegistry =
    val empty : WorkflowRegistry
    val register : name: string -> WorkflowFactory -> WorkflowRegistry -> WorkflowRegistry
    val tryFind : name: string -> WorkflowRegistry -> WorkflowFactory option
```

### Client API

The client is for admission and observation. It writes durable records but does
not execute workflow code.

```fsharp
type InstanceStatus =
    | Running
    | Waiting
    | Completed of Payload
    | Failed of string
    | NotFound

type StartOptions =
    { InstanceId: InstanceId option }

type DurableClient =
    { Start: WorkflowName -> Payload -> Async<InstanceId>
      StartWith: StartOptions -> WorkflowName -> Payload -> Async<InstanceId>
      RaiseSignal: InstanceId -> name: string -> Payload -> Async<unit>
      GetStatus: InstanceId -> Async<InstanceStatus> }
```

Start and signal admission should write to durable S2 streams in a way that is
safe under retries. The exact record shape is a build slice, not a doc-only
promise.

### Host API

The host owns execution. The first ergonomic host should be explicit and
testable before it becomes a daemon.

```fsharp
type HostTickResult =
    | NoWork
    | Claimed of InstanceId
    | Committed of InstanceId
    | Dispatched of InstanceId * count: int
    | Completed of InstanceId * Payload
    | Deposed of InstanceId
    | Failed of InstanceId option * string

type DurableHost =
    { RunOnce: unit -> Async<HostTickResult>
      RunUntilIdle: InstanceId -> Async<HostTickResult list>
      RunForever: System.Threading.CancellationToken -> Async<unit> }
```

`RunOnce` remains the primitive. `RunUntilIdle` is for tests, examples, and
local apps. `RunForever` is the later service loop.

### Runtime Assembly

The runtime joins client, host, registries, and S2 configuration:

```fsharp
type DurableRuntimeOptions =
    { Basin: Eff.S2.Basin
      HostId: string
      Workflows: WorkflowRegistry
      Activities: ActivityRegistry }

type DurableRuntime =
    { Client: DurableClient
      Host: DurableHost }

module DurableRuntime =
    val create : DurableRuntimeOptions -> Async<DurableRuntime>
```

This is the intended package entry point.

## Internal Surfaces

These modules remain available inside the repo but should not be the normal
application surface:

- `S2Substrate`
- `DurableStepper`
- `StepRecordCodec`
- `DurableHost.runOnce` lower-level overloads
- `DurableCommandDispatch`
- fence tokens and owned keys
- raw step records and dispatch checkpoints

Public APIs may expose typed failures, but they should not require users to
understand S2 command records, fencing mismatch details, or dispatch cursor
records unless they opt into advanced diagnostics.

## Execution Model

The host loop is built from already proven lower layers:

```text
discover ready instance
claim instance log fence
fold inbox arrivals into log
run one workflow step
commit step records under fence
select committed outgoing commands
dispatch command adapters
checkpoint dispatch cursor under fence
repeat until waiting, completed, deposed, or failed
```

Important constraints:

- Workflow user code only runs through deterministic replay.
- External effects happen from committed commands, never during replay.
- Every externally visible adapter must carry source sequence provenance.
- Every adapter must be idempotent under retry or use destination-side dedup.
- A stale owner may compute work, but it must not commit progress.
- Dispatch checkpoints are progress records, not evidence that the destination
  performed the side effect unless that adapter's proof establishes it.

## Build Slices

Build one public-facing layer plus proof at a time.

### L1 Activity Registries

Implemented: immutable `ActivityRegistry` and `WorkflowRegistry`.

Proof obligations:

- registration is deterministic and last-write or duplicate-reject behavior is
  explicit: duplicate registration is rejected
- missing handlers fail as typed errors
- workflow lookup is stable across host ticks

### L2 Activity Command Adapter

Consume `CallActivity` commands from `DurableCommandDispatch`, invoke the
registered handler, and append an activity completion record back into the
instance's durable path.

Proof obligations:

- only committed `CallActivity` commands invoke handlers
- handler result becomes `ActivityCompleted(opId, value)`
- dispatch checkpoint is written only after the completion is durably admitted
- retry does not produce two effective completions for the same source command
- stale owner cannot checkpoint adapter progress

### L3 Inbox Fold

Define the concrete `{key}/in` message shape for start, signal, and activity
completion arrivals, then fold fresh inbox records into the fenced log.

Proof obligations:

- inbox cursor is reconstructed from the log
- duplicate arrivals with the same source provenance are ignored
- signal delivery survives host restart
- concurrent inbox arrivals do not break fenced log commits

### L4 Timer Adapter

Consume `ScheduleTimer` and `CancelTimer` commands and admit `TimerFired` when
the deadline is reached.

Proof obligations:

- scheduled timers fire no earlier than their deadline
- canceled timers do not fire
- retry does not create two effective timer firings
- timer outcomes replay deterministically from history

### L5 Durable Client

Expose `Start`, `RaiseSignal`, and `GetStatus`.

Proof obligations:

- start admission is retry-safe for a supplied `InstanceId`
- generated instance ids are unique enough for the target runtime model
- raise-signal admission is durable before acknowledgement
- status is derived from durable history, not volatile host state

### L6 Ergonomic Host

Expose `DurableRuntime.create`, `Host.RunOnce`, `Host.RunUntilIdle`, and then
`Host.RunForever`.

Proof obligations:

- `RunUntilIdle` reaches the same final durable state as repeated `RunOnce`
- host restart does not duplicate committed commands
- deposed hosts stop publishing progress
- failures are typed and observable

## First Example Target

The first end-to-end sample should be a checkout-style workflow:

```fsharp
let checkout orderId =
    durable {
        let! reserved = Workflow.call "reserve" orderId
        let! charged = Workflow.call "charge" reserved
        return charged
    }
```

Minimum acceptance:

- register `reserve` and `charge`
- start one instance
- run host until idle
- observe completed status
- prove the same result survives a host restart between `reserve` and `charge`

This maps directly to the Azure Durable Functions `HelloSequence` shape before
we expand to fan-out, monitor timers, and signal-vs-timeout races.

## Non-Goals For The Next Phase

- no partition manager
- no distributed daemon deployment story
- no entity/critical-section API
- no typed payload codec layer beyond strings
- no web dashboard
- no automatic activity worker pool
- no storage trimming/checkpoint compaction

Those are library features, but they should not precede the first consumable
single-host runtime path.
