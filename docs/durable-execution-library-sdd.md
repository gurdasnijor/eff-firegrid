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
- `ActivityCommandAdapter.runOnce` invokes registered activity handlers for
  committed `CallActivity` commands, publishes `CompleteActivity` envelopes to
  `{key}/in`, and checkpoints only after inbox publication.
- `InboxFold.runOnce` admits fresh `{key}/in` arrivals into the fenced log,
  records inbox cursor/highwater progress, and folds activity completions and
  timer firings into replay history.
- `TimerCommandAdapter.runOnce` consumes committed timer commands, publishes
  due timer firings to `{key}/in`, and leaves future timers revisit-able.
- `DurableHost.runOwnedTick` / `claimAndRunTick` compose one host operation:
  fold inbox, step workflow replay, and dispatch committed activity/timer
  commands.
- `DurableClient.startWith` admits `StartWorkflow` through `{instance}/in`, and
  `DurableHost.runWorkflowTick` folds that start into `WorkflowStarted` before
  selecting a registered workflow.
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

Start and signal admission write durable inbox envelopes. The current
implemented layer exposes deterministic retry surfaces:

```fsharp
module DurableClient =
    val startWith :
        basin: S2.Basin ->
        instanceId: InstanceId ->
        workflowName: WorkflowName ->
        input: Payload ->
        Async<DurableClientStartStatus>

    val raiseSignalWith :
        basin: S2.Basin ->
        instanceId: InstanceId ->
        sourceSeqNum: int64 ->
        name: string ->
        payload: Payload ->
        Async<DurableClientSignalStatus>

    val raiseSignalFrom :
        basin: S2.Basin ->
        instanceId: InstanceId ->
        source: string ->
        sourceSeqNum: int64 ->
        name: string ->
        payload: Payload ->
        Async<DurableClientSignalStatus>

    val getStatusWith :
        basin: S2.Basin ->
        workflows: WorkflowRegistry ->
        instanceId: InstanceId ->
        Async<DurableClientStatusRead>
```

The ergonomic runtime wrapper generates instance ids, isolates signal sources,
allocates source sequence numbers, and closes `GetStatus` over the runtime's
registered workflows above this deterministic core.

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

Implemented first facade:

```fsharp
let runtime =
    DurableRuntime.create
        (DurableRuntimeOptions.create "host-a")
        basin
        workflows
        activities

let! start = runtime.Client.Start (WorkflowName.create "checkout") "order-1"

let! ticks =
    match start with
    | DurableClientStartStatus.Accepted ack -> runtime.Host.RunUntilIdle ack.InstanceId
    | DurableClientStartStatus.Failed _ -> async { return [] }
```

`DurableRuntime.create` is intentionally a thin assembly layer. It generates
instance ids, isolates runtime signal source ids, allocates monotonic signal
source sequence numbers, closes `GetStatus` over the workflow registry, and
exposes `Host.RunOnce` / bounded `Host.RunUntilIdle`. It does not add a daemon
loop or a new persistence path.

The runtime joins client, host, registries, and S2 configuration:

```fsharp
type DurableRuntimeOptions =
    { HostId: string
      Timestamp: unit -> int64
      MaxInboxRecords: int
      MaxActivityCommands: int
      MaxTimerCommands: int
      MaxRunUntilIdleTicks: int }

type DurableRuntime =
    { Client: DurableRuntimeClient
      Host: DurableRuntimeHost
      Workflows: WorkflowRegistry
      Activities: ActivityRegistry }

module DurableRuntime =
    val create :
        DurableRuntimeOptions ->
        Eff.S2.Basin ->
        WorkflowRegistry ->
        ActivityRegistry ->
        DurableRuntime
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

Implemented: consume `CallActivity` commands from the dispatcher-scoped
`"activity"` cursor, invoke the registered handler, and publish a
`CompleteActivity` inbox envelope before checkpointing adapter progress.

Proof obligations:

- only committed `CallActivity` commands invoke handlers
- handler result becomes a `CompleteActivity(opId, value)` inbox envelope with
  source-sequence provenance
- dispatch checkpoint is written only after the completion is durably published
  to `{key}/in`
- retry does not produce two effective completions for the same source command
- retry after publish-before-checkpoint does not re-invoke the handler
- stale owner cannot checkpoint adapter progress

### L3 Inbox Fold

Implemented: `InboxEnvelope` is the concrete `{key}/in` message shape for start,
signal, and activity completion arrivals. `InboxFold.runOnce` folds fresh inbox
records into the fenced log, records source highwater for dedup, and advances
the inbox cursor only in the same fenced append.

Proof obligations:

- inbox cursor is reconstructed from the log
- duplicate arrivals with the same source provenance are ignored
- activity completion delivery survives host restart
- concurrent inbox arrivals do not break fenced log commits

### L4 Activity Inbox Composition

Implemented: compose `DurableHost.runOnce`, `ActivityCommandAdapter.runOnce`,
and `InboxFold.runOnce` so a committed activity command is handled through the
inbox path and later consumed by workflow replay.

Proof obligations:

- a two-activity workflow completes through host, adapter, inbox fold, and host
  ticks
- the host does not advance on an inbox-published completion until the fold
  admits it to history
- adapter publication does not write direct completion history
- restart after inbox publish before fold still advances
- duplicate completion envelopes are ignored by source highwater

### L5 Composed Host Tick

Implemented: expose `DurableHost.runOwnedTick` and
`DurableHost.claimAndRunTick` as the first composed host operation. The tick
claims or uses an owned instance, folds inbox arrivals, steps deterministic
workflow replay, and dispatches activity/timer commands in one typed result.

Proof obligations:

- claim-and-run reports the claimed key and fence
- repeated ticks complete a two-activity workflow
- retry after step commit but before activity dispatch still dispatches the
  pending command while the workflow is waiting
- missing activity handlers surface as typed tick failures
- stale owners cannot advance a composed tick

### L6 Timer Adapter

Implemented: consume `ScheduleTimer` and `CancelTimer` commands, publish due
`FireTimer` inbox envelopes, and let `InboxFold.runOnce` admit `TimerFired`
history only through the inbox path. Future timers remain uncheckpointed so the
adapter revisits them when time advances.

Proof obligations:

- scheduled timers fire no earlier than their deadline
- scheduled timers fire at or after their deadline through inbox admission
- canceled timers do not fire
- retry does not create two effective timer firings
- timer outcomes replay deterministically from history
- composed host ticks advance `Workflow.sleepUntil`

### L7 Durable Client Admission

Implemented: expose `DurableClient.startWith`,
`DurableClient.raiseSignalWith`, and `DurableClient.getStatusWith` as the first
durable client calls, and `DurableHost.runWorkflowTick` as the registry-backed
host entry for started instances. A start writes a `StartWorkflow` envelope to
`{instance}/in`; inbox fold records `WorkflowStarted`; the host selects the
registered workflow factory from that durable record. A signal writes a
`RaiseSignal` envelope to the same inbox; fold records the accepted envelope,
and host delivery commits `SignalReceived` plus
`SignalDelivered(source, sourceSeqNum, opId)` when replay is waiting on that
signal. Status reads the folded log, rebuilds history, and classifies the
instance by replaying without committing.

Proof obligations:

- start admission is retry-safe for a supplied `InstanceId`
- duplicate `StartWith` attempts fold into one effective workflow start
- the host starts the registered workflow from the folded start record
- missing workflows surface as typed failures
- empty instances report no durable start
- signal admission is retry-safe for a supplied source sequence number
- duplicate `RaiseSignalWith` attempts fold into one effective signal
- a delivered signal completes the matching wait from durable history
- the same admitted signal cannot satisfy a later wait
- status for not found, running, waiting, completed, and missing workflow states
  is derived from durable records

The remaining client work is API polish: naming, packaging, and examples that
make the runtime facade the obvious entry point.

### L8 Ergonomic Host

Implemented: expose `DurableRuntime.create`, `Client.Start`,
`Client.StartWith`, `Client.RaiseSignal`, `Client.RaiseSignalWith`,
`Client.GetStatus`, `Host.RunOnce`, and bounded `Host.RunUntilIdle`.
`Host.RunForever` remains the later service loop.

Proof obligations:

- `RunUntilIdle` reaches the same final durable state as repeated `RunOnce`
- host restart does not duplicate committed commands
- deposed hosts stop publishing progress
- failures are typed and observable
- generated client starts return usable instance ids
- runtime signal admission uses an isolated monotonic source

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
