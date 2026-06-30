# Durable Execution Library SDD

## Status

Current product contract for the consumable durable execution library.

This document sits above `durable-fsharp-sdd-r2-substrate.md`. The substrate
document answers "why is this correct over S2?" This document answers "what
should application developers be able to install and use?"

## Objective

Build `Eff.Foundation.Durable` into a stock F#/Fable durable execution library
over S2.

The public vocabulary is:

- app: the user's collection of activities, workflows, and signals
- client: admission and observation
- worker: execution
- test host: local deterministic harness for proofs and examples

`DurableRuntime`, registries, owned keys, fence tokens, step records, and command
dispatchers are infrastructure. They can stay available for focused tests and
advanced diagnostics, but they are not the normal application surface.

## Copyable Surface

The current copyable app shape is handle-based. Users define activities,
workflows, and signals once, add them to a `durableApp`, and pass those handles
to the client and worker APIs.

```fsharp
open Eff.Foundation.Durable
open Eff.Foundation.Durable.App

module D = Eff.Foundation.Durable.App.Durable

let reserve =
    Activity.define "reserve" (fun orderId -> async { return "reserved:" + orderId })

let charge =
    Activity.define "charge" (fun reservation -> async { return "charged:" + reservation })

let approved = Signal.define "approved"

let checkout =
    Workflow.define "checkout" (fun orderId ->
        durable {
            let! reserved = D.call reserve orderId
            let! charged = D.call charge reserved
            do! D.log ("charged " + orderId)
            return charged
        })

let approval =
    Workflow.define "approval" (fun orderId ->
        durable {
            let! approvedBy = D.waitForSignal approved
            return orderId + ":approved-by:" + approvedBy
        })

let app =
    durableApp {
        activity reserve
        activity charge
        workflow checkout
        workflow approval
    }
```

Bootstrap is still explicit while storage configuration is being designed:

```fsharp
let storage = DurableStorage.s2 basin

let client =
    app |> DurableApp.clientWith { Storage = storage }

let worker =
    app
    |> DurableApp.workerWith
        { Storage = storage
          HostId = "host-a"
          MaxRunUntilIdleTicks = None }
```

Starting, running, observing, and signaling do not require workflow names or raw
registries:

```fsharp
let! start = client.start checkout "order-123"

let instanceId =
    match start with
    | DurableAppStartResult.Started instanceId -> instanceId
    | DurableAppStartResult.Rejected failure -> failwith (string failure)

do! worker.runUntilIdle instanceId |> Async.Ignore
let! status = client.status instanceId

let! _ = client.startWith (InstanceId.create "approval-123") approval "order-123"
let! _ = client.signal (InstanceId.create "approval-123") approved "alice"
```

The target environment-backed bootstrap should remove explicit S2 wiring from
most applications:

```fsharp
let client = app |> DurableApp.client { Environment = "prod" }
let worker = app |> DurableApp.worker { Environment = "prod"; HostId = "host-a" }
```

The current implementation intentionally ships `clientWith` and `workerWith`
first so storage wiring remains testable while the app API settles.

## Public API

The durable log stores string payloads. Public handles can stay string-only or
carry explicit codecs so user code can work with typed inputs, outputs, and
signals without changing substrate records.

```fsharp
module Activity =
    val define :
        name: string ->
        handler: (string -> Async<string>) ->
        Activity<string, string>

    val defineWith :
        name: string ->
        encodeInput: ('input -> string) ->
        decodeInput: (string -> 'input) ->
        encodeOutput: ('output -> string) ->
        decodeOutput: (string -> 'output) ->
        handler: ('input -> Async<'output>) ->
        Activity<'input, 'output>

module Workflow =
    val define :
        name: string ->
        factory: (string -> Durable<string>) ->
        Workflow<string, string>

    val defineWith :
        name: string ->
        encodeInput: ('input -> string) ->
        decodeInput: (string -> 'input) ->
        encodeOutput: ('output -> string) ->
        decodeOutput: (string -> 'output) ->
        factory: ('input -> Durable<'output>) ->
        Workflow<'input, 'output>

module Signal =
    val define : name: string -> Signal<string>

    val defineWith :
        name: string ->
        encode: ('payload -> string) ->
        decode: (string -> 'payload) ->
        Signal<'payload>

module Durable =
    val call : Activity<'input, 'output> -> 'input -> Durable<'output>
    val waitForSignal : Signal<'payload> -> Durable<'payload>
    val sleepUntil : deadline: int64 -> Durable<unit>
    val any : RaceTask seq -> Durable<RaceResult>
    val currentTime : Durable<int64>
    val log : message: string -> Durable<unit>

type DurableAppStartResult =
    | Started of InstanceId
    | Rejected of DurableAppStartFailure

type DurableAppSignalResult =
    | Accepted
    | Rejected of DurableAppSignalFailure

type DurableAppWorkflowStatus =
    | NotFound
    | Running of workflow: string
    | Waiting of workflow: string * need: DurableAppNeed
    | Completed of workflow: string * output: string
    | Failed of DurableAppStatusFailure

type DurableAppClient =
    member start : Workflow<'input, 'output> -> 'input -> Async<DurableAppStartResult>
    member startWith : InstanceId -> Workflow<'input, 'output> -> 'input -> Async<DurableAppStartResult>
    member signal : InstanceId -> Signal<'payload> -> 'payload -> Async<DurableAppSignalResult>
    member status : InstanceId -> Async<DurableAppWorkflowStatus>

type DurableAppWorker =
    { runOnce : InstanceId -> Async<DurableWorkflowHostStatus>
      runUntilIdle : InstanceId -> Async<DurableWorkflowHostStatus list>
      runUntilIdleWith : int -> InstanceId -> Async<DurableWorkflowHostStatus list> }
```

Worker `HostId` values are capped at 15 characters in the app facade because S2
fencing tokens are capped at 36 bytes and the substrate appends entropy to the
host id.

## Implemented Assets

Implemented and proof-backed today:

- `durable { ... }`, lower `Workflow.*`, and lower `DurableTask.*` lower author
  code into the pure replay model.
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
- `TimerCommandAdapter.runOnce` consumes committed timer commands, publishes due
  timer firings to `{key}/in`, and leaves future timers revisit-able.
- `DurableHost.runOwnedTick` / `claimAndRunTick` compose one host operation:
  fold inbox, step workflow replay, and dispatch committed activity/timer
  commands.
- `DurableClient.startWith` admits `StartWorkflow` through `{instance}/in`, and
  `DurableHost.runWorkflowTick` folds that start into `WorkflowStarted` before
  selecting a registered workflow.
- `DurableClient.raiseSignalFrom` admits signals with source-sequence
  provenance so duplicate signal messages fold once.
- `DurableClient.getStatusWith` derives not-found, running, waiting, completed,
  and missing-workflow statuses from durable records.
- `DurableRuntime.create` closes the proven client and host operations over a
  basin, workflow registry, and activity registry.
- `Eff.Foundation.Durable.App` exposes the first public app facade:
  `Activity.define`, `Workflow.define`, `Signal.define`, `Durable.call`,
  `Durable.waitForSignal`, `durableApp { ... }`, `DurableApp.clientWith`, and
  `DurableApp.workerWith`.
- App client calls project lower client outcomes into `DurableAppStartResult`,
  `DurableAppSignalResult`, and `DurableAppWorkflowStatus`.
- `Activity.defineWith`, `Workflow.defineWith`, and `Signal.defineWith` attach
  explicit codecs to handles while keeping durable storage as string payloads.
- The compiled proof runner validates the above against pure laws and ephemeral
  S2 streams.

Users should not have to directly compose `OwnedKey`, `FenceToken`,
`StepRecordCodec`, `DurableStepper`, `DurableCommandDispatch`,
`WorkflowRegistry`, `ActivityRegistry`, or `DurableRuntime.create` for normal
application code.

## Execution Model

The worker is built from already proven lower layers:

```text
claim instance log fence
fold inbox arrivals into log
run one workflow replay step
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

## Build Sequence

Build one public-facing layer plus proof at a time.

### L1 App Facade

Implemented in `Eff.Foundation.Durable.App`.

Proof obligations:

- typed workflow handles start without raw workflow names
- worker completes a typed activity workflow
- client status reads completion through the app's workflow set
- typed signal handles complete a waiting workflow
- typed signal handles can participate in `whenAny` races

### L2 Result Polish

Implemented: app client calls return app-level result and status types instead
of raw lower-level `DurableClientStartStatus`, `DurableClientSignalStatus`, and
`DurableClientStatusRead`.

```fsharp
type DurableAppStartResult =
    | Started of InstanceId
    | Rejected of DurableAppStartFailure

type DurableAppSignalResult =
    | Accepted
    | Rejected of DurableAppSignalFailure

type DurableAppWorkflowStatus =
    | NotFound
    | Running of workflow: string
    | Waiting of workflow: string * need: DurableAppNeed
    | Completed of workflow: string * output: string
    | Failed of DurableAppStatusFailure
```

Proof obligations:

- app-level results preserve all lower failure evidence
- status projection never reports completion without durable completion history
- rejected starts and signals are observable without exceptions

### L3 Typed Payload Codecs

Implemented: typed handles attach explicit codecs without changing substrate
records:

```fsharp
let addOne =
    Activity.defineWith
        "add-one"
        string
        int
        string
        int
        (fun value -> async { return value + 1 })

let typedMath =
    Workflow.defineWith
        "typed-math"
        string
        int
        string
        int
        (fun value ->
            durable {
                let! incremented = Durable.call addOne value
                return incremented + 10
            })

let score = Signal.defineWith "score" string int
```

Proof obligations:

- codec-backed activity workflows complete through the durable worker
- codec-backed signal payloads complete waiting workflows
- replay sees stable payload text for a given codec version
- typed activity and workflow handles cannot be crossed accidentally

### L4 Test Host

Implemented in the proof harness: `Eff.Proofs.DurableTestHost` provisions an
isolated S2 basin from the declared proof S2 resource, returns a tracked app
client and worker, exposes durable-status assertions, and cleans up tracked
instance streams.

```fsharp
let! host = DurableTestHost.start ctx app
let! instance = host.client.start checkout "order-123"
do! host.worker.runUntilIdle instance
host.expect.completed instance "charged:reserved:order-123"
```

Proof obligations:

- test host provisions isolated storage per test
- all created instance streams are cleaned up or reported
- assertions replay durable state, not in-memory shortcuts

### L5 Environment Bootstrap

Implemented: `DurableApp.client` and `DurableApp.worker` resolve storage from
environment-specific S2 settings before returning app client and worker handles.

```fsharp
let client =
    app
    |> DurableApp.client
        { Environment = "prod"
          BasinName = None }

let worker =
    app
    |> DurableApp.worker
        { Environment = "prod"
          BasinName = None
          HostId = "host-a"
          MaxRunUntilIdleTicks = None }
```

Resolution order is `EFF_FIREGRID_<ENV>_BASIN`, then `EFF_FIREGRID_BASIN`,
unless `BasinName` is supplied. S2 access token and endpoint settings use the
same environment-specific-then-global pattern for `ACCESS_TOKEN`,
`S2_ACCOUNT_ENDPOINT`, and `S2_BASIN_ENDPOINT`; if none are supplied, the local
S2 CLI config is used.

Proof obligations:

- environment resolution produces the same app client/worker semantics as
  explicit `clientWith` / `workerWith`
- missing or invalid configuration fails before any partial durable writes
- host id validation remains explicit

### L6 Worker Service Loop

Add instance discovery and `RunForever` after the per-instance worker API is
stable:

```fsharp
do! worker.runForever cancellationToken
```

Proof obligations:

- scanner does not skip runnable instances whose inbox/log streams exist
- deposed workers stop publishing progress
- bounded polling and cancellation leave no partially-owned loop state

## Example Target

The maintained example is `examples/durable-tutorial/src/Tutorial.fsx`. It must
compile through `npm run smoke:examples` and stay aligned with the app facade.

The example should cover:

- activity, workflow, and signal definitions
- app assembly
- explicit storage bootstrap
- generated start
- supplied instance id start
- worker `runUntilIdle`
- status read
- signal delivery
- signal/timer race
