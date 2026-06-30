# Durable Ergonomics Proposal

## Why This Exists

The durable core is now useful, but the current tutorial is not a good
developer experience. It teaches users how to assemble the execution substrate:
registries, basins, host ticks, status DUs, source sequence numbers, and cleanup.
That is valuable implementation proof, but it is the wrong first surface for an
application developer.

The target should feel closer to:

- **Fluent Firegrid:** author handlers against a clean interface; keep substrate
  mechanics below the handler edge.
- **Azure Durable Functions F#:** orchestration code reads as ordinary workflow
  code: call activity, await external event, create timer, return value.

The next layer should therefore be an **authoring facade** over the current
`DurableRuntime` core, not another public execution/substrate abstraction.

## Reference Shapes

### Fluent Firegrid

The useful lesson from Fluent Firegrid is not syntax copying; it is boundary
discipline. Application authors define handlers and call operations through a
small public surface. The substrate is not part of the handler authoring loop.

For durable execution, that means the first sample should not mention:

- S2 basins or stream names
- client source sequence numbers
- registries as data structures
- host tick status unions
- cleanup of internal durable streams

### Azure Durable Functions F#

The useful lesson from Azure Durable Functions is that orchestration code should
read like ordinary workflow code:

```fsharp
let! x = call activity input
let! y = call otherActivity x
return y
```

Our equivalent should be a small durable computation expression using typed
activity handles. A hello-sequence sample should not need a deployment assembly
section before the workflow can be understood.

### Restate Tutorial Layout

The useful lesson from the Restate tutorial tree is progressive disclosure.
Examples should be split by concept instead of one large script:

```text
examples/durable/
  01-hello-sequence/
  02-checkout/
  03-external-signal/
  04-signal-timeout/
  05-tests/
```

Each example should focus on one concept and compile in CI.

## Diagnosis

The current facade still leaks implementation concerns:

- Users must manually build `WorkflowRegistry` and `ActivityRegistry`.
- Workflows and activities are registered by string name, so calls are not typed.
- `Workflow.call "reserve" orderId` is stringly typed and detached from the
  activity definition.
- The example has to know `S2.Basin`, `DurableRuntimeOptions`,
  `Host.RunUntilIdle`, `DurableClientStatusRead`, and instance cleanup.
- `RunUntilIdle` appears in user code as if it were the product, when it should
  mostly be a test/local helper.
- There is no single "app" or "service" declaration that groups durable
  workflows, activities, clients, and worker wiring.

The core is doing the right lower-level work. The mistake was calling that layer
ergonomic.

## Naming Decision

Do not use **runtime** as a public product noun.

It is too broad: it could mean the durable interpreter, the host process, the
storage connection, the client, or the app container. Keep `DurableRuntime` as
the current internal/lower-level composition type, but do not teach it first and
do not make users name it in normal application code.

Use these public nouns instead:

- **app**: durable workflows and activities
- **client**: starts workflows, sends signals, reads status
- **worker**: executes admitted work
- **test host**: deterministic local driver for tests and tutorials.

## Design Goals

1. **Workflow code should be small.**
   A hello-sequence workflow should be about the same size as the Azure sample.

2. **Activity references should be typed handles, not strings.**
   A workflow should call `Activities.sayHello` rather than `"sayHello"`.

3. **App assembly should be declarative.**
   Developers should register workflows/activities once and derive a client,
   worker, or test host from that app definition.

4. **Durable substrate details should disappear from examples.**
   No tutorial should start by explaining S2 streams, fences, inbox folds, source
   sequence numbers, or command adapters.

5. **Tests should be first-class.**
   Local `RunUntilIdle` is useful, but it should appear as a test driver or dev
   host helper.

6. **The low-level APIs remain available.**
   `DurableClient.*`, `DurableHost.*`, `DurableRuntime.*`, and proofs remain the
   lower layers. The authoring facade is additive.

## Proposed Public Shape

### Typed Definitions

```fsharp
type Activity<'input, 'output>
type Workflow<'input, 'output>
type DurableApp
type DurableAppClient
type DurableWorker
type DurableTestHost

module Activity =
    val define :
        name: string ->
        handler: ('input -> Async<'output>) ->
        Activity<'input, 'output>

module Workflow =
    val define :
        name: string ->
        program: ('input -> Durable<'output>) ->
        Workflow<'input, 'output>

module Durable =
    val call : Activity<'input, 'output> -> 'input -> Durable<'output>
    val waitForSignal<'payload> : name: string -> Durable<'payload>
    val sleepUntil : deadline: int64 -> Durable<unit>
    val any : DurableRace<'a> list -> Durable<RaceResult<'a>>
```

The first implementation can constrain payloads to the current `string`
`Payload`. The type parameters still matter because they establish the user
shape and give us a clean migration path once codecs are introduced.

### App Builder

```fsharp
let app =
    durableApp {
        activity Activities.sayHello
        workflow Workflows.helloSequence
    }
```

This should lower to the existing `ActivityRegistry` and `WorkflowRegistry`.
The developer should not manually compose those registries.

### Clients And Workers

Application code should not say "runtime" or "run on S2". The product concepts
are:

- **app**: the durable workflows and activities this service exposes
- **client**: starts workflows, sends signals, and reads status
- **worker**: executes admitted workflow work
- **test host**: deterministic local driver for examples and tests

Storage selection belongs in infrastructure/bootstrap code.

Standard application bootstrap:

```fsharp
let client =
    app
    |> DurableApp.client
        { Environment = "dev" }

let worker =
    app
    |> DurableApp.worker
        { HostId = "host-a"
          Environment = "dev" }
```

Infrastructure-specific startup:

```fsharp
let storage = DurableStorage.s2 basin

let client =
    app
    |> DurableApp.clientWith
        { Storage = storage }

let worker =
    app
    |> DurableApp.workerWith
        { HostId = "host-a"
          Storage = storage
          MaxRunUntilIdleTicks = 100
          Timestamp = clock.NowMillis }
```

The default `client` and `worker` functions can read environment/configuration
to resolve storage for the named environment. The explicit `clientWith` and
`workerWith` paths are for service bootstrap, integration tests, and advanced
operators. Workflow authors should not need them.

This should hide `DurableRuntime.create`, default options, generated ids, and
client-scoped signal source allocation.

### Client API

```fsharp
let! instance = client.start Workflows.helloSequence "Tokyo"
let! status = client.status instance
let! _ = client.signal instance Signals.approved "alice"
```

The client should return domain-shaped values first:

```fsharp
type StartResult =
    | Started of InstanceId
    | StartRejected of DurableClientFailure

type InstanceStatus<'output> =
    | NotFound
    | Running
    | Waiting of WaitingOn list
    | Completed of 'output
    | Failed of string
```

The lower `DurableClientStartStatus` / `DurableClientStatusRead` can remain the
core representation.

## Usage Examples

### Hello Sequence

Target authoring experience:

```fsharp
module Activities =
    let sayHello =
        Activity.define "sayHello" (fun name -> async {
            return "Hello " + name + "!"
        })

module Workflows =
    let helloSequence =
        Workflow.define "helloSequence" (fun _ ->
            durable {
                let! tokyo = Durable.call Activities.sayHello "Tokyo"
                let! london = Durable.call Activities.sayHello "London"
                let! seattle = Durable.call Activities.sayHello "Seattle"

                return [ tokyo; london; seattle ]
            })

let app =
    durableApp {
        activity Activities.sayHello
        workflow Workflows.helloSequence
    }
```

This is the standard that the current tutorial fails to meet.

### Checkout Workflow

```fsharp
module Activities =
    let reserveInventory =
        Activity.define "reserveInventory" (fun orderId -> async {
            return "reservation:" + orderId
        })

    let chargeCard =
        Activity.define "chargeCard" (fun reservationId -> async {
            return "charge:" + reservationId
        })

module Workflows =
    let checkout =
        Workflow.define "checkout" (fun orderId ->
            durable {
                let! reservation = Durable.call Activities.reserveInventory orderId
                let! charge = Durable.call Activities.chargeCard reservation
                return charge
            })

let app =
    durableApp {
        activity Activities.reserveInventory
        activity Activities.chargeCard
        workflow Workflows.checkout
    }
```

### External Signal

```fsharp
module Signals =
    let approved = Signal.define<string> "approved"

module Workflows =
    let approval =
        Workflow.define "approval" (fun orderId ->
            durable {
                let! approver = Durable.waitForSignal Signals.approved
                return orderId + ":approved-by:" + approver
            })

let! instance = client.start Workflows.approval "order-1"
let! _ = client.signal instance Signals.approved "alice"
let! status = client.status instance
```

The user should not care about signal source ids or source sequence numbers.
Those are client/storage-adapter concerns below the authoring surface.

### Signal Or Timeout

```fsharp
module Workflows =
    let approvalOrTimeout =
        Workflow.define "approvalOrTimeout" (fun deadline ->
            durable {
                let! winner =
                    Durable.any
                        [ Durable.raceSignal Signals.approved
                          Durable.raceTimer deadline ]

                match winner with
                | SignalWon Signals.approved approver -> return "approved:" + approver
                | TimerWon _ -> return "timed-out"
            })
```

The current `RaceResult` shape exposes indexes and lower event keys. That is
acceptable internally but too awkward for the authoring facade.

### Local Test

```fsharp
let test =
    durableTest {
        app app
        workflow Workflows.checkout
        input "order-1"

        expectCompleted "charge:reservation:order-1"
    }
```

Lowering can still use the current `DurableRuntime.Host.RunUntilIdle` core, but
tests should not force every developer to write the same host loop and status
pattern matching.

### Production Host

```fsharp
let worker =
    app
    |> DurableApp.worker { HostId = "checkout-worker-1"; Environment = "prod" }

do! worker.runForever cancellationToken
```

`runForever` can come later. The important ergonomic point is that application
code should create a worker from the app definition, not manually claim
instances.

## Proposed Modules

```text
Eff.Foundation.Durable
  Semantics.fs          existing replay core
  Api.fs                current low-level workflow CE helpers
  Registry.fs           existing registries
  Runtime.fs            existing lower-level composition core

Eff.Foundation.Durable.App
  Definitions.fs        Activity<'i,'o>, Workflow<'i,'o>, Signal<'a>
  AppBuilder.fs         durableApp { activity ...; workflow ... }
  Client.fs             DurableApp.client / clientWith
  Worker.fs             DurableApp.worker / workerWith
  Storage.fs            DurableStorage.s2 for infrastructure bootstrap
  TestHost.fs           durableTest { ... }
```

The app layer can live inside `src/Foundation/Durable/App/` or as a later
package boundary. Keeping it in the same assembly initially is fine.

## Lowering Strategy

### Activities

```fsharp
type Activity<'input, 'output> =
    { Name: ActivityName
      EncodeInput: 'input -> Payload
      DecodeOutput: Payload -> 'output
      Handler: 'input -> Async<'output> }
```

Initial version:

- support `string -> Async<string>` directly
- add codecs after the string-only authoring facade is proven

Registration lowering:

```fsharp
ActivityRegistry.register
    activity.Name
    (fun payload ->
        async {
            let input = activity.DecodeInput payload
            let! output = activity.Handler input
            return activity.EncodeOutput output
        })
```

### Workflows

```fsharp
type Workflow<'input, 'output> =
    { Name: WorkflowName
      EncodeInput: 'input -> Payload
      DecodeOutput: Payload -> 'output
      Factory: 'input -> Durable<'output> }
```

Lowering:

```fsharp
WorkflowRegistry.register
    workflow.Name
    (fun payload ->
        let input = workflow.DecodeInput payload
        workflow.Factory input |> Durable.map workflow.EncodeOutput)
```

### Calls

```fsharp
Durable.call activity input
```

lowers to existing:

```fsharp
Workflow.call (ActivityName.value activity.Name) (activity.EncodeInput input)
|> Durable.map activity.DecodeOutput
```

### Signals

```fsharp
type Signal<'payload> =
    { Name: string
      Encode: 'payload -> Payload
      Decode: Payload -> 'payload }
```

`Durable.waitForSignal signal` lowers to current `Workflow.waitForSignal`.
`client.signal instance signal payload` lowers to the current lower-level
client `RaiseSignal`.

## Implementation Sequence

### PR 1: String-Typed Authoring Facade

Implement:

- `Activity.define : string -> (string -> Async<string>) -> Activity<string,string>`
- `Workflow.define : string -> (string -> Durable<string>) -> Workflow<string,string>`
- `Signal.define : string -> Signal<string>`
- `Durable.call : Activity<string,string> -> string -> Durable<string>`
- `Durable.waitForSignal : Signal<string> -> Durable<string>`
- `durableApp` builder
- `DurableApp.client`
- `DurableApp.worker`

Proof/example:

- replace the tutorial's registry-building section with `durableApp`
- prove `helloSequence` and `approval` complete through the app facade

### PR 2: Typed Client And Worker Facade

Implement:

- `client.start workflow input`
- `client.startWith instance workflow input`
- `client.signal instance signal payload`
- `client.status workflow instance`
- `worker.runOnce instance`
- `worker.runUntilIdle instance`

Proof/example:

- start typed workflow without passing `WorkflowName`
- status decodes typed completion payload

### PR 3: Race Facade Polish

Implement:

- `Durable.raceSignal`
- `Durable.raceTimer`
- facade-level `SignalWon` / `TimerWon`

Proof/example:

- signal-vs-timeout tutorial reads without indexes or raw `EventKey`.

### PR 4: Test DSL

Implement:

- `durableTest { app ...; workflow ...; input ...; expectCompleted ... }`
- optional step expectations: `expectWaiting`, `sendSignal`, `advanceTime`

Proof/example:

- checkout and approval tests no longer manually call `RunUntilIdle`.

### PR 5: Package/Docs Polish

Implement:

- one top-level README quickstart
- examples split by concept, Restate-style:
  - `01-hello-sequence`
  - `02-checkout`
  - `03-external-signal`
  - `04-signal-timeout`
  - `05-tests`
- CI transpiles all examples

## Acceptance Bar

A new developer should be able to copy a hello-sequence sample and understand
the product surface without knowing:

- S2 stream names
- fences
- inbox fold
- command adapters
- source sequence numbers
- `WorkflowRegistry`
- `ActivityRegistry`
- `DurableHostTickStatus`

Those concepts remain documented for maintainers and advanced operators, but
they should not appear in the first five minutes of usage.

## Non-Goals For This Layer

- no new durability semantics
- no new commit path
- no replacement of proof harness
- no daemon scheduler in the first PR
- no generic serialization system in the first PR

The point is to hide the working lower layers behind a humane authoring API.
