# Fluent Firegrid Infrastructure Library SDD

## Status

Working product specification for turning the current F#/Fable durable substrate
into a usable Firegrid infrastructure library.

This document supersedes `docs/durable-ergonomics-proposal.md` as the next
phase contract. The lower-level durable substrate and proof documents remain
implementation references. This SDD is about the developer-facing library.

The guiding correction is:

> Firegrid should make durable code look like ordinary F# code with the
> smallest possible set of durable substitutions.

## Problem

The repository already contains meaningful durable execution work: S2-backed
foundation primitives, durable step records, replay/status logic, signal/timer
support, app facade work, and a proof harness. The product surface is still too
low-level. It asks users to understand runtimes, registries, workers, storage
wiring, and proof concepts before they can write useful code.

That is not the desired infrastructure library.

The target library should be closer to:

- normal F# domain functions
- a thin durable wrapper around those functions
- first-class typed durable steps
- a `durable {}` computation expression that reads like `async {}`
- ergonomic local/test hosting
- S2-backed hosting hidden behind configuration
- durable actor-like entities only when stateful keyed behavior is needed

## Reference Inputs

### DurableFunctions.FSharp

Reference:

- <https://mikhail.io/2018/12/fairy-tale-of-fsharp-and-durable-functions/>

The important lesson is the transformation from ordinary F# code to durable F#
code.

Ordinary domain workflow:

```fsharp
let workflow wishlist = async {
    let! matches =
        wishlist.Wishes
        |> List.map findMatchingGift
        |> Async.Parallel

    let gift = pickGift (List.concat matches)
    let reservation = { Kid = wishlist.Kid; Product = gift }

    do! reserve reservation
    return reservation
}
```

Durable workflow should be almost the same:

```fsharp
let workflow wishlist = durable {
    let! matches =
        wishlist.Wishes
        |> List.map (call findMatchingGift)
        |> Durable.Parallel

    let! gift = call pickGift (List.concat matches)
    let reservation = { Kid = wishlist.Kid; Product = gift }

    do! call reserve reservation
    return reservation
}
```

The durable substitutions are:

- `async` orchestration body becomes `durable`
- direct nondeterministic function call becomes `call` of a first-class step
- `Async.Parallel` / `Task.WhenAll` becomes `Durable.Parallel`
- fixed-width parallel binds use the normal F# `and!` CE syntax
- durable external waits use `waitFor`

Do not force the user to pass a context around for every operation when a
computation expression can carry it.

### Durable Functions Semantics Paper

Reference:

- <https://angelhof.github.io/files/papers/durable-functions-2021-oopsla.pdf>

The paper's useful vocabulary:

- **activities**: stateless functions called by orchestrations
- **orchestrations**: deterministic async workflows using record/replay
- **entities**: durable actors with state and serialized operations
- **signals/messages**: asynchronous communication
- **workers**: volatile executors over durable state
- **record/replay**: implementation mechanism for workflow progress

Public Firegrid names should be more F#-ergonomic where appropriate:

- use `step` publicly for activity-like durable work
- use `workflow` publicly for orchestration definitions
- use `entity` publicly for durable actor/state abstractions
- use `operation` only where entity actor semantics need a named method
- keep worker/replay/storage vocabulary in advanced docs

### Restate SDK

References:

- <https://github.com/restatedev/sdk-typescript/tree/main/packages/libs/restate-sdk>
- <https://docs.restate.dev/foundations/services>
- <https://docs.restate.dev/develop/ts/services>
- <https://docs.restate.dev/develop/ts/durable-steps>
- <https://docs.restate.dev/develop/ts/state>
- <https://docs.restate.dev/develop/ts/external-events>
- <https://docs.restate.dev/develop/ts/concurrent-tasks>
- <https://docs.restate.dev/develop/ts/serving>
- <https://docs.restate.dev/develop/ts/testing>

Restate's useful product boundaries:

- services, virtual objects, and workflows have different concurrency/state
  semantics
- durable operations are explicit
- objects/entities are keyed and stateful
- workflows are long-running and externally interactable
- serving should be one obvious operation over an app definition
- local testing is a first-class development path

Firegrid should preserve these semantics without copying TypeScript-shaped APIs.

### Fluent Firegrid

Reference:

- <https://github.com/gurdasnijor/firegrid/tree/main/packages/fluent-firegrid>

Important lessons:

- public package surface should be small
- substrate details must stay below the user-code edge
- definitions should carry bindable descriptors
- typed call/send clients derive from those descriptors
- public-surface tests matter as much as low-level correctness tests

## Product Objective

Build `Eff.Firegrid` into a consumable F#/Fable infrastructure library for:

1. defining durable steps from ordinary F# functions
2. composing steps into workflows with minimal syntactic overhead
3. modeling keyed durable actors as entities
4. running and testing locally without S2
5. swapping to S2-backed durable execution without changing user code
6. hosting CLI/ACP agents as durable Firegrid participants
7. building Home Assistant tools on top of the same substrate

## Non-Goals For The Next Phase

- Do not add more substrate-first public APIs.
- Do not expose `DurableRuntime` as a normal user concept.
- Do not require users to manually assemble registries.
- Do not make examples mention basins, sequence numbers, stream names, inbox
  folds, or worker ticks.
- Do not grow proof-only code unless it protects public behavior.
- Do not clone all of Restate before the Firegrid use cases work.
- Do not choose an API that reads like a TypeScript SDK translated into F#.

## Design Principles

### 1. Functions First

Users should start with ordinary domain functions:

```fsharp
let sendEmail email = async {
    return! Email.send email
}

let agentTurn request = async {
    return! Agent.run request
}
```

Firegrid lifts these functions into durable values:

```fsharp
let sendEmailStep = step "sendEmail" sendEmail
let agentTurnStep = step "agentTurn" agentTurn
```

### 2. Durable Substitutions, Not Framework Ceremony

The workflow body should look like normal F# control flow:

```fsharp
let signup user = durable {
    do! call sendEmailStep user.Email

    let! approved =
        waitFor<string> "approved"

    return approved
}
```

Avoid APIs like:

```fsharp
ctx.callActivity "sendEmail" user.Email
ctx.waitForSignal<string> "approved"
```

Those make users reason about runtime taxonomy instead of their domain flow.

### 3. First-Class Durable References

Durable operations should call typed values, not strings:

```fsharp
let! reply =
    call agentTurnStep { Memory = memory; Prompt = prompt }
```

Strings are allowed at definition boundaries for stable durable names:

```fsharp
let agentTurnStep = step "agentTurn" Agent.turn
```

### 4. Entity Means Durable Actor

An `entity` is Firegrid's durable actor abstraction:

- it has an identity/key
- it owns private durable state
- it receives calls/messages
- mutations for one key are serialized
- reads can be modeled as views/queries
- state recovers through the substrate

This is conceptually close to `Fable.Actor`, Orleans grains, Restate virtual
objects, Cloudflare Durable Objects, and Azure Durable Entities. Firegrid should
say "durable actor" in docs, but keep the public primitive named `entity` to
avoid implying the full actor-system vocabulary of supervision, links, and
mailbox receive loops.

### 5. Context Is Scoped, Not Pervasive

Plain steps should not receive a Firegrid context unless they need one.

Workflow code should primarily use CE operations:

```fsharp
call
send
Durable.Parallel
and!
race
timeout
waitFor
```

Entity operation code may need an entity context because state is load-bearing:

```fsharp
let ask prompt = entityAction {
    let! memory = state.getOrDefault "memory" []
    let! reply = call agentTurnStep { Memory = memory; Prompt = prompt }
    do! state.set "memory" (reply :: memory)
    return reply
}
```

The CE can carry the context; users should not pass it manually through every
line.

## Target Public API

### Steps

Steps are durable named functions.

```fsharp
let findMatchingGift =
    step "FindMatchingGift" Domain.findMatchingGift

let pickGift =
    step "PickGift" Domain.pickGift

let reserve =
    step "Reserve" Domain.reserve
```

Required surface:

```fsharp
type Step<'input, 'output>

val step :
    name: string ->
    fn: ('input -> Async<'output>) ->
    Step<'input, 'output>

val stepWith :
    name: string ->
    encodeInput: ('input -> string) ->
    decodeInput: (string -> 'input) ->
    encodeOutput: ('output -> string) ->
    decodeOutput: (string -> 'output) ->
    fn: ('input -> Async<'output>) ->
    Step<'input, 'output>
```

Task-returning steps are a follow-up. This repository currently targets Fable,
and the present Fable toolchain used here does not compile the stock F# `task`
builder or `Async.AwaitTask` path.

### Workflows

Workflows are durable orchestrations expressed with a computation expression.

```fsharp
let fulfill wishlist = durable {
    let! matches =
        wishlist.Wishes
        |> List.map (call findMatchingGift)
        |> Durable.Parallel

    let! gift = call pickGift (List.concat matches)
    let reservation = { Kid = wishlist.Kid; Product = gift }

    do! call reserve reservation
    return reservation
}
```

Fixed-width parallel work uses normal F# computation-expression syntax:

```fsharp
let reserveAndGreet orderId = durable {
    let! reservation = call reserve orderId
    and! greeting = call greet orderId

    return reservation, greeting
}
```

Required surface:

```fsharp
type Workflow<'input, 'output>
type Durable<'output>

val workflow :
    name: string ->
    fn: ('input -> Durable<'output>) ->
    Workflow<'input, 'output>

val call : Step<'input, 'output> -> 'input -> Durable<'output>
module Durable =
    val Parallel : Durable<'a> seq -> Durable<'a list>
val waitFor<'payload> : name: string -> Durable<'payload>
val signal : name: string -> Signal<string>
val signalWith :
    name: string ->
    encode: ('payload -> string) ->
    decode: (string -> 'payload) ->
    Signal<'payload>
val waitForSignal : Signal<'payload> -> Durable<'payload>
```

Near-follow-up surface:

```fsharp
val send : Step<'input, 'output> -> 'input -> Durable<InvocationId>
val race : Durable<'a> seq -> Durable<'a>
val timeout : deadline: Duration -> Durable<'a> -> Durable<TimeoutResult<'a>>
```

Example registration:

```fsharp
let fulfillment =
    workflow "WishlistFulfillment" fulfill

let app =
    firegrid {
        workflow fulfillment
    }
```

Workflow registration should discover step dependencies when possible, but
explicit step registration is acceptable if it produces better diagnostics:

```fsharp
let app =
    firegrid {
        step findMatchingGift
        step pickGift
        step reserve
        workflow fulfillment
    }
```

### Entities

Entities are durable actors. They are needed when code owns keyed state or
requires serialized mutation by key.

Preferred authoring shape:

```fsharp
module Assistant =
    let agentTurnStep =
        step "agentTurn" Agent.turn

    let ask prompt = entityAction {
        let! memory = state.getOrDefault "memory" []

        let! reply =
            call agentTurnStep { Memory = memory; Prompt = prompt }

        do! state.set "memory" (reply :: memory)
        return reply
    }

    let status () = entityView {
        return! state.getOrDefault "status" "idle"
    }

let assistant =
    entity "assistant" {
        operation "ask" Assistant.ask
        view "status" Assistant.status
    }
```

Required surface:

```fsharp
type Entity<'state>
type EntityOp<'input, 'output>
type EntityView<'input, 'output>
type EntityAction<'output>
type EntityViewAction<'output>

val entity :
    name: string ->
    builder: EntityBuilder<'state> ->
    Entity<'state>

val operation :
    name: string ->
    fn: ('input -> EntityAction<'output>) ->
    EntityOp<'input, 'output>

val view :
    name: string ->
    fn: ('input -> EntityViewAction<'output>) ->
    EntityView<'input, 'output>
```

Entity CE operations:

```fsharp
module state =
    val get<'a> : key: string -> EntityAction<'a option>
    val getOrDefault<'a> : key: string -> defaultValue: 'a -> EntityAction<'a>
    val set<'a> : key: string -> value: 'a -> EntityAction<unit>
    val clear : key: string -> EntityAction<unit>

val call : Step<'input, 'output> -> 'input -> EntityAction<'output>
val send : Step<'input, 'output> -> 'input -> EntityAction<InvocationId>
```

Rules:

- operations may read/write state
- views may read state only
- operations for the same entity key are serialized
- views may run concurrently when the substrate can support it
- different entity keys can execute independently

### Services

Services are optional grouping for stateless public operations. They should not
be required for simple apps that only define workflows and steps.

Use services when exposing RPC/tool surfaces:

```fsharp
module Greeter =
    let greet name = async {
        return $"Hello {name}!"
    }

let greeter =
    service "greeter" {
        operation "greet" Greeter.greet
    }
```

Services may lower to steps internally when durability is needed, or to plain
request dispatch when they are purely ingress-facing. This is an implementation
choice; user code should not need to know.

### App And Hosting

Minimal local app:

```fsharp
let app =
    firegrid {
        step findMatchingGift
        step pickGift
        step reserve
        workflow fulfillment
    }

do! Firegrid.serve app
```

Explicit host config:

```fsharp
do! Firegrid.serve {
    app app
    port 9080
    storage (Storage.s2FromEnv ())
}
```

The normal user noun is `serve`, not `worker`. The current client/worker/runtime
split stays beneath this API.

### Clients

Clients should call durable references:

```fsharp
let client = Firegrid.client app

let! started =
    client.start fulfillment wishlist

let! result =
    client.result fulfillment started

let! askResult =
    client.entity(assistant, "home").call Assistant.ask "turn on kitchen lights"
```

If F# method inference makes fluent clients awkward, prefer explicit typed helper
functions over string APIs. Do not regress to:

```fsharp
client.call "assistant" "ask" payload
```

except as an advanced dynamic escape hatch.

### Testing

Tests should be local and substrate-free by default:

```fsharp
let host = Firegrid.TestHost.create app

let! result =
    host.run fulfillment wishlist

Expect.equal expected result
```

Entity test:

```fsharp
let assistant = host.entity(assistant, "home")

let! reply =
    assistant.call Assistant.ask "turn on the lights"

let! status =
    assistant.view Assistant.status ()
```

Replay behavior should be testable through public APIs:

```fsharp
let! first = host.run fulfillment wishlist
let! replay = host.replay fulfillment first.InvocationId

Expect.equal first.Result replay.Result
Expect.stepExecutedOnce findMatchingGift first.Trace
```

The existing proof runner should support these tests; it should not be the only
way to write them.

## CLI Agent Use Case

The near-term proof of product value is running a local CLI/ACP agent as a
durable Firegrid participant.

Public shape:

```fsharp
let claude =
    cliAgent "claude" {
        command "npx"
        args [ "-y"; "@agentclientprotocol/claude-agent-acp@0.36.1" ]
        protocol Acp
        secretEnv "ANTHROPIC_API_KEY"
    }

let agentTurnStep =
    step "agentTurn" (fun request -> async {
        return! claude.run request
    })

module Assistant =
    let ask prompt = entityAction {
        let! memory = state.getOrDefault "memory" []
        let! reply = call agentTurnStep { Memory = memory; Prompt = prompt }
        do! state.set "memory" (reply :: memory)
        return reply
    }
```

Implementation guidance:

- model subprocess ownership after `fluent-acp-process`
- process lifecycle, stdin/stdout/stderr, cancellation, timeout, and exit status
  are explicit at the boundary
- ACP is an adapter on top of raw process ownership
- Firegrid tools become durable steps
- Home Assistant tools become ordinary durable steps
- secrets must not enter logs or transcripts

Target command-line compatibility:

```sh
pnpm firegrid -- run \
  --agent claude-acp \
  --agent-protocol acp \
  --secret-env ANTHROPIC_API_KEY \
  --prompt "Use the Firegrid sleep tool once, then summarize what happened." \
  -- npx -y @agentclientprotocol/claude-agent-acp@0.36.1
```

## Architecture

```text
User F# functions
  -> step/workflow/entity values
    -> durable computation expressions
      -> Firegrid app descriptors
        -> local/test host
        -> S2 host adapter
          -> current durable substrate
            -> S2 foundation primitives
```

The current durable implementation is the adapter target. It should not leak
into examples.

## Implementation Sequence

### PR 1: F#-Native Step And Workflow Surface

Deliver:

- `Step<'input,'output>`
- `step` / `stepWith`
- `Durable<'output>` and `durable {}` CE
- `call`, `Durable.Parallel`, CE `and!`, `waitFor`, `signal`, `waitForSignal`
- `Workflow<'input,'output>` and `workflow`
- `Firegrid.clientWith`, `workerWith`, and `testHostWith`
- examples ported from the Mikhail workflow article shape
- public surface tests for first-class typed steps

Acceptance:

- first tutorial reads like ordinary F# with durable substitutions
- no tutorial mentions S2, runtime, registries, workers, or proofs
- users call step values, not string names

### PR 2: Test Host Over Public API

Deliver:

- `Firegrid.TestHost.create`
- `host.run workflow input`
- trace/assertion APIs for public durable behavior
- replay assertions that steps execute once
- migration of relevant `.fsx` proof examples to public test-host tests

Acceptance:

- examples execute without S2
- replay semantics are visible through public tests
- proof harness becomes support infrastructure, not the product surface

### PR 3: App Builder And Local Serve

Deliver:

- `firegrid { step ...; workflow ... }`
- `Firegrid.serve app`
- local in-memory durable store
- typed client start/result/status for workflows

Acceptance:

- local app can be served with one call
- user workflow code is unchanged between test host and local serve

### PR 4: Entity / Durable Actor Surface

Deliver:

- `entityAction {}` CE for operations
- `entityView {}` CE for read-only entity views
- state helpers: `get`, `getOrDefault`, `set`, `clear`, `stateKeys`
- entity descriptors and typed client/test-host calls
- same-key operation serialization tests

Acceptance:

- entity docs explain "durable actor"
- stateful assistant memory example works locally
- views cannot mutate state through public APIs

### PR 5: S2 Host Adapter

Deliver:

- descriptor lowering from steps/workflows/entities into existing durable core
- `Storage.s2FromEnv`
- `Firegrid.serve { app; storage; port }`
- S2 smoke tests gated on credentials

Acceptance:

- examples can switch from local to S2 by changing host config
- public code does not change

### PR 6: CLI Process Owner

Deliver:

- raw subprocess owner
- command/args/cwd/env/secret-env config
- stdin/stdout/stderr capture
- cancellation, timeout, nonzero exit, spawn failure behavior
- durable transcript model

Acceptance:

- one local command can run as a durable step
- lifecycle edge cases are tested
- no ACP assumptions in raw process owner

### PR 7: ACP Agent Adapter

Deliver:

- ACP framing over process owner
- `cliAgent` builder
- Firegrid tool registration
- sleep tool example
- transcript/status inspection

Acceptance:

- old CLI-agent command is functionally reproduced
- Firegrid sleep tool works through the agent
- secrets remain redacted

### PR 8: Home Assistant Tools

Deliver:

- Home Assistant config
- call-service tool
- get-state tool
- optional event wait tool
- durable assistant entity example

Acceptance:

- local Home Assistant workflow can run end to end
- tool calls are durable steps
- assistant memory/status are entity-backed

## Validation Strategy

Validation should move up the stack:

- public surface tests for `step`, `workflow`, `durable`, `entity`
- local host tests for normal user examples
- replay tests proving step results are reused
- entity tests for same-key serialization and state recovery
- process-owner tests for IO/lifecycle behavior
- S2 smoke tests behind credential gates
- substrate proofs only for invariants below the public API

## Documentation Requirements

The first README should show:

1. ordinary F# functions
2. one-line `step` lifting
3. a `durable {}` workflow that mirrors the ordinary function flow
4. local test host
5. local serve
6. entity as durable actor
7. CLI agent example

Advanced docs can then explain S2, replay, workers, and proofs.

## Open Design Decisions

1. How to add Task-returning steps in a way that remains compatible with Fable.
2. Whether `workflow` should register steps automatically from references or
   require explicit app registration for better diagnostics.
3. Exact representation of typed codecs across .NET and Fable.
4. Whether services are needed in MVP or can wait until workflows/entities/agent
   hosting are ergonomic.
5. Whether entity operations should be grouped by module conventions or only by
   explicit `entity "name" { operation ... }` declarations.
6. How far to push source generation for typed clients.

## Immediate Next Step

Start PR 1 with the F#-native durable surface:

```fsharp
let reserveStep = step "Reserve" reserve

let checkout order = durable {
    do! call reserveStep order
    return order.Id
}

let checkoutWorkflow = workflow "Checkout" checkout
```

Do not add another lower-level adapter before this surface exists and is tested.
