# eff-firegrid

`eff-firegrid` is an F#/Fable playground and library for building durable,
S2-backed workflows. The durable app facade lets application code define:

- activities: async functions with durable inputs and outputs
- workflows: deterministic orchestration over activities, timers, and signals
- signals: external events delivered to a workflow instance
- clients and workers: start, signal, observe, and execute durable instances

The lower durable substrate is still available for proofs and diagnostics, but
normal app code should start with `Eff.Foundation.Durable.App`.

## Requirements

- .NET SDK
- Node.js and npm
- S2 credentials, or an S2 CLI configuration, for examples/proofs that use live
  S2-backed execution

Install dependencies and restore local tools:

```sh
npm install
npm run tools:restore
```

## Build And Check

```sh
npm run format
npm run check
```

Focused commands:

```sh
npm run build
npm run test
npm run smoke:examples
npm run proofs -- --proof durable-production-usecases
```

## Hello Sequence Durable App

```fsharp
open Eff.Foundation.Durable
open Eff.Foundation.Durable.App

module D = Eff.Foundation.Durable.App.Durable

let greet =
    Activity.define "hello.greet" (fun name -> async { return "Hello, " + name })

let punctuate =
    Activity.define "hello.punctuate" (fun message -> async { return message + "!" })

let helloSequence =
    Workflow.define "hello.sequence" (fun name ->
        durable {
            let! greeting = D.call greet name
            return! D.call punctuate greeting
        })

let app =
    durableApp {
        activity greet
        activity punctuate
        workflow helloSequence
    }
```

Create a client and worker:

```fsharp
let client =
    app
    |> DurableApp.client
        { Environment = "dev"
          BasinName = None }

let worker =
    app
    |> DurableApp.worker
        { Environment = "dev"
          BasinName = None
          HostId = "hello"
          MaxRunUntilIdleTicks = Some 10 }
```

Start and run an instance:

```fsharp
let run =
    async {
        let! started = client.start helloSequence "Ada"

        match started with
        | DurableAppStartResult.Rejected failure -> failwith ("start failed: " + string failure)
        | DurableAppStartResult.Started instanceId ->
            let! _ = worker.runUntilIdle instanceId
            let! status = client.statusOf helloSequence instanceId
            printfn "%A" status
    }
```

## S2 Environment Bootstrap

`DurableApp.client` and `DurableApp.worker` resolve configuration from the
requested environment name first, then from global fallbacks.

For `Environment = "dev"`:

```sh
export EFF_FIREGRID_DEV_BASIN="your-basin"
export EFF_FIREGRID_DEV_ACCESS_TOKEN="your-token"
export EFF_FIREGRID_DEV_S2_ACCOUNT_ENDPOINT="https://..."
export EFF_FIREGRID_DEV_S2_BASIN_ENDPOINT="https://..."
```

Fallback names are also accepted:

```sh
export EFF_FIREGRID_BASIN="your-basin"
export EFF_FIREGRID_ACCESS_TOKEN="your-token"
export EFF_FIREGRID_S2_ACCOUNT_ENDPOINT="https://..."
export EFF_FIREGRID_S2_BASIN_ENDPOINT="https://..."
```

If credentials and endpoints are not supplied through environment variables,
the runtime falls back to S2 CLI configuration.

## Examples

Concept-focused durable examples live under `examples/durable`:

- `01-hello-sequence`
- `02-checkout`
- `03-external-signal`
- `04-signal-timeout`
- `05-tests`

Compile all examples:

```sh
npm run smoke:examples
```

The older end-to-end tutorial remains at
`examples/durable-tutorial/src/Tutorial.fsx`.

## Proofs

Run all proofs:

```sh
npm run proofs
```

Run focused durable proofs:

```sh
npm run proofs -- --proof durable-test-host
npm run proofs -- --proof durable-production-usecases
npm run proofs -- --proof durable-worker-service-loop
```

## Design Docs

Start with these for the durable execution library:

- `docs/durable-execution-library-sdd.md`
- `docs/durable-ergonomics-proposal.md`
- `docs/proof-runner-proposal.md`

Deeper substrate and foundation docs:

- `docs/durable-fsharp-sdd-r2-substrate.md`
- `docs/foundational-sdd.md`
