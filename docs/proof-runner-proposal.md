# Eff Firegrid Verification Proposal

## Status

Proposed. The verification surface is a **compiled, enforced proof suite**, not
a REPL/script workflow. First slice landed: the harness and the first
foundation proof are compiled modules under `src/Proofs/`, included in
`eff-firegrid.fsproj`, so `dotnet build`, FS1182, and `fsharplint` enforce that
proofs stay in sync with the production APIs they exercise. `npm run proofs`
runs the suite; it is part of `npm run check` / CI. See
[Compiled Proof Suite](#compiled-proof-suite).

This is a deliberate reversal of the earlier REPL-first framing: the
script-first promotion ladder was hard to systematize, enforce, and automate.
A loose `.fsx` only fails at runtime, is not type-checked by the build, and is
not linted. Moving proofs into the compiled project makes API drift a build
error and makes the suite a first-class, CI-gated artifact.

## Context

`eff-firegrid` is a Fable/F# system that wraps and composes the S2 JavaScript
SDK into F#-native APIs. The verification surface is a compiled proof suite that
exercises production modules directly:

- `src/Proofs/` holds the proof harness and the proofs as compiled modules,
  type-checked against the production surfaces they drive.
- `repl.fsx` remains the live S2 playground and ergonomics probe (exploration,
  not verification).
- `tests/Suite.fsx` remains the S2 client integration suite.
- `docs/foundational-sdd.md` defines the immediate path: prove the smallest
  useful durable-work kernel one property at a time.

The next step is not a full deterministic simulator. The next step is to grow
the proof suite top-down from the surfaces the SDD already specifies — write the
production primitive to its SDD signature, then a compiled proof that drives it
against real S2 — so each durable layer ships with an enforced proof rather than
a scratchpad that someone has to remember to run.

The later goal is an ergonomic proof/scenario runner for distributed behavior
in `eff-firegrid`:

- workflow run lifecycle
- durable timers and signals
- object ownership and serialization
- stale owner fencing
- host crash and restart
- schedule materialization and sweeper races
- exactly-once durable side effects
- linearizability of externally observed operations

This design is inspired by:

- S2 deterministic simulation testing: https://s2.dev/blog/dst
- Porcupine linearizability checking: https://github.com/anishathalye/porcupine
- The existing TypeScript proof runner in `fluent-firegrid/apps/proofs`

The TypeScript runner is useful later-stage prior art, but it should not be
copied literally. For `eff-firegrid`, verification should grow in this order:

1. A compiled, behavior-free harness (`src/Proofs/Harness.fs`) that the build
   and linter enforce.
2. Production primitives in `src/Foundation`, written to their SDD signatures.
3. Compiled proofs in `src/Proofs` that drive those primitives against real S2,
   registered once in `src/Proofs/Registry.fs`.
4. Structured evidence (operation histories, domain events) added to the
   harness when concurrency proofs need it.
5. A full proof runner and deterministic simulation grown from the same harness
   once the suite is broad enough to justify it.

## Goals

1. Make verification a compiled, enforced suite: proofs are type-checked by the
   build and linter and gated in CI, so API drift is a build error.
2. Exercise real `eff-firegrid` production modules, CLIs, and clients directly.
3. Prefer one durable property per proof section.
4. Build production primitives to their SDD signatures, then prove them.
5. Make authoritative records and derived folds easy to print and inspect.
6. Run the whole suite, or one proof by name, from a single command.
7. Preserve a path toward operation histories and replayable counterexamples.
8. Keep the harness generic and behavior-free so it can grow into a runner.
9. Create a clean path toward seed-based deterministic simulation and
   Porcupine-style linearizability checking.

## Non-Goals

1. Do not build the full proof runner (resources/hosts/faults/reports DSL) in
   the first iterations. Grow it from the compiled harness as the suite needs.
2. Do not build a full deterministic simulator in the first iteration.
3. Do not make SQL over traces the primary way to express application
   correctness.
4. Do not require every proof to use a linearizability checker.
5. Do not hide system behavior behind proof-local substitutes for the real
   runtime.
6. Do not couple proof definitions to S2 client operations only. S2 is the
   substrate, not the application-level behavior being proved.
7. Do not add repository-specific proof drivers, adapters, or fixture APIs that
   can fake workflow/object behavior. Proofs must exercise production exports,
   CLIs, HTTP handlers, and generated clients directly.

## Core Idea

Verification is a compiled proof suite over production modules — enforced by the
build, not a disciplined-but-optional REPL habit.

Each proof is a value (`Harness.Proof`) registered once in
`src/Proofs/Registry.fs`. Its body should:

1. State the durable property being proved.
2. Create isolated durable resources with readable names.
3. Drive production modules directly.
4. Print authoritative records and derived folds.
5. Check the property with simple assertions.
6. Register cleanup; the runner tears resources down (unless `PRESERVE=1`
   intentionally keeps a counterexample).

Because proofs are compiled into `eff-firegrid.fsproj`, `dotnet build` and
`fsharplint` check every proof against the real production signatures it calls.
A renamed field or changed return type breaks the build, not a script someone
runs by hand three weeks later.

The immediate unit is a registered proof. A fuller proof runner can later
formalize the same shape:

1. **Resources**: scoped dependencies such as S2 streams and production host
   processes.
2. **Scenario**: actors and clients drive the system with ordinary F# async
   programs.
3. **Evidence**: authoritative records, folds, operation histories, and domain
   events are captured.
4. **Verification**: check workload results, histories, invariants, SQL views,
   and optional linearizability models.

## Compiled Proof Suite

Proofs are compiled F#, not loose scripts. The suite has three compiled pieces
plus one thin launcher:

```text
src/Proofs/
  Harness.fs               # generic, behavior-free: Context, Proof, section,
                           # check/expectEqual/note, uniq, cleanup registry,
                           # S2 config, runProofs (the runner)
  SubjectHistoryProof.fs   # a proof: `let proof = Harness.proof "..." (fun ctx -> ...)`
  Registry.fs              # `let all = [ SubjectHistoryProof.proof; ... ]`

scripts/proofs.fsx         # thin Fable launcher: #loads the compiled modules,
                           # then `Harness.runProofs Registry.all`
```

All three `src/Proofs/*.fs` files are in `eff-firegrid.fsproj`, so they ride the
existing JS-interop compile path that `src/S2/*` already uses: `dotnet build`
type-checks them (with `--warnaserror:1182`), and `fsharplint` lints them. The
proofs run under Fable/Node — the S2 client is JS interop — so `scripts/proofs.fsx`
is a thin launcher that loads the same modules and runs the registry. The
launcher carries no proof logic; if it did, that logic would escape the build's
enforcement.

Why this beats the REPL/script approach on the three axes that were painful:

1. **Systematize.** A proof is a `Harness.Proof` value registered once. Adding
   one is a one-line edit to `Registry.fs`. There is no per-proof `npm` target,
   no `#load` ordering to hand-maintain, no copy-pasted `uniq`/`check`/`section`.
2. **Enforce.** Because proofs are compiled against the real production
   modules, a renamed field or changed signature is a `dotnet build` failure.
   A loose `.fsx` would only fail when someone remembered to run it.
3. **Automate.** `npm run proofs` runs the whole suite (or `PROOF=<substring>`
   for one) and exits non-zero on failure. It is wired into `npm run check`,
   so CI runs it alongside `build`, `lint`, and `test`.

Harness conventions:

- **Cleanup is registered, not inlined.** `Harness.onCleanup` records a teardown
  thunk next to the resource it frees; the runner runs them last-in-first-out.
- **Counterexample preservation is a flag.** Cleanup runs by default; with
  `PRESERVE=1`, a proof that fails keeps its resources and the runner prints
  what it left behind.
- **Config is environment-driven.** The basin defaults to `test-basin-885234`
  and is overridable with `S2_BASIN`.

The harness must stay generic: no workflow/object behavior, hidden stores, fake
clients, event mappers, or alternate interpreters. `repl.fsx` (playground) and
`tests/Suite.fsx` (S2 client integration suite) keep their own small scaffolds;
they are not proofs and are intentionally left as-is.

## Recommended Repo Shape

Near-term (exists now or next):

```text
src/
  Foundation/
    SubjectHistory.fs      # exists
    StateView.fs           # next, to its SDD signature
    KvStore.fs             # next, to its SDD signature

  Proofs/
    Harness.fs             # exists — generic, behavior-free
    SubjectHistoryProof.fs # exists — drives SubjectHistory
    StateViewProof.fs      # next, alongside StateView
    KvStoreProof.fs        # next, alongside KvStore
    Registry.fs            # exists — the one list of all proofs

scripts/
  proofs.fsx               # thin Fable launcher (no proof logic)
```

Later-stage, as the suite grows and the harness needs to formalize (do not
build ahead of need):

```text
src/Proofs/                # grown from Harness.fs as proofs demand it
  Evidence.fs              # operation histories, domain events
  Reports.fs               # JSON trial reports
  Hosts.fs                 # process-host lifecycle
  Faults.fs                # fault plans
  Linearizability.fs       # model + checker adapter
  Sql.fs                   # SQL views over evidence

proofs/                    # distributed-behavior proofs + checker models
  Models/{Counter,WorkflowRun,Lease,Register}.fs
  StoreHostCrashRestart.fs
  ObjectSerialization.fs
  RuntimeScheduleSweep.fs
  StaleOwnerFencing.fs
  Main.fs
```

`src/Foundation` is the near-term production target from
`docs/foundational-sdd.md`. Each layer ships with a compiled proof in
`src/Proofs` that drives it against real S2 streams.

`src/Proofs/Harness.fs` is generic: it knows about resources, sections,
checks, cleanup, and (later) hosts, operations, evidence, reports, faults, and
checkers — never about specific `eff-firegrid` workflow or object APIs. The
later `proofs/` directory holds distributed-behavior proof definitions and
checker models that define the intended sequential semantics of a subsystem,
but no domain-specific support layers that wrap or simulate the system under
test. A proof may import production modules from `src`, start production CLIs as
processes, or call production HTTP/client surfaces, but it must not introduce a
verification-only workflow host, object client, runtime facade, or event
adapter.

If a proof needs an easier way to start or call a subsystem, that is a signal
that the production subsystem needs a clearer entrypoint. Add that entrypoint
to production code, then use it from both proofs and real deployments.

## Runtime Modes

### Mode 1: Live Process

The initial mode should run real Fable/Node processes, a real `s2 lite`
process, and real host crashes.

This mode can prove important behavior immediately:

- persisted state survives host death
- stale owners are fenced by durable stream evidence
- schedule sweepers race through the real store
- same-key object calls serialize through the real owner path

This mode is not deterministic simulation. It is a real-process scenario
runner with structured evidence and reproducible reports.

### Mode 2: Controlled Process

The second mode should still run real processes, but inject runner-owned
services:

- virtual clock
- seedable random source
- fault points
- network/client wrappers
- deterministic port allocation

This mode moves proofs toward replayability without requiring a full simulator.

### Mode 3: Simulated

The final mode should run distributed components against simulated time,
transport, storage, scheduling, and failure. This is the DST-style endgame.

This requires production code to depend on interfaces for time, randomness,
transport, scheduling, and fault points.

## Production Hooks When Needed

To scale beyond simple proofs, production code eventually needs narrow
dependencies that proofs and the later proof runner can control. Add these
only when a durable property needs them; do not pre-build a simulation surface
before the kernel exists.

```fsharp
type IClock =
    abstract NowMillis: unit -> int64
    abstract Sleep: int64 -> Async<unit>

type IRandom =
    abstract NextInt: min: int * max: int -> int
    abstract NextBytes: count: int -> byte[]

type IFaultPoint =
    abstract Reach: name: string * attributes: Map<string, string> -> Async<unit>
```

`IFaultPoint` is especially important. Killing a process after an exported span
is coarse. Crash proofs often need semantic cut points:

```fsharp
do! faultPoint.Reach(
    "workflow.journal.appended.before-ack",
    Map [ "runId", runId; "eventType", "STEP_FINISHED" ]
)
```

The runner can then block, crash, resume, or reorder exactly at that point.

## Evidence Model

The eventual proof runner should record three evidence layers. Before the
runner exists, proofs should print and optionally persist the same concepts in
the simplest useful form.

### Operation Evidence

Operation evidence is client-observed call/return history. This is the primary
input for linearizability checks.

```fsharp
type OperationStatus =
    | Ok
    | Error
    | Interrupted

type OperationEvent =
    { TrialId: string
      ClientId: string
      OperationId: string
      Name: string
      Key: string option
      InputJson: string
      OutputJson: string option
      ErrorJson: string option
      Status: OperationStatus
      CallTimeNanos: int64
      ReturnTimeNanos: int64 }
```

This must be a first-class artifact, not reconstructed from raw traces.

### Domain Event Evidence

Domain event evidence is application-specific but normalized enough for
queries and invariant checks.

```fsharp
type DomainEvent =
    { TrialId: string
      Category: string
      Name: string
      Subject: string option
      Attributes: Map<string, string>
      DataJson: string option
      TimeNanos: int64 }
```

Examples:

- `workflow.event.persisted`
- `workflow.run.finished`
- `object.owner.claimed`
- `object.call.enqueued`
- `lease.acquired`
- `timer.scheduled`
- `schedule.materialized`

### Trace Evidence

Traces and spans remain valuable for diagnostics and low-level checks. They
should be exported and linked to the same trial id.

However, traces should not be the only source of operation history. Span names
and attributes are too loose for linearizability and model checking.

## Report Format

Every trial should write a self-contained JSON report when requested or when a
failure occurs.

```json
{
  "proof": "store.runtime-schedule-sweep",
  "trialId": "store.runtime-schedule-sweep-2026-06-26T18-00-00Z",
  "seed": 123456,
  "mode": "live-process",
  "status": "failed",
  "failedCheck": "single scheduled run completed",
  "replayCommand": "npm run proofs -- --proof store.runtime-schedule-sweep --seed 123456",
  "resources": [],
  "faultPlan": [],
  "operations": [],
  "domainEvents": [],
  "spanSummary": [],
  "counterexample": {}
}
```

Required fields:

- proof name
- trial id
- seed
- mode
- status
- failing check
- replay command
- operation history
- domain events
- fault plan
- resource metadata

## F# DSL Shape

The DSL should feel like normal F# async code with explicit proof boundaries.
The `Eff.Production.*` names below are placeholders for real production
exports. They are not verification-owned facades.

```fsharp
proof "store.host-crash-restart" {
    description
        "A workflow host pauses on a durable timer, dies, restarts, sweeps, and completes from S2 state."

    resources {
        use! s2 =
            processHost "s2-lite" {
                command "s2"
                args [ "lite"; "--port"; "${port}"; "--local-root"; "${tempDir}" ]
                readiness "/"
                exposeEndpoint "S2_ENDPOINT"
            }

        use! host =
            processHost "worker" {
                command "node"
                args [ "build/src/Program.js"; "workflow-host"; "--port"; "${port}" ]
                env [ "S2_ENDPOINT", s2.Endpoint ]
                readiness "/ready"
                exposeEndpoint "WORKER_ENDPOINT"
            }

        let workflow = Eff.Production.WorkflowClient.connect host.Endpoint
    }

    scenario {
        let! started =
            op "workflow.start" {
                client "driver"
                key "crash-sleep:run-1"
                input {| workflowId = "crash-sleep"; runId = "crash-sleep:run-1" |}
                run workflow.StartRun {
                    workflowId = "crash-sleep"
                    runId = "crash-sleep:run-1"
                    input = {| sleepUntil = 5000 |}
                    now = 1000
                }
            }

        do! Fault.killHost "worker"
        do! Fault.restartHost "worker"

        let! tick =
            op "workflow.sweep" {
                client "driver"
                key "crash-sleep:run-1"
                input {| now = 5000 |}
                run workflow.Tick { now = 5000 }
            }

        let! execution =
            op "workflow.load" {
                client "driver"
                key "crash-sleep:run-1"
                run workflow.LoadExecution "crash-sleep:run-1"
            }

        return {| Started = started; Tick = tick; Execution = execution |}
    }

    verify {
        workload (fun r ->
            r.Execution.Status = "finished"
            && r.Execution.Output = box {| completed = true |})

        workflowEvents "run events are exactly once" {
            runId "crash-sleep:run-1"
            events [
                "STEP_FINISHED"
                "SIGNAL_AWAITED"
                "SIGNAL_RESOLVED"
                "STEP_FINISHED"
                "RUN_FINISHED"
            ]
        }

        evidenceSql "host was killed and restarted" """
            SELECT countIf(name = 'host.kill' AND subject = 'worker') = 1
               AND countIf(name = 'host.start' AND subject = 'worker') = 2 AS ok
            FROM domain_events
        """
    }
}
```

The example is intentionally application-level. It proves workflow behavior,
not just S2 append/read behavior.

## Complex Proof Examples

### Object Same-Key Serialization

This proof verifies that concurrent same-key calls serialize through the object
owner and do not lose writes.

```fsharp
proof "object.same-key-serialization" {
    resources {
        use! s2 =
            processHost "s2-lite" {
                command "s2"
                args [ "lite"; "--port"; "${port}"; "--local-root"; "${tempDir}" ]
                readiness "/"
                exposeEndpoint "S2_ENDPOINT"
            }

        use! host =
            processHost "objects" {
                command "node"
                args [ "build/src/Program.js"; "object-host"; "--port"; "${port}" ]
                env [ "S2_ENDPOINT", s2.Endpoint ]
                readiness "/ready"
                exposeEndpoint "OBJECT_ENDPOINT"
            }

        let counter =
            Eff.Production.ObjectClient.connect host.Endpoint "counter" "counter-1"
    }

    scenario {
        let! results =
            concurrently [
                actor "client-a" {
                    return!
                        op "counter.add" {
                            client "client-a"
                            key "counter-1"
                            input {| by = 5 |}
                            run counter.Add 5
                        }
                }

                actor "client-b" {
                    return!
                        op "counter.add" {
                            client "client-b"
                            key "counter-1"
                            input {| by = 7 |}
                            run counter.Add 7
                        }
                }
            ]

        let! final =
            op "counter.value" {
                client "driver"
                key "counter-1"
                run counter.Value()
            }

        return {| Results = results; Final = final |}
    }

    verify {
        workload (fun r ->
            Set.ofList r.Results = Set.ofList [ 5; 12 ]
            && r.Final = 12)

        linearizable CounterModel.model {
            key "counter-1"
            operations [ "counter.add"; "counter.value" ]
        }

        invariant "owner stream has no lost writes" (fun evidence ->
            evidence
            |> Evidence.domainEvents "object.call.completed"
            |> ObjectAssertions.noLostWrites "counter-1")
    }
}
```

Sequential model:

```fsharp
module CounterModel =
    type Op =
        | Add of by: int
        | Value

    type State = int

    let model =
        Linearizability.model {
            initial 0

            operation "counter.add" (fun state input ->
                let by = Json.field<int> "by" input
                let next = state + by
                next, Json.int next)

            operation "counter.value" (fun state _ ->
                state, Json.int state)
        }
```

### Runtime Schedule Sweep Race

This proof verifies that two sweepers racing the same due schedule produce one
completed scheduled run.

```fsharp
proof "runtime.schedule-sweep-single-winner" {
    resources {
        use! s2 =
            processHost "s2-lite" {
                command "s2"
                args [ "lite"; "--port"; "${port}"; "--local-root"; "${tempDir}" ]
                readiness "/"
                exposeEndpoint "S2_ENDPOINT"
            }

        let runtimeA =
            Eff.Production.WorkflowRuntime.create {
                nodeId = "sweep-a"
                s2Endpoint = s2.Endpoint
            }

        let runtimeB =
            Eff.Production.WorkflowRuntime.create {
                nodeId = "sweep-b"
                s2Endpoint = s2.Endpoint
            }
    }

    scenario {
        do!
            op "schedule.materialize" {
                client "driver"
                key "scheduled-workflow:every-second:5000"
                input {| now = 5000 |}
                run runtimeA.MaterializeSchedules { now = 5000 }
            }

        let! sweeps =
            concurrently [
                actor "sweeper-a" {
                    return!
                        op "runtime.sweep" {
                            client "sweeper-a"
                            key "scheduled-workflow:every-second:5000"
                            input {| now = 5000; leaseOwner = "a" |}
                            run runtimeA.Sweep {
                                now = 5000
                                leaseOwner = "a"
                                maxScheduledRuns = 1
                                maxTimers = 0
                            }
                        }
                }

                actor "sweeper-b" {
                    return!
                        op "runtime.sweep" {
                            client "sweeper-b"
                            key "scheduled-workflow:every-second:5000"
                            input {| now = 5000; leaseOwner = "b" |}
                            run runtimeB.Sweep {
                                now = 5000
                                leaseOwner = "b"
                                maxScheduledRuns = 1
                                maxTimers = 0
                            }
                        }
                }
            ]

        let! run =
            op "workflow.load" {
                client "driver"
                key "scheduled-workflow:every-second:5000"
                run runtimeA.LoadExecution "scheduled-workflow:every-second:5000"
            }

        return {| Sweeps = sweeps; Run = run |}
    }

    verify {
        workload (fun r ->
            r.Run.Status = "finished"
            && (r.Sweeps |> List.sumBy _.CompletedCount) = 1)

        invariant "only one lease owner completed scheduled work" (fun evidence ->
            evidence
            |> Evidence.operationsNamed "runtime.sweep"
            |> LeaseAssertions.singleWinner)

        workflowEvents "one RUN_FINISHED event" {
            runId "scheduled-workflow:every-second:5000"
            count "RUN_FINISHED" 1
        }
    }
}
```

### Stale Owner Fencing

This proof verifies that an old object owner cannot commit after a newer owner
has claimed the stream.

```fsharp
proof "object.stale-owner-fencing" {
    resources {
        use! s2 =
            processHost "s2-lite" {
                command "s2"
                args [ "lite"; "--port"; "${port}"; "--local-root"; "${tempDir}" ]
                readiness "/"
                exposeEndpoint "S2_ENDPOINT"
            }

        use! hostA =
            processHost "a" {
                command "node"
                args [ "build/src/Program.js"; "object-host"; "--port"; "${port}" ]
                env [ "S2_ENDPOINT", s2.Endpoint ]
                readiness "/ready"
                exposeEndpoint "OBJECT_A_ENDPOINT"
            }

        use! hostB =
            processHost "b" {
                command "node"
                args [ "build/src/Program.js"; "object-host"; "--port"; "${port}" ]
                env [ "S2_ENDPOINT", s2.Endpoint ]
                readiness "/ready"
                exposeEndpoint "OBJECT_B_ENDPOINT"
            }

        let clientA =
            Eff.Production.ObjectClient.connect hostA.Endpoint "counter" "counter-1"

        let clientB =
            Eff.Production.ObjectClient.connect hostB.Endpoint "counter" "counter-1"
    }

    scenario {
        do!
            op "counter.add" {
                client "a"
                key "counter-1"
                input {| by = 1 |}
                run clientA.Add 1
            }

        do! Fault.killHost "a"

        do!
            op "counter.add" {
                client "b"
                key "counter-1"
                input {| by = 2 |}
                run clientB.Add 2
            }

        do! Fault.restartHost "a"

        let! staleResult =
            op "counter.add" {
                client "a"
                key "counter-1"
                input {| by = 3 |}
                run clientA.Add 3
            }

        let! final =
            op "counter.value" {
                client "b"
                key "counter-1"
                run clientB.Value()
            }

        return {| StaleResult = staleResult; Final = final |}
    }

    verify {
        workload (fun r -> r.Final = 3)

        invariant "stale owner did not commit after fencing" (fun evidence ->
            evidence
            |> ObjectAssertions.staleOwnerRejected "counter-1" "a")

        evidenceSql "fencing error was observed" """
            SELECT countIf(
                category = 'object'
                AND name = 'owner.commit.rejected'
                AND attributes['reason'] = 'stale-owner'
            ) >= 1 AS ok
            FROM domain_events
        """
    }
}
```

## Linearizability Adapter

The runner should expose a small F# model API, then translate operation
histories to a checker.

Initial implementation options:

1. Export operation history and model cases to JSON, then call a Go Porcupine
   sidecar.
2. Implement a small F# linearizability checker for targeted finite models.

The first option is faster and keeps compatibility with known prior art.

Model shape:

```fsharp
type SequentialModel<'state> =
    { Initial: 'state
      Step:
        'state
          -> OperationEvent
          -> Result<'state * JsonValue, string> }
```

Checker input:

```fsharp
type LinearizabilityInput =
    { ModelName: string
      Operations: OperationEvent list
      InitialStateJson: string }
```

The checker should produce:

- pass/fail
- illegal operation
- partial linearization if found
- minimized counterexample when available

## Fault Model

Initial faults:

```fsharp
type FaultController =
    abstract KillHost: name: string -> Async<unit>
    abstract StopHost: name: string -> Async<unit>
    abstract RestartHost: name: string -> Async<unit>
    abstract KillAtFaultPoint:
        host: string
        * point: string
        * attributes: Map<string, string>
        -> Async<unit>
```

Eventually, faults should come from a seedable fault plan:

```fsharp
type FaultPlanStep =
    | KillHost of host: string
    | RestartHost of host: string
    | DelayMessage of link: string * millis: int64
    | DropMessage of link: string
    | AdvanceClock of millis: int64
    | AtFaultPoint of point: string * action: FaultPlanStep
```

The report must include the exact fault plan used.

## Proof Commands

One command runs the suite; selection is a filter, not a separate script.

```sh
npm run proofs                    # compile + run every registered proof
PROOF=foundation-00 npm run proofs   # only proofs whose name contains the substring
PRESERVE=1 npm run proofs            # keep failed proofs' resources for inspection
S2_BASIN=my-basin npm run proofs     # override the default basin

npm run check                     # format + build + lint + fableSmoke + test + proofs
npm run play                      # repl.fsx tour (unchanged)
npm test                          # tests/Suite.fsx integration suite (unchanged)
```

`npm run proofs` is wired through the repo build tool (`tools/repo/Program.fs`)
and runs the thin launcher with the proven single-file Fable path:

```sh
dotnet fable scripts/proofs.fsx --outDir build_proofs --runScript
# PROOF / PRESERVE / S2_BASIN are read from the environment
```

The `Proofs` target also accepts `--proof <name>` directly. `build_proofs/` is
in `.gitignore` and the linter's ignore list. Adding a proof is two edits: a new
`src/Proofs/<Name>Proof.fs` (in the `.fsproj`) and one line in
`src/Proofs/Registry.fs` — no new `npm` target.

Later, after the proof runner grows a CLI, add commands such as:

```sh
npm run proofs -- list
npm run proofs -- run store.host-crash-restart --seed 123456
npm run proofs -- replay reports/store.host-crash-restart-123456.json
```

## First Milestone

Stand up the compiled, enforced proof suite. **Done.**

Deliverables:

1. ✅ `src/Proofs/Harness.fs` — generic `section` / `check` / `expectEqual` /
   `note`, `uniq`, cleanup registry, `config`, and `runProofs`. Compiled into
   `eff-firegrid.fsproj`.
2. ✅ `src/Proofs/SubjectHistoryProof.fs` — proves expected-sequence append,
   conflict classification, and cursor/fold behavior against real S2, driving
   the production `SubjectHistory` surface.
3. ✅ `src/Proofs/Registry.fs` — the single list of proofs.
4. ✅ `scripts/proofs.fsx` — thin Fable launcher; `npm run proofs` (with
   `PROOF` / `PRESERVE` / `S2_BASIN`) wired through the repo build tool.
5. ✅ `npm run proofs` added to `npm run check`, so `dotnet build` (type-check
   + FS1182), `fsharplint`, and the live-S2 run are all CI-gated.

This milestone intentionally avoids the full proof runner, process-host
abstraction, simulator, and linearizability checker.

## Second Milestone

Grow the foundation, top-down from the SDD, one proof per layer.

For each layer: write the production primitive to its SDD signature, add a
compiled proof that drives it against real S2, register it.

Deliverables:

1. `src/Foundation/StateView.fs` + `src/Proofs/StateViewProof.fs` proving
   eventual and strong reads over the KV-demo-style orchestrator loop.
2. `src/Foundation/KvStore.fs` + `src/Proofs/KvStoreProof.fs` proving the S2 KV
   pattern end to end.
3. Both registered in `Registry.fs` and green under `npm run proofs`.

Surface rule: a proof drives the production surface directly. If a proof needs
an easier entrypoint, add it to the production module and use it from both the
proof and real deployments — never a verification-only substitute.

## Third Milestone

Prove the first deferred durable-work semantics after the happy path is stable.
Each is a production primitive plus a compiled proof in the registry.

1. Effectively-once output suppression.
2. Output-producing invocation runtime.
3. Minimal operation ledger proving recorded completion prevents re-execution.
4. Coordination claim.
5. Fencing / checkpoint.
6. Timer / external wait, only after the state-view path is stable.
7. Wake projection rebuild, only after state-view indexes exist.

These stay readable: print the records, print the fold, make the failing check
obvious without a separate report viewer.

## Fourth Milestone

Introduce structured evidence in the harness only after proofs need it.

1. Operation-history record type for concurrent proof sections.
2. JSON trial reports for failed proofs (`src/Proofs/Reports.fs`).
3. Production instrumentation for durable records and folds if useful.
4. F# sequential model API for one small model.
5. Porcupine sidecar or native checker spike once operation histories are
   stable.

## Fifth Milestone

Grow the harness into a proof runner and move toward deterministic simulation.

1. Resource/host primitives in `src/Proofs` once production hosts exist.
2. `IClock`, `IRandom`, and `IFaultPoint` integrated into production code.
3. Controlled-process runtime mode.
4. Seedable fault plan generator.
5. Replay command that restores seed, resources, and fault plan.

## Design Principles

1. Proofs are compiled and drive production APIs; the build enforces them.
2. Proofs print authoritative records and derived folds.
3. The shared harness stays generic and behavior-free.
4. Operation history becomes first-class when concurrency proofs need it.
5. Domain events become first-class when records/folds are not enough context.
6. Traces are diagnostics and supplementary evidence.
7. Faults happen at semantic boundaries where possible.
8. Counterexample preservation starts as "do not clean up this stream"
   (`PRESERVE=1`) and only later becomes structured reports.
9. The runner grows from the harness, then evolves toward deterministic
   simulation.
10. Any eventual proof DSL should remain ordinary F# async code, not a separate
    language.

## Resolved Decisions

The first milestone settled the harness-shaped questions:

1. **Compiled, not scripted.** Proofs live in `src/Proofs/*.fs`, compiled into
   `eff-firegrid.fsproj`, so the build and linter enforce them. A proof is a
   `Name + (Context -> Async<unit>)` value; `scripts/proofs.fsx` is only a
   launcher.
2. **Smallest harness.** `section` / `check` / `expectEqual` / `note`, `uniq`,
   a cleanup registry, `config` (the basin), and `runProofs`. Nothing workflow-
   or S2-specific lives in it.
3. **Preservation is opt-in.** Cleanup runs by default; `PRESERVE=1` keeps a
   failed proof's resources and prints what was left behind. Defaulting to
   cleanup keeps reruns cheap; the flag is there for the rare counterexample.
4. **Print first, JSON later.** Proofs print authoritative records and folds;
   structured JSON evidence is deferred to the Fourth Milestone, introduced only
   once concurrency proofs need it.
5. **Stream naming.** `"<prefix>-<epochMillis>-<counter>"` via `Harness.uniq` —
   readable, unique per run, sortable, and easy to sweep by prefix. The basin
   defaults to `test-basin-885234` and is overridable with `S2_BASIN`.

Still open, deferred to the milestone that needs them:

6. When production hosts arrive, should proofs drive them through HTTP, CLI,
   direct Fable imports, or all of the above?
7. Should the first linearizability checker be a Porcupine sidecar or a small
   native F# checker?

## Recommendation

Make verification a compiled, enforced proof suite from the start — the harness
and proofs live in `src/Proofs` and ride the build. Then grow the
`docs/foundational-sdd.md` layers top-down: write each production primitive to
its SDD signature, add a compiled proof that drives it against real S2, and
register it. Order: SubjectHistory (done), StateView, KvStore.

Keep the harness generic. Add runner machinery — hosts, evidence, reports,
faults, checkers — only when a proof needs it, extracting the generic part into
`src/Proofs`, never domain-specific fakes.

Do not start with a full deterministic simulator. Instead, design production
boundaries so deterministic control can replace live behavior later: clock,
randomness, fault points, transport, and fault plans.

Do not make raw OTel spans the canonical history format. Emit spans for
diagnostics, but make operation history and domain events the canonical proof
ledger.
