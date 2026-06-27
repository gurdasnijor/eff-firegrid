# Eff Firegrid Verification Proposal

## Status

Proposed. First milestone landed: the shared script harness
(`scripts/_prelude.fsx`), the unified runner (`scripts/all.fsx`), and the
`npm run scripts` entrypoint now back the foundation proofs, replacing the
per-scratchpad copies of `uniq` / `check` / `section`. See
[Replacing the .fsx Scratchpads](#replacing-the-fsx-scratchpads).

## Context

`eff-firegrid` is a Fable/F# system that wraps and composes the S2 JavaScript
SDK into F#-native APIs. The current verification surface is intentionally
REPL/script driven:

- `repl.fsx` is the live S2 playground and ergonomics probe.
- `tests/Suite.fsx` is a script-style integration suite.
- `docs/foundational-sdd.md` defines the immediate path: prove the smallest
  useful durable-work kernel one property at a time.

The next step is not a full proof runner. The next step is a REPL-friendly
verification workflow that lets each durable layer start as a small script,
exercise production modules directly, and only promote code into `src/` after
the shape survives use.

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

1. REPL scripts over production modules.
2. Tiny generic script helpers once repetition appears.
3. Promoted foundation primitives in `src/Foundation`.
4. Script suites that exercise those primitives.
5. A proof runner only after the scripts reveal the right shape.

## Goals

1. Keep verification REPL-friendly and script-first.
2. Exercise real `eff-firegrid` production modules, CLIs, and clients directly.
3. Prefer one durable property per script section.
4. Promote code into `src/` only after scripts prove the shape.
5. Make authoritative records and derived folds easy to print and inspect.
6. Preserve a path toward operation histories and replayable counterexamples.
7. Defer a full proof DSL until repeated script friction justifies it.
8. Create a clean path toward seed-based deterministic simulation.
9. Create a clean path toward Porcupine-style linearizability checking.

## Non-Goals

1. Do not build a proof runner in the first iteration.
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

Verification starts as a disciplined REPL workflow, not a framework.

Each script should:

1. State the durable property being explored.
2. Create isolated durable resources with readable names.
3. Drive production modules directly.
4. Print authoritative records and derived folds.
5. Check the property with simple assertions.
6. Clean up resources unless intentionally preserving a counterexample.

The immediate unit is a script section, not a proof object. A proof runner can
later formalize the same shape:

1. **Resources**: scoped dependencies such as S2 streams and production host
   processes.
2. **Scenario**: actors and clients drive the system with ordinary F# async
   programs.
3. **Evidence**: authoritative records, folds, operation histories, and domain
   events are captured.
4. **Verification**: check workload results, histories, invariants, SQL views,
   and optional linearizability models.

## REPL-Friendly Workflow

Use a promotion ladder:

```text
scratch in repl.fsx
  -> focused script in scripts/
  -> repeated helper in scripts/_prelude.fsx
  -> production primitive in src/
  -> regression section in tests/Suite.fsx or scripts/all.fsx
  -> later proof runner support only if the pattern stabilizes
```

The `scripts/` directory should be treated as executable design work. Scripts
may be messy while an idea is forming, but each committed script should have a
small property statement and remain runnable with `npm run script -- <name>` or
an equivalent command.

The only shared script helpers should be generic:

- `proof` / `section` printing
- `check` / `expectEqual`
- unique name generation
- cleanup registration
- script config loading
- optional preservation of failed resources

Shared helpers must not contain workflow/object behavior, hidden stores,
fake clients, event mappers, or alternate interpreters.

## Replacing the .fsx Scratchpads

The repo already had four `.fsx` scratchpads, and three of them each rolled
their own copy of the same harness:

- `tests/Suite.fsx` — `uniq` / `check` / `test` plus `passed` / `failed` /
  `failures` globals and an `exit` tail.
- `scripts/foundation-00-subject-history.fsx` — a `Proof` module with `uniq`,
  `check`, `section`, `finish`, plus inline create/delete-on-failure cleanup.
- `scripts/subject-history-playground.fsx` — its own ad-hoc scaffolding.
- `repl.fsx` — a `section` wrapper (no checks; it is a tour, not a proof).

That duplication is exactly what the proposal's `_prelude.fsx` exists to
absorb. The replacement is deliberately small and behavior-free:

```text
scripts/
  _prelude.fsx   # the only shared harness: section/check/expectEqual/note,
                 # uniq, cleanup registry, S2 config, runProofs (the runner)
  foundation-00-subject-history.fsx   # a definition module exposing `proof`
  all.fsx        # loads each proof module, runs them through one harness
```

Key shape decisions that make the scratchpads collapse into one workflow:

1. **A proof is a value, not a self-running script.** Each
   `foundation-*.fsx` is now a `module` that exposes
   `proof : Prelude.Proof` and performs no top-level execution. That is what
   lets `all.fsx` `#load` several proofs and run them under a single shared
   `Context` with one combined summary — a self-running script cannot be
   composed because `#load` re-executes its tail.
2. **One entrypoint, with a name filter.** `npm run scripts` runs every
   committed proof; `PROOF=<substring>` (or `npm run script:subject-history`)
   filters to one. There is no longer a bespoke `npm` target per script body.
3. **Cleanup is registered, not inlined.** `Prelude.onCleanup` records a
   teardown thunk next to the resource it frees. The runner runs them
   last-in-first-out after the proof, so no proof needs its own
   delete-on-success / delete-on-failure ceremony.
4. **Counterexample preservation is a flag.** Cleanup runs by default; set
   `PRESERVE=1` and, when the proof has failures, the runner skips teardown and
   prints the resources it left behind for inspection.

`tests/Suite.fsx` and `repl.fsx` still run as before; the prelude is a strict
superset of what they need, so they can adopt it next without changing
behavior. They are intentionally left untouched in this milestone so the
canonical `npm test` and `npm run play` paths stay green.

## Recommended Repo Shape

```text
src/
  Foundation/
    SubjectHistory.fs
    StateView.fs
    KvStore.fs

  Proofs/
    Core.fs
    Runtime.fs
    Evidence.fs
    Reports.fs
    Hosts.fs
    Faults.fs
    Checkers.fs
    Linearizability.fs
    Sql.fs

scripts/
  _prelude.fsx
  foundation-00-subject-history.fsx
  foundation-01-state-view.fsx
  foundation-02-kv-store.fsx
  all.fsx

proofs/
  Models/
    Counter.fs
    WorkflowRun.fs
    Lease.fs
    Register.fs

  StoreHostCrashRestart.fs
  ObjectSerialization.fs
  RuntimeScheduleSweep.fs
  StaleOwnerFencing.fs
  CapabilityAAtomicReplay.fs
  Main.fs
```

`src/Foundation` is the near-term production target from
`docs/foundational-sdd.md`. These modules should appear only after scripts prove
the underlying shape against real S2 streams.

`scripts` is the immediate verification surface. Scripts import production
modules from `src` and exercise them directly.

`src/Proofs` and `proofs` are later-stage. Do not create them until the script
suite has enough repetition to justify a runner.

When `src/Proofs` eventually exists, it should know about concepts such as
resources, hosts, operations, evidence, reports, faults, and checkers, but not
about specific `eff-firegrid` workflow or object APIs.

The `proofs` directory contains executable proof definitions and the CLI
registry. It may contain checker models that define the intended sequential
semantics of a subsystem, but it should not contain domain-specific support
layers that wrap or simulate the system under test. A proof may import
production modules from `src`, start production CLIs as processes, or call
production HTTP/client surfaces, but it should not introduce a
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

To scale beyond simple scripts, production code eventually needs narrow
dependencies that scripts and the later proof runner can control. Add these
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
runner exists, scripts should print and optionally persist the same concepts in
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

## Script Commands

Commands preserve the current `npm run play` style. Because a proof is now a
value rather than a self-running script, all proof execution goes through the
unified `all.fsx` runner, and per-proof selection is a filter rather than a
separate compiled script.

```sh
npm run play                     # repl.fsx tour (unchanged)
npm run watch                    # fable watch on repl.fsx (unchanged)
npm run scripts                  # compile + run every foundation proof
npm run script:subject-history   # same runner, filtered to one proof
npm test                         # tests/Suite.fsx integration suite (unchanged)

PROOF=foundation-00 npm run scripts   # ad-hoc substring filter
PRESERVE=1 npm run scripts            # keep failed proofs' resources
S2_BASIN=my-basin npm run scripts     # override the default basin
```

These are wired through the repo build tool (`tools/repo/Program.fs`), matching
the existing `dotnet run --project tools/repo/Build.fsproj -- --target ...`
pattern, so the underlying compile/run stays:

```sh
dotnet fable scripts/all.fsx --outDir build_scripts
node build_scripts/all.js          # PROOF / PRESERVE / S2_BASIN read from env
```

The `Scripts` target also accepts `--proof <name>` directly (this is how the
`ScriptSubjectHistory` target pins itself to one proof). `build_scripts/` is
already in `.gitignore`. New foundation proofs are added by `#load`-ing them in
`all.fsx` and appending their `proof` value to the `runProofs` list — no new
`npm` target per script.

Later, after the proof runner exists, add commands such as:

```sh
npm run proofs -- list
npm run proofs -- run store.host-crash-restart --seed 123456
npm run proofs -- replay reports/store.host-crash-restart-123456.json
```

## First Milestone

Build the smallest useful REPL verification harness.

Deliverables:

1. ✅ `scripts/_prelude.fsx` with generic `section`, `check`, `expectEqual`,
   `note`, unique-name, cleanup, config, and `runProofs` helpers.
2. ✅ `scripts/foundation-00-subject-history.fsx` proving expected-sequence
   append, conflict classification, and cursor/fold behavior against real S2,
   now expressed as a `proof` value on the shared harness.
3. `scripts/foundation-01-state-view.fsx` proving eventual and strong reads
   over the KV-demo-style orchestrator loop.
4. `scripts/foundation-02-kv-store.fsx` proving the S2 KV pattern end to end.
5. ✅ `scripts/all.fsx` that runs the current committed foundation proofs
   through one harness with a combined summary.
6. ✅ `npm run scripts` (all) and `npm run script:subject-history` (filtered),
   wired through the repo build tool.

Deliverables 3 and 4 are the next step. They are not yet shipped because each
needs its production module to drive — `src/Foundation/StateView.fs` and
`src/Foundation/KvStore.fs` do not exist yet — and the proposal's own rule is
that a proof must exercise a real production surface, not a verification-only
substitute. Authoring them responsibly requires writing those primitives and
running the proofs against live S2, so they are deferred to the next pass
rather than stubbed.

This milestone intentionally avoids a proof runner, process-host abstraction,
full simulator, and linearizability checker.

## Second Milestone

Promote script-proven primitives into production modules.

Deliverables:

1. `src/Foundation/SubjectHistory.fs`
2. `src/Foundation/StateView.fs`
3. `src/Foundation/KvStore.fs`
4. Regression coverage in `tests/Suite.fsx` or `scripts/all.fsx`

Promotion rule: only move helper code from scripts into `src/Foundation` after
the SDD layer cannot be expressed clearly without it or a later script needs the
same production surface.

## Third Milestone

Prove the first deferred durable-work semantics after the happy path is stable.

Deliverables:

1. Effectively-once output suppression script and production primitive.
2. Output-producing invocation runtime script and production primitive.
3. Minimal operation ledger script proving recorded completion prevents
   re-execution.
4. Coordination claim script and production primitive.
5. Fencing/checkpoint script and production primitive.
6. Timer/external wait script only after the state-view path is stable.
7. Wake projection rebuild script only after state-view indexes exist.

These should remain REPL-friendly: print the records, print the fold, and make
the property failure obvious without a separate report viewer.

## Fourth Milestone

Introduce structured evidence only after scripts need it.

Deliverables:

1. Minimal operation-history record type for concurrent script sections.
2. Optional JSON report output for failed scripts.
3. Production instrumentation for durable records and folds if useful.
4. F# sequential model API for one small model.
5. Porcupine sidecar or native checker spike only after operation histories are
   stable.

## Fifth Milestone

Move toward a proof runner and deterministic simulation.

Deliverables:

1. Generic runner primitives extracted from repeated scripts.
2. Process-host lifecycle only when production hosts exist.
3. `IClock`, `IRandom`, and `IFaultPoint` integrated into production code.
4. Controlled-process runtime mode.
5. Seedable fault plan generator.
6. Replay command that restores seed, resources, and fault plan.

## Design Principles

1. Scripts drive production APIs.
2. Scripts print authoritative records and derived folds.
3. Shared script helpers stay generic and behavior-free.
4. Operation history becomes first-class when concurrency proofs need it.
5. Domain events become first-class when records/folds are not enough context.
6. Traces are diagnostics and supplementary evidence.
7. Faults happen at semantic boundaries where possible.
8. Counterexample preservation starts as "do not clean up this stream" and only
   later becomes structured reports.
9. The runner starts after scripts stabilize, then evolves toward deterministic
   simulation.
10. Any eventual proof DSL should remain ordinary F# async code, not a separate
    language.

## Resolved Decisions

The first milestone settled the harness-shaped questions:

1. **Smallest `_prelude.fsx`.** `section` / `check` / `expectEqual` / `note`,
   `uniq`, a cleanup registry, `config` (the basin), and `runProofs`. Nothing
   workflow- or S2-specific lives there; a proof is a `Name + (Context ->
   Async<unit>)` value.
2. **Preservation is opt-in.** Cleanup runs by default; `PRESERVE=1` keeps a
   failed proof's resources and prints what was left behind. Defaulting to
   cleanup keeps reruns cheap; the flag is there for the rare counterexample.
3. **Print first, JSON later.** Scripts print authoritative records and folds;
   structured JSON evidence is deferred to the Fourth Milestone, where it is
   introduced only once concurrency proofs need it.
4. **Stream naming.** `"<prefix>-<epochMillis>-<counter>"` via `Prelude.uniq` —
   readable, unique per run, sortable, and easy to sweep by prefix. The basin
   defaults to `test-basin-885234` and is overridable with `S2_BASIN`.

Still open, deferred to the milestone that needs them:

5. When production hosts arrive, should scripts drive them through HTTP, CLI,
   direct Fable imports, or all of the above?
6. Should the first linearizability checker be a Porcupine sidecar or a small
   native F# checker?

## Recommendation

Start with REPL-friendly durable scripts, not a proof runner. Add a tiny
generic `_prelude.fsx`, then build the `docs/foundational-sdd.md` layers in
order: SubjectHistory, StateView, and KvStore.

Use scripts to decide which APIs belong in `src/Foundation`. Once several
scripts repeat the same generic orchestration, extract only that generic
orchestration into runner primitives.

Do not start with a full deterministic simulator. Instead, design production
boundaries so deterministic control can replace live behavior later: clock,
randomness, fault points, transport, and fault plans.

Do not make raw OTel spans the canonical history format. Emit spans for
diagnostics, but make operation history and domain events the canonical proof
ledger.
