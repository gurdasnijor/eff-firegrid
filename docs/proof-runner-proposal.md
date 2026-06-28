# Eff Firegrid Proof Runner Proposal

## Status

Proposed revision.

The previous draft mixed two different ideas:

1. A small compiled assertion harness with `section`, `check`, and
   `expectEqual`.
2. A property runner like `fluent-firegrid/apps/proofs`, where a proof declares
   resources, runs a workload, captures evidence, and verifies the result after
   the run.

Only the second shape is the target. The first shape is useful as a temporary
test helper, but it is not enough proof infrastructure. It does not model
resources, hosts, workload results, operation histories, domain events, trial
identity, reports, or replayable counterexamples.

The proof runner should be a compiled F# system with ergonomic computation
expressions. Proof definitions remain compiled into `eff-firegrid.fsproj`, so
`dotnet build`, FS1182, and `fsharplint` enforce that they stay aligned with
production APIs.

## Prior Art

The target shape is aligned with `fluent-firegrid/apps/proofs`, especially:

- `proof(...).describedAs(...).spec(...)`
- `property(...).s2Lite(...).hosts(...).workload(...).verify(...)`
- `processHost(...)` resources with readiness probes
- workload result checks plus evidence checks

The F# implementation should not copy TypeScript APIs literally, but the
semantic phases should match:

1. Define a proof and description.
2. Provision resources such as `s2 lite` and one or more host processes.
3. Run a workload against production APIs, HTTP handlers, or generated clients.
4. Capture workload results and runner evidence.
5. Verify the result and evidence after the workload completes.
6. Tear resources down, or preserve them on failure when requested.

## Goals

1. Keep proofs compiled and enforced by the normal project build.
2. Make proof authoring feel like normal F# async code.
3. Support multiple live host processes as first-class property resources.
4. Separate resource setup, workload execution, and verification.
5. Make workload result verification explicit.
6. Capture runner evidence for hosts, S2, operations, and later domain events.
7. Keep production behavior under test in `src/Foundation` and later production
   modules, not in proof-local substitutes.
8. Keep deterministic simulation as a later mode, not the first implementation.

## Non-Goals

1. Do not use `.fsx` proof launchers.
2. Do not center the API on `section/check/expectEqual`.
3. Do not require a full linearizability checker for the first proof.
4. Do not make raw traces the only canonical evidence format.
5. Do not create verification-only workflow/object facades that hide production
   behavior.
6. Do not start with a large pseudo-language that is not valid F#.

## Target Authoring Shape

Use computation expressions, but keep them shallow. A proof is a compiled F#
value. A property is a compiled F# value. The workload is ordinary
`Async<'result>`.

The preferred shape is a valid F# expression, not a sketch language:

```fsharp
let storeObjectSerialization =
    proof "store.object-serialization" {
        describedAs
            "Concurrent same-key actor calls serialize through the S2-backed actor log and do not lose writes."

        property (
            property "store.object-serialization-proof" {
                s2Lite LocalRoot

                workload (fun ctx ->
                    async {
                        let counter =
                            Counter.create {
                                Basin = ctx.S2.Basin
                                Namespace = "object-serialization-" + ctx.TrialId
                                Name = "counter"
                                Key = "counter-1"
                            }

                        let! results =
                            Async.Parallel
                                [ counter.Add 5
                                  counter.Add 7 ]

                        let! value = counter.Value()

                        return
                            {| CompletedCalls = results.Length
                               MaxResult = Array.max results
                               Value = value |}
                    })

                verify [
                    Expect.workloadResult
                        {| CompletedCalls = 2
                           MaxResult = 12
                           Value = 12 |}
                ]
            })
    }
```

This is intentionally close to the TypeScript proof:

```ts
proof("store.object-serialization")
  .describedAs(...)
  .spec(({ property, trialId }) =>
    property("store.object-serialization-proof")
      .s2Lite(...)
      .workload(...)
      .verify([...])
  )
```

## Multiple Host Processes

Multiple host processes should be property resources. The runner starts them,
injects standard environment, waits for readiness, records evidence, and tears
them down.

Authoring shape:

```fsharp
let objectLiveFencing =
    proof "store.object-live-fencing" {
        describedAs
            "A live deposed object owner cannot append stale state after another host takes over."

        property (
            property "store.object-live-fencing-proof" {
                s2Lite LocalRoot

                hosts [
                    processHost "a" {
                        command "pnpm"
                        args [ "exec"; "tsx"; workerPath ]
                        env "HOST_PORT" (string portA)
                        readinessUrl (hostA + "/ready")
                        readinessAttempts 400
                        readinessIntervalMs 50
                    }

                    processHost "b" {
                        command "pnpm"
                        args [ "exec"; "tsx"; workerPath ]
                        env "HOST_PORT" (string portB)
                        readinessUrl (hostB + "/ready")
                        readinessAttempts 400
                        readinessIntervalMs 50
                    }
                ]

                workload (fun ctx ->
                    async {
                        let hostA = ctx.Hosts["a"].Endpoint
                        let hostB = ctx.Hosts["b"].Endpoint
                        let s2Endpoint = ctx.S2.Endpoint

                        let namespaceName =
                            "object-live-fencing-" + ctx.TrialId

                        let streamName =
                            ObjectRuntime.invocationStreamName namespaceName "counter-1" "cross-host-counter"

                        let! deposedRequest =
                            Http.post (hostA + "/deposed-add?by=5")
                            |> Async.StartChild

                        let! startedCount =
                            ObjectRuntime.waitForStartedCount s2Endpoint streamName 2

                        do! Http.post (hostB + "/now?value=3000")

                        let! takeover =
                            Http.postJson<{| HostId: string; Value: int |}> (hostB + "/add?by=7")

                        do! Async.Sleep 1000
                        do! deposedRequest |> Async.Ignore

                        let! loaded =
                            Http.getJson<{| HostId: string; Value: int |}> (hostB + "/value")

                        return
                            {| DeposedStartedEvents = startedCount
                               LateOwnerDidNotChangeValue = loaded.Value = takeover.Value
                               TakeoverHostId = takeover.HostId |}
                    })

                verify [
                    Expect.workloadResult
                        {| DeposedStartedEvents = 2
                           LateOwnerDidNotChangeValue = true
                           TakeoverHostId = "b" |}

                    Evidence.hostStarted "a"
                    Evidence.hostStarted "b"
                    Evidence.s2LiteStarted
                ]
            })
    }
```

The important part is ownership: proof definitions describe host specs; the
runner owns actual process lifecycle.

## Core Types

Initial types should be small and direct.

```fsharp
type Proof<'result> =
    { Name: string
      Description: string option
      Properties: PropertySpec<'result> list }

type PropertySpec<'result> =
    { Name: string
      S2Lite: S2LiteSpec option
      Hosts: HostSpec list
      Workload: WorkloadContext -> Async<'result>
      Verifiers: Check<'result> list }

type S2LitePersistence =
    | LocalRoot

type S2LiteSpec =
    { Persistence: S2LitePersistence }

type Readiness =
    { Url: string
      Attempts: int
      IntervalMs: int }

type HostSpec =
    { Name: string
      Command: string
      Args: string list
      Env: Map<string, string>
      Readiness: Readiness option }

type RunningS2 =
    { Endpoint: string
      Basin: S2.Basin }

type RunningHost =
    { Name: string
      Endpoint: string
      ProcessId: int }

type WorkloadContext =
    { TrialId: string
      S2: RunningS2
      Hosts: Map<string, RunningHost>
      Evidence: EvidenceSink
      Faults: FaultController }

type CompletedTrial<'result> =
    { ProofName: string
      PropertyName: string
      TrialId: string
      Result: Result<'result, exn>
      Evidence: EvidenceStore }

type Check<'result> =
    { Name: string
      Run: CompletedTrial<'result> -> Async<Result<unit, string>> }
```

The first implementation can constrain all properties to require `s2Lite`.
Relax that later if a proof does not need S2.

## Runner Order

For each selected proof property:

1. Generate a trial id.
2. Start `s2 lite` if requested.
3. Start all declared host processes.
4. Inject standard environment into every host:
   - `EFF_TRIAL_ID`
   - `EFF_HOST_ID`
   - `S2_ENDPOINT`
   - user-specified env such as `HOST_PORT`
5. Wait for every readiness probe.
6. Record `verification.host.start` evidence for each host.
7. Run the workload.
8. Capture the workload result or exception.
9. Run verifiers against `CompletedTrial`.
10. Write a JSON report when requested or on failure.
11. Stop hosts in reverse creation order.
12. Stop `s2 lite`, unless preservation is requested after a failure.

This runner order is what makes checks such as `Evidence.hostStarted "a"` real.
They assert runner-owned evidence, not proof-local guesses.

## Computation Expression Modules

Recommended source layout:

```text
src/
  Proofs/
    Proof.fs          # proof CE and Proof<'result>
    Property.fs       # property CE and PropertySpec<'result>
    ProcessHost.fs    # processHost CE, readiness, lifecycle
    S2Lite.fs         # s2 lite supervisor
    Expect.fs         # workloadResult, predicate
    Evidence.fs       # initial runner evidence checks
    Reports.fs        # JSON reports
    Runner.fs         # selection, execution, summary, exit code
    Registry.fs       # compiled list of proofs
```

Avoid a central `Harness.fs` abstraction. If an assertion helper survives, keep
it private or compatibility-only. The proof runner is `Property + Runner`, not
`Context + check`.

## Initial Evidence

Start with runner-owned evidence. It is enough to make host and S2 checks real.

```fsharp
type EvidenceEvent =
    { TrialId: string
      Category: string
      Name: string
      Subject: string option
      Attributes: Map<string, string>
      TimeNanos: int64 }
```

Initial events:

- `verification.s2_lite.start`
- `verification.s2_lite.stop`
- `verification.host.start`
- `verification.host.ready`
- `verification.host.stop`
- `verification.host.kill`
- `verification.workload.start`
- `verification.workload.finish`

Initial checks:

```fsharp
module Expect =
    val workloadResult: expected: 'result -> Check<'result> when 'result: equality
    val workload: name: string -> predicate: ('result -> bool) -> Check<'result>

module Evidence =
    val hostStarted: name: string -> Check<'result>
    val hostReady: name: string -> Check<'result>
    val s2LiteStarted: Check<'result>
```

SQL over evidence can come later. The first runner can query an in-memory
`EvidenceEvent list`.

## Operation Evidence

Operation history is required before linearizability checks. It should not be
reconstructed from logs or raw spans.

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

Add an `operation` helper only when the first concurrent proof needs it. The
object serialization proof can initially verify workload result plus host/S2
evidence. A later linearizability proof can add operation recording.

## Reports

Every failed trial should write a report that can explain what happened without
rerunning immediately.

```json
{
  "proof": "store.object-live-fencing",
  "property": "store.object-live-fencing-proof",
  "trialId": "store.object-live-fencing-1782622956956",
  "status": "failed",
  "failedChecks": [],
  "resources": [],
  "evidence": [],
  "operations": [],
  "replayCommand": "npm run proofs -- --proof store.object-live-fencing --trial-id store.object-live-fencing-1782622956956"
}
```

The report format can grow, but the first version should include:

- proof name
- property name
- trial id
- workload result or error
- failed checks
- host metadata
- S2 metadata
- runner evidence
- replay command

## Commands

Proof execution remains compiled through the normal project.

```sh
npm run proofs
npm run proofs -- --proof store.object-serialization
npm run proofs -- --proof store.object-live-fencing
npm run proofs -- --trial-id my-repro
PRESERVE=1 npm run proofs -- --proof store.object-live-fencing
```

The build target should compile the project and run the emitted CLI:

```sh
dotnet fable eff-firegrid.fsproj --outDir build_proofs --noCache
node build_proofs/src/Program.js proofs
```

The CLI should parse arguments directly instead of relying only on environment
variables:

```text
proofs list
proofs run [--proof <substring>] [--trial-id <id>] [--preserve]
proofs replay <report.json>
```

`npm run proofs` can continue to map to `proofs run`.

## Milestone 1: Compiled Property Runner

This is the first real milestone. It is not done until a proof can be expressed
as a property with resources, workload, and verification.

Deliverables:

1. `src/Proofs/Proof.fs`
2. `src/Proofs/Property.fs`
3. `src/Proofs/ProcessHost.fs`
4. `src/Proofs/S2Lite.fs`
5. `src/Proofs/Expect.fs`
6. `src/Proofs/Evidence.fs`
7. `src/Proofs/Reports.fs`
8. `src/Proofs/Runner.fs`
9. `src/Proofs/Registry.fs`
10. `src/Program.fs` CLI that runs `Runner`

Acceptance criteria:

1. Proofs are compiled into `eff-firegrid.fsproj`.
2. `npm run proofs -- --proof store.object-serialization` starts S2 if needed,
   runs the workload, runs verifiers, and exits non-zero on failure.
3. Failed proofs write a JSON report.
4. `PRESERVE=1` or `--preserve` keeps resources after failure.
5. No `.fsx` proof launcher exists.

## Milestone 2: First Object Serialization Proof

Implement the closest F# equivalent of the fluent
`store-object-serialization.ts` proof.

Production surface:

- a small durable actor/object surface in `src/Foundation` or the production
  runtime module
- a counter object/actor used by the proof
- no proof-local fake runtime

Proof:

- `src/Proofs/StoreObjectSerializationProof.fs`
- workload runs two concurrent same-key `add` calls
- workload reads final value
- verifier checks completed calls, max result, and final value

This milestone proves that the property runner is useful before adding process
host complexity.

## Milestone 3: Multiple Host Live-Fencing Proof

Implement the closest F# equivalent of the fluent
`store-object-live-fencing.ts` proof.

Runner additions:

- `hosts [ processHost "a" { ... }; processHost "b" { ... } ]`
- readiness probes
- runner evidence for host start/ready/stop
- fault controller with host kill/stop/restart

Proof:

- `src/Proofs/StoreObjectLiveFencingProof.fs`
- starts S2 lite
- starts host `a`
- starts host `b`
- begins a deposed request on host `a`
- waits until S2 evidence shows the deposed call started
- sends takeover request to host `b`
- verifies host `b` owns the final value
- verifies runner evidence saw both hosts start

## Milestone 4: Operation History

Add first-class operation evidence when concurrent proofs need model checking.

Deliverables:

- `OperationEvent`
- operation wrapper for workload code
- JSON report operation history
- initial linearizability adapter API

Do not block Milestones 1-3 on this. Workload result checks and runner evidence
are enough to make the first object proofs meaningful.

## Design Rules

1. A proof is a compiled declaration of properties.
2. A property has resources, workload, and verifiers.
3. Host processes are resources owned by the runner.
4. Proof code may drive production APIs, HTTP handlers, CLIs, or generated
   clients.
5. Proof code must not define a fake production runtime to make verification
   easier.
6. Runner evidence is canonical for host/S2 lifecycle.
7. Operation history is canonical for linearizability.
8. Traces are useful diagnostics and optional evidence, not the only ledger.
9. `section/check/expectEqual` are not the proof API.
10. CE syntax must remain valid F#.

## Migration From Current Harness

Current `src/Proofs/Harness.fs` should not be the long-term abstraction.

Short-term options:

1. Delete it when `Runner.fs` lands.
2. Keep only tiny formatting helpers behind `Expect`.
3. Move cleanup and summary behavior into `Runner.fs`.

Current `SubjectHistoryProof` can be migrated into a property:

```fsharp
let subjectHistory =
    proof "foundation.subject-history" {
        describedAs "SubjectHistory append, conflict, and fold behavior."

        property (
            property "foundation.subject-history-proof" {
                s2LiveFromEnv

                workload (fun ctx ->
                    async {
                        // create stream, append records, fold, return summary
                    })

                verify [
                    Expect.workload "append and fold summary is valid" (fun result ->
                        result.TailAdvanced
                        && result.ConflictObserved
                        && result.FoldObservedPendingTimer)
                ]
            })
    }
```

That keeps the compiled foundation proof, but expresses it through the same
property runner as the object proofs.
