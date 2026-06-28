# Eff Firegrid Proof Runner SDD

## Status

Proposed, with Milestone 0 implementation started.

This SDD replaces the earlier custom in-memory evidence model. The proof
runner should use the same observability substrate the production system needs:
OpenTelemetry spans emitted from runner lifecycle code, proof adapters, host
processes, and product-owned instrumentation. Proof verification queries those
traces through `chdb`, the npm package for `chdb-io/chdb-node`.

The proof runner is validation infrastructure. It is not a rapid iteration
REPL, not a `.fsx` launcher, and not a proof-local fake runtime.

## Prior Art

The target shape remains aligned with `fluent-firegrid/apps/proofs`,
especially:

- `proof(...).describedAs(...).spec(...)`
- `property(...).s2Lite(...).hosts(...).workload(...).verify(...)`
- `processHost(...)` resources with readiness probes
- workload result checks plus evidence checks
- host choreography for live fencing

The F# API should be idiomatic F#, not a literal TypeScript port. Computation
expressions are the right shape for declaration. Workloads stay ordinary
`Async<'result>` values.

## Goals

1. Keep proofs compiled into `eff-firegrid.fsproj`, so `dotnet build`, FS1182,
   and `fsharplint` catch drift from production APIs.
2. Make instrumentation look like Effect's `withSpan`: local, explicit, and
   usable from production, runner, or proof-adapter code.
3. Use OpenTelemetry traces as the canonical evidence ledger for proofs and
   baseline observability.
4. Query proof evidence with `chdb` instead of introducing a second custom
   evidence database.
5. Support runner-owned resources: `s2 lite`, one or more host processes,
   readiness probes, teardown, and failure preservation.
6. Support deterministic faults and negative controls, so a green proof also
   demonstrates that it would fail under a known regression.
7. Keep production behavior under test in `src/Foundation` and later runtime
   modules, not in proof-local substitutes.
8. Define operation history and linearizability early enough that concurrent
   object proofs do not stop at result equality.

## Non-Goals

1. Do not use `.fsx` proof launchers.
2. Do not center the API on `section/check/expectEqual`.
3. Do not introduce a second runner-specific evidence bus unless OTel cannot
   represent a required fact.
4. Do not require an out-of-band preload/bootstrap script for normal production
   instrumentation.
5. Do not create verification-only workflow/object facades that hide production
   behavior.
6. Do not start with a large pseudo-language that is not valid F#.
7. Do not implement a full linearizability checker before the first executable
   proof runner exists.

## Architecture

The system has four layers:

1. **Telemetry primitives** in `src/Telemetry`. `Trace.withSpan`,
   `Trace.annotate`, and `Trace.event` are available to production code,
   runner code, and proof adapters.
2. **Runtime OTel setup** in `Eff.Telemetry.Otel`. Node/Fable entrypoints call
   `Otel.startFromEnv` once at process start and `Otel.shutdown` at process
   exit.
3. **Proof runner resources** in `src/Proofs`. The runner starts S2, starts
   host processes, injects env, records runner lifecycle spans, runs workloads,
   runs negative controls, writes reports, and tears down resources.
4. **Trace verification** in `src/Proofs/TraceSql.fs`. Verifiers query exported
   trace files or tables through `chdb`.

There is one evidence path:

```text
compiled proof/workload/host
  -> runner/proof-adapter spans plus product-owned spans
  -> OpenTelemetry spans
  -> runner-owned OTLP receiver/export file
  -> chdb SQL verification
  -> proof checks and JSON report
```

The runner may keep a small in-memory summary of resources for convenience, but
verification evidence should come from the trace store whenever possible.

## Instrumentation API

Milestone 0 starts with a small Fable-friendly API:

```fsharp
namespace Eff.Telemetry

module Trace =
    type Attribute = string * string

    val withSpan:
        name: string
        -> attributes: Attribute list
        -> work: Async<'a>
        -> Async<'a>

    val annotate: attributes: Attribute list -> Async<unit>
    val event: name: string -> attributes: Attribute list -> Async<unit>
```

Proof-owned evidence must not be added to production modules. If a proof needs
a span that exists only to validate the proof, emit it from the runner, host
adapter, or proof workload wrapper.

Product modules may use the same API only for baseline observability that would
still be useful without the proof runner. Those span names and attributes must
describe product behavior, not proof checks.

Proof-adapter shape:

```fsharp
let appendExpectedWithEvidence basin codec subject expected records =
    SubjectHistory.appendExpected basin codec subject expected records
    |> Trace.withSpan
        "proof.subject_history.append_expected"
        [ "proof.subject", subject
          "proof.expected.version", string expected
          "proof.record.count", string records.Length ]
```

Product-observability shape, only when the product wants this span regardless
of proofs:

```fsharp
let appendCommand basin command =
    runAppend command
    |> Trace.withSpan
        "eff.command.append"
        [ "eff.stream", command.Stream
          "eff.record.count", string command.Records.Length ]
```

This keeps validation vocabulary out of production code while still allowing
proofs to query OTel traces as their evidence ledger.

## Runtime OTel Setup

Node/Fable entrypoints should start OTel explicitly:

```fsharp
do!
    Otel.startFromEnv
        "eff-firegrid-proof-host"
        [ "eff.trial_id", trialId
          "eff.host_id", hostId ]

try
    do! runHost ()
finally
    do! Otel.shutdown ()
```

The runner injects environment, not instrumentation code:

- `EFF_TRIAL_ID`
- `EFF_HOST_ID`
- `OTEL_SERVICE_NAME`
- `OTEL_RESOURCE_ATTRIBUTES`
- `OTEL_EXPORTER_OTLP_ENDPOINT`
- `S2_ENDPOINT`

This avoids `node --require` or an out-of-band bootstrap while still giving the
runner control over where spans are exported.

## Trace Store And chDB

The runner owns a per-trial trace directory:

```text
.eff-firegrid/proofs/
  <trial-id>/
    traces/
      spans.jsonl
      spans.parquet
    report.json
```

The first exporter can be simple:

1. Start a local OTLP HTTP receiver for the trial.
2. Convert received spans into JSONL with stable columns.
3. Optionally write Parquet once the schema stabilizes.
4. Query with `chdb`.

Initial `spans` columns:

```text
trial_id String
service_name String
host_id Nullable(String)
trace_id String
span_id String
parent_span_id Nullable(String)
name String
kind String
status_code String
status_message Nullable(String)
start_unix_nanos UInt64
end_unix_nanos UInt64
attributes JSON
events JSON
```

Example verifier query:

```sql
SELECT count()
FROM file('spans.jsonl', JSONEachRow)
WHERE trial_id = {trial_id:String}
  AND name = 'verification.host.ready'
  AND attributes.`host.name` = 'a'
```

The F# API should keep verifiers typed at the edge:

```fsharp
module TraceSql =
    type Query =
        { Sql: string
          Parameters: Map<string, string> }

    val scalarInt: TraceStore -> Query -> Async<int>
    val exists: TraceStore -> Query -> Async<bool>
```

The implementation can bind to `chdb` from Fable using small interop functions.

## Authoring Shape

Use shallow computation expressions. A proof is a compiled F# value; a property
has resources, workload, and verifiers.

```fsharp
let storeObjectSerialization =
    proof "store.object-serialization" {
        describedAs
            "Concurrent same-key object calls serialize through the S2-backed object log and do not lose writes."

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

                    TraceExpect.spanExists
                        "object append happened"
                        "object.append"
                        [ "object.key", "counter-1" ]
                ]
            })
    }
```

## Multiple Host Processes

Multiple host processes are property resources. The runner starts them, injects
standard environment, waits for readiness, records lifecycle spans, and tears
them down.

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
                    }

                    processHost "b" {
                        command "pnpm"
                        args [ "exec"; "tsx"; workerPath ]
                        env "HOST_PORT" (string portB)
                        readinessUrl (hostB + "/ready")
                    }
                ]

                workload StoreObjectLiveFencing.run

                verify [
                    Expect.workload "late owner did not change value" (fun r ->
                        r.LateOwnerDidNotChangeValue)

                    TraceExpect.spanExists
                        "host a was ready"
                        "verification.host.ready"
                        [ "host.name", "a" ]

                    TraceExpect.spanExists
                        "host b was ready"
                        "verification.host.ready"
                        [ "host.name", "b" ]
                ]
            })
    }
```

The important part is ownership: proof definitions describe host specs; the
runner owns actual process lifecycle and emits runner lifecycle spans.

## Guarantee Model

A green proof is not a guarantee by itself. It says one workload passed against
the current implementation. To trust a proof, the runner must also establish
that the proof would fail under a known regression or deterministic fault.

Every durable correctness claim needs:

1. **Positive control**: the production implementation satisfies the property.
2. **Negative control**: the same property fails when the runner injects a
   targeted mutation, append failure, crash point, or known-bad variant.

Authoring shape:

```fsharp
property "effect-ledger-proof" {
    s2Lite LocalRoot

    workload EffectLedgerProof.runHappyPath

    verify [
        Expect.workload "journaled step invariants hold" EffectLedgerProof.invariantsHold
    ]

    negativeControl "broken result decoder is caught" {
        mutation (Mutation.replace "EffectLedger.decodeResult" EffectLedgerFaults.destructiveSplit)
        expectFailure "journaled step invariants hold"
    }
}
```

The exact mutation mechanism can start simple. A negative control may be:

- a proof-owned alternate codec or reducer marked as intentionally broken
- a runner-owned S2 append failure at a deterministic boundary
- a process crash after a named trace span or event
- a build-time mutation hook if the codebase later supports mutation testing

The rule is strict: a claim is not proven unless the positive proof passes and
its negative controls fail for the expected reason.

## Deterministic Fault Injection

Faults are not only process kills. The runner needs deterministic, replayable
faults at storage and trace boundaries.

Initial fault controls:

```fsharp
type FaultController =
    abstract FailAppend: ordinal: int -> Async<unit>
    abstract KillHost: name: string -> Async<unit>
    abstract RestartHost: name: string -> Async<unit>
    abstract KillAfterSpan:
        host: string
        * spanName: string
        * attributes: Map<string, string>
        -> Async<unit>
```

The first high-value cases:

1. **Effect ledger append boundaries**: fail append `k`, restart by re-folding,
   and assert effectively-once still holds at every journal boundary.
2. **Crash before result**: kill after the intent span/event but before the
   result span/event, then assert the effect may run again and the ledger
   records one committed result.
3. **Fencing**: install a newer fence token, then assert an older owner gets
   `FencingTokenMismatch` and cannot commit.
4. **Live owner race**: kill or pause host `a` after trace evidence says it
   started a call, allow host `b` to take over, then assert host `a` cannot
   commit stale state.

Every fault must be represented in spans, report JSON, and replay command.
Random faults are allowed only when seeded and reported.

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
      Verifiers: Check<'result> list
      NegativeControls: NegativeControlSpec list }

type RunningS2 =
    { Endpoint: string
      Basin: S2.Basin }

type RunningHost =
    { Name: string
      Endpoint: string
      ProcessId: int }

type TraceStore =
    { TrialId: string
      Root: string
      SpansJsonl: string }

type WorkloadContext =
    { TrialId: string
      S2: RunningS2
      Hosts: Map<string, RunningHost>
      Traces: TraceStore
      Faults: FaultController }

type CompletedTrial<'result> =
    { ProofName: string
      PropertyName: string
      TrialId: string
      Result: Result<'result, exn>
      Traces: TraceStore
      NegativeControlResults: NegativeControlResult list }

type Check<'result> =
    { Name: string
      Run: CompletedTrial<'result> -> Async<Result<unit, string>> }
```

The first implementation can constrain all properties to require `s2Lite`.
Relax that later if a proof does not need S2.

## Runner Order

For each selected proof property:

1. Generate a trial id.
2. Create a per-trial root directory.
3. Start the OTLP receiver/exporter for the trial.
4. Start `s2 lite` if requested.
5. Start all declared host processes.
6. Inject standard environment into every host.
7. Wait for every readiness probe.
8. Emit `verification.host.ready` and resource lifecycle spans.
9. Run the workload.
10. Capture the workload result or exception.
11. Flush spans into the trace store.
12. Run verifiers against `CompletedTrial`.
13. Run every negative control as a separate trial.
14. Assert every negative control fails for the expected reason.
15. Write a JSON report when requested, on positive failure, or on unexpected
    negative-control pass.
16. Stop hosts in reverse creation order.
17. Stop `s2 lite`, unless preservation is requested after a failure.
18. Stop the OTLP receiver after final span flush.

Checks such as `TraceExpect.spanExists "host a ready"` assert runner-observed
spans, not proof-local guesses.

## Operation History And Linearizability

Workload-result equality checks one outcome. It does not prove a concurrent
history is legal. Operation history should be represented as spans using a
stable schema.

Required operation span attributes:

```text
eff.operation.id
eff.operation.client_id
eff.operation.name
eff.operation.key
eff.operation.input_json
eff.operation.output_json
eff.operation.error_json
eff.operation.status
```

The model interface should be defined before full checker implementation:

```fsharp
type OperationEvent =
    { ClientId: string
      OperationId: string
      Name: string
      Key: string option
      InputJson: string
      OutputJson: string option
      ErrorJson: string option
      Status: string
      CallTimeNanos: uint64
      ReturnTimeNanos: uint64 }

type SequentialModel<'state> =
    { Name: string
      Initial: 'state
      Step: 'state -> OperationEvent -> Result<'state, string> }
```

The checker loads `OperationEvent` values from span rows and runs a
Porcupine/Knossos-style search against the sequential model.

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
  "traceStore": {
    "spansJsonl": ".eff-firegrid/proofs/<trial>/traces/spans.jsonl"
  },
  "queries": [],
  "negativeControls": [],
  "faultPlan": [],
  "seed": 123456,
  "generatedWorkload": null,
  "replayCommand": "npm run proofs -- --proof store.object-live-fencing --trial-id store.object-live-fencing-1782622956956"
}
```

Reports must include:

- proof name
- property name
- trial id
- workload result or error
- failed checks
- host metadata
- S2 metadata
- trace store paths
- failed SQL queries or query summaries
- negative-control results
- fault plan
- seed and generated workload, when present
- replay command

## Claim Coverage

Narrative checkboxes drift. The runner should bind durable claims to proofs and
fail CI when a claim marked as proven lacks a passing proof.

```fsharp
type ClaimCoverage =
    { Claim: string
      Proofs: string list
      RequiredChecks: string list
      RequiresNegativeControl: bool }
```

The CI gate should fail when:

- a `docs/*` claim is marked proven but has no `ClaimCoverage`
- a required proof did not run
- a required check did not pass
- a claim requires negative controls and none failed as expected

## Source Layout

Recommended source layout:

```text
src/
  Telemetry/
    Trace.fs         # withSpan, annotate, event
    Otel.fs          # Node OTel SDK setup for Fable entrypoints
  Proofs/
    Proof.fs         # proof CE and Proof<'result>
    Property.fs      # property CE and PropertySpec<'result>
    ProcessHost.fs   # processHost CE, readiness, lifecycle
    S2Lite.fs        # s2 lite supervisor
    Expect.fs        # workloadResult, predicate
    TraceSql.fs      # chdb query interop
    TraceExpect.fs   # spanExists and operation-history checks
    Faults.fs        # fault controller
    NegativeControls.fs
    Reports.fs
    Runner.fs        # selection, execution, summary, exit code
    Registry.fs      # compiled list of proofs
```

Avoid a central `Harness.fs` abstraction. The proof runner is
`Property + Runner + Trace verification`, not `Context + check`.

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

The CLI should parse arguments directly:

```text
proofs list
proofs run [--proof <substring>] [--trial-id <id>] [--preserve]
proofs replay <report.json>
```

`npm run proofs` can map to `proofs run`.

## Milestone 0: Telemetry Foundation

This milestone has started.

Deliverables:

1. `src/Telemetry/Trace.fs`
2. `src/Telemetry/Otel.fs`
3. npm dependencies for `@opentelemetry/api`,
   `@opentelemetry/sdk-node`, `@opentelemetry/exporter-trace-otlp-http`,
   and `chdb`
4. first trace-query building block in `src/Proofs/TraceSql.fs`
5. compiled Fable smoke coverage that exercises `TraceSql` against a real
   JSONEachRow span file

Acceptance criteria:

1. `dotnet build` succeeds with FS1182.
2. Fable compilation succeeds.
3. `npm run check` proves `TraceSql.spanExists` can find and reject
   proof-owned span rows through `chdb`.
4. The tracing API remains usable without a proof runner.
5. No proof/validation spans are emitted from production modules.

## Milestone 1: Compiled Property Runner

Deliverables:

1. proof and property CEs
2. compiled proof registry
3. basic runner CLI
4. S2 lite resource lifecycle
5. workload result checks
6. OTLP receiver/exporter writing `spans.jsonl`
7. `TraceSql` backed by `chdb`
8. `TraceExpect.spanExists`

Acceptance criteria:

1. Proofs are compiled into `eff-firegrid.fsproj`.
2. `npm run proofs -- --proof foundation.subject-history` starts the trace
   store, runs the workload, runs verifiers, and exits non-zero on failure.
3. At least one verifier queries spans through `chdb`.
4. Failed proofs write a JSON report with trace store paths and replay command.
5. No `.fsx` proof launcher exists.

## Milestone 2: Negative Controls And Faults

Deliverables:

1. `FaultController`
2. negative-control runner
3. deterministic append failure hooks
4. process kill/restart hooks
5. expected-failure verification

Acceptance criteria:

1. At least one proof has a negative control that fails as expected.
2. An unexpected negative-control pass fails the run.
3. Fault plans are represented in spans and reports.
4. Replay can reproduce the same fault plan.

## Milestone 3: Effect Ledger Boundary Proofs

Production surface:

- `src/Foundation/EffectLedger.fs`
- journaled steps
- replay from S2 records
- deterministic ids

Proof requirements:

- positive control proves effectively-once on replay
- positive control proves at-least-once on crash-before-result
- positive control proves deterministic ids
- negative control reintroduces a known-bad decode/mutation and fails for the
  expected check
- deterministic fault plan fails or kills at each append boundary and asserts
  the invariant after recovery

## Milestone 4: First Object Serialization Proof

Implement the closest F# equivalent of the fluent
`store-object-serialization.ts` proof.

Proof:

- workload runs two concurrent same-key `add` calls
- workload reads final value
- verifier checks completed calls, max result, and final value
- verifier checks relevant object and S2 spans through `TraceSql`
- no proof-local fake runtime

## Milestone 5: Multiple Host Live-Fencing Proof

Implement the closest F# equivalent of the fluent
`store-object-live-fencing.ts` proof.

Runner additions:

- `hosts [ processHost "a" { ... }; processHost "b" { ... } ]`
- readiness probes
- runner spans for host start/ready/stop
- fault controller with host kill/stop/restart

Proof:

- starts S2 lite
- starts host `a`
- starts host `b`
- begins a deposed request on host `a`
- waits until trace evidence shows the deposed call started
- sends takeover request to host `b`
- verifies host `b` owns the final value
- verifies spans show both hosts were ready and the stale owner was fenced

## Milestone 6: Operation History And Linearizability

Deliverables:

- operation span schema
- operation wrapper for workload code
- JSON report operation history
- initial linearizability adapter API
- counter sequential model
- CI check for illegal histories

Workload-result checks and lifecycle spans are useful smoke tests. Operation
history plus a sequential model is the guarantee for concurrent object
behavior.

## Milestone 7: Generated Workloads And Claim Coverage

Deliverables:

- generated workload API
- seed and shrinker support
- minimized counterexample reports
- claim coverage metadata
- CI gate for proven claims without passing coverage

## Design Rules

1. A proof is a compiled declaration of properties.
2. A property has resources, workload, and verifiers.
3. Host processes are resources owned by the runner.
4. Proof-owned evidence is emitted by runner/proof-adapter code, not production
   modules.
5. Production code may use `Trace.withSpan` only for product-owned
   observability that is useful without proofs.
6. Proof code may drive production APIs, HTTP handlers, CLIs, or generated
   clients.
7. Proof code must not define a fake production runtime to make verification
   easier.
8. OTel traces are the canonical proof evidence ledger.
9. chDB SQL is the first query path for proof evidence.
10. Negative controls are required for claims marked proven.
11. Deterministic fault plans must be reportable and replayable.
12. `section/check/expectEqual` are not the proof API.
13. CE syntax must remain valid F#.
14. The compiled-project enforcement must survive the migration from
    `Harness.fs` to `Runner.fs`.

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

                workload FoundationSubjectHistoryProof.run

                verify [
                    Expect.workload "append and fold summary is valid" (fun result ->
                        result.TailAdvanced
                        && result.ConflictObserved
                        && result.FoldObservedPendingTimer)

                    TraceExpect.spanExists
                        "proof append expected span emitted"
                        "proof.subject_history.append_expected"
                        [ "proof.subject", result.Subject ]
                ]
            })
    }
```

That keeps the compiled foundation proof, but expresses it through the same
property runner as the object proofs.
