# Durable Execution Next-Agent Handoff

## Purpose

The next agent should move this project from "proven slices" toward a
maintainable, consumable durable execution library with production-usecase
testing. The immediate goal is not to add more substrate mechanics. The goal is
to refactor the current durable surface into a cleaner shape, keep the existing
proof coverage intact, and start testing realistic workflows against S2-backed
execution.

Use this document as the operational handoff. The product/spec source of truth
is still:

- `docs/durable-execution-library-sdd.md`
- `docs/durable-fsharp-sdd-r2-substrate.md`
- `docs/durable-ergonomics-proposal.md`
- `docs/proof-runner-proposal.md`
- `docs/foundational-sdd.md`

## Current Repository State

As of `main` after PR #53:

- `main` is clean and fast-forwarded to `origin/main`.
- The durable execution facade has shipped through L9 in
  `docs/durable-execution-library-sdd.md`.
- The latest full validation baseline is:
  - `npm run format`
  - `npm run check`
  - 55 integration tests passed
  - 24 proof properties passed
  - 0 failures
- GitHub CI static quality gate passed for each recent PR.
- The live S2 CI job is currently skipped by workflow rules; local integration
  checks have been exercising S2 through the configured test basin.

Recent merged durable PRs:

- #45 app facade: `Activity`, `Workflow`, `Signal`, `durableApp`, app
  `clientWith` / `workerWith`
- #46 app-level client result/status types
- #47 typed codecs on handles
- #48 proof-layer durable test host
- #49 environment bootstrap
- #50 worker discovery and service loop
- #51 typed workflow status
- #52 race facade
- #53 typed test-host assertions

## Current Public Shape

Normal app code should think in these nouns:

- app: collection of activities, workflows, and signals
- client: start workflows, signal instances, read status
- worker: execute admitted durable work
- test host: proof/test driver for local deterministic execution

The important public modules/types are in `src/Foundation/Durable/App.fs`:

- `Activity<'input,'output>`
- `Workflow<'input,'output>`
- `Signal<'payload>`
- `DurableRace<'result>`
- `Activity.define` / `defineWith`
- `Workflow.define` / `defineWith`
- `Signal.define` / `defineWith`
- `Durable.call`
- `Durable.waitForSignal`
- `Durable.raceSignal`
- `Durable.raceTimer`
- `Durable.anyOf`
- `durableApp { activity ...; workflow ... }`
- `DurableApp.clientWith` / `workerWith`
- `DurableApp.client` / `worker`
- `DurableAppClient.statusOf`
- `DurableAppWorker.runReady`, `runReadyWith`, `runForever`

Proof/test helper surface is in `src/Proofs/DurableTestHost.fs`:

- `DurableTestHost.start ctx app`
- `host.client.start`, `startWith`, `signal`, `status`, `statusOf`
- `host.expect.completed`
- `host.expect.completedOf`
- `DurableTestHost.runUntilCompleted`
- `host.cleanup`

## Core Architecture Map

Foundation:

- `src/Foundation/SubjectHistory.fs`
- `src/Foundation/StateView.fs`
- `src/Foundation/KvStore.fs`

Durable substrate:

- `src/Foundation/Durable/Semantics.fs`
  Pure replay model, history, needs, race semantics.
- `src/Foundation/Durable/Api.fs`
  Lower workflow helpers and computation expression.
- `src/Foundation/Durable/S2Substrate.fs`
  `{instance}/log` and `{instance}/in` stream model, ownership/fencing.
- `src/Foundation/Durable/Stepper.fs`
  Replay need to durable step records.
- `src/Foundation/Durable/StepRecordCodec.fs`
  Shared log codec.
- `src/Foundation/Durable/InboxFold.fs`
  Durable inbox admission into log history.
- `src/Foundation/Durable/ActivityAdapter.fs`
  Committed activity command dispatch, inbox completion publish,
  checkpointing.
- `src/Foundation/Durable/TimerAdapter.fs`
  Committed timer command dispatch, due timer publish, checkpointing.
- `src/Foundation/Durable/Host.fs`
  Claimed durable host tick.
- `src/Foundation/Durable/Client.fs`
  Start, signal, and status admission/observation.
- `src/Foundation/Durable/Runtime.fs`
  Lower runtime composition.
- `src/Foundation/Durable/App.fs`
  Current public app facade.

Proof system:

- `src/Proofs/Proof.fs`, `Property.fs`, `Verification.fs`, `Expect.fs`
- `src/Proofs/Trace*.fs`
- `src/Proofs/Durable*Proof.fs`
- `src/Proofs/Registry.fs`

Examples:

- `examples/durable-tutorial/src/Tutorial.fsx`

## Known Design Debt

These are the areas most likely to block a production-quality developer
experience.

### App.fs Is Too Large

`src/Foundation/Durable/App.fs` now contains:

- handle definitions
- storage/environment bootstrap
- client projection
- worker discovery/service loop
- race facade
- builder syntax

Refactor target:

```text
src/Foundation/Durable/App/
  Definitions.fs
  DurableFacade.fs
  Client.fs
  Worker.fs
  Environment.fs
  Storage.fs
  Builder.fs
```

Keep the namespace and public surface stable while splitting files. Update
`eff-firegrid.fsproj` ordering carefully.

### Proof-Only Test Host Is Not Yet Productized

`Eff.Proofs.DurableTestHost` is useful but lives in `src/Proofs`. That is
correct for proof harness work, but if we want application developers to test
durable apps outside proof scripts, we need either:

- a public test package/module later, or
- a clean sample pattern that proves the intended shape first.

Do not prematurely move it into the public app namespace until the production
test scenarios below force the right shape.

### Codecs Are Manual

Typed handles currently require explicit encode/decode functions:

```fsharp
Workflow.defineWith "typed-math" string int string int ...
```

This is correct for proofable substrate stability, but not ergonomic enough for
real apps. Future work should consider:

- JSON codec helpers for common Fable payloads
- versioned payload codecs
- decode failure behavior as a first-class app concern

Do not change durable storage away from string payloads without a dedicated SDD
update and migration story.

### Worker Discovery Is Stream-Listing Based

`DurableAppWorker.discover` lists active streams and selects names ending in
`/in`. This is acceptable for the first worker loop proof, but it is not a
production-grade scheduler/index.

Production direction:

- durable instance index stream
- ready/active instance cursor
- bounded fairness
- avoiding repeated scans over completed instances
- metrics around discovered/active/completed/deposed/failed instances

The current `Active` flag prevents completed streams from making `runForever`
spin, but discovery still scans old streams.

### Environment Bootstrap Is Minimal

`DurableApp.client` / `worker` resolve S2 environment variables:

- `EFF_FIREGRID_<ENV>_BASIN`
- `EFF_FIREGRID_BASIN`
- `EFF_FIREGRID_<ENV>_ACCESS_TOKEN`
- `EFF_FIREGRID_ACCESS_TOKEN`
- `EFF_FIREGRID_<ENV>_S2_ACCOUNT_ENDPOINT`
- `EFF_FIREGRID_S2_ACCOUNT_ENDPOINT`
- `EFF_FIREGRID_<ENV>_S2_BASIN_ENDPOINT`
- `EFF_FIREGRID_S2_BASIN_ENDPOINT`

If none are supplied for credentials/endpoints, it falls back to S2 CLI config.

This is enough for examples/proofs, not enough for polished production
configuration. Expect to add explicit config validation and better error
messages.

### Error Surface Needs Review

There are now app-level failures, host failures, client failures, adapter
failures, status failures, and proof failures. They are mostly typed but not yet
curated into a coherent product story.

Production direction:

- classify admission failures vs execution failures vs observation failures
- decide what should be retryable
- expose stable app-facing failure types
- keep lower substrate evidence available for diagnostics

## Required Worktree Discipline

Always work in isolated worktrees for nontrivial changes.

Recommended pattern:

```sh
cd /Users/gnijor/gurdasnijor/eff-firegrid
git fetch origin main
git merge --ff-only origin/main
git worktree add ../eff-firegrid-<slice-name> -b <branch-name> origin/main
cd ../eff-firegrid-<slice-name>
```

Before commit:

```sh
npm run format
npm run check
git diff --check
git status --short
```

If a fresh worktree lacks `node_modules`, run:

```sh
npm install
```

Be aware that `npm install` currently mutates `package-lock.json` metadata on
this machine. Do not commit that drift unless intentionally changing
dependencies. The usual unwanted diff is:

- removes `libc: ["glibc"]` from linux `@chdb` packages
- adds `"peer": true` to some dependencies

Restore that diff before committing.

## Immediate Next Agent Mission

The next agent should not keep adding small facade methods blindly. The next
phase should be:

1. Refactor the durable app facade into cleaner files without changing public
   behavior.
2. Split examples into concept-focused durable examples.
3. Add production-usecase tests/proofs against realistic workflows.
4. Improve docs so a developer can start from the README and build a durable app
   without learning substrate internals first.

## Suggested PR Sequence

### PR A: Refactor App Facade File Layout

Goal: make future work maintainable.

Scope:

- split `src/Foundation/Durable/App.fs` into multiple files or clearly
  separated modules
- preserve public namespace/API
- update `eff-firegrid.fsproj`
- no semantic changes unless a bug is found

Acceptance:

- no public example changes required
- `npm run check` passes
- proof count remains 24 unless a new proof is added

Risks:

- F# file ordering is strict; move definitions before consumers
- avoid changing generated JS module names in ways Fable cannot resolve

### PR B: Split Examples By Concept

Goal: make examples feel closer to Restate tutorial organization.

Target structure:

```text
examples/durable/
  01-hello-sequence/src/Program.fsx
  02-checkout/src/Program.fsx
  03-external-signal/src/Program.fsx
  04-signal-timeout/src/Program.fsx
  05-tests/src/Program.fsx
```

Keep `examples/durable-tutorial` temporarily if needed, but avoid two divergent
sources of truth for long.

Update `tools/repo/Program.fs` so `ExamplesSmoke` compiles every durable example,
not just `examples/durable-tutorial/src/Tutorial.fsx`.

Acceptance:

- `npm run smoke:examples` compiles all examples
- examples avoid raw substrate APIs in first screens
- examples use `statusOf`, `anyOf`, and typed test-host helpers where relevant

### PR C: Production Usecase Proofs

Goal: start testing the system against workflows that resemble real usage, not
only layer-specific proofs.

Add one new proof file, for example:

```text
src/Proofs/DurableProductionUsecasesProof.fs
```

Initial scenarios:

- checkout success:
  - reserve inventory activity
  - charge payment activity
  - send receipt/log activity or log step
  - typed status completes with order result
- human approval:
  - workflow waits for external signal
  - duplicate signal admission folds once
  - signal after wait completes workflow
- approval timeout:
  - `Durable.anyOf [ raceSignal; raceTimer ]`
  - signal wins before deadline
  - timer wins after deadline
- fan-out/fan-in:
  - parallel activity set
  - all completions are required exactly once
- idempotent activity dispatch:
  - publish-before-checkpoint retry does not invoke handler again
  - completion is durable before checkpoint
- worker restart:
  - run part of workflow
  - recreate client/worker over same basin
  - continue to completion from durable records
- multi-worker fencing:
  - two workers try to run the same instance
  - stale/deposed worker cannot commit progress
- codec failure:
  - bad completion/status decode is typed
  - no silent success

Acceptance:

- proof uses app facade where possible
- lower substrate APIs appear only when proving crash/fencing/restart behavior
- proof emits spans with scenario labels
- proof has workload assertions and trace assertions

### PR D: Production Worker Loop Test

Goal: test a daemon-like worker path over more than one instance.

Scenarios:

- start many instances
- `runReadyWith` honors bound
- repeated `runReady` eventually completes all
- completed instances become inactive
- a waiting signal instance stays waiting but does not starve runnable instances
- cancellation of `runForever` exits without extra durable writes

This should probably build on PR C rather than precede it.

### PR E: README Quickstart

Goal: a developer can copy a small app and know how to run it.

Add root `README.md` or update if one exists.

Must include:

- what this repo/library is
- install/build commands
- hello-sequence durable app snippet
- explicit local/S2 environment bootstrap
- run worker locally
- run tests/proofs
- where to find deeper SDDs

Avoid:

- making users read about fences, inbox streams, or `DurableRuntime` first

## Production Usecase Test Design

Use production-usecase tests to prove product claims, not implementation details.

Preferred pattern:

```fsharp
let private app =
    durableApp {
        activity reserveInventory
        activity chargePayment
        workflow checkout
    }

let private runWorkload ctx =
    ProofOperation.run ctx "durable.production.checkout" "durable-production-usecases" options (async {
        let! host = DurableTestHost.start ctx app
        let! completed = DurableTestHost.runUntilCompleted host checkout input expected
        do! host.cleanup ()
        return { CheckoutCompleted = completed }
    })
```

When crash/fencing/restart behavior is the point, it is acceptable to drop to
lower APIs, but keep that contained and explain why in code comments or proof
names.

Required proof evidence:

- workload booleans for functional behavior
- trace span emitted for each production scenario
- operation trace recorded through `ProofOperation.run`
- where possible, status is read from durable history, not from local variables

## Production Usecase Matrix

Start with these cases.

| Usecase | What It Proves | Preferred Surface |
| --- | --- | --- |
| Hello sequence | simple sequential activity chain | app facade |
| Checkout | multiple typed activities and typed status | app facade |
| Approval signal | durable external event admission | app facade |
| Approval timeout | race facade over signal/timer | app facade |
| Fan-out/fan-in | multiple activity scheduling/completion | lower `Workflow.all` or facade if added |
| Worker restart | durable recovery from log/inbox | app facade plus recreated worker |
| Duplicate signal | source sequence idempotency | lower client or controlled app facade |
| Duplicate activity completion | inbox fold idempotency | lower adapter/fold as needed |
| Multi-worker same instance | fencing/deposed behavior | lower host/runtime |
| Long timer not due | timer adapter revisit behavior | lower timer proof or app facade |
| Codec decode failure | typed failure, no false completion | app facade |
| Worker service loop many instances | discovery/bounded pass/fairness | app worker facade |

## Refactor Rules

- Keep public API stable unless the SDD is updated first.
- Keep proofs green after each slice.
- Prefer moving code over rewriting behavior in refactor PRs.
- Do not mix broad file layout refactors with semantic changes.
- Preserve lower APIs for proofs and diagnostics.
- Add a proof whenever changing durable semantics, admission, worker behavior, or
  error projection.
- Add an example when changing app ergonomics.

## Validation Commands

Normal:

```sh
npm run format
npm run check
```

Focused proof:

```sh
npm run proofs -- --proof durable-test-host
npm run proofs -- --proof durable-race-facade
npm run proofs -- --proof durable-typed-status
```

Examples:

```sh
npm run smoke:examples
```

Full clean when state seems stale:

```sh
npm run clean
npm run check
```

## Current CI Caveat

GitHub's static quality gate is the reliable PR gate. The live S2 integration
job has been skipped by workflow rules on recent PRs. Local `npm run check`
currently runs the integration suite and proof suite against the configured
environment. If making production-usecase tests, explicitly verify whether the
CI workflow should start running live S2 coverage, or whether the proof should
stay inside `s2Lite`.

## What Not To Do Next

- Do not add another facade feature before refactoring `App.fs`, unless the
  production-usecase proof makes it obviously necessary.
- Do not introduce a public "runtime" concept. The user explicitly disliked that
  terminology. Use app/client/worker/test host.
- Do not expose `OwnedKey`, `FenceToken`, `DurableStepper`, `StepRecordCodec`,
  `DurableCommandDispatch`, or `DurableRuntime.create` in beginner docs.
- Do not treat proof booleans as sufficient production evidence. Use trace
  assertions and durable status/log evidence.
- Do not merge lockfile drift from `npm install`.

## Suggested Bootstrap Prompt For The Next Agent

Use this if starting a fresh agent:

```text
You are working in /Users/gnijor/gurdasnijor/eff-firegrid.

Read these first:
- docs/durable-next-agent-handoff.md
- docs/durable-execution-library-sdd.md
- docs/durable-ergonomics-proposal.md
- docs/durable-fsharp-sdd-r2-substrate.md
- docs/proof-runner-proposal.md

Work in an isolated git worktree off origin/main. Do not edit directly on main.

Mission:
1. Refactor the durable app facade into a cleaner file/module layout without
   changing public behavior.
2. Then split examples by concept and make smoke:examples compile all of them.
3. Then add production-usecase proofs for checkout, approval signal,
   approval timeout, fan-out/fan-in, restart, duplicate admission, multi-worker
   fencing, codec failure, and worker service loop behavior.

Use app/client/worker/test-host vocabulary. Do not introduce "runtime" as a
public concept. Keep substrate APIs available only for proofs/diagnostics.

Validation before each PR:
- npm run format
- npm run check
- git diff --check

Open a draft PR, wait for CI, then mark ready and merge when green.
```
