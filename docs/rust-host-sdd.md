# Fable Rust Target SDD

## Status

Draft implementation direction for making Firegrid's F# durable core compile
through Fable's Rust target.

This is not a proposal to hand-port the core to Rust. F# remains the source of
truth for durable semantics while we evaluate Rust as a compile target.

## Problem

The current repository has useful durable semantics, but the runtime path is
coupled to JavaScript-specific Fable interop:

- S2 access goes through `@s2-dev/streamstore`
- telemetry imports Node/OpenTelemetry packages
- proof resources use Node filesystem/process/chdb interop
- subprocess work would otherwise require more `child_process` bindings

That coupling blocks a clean Rust compile and makes process supervision harder
than it needs to be.

## Goal

Make an increasing slice of the existing F# codebase compile through:

```sh
dotnet fable targets/fable-rust/FiregridRust.fsproj --lang rust --outDir targets/fable-rust
```

Then compile and run the generated Rust with Cargo:

```sh
cd targets/fable-rust
cargo run
```

Native Rust crates are still useful for systems boundaries later, especially
`s2-sdk` and process supervision, but the first priority is F# source
compatibility with Fable Rust.

Target split:

```text
Authoring layer
  step / workflow / entity descriptors
  durable authoring ergonomics
  descriptor serialization

Rust durable core
  shared value types
  codecs
  durable state machines
  replay/fold semantics
  command planning
  descriptor validation

Rust host
  native S2 client
  durable stream/inbox/log ownership
  host loop
  timer dispatcher
  process supervision
  telemetry
```

## Fable Rust Design Rules

The F# source should become friendlier to Fable Rust without breaking the
current .NET/Fable-JS path.

Required rules:

- preserve F# as the durable core source of truth
- avoid JS interop in files included by `targets/fable-rust/FiregridRust.fsproj`
- keep target-specific `#if FABLE_RUST` sections small and explicit
- prefer refactors that also improve the normal F# code
- do not introduce hand-written Rust equivalents for core semantics while a
  Fable Rust path is viable
- expand the Fable Rust target one source file at a time

Non-goals for this phase:

- hand-porting durable core modules to Rust
- forcing the whole current repo through Fable Rust immediately
- pretending Fable Rust can currently lower every F# continuation pattern
- replacing the production runtime before the compile target is credible

## Native Dependencies

Initial Rust host dependencies:

- `s2-sdk`: native S2 client
- `tokio`: async runtime
- `processkit`: candidate subprocess/process-tree runner
- `tracing` / `opentelemetry`: native telemetry path

The branch currently includes a compile-backed `firegrid-host` scaffold using
`s2-sdk = "0.31.7"`.

## Migration Inventory

The migration should be explicit about what moves, what is replaced, and what
stays as an authoring/proof concern.

| Current area | Current files | Rust target | Notes |
| --- | --- | --- | --- |
| S2 client bindings | `src/S2/Interop.fs`, `src/S2/Client.fs`, `src/S2/Patterns.fs`, `src/S2/Cli.fs`, `src/S2/Errors.fs` | `crates/firegrid-host/src/s2/*` or `crates/firegrid-s2` | Replace JS SDK calls with `s2-sdk`. Keep only concepts needed by durable host: basin, stream, append, read, fence, trim, sessions if required. |
| Subject history | `src/Foundation/SubjectHistory.fs` | `crates/firegrid-core/src/subject_history.rs` | Port directly. This is mostly pure fold/cursor/version logic and should be a first Rust unit-test target. |
| State view | `src/Foundation/StateView.fs` | `crates/firegrid-core/src/state_view.rs` plus host integration if needed | Split pure fold state from background cursor/pump behavior. Rust implementation can use native async tasks. |
| KV store | `src/Foundation/KvStore.fs` | likely defer or port after substrate | Useful validation case, but not needed before durable workflow host. |
| Durable semantics/API | `src/Foundation/Durable/Semantics.fs`, `src/Foundation/Durable/Api.fs` | `crates/firegrid-core/src/durable/*` | Port replay terms, history, needs, race terms, activity/timer/signal records. This is the core behavior to preserve. |
| Registry | `src/Foundation/Durable/Registry.fs` | `crates/firegrid-core/src/registry.rs` | Needed for named workflows/activities. Rust may use stronger typed maps and duplicate detection. |
| S2 substrate | `src/Foundation/Durable/S2Substrate.fs` | `crates/firegrid-host/src/substrate.rs` | Native S2 implementation with fencing and inbox/log streams. Depends on `s2-sdk`. |
| Stepper/codecs | `src/Foundation/Durable/Stepper.fs`, `src/Foundation/Durable/StepRecordCodec.fs` | `crates/firegrid-core/src/stepper.rs`, `crates/firegrid-core/src/codec.rs` | Port command planning and record encoding early. Prefer explicit serde formats over ad hoc string parsing if compatibility is not required. |
| Command dispatch | `src/Foundation/Durable/CommandDispatch.fs` | `crates/firegrid-host/src/dispatch.rs` | Reads outgoing commands, checkpoints dispatcher progress, enforces idempotence. |
| Activity adapter | `src/Foundation/Durable/ActivityAdapter.fs` | `crates/firegrid-host/src/activity.rs` | Dispatch registered activity/process steps and publish completion envelopes. |
| Inbox fold | `src/Foundation/Durable/InboxFold.fs` | `crates/firegrid-host/src/inbox.rs` | Fold inbox completions/signals/starts into workflow log exactly once. |
| Timer adapter | `src/Foundation/Durable/TimerAdapter.fs` | `crates/firegrid-host/src/timers.rs` | Native Rust timer loop; no JS timers. |
| Durable host/runtime/app | `src/Foundation/Durable/Host.fs`, `Runtime.fs`, `App.fs`, `Client.fs` | `crates/firegrid-host/src/runtime.rs`, `client.rs`, `app.rs` | Rebuild around Rust ownership, host loops, typed status, and app descriptor loading. |
| Public F# API | `src/Firegrid.fs` | defer; descriptor authoring layer | Do not port first. The Rust host should define what descriptors it consumes; ergonomic F# can target that after host shape stabilizes. |
| Telemetry | `src/Telemetry/Trace.fs`, `src/Telemetry/Otel.fs` | `crates/firegrid-host/src/telemetry.rs` | Replace JS OpenTelemetry imports with `tracing` first; OTLP export can follow. |
| Proof runner resources | `src/Proofs/*`, especially `S2Lite.fs`, `ProcessHost.fs`, `Reports.fs`, `TraceSql.fs` | Rust unit/integration tests plus optional acceptance runner | Do not port wholesale. Preserve proof properties as test cases where useful. |
| CLI/process agent support | PR #62 concepts, not merged here | `crates/firegrid-host/src/process.rs` | Implement with `processkit`; subprocess execution belongs in Rust host, not Fable interop. |

## Compile Target

The first compile contract is:

```sh
cargo check -p firegrid-host
```

This should compile without Node, npm packages, Fable JS interop, or generated
JavaScript.

The current scaffold performs a native S2 client construction and optional smoke
operation:

```sh
S2_ACCESS_TOKEN=... cargo run -p firegrid-host
```

Without credentials it exits successfully and prints a setup hint. With
credentials it calls `list_basins` through the native Rust SDK.

## Migration Plan

### Phase 1: Host Skeleton

Deliver:

- Cargo workspace
- `firegrid-host` binary
- native S2 client config
- no-credential startup path
- credentialed `list_basins` smoke

Acceptance:

- `cargo check -p firegrid-host` passes
- no Node/Fable packages are required for the Rust host

### Phase 2: Native S2 Substrate

Implement the durable S2 substrate directly against `s2-sdk`.

- ensure log and inbox streams
- claim ownership with fencing
- append committed records
- read replay history
- append inbox messages

Acceptance:

- Rust tests cover the same substrate invariants as the F# S2 substrate proof
- S2 SDK usage is native Rust only
- no JavaScript SDK, npm package, or Fable interop is involved

### Phase 3: Rust Durable Core

Port the stable durable semantics into Rust:

- step records
- codecs
- replay state machine
- command planning
- activity completion folding
- status derivation

Acceptance:

- Rust unit tests mirror the meaningful F# proof properties
- durable core has no direct S2/process/telemetry calls
- code reads as native Rust, not transliterated F# where that hurts clarity

### Phase 4: Process Runner

Add a process runner using `processkit` or a comparably strong native runner.

Required behavior:

- command/args/cwd/env allow-list
- secret env projection
- stdin/stdout/stderr streaming
- timeout and cancellation
- whole process tree termination
- structured exit status and failure payloads

Acceptance:

- nonzero exit, timeout, and process-tree kill tests pass
- no shell invocation is required for normal commands
- process output limits are enforced
- process runner failures map cleanly into durable activity failures

### Phase 5: Descriptor Boundary

Define the contract between developer-facing code and the Rust host.

Candidate format:

```text
firegrid app descriptor
  steps
  workflows
  entities
  codecs
  CLI/process-backed step definitions
```

This boundary should be stable enough that an F# authoring layer, Rust authoring
layer, or future CLI tooling can all target the same host.

Acceptance:

- a workflow descriptor can be loaded by the Rust host
- a process-backed step can be invoked by the Rust host
- host output/status can be queried without Fable/Node
- descriptor validation rejects duplicate names, missing handlers, invalid
  codecs, and unsafe process configs

## Fable Rust Position

Fable's Rust target is useful for experiments, but it should not drive the
runtime migration.

Use Fable Rust for:

- exploratory compile checks
- future authoring-layer experiments
- identifying which F# patterns keep future transpilation options open

Do not use it yet for:

- Rust host implementation
- native S2 access
- subprocess ownership
- telemetry
- the long-running production host

Those pieces should be Rust-native.

## Open Questions

- Should the Rust host consume serialized descriptors, generated Rust code, or both?
- Which parts of the current F# durable core are worth porting directly versus
  redesigning around Rust types?
- Should the first process runner use `processkit` immediately or start with
  `tokio::process` and graduate once semantics are clear?
- How much of the proof harness should move to Rust tests versus remaining as
  cross-language acceptance checks?
- Which legacy F# proof properties are mandatory acceptance gates for the first
  Rust host milestone?
