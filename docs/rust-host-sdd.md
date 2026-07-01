# Rust Host SDD

## Status

Draft implementation direction for moving Firegrid systems pieces off the
current Fable/Node runtime and onto a native Rust host.

This is not a proposal to rewrite the developer-facing API first. It is a
proposal to extract the volatile systems boundary first: S2 access, process
supervision, durable host loops, timers, and telemetry.

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

Build a native `firegrid-host` Rust binary that owns systems concerns while the
high-level authoring surface remains free to evolve.

Target split:

```text
F# / future authoring layer
  step / workflow / entity descriptors
  durable authoring ergonomics
  descriptor serialization

Rust host
  native S2 client
  durable stream/inbox/log ownership
  host loop
  timer dispatcher
  process supervision
  telemetry

Rust core library
  shared codecs
  durable state machines
  replay/fold semantics
  validation model
```

## Native Dependencies

Initial Rust host dependencies:

- `s2-sdk`: native S2 client
- `tokio`: async runtime
- `processkit`: candidate subprocess/process-tree runner
- `tracing` / `opentelemetry`: native telemetry path

The branch currently includes a compile-backed `firegrid-host` scaffold using
`s2-sdk = "0.31.7"`.

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

### Phase 2: Rust S2 Substrate

Port the current durable S2 substrate concepts from F#:

- ensure log and inbox streams
- claim ownership with fencing
- append committed records
- read replay history
- append inbox messages

Acceptance:

- Rust tests cover the same substrate invariants as the F# S2 substrate proof
- S2 SDK usage is native Rust only

### Phase 3: Rust Durable Core

Port only the stable pure semantics:

- step records
- codecs
- replay state machine
- command planning
- activity completion folding
- status derivation

Acceptance:

- Rust unit tests mirror the meaningful F# proof properties
- no JS interop appears in durable core

### Phase 4: Process Runner

Add a process boundary using `processkit` or a comparably strong native runner.

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

## Fable Rust Position

Fable's Rust target is useful for experiments and may compile pure F# domain
logic, but it should not be the systems-runtime plan yet.

Use Fable Rust for:

- probing whether pure F# semantics can become Rust
- validating the shape of small authoring/core slices

Do not use it yet for:

- native S2 access
- subprocess ownership
- telemetry
- the long-running production host

Those pieces should be Rust-native.

## Open Questions

- Should the Rust host consume serialized descriptors, generated Rust code, or
  both?
- Which parts of the current F# durable core are worth porting directly versus
  redesigning around Rust types?
- Should the first process runner use `processkit` immediately or start with
  `tokio::process` and graduate once semantics are clear?
- How much of the proof harness should move to Rust tests versus remaining as
  cross-language acceptance checks?
