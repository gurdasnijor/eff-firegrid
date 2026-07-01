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

The codebase should maximize target flexibility. Firegrid should not repeat the
current JavaScript coupling in Rust form. The durable core must stay portable
across targets; only narrow adapter layers should know about Node, .NET, native
Rust crates, local filesystems, process APIs, clocks, network clients, or
telemetry backends.

Target split:

```text
Authoring layer
  step / workflow / entity descriptors
  durable authoring ergonomics
  descriptor serialization

Target-agnostic core
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

## Target-Agnostic Core Requirement

Most of the codebase should be written as target-agnostic core logic.

Target-agnostic means:

- no `Fable.Core.JsInterop` in core modules
- no Node-specific packages in core modules
- no direct filesystem, process, environment, clock, network, or telemetry calls
- no promise-specific APIs such as `Async.AwaitPromise` in core modules
- no generated JavaScript assumptions in public semantics
- deterministic functions over explicit input values wherever possible
- all nondeterminism enters through small port interfaces

Target-specific code belongs behind ports:

```text
StoragePort
  append / read / claim / ensure-stream

ClockPort
  now / sleep-until

ProcessPort
  run / stream / kill-tree

TelemetryPort
  span / event / attributes

EnvironmentPort
  read variable / project secret
```

Each port can have multiple adapters:

- Fable/Node adapter for current continuity
- native Rust adapter for the new host
- .NET adapter if we later want a normal .NET/F# runtime
- fake/in-memory adapter for deterministic tests

Compile contracts should reflect this split:

```text
pure core
  dotnet build
  dotnet fable --lang javascript
  dotnet fable --lang rust, where supported
  cargo test for Rust-native core ports

target adapters
  adapter-specific compile and integration tests
```

The first rule for new work: if a feature can be expressed in target-agnostic
core, put it there. Only put code in the Rust host, Fable/Node layer, or future
.NET host when it is genuinely about that target.

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

Implement a native Rust adapter for the storage port using `s2-sdk`.

- ensure log and inbox streams
- claim ownership with fencing
- append committed records
- read replay history
- append inbox messages

Acceptance:

- Rust tests cover the same substrate invariants as the F# S2 substrate proof
- S2 SDK usage is native Rust only
- target-agnostic storage semantics remain independent of `s2-sdk`

### Phase 3: Target-Agnostic Durable Core

Extract or port only the stable pure semantics:

- step records
- codecs
- replay state machine
- command planning
- activity completion folding
- status derivation

Acceptance:

- Rust unit tests mirror the meaningful F# proof properties
- no JS interop appears in durable core
- the same semantics have a clear path to Fable Rust or native Rust validation

### Phase 4: Process Runner

Add a native Rust adapter for the process port using `processkit` or a
comparably strong native runner.

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
- workflow semantics call a process port, not `processkit` directly

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
- descriptor validation is target-agnostic

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
- What is the minimal first `core` compile contract that should be enforced in
  CI across more than one target?
- Should the first process runner use `processkit` immediately or start with
  `tokio::process` and graduate once semantics are clear?
- How much of the proof harness should move to Rust tests versus remaining as
  cross-language acceptance checks?
