# Rust Host SDD

## Status

This branch is reset to a narrow Rust host scaffold. It should not introduce a
parallel durable IR, parallel planner, parallel codec, or parallel authoring API.

The existing F# durable implementation remains the design source of truth:

- `src/Foundation/Durable/Semantics.fs`
- `src/Foundation/Durable/Stepper.fs`
- `src/Foundation/Durable/StepRecordCodec.fs`
- `src/Foundation/Durable/S2Substrate.fs`
- `src/Foundation/Durable/Host.fs`
- `src/Foundation/Durable/Runtime.fs`
- `src/Foundation/Durable/App.fs`
- `src/Firegrid.fs`

## Goal

Build a native Rust systems host that follows the existing Firegrid durable host
model instead of inventing another durable stack.

The Rust host should eventually own:

- native S2 access through `s2-sdk`
- process supervision for external agents
- activity command dispatch
- timer dispatch
- command checkpoints
- inbox/history append loops

The Rust host should not define new workflow semantics unless the existing F#
design is explicitly changed first.

## Current Scaffold

The branch currently includes:

- Cargo workspace root
- `crates/firegrid-host`
- a small `s2-sdk` client construction/list-basins executable

Validation:

```sh
cargo check -p firegrid-host
```

Credentialed smoke:

```sh
S2_ACCESS_TOKEN=... cargo run -p firegrid-host
```

Without credentials, the binary exits successfully with a setup hint.

## Correct Next Step

Before more Rust code is added, produce a direct mapping from the existing F#
host modules to Rust host modules:

| Existing F# module | Rust host responsibility |
| --- | --- |
| `StepRecordCodec.fs` | decode/encode the same durable log records |
| `Stepper.fs` | plan host commands from replay outcome |
| `S2Substrate.fs` | native S2 read/append/fencing |
| `CommandDispatch.fs` | dispatch outgoing commands idempotently |
| `ActivityAdapter.fs` | run registered/process-backed activities |
| `TimerAdapter.fs` | schedule/fire/cancel timers |
| `InboxFold.fs` | fold starts/signals/completions into history |
| `Host.fs` | coordinate one workflow tick |
| `Runtime.fs` / `App.fs` | assemble client, worker, registry, and app facade |

The first real implementation slice should be one existing responsibility,
ported or adapted directly, with the old module used as the behavioral reference.

## Non-Goals

- No `DurableIr` replacement stack.
- No separate portable authoring namespace.
- No duplicate planner or codec while `Stepper.fs` and `StepRecordCodec.fs`
  already define that behavior.
- No in-memory local runner unless it is explicitly part of the product surface.
- No npm/Fable-JS validation for this Rust host scaffold.
