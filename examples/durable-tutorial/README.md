# Durable Runtime Tutorial

This tutorial is the first copyable application surface for the durable runtime.
It intentionally uses `DurableRuntime.create` rather than the lower substrate
modules.

The script covers:

- registering activities and workflows
- constructing a runtime from an S2 basin
- starting a workflow with a generated instance id
- running the host until the instance is idle
- reading durable status
- raising an external signal
- racing an external signal against a timer

## Run

The tutorial uses the same S2 auth path as `repl.fsx`.

```sh
EFF_FIREGRID_BASIN=test-basin-885234 \
  dotnet fable examples/durable-tutorial/src/Tutorial.fsx --outDir build_examples --runScript
```

The basin must already exist. The script creates per-instance durable streams
and deletes them before exiting.

## Maintained By Check

`npm run check` transpiles the tutorial through Fable. It does not run the
script, because running requires live S2 credentials and a basin.
