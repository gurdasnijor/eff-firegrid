# Firegrid Tutorial

Small F#-native Firegrid authoring example.

The example intentionally starts at the public `Eff.Firegrid` surface:

- ordinary `async` domain functions
- `step` values
- `durable {}` workflows
- `call`, `all`, and `both`
- `firegrid { step ...; workflow ... }`

It does not require authors to assemble durable registries or runtime internals.

