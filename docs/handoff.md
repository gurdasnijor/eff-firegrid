# Session Handoff — eff-firegrid

Living handoff for the next session/agent. Last updated 2026-06-27.

## TL;DR

`eff-firegrid` is an **F#/Fable binding over the S2 (s2.dev) streaming SDK**, now
growing a durable stream-processing **foundation** on top of it
(`StreamLog → StateView → KvStore`, modeled on the S2 KV demo).

- **`main` (`ebf476f`):** complete S2 client (L0) + patterns + live test suite +
  quality gate + benchmarks. Pushed, CI green.
- **PR #1 (`foundation-streamlog-stateview-sdd`, OPEN, MERGEABLE):** adds L1
  `StreamLog`, the L0 pull cursor, the first proof script, and a major SDD
  rewrite. Review feedback already addressed (`b05062f`). **This branch is the
  current checkout.**
- **Next:** L2 `StateView` (the KV-demo orchestrator) — `scripts/foundation-01-state-view.fsx`.

Auth is zero-config via the local `s2` CLI config (`~/.config/s2/config.toml`,
read by `S2Cli`). Public repo: `github.com/gurdasnijor/eff-firegrid`.

## Run it

```bash
npm run play        # repl.fsx — full-surface playground tour (live)
npm run watch       # edit/save/rerun loop (RESTART after adding a #load — see gotchas)
npm test            # tests/Suite.fsx — live integration suite (ephemeral stream + cleanup)
npm run check       # quality gate: format-check + build + lint + suite (tools/check.sh)
npm run bench       # Tier-1 mitata micro-benchmarks (deterministic, no token)
npm run bench:e2e   # Tier-2 live throughput
npm run script:stream-log   # the StreamLog proof (live)
npm run format      # Fantomas
```

All `dotnet fable` runs go to a `--outDir` (gitignored). To verify a script
without racing the watcher's `build/`, compile to a **separate** out dir
(`build_test`, `build_bench`, `build_script`) then `node <dir>/<entry>.js`.

## Architecture / foundation roadmap

```
S2 client (L0)  ── src/S2/*              done (main)
  └ StreamLog (L1)  ── src/Foundation/StreamLog.fs   done (PR #1)
      └ StateView (L2) ── KV-demo orchestrator        NEXT
          └ KvStore (L3)                              after
```

`StateView` is the **s2-kv-demo `main.rs` orchestrator** generalized: one
host-local loop owns in-memory state, tails a stream via the L0 cursor, folds
records into state, serves eventual reads now and strong reads by waiting until
`AppliedTail >= checkTail`. **No progress stream.** `InvocationRuntime` (Pulsar
at-least-once, output-producing) is explicitly **deferred** — see
`docs/foundational-sdd.md`.

## Codebase map

```
src/S2/
  Interop.fs   internal raw JS bindings (interfaces + [<Emit>]/import helpers)
  Errors.fs    S2Errors.classify -> SeqNumMismatch | FencingTokenMismatch | RangeNotSatisfiable | Other
  Client.fs    the S2 module (~960 lines): connect, basin/stream, append/read,
               config plane, sessions (appendSession/readSession), the read CURSOR
               (readCursor/tryNext/closeReadCursor), managers (locations/metrics/tokens)
  Cli.fs       S2Cli — reads access_token from ~/.config/s2/config.toml (Node fs/os)
  Patterns.fs  S2Patterns — typed chunked/deduped messaging over @s2-dev/streamstore-patterns
src/Foundation/
  StreamLog.fs typed L1 boundary (codec over raw records + sessions + cursor)
tests/Suite.fsx          live integration suite (npm test)
benchmarks/Micro.fsx     Tier-1 (mitata);  Throughput.fsx  Tier-2 (live);  RESULTS.md
scripts/foundation-00-stream-log.fsx   the L1 proof
repl.fsx                 playground tour
docs/                    foundational-sdd.md, benchmarking-proposal.md, proof-runner-proposal.md, this file
tools/                   check.sh (CI gate), hooks/pre-commit (autoformat)
```

## L0 client quick reference (`Eff.S2`)

- Connect: `connect token` / `connectWith ConnectOptions` (endpoints/retry/compression).
- Handles: `basin name`, `stream name`, `basinName`, `streamName`.
- Append: `append (Record list)`, `appendStrings`, `appendWith opts`,
  `appendIfSeqNum`, `appendIfFenced`, `tryAppendWith` → `Result<AppendAck,_>`.
  Records: `Record.text/textWith/bytes/bytesWith/fence/trim`.
- Read: `read from count`, `readWith ReadOptions`, `readLast n`, `checkTail`.
- Append sessions: `appendSession` → `submit` → `Ticket` → `ack` (durable
  `AppendAck`), `submitAck`, `lastAcked`, `closeAppendSession`.
- Read sessions: `readSession`/`iter`/`take`/`closeReadSession`, and the
  **cursor** `readCursor`/`tryNext` (pull-one, session stays open)/`closeReadCursor`.
- Config plane: `createStreamWith`/`getStreamConfig`/`reconfigureStream`/`ensureStreamWith`,
  basin equivalents, `listStreamsWith`/`listBasinsWith`.
- Managers: `listLocations`/`getDefaultLocation`/`setDefaultLocation`,
  `accountMetrics`/`basinMetrics`/`streamMetrics`, `listTokens`/`issueToken`/`revokeToken`.

## Key invariants / decisions (do NOT re-derive)

- **Version = exclusive next-seq.** `AppendAck.End.SeqNum` / `Tail.SeqNum` and
  `checkTail` all return the *next* seq (the value to compare against). A
  record's own position is `ReadRecord.SeqNum`. StreamLog encodes this as
  `StreamVersion` (exclusive) vs `SeqNum` (position) — keep them distinct.
- **Foundation = 1 event = 1 raw S2 record + codec.** NOT `S2Patterns` (its
  per-message chunk/frame/dedupe is for >1 MiB payloads / idempotent retries).
- **Writes ack on durable append, before local apply** — valid only for
  ops that don't return prior state (put/delete). Ops returning prior state need
  a different path.
- **Strong read = `checkTail` + wait `AppliedTail >= tail`**, pending reads keyed
  by required tail, drained as records apply (s2-kv-demo `PendingResponses`).
- **Conditional append:** `matchSeqNum expected` → `SeqNumMismatch actual` →
  `AppendConflict { Expected; Actual }`.
- Benchmarks: BenchmarkDotNet does **not** fit (Fable/JS, not .NET) — use mitata
  (micro) + a live throughput script. Wrapper overhead is proven negligible
  (~320 ns/append); serialization is the only CPU that scales (~0.86 ns/byte).

## Gotchas (hard-won; save yourself the rediscovery)

- **Fable watcher does not re-crack `#load` when you add a new module file —
  RESTART `npm run watch`.** Keep `.fsproj` `<Compile>` order and script `#load`
  order in sync (deps first).
- **Read-session shapes differ:** the base `ReadSession` is the SDK's custom
  *AsyncIterable* (consume via `Symbol.asyncIterator`); the patterns
  `DeserializingReadSession` is a plain web `ReadableStream` (consume via
  `getReader()`). The patterns DRS **early-cancel is broken** (its `cancel`
  handler cancels a locked underlying session) — we work around it by opening
  with `waitSecs = 0` (drain to tail) + `releaseLock`, never `cancel`.
- **Tailing read sessions never end**; `iter` would block forever. Use the
  **cursor `tryNext`** (pull-one) for orchestrators. ⚠️ The "stop a *blocked*
  tailing cursor from another fiber" path (`closeReadCursor` while `tryNext` is
  awaiting) is **NOT yet proven** — the StateView proof must validate it.
- **Fable interop:** anonymous records keep camelCase field names verbatim;
  `byte[]` ↔ `Uint8Array`; F# tuples → JS arrays; `Async.AwaitPromise`/
  `Async.StartImmediate`; `[<Emit>]` for raw JS; `isNil` = `== null`
  (null or undefined). `Date.now()`/`new Date()` work at runtime (the no-`Date`
  rule is for workflow scripts, not Fable output).
- **`S2Patterns.Json.deserialize` returns a structurally-correct object, NOT a
  real F# record** — field access works, but `=`/pattern-matching are
  unreliable. Use a real decoder (Thoth.Json) if you need those.
- **`%A` on a Fable `option`** prints the raw JS (`true`/`undefined`), not
  `Some/None` — cosmetic only.
- Pre-commit hook autoformats staged `.fs`/`.fsx` (Fantomas, via
  `core.hooksPath tools/hooks`, set by the `prepare` script).

## Open items / next steps (prioritized)

1. **L2 `StateView`** — `scripts/foundation-01-state-view.fsx` → promote
   `src/Foundation/StateView.fs`. Shape: a `MailboxProcessor` (Fable-supported)
   racing `StreamLog.tryNext` (the cursor) against commands; fold into state +
   `AppliedTail`; eventual reads now, strong reads deferred in a required-tail
   set drained on apply. **Must prove: stop a blocked tailing cursor** (the
   unproven L0 path above) and the eventual-vs-strong read window.
2. **L3 `KvStore`** — concrete `StateView` with `Put/Delete` events,
   `get` eventual/strong.
3. **PR #1 residual minors** (optional, non-blocking): `appendExpected`
   `failwithf` on non-mismatch errors loses the typed `S2Failure`;
   `TypedReadSession.take` is one-shot (cancels the session) — prefer the cursor
   for repeated pulls; `closeReadCursor` double-cancels (`iterReturn` + `cancel`)
   — works in practice, confirm under concurrent blocked `tryNext`.

## Earlier-recommended hygiene (not yet done)

- **CI runs zero executable tests.** Add fast, no-token **unit tests** of the
  pure wrapper logic (`mkInput`/`readInput` are now `internal`; also config
  build↔parse, `S2Errors.classify`, `Json` round-trip). The patterns are
  unit-testable with a **fake append session** (see the TS `serialization.test.ts`).
- Shared **`scripts/_prelude.fsx`** to DRY the duplicated `#load` block +
  `exit`/`now`/`check`/`section`/`uniq` across Suite/Micro/Throughput/proofs.
- Extract config types + nullable-JS mapping to **`src/S2/Config.fs`**; centralize
  the `if isNil x then None else Some(unbox …)` pattern into one helper.
- Top-level **`README.md`** (public repo has none).
- **Pipelined producer** (the Tier-2 gap): wrap the patterns
  `SerializingAppendSession`'s `WritableStream`/`pipeTo` for high-throughput
  typed messaging (`producer.submit` is durable-per-message → ~10 rec/sec).
- CI: bump action majors (Node 20 deprecation warning); add the
  `S2_ACCESS_TOKEN` secret if you want the live integration job to actually run
  (it currently skips gracefully without it).

## References (cited by the SDD; reviewed)

- S2 KV blog: <https://s2.dev/blog/kv-store> — the read/write model.
- **`s2-kv-demo/src/main.rs`** — the authoritative orchestrator (no progress
  stream; in-memory map + `applied_state` exclusive bound; ack-on-append;
  strong-read pending heap; bus-stand `check_tail` batching).
- Pulsar functions-concepts — at-least-once/effectively-once (for the deferred
  InvocationRuntime).
- Durable Yjs rooms — checkpoint + trim + fencing (the deferred advanced story).
- Patterns examples: `read-write.ts` (paired submit/read), `ai-sdk.ts`
  (`tee` + `pipeTo` streaming), `serialization.test.ts` (framing/dedupe semantics
  + fake-session unit testing).
