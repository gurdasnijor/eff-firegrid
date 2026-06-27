namespace Eff

open Fable.Core
open Fable.Core.JsInterop
open Eff.Interop

/// Typed, chunked, deduplicated messaging on top of the base S2 sessions.
///
/// Wraps `@s2-dev/streamstore-patterns` (`SerializingAppendSession` /
/// `DeserializingReadSession`), fed by our own `S2.appendSession` and a bytes
/// `S2.readSession`. Adds:
///   - typed messages (bring a serializer/deserializer; `Json` provided)
///   - automatic chunking of messages > 1 MiB, with framing/reassembly
///   - dedupe headers for idempotent retries (single active writer assumed)
module S2Patterns =

    // The package re-exports everything under a `serialization` namespace.
    let private serialization: obj =
        import "serialization" "@s2-dev/streamstore-patterns"

    let private sasCtor: obj = serialization?SerializingAppendSession
    let private drsCtor: obj = serialization?DeserializingReadSession

    [<Emit("new $0($1, $2, $3)")>]
    let private newSAS (ctor: obj) (sess: obj) (serialize: obj) (opts: obj) : obj = jsNative

    [<Emit("new $0($1, $2)")>]
    let private newDRS (ctor: obj) (sess: obj) (deserialize: obj) : obj = jsNative

    [<Emit("$0.submit($1)")>]
    let private sasSubmit (sas: obj) (msg: obj) : JS.Promise<obj> = jsNative

    [<Emit("$0.start")>]
    let private rangeStart (r: obj) : float = jsNative

    [<Emit("$0.end")>]
    let private rangeEnd (r: obj) : float = jsNative

    [<Emit("$0.cancel()")>]
    let private drsCancel (drs: obj) : JS.Promise<unit> = jsNative

    // DeserializingReadSession is a plain web ReadableStream; drain it via a reader.
    [<Emit("$0.getReader()")>]
    let private getReader (rs: obj) : obj = jsNative

    [<Emit("$0.read()")>]
    let private readerRead (rdr: obj) : JS.Promise<obj> = jsNative

    [<Emit("$0.releaseLock()")>]
    let private releaseLock (rdr: obj) : unit = jsNative

    /// Range of seq nums a submitted message occupied (may span multiple records when chunked).
    type MessageRange = { Start: int64; End: int64 }

    /// A typed producer: serializes, chunks (>1 MiB), frames, and dedupes appends.
    type Producer<'msg> =
        internal
            { Raw: obj
              Session: IAppendSession }

    /// A typed consumer: dedupes, reassembles frames, and deserializes reads.
    type Consumer<'msg> = internal { Raw: obj }

    // ---- Producer ----

    /// Open a producer on a stream with the given serializer (`'msg -> byte[]`).
    let producer (serialize: 'msg -> byte[]) (stream: S2.Stream) : Async<Producer<'msg>> =
        async {
            let! sess = stream |> S2.openRawAppendSession
            let sas = newSAS sasCtor sess (box serialize) (createObj [ "dedupeSeq" ==> 0 ])
            return { Producer.Raw = sas; Session = sess }
        }

    /// Submit a message; resolves once it is durably appended.
    let submit (msg: 'msg) (p: Producer<'msg>) : Async<MessageRange> =
        async {
            let! r = Async.AwaitPromise(sasSubmit p.Raw (box msg))

            return
                { Start = int64 (rangeStart r)
                  End = int64 (rangeEnd r) }
        }

    /// Flush and close a producer.
    let closeProducer (p: Producer<'msg>) : Async<unit> =
        async { do! Async.AwaitPromise(p.Session.close ()) }

    // ---- Consumer ----

    /// Open a consumer on a stream from a position, with the given deserializer (`byte[] -> 'msg`).
    ///
    /// Reads from `from` up to the current tail and then ends (`waitSecs = 0`), so it
    /// drains existing messages rather than tailing indefinitely.
    let consumer (deserialize: byte[] -> 'msg) (from: S2.ReadFrom) (stream: S2.Stream) : Async<Consumer<'msg>> =
        async {
            let! sess =
                stream
                |> S2.openRawBytesReadSession
                    { S2.ReadOptions.empty with
                        Start = Some from
                        WaitSecs = Some 0 }

            let drs = newDRS drsCtor sess (box deserialize)
            return { Consumer.Raw = drs }
        }

    /// Consume messages, invoking `handler` for each until the session ends.
    let iter (handler: 'msg -> Async<unit>) (c: Consumer<'msg>) : Async<unit> =
        async {
            let reader = getReader c.Raw
            let mutable go = true

            while go do
                let! res = Async.AwaitPromise(readerRead reader)

                if iterDone res then
                    go <- false
                else
                    do! handler (unbox<'msg> (iterValue res))

            releaseLock reader
        }

    /// Read up to `n` messages, then stop (cancels the session).
    let take (n: int) (c: Consumer<'msg>) : Async<'msg list> =
        async {
            let reader = getReader c.Raw
            let acc = ResizeArray<'msg>()
            let mutable go = true

            while go && acc.Count < n do
                let! res = Async.AwaitPromise(readerRead reader)

                if iterDone res then
                    go <- false
                else
                    acc.Add(unbox<'msg> (iterValue res))

            releaseLock reader
            return List.ofSeq acc
        }

    /// Cancel/close a consumer (best-effort; safe to call after a partial `take`).
    let closeConsumer (c: Consumer<'msg>) : Async<unit> =
        async {
            try
                do! Async.AwaitPromise(drsCancel c.Raw)
            with _ ->
                ()
        }

    // ---- JSON convenience (zero-dependency) ----

    module Json =
        [<Emit("new TextEncoder().encode(JSON.stringify($0))")>]
        let private encode (x: obj) : byte[] = jsNative

        [<Emit("JSON.parse(new TextDecoder().decode($0))")>]
        let private decode (b: byte[]) : obj = jsNative

        /// Serialize a value to JSON bytes (records/values stringify directly).
        let serialize<'msg> (m: 'msg) : byte[] = encode (box m)

        /// Deserialize JSON bytes. NOTE: yields a structurally-correct object with working
        /// field access, but not a real F# record instance — use a proper decoder (e.g.
        /// Thoth.Json) if you need structural equality / pattern matching on the result.
        let deserialize<'msg> (b: byte[]) : 'msg = unbox<'msg> (decode b)
